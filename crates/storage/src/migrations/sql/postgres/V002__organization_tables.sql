-- ORCHER Organizations & Multi-Tenancy - Migration V002
-- This migration creates the organization tables for multi-tenancy support.
-- Organizations are created early (V002) so that organization_id foreign keys
-- can be added natively to all subsequent tables.

-- ============================================================================
-- EXTENSIONS
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ============================================================================
-- ENUMS
-- ============================================================================

-- Organization member role hierarchy
CREATE TYPE organization_member_role AS ENUM (
    'owner',      -- Full control over organization
    'admin',      -- Administrative access, can manage members
    'member',     -- Standard access to organization resources
    'guest'       -- Limited read-only access
);

-- Organization status
CREATE TYPE organization_status AS ENUM (
    'active',     -- Organization is active and operational
    'suspended',  -- Temporarily suspended (billing issues, etc.)
    'deleted',    -- Soft-deleted organization
    'trial'       -- Trial period organization
);

-- Organization plan type
CREATE TYPE organization_plan AS ENUM (
    'free',         -- Free tier with basic limits
    'starter',      -- Starter plan for small teams
    'professional', -- Professional plan for growing teams
    'enterprise'    -- Enterprise plan with custom limits
);

-- Invitation status
CREATE TYPE invitation_status AS ENUM (
    'pending',    -- Invitation sent, awaiting response
    'accepted',   -- Invitation accepted
    'rejected',   -- Invitation rejected by recipient
    'expired',    -- Invitation expired
    'cancelled'   -- Invitation cancelled by sender
);

-- ============================================================================
-- CORE ORGANIZATION TABLE
-- ============================================================================

CREATE TABLE organizations (
    -- Primary Key
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Organization Identity
    name VARCHAR(255) NOT NULL,
    display_name VARCHAR(255) NOT NULL,
    slug VARCHAR(100) UNIQUE NOT NULL,

    -- Organization Details
    description TEXT,
    logo_url TEXT,
    website TEXT,

    -- Organization Settings
    settings JSONB DEFAULT '{}',
    metadata JSONB DEFAULT '{}',

    -- Plan & Limits
    plan_type organization_plan NOT NULL DEFAULT 'free',
    max_users INTEGER NOT NULL DEFAULT 5,
    max_tasks_per_month INTEGER NOT NULL DEFAULT 1000,
    storage_limit_mb INTEGER NOT NULL DEFAULT 1000,

    -- Usage Tracking (computed/cached values)
    current_user_count INTEGER NOT NULL DEFAULT 0,
    tasks_this_month INTEGER NOT NULL DEFAULT 0,
    storage_used_mb INTEGER NOT NULL DEFAULT 0,

    -- Status
    status organization_status NOT NULL DEFAULT 'active',
    is_active BOOLEAN NOT NULL DEFAULT true,

    -- Billing Information
    billing_email VARCHAR(255),
    subscription_id VARCHAR(255),
    subscription_status VARCHAR(50),
    trial_ends_at TIMESTAMPTZ,

    -- Ownership
    owner_id UUID NOT NULL, -- References users.id (set after V005)

    -- Audit Fields
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by UUID, -- References users.id (set after V005)
    deleted_at TIMESTAMPTZ, -- Soft delete timestamp

    -- Constraints
    CONSTRAINT organizations_slug_format CHECK (
        slug ~* '^[a-z0-9][a-z0-9-]*[a-z0-9]$'
        AND LENGTH(slug) >= 3
        AND LENGTH(slug) <= 100
    ),
    CONSTRAINT organizations_name_length CHECK (LENGTH(name) >= 1 AND LENGTH(name) <= 255),
    CONSTRAINT organizations_limits_positive CHECK (
        max_users > 0
        AND max_tasks_per_month > 0
        AND storage_limit_mb > 0
    ),
    CONSTRAINT organizations_usage_within_limits CHECK (
        current_user_count >= 0
        AND tasks_this_month >= 0
        AND storage_used_mb >= 0
    )
);

-- Indexes for organizations
CREATE INDEX idx_organizations_slug ON organizations(slug) WHERE deleted_at IS NULL;
CREATE INDEX idx_organizations_owner_id ON organizations(owner_id);
CREATE INDEX idx_organizations_status ON organizations(status);
CREATE INDEX idx_organizations_plan_type ON organizations(plan_type);
CREATE INDEX idx_organizations_created_at ON organizations(created_at DESC);
CREATE INDEX idx_organizations_active ON organizations(is_active) WHERE is_active = true AND deleted_at IS NULL;
CREATE INDEX idx_organizations_settings ON organizations USING GIN(settings);
CREATE INDEX idx_organizations_metadata ON organizations USING GIN(metadata);

-- Comments
COMMENT ON TABLE organizations IS 'Core organizations table for multi-tenancy support';
COMMENT ON COLUMN organizations.slug IS 'URL-safe unique identifier (e.g., acme-corp, my-startup)';
COMMENT ON COLUMN organizations.settings IS 'Organization-specific settings as JSON (security, workflow, notifications)';
COMMENT ON COLUMN organizations.metadata IS 'Custom metadata and key-value pairs';
COMMENT ON COLUMN organizations.current_user_count IS 'Cached count of active members (updated via trigger)';
COMMENT ON COLUMN organizations.tasks_this_month IS 'Count of tasks created this month (reset monthly)';

-- ============================================================================
-- ORGANIZATION MEMBERS TABLE
-- ============================================================================

CREATE TABLE organization_members (
    -- Primary Key
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Relationships
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    user_id UUID NOT NULL, -- References users.id (set after V005)

    -- Member Details
    role organization_member_role NOT NULL DEFAULT 'member',
    title VARCHAR(100),       -- Job title within organization
    department VARCHAR(100),  -- Department within organization

    -- Status
    status VARCHAR(50) NOT NULL DEFAULT 'active',
    is_active BOOLEAN NOT NULL DEFAULT true,

    -- Invitation Tracking
    invited_by UUID, -- References users.id
    invited_at TIMESTAMPTZ,

    -- Activity Tracking
    joined_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_activity_at TIMESTAMPTZ,

    -- Audit Fields
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    removed_at TIMESTAMPTZ,  -- Soft delete for member removal
    removed_by UUID,         -- References users.id

    -- Constraints
    CONSTRAINT organization_members_status_check CHECK (
        status IN ('active', 'inactive', 'suspended')
    ),
    CONSTRAINT organization_members_unique_membership
        UNIQUE(organization_id, user_id)
);

-- Indexes for organization_members
CREATE INDEX idx_org_members_org_id ON organization_members(organization_id);
CREATE INDEX idx_org_members_user_id ON organization_members(user_id);
CREATE INDEX idx_org_members_role ON organization_members(role);
CREATE INDEX idx_org_members_status ON organization_members(status);
CREATE INDEX idx_org_members_active_members ON organization_members(organization_id, status)
    WHERE status = 'active' AND removed_at IS NULL;
CREATE INDEX idx_org_members_user_orgs ON organization_members(user_id, organization_id)
    WHERE status = 'active' AND removed_at IS NULL;

-- Comments
COMMENT ON TABLE organization_members IS 'User membership and roles within organizations';
COMMENT ON COLUMN organization_members.role IS 'Member role determining permissions within organization';
COMMENT ON COLUMN organization_members.title IS 'Optional job title for display purposes';
COMMENT ON COLUMN organization_members.invited_by IS 'User who invited this member (for audit trail)';

-- ============================================================================
-- ORGANIZATION INVITATIONS TABLE
-- ============================================================================

CREATE TABLE organization_invitations (
    -- Primary Key
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Relationships
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    invited_by UUID NOT NULL, -- References users.id

    -- Invitation Details
    email VARCHAR(255) NOT NULL,
    role organization_member_role NOT NULL DEFAULT 'member',

    -- Invitation Token (secure, unique)
    invitation_token TEXT UNIQUE NOT NULL,
    token_hash TEXT NOT NULL, -- Hashed version for security

    -- Status
    status invitation_status NOT NULL DEFAULT 'pending',

    -- Personal Message
    message TEXT,

    -- Expiration
    expires_at TIMESTAMPTZ NOT NULL,

    -- Acceptance/Rejection Details
    accepted_by UUID, -- References users.id
    accepted_at TIMESTAMPTZ,
    rejected_at TIMESTAMPTZ,
    rejection_reason TEXT,

    -- Audit Fields
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    cancelled_at TIMESTAMPTZ,
    cancelled_by UUID, -- References users.id

    -- Constraints
    CONSTRAINT org_invitations_email_format CHECK (
        email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
    ),
    CONSTRAINT org_invitations_message_length CHECK (
        message IS NULL OR LENGTH(message) <= 1000
    ),
    CONSTRAINT org_invitations_expires_in_future CHECK (
        expires_at > created_at
    )
);

-- Indexes for organization_invitations
CREATE INDEX idx_org_invitations_org_id ON organization_invitations(organization_id);
CREATE INDEX idx_org_invitations_email ON organization_invitations(email);
CREATE INDEX idx_org_invitations_token ON organization_invitations(invitation_token)
    WHERE status = 'pending';
CREATE INDEX idx_org_invitations_token_hash ON organization_invitations(token_hash);
CREATE INDEX idx_org_invitations_status ON organization_invitations(status);
CREATE INDEX idx_org_invitations_pending ON organization_invitations(organization_id, status, expires_at)
    WHERE status = 'pending';
CREATE INDEX idx_org_invitations_invited_by ON organization_invitations(invited_by);

-- Comments
COMMENT ON TABLE organization_invitations IS 'Organization member invitation system with secure token-based flow';
COMMENT ON COLUMN organization_invitations.invitation_token IS 'Unique token sent in invitation email (plaintext for lookups)';
COMMENT ON COLUMN organization_invitations.token_hash IS 'Hashed token for additional security validation';
COMMENT ON COLUMN organization_invitations.expires_at IS 'Invitation expiration timestamp (typically 7 days from creation)';

-- ============================================================================
-- ORGANIZATION SETTINGS TABLE
-- ============================================================================

CREATE TABLE organization_settings (
    -- Primary Key
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Relationship
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,

    -- Settings Categories (stored as JSONB for flexibility)
    security_settings JSONB DEFAULT '{
        "require_mfa": false,
        "password_policy": {
            "min_length": 8,
            "require_uppercase": true,
            "require_lowercase": true,
            "require_numbers": true,
            "require_special": false
        },
        "session_timeout_minutes": 480,
        "allowed_ip_ranges": []
    }',

    workflow_settings JSONB DEFAULT '{
        "max_concurrent_workflows": 10,
        "default_timeout_minutes": 60,
        "enable_auto_retry": true,
        "max_retry_attempts": 3
    }',

    notification_settings JSONB DEFAULT '{
        "email_notifications": true,
        "webhook_notifications": false,
        "slack_notifications": false,
        "notification_channels": []
    }',

    integration_settings JSONB DEFAULT '{
        "enabled_integrations": [],
        "api_rate_limits": {
            "requests_per_minute": 100,
            "requests_per_hour": 5000
        }
    }',

    -- Feature Flags (organization-specific features)
    features_enabled JSONB DEFAULT '{
        "advanced_workflows": false,
        "custom_plugins": false,
        "priority_support": false,
        "audit_logs": true,
        "api_access": true
    }',

    -- Customization
    branding JSONB DEFAULT '{
        "primary_color": "#1E40AF",
        "logo_url": null,
        "favicon_url": null
    }',

    theme JSONB DEFAULT '{
        "mode": "light",
        "custom_css": null
    }',

    -- Audit Fields
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Constraints
    CONSTRAINT organization_settings_unique_org UNIQUE(organization_id)
);

-- Indexes for organization_settings
CREATE INDEX idx_org_settings_org_id ON organization_settings(organization_id);
CREATE INDEX idx_org_settings_security ON organization_settings USING GIN(security_settings);
CREATE INDEX idx_org_settings_features ON organization_settings USING GIN(features_enabled);

-- Comments
COMMENT ON TABLE organization_settings IS 'Detailed organization settings and configuration (JSONB for flexibility)';
COMMENT ON COLUMN organization_settings.security_settings IS 'Security policies including MFA, password requirements, session timeouts';
COMMENT ON COLUMN organization_settings.workflow_settings IS 'Workflow execution settings and limits';
COMMENT ON COLUMN organization_settings.features_enabled IS 'Feature flags for plan-based feature access';

-- ============================================================================
-- DATABASE FUNCTIONS
-- ============================================================================

-- Function: Check if user is member of organization
CREATE OR REPLACE FUNCTION is_organization_member(
    p_user_id UUID,
    p_organization_id UUID
) RETURNS BOOLEAN AS $$
BEGIN
    RETURN EXISTS (
        SELECT 1
        FROM organization_members
        WHERE user_id = p_user_id
          AND organization_id = p_organization_id
          AND status = 'active'
          AND removed_at IS NULL
    );
END;
$$ LANGUAGE plpgsql STABLE SECURITY DEFINER;

COMMENT ON FUNCTION is_organization_member IS 'Check if a user is an active member of an organization';

-- Function: Get user's organizations with role
CREATE OR REPLACE FUNCTION get_user_organizations(p_user_id UUID)
RETURNS TABLE(
    organization_id UUID,
    organization_name VARCHAR,
    organization_slug VARCHAR,
    role organization_member_role,
    is_owner BOOLEAN
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        o.id,
        o.name,
        o.slug,
        om.role,
        o.owner_id = p_user_id AS is_owner
    FROM organizations o
    INNER JOIN organization_members om ON o.id = om.organization_id
    WHERE om.user_id = p_user_id
      AND om.status = 'active'
      AND om.removed_at IS NULL
      AND o.is_active = true
      AND o.deleted_at IS NULL
    ORDER BY om.joined_at DESC;
END;
$$ LANGUAGE plpgsql STABLE SECURITY DEFINER;

COMMENT ON FUNCTION get_user_organizations IS 'Get all organizations a user belongs to with their roles';

-- Function: Check if user has minimum role in organization
CREATE OR REPLACE FUNCTION has_organization_role(
    p_user_id UUID,
    p_organization_id UUID,
    p_required_role organization_member_role
) RETURNS BOOLEAN AS $$
DECLARE
    v_user_role organization_member_role;
    v_role_hierarchy INTEGER;
    v_required_hierarchy INTEGER;
BEGIN
    SELECT role INTO v_user_role
    FROM organization_members
    WHERE user_id = p_user_id
      AND organization_id = p_organization_id
      AND status = 'active'
      AND removed_at IS NULL;

    IF NOT FOUND THEN
        RETURN FALSE;
    END IF;

    v_role_hierarchy := CASE v_user_role
        WHEN 'owner' THEN 4
        WHEN 'admin' THEN 3
        WHEN 'member' THEN 2
        WHEN 'guest' THEN 1
    END;

    v_required_hierarchy := CASE p_required_role
        WHEN 'owner' THEN 4
        WHEN 'admin' THEN 3
        WHEN 'member' THEN 2
        WHEN 'guest' THEN 1
    END;

    RETURN v_role_hierarchy >= v_required_hierarchy;
END;
$$ LANGUAGE plpgsql STABLE SECURITY DEFINER;

COMMENT ON FUNCTION has_organization_role IS 'Check if user has at least the required role in organization (supports role hierarchy)';

-- Function: Get organization member count
CREATE OR REPLACE FUNCTION get_organization_member_count(p_organization_id UUID)
RETURNS INTEGER AS $$
DECLARE
    v_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO v_count
    FROM organization_members
    WHERE organization_id = p_organization_id
      AND status = 'active'
      AND removed_at IS NULL;

    RETURN COALESCE(v_count, 0);
END;
$$ LANGUAGE plpgsql STABLE SECURITY DEFINER;

COMMENT ON FUNCTION get_organization_member_count IS 'Get count of active members in an organization';

-- Function: Check if organization is within user limit
CREATE OR REPLACE FUNCTION is_within_user_limit(p_organization_id UUID)
RETURNS BOOLEAN AS $$
DECLARE
    v_max_users INTEGER;
    v_current_count INTEGER;
BEGIN
    SELECT max_users INTO v_max_users
    FROM organizations
    WHERE id = p_organization_id;

    v_current_count := get_organization_member_count(p_organization_id);

    RETURN v_current_count < v_max_users;
END;
$$ LANGUAGE plpgsql STABLE SECURITY DEFINER;

COMMENT ON FUNCTION is_within_user_limit IS 'Check if organization can add more users based on plan limits';

-- ============================================================================
-- TRIGGERS
-- ============================================================================

-- Trigger: Update updated_at on organizations
CREATE TRIGGER update_organizations_updated_at
    BEFORE UPDATE ON organizations
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Trigger: Update updated_at on organization_members
CREATE TRIGGER update_organization_members_updated_at
    BEFORE UPDATE ON organization_members
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Trigger: Update updated_at on organization_invitations
CREATE TRIGGER update_organization_invitations_updated_at
    BEFORE UPDATE ON organization_invitations
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Trigger: Update updated_at on organization_settings
CREATE TRIGGER update_organization_settings_updated_at
    BEFORE UPDATE ON organization_settings
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Trigger: Update organization member count cache
CREATE OR REPLACE FUNCTION update_organization_member_count()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' AND NEW.status = 'active' THEN
        UPDATE organizations
        SET current_user_count = current_user_count + 1
        WHERE id = NEW.organization_id;
    ELSIF TG_OP = 'UPDATE' THEN
        IF OLD.status = 'active' AND NEW.status != 'active' THEN
            UPDATE organizations
            SET current_user_count = GREATEST(current_user_count - 1, 0)
            WHERE id = NEW.organization_id;
        ELSIF OLD.status != 'active' AND NEW.status = 'active' THEN
            UPDATE organizations
            SET current_user_count = current_user_count + 1
            WHERE id = NEW.organization_id;
        END IF;
    ELSIF TG_OP = 'DELETE' AND OLD.status = 'active' THEN
        UPDATE organizations
        SET current_user_count = GREATEST(current_user_count - 1, 0)
        WHERE id = OLD.organization_id;
    END IF;

    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_org_member_count
    AFTER INSERT OR UPDATE OR DELETE ON organization_members
    FOR EACH ROW
    EXECUTE FUNCTION update_organization_member_count();

COMMENT ON FUNCTION update_organization_member_count IS 'Automatically update cached member count when members are added/removed';

-- Trigger: Create default organization settings
CREATE OR REPLACE FUNCTION create_default_organization_settings()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO organization_settings (organization_id)
    VALUES (NEW.id)
    ON CONFLICT (organization_id) DO NOTHING;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_create_org_settings
    AFTER INSERT ON organizations
    FOR EACH ROW
    EXECUTE FUNCTION create_default_organization_settings();

COMMENT ON FUNCTION create_default_organization_settings IS 'Automatically create default settings when organization is created';

-- Trigger: Auto-add organization owner as member
CREATE OR REPLACE FUNCTION add_owner_as_member()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO organization_members (
        organization_id,
        user_id,
        role,
        status,
        joined_at
    ) VALUES (
        NEW.id,
        NEW.owner_id,
        'owner',
        'active',
        NOW()
    )
    ON CONFLICT (organization_id, user_id) DO NOTHING;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_add_owner_as_member
    AFTER INSERT ON organizations
    FOR EACH ROW
    EXECUTE FUNCTION add_owner_as_member();

COMMENT ON FUNCTION add_owner_as_member IS 'Automatically add organization owner as a member with owner role';
