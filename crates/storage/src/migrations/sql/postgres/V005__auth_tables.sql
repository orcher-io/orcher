-- ORCHER Authentication System - Migration V005
-- This migration creates all authentication and authorization tables.
-- The users table includes default_organization_id from multi-tenancy support.

-- ============================================================================
-- EXTENSIONS
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ============================================================================
-- ENUMS
-- ============================================================================

CREATE TYPE user_status AS ENUM (
    'pending',
    'active',
    'suspended',
    'deactivated',
    'archived'
);

-- ============================================================================
-- USERS TABLE
-- ============================================================================

CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    username VARCHAR(100) UNIQUE,
    email VARCHAR(255) UNIQUE NOT NULL,
    display_name VARCHAR(255),

    -- Password authentication
    password_hash TEXT NOT NULL,
    password_salt TEXT,
    hash_algorithm VARCHAR(20) NOT NULL DEFAULT 'bcrypt',
    password_changed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Account status and security
    is_active BOOLEAN NOT NULL DEFAULT true,
    status user_status NOT NULL DEFAULT 'active',
    email_verified BOOLEAN NOT NULL DEFAULT false,
    email_verification_token TEXT,
    email_verification_expires TIMESTAMPTZ,

    -- Account lockout and security
    failed_login_attempts INTEGER NOT NULL DEFAULT 0,
    locked_until TIMESTAMPTZ,
    last_login_at TIMESTAMPTZ,
    last_login_ip INET,

    -- Multi-factor authentication
    mfa_enabled BOOLEAN NOT NULL DEFAULT false,
    mfa_secret TEXT,
    mfa_backup_codes TEXT[],
    mfa_recovery_codes_used INTEGER NOT NULL DEFAULT 0,

    -- Password reset
    password_reset_token TEXT,
    password_reset_expires TIMESTAMPTZ,

    -- User preferences and metadata
    preferences JSONB DEFAULT '{}',
    metadata JSONB DEFAULT '{}',
    timezone VARCHAR(50) DEFAULT 'UTC',
    locale VARCHAR(10) DEFAULT 'en',

    -- User profile
    avatar_url TEXT,
    last_activity_at TIMESTAMPTZ,

    -- OAuth integration
    oauth_providers TEXT[] DEFAULT '{}',
    preferred_auth_method VARCHAR(20) DEFAULT 'password',

    -- Multi-tenancy: default organization context for user (last used or preferred)
    default_organization_id UUID REFERENCES organizations(id) ON DELETE SET NULL,

    -- Audit fields
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by UUID REFERENCES users(id),

    -- Constraints
    CONSTRAINT users_username_length CHECK (username IS NULL OR (LENGTH(username) >= 3 AND LENGTH(username) <= 100)),
    CONSTRAINT users_email_format CHECK (email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'),
    CONSTRAINT users_hash_algorithm_check CHECK (hash_algorithm IN ('bcrypt', 'argon2')),
    CONSTRAINT users_failed_attempts_check CHECK (failed_login_attempts >= 0),
    CONSTRAINT users_mfa_recovery_check CHECK (mfa_recovery_codes_used >= 0),
    CONSTRAINT users_preferred_auth_check CHECK (preferred_auth_method IN ('password', 'oauth', 'sso'))
);

-- ============================================================================
-- USER ROLES TABLE
-- ============================================================================

CREATE TABLE user_roles (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(50) UNIQUE NOT NULL,
    display_name VARCHAR(100) NOT NULL,
    description TEXT,

    is_system_role BOOLEAN NOT NULL DEFAULT false,
    is_active BOOLEAN NOT NULL DEFAULT true,

    inherits_from UUID REFERENCES user_roles(id),

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT user_roles_name_format CHECK (name ~* '^[a-z][a-z0-9_]*$')
);

-- ============================================================================
-- USER ROLE ASSIGNMENTS TABLE
-- ============================================================================

CREATE TABLE user_role_assignments (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    role_id UUID NOT NULL REFERENCES user_roles(id) ON DELETE CASCADE,

    assigned_by UUID REFERENCES users(id),
    assigned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ,

    context JSONB DEFAULT '{}',

    UNIQUE(user_id, role_id)
);

-- ============================================================================
-- PERMISSIONS TABLE
-- ============================================================================

CREATE TABLE permissions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(100) UNIQUE NOT NULL,
    display_name VARCHAR(150) NOT NULL,
    description TEXT,

    category VARCHAR(50) NOT NULL,
    subcategory VARCHAR(50),

    is_system_permission BOOLEAN NOT NULL DEFAULT false,
    is_dangerous BOOLEAN NOT NULL DEFAULT false,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT permissions_name_format CHECK (name ~* '^[A-Z][A-Za-z0-9]*$'),
    CONSTRAINT permissions_category_check CHECK (
        category IN ('Task', 'Workflow', 'System', 'User', 'ApiKey', 'Metrics', 'Logs', 'Alerts', 'Config')
    )
);

-- ============================================================================
-- ROLE PERMISSIONS TABLE
-- ============================================================================

CREATE TABLE role_permissions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    role_id UUID NOT NULL REFERENCES user_roles(id) ON DELETE CASCADE,
    permission_id UUID NOT NULL REFERENCES permissions(id) ON DELETE CASCADE,

    granted_by UUID REFERENCES users(id),
    granted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE(role_id, permission_id)
);

-- ============================================================================
-- USER SESSIONS TABLE
-- ============================================================================

CREATE TABLE user_sessions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    session_id VARCHAR(255) UNIQUE NOT NULL,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,

    session_type VARCHAR(50) NOT NULL DEFAULT 'web',
    user_agent TEXT,
    ip_address INET,
    country_code CHAR(2),
    city VARCHAR(100),

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_activity_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL,

    is_active BOOLEAN NOT NULL DEFAULT true,

    login_method VARCHAR(50) DEFAULT 'password',
    mfa_verified BOOLEAN NOT NULL DEFAULT false,
    risk_score DECIMAL(3,2) DEFAULT 0.0,

    session_data JSONB DEFAULT '{}',

    CONSTRAINT user_sessions_type_check CHECK (
        session_type IN ('web', 'mobile', 'api', 'websocket', 'cli')
    ),
    CONSTRAINT user_sessions_login_method_check CHECK (
        login_method IN ('password', 'mfa', 'api_key', 'oauth', 'sso')
    ),
    CONSTRAINT user_sessions_risk_score_check CHECK (risk_score >= 0.0 AND risk_score <= 1.0)
);

-- ============================================================================
-- REFRESH TOKENS TABLE
-- ============================================================================

CREATE TABLE refresh_tokens (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    token_id UUID UNIQUE NOT NULL,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    session_id UUID REFERENCES user_sessions(id) ON DELETE CASCADE,

    token_hash TEXT NOT NULL,
    family_id UUID NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL,
    used_at TIMESTAMPTZ,
    revoked_at TIMESTAMPTZ,

    ip_address INET,
    user_agent TEXT,

    is_revoked BOOLEAN NOT NULL DEFAULT false,
    revocation_reason VARCHAR(100),

    CONSTRAINT refresh_tokens_revocation_check CHECK (
        (is_revoked = false AND revoked_at IS NULL) OR
        (is_revoked = true AND revoked_at IS NOT NULL)
    )
);

-- ============================================================================
-- API KEYS TABLE
-- ============================================================================

CREATE TABLE api_keys (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,

    -- Multi-tenancy
    organization_id UUID REFERENCES organizations(id) ON DELETE CASCADE,

    name VARCHAR(255) NOT NULL,
    key_prefix VARCHAR(20) NOT NULL,
    key_hash TEXT NOT NULL,

    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ,
    last_used_at TIMESTAMPTZ,

    rate_limit_tier VARCHAR(50) DEFAULT 'standard',
    requests_per_minute INTEGER DEFAULT 60,
    requests_per_hour INTEGER DEFAULT 1000,
    requests_per_day INTEGER DEFAULT 10000,

    total_requests BIGINT NOT NULL DEFAULT 0,
    successful_requests BIGINT NOT NULL DEFAULT 0,
    failed_requests BIGINT NOT NULL DEFAULT 0,

    allowed_ips INET[],
    allowed_domains TEXT[],

    description TEXT,
    metadata JSONB DEFAULT '{}',

    CONSTRAINT api_keys_name_length CHECK (LENGTH(name) >= 1 AND LENGTH(name) <= 255),
    CONSTRAINT api_keys_rate_limits_check CHECK (
        requests_per_minute > 0 AND
        requests_per_hour > 0 AND
        requests_per_day > 0
    ),
    CONSTRAINT api_keys_tier_check CHECK (
        rate_limit_tier IN ('basic', 'standard', 'premium', 'enterprise', 'unlimited')
    )
);

-- ============================================================================
-- API KEY PERMISSIONS TABLE
-- ============================================================================

CREATE TABLE api_key_permissions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    api_key_id UUID NOT NULL REFERENCES api_keys(id) ON DELETE CASCADE,
    permission_id UUID NOT NULL REFERENCES permissions(id) ON DELETE CASCADE,

    granted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    granted_by UUID REFERENCES users(id),

    UNIQUE(api_key_id, permission_id)
);

-- ============================================================================
-- PASSWORD HISTORY TABLE
-- ============================================================================

CREATE TABLE password_history (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,

    password_hash TEXT NOT NULL,
    hash_algorithm VARCHAR(20) NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT password_history_algorithm_check CHECK (hash_algorithm IN ('bcrypt', 'argon2'))
);

-- ============================================================================
-- LOGIN ATTEMPTS TABLE
-- ============================================================================

CREATE TABLE login_attempts (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    username VARCHAR(100),
    email VARCHAR(255),
    user_id UUID REFERENCES users(id),

    ip_address INET NOT NULL,
    user_agent TEXT,
    success BOOLEAN NOT NULL,
    failure_reason VARCHAR(100),

    country_code CHAR(2),
    city VARCHAR(100),

    attempted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    risk_score DECIMAL(3,2) DEFAULT 0.0,

    metadata JSONB DEFAULT '{}',

    CONSTRAINT login_attempts_risk_score_check CHECK (risk_score >= 0.0 AND risk_score <= 1.0)
);

-- ============================================================================
-- AUDITS TABLE
-- ============================================================================

CREATE TABLE audits (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    event_type VARCHAR(100) NOT NULL,
    event_category VARCHAR(50) NOT NULL,

    user_id UUID REFERENCES users(id),
    session_id UUID REFERENCES user_sessions(id),
    api_key_id UUID REFERENCES api_keys(id),

    resource_type VARCHAR(100),
    resource_id TEXT,
    action VARCHAR(100) NOT NULL,
    outcome VARCHAR(50) NOT NULL,

    ip_address INET,
    user_agent TEXT,
    request_id UUID,

    event_data JSONB DEFAULT '{}',
    old_values JSONB,
    new_values JSONB,

    occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    risk_level VARCHAR(20) DEFAULT 'low',

    CONSTRAINT audit_category_check CHECK (
        event_category IN (
            'authentication',
            'authorization',
            'user_management',
            'api_access',
            'system',
            'service_operation',
            'workflow_execution'
        )
    ),
    CONSTRAINT audit_outcome_check CHECK (
        outcome IN ('success', 'failure', 'blocked', 'warning')
    ),
    CONSTRAINT audit_risk_check CHECK (
        risk_level IN ('low', 'medium', 'high', 'critical')
    )
);

-- ============================================================================
-- OAUTH ACCOUNTS TABLE
-- ============================================================================

CREATE TABLE oauth_accounts (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,

    provider VARCHAR(50) NOT NULL,
    provider_user_id VARCHAR(255) NOT NULL,

    access_token_encrypted TEXT,
    refresh_token_encrypted TEXT,
    token_expires_at TIMESTAMPTZ,

    provider_email VARCHAR(255),
    provider_username VARCHAR(255),
    provider_display_name VARCHAR(255),
    provider_avatar_url TEXT,
    provider_profile_url TEXT,

    scopes TEXT[],

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_login_at TIMESTAMPTZ,

    UNIQUE(provider, provider_user_id),
    UNIQUE(user_id, provider),
    CONSTRAINT oauth_accounts_provider_check CHECK (
        provider IN ('github', 'google', 'microsoft', 'gitlab', 'bitbucket')
    )
);

-- ============================================================================
-- OAUTH STATE TOKENS TABLE
-- ============================================================================

CREATE TABLE oauth_state_tokens (
    state_token VARCHAR(64) PRIMARY KEY,

    code_challenge VARCHAR(128) NOT NULL,
    code_challenge_method VARCHAR(10) NOT NULL DEFAULT 'S256',
    code_verifier VARCHAR(128) NOT NULL,

    provider VARCHAR(50) NOT NULL,
    redirect_uri TEXT NOT NULL,

    user_id UUID REFERENCES users(id) ON DELETE CASCADE,

    ip_address INET,
    user_agent TEXT,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL DEFAULT NOW() + INTERVAL '5 minutes',

    used BOOLEAN NOT NULL DEFAULT false,
    used_at TIMESTAMPTZ,

    CONSTRAINT oauth_state_provider_check CHECK (
        provider IN ('github', 'google', 'microsoft', 'gitlab', 'bitbucket')
    ),
    CONSTRAINT oauth_state_used_check CHECK (
        (used = false AND used_at IS NULL) OR
        (used = true AND used_at IS NOT NULL)
    )
);

-- ============================================================================
-- INDEXES
-- ============================================================================

-- Users table indexes
CREATE INDEX idx_users_username ON users(username);
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_users_active ON users(is_active);
CREATE INDEX idx_users_email_verified ON users(email_verified);
CREATE INDEX idx_users_locked ON users(locked_until) WHERE locked_until IS NOT NULL;
CREATE INDEX idx_users_last_login ON users(last_login_at);
CREATE INDEX idx_users_created_at ON users(created_at);
CREATE INDEX idx_users_last_activity ON users(last_activity_at) WHERE last_activity_at IS NOT NULL;
CREATE INDEX idx_users_default_org ON users(default_organization_id);

-- User sessions indexes
CREATE INDEX idx_user_sessions_session_id ON user_sessions(session_id);
CREATE INDEX idx_user_sessions_user_id ON user_sessions(user_id);
CREATE INDEX idx_user_sessions_active ON user_sessions(is_active, expires_at);
CREATE INDEX idx_user_sessions_cleanup ON user_sessions(expires_at) WHERE is_active = false;
CREATE INDEX idx_user_sessions_activity ON user_sessions(last_activity_at);
CREATE INDEX idx_user_sessions_ip ON user_sessions(ip_address);

-- API keys indexes
CREATE INDEX idx_api_keys_user_id ON api_keys(user_id);
CREATE INDEX idx_api_keys_active ON api_keys(is_active);
CREATE INDEX idx_api_keys_prefix ON api_keys(key_prefix);
CREATE INDEX idx_api_keys_expires ON api_keys(expires_at) WHERE expires_at IS NOT NULL;
CREATE INDEX idx_api_keys_last_used ON api_keys(last_used_at);

-- Refresh tokens indexes
CREATE INDEX idx_refresh_tokens_token_id ON refresh_tokens(token_id);
CREATE INDEX idx_refresh_tokens_user_id ON refresh_tokens(user_id);
CREATE INDEX idx_refresh_tokens_family ON refresh_tokens(family_id);
CREATE INDEX idx_refresh_tokens_expires ON refresh_tokens(expires_at);
CREATE INDEX idx_refresh_tokens_revoked ON refresh_tokens(is_revoked, revoked_at);

-- Role and permission indexes
CREATE INDEX idx_user_role_assignments_user ON user_role_assignments(user_id);
CREATE INDEX idx_user_role_assignments_role ON user_role_assignments(role_id);
CREATE INDEX idx_user_role_assignments_expires ON user_role_assignments(expires_at) WHERE expires_at IS NOT NULL;
CREATE INDEX idx_role_permissions_role ON role_permissions(role_id);
CREATE INDEX idx_role_permissions_permission ON role_permissions(permission_id);
CREATE INDEX idx_api_key_permissions_key ON api_key_permissions(api_key_id);
CREATE INDEX idx_api_key_permissions_permission ON api_key_permissions(permission_id);

-- Security and audit indexes
CREATE INDEX idx_login_attempts_username ON login_attempts(username);
CREATE INDEX idx_login_attempts_ip ON login_attempts(ip_address);
CREATE INDEX idx_login_attempts_success ON login_attempts(success, attempted_at);
CREATE INDEX idx_login_attempts_user_id ON login_attempts(user_id) WHERE user_id IS NOT NULL;
CREATE INDEX idx_login_attempts_time ON login_attempts(attempted_at);

CREATE INDEX idx_password_history_user ON password_history(user_id, created_at DESC);

CREATE INDEX idx_audit_user ON audits(user_id);
CREATE INDEX idx_audit_event ON audits(event_type, occurred_at);
CREATE INDEX idx_audit_category ON audits(event_category, occurred_at);
CREATE INDEX idx_audit_outcome ON audits(outcome, occurred_at);
CREATE INDEX idx_audit_risk ON audits(risk_level, occurred_at);
CREATE INDEX idx_audit_resource ON audits(resource_type, resource_id);

-- Composite indexes for common query patterns
CREATE INDEX idx_users_login_lookup ON users(username, is_active, email_verified);
CREATE INDEX idx_users_security_check ON users(failed_login_attempts, locked_until);
CREATE INDEX idx_sessions_user_active ON user_sessions(user_id, is_active, expires_at);
CREATE INDEX idx_api_keys_user_active ON api_keys(user_id, is_active, expires_at);

-- OAuth accounts indexes
CREATE INDEX idx_oauth_accounts_user_id ON oauth_accounts(user_id);
CREATE INDEX idx_oauth_accounts_provider_user_id ON oauth_accounts(provider, provider_user_id);
CREATE INDEX idx_oauth_accounts_provider_email ON oauth_accounts(provider_email);
CREATE INDEX idx_oauth_accounts_provider ON oauth_accounts(provider);
CREATE INDEX idx_oauth_accounts_last_login ON oauth_accounts(last_login_at);

-- OAuth state tokens indexes
CREATE INDEX idx_oauth_state_tokens_expires ON oauth_state_tokens(expires_at);
CREATE INDEX idx_oauth_state_tokens_cleanup ON oauth_state_tokens(expires_at) WHERE NOT used;
CREATE INDEX idx_oauth_state_tokens_user_id ON oauth_state_tokens(user_id) WHERE user_id IS NOT NULL;
CREATE INDEX idx_oauth_state_tokens_provider ON oauth_state_tokens(provider);

-- ============================================================================
-- TRIGGERS
-- ============================================================================

CREATE TRIGGER update_users_updated_at BEFORE UPDATE ON users
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_user_roles_updated_at BEFORE UPDATE ON user_roles
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_permissions_updated_at BEFORE UPDATE ON permissions
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_oauth_accounts_updated_at BEFORE UPDATE ON oauth_accounts
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- VIEWS
-- ============================================================================

CREATE VIEW v_user_permissions AS
SELECT DISTINCT
    u.id as user_id,
    u.username,
    p.name as permission_name,
    p.category as permission_category,
    'role' as grant_source,
    ur.name as source_role
FROM users u
JOIN user_role_assignments ura ON u.id = ura.user_id
JOIN user_roles ur ON ura.role_id = ur.id
JOIN role_permissions rp ON ur.id = rp.role_id
JOIN permissions p ON rp.permission_id = p.id
WHERE u.is_active = true
  AND ur.is_active = true
  AND (ura.expires_at IS NULL OR ura.expires_at > NOW());

CREATE VIEW v_active_sessions AS
SELECT
    s.*,
    u.username,
    u.email,
    u.display_name
FROM user_sessions s
JOIN users u ON s.user_id = u.id
WHERE s.is_active = true
  AND s.expires_at > NOW()
  AND u.is_active = true;

CREATE VIEW v_api_key_permissions AS
SELECT DISTINCT
    ak.id as api_key_id,
    ak.name as api_key_name,
    ak.user_id,
    u.username,
    p.name as permission_name,
    p.category as permission_category
FROM api_keys ak
JOIN users u ON ak.user_id = u.id
JOIN api_key_permissions akp ON ak.id = akp.api_key_id
JOIN permissions p ON akp.permission_id = p.id
WHERE ak.is_active = true
  AND u.is_active = true
  AND (ak.expires_at IS NULL OR ak.expires_at > NOW());

-- ============================================================================
-- SEED DATA: Default roles and permissions
-- ============================================================================

INSERT INTO user_roles (name, display_name, description, is_system_role) VALUES
('admin', 'Administrator', 'Full system access with all permissions', true),
('manager', 'Manager', 'Can manage workflows, tasks, and team members', true),
('user', 'User', 'Standard user with task and workflow permissions', true),
('viewer', 'Viewer', 'Read-only access to tasks and workflows', true),
('service', 'Service Account', 'For automated systems and integrations', true);

INSERT INTO permissions (name, display_name, description, category, is_system_permission) VALUES
('TaskCreate', 'Create Tasks', 'Create new tasks', 'Task', true),
('TaskRead', 'Read Tasks', 'View tasks and their details', 'Task', true),
('TaskUpdate', 'Update Tasks', 'Modify existing tasks', 'Task', true),
('TaskDelete', 'Delete Tasks', 'Remove tasks from the system', 'Task', true),
('TaskExecute', 'Execute Tasks', 'Start and manage task execution', 'Task', true),
('TaskCancel', 'Cancel Tasks', 'Cancel running tasks', 'Task', true),
('TaskRetry', 'Retry Tasks', 'Retry failed tasks', 'Task', true),
('WorkflowCreate', 'Create Workflows', 'Create new workflows', 'Workflow', true),
('WorkflowRead', 'Read Workflows', 'View workflows and their definitions', 'Workflow', true),
('WorkflowUpdate', 'Update Workflows', 'Modify existing workflows', 'Workflow', true),
('WorkflowDelete', 'Delete Workflows', 'Remove workflows from the system', 'Workflow', true),
('WorkflowExecute', 'Execute Workflows', 'Start workflow execution', 'Workflow', true),
('WorkflowPause', 'Pause Workflows', 'Pause running workflows', 'Workflow', true),
('WorkflowResume', 'Resume Workflows', 'Resume paused workflows', 'Workflow', true),
('WorkflowCancel', 'Cancel Workflows', 'Cancel workflow execution', 'Workflow', true),
('SystemRead', 'System Read', 'View system information and status', 'System', true),
('SystemWrite', 'System Write', 'Modify system configuration', 'System', true),
('SystemAdmin', 'System Administration', 'Full system administration access', 'System', true),
('UserCreate', 'Create Users', 'Create new user accounts', 'User', true),
('UserRead', 'Read Users', 'View user information', 'User', true),
('UserUpdate', 'Update Users', 'Modify user accounts', 'User', true),
('UserDelete', 'Delete Users', 'Remove user accounts', 'User', true),
('UserManage', 'Manage Users', 'Full user account management', 'User', true),
('ApiKeyCreate', 'Create API Keys', 'Create new API keys', 'ApiKey', true),
('ApiKeyRead', 'Read API Keys', 'View API key information', 'ApiKey', true),
('ApiKeyUpdate', 'Update API Keys', 'Modify existing API keys', 'ApiKey', true),
('ApiKeyDelete', 'Delete API Keys', 'Remove API keys', 'ApiKey', true),
('MetricsRead', 'Read Metrics', 'View system and application metrics', 'Metrics', true),
('LogsRead', 'Read Logs', 'View system and application logs', 'Logs', true),
('AlertsRead', 'Read Alerts', 'View system alerts and notifications', 'Alerts', true),
('AlertsManage', 'Manage Alerts', 'Configure and manage alerts', 'Alerts', true),
('ConfigRead', 'Read Configuration', 'View system configuration', 'Config', true),
('ConfigWrite', 'Write Configuration', 'Modify system configuration', 'Config', true);

DO $$
DECLARE
    admin_role_id UUID;
    manager_role_id UUID;
    user_role_id UUID;
    viewer_role_id UUID;
    service_role_id UUID;
BEGIN
    SELECT id INTO admin_role_id FROM user_roles WHERE name = 'admin';
    SELECT id INTO manager_role_id FROM user_roles WHERE name = 'manager';
    SELECT id INTO user_role_id FROM user_roles WHERE name = 'user';
    SELECT id INTO viewer_role_id FROM user_roles WHERE name = 'viewer';
    SELECT id INTO service_role_id FROM user_roles WHERE name = 'service';

    INSERT INTO role_permissions (role_id, permission_id)
    SELECT admin_role_id, id FROM permissions;

    INSERT INTO role_permissions (role_id, permission_id)
    SELECT manager_role_id, id FROM permissions
    WHERE name NOT IN ('SystemAdmin', 'UserDelete', 'ConfigWrite');

    INSERT INTO role_permissions (role_id, permission_id)
    SELECT user_role_id, id FROM permissions
    WHERE name IN (
        'TaskCreate', 'TaskRead', 'TaskUpdate', 'TaskExecute', 'TaskCancel', 'TaskRetry',
        'WorkflowRead', 'WorkflowExecute', 'WorkflowPause', 'WorkflowResume',
        'ApiKeyRead', 'MetricsRead'
    );

    INSERT INTO role_permissions (role_id, permission_id)
    SELECT viewer_role_id, id FROM permissions
    WHERE name IN ('TaskRead', 'WorkflowRead', 'UserRead', 'MetricsRead', 'LogsRead');

    INSERT INTO role_permissions (role_id, permission_id)
    SELECT service_role_id, id FROM permissions
    WHERE name IN (
        'TaskCreate', 'TaskRead', 'TaskUpdate', 'TaskExecute', 'TaskCancel',
        'WorkflowRead', 'WorkflowExecute', 'ApiKeyRead', 'MetricsRead'
    );
END $$;

-- Default admin user (password: 'admin123' - change in production!)
INSERT INTO users (
    username, email, display_name, password_hash, hash_algorithm, is_active, email_verified
) VALUES (
    'admin',
    'admin@orcher.local',
    'System Administrator',
    '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewdBPj/OJA.lC.9.',
    'bcrypt',
    true,
    true
);

INSERT INTO user_role_assignments (user_id, role_id)
SELECT u.id, r.id
FROM users u, user_roles r
WHERE u.username = 'admin' AND r.name = 'admin';

-- ============================================================================
-- OAUTH DEVICE FLOW (RFC 8628)
-- ============================================================================

CREATE TYPE device_code_status AS ENUM (
    'pending',
    'authorized',
    'denied',
    'expired'
);

CREATE TABLE oauth_device_codes (
    device_code VARCHAR(128) PRIMARY KEY,
    user_code VARCHAR(16) UNIQUE NOT NULL,
    provider VARCHAR(50) NOT NULL,
    verification_uri TEXT NOT NULL,
    verification_uri_complete TEXT,
    expires_at TIMESTAMPTZ NOT NULL DEFAULT (NOW() + INTERVAL '15 minutes'),
    interval_seconds INTEGER NOT NULL DEFAULT 5,
    last_poll_at TIMESTAMPTZ,
    status device_code_status NOT NULL DEFAULT 'pending',
    user_id UUID REFERENCES users(id) ON DELETE CASCADE,
    authorized_at TIMESTAMPTZ,
    poll_count INTEGER NOT NULL DEFAULT 0,
    ip_address INET,
    user_agent TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT valid_interval CHECK (interval_seconds >= 1 AND interval_seconds <= 60),
    CONSTRAINT authorized_requires_user CHECK (
        (status = 'authorized' AND user_id IS NOT NULL) OR
        (status != 'authorized')
    )
);

CREATE INDEX idx_device_codes_user_code ON oauth_device_codes(user_code) WHERE status = 'pending';
CREATE INDEX idx_device_codes_status ON oauth_device_codes(status);
CREATE INDEX idx_device_codes_expires ON oauth_device_codes(expires_at) WHERE status = 'pending';
CREATE INDEX idx_device_codes_user_id ON oauth_device_codes(user_id) WHERE user_id IS NOT NULL;
CREATE INDEX idx_device_codes_provider ON oauth_device_codes(provider);
CREATE INDEX idx_device_codes_created ON oauth_device_codes(created_at);

CREATE OR REPLACE FUNCTION expire_device_codes()
RETURNS void AS $$
BEGIN
    UPDATE oauth_device_codes
    SET status = 'expired', updated_at = NOW()
    WHERE status = 'pending' AND expires_at < NOW();
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_device_code_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_device_code_timestamp
    BEFORE UPDATE ON oauth_device_codes
    FOR EACH ROW
    EXECUTE FUNCTION update_device_code_timestamp();

CREATE OR REPLACE FUNCTION poll_device_code(
    p_device_code VARCHAR(128),
    OUT out_status device_code_status,
    OUT out_user_id UUID,
    OUT out_should_slow_down BOOLEAN
)
AS $$
DECLARE
    v_last_poll TIMESTAMPTZ;
    v_interval INTEGER;
    v_poll_count INTEGER;
BEGIN
    SELECT status, user_id, last_poll_at, interval_seconds, poll_count
    INTO out_status, out_user_id, v_last_poll, v_interval, v_poll_count
    FROM oauth_device_codes
    WHERE device_code = p_device_code;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Device code not found';
    END IF;

    IF v_last_poll IS NOT NULL AND
       NOW() - v_last_poll < (v_interval || ' seconds')::INTERVAL THEN
        out_should_slow_down := TRUE;
    ELSE
        out_should_slow_down := FALSE;
    END IF;

    UPDATE oauth_device_codes
    SET last_poll_at = NOW(), poll_count = poll_count + 1
    WHERE device_code = p_device_code;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- SERVICE AUTHENTICATION (service-based worker model)
-- ============================================================================

CREATE TYPE service_status AS ENUM ('active', 'suspended', 'deleted');

CREATE TABLE service_identities (
    service_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    service_name VARCHAR(255) NOT NULL,
    namespace VARCHAR(100) NOT NULL DEFAULT 'default',
    allowed_task_queues TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    status service_status NOT NULL DEFAULT 'active',
    registered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT service_identities_name_namespace_unique UNIQUE (service_name, namespace)
);

CREATE INDEX idx_service_identities_namespace ON service_identities(namespace);
CREATE INDEX idx_service_identities_status ON service_identities(status) WHERE status = 'active';
CREATE INDEX idx_service_identities_last_seen ON service_identities(last_seen_at DESC) WHERE status = 'active';
CREATE INDEX idx_service_identities_name ON service_identities(service_name);
CREATE INDEX idx_service_identities_task_queues ON service_identities USING GIN(allowed_task_queues);

CREATE TABLE service_api_keys (
    key_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    service_id UUID NOT NULL REFERENCES service_identities(service_id) ON DELETE CASCADE,
    key_hash TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ,
    is_active BOOLEAN NOT NULL DEFAULT true,
    last_used_at TIMESTAMPTZ,
    description TEXT,
    created_by UUID REFERENCES users(id),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT service_api_keys_expires_future CHECK (
        expires_at IS NULL OR expires_at > created_at
    )
);

CREATE INDEX idx_service_api_keys_service ON service_api_keys(service_id);
CREATE INDEX idx_service_api_keys_active ON service_api_keys(service_id, is_active) WHERE is_active = true;
CREATE INDEX idx_service_api_keys_expires ON service_api_keys(expires_at) WHERE expires_at IS NOT NULL AND is_active = true;
CREATE INDEX idx_service_api_keys_last_used ON service_api_keys(last_used_at DESC);

CREATE TABLE service_permissions (
    service_id UUID NOT NULL REFERENCES service_identities(service_id) ON DELETE CASCADE,
    permission VARCHAR(50) NOT NULL,
    granted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    granted_by UUID REFERENCES users(id),

    PRIMARY KEY (service_id, permission),
    CONSTRAINT service_permissions_check CHECK (
        permission IN (
            'poll_workflow_steps', 'poll_activity_tasks', 'start_workflow',
            'signal_workflow', 'query_workflow', 'cancel_workflow',
            'complete_workflow_step', 'complete_activity_task',
            'fail_workflow_step', 'fail_activity_task'
        )
    )
);

CREATE INDEX idx_service_permissions_service ON service_permissions(service_id);
CREATE INDEX idx_service_permissions_permission ON service_permissions(permission);

CREATE TABLE service_operation_log (
    log_id BIGSERIAL PRIMARY KEY,
    service_id UUID NOT NULL REFERENCES service_identities(service_id) ON DELETE CASCADE,
    operation VARCHAR(50) NOT NULL,
    workflow_id UUID,
    execution_id TEXT,
    task_queue VARCHAR(255),
    success BOOLEAN NOT NULL,
    error_message TEXT,
    ip_address INET,
    user_agent TEXT,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    duration_ms INT,
    metadata JSONB DEFAULT '{}'
);

CREATE INDEX idx_service_op_log_service ON service_operation_log(service_id, timestamp DESC);
CREATE INDEX idx_service_op_log_timestamp ON service_operation_log(timestamp DESC);
CREATE INDEX idx_service_op_log_operation ON service_operation_log(operation, timestamp DESC);
CREATE INDEX idx_service_op_log_workflow ON service_operation_log(workflow_id, timestamp DESC) WHERE workflow_id IS NOT NULL;
CREATE INDEX idx_service_op_log_execution ON service_operation_log(execution_id, timestamp DESC) WHERE execution_id IS NOT NULL;
CREATE INDEX idx_service_op_log_failed ON service_operation_log(timestamp DESC) WHERE success = false;

CREATE OR REPLACE FUNCTION update_service_last_seen(p_service_id UUID) RETURNS VOID AS $$
BEGIN
    UPDATE service_identities SET last_seen_at = NOW(), updated_at = NOW()
    WHERE service_id = p_service_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION service_can_access_task_queue(
    p_service_id UUID, p_task_queue VARCHAR(255)
) RETURNS BOOLEAN AS $$
DECLARE
    v_allowed_queues TEXT[];
BEGIN
    SELECT allowed_task_queues INTO v_allowed_queues
    FROM service_identities
    WHERE service_id = p_service_id AND status = 'active';

    IF NOT FOUND THEN RETURN FALSE; END IF;
    RETURN p_task_queue = ANY(v_allowed_queues);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_active_services(p_namespace VARCHAR(100))
RETURNS TABLE (
    service_id UUID,
    service_name VARCHAR(255),
    allowed_task_queues TEXT[],
    last_seen_at TIMESTAMPTZ,
    minutes_since_last_seen INT
) AS $$
BEGIN
    RETURN QUERY
    SELECT si.service_id, si.service_name, si.allowed_task_queues, si.last_seen_at,
        EXTRACT(EPOCH FROM (NOW() - si.last_seen_at))::INT / 60 AS minutes_since_last_seen
    FROM service_identities si
    WHERE si.namespace = p_namespace AND si.status = 'active'
    ORDER BY si.last_seen_at DESC NULLS LAST;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER service_identities_update_timestamp
    BEFORE UPDATE ON service_identities
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER service_api_keys_update_timestamp
    BEFORE UPDATE ON service_api_keys
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE VIEW v_active_services AS
SELECT
    si.service_id, si.service_name, si.namespace, si.allowed_task_queues, si.last_seen_at,
    EXTRACT(EPOCH FROM (NOW() - si.last_seen_at))::INT / 60 AS minutes_since_last_seen,
    array_agg(DISTINCT sp.permission) FILTER (WHERE sp.permission IS NOT NULL) AS permissions,
    COUNT(DISTINCT sak.key_id) FILTER (WHERE sak.is_active = true) AS active_key_count
FROM service_identities si
LEFT JOIN service_permissions sp ON si.service_id = sp.service_id
LEFT JOIN service_api_keys sak ON si.service_id = sak.service_id
WHERE si.status = 'active'
GROUP BY si.service_id, si.service_name, si.namespace, si.allowed_task_queues, si.last_seen_at;

-- ============================================================================
-- COMMENTS
-- ============================================================================

COMMENT ON TABLE users IS 'Core user accounts with authentication and profile data';
COMMENT ON TABLE user_roles IS 'Role definitions for role-based access control';
COMMENT ON TABLE user_role_assignments IS 'User-to-role assignments with optional expiration';
COMMENT ON TABLE permissions IS 'System permission definitions';
COMMENT ON TABLE role_permissions IS 'Role-to-permission assignments';
COMMENT ON TABLE user_sessions IS 'Active user sessions with security tracking';
COMMENT ON TABLE refresh_tokens IS 'JWT refresh tokens with rotation support';
COMMENT ON TABLE api_keys IS 'API authentication keys with usage tracking';
COMMENT ON TABLE api_key_permissions IS 'API key permission assignments';
COMMENT ON TABLE password_history IS 'Historical passwords to prevent reuse';
COMMENT ON TABLE login_attempts IS 'All login attempts for security monitoring';
COMMENT ON TABLE audits IS 'Comprehensive security event audit trail';
COMMENT ON TABLE oauth_accounts IS 'OAuth provider accounts linked to users for social login';
COMMENT ON TABLE oauth_state_tokens IS 'Temporary OAuth state tokens for CSRF protection and PKCE flow';
COMMENT ON TABLE oauth_device_codes IS 'OAuth 2.0 Device Flow codes for CLI and headless authentication (RFC 8628)';
COMMENT ON TABLE service_identities IS 'Registered services (workers) that connect to Orcher to execute workflows/tasks';
COMMENT ON TABLE service_api_keys IS 'API keys for service authentication (one service can have multiple keys for rotation)';
COMMENT ON TABLE service_permissions IS 'Fine-grained permissions for services (what operations they can perform)';
COMMENT ON TABLE service_operation_log IS 'Audit log for all service operations (polling, completing tasks, etc.)';

COMMENT ON COLUMN users.avatar_url IS 'URL or path to user profile avatar image. NULL if no avatar set.';
COMMENT ON COLUMN users.last_activity_at IS 'Timestamp of last user activity (any action). Used for session management and activity tracking.';
COMMENT ON COLUMN users.locked_until IS 'Timestamp when account lock expires. NULL if not locked. Account is locked when locked_until > NOW().';
COMMENT ON COLUMN users.is_active IS 'Whether user account is active. Maps to UserStatus enum in application layer.';
COMMENT ON COLUMN users.oauth_providers IS 'List of linked OAuth providers (github, google, microsoft, etc.)';
COMMENT ON COLUMN users.preferred_auth_method IS 'User preferred authentication method: password, oauth, or sso';
COMMENT ON COLUMN users.default_organization_id IS 'Default organization context for user (last used or preferred)';
COMMENT ON COLUMN oauth_accounts.provider IS 'OAuth provider: github, google, microsoft, gitlab, or bitbucket';
COMMENT ON COLUMN oauth_accounts.provider_user_id IS 'User ID from the OAuth provider (immutable)';
COMMENT ON COLUMN oauth_accounts.access_token_encrypted IS 'Encrypted OAuth access token (encrypted by application)';
COMMENT ON COLUMN oauth_accounts.refresh_token_encrypted IS 'Encrypted OAuth refresh token (encrypted by application)';
COMMENT ON COLUMN audits.event_category IS 'Event category: authentication, authorization, user_management, api_access, system, service_operation, workflow_execution';
COMMENT ON COLUMN service_identities.service_name IS 'Unique service name (e.g., payment-service, order-service)';
COMMENT ON COLUMN service_identities.namespace IS 'Logical namespace for multi-tenancy';
COMMENT ON COLUMN service_identities.allowed_task_queues IS 'Array of task queues this service is allowed to poll';
COMMENT ON COLUMN service_identities.status IS 'Service status: active, suspended, deleted';
COMMENT ON COLUMN service_identities.last_seen_at IS 'Last time service polled or sent heartbeat';
COMMENT ON COLUMN service_api_keys.key_hash IS 'Hashed API key (bcrypt/argon2) - raw key shown only once at creation';
COMMENT ON COLUMN service_api_keys.expires_at IS 'Optional expiration time for key rotation';
COMMENT ON COLUMN service_api_keys.last_used_at IS 'Last time this key was used for authentication';
COMMENT ON COLUMN service_permissions.permission IS 'Permission type: poll_workflow_steps, poll_activity_tasks, start_workflow, etc.';
COMMENT ON COLUMN service_operation_log.operation IS 'Operation type: poll, complete_step, start_workflow, etc.';
COMMENT ON COLUMN service_operation_log.duration_ms IS 'Operation duration in milliseconds';
COMMENT ON VIEW v_active_services IS 'Active services with their permissions and key counts';
