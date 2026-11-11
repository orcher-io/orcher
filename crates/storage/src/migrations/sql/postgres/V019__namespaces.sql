-- ORCHER Task Orchestrator - Namespace Management
-- Migration V019: First-class namespace entities for workflow isolation

-- ============================================================================
-- NAMESPACES TABLE
-- ============================================================================
--
-- Namespaces isolate workflow executions, task queues, and services.
-- Each namespace is unique within an organization.

CREATE TABLE namespaces (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Namespace identity
    name VARCHAR(100) NOT NULL,
    description TEXT NOT NULL DEFAULT '',

    -- Status: active, deprecated, deleted
    status VARCHAR(20) NOT NULL DEFAULT 'active',

    -- Ownership
    owner_email VARCHAR(255),

    -- Data retention for completed workflows (in days)
    retention_period_days INTEGER NOT NULL DEFAULT 7,

    -- Arbitrary extension data
    data JSONB NOT NULL DEFAULT '{}',

    -- Multi-tenancy: namespace is unique within an organization
    organization_id UUID,

    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMPTZ,

    CONSTRAINT uq_namespace_org UNIQUE (organization_id, name),
    CONSTRAINT chk_namespace_status CHECK (status IN ('active', 'deprecated', 'deleted')),
    CONSTRAINT chk_retention_positive CHECK (retention_period_days > 0)
);

-- ============================================================================
-- TRIGGERS
-- ============================================================================

CREATE OR REPLACE FUNCTION update_namespace_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_namespace_updated_at
    BEFORE UPDATE ON namespaces
    FOR EACH ROW
    EXECUTE FUNCTION update_namespace_updated_at();

-- ============================================================================
-- INDEXES
-- ============================================================================

CREATE INDEX idx_ns_org_status ON namespaces(organization_id, status);
CREATE INDEX idx_ns_name ON namespaces(name);

-- ============================================================================
-- SEED DATA
-- ============================================================================

INSERT INTO namespaces (name, description, retention_period_days)
VALUES ('default', 'Default namespace', 7)
ON CONFLICT DO NOTHING;

-- ============================================================================
-- COMMENTS
-- ============================================================================

COMMENT ON TABLE namespaces IS 'First-class namespace entities for workflow isolation';
COMMENT ON COLUMN namespaces.name IS 'Unique namespace name within an organization';
COMMENT ON COLUMN namespaces.status IS 'Lifecycle status: active, deprecated, or deleted';
COMMENT ON COLUMN namespaces.retention_period_days IS 'How long completed workflow data is retained';
COMMENT ON COLUMN namespaces.organization_id IS 'Owning organization (NULL for system namespaces)';
