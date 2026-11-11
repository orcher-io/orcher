-- ORCHER Task Orchestrator - Actor State Storage
-- Migration V016: Durable state for actor instances (virtual objects) with multi-tenancy

-- ============================================================================
-- ACTOR STATE TABLE
-- ============================================================================

CREATE TABLE actor_state (
    -- Primary key: state identifier
    key VARCHAR(512) PRIMARY KEY,

    -- State value (JSON serialized)
    value BYTEA NOT NULL,

    -- Version for optimistic locking
    version INTEGER NOT NULL DEFAULT 1,

    -- Multi-tenancy (from V035)
    organization_id UUID REFERENCES organizations(id) ON DELETE CASCADE,

    -- Metadata
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Size tracking for monitoring
    size_bytes INTEGER GENERATED ALWAYS AS (octet_length(value)) STORED,

    -- Optional TTL support
    expires_at TIMESTAMPTZ NULL
);

-- ============================================================================
-- TRIGGERS
-- ============================================================================

CREATE OR REPLACE FUNCTION update_actor_state_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    NEW.version = OLD.version + 1;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_actor_state_updated_at
    BEFORE UPDATE ON actor_state
    FOR EACH ROW
    EXECUTE FUNCTION update_actor_state_updated_at();

-- ============================================================================
-- INDEXES
-- ============================================================================

-- Pattern matching (actor:ActorName:key:*)
CREATE INDEX idx_actor_state_pattern ON actor_state USING btree (key text_pattern_ops);

-- TTL cleanup
CREATE INDEX idx_actor_state_expires ON actor_state(expires_at) WHERE expires_at IS NOT NULL;

-- Size monitoring
CREATE INDEX idx_actor_state_size ON actor_state(size_bytes DESC);

-- Recent updates
CREATE INDEX idx_actor_state_updated ON actor_state(updated_at DESC);

-- Organization indexes (from V035)
CREATE INDEX idx_actor_state_organization_id ON actor_state(organization_id) WHERE organization_id IS NOT NULL;
CREATE INDEX idx_actor_state_org_key ON actor_state(organization_id, key) WHERE organization_id IS NOT NULL;

-- Unique org + key constraint (from V035 Section 9)
CREATE UNIQUE INDEX idx_actor_state_org_unique ON actor_state(organization_id, key) WHERE organization_id IS NOT NULL;

-- ============================================================================
-- FUNCTIONS
-- ============================================================================

CREATE OR REPLACE FUNCTION cleanup_expired_actor_state()
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM actor_state
    WHERE expires_at IS NOT NULL
    AND expires_at < NOW();

    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- VIEWS
-- ============================================================================

CREATE OR REPLACE VIEW actor_state_stats AS
SELECT
    split_part(key, ':', 2) AS actor_name,
    split_part(key, ':', 3) AS actor_key,
    COUNT(*) AS state_count,
    SUM(size_bytes) AS total_size_bytes,
    AVG(size_bytes) AS avg_size_bytes,
    MAX(size_bytes) AS max_size_bytes,
    MAX(updated_at) AS last_updated_at
FROM actor_state
GROUP BY split_part(key, ':', 2), split_part(key, ':', 3);

CREATE OR REPLACE VIEW actor_state_by_type AS
SELECT
    split_part(key, ':', 2) AS actor_name,
    COUNT(DISTINCT split_part(key, ':', 3)) AS unique_instances,
    COUNT(*) AS total_state_keys,
    SUM(size_bytes) AS total_size_bytes,
    AVG(size_bytes) AS avg_size_bytes,
    MAX(updated_at) AS last_updated_at
FROM actor_state
GROUP BY split_part(key, ':', 2);

-- ============================================================================
-- COMMENTS
-- ============================================================================

COMMENT ON TABLE actor_state IS 'Durable state storage for actor instances (virtual objects, single-writer per key)';
COMMENT ON COLUMN actor_state.key IS 'State key in format: actor:{actor_name}:{instance_key}:{state_key}';
COMMENT ON COLUMN actor_state.value IS 'JSON-serialized state value (stored as binary for efficiency)';
COMMENT ON COLUMN actor_state.version IS 'Version number for optimistic locking (incremented on each update)';
COMMENT ON COLUMN actor_state.size_bytes IS 'Size of value in bytes (automatically calculated)';
COMMENT ON COLUMN actor_state.expires_at IS 'Optional expiration timestamp for automatic cleanup';
COMMENT ON COLUMN actor_state.organization_id IS 'Organization ID for multi-tenant isolation';

COMMENT ON INDEX idx_actor_state_pattern IS 'Index for pattern matching queries (e.g., actor:ShoppingCart:user-123:*)';
COMMENT ON INDEX idx_actor_state_expires IS 'Index for finding expired entries for cleanup';
COMMENT ON INDEX idx_actor_state_size IS 'Index for monitoring large state entries';
COMMENT ON INDEX idx_actor_state_updated IS 'Index for finding recently updated state';

COMMENT ON FUNCTION update_actor_state_updated_at() IS 'Auto-update updated_at and version on state modifications';
COMMENT ON FUNCTION cleanup_expired_actor_state() IS 'Remove expired actor state entries (call from scheduled job)';

COMMENT ON VIEW actor_state_stats IS 'Statistics per actor instance (actor_name + instance_key)';
COMMENT ON VIEW actor_state_by_type IS 'Statistics grouped by actor type (actor_name)';
