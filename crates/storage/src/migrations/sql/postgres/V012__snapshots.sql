-- ORCHER Task Orchestrator - Snapshot Store
-- Migration V012: Event sourcing snapshots for performance optimization with multi-tenancy

-- ============================================================================
-- SNAPSHOTS TABLE: Aggregate and projection state snapshots
-- ============================================================================

CREATE TABLE snapshots (
    -- Primary identification
    snapshot_id TEXT PRIMARY KEY,

    -- Stream/aggregate identification
    stream_id TEXT NOT NULL,
    aggregate_id UUID,
    aggregate_type VARCHAR(100),

    -- Position tracking
    stream_position BIGINT NOT NULL,
    global_position BIGINT NOT NULL DEFAULT 0,

    -- Snapshot data
    data BYTEA NOT NULL,
    data_format VARCHAR(50) NOT NULL DEFAULT 'json',

    -- Metadata
    metadata JSONB NOT NULL DEFAULT '{}',
    version INTEGER NOT NULL DEFAULT 1,

    -- Size tracking
    size_bytes BIGINT,
    compressed_size_bytes BIGINT,

    -- Multi-tenancy
    organization_id UUID REFERENCES organizations(id) ON DELETE CASCADE,

    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT positive_stream_position CHECK (stream_position >= 0),
    CONSTRAINT positive_global_position CHECK (global_position >= 0),
    CONSTRAINT valid_version CHECK (version > 0)
);

-- ============================================================================
-- PROJECTION SNAPSHOTS TABLE: CQRS read model restoration
-- ============================================================================

CREATE TABLE projection_snapshots (
    -- Primary identification
    snapshot_id TEXT PRIMARY KEY,

    -- Projection identification
    projection_name TEXT NOT NULL,

    -- Position tracking
    last_processed_position BIGINT NOT NULL,
    global_position BIGINT NOT NULL DEFAULT 0,

    -- Snapshot data
    state JSONB NOT NULL,

    -- Metadata
    version INTEGER NOT NULL DEFAULT 1,
    snapshot_version INTEGER NOT NULL DEFAULT 1,

    -- Size tracking
    state_size_bytes BIGINT,

    -- Multi-tenancy
    organization_id UUID REFERENCES organizations(id) ON DELETE CASCADE,

    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT positive_processed_position CHECK (last_processed_position >= 0),
    CONSTRAINT positive_version CHECK (version > 0)
);

-- ============================================================================
-- SNAPSHOT METADATA TABLE: Snapshot statistics per stream
-- ============================================================================

CREATE TABLE snapshot_metadata (
    stream_id TEXT PRIMARY KEY,

    -- Statistics
    total_snapshots INTEGER NOT NULL DEFAULT 0,
    latest_snapshot_id TEXT,
    latest_snapshot_position BIGINT,
    latest_snapshot_created_at TIMESTAMPTZ,

    -- Storage stats
    total_size_bytes BIGINT NOT NULL DEFAULT 0,
    average_size_bytes BIGINT,

    -- Performance tracking
    last_cleanup_at TIMESTAMPTZ,
    next_snapshot_due_at TIMESTAMPTZ,

    -- Multi-tenancy
    organization_id UUID REFERENCES organizations(id) ON DELETE CASCADE,

    -- Metadata
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT fk_latest_snapshot
        FOREIGN KEY (latest_snapshot_id)
        REFERENCES snapshots(snapshot_id)
        ON DELETE SET NULL
);

-- ============================================================================
-- TRIGGERS
-- ============================================================================

CREATE OR REPLACE FUNCTION update_snapshot_metadata()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO snapshot_metadata (
        stream_id,
        total_snapshots,
        latest_snapshot_id,
        latest_snapshot_position,
        latest_snapshot_created_at,
        total_size_bytes,
        updated_at
    )
    VALUES (
        NEW.stream_id,
        1,
        NEW.snapshot_id,
        NEW.stream_position,
        NEW.created_at,
        COALESCE(NEW.compressed_size_bytes, NEW.size_bytes, 0),
        NOW()
    )
    ON CONFLICT (stream_id) DO UPDATE SET
        total_snapshots = snapshot_metadata.total_snapshots + 1,
        latest_snapshot_id = CASE
            WHEN NEW.stream_position > snapshot_metadata.latest_snapshot_position
            THEN NEW.snapshot_id
            ELSE snapshot_metadata.latest_snapshot_id
        END,
        latest_snapshot_position = GREATEST(NEW.stream_position, snapshot_metadata.latest_snapshot_position),
        latest_snapshot_created_at = CASE
            WHEN NEW.stream_position > snapshot_metadata.latest_snapshot_position
            THEN NEW.created_at
            ELSE snapshot_metadata.latest_snapshot_created_at
        END,
        total_size_bytes = snapshot_metadata.total_size_bytes + COALESCE(NEW.compressed_size_bytes, NEW.size_bytes, 0),
        updated_at = NOW();

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_snapshot_metadata
AFTER INSERT ON snapshots
FOR EACH ROW
EXECUTE FUNCTION update_snapshot_metadata();

-- ============================================================================
-- INDEXES
-- ============================================================================

-- Snapshots indexes
CREATE INDEX idx_snapshots_stream_position ON snapshots(stream_id, stream_position DESC);
CREATE INDEX idx_snapshots_aggregate ON snapshots(aggregate_id, stream_position DESC) WHERE aggregate_id IS NOT NULL;
CREATE INDEX idx_snapshots_aggregate_type ON snapshots(aggregate_type, stream_position DESC) WHERE aggregate_type IS NOT NULL;
CREATE INDEX idx_snapshots_created_at ON snapshots(created_at DESC);
CREATE INDEX idx_snapshots_global_position ON snapshots(global_position DESC);
CREATE INDEX idx_snapshots_metadata ON snapshots USING GIN(metadata);

-- Organization indexes for snapshots (from V035)
CREATE INDEX idx_snapshots_organization_id ON snapshots(organization_id) WHERE organization_id IS NOT NULL;
CREATE INDEX idx_snapshots_org_aggregate ON snapshots(organization_id, aggregate_type, aggregate_id) WHERE organization_id IS NOT NULL;

-- Projection snapshots indexes
CREATE INDEX idx_projection_snapshots_name ON projection_snapshots(projection_name, created_at DESC);
CREATE INDEX idx_projection_snapshots_position ON projection_snapshots(projection_name, last_processed_position DESC);
CREATE INDEX idx_projection_snapshots_created_at ON projection_snapshots(created_at DESC);

-- Organization indexes for projection_snapshots (from V035)
CREATE INDEX idx_projection_snapshots_organization_id ON projection_snapshots(organization_id) WHERE organization_id IS NOT NULL;
CREATE INDEX idx_projection_snapshots_org_projection ON projection_snapshots(organization_id, projection_name) WHERE organization_id IS NOT NULL;

-- Snapshot metadata indexes
CREATE INDEX idx_snapshot_metadata_cleanup ON snapshot_metadata(next_snapshot_due_at) WHERE next_snapshot_due_at IS NOT NULL;

-- Organization indexes for snapshot_metadata (from V035)
CREATE INDEX idx_snapshot_metadata_organization_id ON snapshot_metadata(organization_id) WHERE organization_id IS NOT NULL;

-- ============================================================================
-- FUNCTIONS
-- ============================================================================

CREATE OR REPLACE FUNCTION cleanup_old_snapshots(
    p_stream_id TEXT,
    p_keep_count INTEGER DEFAULT 10
)
RETURNS INTEGER AS $$
DECLARE
    v_deleted_count INTEGER;
BEGIN
    WITH ranked_snapshots AS (
        SELECT snapshot_id,
               ROW_NUMBER() OVER (PARTITION BY stream_id ORDER BY stream_position DESC) as rn
        FROM snapshots
        WHERE stream_id = p_stream_id
    )
    DELETE FROM snapshots
    WHERE snapshot_id IN (
        SELECT snapshot_id
        FROM ranked_snapshots
        WHERE rn > p_keep_count
    );

    GET DIAGNOSTICS v_deleted_count = ROW_COUNT;

    RETURN v_deleted_count;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION cleanup_snapshots_older_than(
    p_max_age INTERVAL DEFAULT '30 days'
)
RETURNS INTEGER AS $$
DECLARE
    v_deleted_count INTEGER;
BEGIN
    DELETE FROM snapshots
    WHERE created_at < (NOW() - p_max_age);

    GET DIAGNOSTICS v_deleted_count = ROW_COUNT;

    RETURN v_deleted_count;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- COMMENTS
-- ============================================================================

COMMENT ON TABLE snapshots IS 'Point-in-time snapshots of aggregate/projection state for fast restoration';
COMMENT ON COLUMN snapshots.snapshot_id IS 'Unique identifier for the snapshot (typically stream_id-position)';
COMMENT ON COLUMN snapshots.stream_id IS 'Stream identifier (category:id format)';
COMMENT ON COLUMN snapshots.stream_position IS 'Position in the stream when snapshot was taken';
COMMENT ON COLUMN snapshots.global_position IS 'Global event store position when snapshot was taken';
COMMENT ON COLUMN snapshots.data IS 'Compressed binary snapshot data';
COMMENT ON COLUMN snapshots.data_format IS 'Format of the snapshot data (json, bincode, msgpack, etc.)';
COMMENT ON COLUMN snapshots.version IS 'Snapshot schema version for handling upgrades';
COMMENT ON COLUMN snapshots.organization_id IS 'Organization ID for multi-tenant isolation';

COMMENT ON TABLE projection_snapshots IS 'Projection state snapshots for fast CQRS read model restoration';
COMMENT ON COLUMN projection_snapshots.projection_name IS 'Name of the projection';
COMMENT ON COLUMN projection_snapshots.last_processed_position IS 'Last event position processed by the projection';
COMMENT ON COLUMN projection_snapshots.state IS 'Complete projection state as JSON';
COMMENT ON COLUMN projection_snapshots.organization_id IS 'Organization ID for multi-tenant isolation';

COMMENT ON TABLE snapshot_metadata IS 'Snapshot statistics and cleanup schedules per stream';
COMMENT ON FUNCTION cleanup_old_snapshots IS 'Remove old snapshots keeping only the N most recent per stream';
COMMENT ON FUNCTION cleanup_snapshots_older_than IS 'Remove snapshots older than specified duration';
