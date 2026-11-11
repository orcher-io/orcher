-- ORCHER Task Orchestrator - Event Store
-- Migration V011: Stream-based event sourcing infrastructure with multi-tenancy and shard support

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "btree_gist";

-- ============================================================================
-- EVENTS TABLE: Stream-based append-only event log
-- ============================================================================
--
-- IMPORTANT EVENT ROUTING RULES:
-- - Task lifecycle events → events table (stream: "task:task_id")
-- - Actor events → events table (stream: "actor:actor_id")
-- - Worker events → events table (stream: "worker:worker_id")
-- - Workflow orchestration events → workflow_execution_journal (V015) ONLY
--
-- DO NOT use this table for workflow events.

CREATE TABLE events (
    -- Primary identification
    event_id BIGSERIAL PRIMARY KEY,

    -- Stream identification (category:id format)
    stream_id TEXT NOT NULL,
    stream_category TEXT NOT NULL,

    -- Event details
    event_type VARCHAR(100) NOT NULL,

    -- Event data
    payload JSONB NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}',

    -- Timestamps
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Position tracking
    stream_position BIGINT NOT NULL,
    global_position BIGINT NOT NULL,

    -- Causation and correlation for event chains and distributed tracing
    causation_id UUID,
    correlation_id UUID,

    -- Multi-tenancy
    organization_id UUID REFERENCES organizations(id) ON DELETE CASCADE,

    -- Optimistic concurrency
    CONSTRAINT unique_stream_position UNIQUE (stream_id, stream_position),

    CONSTRAINT positive_stream_position CHECK (stream_position > 0),
    CONSTRAINT positive_global_position CHECK (global_position > 0)
);

-- ============================================================================
-- EVENT SNAPSHOTS TABLE: Fast aggregate reconstruction
-- ============================================================================

CREATE TABLE event_snapshots (
    snapshot_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    stream_id TEXT NOT NULL,
    stream_category TEXT NOT NULL,
    stream_position BIGINT NOT NULL,
    state JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Multi-tenancy
    organization_id UUID REFERENCES organizations(id) ON DELETE CASCADE,

    CONSTRAINT unique_snapshot_position UNIQUE (stream_id, stream_position),
    CONSTRAINT positive_snapshot_position CHECK (stream_position > 0)
);

-- ============================================================================
-- INDEXES
-- ============================================================================

-- Events indexes
CREATE INDEX idx_events_stream_id ON events(stream_id, stream_position);
CREATE INDEX idx_events_stream_category ON events(stream_category, global_position);
CREATE INDEX idx_events_global_position ON events(global_position);
CREATE INDEX idx_events_event_type ON events(event_type, timestamp DESC);
CREATE INDEX idx_events_timestamp ON events(timestamp DESC);
CREATE INDEX idx_events_correlation ON events(correlation_id) WHERE correlation_id IS NOT NULL;
CREATE INDEX idx_events_causation ON events(causation_id) WHERE causation_id IS NOT NULL;
CREATE INDEX idx_events_payload ON events USING GIN(payload);
CREATE INDEX idx_events_metadata ON events USING GIN(metadata);
CREATE INDEX idx_events_category_position ON events(stream_category, stream_position);

-- Organization indexes (from V035)
CREATE INDEX idx_events_organization_id ON events(organization_id) WHERE organization_id IS NOT NULL;
CREATE INDEX idx_events_org_stream ON events(organization_id, stream_id) WHERE organization_id IS NOT NULL;
CREATE INDEX idx_events_org_stream_position ON events(organization_id, stream_id, stream_position) WHERE organization_id IS NOT NULL;

-- Event snapshots indexes
CREATE INDEX idx_snapshots_stream ON event_snapshots(stream_id, stream_position DESC);
CREATE INDEX idx_snapshots_category ON event_snapshots(stream_category, created_at DESC);

-- Organization indexes for event_snapshots
CREATE INDEX idx_event_snapshots_organization_id ON event_snapshots(organization_id) WHERE organization_id IS NOT NULL;
CREATE INDEX idx_event_snapshots_org_stream ON event_snapshots(organization_id, stream_id) WHERE organization_id IS NOT NULL;

-- ============================================================================
-- FUNCTIONS
-- ============================================================================

CREATE OR REPLACE FUNCTION get_next_stream_position(p_stream_id TEXT)
RETURNS BIGINT AS $$
DECLARE
    v_next_position BIGINT;
BEGIN
    SELECT COALESCE(MAX(stream_position), 0) + 1
    INTO v_next_position
    FROM events
    WHERE stream_id = p_stream_id;

    RETURN v_next_position;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION get_next_stream_position IS 'Get the next position for appending to a stream';

CREATE OR REPLACE FUNCTION get_current_global_position()
RETURNS BIGINT AS $$
DECLARE
    v_position BIGINT;
BEGIN
    SELECT COALESCE(MAX(global_position), 0)
    INTO v_position
    FROM events;

    RETURN v_position;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION get_current_global_position IS 'Get the current highest global position';

CREATE OR REPLACE FUNCTION extract_aggregate_id(p_stream_id TEXT)
RETURNS UUID AS $$
DECLARE
    v_parts TEXT[];
    v_uuid UUID;
BEGIN
    v_parts := string_to_array(p_stream_id, ':');

    IF array_length(v_parts, 1) >= 2 THEN
        BEGIN
            v_uuid := v_parts[2]::UUID;
            RETURN v_uuid;
        EXCEPTION
            WHEN OTHERS THEN
                RETURN NULL;
        END;
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

COMMENT ON FUNCTION extract_aggregate_id IS 'Extract UUID from stream_id (returns NULL if not a UUID)';

CREATE OR REPLACE FUNCTION extract_category(p_stream_id TEXT)
RETURNS TEXT AS $$
DECLARE
    v_parts TEXT[];
BEGIN
    v_parts := string_to_array(p_stream_id, ':');

    IF array_length(v_parts, 1) >= 1 THEN
        RETURN v_parts[1];
    END IF;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

COMMENT ON FUNCTION extract_category IS 'Extract category from stream_id';

-- ============================================================================
-- COMMENTS
-- ============================================================================

COMMENT ON TABLE events IS 'Stream-based append-only event log for platform-wide events (tasks, actors, workers). Workflow events go to workflow_execution_journal.';
COMMENT ON COLUMN events.stream_id IS 'Stream identifier in "category:id" format (e.g., "task:uuid", "actor:actor_id"). Do NOT use for workflows.';
COMMENT ON COLUMN events.stream_category IS 'Stream category: task, actor, worker. Do NOT use "workflow" - use workflow_execution_journal.';
COMMENT ON COLUMN events.event_type IS 'Specific event type: TaskSubmitted, TaskCompleted, etc.';
COMMENT ON COLUMN events.stream_position IS 'Position within the stream (per-stream sequence starting at 1)';
COMMENT ON COLUMN events.global_position IS 'Global position across all streams (total event ordering)';
COMMENT ON COLUMN events.causation_id IS 'Event that directly caused this event (for event chains)';
COMMENT ON COLUMN events.correlation_id IS 'For distributed tracing across services';
COMMENT ON COLUMN events.organization_id IS 'Organization ID for multi-tenant isolation';

COMMENT ON TABLE event_snapshots IS 'Snapshots for fast aggregate reconstruction. Primarily used for actor state snapshots.';
COMMENT ON COLUMN event_snapshots.stream_position IS 'Snapshot taken at this stream position';
COMMENT ON COLUMN event_snapshots.state IS 'Full serialized aggregate state at snapshot time';
COMMENT ON COLUMN event_snapshots.organization_id IS 'Organization ID for multi-tenant isolation';
