-- V025: Batch operations for bulk workflow administrative actions
--
-- Batch operations allow terminate, cancel, send event, or reset
-- on all workflows matching a filter query. Rate-limited and resumable.

CREATE TABLE batch_operations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Operation type: 'terminate', 'cancel', 'send_event', 'reset'
    operation_type VARCHAR(50) NOT NULL,

    -- Query filter for matching workflows
    namespace VARCHAR(255) NOT NULL DEFAULT 'default',
    filter_status VARCHAR(50),
    filter_workflow_type VARCHAR(255),
    filter_started_after TIMESTAMPTZ,
    filter_started_before TIMESTAMPTZ,

    -- Event-specific payload (NULL for terminate/cancel)
    event_name VARCHAR(255),
    event_payload JSONB,

    -- Reset-specific (NULL for other ops)
    reset_event_id BIGINT,
    reset_reason TEXT,

    -- Job state
    status VARCHAR(50) NOT NULL DEFAULT 'pending',

    -- Progress tracking
    total_count BIGINT NOT NULL DEFAULT 0,
    processed_count BIGINT NOT NULL DEFAULT 0,
    success_count BIGINT NOT NULL DEFAULT 0,
    failure_count BIGINT NOT NULL DEFAULT 0,

    -- Rate limiting
    rate_limit_per_second INT NOT NULL DEFAULT 50,

    -- Error details
    error TEXT,

    -- Distributed claim (FOR UPDATE SKIP LOCKED pattern)
    claimed_at TIMESTAMPTZ,
    claimed_by VARCHAR(255),

    -- Cursor for resumable iteration
    last_processed_execution_id TEXT,

    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,

    -- Audit
    created_by VARCHAR(255),

    CONSTRAINT batch_operations_type_check CHECK (
        operation_type IN ('terminate', 'cancel', 'send_event', 'reset')
    ),
    CONSTRAINT batch_operations_status_check CHECK (
        status IN ('pending', 'processing', 'completed', 'failed', 'terminated')
    ),
    CONSTRAINT batch_operations_rate_check CHECK (rate_limit_per_second > 0 AND rate_limit_per_second <= 1000)
);

-- Claim pending jobs
CREATE INDEX idx_batch_ops_pending ON batch_operations(status, created_at)
    WHERE status IN ('pending', 'processing');

-- List by status
CREATE INDEX idx_batch_ops_status ON batch_operations(status);

-- Recent operations
CREATE INDEX idx_batch_ops_created ON batch_operations(created_at DESC);
