-- ORCHER Task Orchestrator - Coordination Queues
-- Migration V014: Outbox queue, timer queue, and dead letter queue with lease support, multi-tenancy, and sharding

-- ============================================================================
-- OUTBOX QUEUE: Atomic task dispatch pattern
-- ============================================================================

CREATE TABLE outbox_queue (
    task_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Workflow association
    workflow_id UUID NOT NULL,

    -- Task type determines how the task is processed
    task_type VARCHAR(50) NOT NULL,

    -- Task payload
    payload JSONB NOT NULL,

    -- Idempotency support
    idempotency_key TEXT UNIQUE,

    -- Status tracking
    status VARCHAR(20) NOT NULL DEFAULT 'pending',

    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processing_started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,

    -- Retry logic
    attempt_count INT NOT NULL DEFAULT 0,
    max_retries INT NOT NULL DEFAULT 3,
    next_retry_at TIMESTAMPTZ,
    backoff_multiplier DOUBLE PRECISION NOT NULL DEFAULT 2.0,

    -- Error tracking
    last_error TEXT,
    error_details JSONB DEFAULT '{}'::JSONB,

    -- Lease-based claiming (from V029)
    lease_expires_at TIMESTAMPTZ,
    lease_owner TEXT,

    -- Multi-tenancy (from V035)
    organization_id UUID REFERENCES organizations(id) ON DELETE CASCADE,

    -- Horizontal sharding (from V036)
    shard_id INTEGER DEFAULT NULL,

    CONSTRAINT outbox_queue_status_check CHECK (
        status IN ('pending', 'processing', 'completed', 'failed')
    ),
    CONSTRAINT outbox_queue_task_type_check CHECK (
        task_type IN (
            'dispatch_step',
            'dispatch_task',
            'send_event_workflow',
            'update_visibility',
            'fire_event_gate'
        )
    ),
    CONSTRAINT outbox_queue_attempt_check CHECK (
        attempt_count >= 0 AND attempt_count <= max_retries + 1
    ),
    CONSTRAINT outbox_queue_backoff_check CHECK (
        backoff_multiplier >= 1.0 AND backoff_multiplier <= 10.0
    )
);

-- ============================================================================
-- TIMER QUEUE: Durable scheduling
-- ============================================================================

CREATE TABLE timer_queue (
    task_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Workflow association
    workflow_id UUID NOT NULL,

    -- Task type determines how the timer is processed
    task_type VARCHAR(50) NOT NULL,

    -- Task payload
    payload JSONB NOT NULL,

    -- Timer scheduling
    fire_at TIMESTAMPTZ NOT NULL,

    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Status tracking
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    processing_started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,

    -- Retry tracking
    attempt_count INT NOT NULL DEFAULT 0,
    max_retries INT NOT NULL DEFAULT 3,
    last_error TEXT,

    -- Multi-tenancy (from V035)
    organization_id UUID REFERENCES organizations(id) ON DELETE CASCADE,

    CONSTRAINT timer_queue_status_check CHECK (
        status IN ('pending', 'processing', 'completed', 'failed', 'cancelled')
    ),
    CONSTRAINT timer_queue_task_type_check CHECK (
        task_type IN (
            'fire_timer',
            'timeout_step',
            'timeout_task',
            'retry_step',
            'schedule_to_start_timeout'
        )
    ),
    CONSTRAINT timer_queue_attempt_check CHECK (
        attempt_count >= 0 AND attempt_count <= max_retries + 1
    ),
    CONSTRAINT timer_queue_fire_at_future CHECK (
        fire_at >= created_at
    )
);

-- ============================================================================
-- DEAD LETTER QUEUE: Failed task tracking
-- ============================================================================

CREATE TABLE dead_letter_queue (
    dlq_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Original task information
    original_task_id UUID NOT NULL,
    original_task_type VARCHAR(20) NOT NULL,
    workflow_id UUID NOT NULL,

    -- Task data
    payload JSONB NOT NULL,

    -- Failure information
    attempt_count INT NOT NULL,
    last_error TEXT NOT NULL,
    error_details JSONB DEFAULT '{}'::JSONB,

    -- Timestamps
    original_created_at TIMESTAMPTZ NOT NULL,
    failed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Processing history (for debugging)
    processing_history JSONB,

    -- Multi-tenancy (from V035)
    organization_id UUID REFERENCES organizations(id) ON DELETE CASCADE,

    CONSTRAINT dlq_task_type_check CHECK (
        original_task_type IN ('outbox', 'timer')
    ),
    CONSTRAINT dlq_attempt_count_check CHECK (
        attempt_count > 0
    )
);

-- ============================================================================
-- INDEXES
-- ============================================================================

-- Outbox queue indexes
CREATE INDEX idx_outbox_pending_workflow ON outbox_queue(workflow_id, created_at) WHERE status = 'pending';
CREATE INDEX idx_outbox_claim_tasks ON outbox_queue(status, next_retry_at, created_at) WHERE status = 'pending';
CREATE INDEX idx_outbox_retry_ready ON outbox_queue(next_retry_at) WHERE status = 'pending' AND next_retry_at IS NOT NULL;
CREATE INDEX idx_outbox_stuck_processing ON outbox_queue(processing_started_at) WHERE status = 'processing';
CREATE INDEX idx_outbox_completed ON outbox_queue(completed_at DESC) WHERE status = 'completed';
CREATE INDEX idx_outbox_failed ON outbox_queue(completed_at DESC) WHERE status = 'failed';
CREATE INDEX idx_outbox_task_type ON outbox_queue(task_type, created_at DESC);
CREATE INDEX idx_outbox_payload_gin ON outbox_queue USING GIN(payload);

-- Lease-based claiming indexes (from V029)
CREATE INDEX idx_outbox_lease_expired ON outbox_queue(lease_expires_at)
    WHERE status = 'processing' AND lease_expires_at IS NOT NULL;
CREATE INDEX idx_outbox_claimable_tasks ON outbox_queue(status, lease_expires_at, created_at)
    WHERE status IN ('pending', 'processing');

-- Organization indexes for outbox_queue (from V035)
CREATE INDEX idx_outbox_queue_organization_id ON outbox_queue(organization_id) WHERE organization_id IS NOT NULL;
CREATE INDEX idx_outbox_queue_org_type_status ON outbox_queue(organization_id, task_type, status) WHERE organization_id IS NOT NULL;

-- Shard index for outbox_queue (from V036)
CREATE INDEX idx_outbox_shard_id ON outbox_queue(shard_id) WHERE shard_id IS NOT NULL;

-- Performance indexes for outbox
CREATE INDEX idx_outbox_queue_task_polling ON outbox_queue(task_type, created_at) WHERE status = 'pending';
CREATE INDEX idx_outbox_queue_task_queue_match ON outbox_queue((payload->>'task_queue'), (payload->>'namespace'), created_at)
    WHERE status = 'pending' AND task_type = 'dispatch_task';
CREATE INDEX idx_outbox_queue_lease_expiry ON outbox_queue(lease_expires_at)
    WHERE status = 'processing' AND lease_expires_at IS NOT NULL;
CREATE INDEX idx_outbox_queue_workflow_ordering ON outbox_queue(workflow_id, created_at) WHERE status = 'pending';

-- Timer queue indexes
CREATE INDEX idx_timer_fire_at ON timer_queue(fire_at) WHERE status = 'pending';
CREATE INDEX idx_timer_workflow ON timer_queue(workflow_id, fire_at) WHERE status IN ('pending', 'processing');
CREATE INDEX idx_timer_stuck_processing ON timer_queue(processing_started_at) WHERE status = 'processing';
CREATE INDEX idx_timer_completed ON timer_queue(completed_at DESC) WHERE status = 'completed';
CREATE INDEX idx_timer_failed ON timer_queue(completed_at DESC) WHERE status = 'failed';
CREATE INDEX idx_timer_task_type ON timer_queue(task_type, created_at DESC);
CREATE INDEX idx_timer_payload_gin ON timer_queue USING GIN(payload);

-- Organization indexes for timer_queue (from V035)
CREATE INDEX idx_timer_queue_organization_id ON timer_queue(organization_id) WHERE organization_id IS NOT NULL;
CREATE INDEX idx_timer_queue_org_fire_time ON timer_queue(organization_id, fire_at, status) WHERE organization_id IS NOT NULL;
CREATE INDEX idx_timer_queue_org_workflow ON timer_queue(organization_id, workflow_id) WHERE organization_id IS NOT NULL;

-- Performance index for timer
CREATE INDEX idx_timer_queue_due ON timer_queue(fire_at, status) WHERE status = 'pending';

-- Dead letter queue indexes
CREATE INDEX idx_dlq_workflow ON dead_letter_queue(workflow_id, failed_at DESC);
CREATE INDEX idx_dlq_failed_at ON dead_letter_queue(failed_at DESC);
CREATE INDEX idx_dlq_task_type ON dead_letter_queue(original_task_type, failed_at DESC);
CREATE INDEX idx_dlq_original_task ON dead_letter_queue(original_task_id);
CREATE INDEX idx_dlq_payload_gin ON dead_letter_queue USING GIN(payload);
CREATE INDEX idx_dlq_error_details_gin ON dead_letter_queue USING GIN(error_details);

-- Organization indexes for dead_letter_queue (from V035)
CREATE INDEX idx_dead_letter_queue_organization_id ON dead_letter_queue(organization_id) WHERE organization_id IS NOT NULL;
CREATE INDEX idx_dead_letter_queue_org_task_type ON dead_letter_queue(organization_id, original_task_type) WHERE organization_id IS NOT NULL;

-- ============================================================================
-- FUNCTIONS
-- ============================================================================

CREATE OR REPLACE FUNCTION move_outbox_to_dlq(
    p_task_id UUID
) RETURNS UUID AS $$
DECLARE
    v_dlq_id UUID;
BEGIN
    INSERT INTO dead_letter_queue (
        original_task_id,
        original_task_type,
        workflow_id,
        payload,
        attempt_count,
        last_error,
        error_details,
        original_created_at,
        processing_history
    )
    SELECT
        task_id,
        'outbox',
        workflow_id,
        payload,
        attempt_count,
        last_error,
        error_details,
        created_at,
        jsonb_build_object(
            'attempts', attempt_count,
            'last_processing_started_at', processing_started_at,
            'task_type', task_type
        )
    FROM outbox_queue
    WHERE task_id = p_task_id
    RETURNING dlq_id INTO v_dlq_id;

    UPDATE outbox_queue
    SET status = 'failed',
        completed_at = NOW()
    WHERE task_id = p_task_id;

    RETURN v_dlq_id;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION move_outbox_to_dlq IS 'Move an outbox task to the dead letter queue after max retries';

CREATE OR REPLACE FUNCTION move_timer_to_dlq(
    p_task_id UUID
) RETURNS UUID AS $$
DECLARE
    v_dlq_id UUID;
BEGIN
    INSERT INTO dead_letter_queue (
        original_task_id,
        original_task_type,
        workflow_id,
        payload,
        attempt_count,
        last_error,
        error_details,
        original_created_at,
        processing_history
    )
    SELECT
        task_id,
        'timer',
        workflow_id,
        payload,
        attempt_count,
        last_error,
        NULL,
        created_at,
        jsonb_build_object(
            'attempts', attempt_count,
            'fire_at', fire_at,
            'task_type', task_type
        )
    FROM timer_queue
    WHERE task_id = p_task_id
    RETURNING dlq_id INTO v_dlq_id;

    UPDATE timer_queue
    SET status = 'failed',
        completed_at = NOW()
    WHERE task_id = p_task_id;

    RETURN v_dlq_id;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION move_timer_to_dlq IS 'Move a timer task to the dead letter queue after max retries';

CREATE OR REPLACE FUNCTION get_queue_metrics()
RETURNS TABLE (
    queue_name TEXT,
    pending_count BIGINT,
    processing_count BIGINT,
    completed_count BIGINT,
    failed_count BIGINT,
    dlq_count BIGINT,
    oldest_pending_age_seconds BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        'outbox'::TEXT,
        COUNT(*) FILTER (WHERE status = 'pending')::BIGINT,
        COUNT(*) FILTER (WHERE status = 'processing')::BIGINT,
        COUNT(*) FILTER (WHERE status = 'completed')::BIGINT,
        COUNT(*) FILTER (WHERE status = 'failed')::BIGINT,
        (SELECT COUNT(*) FROM dead_letter_queue WHERE original_task_type = 'outbox')::BIGINT,
        COALESCE(EXTRACT(EPOCH FROM (NOW() - MIN(created_at) FILTER (WHERE status = 'pending')))::BIGINT, 0)
    FROM outbox_queue;

    RETURN QUERY
    SELECT
        'timer'::TEXT,
        COUNT(*) FILTER (WHERE status = 'pending')::BIGINT,
        COUNT(*) FILTER (WHERE status = 'processing')::BIGINT,
        COUNT(*) FILTER (WHERE status = 'completed')::BIGINT,
        COUNT(*) FILTER (WHERE status = 'failed')::BIGINT,
        (SELECT COUNT(*) FROM dead_letter_queue WHERE original_task_type = 'timer')::BIGINT,
        COALESCE(EXTRACT(EPOCH FROM (NOW() - MIN(created_at) FILTER (WHERE status = 'pending')))::BIGINT, 0)
    FROM timer_queue;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION get_queue_metrics IS 'Get comprehensive metrics for outbox and timer queues';

CREATE OR REPLACE FUNCTION cleanup_completed_queue_tasks(
    p_retention_days INT DEFAULT 7
) RETURNS TABLE (
    outbox_deleted BIGINT,
    timer_deleted BIGINT
) AS $$
DECLARE
    v_outbox_deleted BIGINT;
    v_timer_deleted BIGINT;
BEGIN
    WITH deleted AS (
        DELETE FROM outbox_queue
        WHERE status = 'completed'
        AND completed_at < NOW() - (p_retention_days || ' days')::INTERVAL
        RETURNING 1
    )
    SELECT COUNT(*) INTO v_outbox_deleted FROM deleted;

    WITH deleted AS (
        DELETE FROM timer_queue
        WHERE status = 'completed'
        AND completed_at < NOW() - (p_retention_days || ' days')::INTERVAL
        RETURNING 1
    )
    SELECT COUNT(*) INTO v_timer_deleted FROM deleted;

    RETURN QUERY SELECT v_outbox_deleted, v_timer_deleted;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION cleanup_completed_queue_tasks IS 'Cleanup completed tasks older than retention period (default 7 days)';

-- ============================================================================
-- VIEWS
-- ============================================================================

CREATE VIEW v_outbox_active_tasks AS
SELECT
    task_id,
    workflow_id,
    task_type,
    status,
    created_at,
    processing_started_at,
    attempt_count,
    max_retries,
    next_retry_at,
    EXTRACT(EPOCH FROM (NOW() - created_at))::INT AS age_seconds,
    CASE
        WHEN status = 'processing' THEN
            EXTRACT(EPOCH FROM (NOW() - processing_started_at))::INT
        ELSE NULL
    END AS processing_duration_seconds
FROM outbox_queue
WHERE status IN ('pending', 'processing')
ORDER BY created_at;

COMMENT ON VIEW v_outbox_active_tasks IS 'Currently active outbox tasks with age and processing duration';

CREATE VIEW v_timer_active_tasks AS
SELECT
    task_id,
    workflow_id,
    task_type,
    status,
    fire_at,
    created_at,
    processing_started_at,
    attempt_count,
    max_retries,
    EXTRACT(EPOCH FROM (fire_at - NOW()))::INT AS seconds_until_fire,
    CASE
        WHEN status = 'processing' THEN
            EXTRACT(EPOCH FROM (NOW() - processing_started_at))::INT
        ELSE NULL
    END AS processing_duration_seconds
FROM timer_queue
WHERE status IN ('pending', 'processing')
ORDER BY fire_at;

COMMENT ON VIEW v_timer_active_tasks IS 'Currently active timer tasks with time until fire';

CREATE VIEW v_dlq_summary AS
SELECT
    workflow_id,
    original_task_type,
    COUNT(*) AS failure_count,
    MIN(failed_at) AS first_failure,
    MAX(failed_at) AS last_failure,
    AVG(attempt_count)::NUMERIC(10,2) AS avg_attempts
FROM dead_letter_queue
GROUP BY workflow_id, original_task_type
ORDER BY failure_count DESC;

COMMENT ON VIEW v_dlq_summary IS 'Dead letter queue summary grouped by workflow and task type';

-- ============================================================================
-- COMMENTS
-- ============================================================================

COMMENT ON TABLE outbox_queue IS 'Outbox pattern for atomic task dispatch with guaranteed delivery';
COMMENT ON COLUMN outbox_queue.task_type IS 'Type of task: dispatch_step, dispatch_task, send_event_workflow, update_visibility, fire_event_gate';
COMMENT ON COLUMN outbox_queue.idempotency_key IS 'Optional idempotency key to prevent duplicate task creation';
COMMENT ON COLUMN outbox_queue.status IS 'Task status: pending, processing, completed, failed';
COMMENT ON COLUMN outbox_queue.next_retry_at IS 'When to retry this task (NULL if not retrying)';
COMMENT ON COLUMN outbox_queue.backoff_multiplier IS 'Exponential backoff multiplier (default 2.0)';
COMMENT ON COLUMN outbox_queue.lease_expires_at IS 'When the current lease expires. Workers must renew before expiration.';
COMMENT ON COLUMN outbox_queue.lease_owner IS 'Identity of the worker holding the current lease.';
COMMENT ON COLUMN outbox_queue.organization_id IS 'Organization ID for multi-tenant isolation - CRITICAL for task routing';
COMMENT ON COLUMN outbox_queue.shard_id IS 'Shard identifier for horizontal partitioning';

COMMENT ON TABLE timer_queue IS 'Durable timer queue for scheduled operations';
COMMENT ON COLUMN timer_queue.task_type IS 'Type of timer: fire_timer, timeout_step, timeout_task, retry_step, schedule_to_start_timeout';
COMMENT ON COLUMN timer_queue.fire_at IS 'When to fire this timer (scheduled time)';
COMMENT ON COLUMN timer_queue.status IS 'Timer status: pending, processing, completed, failed, cancelled';
COMMENT ON COLUMN timer_queue.organization_id IS 'Organization ID for multi-tenant isolation - CRITICAL for timer scheduling';

COMMENT ON TABLE dead_letter_queue IS 'Dead letter queue for tasks that failed after max retries';
COMMENT ON COLUMN dead_letter_queue.original_task_type IS 'Type of original task: outbox or timer';
COMMENT ON COLUMN dead_letter_queue.processing_history IS 'JSON array of processing attempts with timestamps and errors';
COMMENT ON COLUMN dead_letter_queue.organization_id IS 'Organization ID for multi-tenant isolation';
