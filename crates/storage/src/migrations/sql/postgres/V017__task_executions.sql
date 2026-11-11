-- ORCHER Task Orchestrator - Task Executions
-- Migration V017: Task execution lifecycle tracking for the service-pull model with multi-tenancy and sharding

-- ============================================================================
-- TASK EXECUTIONS TABLE
-- ============================================================================
--
-- Tracks the lifecycle of task executions within workflows.
-- Each row represents one execution attempt of a task.
--
-- Orcher Service-Pull Model:
-- - Services poll for tasks via PollTaskExecution
-- - Services self-identify (no server-side assignment)
-- - Task executions are scoped to workflow executions
-- - Multiple attempts tracked separately (retries)
-- - Task lifecycle: scheduled -> started -> completed/failed

CREATE TABLE task_executions (
    -- Primary key
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Task identification
    task_id VARCHAR(255) NOT NULL,
    task_type VARCHAR(255) NOT NULL,

    -- Workflow context
    workflow_id UUID NOT NULL,
    execution_id UUID NOT NULL,

    -- Task queue
    task_queue VARCHAR(255) NOT NULL,
    namespace VARCHAR(255) NOT NULL DEFAULT 'default',

    -- Service information
    service_identity VARCHAR(255),

    -- Execution lifecycle
    attempt_number INTEGER NOT NULL DEFAULT 1,
    status VARCHAR(50) NOT NULL,

    -- Timing information
    scheduled_at TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,

    -- Task token
    task_token TEXT,

    -- Timeouts
    schedule_to_start_timeout_secs INTEGER,
    start_to_close_timeout_secs INTEGER,
    heartbeat_timeout_secs INTEGER,

    -- Task input
    input BYTEA,

    -- Task result or error
    result BYTEA,
    error_message TEXT,
    error_type VARCHAR(255),
    error_details JSONB,

    -- Retry policy
    retry_policy JSONB,

    -- Headers/metadata
    headers JSONB DEFAULT '{}',
    metadata JSONB DEFAULT '{}',

    -- Heartbeat tracking
    last_heartbeat_at TIMESTAMPTZ,
    heartbeat_details BYTEA,

    -- Multi-tenancy (from V035)
    organization_id UUID REFERENCES organizations(id) ON DELETE CASCADE,

    -- Horizontal sharding (from V036)
    shard_id INTEGER DEFAULT NULL,

    -- Audit timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT task_executions_status_check CHECK (
        status IN ('scheduled', 'started', 'completed', 'failed', 'timed_out', 'cancelled')
    ),
    CONSTRAINT task_executions_attempt_check CHECK (attempt_number > 0),
    CONSTRAINT task_executions_completed_requires_time CHECK (
        (status NOT IN ('completed', 'failed', 'timed_out', 'cancelled')) OR (completed_at IS NOT NULL)
    ),
    CONSTRAINT task_executions_started_requires_time CHECK (
        (status = 'scheduled') OR (started_at IS NOT NULL)
    )
);

-- ============================================================================
-- TRIGGERS
-- ============================================================================

CREATE TRIGGER update_task_executions_updated_at BEFORE UPDATE ON task_executions
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- INDEXES
-- ============================================================================

-- Primary lookups
CREATE INDEX idx_task_executions_task_id ON task_executions(task_id);
CREATE INDEX idx_task_executions_workflow_id ON task_executions(workflow_id);
CREATE INDEX idx_task_executions_execution_id ON task_executions(execution_id);
CREATE INDEX idx_task_executions_workflow_task ON task_executions(workflow_id, task_id);

-- Status and lifecycle queries
CREATE INDEX idx_task_executions_status ON task_executions(status);
CREATE INDEX idx_task_executions_status_scheduled_at ON task_executions(status, scheduled_at)
    WHERE status IN ('scheduled', 'started');

-- Worker queries
CREATE INDEX idx_task_executions_service_identity ON task_executions(service_identity)
    WHERE service_identity IS NOT NULL;

-- Time-based queries
CREATE INDEX idx_task_executions_scheduled_at ON task_executions(scheduled_at);
CREATE INDEX idx_task_executions_started_at ON task_executions(started_at)
    WHERE started_at IS NOT NULL;
CREATE INDEX idx_task_executions_completed_at ON task_executions(completed_at)
    WHERE completed_at IS NOT NULL;

-- Task queue queries
CREATE INDEX idx_task_executions_task_queue ON task_executions(task_queue);
CREATE INDEX idx_task_executions_namespace_queue ON task_executions(namespace, task_queue);

-- Task type queries
CREATE INDEX idx_task_executions_task_type ON task_executions(task_type);

-- Composite index for common workflow task queries
CREATE INDEX idx_task_executions_workflow_status ON task_executions(workflow_id, status, scheduled_at DESC);

-- Failed tasks investigation
CREATE INDEX idx_task_executions_failures ON task_executions(status, error_type, completed_at DESC)
    WHERE status IN ('failed', 'timed_out');

-- Retry analysis
CREATE INDEX idx_task_executions_retries ON task_executions(task_id, attempt_number);

-- Idempotency index (from V030): prevents duplicate task creation during workflow replay
CREATE INDEX idx_task_executions_idempotency_key ON task_executions(workflow_id, execution_id, task_id);

-- Organization indexes (from V035)
CREATE INDEX idx_task_executions_org_workflow_task ON task_executions(organization_id, workflow_id, task_id)
    WHERE organization_id IS NOT NULL;

-- Task execution organization index
CREATE INDEX idx_task_executions_organization_id ON task_executions(organization_id)
    WHERE organization_id IS NOT NULL;

-- Shard index (from V036)
CREATE INDEX idx_task_exec_shard_id ON task_executions(shard_id) WHERE shard_id IS NOT NULL;

-- Performance indexes for task executions
CREATE INDEX idx_task_executions_status_v032 ON task_executions(workflow_id, status)
    WHERE status IN ('scheduled', 'running');
CREATE INDEX idx_task_executions_task_id_lookup ON task_executions(task_id, workflow_id);

-- ============================================================================
-- COMMENTS
-- ============================================================================

COMMENT ON TABLE task_executions IS 'Task execution lifecycle tracking - Orcher service-pull model';
COMMENT ON COLUMN task_executions.task_id IS 'Unique task identifier within the workflow (e.g., task-1, send-email)';
COMMENT ON COLUMN task_executions.task_type IS 'Type of task/activity (e.g., SendEmail, ProcessPayment)';
COMMENT ON COLUMN task_executions.workflow_id IS 'Workflow execution that scheduled this task';
COMMENT ON COLUMN task_executions.execution_id IS 'Execution ID within the workflow';
COMMENT ON COLUMN task_executions.task_queue IS 'Task queue where services poll for this task';
COMMENT ON COLUMN task_executions.service_identity IS 'Self-reported identity of the service that claimed this task';
COMMENT ON COLUMN task_executions.attempt_number IS 'Attempt number (1 for first attempt, 2+ for retries)';
COMMENT ON COLUMN task_executions.status IS 'Current status: scheduled | started | completed | failed | timed_out | cancelled';
COMMENT ON COLUMN task_executions.task_token IS 'Opaque token from PollTaskExecution';
COMMENT ON COLUMN task_executions.input IS 'Serialized task input payload (bytes)';
COMMENT ON COLUMN task_executions.result IS 'Serialized task result payload (bytes) - only for completed tasks';
COMMENT ON COLUMN task_executions.retry_policy IS 'Retry policy configuration';
COMMENT ON COLUMN task_executions.last_heartbeat_at IS 'Last heartbeat received from service (for long-running tasks)';
COMMENT ON COLUMN task_executions.organization_id IS 'Organization ID for multi-tenant isolation';
COMMENT ON COLUMN task_executions.shard_id IS 'Shard identifier for horizontal partitioning';
COMMENT ON INDEX idx_task_executions_idempotency_key IS 'Index for idempotency checks during task scheduling. Prevents duplicate task creation during workflow replay.';
