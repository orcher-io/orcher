-- ORCHER Storage - Workflow Execution Tables
-- Migration V003: Workflow execution tracking and runtime state
--
-- This migration creates the schema for workflow execution tracking in
-- Orcher's orchestration model where:
-- - Developers deploy their own services (workers)
-- - Services connect to Orcher and register workflows at runtime
-- - Orcher tracks execution instances only (no code deployment)
--
-- Tables:
-- - workflow_executions: Runtime instances of workflow executions
-- - workflow_execution_logs: Real-time log streaming
-- - workflow_execution_metrics: Performance metrics aggregation

-- ============================================================================
-- WORKFLOW EXECUTIONS TABLE
-- ============================================================================

CREATE TABLE workflow_executions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- External-facing unique execution identifier
    execution_id TEXT NOT NULL UNIQUE,

    -- Optional user-provided workflow identifier for client lookups and correlation
    -- Distinct from execution_id which is system-generated
    workflow_id TEXT,

    -- Registry reference (optional, for workflow registry integration)
    registry_id UUID,

    -- Workflow identification (registered by services at runtime)
    workflow_name VARCHAR(255) NOT NULL,
    workflow_version VARCHAR(50) NOT NULL DEFAULT '1.0.0',
    namespace VARCHAR(100) NOT NULL DEFAULT 'default',

    -- Execution state
    status VARCHAR(50) NOT NULL DEFAULT 'pending',

    -- Input and output
    parameters JSONB NOT NULL DEFAULT '{}',
    result JSONB,
    error TEXT,
    error_details JSONB,

    -- Execution context
    context JSONB DEFAULT '{}',
    environment JSONB DEFAULT '{}',

    -- Progress tracking
    progress JSONB,
    current_step TEXT,

    -- Statistics
    statistics JSONB,
    total_steps INTEGER DEFAULT 0,
    completed_steps INTEGER DEFAULT 0,
    failed_steps INTEGER DEFAULT 0,
    retried_steps INTEGER DEFAULT 0,

    -- Performance metrics
    duration_ms BIGINT,
    cpu_time_ms BIGINT,
    memory_peak_bytes BIGINT,
    average_step_duration_ms BIGINT,

    -- Idempotency support
    idempotency_key TEXT UNIQUE,

    -- Execution metadata
    priority INTEGER DEFAULT 0,
    timeout_secs INTEGER,
    retry_count INTEGER DEFAULT 0,
    max_retries INTEGER DEFAULT 3,

    -- Tags for categorization and filtering
    tags JSONB DEFAULT '{}',

    -- Indexed custom metadata for workflow filtering (e.g., customer_id, order_status)
    labels JSONB NOT NULL DEFAULT '{}',

    -- Trigger information
    trigger_type VARCHAR(50) NOT NULL DEFAULT 'manual',
    triggered_by UUID,

    -- Parent/child workflow relationships
    parent_execution_id TEXT REFERENCES workflow_executions(execution_id) ON DELETE CASCADE,
    root_execution_id TEXT,

    -- Ownership (multi-tenancy)
    user_id UUID,
    organization_id UUID REFERENCES organizations(id) ON DELETE CASCADE,

    -- Sharding support (horizontal partitioning across orchestrator instances)
    shard_id INTEGER DEFAULT NULL,

    -- Claim semantics for safe distributed polling
    -- Identity of the service/worker that claimed this execution for processing
    claimed_by TEXT,
    -- Timestamp when this execution was claimed by a service
    claimed_at TIMESTAMPTZ,
    -- Timestamp when the claim expires; used to detect crashed services and reclaim executions
    claim_timeout_at TIMESTAMPTZ,
    -- Task queue name for routing work to appropriate services
    task_queue VARCHAR(255) NOT NULL DEFAULT 'default',

    -- Pending requeue support (Solution B for task-aware workflow re-polling)
    -- Flag indicating workflow needs to be re-queued to Redis after claim release
    pending_requeue BOOLEAN NOT NULL DEFAULT FALSE,
    -- Timestamp when pending_requeue was set; used for debugging and fallback timeout
    pending_requeue_at TIMESTAMPTZ,

    -- Timing
    submitted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    cancelled_at TIMESTAMPTZ,
    last_retry_at TIMESTAMPTZ,
    next_retry_at TIMESTAMPTZ,

    -- Audit
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Constraints
    CONSTRAINT workflow_exec_status_check CHECK (
        status IN (
            'pending', 'queued', 'running', 'paused',
            'completed', 'failed', 'cancelled', 'terminated', 'timed_out',
            'retrying', 'waiting'
        )
    ),
    CONSTRAINT workflow_exec_trigger_check CHECK (
        trigger_type IN ('manual', 'api', 'scheduled', 'event', 'webhook', 'workflow', 'cli')
    ),
    CONSTRAINT workflow_exec_priority_check CHECK (
        priority >= 0 AND priority <= 100
    ),
    CONSTRAINT workflow_exec_retry_check CHECK (
        retry_count >= 0 AND retry_count <= max_retries
    ),
    CONSTRAINT workflow_exec_steps_check CHECK (
        completed_steps + failed_steps + retried_steps <= total_steps
    )
);

-- ============================================================================
-- INDEXES: Optimized for API queries, monitoring, and distributed polling
-- ============================================================================

-- Core lookup indexes
CREATE INDEX idx_workflow_exec_execution_id ON workflow_executions(execution_id);
CREATE INDEX idx_workflow_exec_workflow_name ON workflow_executions(workflow_name);
CREATE INDEX idx_workflow_exec_namespace ON workflow_executions(namespace);
CREATE INDEX idx_workflow_exec_status ON workflow_executions(status);

-- User-provided workflow_id lookup
CREATE INDEX idx_workflow_exec_workflow_id ON workflow_executions(workflow_id)
    WHERE workflow_id IS NOT NULL;

-- Idempotency support
CREATE INDEX idx_workflow_exec_idempotency ON workflow_executions(idempotency_key) WHERE idempotency_key IS NOT NULL;

-- Time-based queries
CREATE INDEX idx_workflow_exec_submitted_at ON workflow_executions(submitted_at DESC);
CREATE INDEX idx_workflow_exec_started_at ON workflow_executions(started_at DESC) WHERE started_at IS NOT NULL;
CREATE INDEX idx_workflow_exec_completed_at ON workflow_executions(completed_at DESC) WHERE completed_at IS NOT NULL;

-- Ownership and multi-tenancy
CREATE INDEX idx_workflow_exec_user_id ON workflow_executions(user_id) WHERE user_id IS NOT NULL;
CREATE INDEX idx_workflow_exec_organization ON workflow_executions(organization_id) WHERE organization_id IS NOT NULL;
CREATE INDEX idx_workflow_exec_triggered_by ON workflow_executions(triggered_by) WHERE triggered_by IS NOT NULL;

-- Relationship tracking
CREATE INDEX idx_workflow_exec_parent ON workflow_executions(parent_execution_id) WHERE parent_execution_id IS NOT NULL;
CREATE INDEX idx_workflow_exec_root ON workflow_executions(root_execution_id) WHERE root_execution_id IS NOT NULL;

-- Composite indexes for common query patterns
CREATE INDEX idx_workflow_exec_namespace_name ON workflow_executions(namespace, workflow_name, submitted_at DESC);
CREATE INDEX idx_workflow_exec_namespace_status ON workflow_executions(namespace, status, submitted_at DESC);
CREATE INDEX idx_workflow_exec_user_status ON workflow_executions(user_id, status, submitted_at DESC) WHERE user_id IS NOT NULL;
CREATE INDEX idx_workflow_exec_org_status ON workflow_executions(organization_id, status, submitted_at DESC) WHERE organization_id IS NOT NULL;

-- Active executions (frequently queried)
CREATE INDEX idx_workflow_exec_active ON workflow_executions(status, priority DESC, submitted_at DESC)
    WHERE status IN ('pending', 'queued', 'running', 'retrying', 'waiting') AND cancelled_at IS NULL;

-- Queue management (priority + FIFO)
CREATE INDEX idx_workflow_exec_queue ON workflow_executions(namespace, status, priority DESC, submitted_at)
    WHERE status IN ('pending', 'queued') AND cancelled_at IS NULL;

-- Failed executions (for monitoring and retry)
CREATE INDEX idx_workflow_exec_failed ON workflow_executions(status, submitted_at DESC)
    WHERE status = 'failed' AND retry_count < max_retries;

-- Workflow retry scheduling (for automatic retry system)
CREATE INDEX idx_workflow_exec_pending_retry ON workflow_executions(next_retry_at, status, retry_count)
    WHERE status = 'retrying' AND next_retry_at IS NOT NULL;

-- Long-running executions (for timeout detection)
CREATE INDEX idx_workflow_exec_long_running ON workflow_executions(started_at)
    WHERE status = 'running';

-- Claim-based polling (V025)
CREATE INDEX idx_workflow_executions_poll_available
    ON workflow_executions (task_queue, namespace, status, priority DESC, submitted_at)
    WHERE claimed_at IS NULL AND cancelled_at IS NULL;

CREATE INDEX idx_workflow_executions_stale_claims
    ON workflow_executions (claim_timeout_at)
    WHERE claimed_at IS NOT NULL;

CREATE INDEX idx_workflow_executions_claimed_by
    ON workflow_executions (claimed_by)
    WHERE claimed_at IS NOT NULL;

-- Pending requeue (V031 + V032)
CREATE INDEX idx_workflow_exec_pending_requeue
    ON workflow_executions(pending_requeue, claim_timeout_at)
    WHERE pending_requeue = TRUE;

CREATE INDEX idx_workflow_executions_pending_requeue
    ON workflow_executions (id)
    WHERE pending_requeue = true;

-- High-throughput polling (V032)
CREATE INDEX idx_workflow_executions_polling
    ON workflow_executions (task_queue, namespace, created_at)
    WHERE status = 'running';

CREATE INDEX idx_workflow_executions_claim
    ON workflow_executions (task_queue, namespace, priority DESC, submitted_at)
    WHERE status IN ('pending', 'queued', 'running', 'retrying', 'waiting')
      AND claimed_at IS NULL
      AND cancelled_at IS NULL;

-- Shard-based queries (V036)
CREATE INDEX idx_workflow_exec_shard_id ON workflow_executions (shard_id) WHERE shard_id IS NOT NULL;

-- Organization + namespace composite (V035)
CREATE INDEX idx_workflow_executions_org_namespace_exec
    ON workflow_executions(organization_id, namespace, execution_id)
    WHERE organization_id IS NOT NULL;

-- GIN indexes for JSONB queries
CREATE INDEX idx_workflow_exec_parameters_gin ON workflow_executions USING GIN(parameters);
CREATE INDEX idx_workflow_exec_result_gin ON workflow_executions USING GIN(result);
CREATE INDEX idx_workflow_exec_tags_gin ON workflow_executions USING GIN(tags);
CREATE INDEX idx_workflow_exec_context_gin ON workflow_executions USING GIN(context);
CREATE INDEX idx_workflow_exec_statistics_gin ON workflow_executions USING GIN(statistics);
CREATE INDEX idx_workflow_exec_labels_gin ON workflow_executions USING GIN(labels);

-- ============================================================================
-- COMMENTS
-- ============================================================================

COMMENT ON TABLE workflow_executions IS 'Runtime execution instances of workflows';
COMMENT ON COLUMN workflow_executions.execution_id IS 'External-facing unique execution identifier (string format for API)';
COMMENT ON COLUMN workflow_executions.workflow_id IS 'Optional user-provided workflow identifier for client lookups and correlation. Distinct from execution_id which is system-generated.';
COMMENT ON COLUMN workflow_executions.workflow_name IS 'Workflow function name registered by service at runtime';
COMMENT ON COLUMN workflow_executions.workflow_version IS 'Workflow version (semantic versioning)';
COMMENT ON COLUMN workflow_executions.idempotency_key IS 'Optional client-provided key for idempotent execution requests';
COMMENT ON COLUMN workflow_executions.parameters IS 'Input parameters passed to workflow execution (JSONB)';
COMMENT ON COLUMN workflow_executions.result IS 'Execution result/output data (JSONB)';
COMMENT ON COLUMN workflow_executions.progress IS 'Real-time progress: {current_step, total_steps, completed_steps, progress_percent, message}';
COMMENT ON COLUMN workflow_executions.statistics IS 'Execution statistics: {total_steps, successful_steps, failed_steps, retried_steps, total_duration_ms}';
COMMENT ON COLUMN workflow_executions.tags IS 'Key-value tags for filtering and categorization';
COMMENT ON COLUMN workflow_executions.labels IS 'Indexed custom metadata for workflow filtering (JSONB, GIN indexed)';
COMMENT ON COLUMN workflow_executions.parent_execution_id IS 'Parent execution for sub-workflows (parent-child relationship)';
COMMENT ON COLUMN workflow_executions.root_execution_id IS 'Root execution in a workflow chain (for tracing entire execution tree)';
COMMENT ON COLUMN workflow_executions.trigger_type IS 'How workflow was initiated: manual, api, scheduled, event, webhook, workflow, cli';
COMMENT ON COLUMN workflow_executions.namespace IS 'Logical namespace for multi-tenancy and isolation';
COMMENT ON COLUMN workflow_executions.status IS 'Workflow status: pending, queued, running, paused, completed, failed, cancelled, terminated, timed_out, retrying, waiting';
COMMENT ON COLUMN workflow_executions.last_retry_at IS 'Timestamp of the last retry attempt (NULL if never retried)';
COMMENT ON COLUMN workflow_executions.next_retry_at IS 'Timestamp when the next retry should be attempted (NULL if not scheduled for retry)';
COMMENT ON COLUMN workflow_executions.claimed_by IS 'Identity of the service/worker that claimed this execution for processing';
COMMENT ON COLUMN workflow_executions.claimed_at IS 'Timestamp when this execution was claimed by a service';
COMMENT ON COLUMN workflow_executions.claim_timeout_at IS 'Timestamp when the claim expires; used to detect crashed services and reclaim executions';
COMMENT ON COLUMN workflow_executions.task_queue IS 'Task queue name for routing work to appropriate services';
COMMENT ON COLUMN workflow_executions.pending_requeue IS 'Flag indicating workflow needs to be re-queued to Redis after claim release. Set when task completes during in-progress CompleteWorkflowExecution RPC.';
COMMENT ON COLUMN workflow_executions.pending_requeue_at IS 'Timestamp when pending_requeue was set. Used for debugging and fallback timeout.';
COMMENT ON COLUMN workflow_executions.shard_id IS 'Shard identifier for horizontal partitioning across orchestrator instances';

-- ============================================================================
-- WORKFLOW EXECUTION LOGS TABLE: Real-time execution logs
-- ============================================================================

CREATE TABLE workflow_execution_logs (
    id BIGSERIAL PRIMARY KEY,

    -- Execution reference
    execution_id TEXT NOT NULL,

    -- Log entry details
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    level VARCHAR(20) NOT NULL,
    message TEXT NOT NULL,

    -- Context
    step TEXT,
    task_id UUID,
    worker_id TEXT,

    -- Additional metadata
    metadata JSONB DEFAULT '{}',

    -- Source information
    source VARCHAR(100),

    -- Multi-tenancy
    organization_id UUID REFERENCES organizations(id) ON DELETE CASCADE,

    -- Audit
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Constraints
    CONSTRAINT workflow_exec_logs_level_check CHECK (
        level IN ('trace', 'debug', 'info', 'warn', 'error', 'fatal')
    ),

    CONSTRAINT fk_workflow_exec_logs_execution
        FOREIGN KEY (execution_id)
        REFERENCES workflow_executions(execution_id)
        ON DELETE CASCADE
);

-- Indexes for log streaming (SSE) and querying
CREATE INDEX idx_workflow_exec_logs_execution_timestamp ON workflow_execution_logs(execution_id, timestamp DESC);
CREATE INDEX idx_workflow_exec_logs_execution_id_asc ON workflow_execution_logs(execution_id, id ASC);
CREATE INDEX idx_workflow_exec_logs_timestamp ON workflow_execution_logs(timestamp DESC);
CREATE INDEX idx_workflow_exec_logs_level ON workflow_execution_logs(level, timestamp DESC)
    WHERE level IN ('error', 'fatal');
CREATE INDEX idx_workflow_exec_logs_step ON workflow_execution_logs(execution_id, step, timestamp DESC)
    WHERE step IS NOT NULL;
CREATE INDEX idx_workflow_exec_logs_after_id ON workflow_execution_logs(execution_id, id)
    WHERE id > 0;
CREATE INDEX idx_workflow_exec_logs_metadata_gin ON workflow_execution_logs USING GIN(metadata);
CREATE INDEX idx_workflow_execution_logs_organization_id ON workflow_execution_logs(organization_id)
    WHERE organization_id IS NOT NULL;

-- Comments
COMMENT ON TABLE workflow_execution_logs IS 'Real-time execution logs with streaming support (SSE)';
COMMENT ON COLUMN workflow_execution_logs.execution_id IS 'Reference to workflow_executions.execution_id';
COMMENT ON COLUMN workflow_execution_logs.level IS 'Log level: trace, debug, info, warn, error, fatal';
COMMENT ON COLUMN workflow_execution_logs.step IS 'Workflow step/phase generating the log';
COMMENT ON COLUMN workflow_execution_logs.task_id IS 'Associated task ID (if log from specific task)';
COMMENT ON COLUMN workflow_execution_logs.metadata IS 'Additional structured log data (JSONB)';
COMMENT ON COLUMN workflow_execution_logs.source IS 'Log source identifier (e.g., worker ID, component name)';

-- ============================================================================
-- WORKFLOW EXECUTION METRICS TABLE: Performance metrics aggregation
-- ============================================================================

CREATE TABLE workflow_execution_metrics (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Workflow identification
    namespace VARCHAR(100) NOT NULL,
    workflow_name VARCHAR(255) NOT NULL,
    workflow_version VARCHAR(50) NOT NULL,

    -- Multi-tenancy
    organization_id UUID REFERENCES organizations(id) ON DELETE CASCADE,

    -- Time window
    metric_date DATE NOT NULL,
    hour_of_day INTEGER,

    -- Execution counts
    total_executions INTEGER NOT NULL DEFAULT 0,
    completed_executions INTEGER NOT NULL DEFAULT 0,
    failed_executions INTEGER NOT NULL DEFAULT 0,
    cancelled_executions INTEGER NOT NULL DEFAULT 0,
    timed_out_executions INTEGER NOT NULL DEFAULT 0,

    -- Duration statistics (in milliseconds)
    avg_duration_ms BIGINT,
    min_duration_ms BIGINT,
    max_duration_ms BIGINT,
    p50_duration_ms BIGINT,
    p95_duration_ms BIGINT,
    p99_duration_ms BIGINT,

    -- Step statistics
    avg_total_steps NUMERIC(10,2),
    avg_failed_steps NUMERIC(10,2),
    avg_retried_steps NUMERIC(10,2),

    -- Resource statistics
    avg_cpu_time_ms BIGINT,
    avg_memory_peak_bytes BIGINT,

    -- Last update
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Constraints
    CONSTRAINT workflow_exec_metrics_hour_check CHECK (
        hour_of_day IS NULL OR (hour_of_day >= 0 AND hour_of_day <= 23)
    ),

    CONSTRAINT workflow_exec_metrics_unique UNIQUE (
        namespace, workflow_name, workflow_version, metric_date, hour_of_day
    )
);

-- Indexes for time-series queries
CREATE INDEX idx_workflow_exec_metrics_namespace ON workflow_execution_metrics(namespace, metric_date DESC);
CREATE INDEX idx_workflow_exec_metrics_workflow ON workflow_execution_metrics(workflow_name, metric_date DESC);
CREATE INDEX idx_workflow_exec_metrics_date ON workflow_execution_metrics(metric_date DESC);
CREATE INDEX idx_workflow_exec_metrics_daily ON workflow_execution_metrics(namespace, workflow_name, workflow_version, metric_date DESC)
    WHERE hour_of_day IS NULL;
CREATE INDEX idx_workflow_execution_metrics_organization_id ON workflow_execution_metrics(organization_id)
    WHERE organization_id IS NOT NULL;
CREATE INDEX idx_workflow_execution_metrics_org_namespace ON workflow_execution_metrics(organization_id, namespace)
    WHERE organization_id IS NOT NULL;

-- Comments
COMMENT ON TABLE workflow_execution_metrics IS 'Aggregated workflow execution metrics for monitoring and analytics';
COMMENT ON COLUMN workflow_execution_metrics.metric_date IS 'Date of metrics aggregation';
COMMENT ON COLUMN workflow_execution_metrics.hour_of_day IS 'Hour (0-23) for hourly aggregation, NULL for daily aggregation';

-- ============================================================================
-- TRIGGERS: Automatic updates
-- ============================================================================

-- Update workflow_executions.updated_at on changes
CREATE OR REPLACE FUNCTION update_workflow_executions_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER workflow_executions_update_timestamp
    BEFORE UPDATE ON workflow_executions
    FOR EACH ROW
    EXECUTE FUNCTION update_workflow_executions_timestamp();

-- Automatically calculate duration_ms when execution completes
CREATE OR REPLACE FUNCTION calculate_workflow_execution_duration()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.completed_at IS NOT NULL AND NEW.started_at IS NOT NULL AND OLD.completed_at IS NULL THEN
        NEW.duration_ms = EXTRACT(EPOCH FROM (NEW.completed_at - NEW.started_at)) * 1000;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER workflow_executions_calculate_duration
    BEFORE UPDATE ON workflow_executions
    FOR EACH ROW
    EXECUTE FUNCTION calculate_workflow_execution_duration();

-- PostgreSQL LISTEN/NOTIFY for real-time workflow failure notifications (V028)
-- Enables the retry scheduler to be notified immediately when a workflow fails,
-- reducing retry latency from 30+ seconds to milliseconds.
CREATE OR REPLACE FUNCTION notify_workflow_failed()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.status = 'failed' AND (OLD.status IS NULL OR OLD.status != 'failed') THEN
        PERFORM pg_notify(
            'workflow_failed',
            json_build_object(
                'workflow_id', NEW.id::text,
                'execution_id', NEW.execution_id,
                'namespace', NEW.namespace,
                'workflow_name', NEW.workflow_name,
                'error', NEW.error,
                'retry_count', NEW.retry_count,
                'max_retries', NEW.max_retries,
                'failed_at', NEW.updated_at
            )::text
        );
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER workflow_failed_notify
    AFTER UPDATE ON workflow_executions
    FOR EACH ROW
    WHEN (NEW.status = 'failed')
    EXECUTE FUNCTION notify_workflow_failed();

-- Comments on trigger functions
COMMENT ON FUNCTION update_workflow_executions_timestamp IS 'Automatically update workflow_executions.updated_at timestamp';
COMMENT ON FUNCTION calculate_workflow_execution_duration IS 'Automatically calculate execution duration on completion';
COMMENT ON FUNCTION notify_workflow_failed IS 'Sends PostgreSQL notification when a workflow transitions to failed status. Used by retry scheduler for real-time failure detection.';
COMMENT ON TRIGGER workflow_failed_notify ON workflow_executions IS 'Triggers immediate notification to retry scheduler when workflow fails, enabling sub-second retry detection instead of 30+ second polling.';

-- ============================================================================
-- VIEWS: Convenient query interfaces
-- ============================================================================

-- Active executions view
CREATE VIEW v_active_workflow_executions AS
SELECT
    execution_id,
    workflow_name,
    workflow_version,
    namespace,
    status,
    priority,
    submitted_at,
    started_at,
    EXTRACT(EPOCH FROM (NOW() - started_at))::INTEGER AS running_seconds,
    user_id,
    organization_id,
    retry_count,
    max_retries
FROM workflow_executions
WHERE status IN ('pending', 'queued', 'running', 'retrying', 'waiting')
    AND cancelled_at IS NULL
ORDER BY priority DESC, submitted_at;

COMMENT ON VIEW v_active_workflow_executions IS 'Currently active workflow executions (pending, queued, running, retrying, waiting)';

-- Execution summary view (last 30 days)
CREATE VIEW v_workflow_execution_summary AS
SELECT
    namespace,
    workflow_name,
    workflow_version,
    status,
    COUNT(*) AS total_executions,
    AVG(duration_ms) AS avg_duration_ms,
    MAX(duration_ms) AS max_duration_ms,
    MIN(duration_ms) AS min_duration_ms,
    COUNT(*) FILTER (WHERE status = 'completed') AS completed_count,
    COUNT(*) FILTER (WHERE status = 'failed') AS failed_count,
    COUNT(*) FILTER (WHERE status = 'cancelled') AS cancelled_count,
    COUNT(*) FILTER (WHERE status = 'timed_out') AS timed_out_count,
    AVG(retry_count)::NUMERIC(10,2) AS avg_retry_count
FROM workflow_executions
WHERE submitted_at > NOW() - INTERVAL '30 days'
GROUP BY namespace, workflow_name, workflow_version, status;

COMMENT ON VIEW v_workflow_execution_summary IS '30-day summary of workflow execution statistics grouped by workflow and status';

-- Failed executions eligible for retry
CREATE VIEW v_failed_retryable_executions AS
SELECT
    execution_id,
    workflow_name,
    workflow_version,
    namespace,
    error,
    retry_count,
    max_retries,
    submitted_at,
    completed_at
FROM workflow_executions
WHERE status = 'failed'
    AND retry_count < max_retries
    AND completed_at > NOW() - INTERVAL '24 hours'
ORDER BY priority DESC, submitted_at;

COMMENT ON VIEW v_failed_retryable_executions IS 'Failed executions that are eligible for retry (within 24 hours, retry count < max)';

-- Chronicle query layer view
CREATE VIEW workflow_state_projection AS
SELECT
    id AS workflow_id,
    workflow_name,
    namespace,
    status,
    started_at,
    completed_at,
    triggered_by::text AS started_by,
    CASE
        WHEN parent_execution_id IS NOT NULL AND parent_execution_id ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        THEN parent_execution_id::uuid
        ELSE NULL
    END AS parent_workflow_id,
    labels,
    tags,
    error AS last_error
FROM workflow_executions;

COMMENT ON VIEW workflow_state_projection IS 'Chronicle query layer view for listing and filtering workflow executions';

-- ============================================================================
-- HELPER FUNCTIONS
-- ============================================================================

CREATE OR REPLACE FUNCTION get_workflow_execution_stats(
    p_execution_id TEXT
) RETURNS TABLE (
    execution_id TEXT,
    total_steps INTEGER,
    completed_steps INTEGER,
    failed_steps INTEGER,
    retried_steps INTEGER,
    progress_percent NUMERIC,
    duration_ms BIGINT,
    log_count BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        we.execution_id,
        we.total_steps,
        we.completed_steps,
        we.failed_steps,
        we.retried_steps,
        CASE
            WHEN we.total_steps > 0
            THEN ROUND((we.completed_steps::NUMERIC / we.total_steps::NUMERIC) * 100, 2)
            ELSE 0
        END AS progress_percent,
        we.duration_ms,
        (SELECT COUNT(*) FROM workflow_execution_logs WHERE execution_id = p_execution_id) AS log_count
    FROM workflow_executions we
    WHERE we.execution_id = p_execution_id;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION get_workflow_execution_stats IS 'Get comprehensive statistics for a workflow execution';
