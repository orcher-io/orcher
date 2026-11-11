-- ORCHER Task Orchestrator - Durable Execution Log
-- Migration V013: Step-level execution tracking, idempotency cache, and event gates with multi-tenancy

-- ============================================================================
-- ENUMS
-- ============================================================================

CREATE TYPE execution_log_status AS ENUM (
    'pending',      -- Step is queued for execution
    'running',      -- Step is currently executing
    'completed',    -- Step completed successfully
    'failed'        -- Step failed (error recorded)
);

CREATE TYPE execution_step_type AS ENUM (
    'task',              -- Distributed task execution (ctx.execute_task)
    'closure',           -- Inline closure execution (ctx.execute)
    'child_workflow',    -- Child workflow execution
    'side_effect'        -- Side effects, local activities, etc.
);

CREATE TYPE idempotency_status AS ENUM (
    'pending',      -- Request received, execution not started
    'running',      -- Workflow is executing
    'completed',    -- Workflow completed successfully
    'failed'        -- Workflow failed
);

CREATE TYPE event_gate_status AS ENUM (
    'waiting',      -- Workflow is waiting for the gate to open
    'opened',       -- Gate has been opened, event delivered
    'timeout',      -- Wait timed out
    'cancelled'     -- Wait was cancelled
);

-- ============================================================================
-- EXECUTION LOG TABLE: Durable step-level execution tracking
-- ============================================================================

CREATE TABLE execution_log (
    workflow_id UUID NOT NULL,
    step_name TEXT NOT NULL,

    -- Step type
    step_type execution_step_type NOT NULL DEFAULT 'task',

    -- Execution tracking
    execution_attempt INT NOT NULL DEFAULT 1,
    status execution_log_status NOT NULL DEFAULT 'pending',

    -- Step input/output
    input JSONB,
    result JSONB,

    -- Error tracking
    error_message TEXT,
    error_details JSONB,

    -- Timing information
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,

    -- Duration in milliseconds
    duration_ms BIGINT,

    -- Metadata
    retry_count INT NOT NULL DEFAULT 0,
    last_error TEXT,

    -- Multi-tenancy
    organization_id UUID REFERENCES organizations(id) ON DELETE CASCADE,

    PRIMARY KEY (workflow_id, step_name),

    CONSTRAINT execution_log_completed_at_check
        CHECK (status != 'completed' OR completed_at IS NOT NULL),
    CONSTRAINT execution_log_failed_at_check
        CHECK (status != 'failed' OR completed_at IS NOT NULL),
    CONSTRAINT execution_log_duration_positive
        CHECK (duration_ms IS NULL OR duration_ms >= 0)
);

-- ============================================================================
-- EXECUTION IDEMPOTENCY TABLE: Workflow-level deduplication
-- ============================================================================

CREATE TABLE execution_idempotency (
    execution_id TEXT NOT NULL PRIMARY KEY,

    -- Workflow identification
    workflow_id UUID NOT NULL,
    workflow_name TEXT NOT NULL,
    workflow_version TEXT NOT NULL,

    -- Execution status
    status idempotency_status NOT NULL DEFAULT 'pending',

    -- Input and result
    input JSONB NOT NULL,
    result JSONB,

    -- Error tracking
    error_message TEXT,
    error_details JSONB,

    -- Timing
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    expires_at TIMESTAMPTZ,

    -- TTL support
    ttl_seconds INT DEFAULT 86400,

    -- Multi-tenancy
    organization_id UUID REFERENCES organizations(id) ON DELETE CASCADE,

    CONSTRAINT execution_idempotency_completed_check
        CHECK (status != 'completed' OR completed_at IS NOT NULL),
    CONSTRAINT execution_idempotency_failed_check
        CHECK (status != 'failed' OR completed_at IS NOT NULL)
);

-- ============================================================================
-- EVENT GATES TABLE: Durable wait/wakeup primitives
-- ============================================================================

CREATE TABLE event_gates (
    workflow_id UUID NOT NULL,
    gate_name TEXT NOT NULL,

    -- Status
    status event_gate_status NOT NULL DEFAULT 'waiting',

    -- Event payload
    event_payload JSONB,

    -- Timing
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    opened_at TIMESTAMPTZ,
    timeout_at TIMESTAMPTZ,

    -- Metadata
    timeout_seconds INT,
    opened_by TEXT,

    -- Multi-tenancy
    organization_id UUID REFERENCES organizations(id) ON DELETE CASCADE,

    PRIMARY KEY (workflow_id, gate_name),

    CONSTRAINT event_gates_opened_check
        CHECK (status != 'opened' OR opened_at IS NOT NULL)
);

-- ============================================================================
-- INDEXES
-- ============================================================================

-- Execution log indexes
CREATE INDEX idx_execution_log_workflow_id ON execution_log(workflow_id);
CREATE INDEX idx_execution_log_workflow_status ON execution_log(workflow_id, status);
CREATE INDEX idx_execution_log_workflow_type ON execution_log(workflow_id, step_type);
CREATE INDEX idx_execution_log_workflow_type_status ON execution_log(workflow_id, step_type, status);
CREATE INDEX idx_execution_log_status_created ON execution_log(status, created_at);
CREATE INDEX idx_execution_log_type_duration ON execution_log(step_type, duration_ms DESC) WHERE status = 'completed';
CREATE INDEX idx_execution_log_failed ON execution_log(workflow_id, status) WHERE status = 'failed';
CREATE INDEX idx_execution_log_failed_closures ON execution_log(workflow_id, step_name, error_message)
    WHERE step_type = 'closure' AND status = 'failed';

-- Organization indexes for execution_log (from V035)
CREATE INDEX idx_execution_log_organization_id ON execution_log(organization_id) WHERE organization_id IS NOT NULL;
CREATE INDEX idx_execution_log_org_workflow ON execution_log(organization_id, workflow_id) WHERE organization_id IS NOT NULL;

-- Performance indexes for execution_log
CREATE INDEX idx_execution_log_workflow_lookup ON execution_log(workflow_id, step_name, created_at);
CREATE INDEX idx_execution_log_status ON execution_log(workflow_id, status) WHERE status IN ('pending', 'running');

-- Execution idempotency indexes
CREATE INDEX idx_execution_idempotency_workflow_id ON execution_idempotency(workflow_id);
CREATE INDEX idx_execution_idempotency_status ON execution_idempotency(status, created_at);
CREATE INDEX idx_execution_idempotency_expires ON execution_idempotency(expires_at) WHERE expires_at IS NOT NULL;

-- Organization indexes for execution_idempotency (from V035)
CREATE INDEX idx_execution_idempotency_organization_id ON execution_idempotency(organization_id) WHERE organization_id IS NOT NULL;
CREATE INDEX idx_execution_idempotency_org_execution ON execution_idempotency(organization_id, execution_id) WHERE organization_id IS NOT NULL;

-- Event gates indexes
CREATE INDEX idx_event_gates_workflow_id ON event_gates(workflow_id);
CREATE INDEX idx_event_gates_status ON event_gates(status, created_at);
CREATE INDEX idx_event_gates_waiting ON event_gates(workflow_id, status) WHERE status = 'waiting';
CREATE INDEX idx_event_gates_timeout ON event_gates(timeout_at) WHERE status = 'waiting' AND timeout_at IS NOT NULL;

-- Organization indexes for event_gates (from V035)
CREATE INDEX idx_event_gates_organization_id ON event_gates(organization_id) WHERE organization_id IS NOT NULL;
CREATE INDEX idx_event_gates_org_gate ON event_gates(organization_id, workflow_id, gate_name) WHERE organization_id IS NOT NULL;

-- ============================================================================
-- FUNCTIONS
-- ============================================================================

CREATE OR REPLACE FUNCTION cleanup_expired_idempotency()
RETURNS INT AS $$
DECLARE
    deleted_count INT;
BEGIN
    DELETE FROM execution_idempotency
    WHERE expires_at IS NOT NULL
      AND expires_at < NOW();

    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION execution_log_mark_completed(
    p_workflow_id UUID,
    p_step_name TEXT,
    p_result JSONB
) RETURNS VOID AS $$
BEGIN
    UPDATE execution_log
    SET
        status = 'completed',
        result = p_result,
        completed_at = NOW(),
        duration_ms = EXTRACT(EPOCH FROM (NOW() - started_at)) * 1000
    WHERE
        workflow_id = p_workflow_id
        AND step_name = p_step_name;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION execution_log_mark_failed(
    p_workflow_id UUID,
    p_step_name TEXT,
    p_error_message TEXT,
    p_error_details JSONB DEFAULT NULL
) RETURNS VOID AS $$
BEGIN
    UPDATE execution_log
    SET
        status = 'failed',
        error_message = p_error_message,
        error_details = p_error_details,
        completed_at = NOW(),
        duration_ms = EXTRACT(EPOCH FROM (NOW() - started_at)) * 1000,
        retry_count = retry_count + 1,
        last_error = p_error_message
    WHERE
        workflow_id = p_workflow_id
        AND step_name = p_step_name;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- COMMENTS
-- ============================================================================

COMMENT ON TABLE execution_log IS 'Durable execution log for workflow steps (ctx.execute() API). Provides step-level replay protection and result caching.';
COMMENT ON TABLE execution_idempotency IS 'Workflow-level idempotency cache (ExecutionId). Provides exactly-once semantics for workflow execution.';
COMMENT ON TABLE event_gates IS 'Durable event gate registry for ctx.event_gate() API. Enables workflows to wait for external events.';

COMMENT ON COLUMN execution_log.workflow_id IS 'Workflow execution identifier';
COMMENT ON COLUMN execution_log.step_name IS 'Unique step name within the workflow (from ctx.execute("step_name", ...))';
COMMENT ON COLUMN execution_log.step_type IS 'Step execution type: task (distributed), closure (inline), child_workflow, or side_effect';
COMMENT ON COLUMN execution_log.execution_attempt IS 'Execution attempt number (increments on retry)';
COMMENT ON COLUMN execution_log.input IS 'Step input parameters (JSONB for queryability)';
COMMENT ON COLUMN execution_log.result IS 'Step result value (JSONB, NULL until completed)';
COMMENT ON COLUMN execution_log.organization_id IS 'Organization ID for multi-tenant isolation';

COMMENT ON COLUMN execution_idempotency.execution_id IS 'Client-provided idempotency key (ExecutionId)';
COMMENT ON COLUMN execution_idempotency.organization_id IS 'Organization ID for multi-tenant isolation';

COMMENT ON COLUMN event_gates.gate_name IS 'Unique gate name within the workflow (from ctx.event_gate("gate_name"))';
COMMENT ON COLUMN event_gates.organization_id IS 'Organization ID for multi-tenant isolation';
