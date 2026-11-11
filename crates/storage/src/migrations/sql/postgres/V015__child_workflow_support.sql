-- ORCHER Task Orchestrator - Child Workflow Support and Journal
-- Migration V015: Workflow execution journal and parent-child workflow relationships with multi-tenancy and sharding

-- ============================================================================
-- WORKFLOW EXECUTION JOURNAL: Event-sourced workflow history
-- ============================================================================
--
-- Stores the complete execution journal (history) for each workflow execution.
-- Each journal entry represents a state transition or significant event.
-- This is the authoritative event-sourced history for workflow execution replay.
--
-- RELATIONSHIP WITH OTHER TABLES:
-- - V011 events: Platform-wide event sourcing (tasks, actors, workers)
-- - V013 execution_log: Step-level execution tracking for ctx.execute()
-- - V015 workflow_execution_journal: Workflow-level event sourcing (this migration)
-- - V015 child_workflow_executions: Parent-child relationships (this migration)

CREATE TABLE workflow_execution_journal (
    -- Primary key
    id BIGSERIAL PRIMARY KEY,

    -- Workflow execution reference
    workflow_id VARCHAR(255) NOT NULL,
    execution_id VARCHAR(255) NOT NULL,

    -- Journal entry metadata
    event_id BIGINT NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Event payload
    payload JSONB NOT NULL DEFAULT '{}',

    -- Version control and task correlation
    version INTEGER NOT NULL DEFAULT 0,
    task_id BIGINT NOT NULL DEFAULT 0,

    -- Multi-tenancy (from V035)
    organization_id UUID REFERENCES organizations(id) ON DELETE CASCADE,

    -- Horizontal sharding (from V036)
    shard_id INTEGER DEFAULT NULL,

    -- Audit
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_journal_event_id UNIQUE (workflow_id, execution_id, event_id),
    CONSTRAINT chk_event_id_positive CHECK (event_id > 0),
    CONSTRAINT chk_event_type_not_empty CHECK (event_type <> '')
);

-- ============================================================================
-- CHILD WORKFLOW EXECUTIONS: Parent-child workflow relationships
-- ============================================================================

CREATE TABLE child_workflow_executions (
    -- Primary key
    id BIGSERIAL PRIMARY KEY,

    -- Parent workflow information
    parent_workflow_id VARCHAR(255) NOT NULL,
    parent_execution_id VARCHAR(255) NOT NULL,
    parent_initiated_event_id BIGINT NOT NULL,
    parent_started_event_id BIGINT,

    -- Child workflow information
    child_workflow_id VARCHAR(255) NOT NULL,
    child_execution_id VARCHAR(255) NOT NULL,
    child_workflow_type VARCHAR(255) NOT NULL,

    -- Configuration
    task_queue VARCHAR(255) NOT NULL,
    input JSONB DEFAULT '{}',

    -- Parent close policy
    -- ABANDON: Let child continue running
    -- REQUEST_CANCEL: Request cancellation of child
    -- TERMINATE: Force terminate child immediately
    -- WAIT: Parent waits for child to complete before closing
    parent_close_policy VARCHAR(50) NOT NULL DEFAULT 'ABANDON',

    -- Child workflow state
    -- INITIATED: Parent has requested child creation
    -- STARTED: Child workflow has begun execution
    -- COMPLETED: Child workflow completed successfully
    -- FAILED: Child workflow failed
    -- TIMED_OUT: Child workflow timed out
    -- CANCELED: Child workflow was canceled
    -- TERMINATED: Child workflow was forcefully terminated
    state VARCHAR(50) NOT NULL DEFAULT 'INITIATED',

    -- Results and errors
    result JSONB,
    failure_reason TEXT,
    failure_details JSONB,

    -- Timing information
    initiated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,

    -- Multi-tenancy (from V035)
    organization_id UUID REFERENCES organizations(id) ON DELETE CASCADE,

    -- Horizontal sharding (from V036)
    shard_id INTEGER DEFAULT NULL,

    -- Audit
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_parent_close_policy CHECK (
        parent_close_policy IN ('ABANDON', 'REQUEST_CANCEL', 'TERMINATE', 'WAIT')
    ),
    CONSTRAINT chk_child_state CHECK (
        state IN ('INITIATED', 'STARTED', 'COMPLETED', 'FAILED', 'TIMED_OUT', 'CANCELED', 'TERMINATED')
    ),
    CONSTRAINT chk_completed_at_after_started CHECK (
        completed_at IS NULL OR started_at IS NULL OR completed_at >= started_at
    ),
    CONSTRAINT chk_started_at_after_initiated CHECK (
        started_at IS NULL OR started_at >= initiated_at
    ),
    CONSTRAINT uq_parent_child_initiated_event UNIQUE (parent_workflow_id, parent_execution_id, parent_initiated_event_id)
);

-- ============================================================================
-- TRIGGERS
-- ============================================================================

CREATE TRIGGER update_child_workflow_executions_updated_at
    BEFORE UPDATE ON child_workflow_executions
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- INDEXES
-- ============================================================================

-- Journal indexes
CREATE INDEX idx_journal_workflow_execution ON workflow_execution_journal(workflow_id, execution_id);
CREATE INDEX idx_journal_event_id ON workflow_execution_journal(workflow_id, execution_id, event_id);
CREATE INDEX idx_journal_event_type ON workflow_execution_journal(event_type);
CREATE INDEX idx_journal_timestamp ON workflow_execution_journal(timestamp);

-- Organization indexes for journal (from V035)
CREATE INDEX idx_workflow_execution_journal_organization_id ON workflow_execution_journal(organization_id) WHERE organization_id IS NOT NULL;
CREATE INDEX idx_workflow_execution_journal_org_workflow ON workflow_execution_journal(organization_id, workflow_id, execution_id) WHERE organization_id IS NOT NULL;

-- Shard index for journal (from V036)
CREATE INDEX idx_journal_shard_id ON workflow_execution_journal(shard_id) WHERE shard_id IS NOT NULL;

-- Performance index for journal
CREATE INDEX idx_journal_execution_event ON workflow_execution_journal(execution_id, event_id);

-- Child workflow indexes
CREATE INDEX idx_child_workflows_parent ON child_workflow_executions(parent_workflow_id, parent_execution_id);
CREATE INDEX idx_child_workflows_child ON child_workflow_executions(child_workflow_id, child_execution_id);
CREATE INDEX idx_child_workflows_state ON child_workflow_executions(state);
CREATE INDEX idx_child_workflows_parent_state ON child_workflow_executions(parent_workflow_id, parent_execution_id, state);
CREATE INDEX idx_child_workflows_policy ON child_workflow_executions(parent_close_policy);

-- Organization indexes for child_workflow_executions (from V035)
CREATE INDEX idx_child_workflow_executions_organization_id ON child_workflow_executions(organization_id) WHERE organization_id IS NOT NULL;
CREATE INDEX idx_child_workflow_executions_org_parent ON child_workflow_executions(organization_id, parent_workflow_id, parent_execution_id) WHERE organization_id IS NOT NULL;

-- Shard index for child_workflow_executions (from V036)
CREATE INDEX idx_child_workflow_shard_id ON child_workflow_executions(shard_id) WHERE shard_id IS NOT NULL;

-- ============================================================================
-- FUNCTIONS
-- ============================================================================

CREATE OR REPLACE FUNCTION get_next_event_id(
    p_workflow_id VARCHAR(255),
    p_execution_id VARCHAR(255)
) RETURNS BIGINT AS $$
DECLARE
    v_max_event_id BIGINT;
BEGIN
    SELECT COALESCE(MAX(event_id), 0) INTO v_max_event_id
    FROM workflow_execution_journal
    WHERE workflow_id = p_workflow_id
      AND execution_id = p_execution_id;

    RETURN v_max_event_id + 1;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION get_next_event_id IS 'Returns the next available event_id for a workflow execution journal';

CREATE OR REPLACE FUNCTION get_pending_child_workflows(
    p_parent_workflow_id VARCHAR(255),
    p_parent_execution_id VARCHAR(255)
) RETURNS TABLE(
    id BIGINT,
    child_workflow_id VARCHAR(255),
    child_execution_id VARCHAR(255),
    state VARCHAR(50),
    parent_close_policy VARCHAR(50)
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        c.id,
        c.child_workflow_id,
        c.child_execution_id,
        c.state,
        c.parent_close_policy
    FROM child_workflow_executions c
    WHERE c.parent_workflow_id = p_parent_workflow_id
      AND c.parent_execution_id = p_parent_execution_id
      AND c.state IN ('INITIATED', 'STARTED');
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION get_pending_child_workflows IS 'Returns all child workflows that are still running (not in a terminal state)';

CREATE OR REPLACE FUNCTION can_parent_close(
    p_parent_workflow_id VARCHAR(255),
    p_parent_execution_id VARCHAR(255)
) RETURNS BOOLEAN AS $$
DECLARE
    v_wait_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO v_wait_count
    FROM child_workflow_executions
    WHERE parent_workflow_id = p_parent_workflow_id
      AND parent_execution_id = p_parent_execution_id
      AND parent_close_policy = 'WAIT'
      AND state IN ('INITIATED', 'STARTED');

    RETURN v_wait_count = 0;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION can_parent_close IS 'Returns true if parent workflow can close (all WAIT children are complete)';

-- ============================================================================
-- COMMENTS
-- ============================================================================

COMMENT ON TABLE workflow_execution_journal IS 'Event-sourced journal entries for workflow executions. Each entry represents a state transition or significant event.';
COMMENT ON COLUMN workflow_execution_journal.event_id IS 'Monotonically increasing event ID within the execution';
COMMENT ON COLUMN workflow_execution_journal.event_type IS 'Type of event (e.g., WorkflowExecutionStarted, ChildWorkflowExecutionCompleted)';
COMMENT ON COLUMN workflow_execution_journal.payload IS 'JSON serialization of the event payload (step results, failure details, event data)';
COMMENT ON COLUMN workflow_execution_journal.organization_id IS 'Organization ID for multi-tenant isolation';
COMMENT ON COLUMN workflow_execution_journal.shard_id IS 'Shard identifier for horizontal partitioning';

COMMENT ON TABLE child_workflow_executions IS 'Tracks parent-child workflow relationships and child workflow lifecycle';
COMMENT ON COLUMN child_workflow_executions.parent_close_policy IS 'Determines child behavior when parent closes: ABANDON, REQUEST_CANCEL, TERMINATE, or WAIT';
COMMENT ON COLUMN child_workflow_executions.state IS 'Current state of the child workflow execution';
COMMENT ON COLUMN child_workflow_executions.parent_initiated_event_id IS 'Event ID in parent journal when child was initiated';
COMMENT ON COLUMN child_workflow_executions.parent_started_event_id IS 'Event ID in parent journal when child started';
COMMENT ON COLUMN child_workflow_executions.organization_id IS 'Organization ID for multi-tenant isolation';
COMMENT ON COLUMN child_workflow_executions.shard_id IS 'Shard identifier for horizontal partitioning';
