-- ORCHER Task Orchestrator - Scheduling System
-- Migration V004: Scheduling and cron job functionality with multi-tenancy

-- ============================================================================
-- SCHEDULES TABLE: Managed scheduled task definitions
-- ============================================================================

CREATE TABLE schedules (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL,
    description TEXT,

    -- Multi-tenancy
    organization_id UUID REFERENCES organizations(id) ON DELETE CASCADE,

    -- Schedule configuration
    cron_expression VARCHAR(255) NOT NULL,
    timezone VARCHAR(100) NOT NULL DEFAULT 'UTC',
    enabled BOOLEAN NOT NULL DEFAULT true,

    -- Task template for scheduled execution
    task_template JSONB NOT NULL,

    -- Execution limits and configuration
    max_instances INTEGER DEFAULT 1,
    overlap_policy VARCHAR(50) NOT NULL DEFAULT 'skip',
    timeout_seconds INTEGER,
    retry_policy JSONB DEFAULT '{"max_attempts": 3, "delay_seconds": 60}',

    -- Scheduling constraints
    start_date TIMESTAMPTZ,
    end_date TIMESTAMPTZ,
    max_runs INTEGER,

    -- State tracking
    -- 'processing' status is used for claim-based locking by ScheduleProcessor
    -- to prevent double-fires across orchestrator instances (via FOR UPDATE SKIP LOCKED)
    status VARCHAR(50) NOT NULL DEFAULT 'active',
    next_run_at TIMESTAMPTZ,
    last_run_at TIMESTAMPTZ,
    run_count BIGINT NOT NULL DEFAULT 0,
    success_count BIGINT NOT NULL DEFAULT 0,
    failure_count BIGINT NOT NULL DEFAULT 0,

    -- Metadata and tags
    tags TEXT[] DEFAULT '{}',
    metadata JSONB DEFAULT '{}',

    -- Audit fields
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by VARCHAR(255),

    -- Constraints
    CONSTRAINT schedules_status_check CHECK (
        status IN ('active', 'paused', 'disabled', 'completed', 'failed', 'processing')
    ),
    CONSTRAINT schedules_overlap_policy_check CHECK (
        overlap_policy IN ('skip', 'queue', 'replace', 'parallel')
    ),
    CONSTRAINT schedules_max_instances_check CHECK (max_instances > 0),
    CONSTRAINT schedules_date_range_check CHECK (
        start_date IS NULL OR end_date IS NULL OR start_date <= end_date
    ),
    CONSTRAINT schedules_max_runs_check CHECK (max_runs IS NULL OR max_runs > 0),
    UNIQUE(name)
);

-- ============================================================================
-- SCHEDULE EXECUTIONS TABLE: Individual schedule runs
-- ============================================================================

CREATE TABLE schedule_executions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    schedule_id UUID NOT NULL REFERENCES schedules(id) ON DELETE CASCADE,
    task_id UUID,

    -- Multi-tenancy
    organization_id UUID REFERENCES organizations(id) ON DELETE CASCADE,

    -- Execution details
    execution_number BIGINT NOT NULL,
    scheduled_at TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,

    -- Execution status and results
    status VARCHAR(50) NOT NULL DEFAULT 'scheduled',
    result JSONB,
    error_message TEXT,
    error_details JSONB,

    -- Execution context
    trigger_type VARCHAR(50) NOT NULL DEFAULT 'cron',
    execution_context JSONB DEFAULT '{}',

    -- Performance metrics
    duration_ms BIGINT,
    resource_usage JSONB DEFAULT '{}',

    -- Audit
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT schedule_executions_status_check CHECK (
        status IN ('scheduled', 'running', 'completed', 'failed', 'skipped', 'cancelled')
    ),
    CONSTRAINT schedule_executions_trigger_check CHECK (
        trigger_type IN ('cron', 'manual', 'api', 'dependency')
    ),
    CONSTRAINT schedule_executions_number_check CHECK (execution_number > 0)
);

-- ============================================================================
-- SCHEDULE INSTANCES TABLE: Concurrent schedule instances
-- ============================================================================

CREATE TABLE schedule_instances (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    schedule_id UUID NOT NULL REFERENCES schedules(id) ON DELETE CASCADE,
    instance_key VARCHAR(255) NOT NULL,

    -- Multi-tenancy
    organization_id UUID REFERENCES organizations(id) ON DELETE CASCADE,

    -- Instance state
    status VARCHAR(50) NOT NULL DEFAULT 'running',
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_heartbeat TIMESTAMPTZ,

    -- Task references
    current_task_id UUID,
    task_ids UUID[] DEFAULT '{}',

    -- Instance metadata
    worker_id VARCHAR(255),
    execution_context JSONB DEFAULT '{}',
    metadata JSONB DEFAULT '{}',

    CONSTRAINT schedule_instances_status_check CHECK (
        status IN ('running', 'completed', 'failed', 'cancelled')
    ),
    UNIQUE(schedule_id, instance_key)
);

-- ============================================================================
-- CRON VALIDATIONS TABLE: Cache validated expressions
-- ============================================================================

CREATE TABLE cron_validations (
    expression VARCHAR(255) PRIMARY KEY,
    is_valid BOOLEAN NOT NULL,
    error_message TEXT,
    next_runs TIMESTAMPTZ[],
    validated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    expires_at TIMESTAMPTZ NOT NULL DEFAULT (NOW() + INTERVAL '24 hours')
);

-- ============================================================================
-- TRIGGERS
-- ============================================================================

-- Update schedules.updated_at on changes
CREATE TRIGGER update_schedules_updated_at BEFORE UPDATE ON schedules
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Note: calculate_next_run and trigger_update_schedule_next_run removed.
-- Cron parsing is handled in Rust via the `cron` crate for correctness and timezone support.

-- ============================================================================
-- INDEXES
-- ============================================================================

-- Schedule indexes
CREATE INDEX idx_schedules_status ON schedules(status);
CREATE INDEX idx_schedules_enabled ON schedules(enabled);
CREATE INDEX idx_schedules_next_run ON schedules(next_run_at) WHERE next_run_at IS NOT NULL AND enabled = true;
CREATE INDEX idx_schedules_cron_expression ON schedules(cron_expression);
CREATE INDEX idx_schedules_created_at ON schedules(created_at);
CREATE INDEX idx_schedules_tags ON schedules USING GIN(tags);
CREATE INDEX idx_schedules_metadata ON schedules USING GIN(metadata);
CREATE INDEX idx_schedules_organization_id ON schedules(organization_id) WHERE organization_id IS NOT NULL;
CREATE INDEX idx_schedules_org_name ON schedules(organization_id, name) WHERE organization_id IS NOT NULL;
CREATE INDEX idx_schedules_org_status ON schedules(organization_id, status) WHERE organization_id IS NOT NULL;

-- Schedule execution indexes
CREATE INDEX idx_schedule_executions_schedule_id ON schedule_executions(schedule_id);
CREATE INDEX idx_schedule_executions_task_id ON schedule_executions(task_id);
CREATE INDEX idx_schedule_executions_status ON schedule_executions(status);
CREATE INDEX idx_schedule_executions_scheduled_at ON schedule_executions(scheduled_at);
CREATE INDEX idx_schedule_executions_started_at ON schedule_executions(started_at);
CREATE INDEX idx_schedule_executions_execution_number ON schedule_executions(schedule_id, execution_number);
CREATE INDEX idx_schedule_executions_organization_id ON schedule_executions(organization_id) WHERE organization_id IS NOT NULL;
CREATE INDEX idx_schedule_executions_org_schedule ON schedule_executions(organization_id, schedule_id) WHERE organization_id IS NOT NULL;

-- Schedule instance indexes
CREATE INDEX idx_schedule_instances_schedule_id ON schedule_instances(schedule_id);
CREATE INDEX idx_schedule_instances_status ON schedule_instances(status);
CREATE INDEX idx_schedule_instances_started_at ON schedule_instances(started_at);
CREATE INDEX idx_schedule_instances_heartbeat ON schedule_instances(last_heartbeat);
CREATE INDEX idx_schedule_instances_worker ON schedule_instances(worker_id);
CREATE INDEX idx_schedule_instances_organization_id ON schedule_instances(organization_id) WHERE organization_id IS NOT NULL;
CREATE INDEX idx_schedule_instances_org_schedule ON schedule_instances(organization_id, schedule_id) WHERE organization_id IS NOT NULL;

-- Cron validation indexes
CREATE INDEX idx_cron_validations_valid ON cron_validations(is_valid);
CREATE INDEX idx_cron_validations_expires ON cron_validations(expires_at);

-- Composite indexes for common query patterns
CREATE INDEX idx_schedules_active_next_run ON schedules(enabled, status, next_run_at)
    WHERE enabled = true AND status = 'active';
CREATE INDEX idx_schedule_executions_recent ON schedule_executions(schedule_id, scheduled_at DESC);
CREATE INDEX idx_schedule_instances_active ON schedule_instances(schedule_id, status)
    WHERE status = 'running';
CREATE INDEX idx_schedule_executions_schedule_fk ON schedule_executions(schedule_id);
CREATE INDEX idx_schedule_instances_schedule_fk ON schedule_instances(schedule_id);

-- ============================================================================
-- FUNCTIONS
-- ============================================================================

CREATE OR REPLACE FUNCTION cleanup_expired_cron_validations()
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM cron_validations WHERE expires_at < NOW();
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- COMMENTS
-- ============================================================================

COMMENT ON TABLE schedules IS 'Scheduled task definitions with cron expressions and execution policies';
COMMENT ON TABLE schedule_executions IS 'Individual schedule execution history and results';
COMMENT ON TABLE schedule_instances IS 'Active schedule instances for overlap control';
COMMENT ON TABLE cron_validations IS 'Cached cron expression validations for performance';

COMMENT ON COLUMN schedules.overlap_policy IS 'How to handle overlapping executions: skip, queue, replace, parallel';
COMMENT ON COLUMN schedules.task_template IS 'Template for creating tasks from this schedule';
COMMENT ON COLUMN schedules.max_instances IS 'Maximum concurrent instances allowed';
COMMENT ON COLUMN schedules.status IS 'Schedule status: active, paused, disabled, completed, failed, or processing (claim-based lock for ScheduleProcessor)';
COMMENT ON COLUMN schedule_executions.execution_number IS 'Sequential execution number for this schedule';
COMMENT ON COLUMN schedule_instances.instance_key IS 'Unique identifier for this instance execution';
