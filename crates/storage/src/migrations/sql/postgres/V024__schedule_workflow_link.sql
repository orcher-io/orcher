-- V024: Link schedule executions to workflow executions
--
-- Adds workflow_execution_id to schedule_executions so each schedule firing
-- is linked to the actual workflow run it created. This enables:
-- - "Show me all workflow runs triggered by this schedule"
-- - "This workflow failed — which schedule triggered it?"
-- - "What's the success rate of this schedule's workflows?"
--
-- Also adds workflow_type and namespace as materialized columns on schedules
-- for efficient querying without JSONB extraction.

-- 1. Add workflow_execution_id to schedule_executions
ALTER TABLE schedule_executions
    ADD COLUMN IF NOT EXISTS workflow_execution_id UUID;

-- FK constraint (nullable — set after workflow is created)
ALTER TABLE schedule_executions
    ADD CONSTRAINT fk_schedule_exec_workflow_exec
    FOREIGN KEY (workflow_execution_id)
    REFERENCES workflow_executions(id)
    ON DELETE SET NULL;

-- Index for reverse lookup: "which schedule triggered this workflow?"
CREATE INDEX IF NOT EXISTS idx_schedule_exec_workflow_id
    ON schedule_executions(workflow_execution_id)
    WHERE workflow_execution_id IS NOT NULL;

-- 2. Materialize workflow_type and namespace from task_template JSONB
-- This avoids repeated JSONB extraction in queries and enables indexing
ALTER TABLE schedules
    ADD COLUMN IF NOT EXISTS workflow_type VARCHAR(255)
    GENERATED ALWAYS AS (task_template->>'workflow_type') STORED;

ALTER TABLE schedules
    ADD COLUMN IF NOT EXISTS namespace VARCHAR(255)
    GENERATED ALWAYS AS (task_template->>'namespace') STORED;

-- Index for filtering schedules by workflow type
CREATE INDEX IF NOT EXISTS idx_schedules_workflow_type
    ON schedules(workflow_type);

-- Index for filtering schedules by namespace
CREATE INDEX IF NOT EXISTS idx_schedules_namespace
    ON schedules(namespace)
    WHERE namespace IS NOT NULL;

-- 3. Add composite index for "recent executions of a schedule with workflow link"
CREATE INDEX IF NOT EXISTS idx_schedule_exec_recent_with_wf
    ON schedule_executions(schedule_id, scheduled_at DESC, workflow_execution_id);
