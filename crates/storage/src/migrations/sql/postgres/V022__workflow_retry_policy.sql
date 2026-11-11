-- Add retry_policy JSONB column to workflow_executions for per-workflow backoff configuration.
ALTER TABLE workflow_executions
    ADD COLUMN IF NOT EXISTS retry_policy JSONB;
