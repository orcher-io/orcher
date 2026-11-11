-- Add dedicated workflow_type column to workflow_executions.
-- Previously workflow_name held both the instance identifier and the workflow class type.
-- This column separates the two concepts; it is backfilled from workflow_name.
ALTER TABLE workflow_executions
    ADD COLUMN IF NOT EXISTS workflow_type TEXT NOT NULL DEFAULT '';

UPDATE workflow_executions
    SET workflow_type = workflow_name
    WHERE workflow_type = '';
