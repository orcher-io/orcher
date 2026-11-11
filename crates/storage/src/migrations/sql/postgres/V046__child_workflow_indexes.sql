-- V046: index the two ways child tracking rows are looked up
--
-- A run that has just finished is looked up by its own execution id to find
-- out whether it is somebody's child, and the child reconciliation sweep pages
-- through the rows whose outcome has not been delivered yet. Neither was
-- indexed, so both scanned the table, and it only grows: settled rows stay.
--
-- The sweep's index is partial. Open rows are the few children still running;
-- every settled row would only make it larger.
--
-- CONCURRENTLY, as in V042, so building them on a large table does not block
-- the writes that go on while it builds; the migration runner executes such
-- statements outside a transaction.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_child_workflows_child_execution_id
    ON child_workflow_executions (child_execution_id);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_child_workflows_open
    ON child_workflow_executions (id)
    WHERE state IN ('INITIATED', 'STARTED');
