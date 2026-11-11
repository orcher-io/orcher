-- orcher: outside a transaction
-- V063: find the cancellations whose cleanup time is up
--
-- Read by the timeout watcher, which terminates a run still live past its
-- cancel_deadline (V062). Partial over the few runs that have one, so it costs
-- nothing for the rest. CONCURRENTLY, as in V061.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_workflow_executions_cancel_deadline
    ON workflow_executions (cancel_deadline)
    WHERE cancel_deadline IS NOT NULL;
