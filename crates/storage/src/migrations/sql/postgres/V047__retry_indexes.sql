-- V047: index what the retry scheduler scans for
--
-- Every poll looks for failed runs due for a retry. A failed run the policy
-- will not retry stays failed forever, and without an index that leaves them
-- out every poll read them all again; the predicate here matches the poll's
-- (settled failures, and failures out of retries, are not in it), ordered as
-- the poll takes them.
--
-- The second index is for runs taken for a retry whose successor was never
-- recorded as created. There should be almost none: a run leaves it as soon
-- as its successor exists.
--
-- CONCURRENTLY, as in V042 and V046: the migration runner executes these
-- outside a transaction, so the builds do not block writes.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_workflow_exec_retry_eligible
    ON workflow_executions (submitted_at)
    WHERE status = 'failed'
      AND retries_settled_at IS NULL
      AND retry_count < max_retries;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_workflow_exec_retry_in_flight
    ON workflow_executions (updated_at)
    WHERE status = 'restarted_fresh'
      AND next_retry_at IS NOT NULL;
