-- V067: a run timeout is retried, and the execution timeout bounds every run
--
-- A run past its run timeout ends timed_out, and the workflow's retry policy
-- applies to it as to a failure: the write that times it out also schedules
-- its retry (next_retry_at) or settles it (retries_settled_at). Until the
-- retry is taken, a timed-out run with a retry scheduled is open, exactly as
-- a failure waiting for its retry is: it holds its workflow id, and the retry
-- scheduler finds it. A run past the execution timeout is settled as it times
-- out and is never retried.
--
-- execution_deadline is when the execution timeout runs out: the first run's
-- submitted_at plus timeout_secs, carried unchanged to every retry and
-- restart-fresh, so the timeout covers the workflow across all its runs
-- rather than restarting with each. A reset is an operator restarting the
-- workflow: it starts a new deadline from its own submitted_at, which its
-- successors carry from there. NULL means no
-- execution timeout; a run written before this has none of its own and is
-- measured from its own submitted_at, as it always was.
--
-- The timeout watcher used to schedule a retry on every timed-out run, and
-- nothing ever took it. Those runs are settled first, so none of them is
-- retried now, long after, or found holding its workflow id beside a newer
-- run of it — which would fail the unique index built below.
--
-- Run outside a transaction, statement by statement, as in V059: the column
-- is added without a rewrite, and both indexes are built CONCURRENTLY so
-- writes carry on throughout. The workflow id index is rebuilt beside the old
-- one and renamed into its place, so a workflow id is never left unguarded;
-- dropping a half-built one first makes a re-run rebuild it.
ALTER TABLE workflow_executions
    ADD COLUMN IF NOT EXISTS execution_deadline TIMESTAMPTZ;

UPDATE workflow_executions
SET retries_settled_at = COALESCE(completed_at, updated_at),
    next_retry_at = NULL
WHERE status = 'timed_out'
  AND next_retry_at IS NOT NULL
  AND retries_settled_at IS NULL;

DROP INDEX CONCURRENTLY IF EXISTS uq_workflow_executions_open_workflow_id_v067;

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS uq_workflow_executions_open_workflow_id_v067
    ON workflow_executions (
        (COALESCE(organization_id, '00000000-0000-0000-0000-000000000000'::uuid)),
        namespace,
        workflow_id
    )
    WHERE holds_workflow_id
      AND (status NOT IN ('completed', 'failed', 'cancelled', 'terminated',
                          'timed_out', 'restarted_fresh', 'reset')
           OR (retries_settled_at IS NULL AND retry_count < max_retries
               AND (status = 'failed'
                    OR (status = 'timed_out' AND next_retry_at IS NOT NULL))));

DROP INDEX CONCURRENTLY IF EXISTS uq_workflow_executions_open_workflow_id;

ALTER INDEX IF EXISTS uq_workflow_executions_open_workflow_id_v067
    RENAME TO uq_workflow_executions_open_workflow_id;

-- What the retry scheduler scans for, timed-out runs with it (V047).
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_workflow_exec_retry_due
    ON workflow_executions (submitted_at)
    WHERE status IN ('failed', 'timed_out')
      AND retries_settled_at IS NULL
      AND retry_count < max_retries;

DROP INDEX CONCURRENTLY IF EXISTS idx_workflow_exec_retry_eligible;
