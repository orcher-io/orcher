-- orcher: outside a transaction
-- V061: find the claims a worker process holds
--
-- Read when a process stops being seen, to release what it held (V060).
-- Partial over exactly the claims that release takes, as in V052: a workflow
-- claim whose activation is still out — a parked one, its activation
-- completed, does not wait on its worker and is never released for it — and
-- a task attempt still running. Ordered by claim time within a process, which
-- the release bounds.
--
-- CONCURRENTLY, as in V052 and V056, so building them on large tables does not
-- block the writes that go on meanwhile; the runner executes this outside a
-- transaction, statement by statement.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_workflow_executions_claimed_by_instance
    ON workflow_executions (claimed_by_instance, claimed_at)
    WHERE claimed_by IS NOT NULL
      AND claimed_by_instance IS NOT NULL
      AND activation_seq > COALESCE(completed_activation, 0);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_task_executions_started_by_instance
    ON task_executions (worker_instance_id)
    WHERE status = 'started' AND worker_instance_id IS NOT NULL;
