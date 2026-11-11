-- V026: add 'restarted_fresh' to the workflow execution status set.
--
-- A workflow that restarts fresh (orcher's continue-as-new) marks its original
-- execution 'restarted_fresh' — a distinct terminal state: it produced no result
-- of its own, the logical workflow continues in a new execution (linked by
-- workflow_id + parent_execution_id). The engine already used this status value
-- and the RestartedFresh proto/status mapping, but it was missing from the CHECK
-- constraint, so the restart could never persist. This lets the result path
-- recognise the chain and follow it to the final execution's result rather than
-- returning the original's (empty) terminus.

ALTER TABLE workflow_executions DROP CONSTRAINT IF EXISTS workflow_exec_status_check;

ALTER TABLE workflow_executions ADD CONSTRAINT workflow_exec_status_check CHECK (
    status IN (
        'pending', 'queued', 'running', 'paused',
        'completed', 'failed', 'cancelled', 'terminated', 'timed_out',
        'retrying', 'waiting', 'restarted_fresh'
    )
);
