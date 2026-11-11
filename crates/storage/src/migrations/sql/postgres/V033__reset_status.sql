-- V033: add 'reset' to the workflow execution status set.
--
-- Resetting a workflow marks the original execution 'reset' — a distinct
-- terminal state: it produced no result of its own, and the logical workflow
-- continues in a new execution that replays the journal up to the target event
-- (linked by workflow_id + parent_execution_id, as a fresh restart is).
--
-- The engine already wrote this status value and all three SDKs document it,
-- but it was missing from the CHECK constraint, so the status update failed and
-- the reset could never persist. Same shape as V026 for 'restarted_fresh'.

ALTER TABLE workflow_executions DROP CONSTRAINT IF EXISTS workflow_exec_status_check;

ALTER TABLE workflow_executions ADD CONSTRAINT workflow_exec_status_check CHECK (
    status IN (
        'pending', 'queued', 'running', 'paused',
        'completed', 'failed', 'cancelled', 'terminated', 'timed_out',
        'retrying', 'waiting', 'restarted_fresh', 'reset'
    )
);
