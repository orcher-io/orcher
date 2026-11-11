-- V078: a run's step results become entries of its journal.
--
-- A task's result, a step's terminal failure and a cancelled step reached the
-- workflow as StepCompleted entries the engine rebuilt from execution_log on
-- every activation, numbered after the journal's own entries. Each activation
-- read the run's whole execution log, and the rebuilt entries' ids moved
-- whenever the journal grew. A run created from now on gets a StepCompleted
-- entry in its journal when each step settles, written with the rest of that
-- step's outcome, and its activations send the journal as it is.
--
-- Rows already here keep the rebuilt entries (FALSE); the engine sets TRUE on
-- the runs it creates, and a reset carries the old run's value.
ALTER TABLE workflow_executions
    ADD COLUMN IF NOT EXISTS step_results_journaled BOOLEAN NOT NULL DEFAULT FALSE;

COMMENT ON COLUMN workflow_executions.step_results_journaled IS
    'Whether the run''s step results are StepCompleted entries of its journal, rather than rebuilt from execution_log on each activation.';

-- A step's result is written once: the write checks for the step's entry
-- first, which this index answers from one entry, whatever the run's length.
-- Built CONCURRENTLY, as V077's are, so journal writes carry on meanwhile; a
-- build that failed left an invalid index, dropped first on a re-run.
DROP INDEX CONCURRENTLY IF EXISTS idx_journal_step_completed;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_journal_step_completed
    ON workflow_execution_journal (workflow_id, execution_id, (payload->>'step_name'))
    WHERE event_type = 'StepCompleted';
