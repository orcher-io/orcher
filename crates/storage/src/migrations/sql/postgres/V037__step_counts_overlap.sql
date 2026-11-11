-- Step counts: retried is an attribute, not a partition.
--
-- The original constraint read
--
--     completed_steps + failed_steps + retried_steps <= total_steps
--
-- which only holds if the three are disjoint groups of steps. They are not.
-- `retried_steps` counts steps whose retry_count > 0, and such a step also ends
-- up completed or failed, so it is counted twice. A single step that failed
-- after two retries yields 0 + 1 + 1 = 2 against a total of 1 and the workflow
-- cannot be finalized.
--
-- This never fired because the value feeding retry_count was always 0: the task
-- token carried the outbox delivery counter rather than the execution attempt,
-- so every step recorded attempt 0 and no retries. Recording the real attempt
-- surfaced the latent mismodelling immediately.
--
-- Only the disjoint pair is bounded by the total. `retried_steps` gets its own
-- bound, since it can be at most the number of steps.

ALTER TABLE workflow_executions
    DROP CONSTRAINT IF EXISTS workflow_exec_steps_check;

ALTER TABLE workflow_executions
    ADD CONSTRAINT workflow_exec_steps_check CHECK (
        completed_steps + failed_steps <= total_steps
    );

ALTER TABLE workflow_executions
    ADD CONSTRAINT workflow_exec_retried_steps_check CHECK (
        retried_steps >= 0 AND retried_steps <= total_steps
    );
