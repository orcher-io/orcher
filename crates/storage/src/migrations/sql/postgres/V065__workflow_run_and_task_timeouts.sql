-- V065: the run and activation time limits a workflow was started with
--
-- StartWorkflow has always accepted run_timeout and task_timeout, and the
-- engine dropped both. They are now stored on the run and enforced:
--
--   run_timeout_secs   how long one run may last, from its submitted_at. A
--                      successor run (a retry, a restart-fresh, a reset)
--                      carries the limit and starts its own clock.
--   task_timeout_secs  how long one activation of the run may stay claimed
--                      by a worker without an answer before it is handed to
--                      another; overrides the engine's claim timeout.
--
-- NULL means the limit was not asked for. Nullable columns with no default
-- change only the catalogue, as in V060.
SET LOCAL lock_timeout = '5s';

ALTER TABLE workflow_executions
    ADD COLUMN IF NOT EXISTS run_timeout_secs INTEGER,
    ADD COLUMN IF NOT EXISTS task_timeout_secs INTEGER;
