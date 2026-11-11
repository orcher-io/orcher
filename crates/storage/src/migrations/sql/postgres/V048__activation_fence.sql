-- V048: number each activation, and remember which one was last applied
--
-- A workflow completion used to be applied on the strength of its ids alone.
-- The same completion sent twice — a retry after its reply was lost — was
-- applied twice, and a completion from a worker whose claim had timed out was
-- applied over the activation handed to the next worker. Both scheduled tasks
-- again, appended journal entries again, and moved state backwards.
--
-- activation_seq is bumped by every claim that hands the workflow to a worker,
-- and the activation token the worker gets back carries it. A completion is
-- applied only for the activation that is current; completed_activation is the
-- latest one whose completion has been applied, so the same completion sent
-- again is answered without being applied again.
--
-- Rolling out: an engine from before this migration claims without numbering
-- and completes without recording, so to an upgraded engine a workflow it
-- completed looks as if its activation were still in flight, and a wake-up is
-- left for a completion that never comes. That is bounded: an activation
-- counts as in flight only for ORCHER_ACTIVATION_IN_FLIGHT_SECS (default 60)
-- after its claim, and past that a wake-up releases the claim as before. Once
-- every engine is upgraded it no longer arises.
--
-- Adding a NOT NULL column with a constant default and a nullable one are both
-- catalogue-only changes, so neither rewrites workflow_executions.
-- The ALTERs need a brief exclusive lock on each table. Without a bound they
-- queue behind any long-running transaction on it — and every query arriving
-- afterwards queues behind them, which stalls the engine for as long as that
-- transaction lasts. Failing after five seconds instead lets the migration be
-- retried at a quieter moment. SET LOCAL holds for this migration only: the
-- runner executes it inside a transaction.
SET LOCAL lock_timeout = '5s';

ALTER TABLE workflow_executions
    ADD COLUMN IF NOT EXISTS activation_seq BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS completed_activation BIGINT;

-- Marks the task rows a ScheduleTask command created, so that V049 can make
-- them unique per (execution, task, attempt) without having to prove the
-- rows written before it were. Existing rows stay NULL and outside the index;
-- the column has no default, so only the insert that sets it is covered and
-- the retry path's own inserts are left exactly as they were.
ALTER TABLE task_executions
    ADD COLUMN IF NOT EXISTS scheduled_by_command BOOLEAN;
