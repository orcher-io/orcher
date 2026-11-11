-- V051: remember which engine handed each claim out
--
-- A claim records the worker it was handed to, not the engine that handed it.
-- When an engine died, the activations it had handed out and whose replies
-- never reached their workers, and the executions it had claimed into its
-- prefetch buffer and not handed to anyone yet, all stayed claimed until the
-- claim timeout — five minutes in production — although every other engine
-- knew within seconds, from its missed heartbeats, that the engine was gone.
-- Every claim now names the engine that made it, so a surviving engine can
-- release exactly the ones the dead engine left behind.
--
-- A task has the same gap in a narrower window. A poll claims the task's
-- matching row and then marks the task started; an engine that died between
-- the two left the row claimed and the task scheduled, and nothing returns a
-- claimed matching row, so the task was never delivered. The matching claim
-- now records the engine and the time too, and only a task still scheduled
-- is offered again — one that was started may be running on a worker.
--
-- NULL on every row an engine without clustering claims, and on every claim
-- made before this migration; neither is ever released on its account.
--
-- A nullable column with no default is a catalogue-only change and rewrites
-- neither table. The locks the ALTERs need are bounded, as in V048, so they
-- fail and can be retried rather than stall the engine behind a long
-- transaction. SET LOCAL holds for this migration only: the runner executes
-- it inside a transaction.
SET LOCAL lock_timeout = '5s';

ALTER TABLE workflow_executions
    ADD COLUMN IF NOT EXISTS claimed_on_node TEXT;

ALTER TABLE matching_partitions
    ADD COLUMN IF NOT EXISTS claimed_on_node TEXT,
    ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ;
