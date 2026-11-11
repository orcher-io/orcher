-- V062: a cancellation is a request the workflow can clean up after
--
-- Cancelling a running workflow used to write its final status directly, so
-- the workflow never ran again and could not undo what it had done. A
-- cancellation of a running workflow is now recorded as a request: a journal
-- entry the workflow observes, and these columns on the run. The run stays
-- running until the workflow ends itself, and its status keeps meaning what
-- it meant, so nothing that reads status changes.
--
--   cancel_requested_at  when the request was made; set once.
--   cancel_reason        the reason given with it.
--   cancel_deadline      when the run is terminated if it has not ended;
--                        NULL means no limit.
--
-- Nullable columns with no default change only the catalogue, as in V060.
SET LOCAL lock_timeout = '5s';

ALTER TABLE workflow_executions
    ADD COLUMN IF NOT EXISTS cancel_requested_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS cancel_reason TEXT,
    ADD COLUMN IF NOT EXISTS cancel_deadline TIMESTAMPTZ;
