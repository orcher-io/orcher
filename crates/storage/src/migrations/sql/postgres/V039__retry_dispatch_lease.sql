-- Let the retry sweep move past rows nobody is going to run.
--
-- The dispatcher takes the hundred oldest ready retries each tick and never
-- marks them, because a row leaves 'scheduled' only when a worker claims it.
-- So a retry nobody will ever run — its workflow already finished, its task
-- queue has no workers left — keeps its place at the head of that queue for
-- good. A hundred of them and every new retry on the deployment is starved,
-- with nothing logged above debug: tasks simply stop retrying.
--
-- Found with a two-week-old row at the head of a real queue, holding up a
-- retry scheduled seconds earlier.
--
-- Recording when a row was last handed to the outbox lets the next sweep skip
-- it and reach the rest of the backlog. NULL means never dispatched, which is
-- every row that exists today and reads as "due now".

ALTER TABLE task_executions
    ADD COLUMN IF NOT EXISTS last_dispatched_at TIMESTAMPTZ;

-- The sweep's predicate is (status, attempt_number, scheduled_at) filtered by
-- this column; keeping it in the index means a saturated backlog is still one
-- index scan rather than a heap trawl.
CREATE INDEX IF NOT EXISTS idx_task_exec_retry_sweep
    ON task_executions (scheduled_at, last_dispatched_at)
    WHERE status = 'scheduled' AND attempt_number > 1;
