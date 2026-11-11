-- V058: the heartbeat timeout a task's attempt runs under when it sets none
--
-- A worker that heartbeats every task on its own says so on its poll. A task
-- it takes that sets no heartbeat timeout of its own is given the engine's
-- default for that attempt, recorded here, and is timed out once that long
-- passes without a heartbeat, so a task whose worker died or whose poll
-- response was lost is retried rather than left started for good. NULL for an
-- attempt taken by a worker that does not heartbeat on its own, which is timed
-- out only by timeouts of its own, as before.
--
-- A column of its own rather than a value written into heartbeat_timeout_secs:
-- that one is the task's declaration and is copied onto its retries, which may
-- be taken by a worker that does not heartbeat.
--
-- Nullable with no default, so adding it rewrites nothing.
ALTER TABLE task_executions
    ADD COLUMN IF NOT EXISTS auto_heartbeat_timeout_secs INTEGER;
