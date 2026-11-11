-- V027: add 'shard_id' to timer_queue.
--
-- The TimerTask row struct (schema/types/coordination.rs) carries a nullable
-- shard_id, matching every other sharded table (workflow_executions,
-- workflow_execution_journal, child_workflow_executions, task_executions). But
-- timer_queue (created in V014) never got the column, so the timer processor's
-- `claim_ready_tasks` (RETURNING * -> TimerTask) failed to deserialize with
-- "no column found for name: shard_id" on every claim — no scheduled timer could
-- ever be claimed or fired. Add the column (nullable; timers don't shard today,
-- so it stays NULL) to make the queue consistent with the rest of the schema.

ALTER TABLE timer_queue ADD COLUMN IF NOT EXISTS shard_id INTEGER;
