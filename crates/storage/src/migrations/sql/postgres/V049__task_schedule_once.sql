-- V049: a task is scheduled by its workflow once
--
-- A ScheduleTask command applied twice inserted a second attempt-1 row beside
-- the first, and the task ran twice. The in-flight check in front of the
-- insert only caught a first row that had not finished yet; once it had, the
-- repeat went straight in. With this index the repeat conflicts and is dropped.
--
-- Partial, over the rows V048 marks: task_executions may already hold such
-- duplicates, and a unique index built over them would fail — and, built
-- concurrently, leave an invalid index behind. No existing row carries the
-- mark, so the build cannot trip on history.
--
-- CONCURRENTLY, as in V042, V046 and V047, so building it on a large table does
-- not block the writes that go on meanwhile; the migration runner executes it
-- outside a transaction.
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS uq_task_executions_scheduled_once
    ON task_executions (execution_id, task_id, attempt_number)
    WHERE scheduled_by_command;
