-- V077: answer a task's completion checks by index.
--
-- Completing a task first checks whether the run's journal already holds a
-- TaskCompleted for it (a task re-dispatched during failover can report twice).
-- The check matches the task's id inside the entry's payload, which no index
-- covered, so every completion read the run's whole journal: a run of n tasks
-- read on the order of n^2 journal rows to complete them (339,302 blocks for a
-- 1,000-task run in the benchmark). This partial index answers it from one
-- entry, whatever the run's length.
--
-- Built CONCURRENTLY, so writes to the journal carry on while it builds; a
-- build that fails leaves an invalid index, which is dropped first on a re-run.
DROP INDEX CONCURRENTLY IF EXISTS idx_journal_task_completed;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_journal_task_completed
    ON workflow_execution_journal (workflow_id, execution_id, (payload->>'task_id'))
    WHERE event_type = 'TaskCompleted';

-- When a task finishes, its workflow is woken if none of its tasks is still
-- unfinished. That was counted over every task of the workflow, on every
-- completion (298,670 blocks for a 1,000-task run). This partial index holds
-- only unfinished tasks, so the question is one seek. It replaces the partial
-- index for 'scheduled' and 'running': no task has had the status 'running'
-- since 'started' replaced it, so that index answered nothing.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_task_executions_open
    ON task_executions (workflow_id)
    WHERE status IN ('scheduled', 'started');

DROP INDEX CONCURRENTLY IF EXISTS idx_task_executions_status_v032;
