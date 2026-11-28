-- V090: a schedule's fires link the run they started
--
-- A fire recorded the run it started in `schedule_executions.task_id`, by its
-- execution id, rather than in `workflow_execution_id` by its row id, which is
-- the link V024 added and the one the overlap check and the run history read.
-- Nothing found a fire's run, so every overlap policy let every tick through
-- (orcher-io/orcher#46). The fires recorded so far are linked here; a fire
-- whose run no longer exists is left unlinked.
UPDATE schedule_executions se
SET workflow_execution_id = we.id,
    task_id = NULL
FROM workflow_executions we
WHERE se.workflow_execution_id IS NULL
  AND se.task_id IS NOT NULL
  AND we.execution_id = se.task_id::text;

-- A fire recorded with no run was one whose start failed. It was recorded as
-- running, and nothing ever moved it on.
UPDATE schedule_executions
SET status = 'failed',
    completed_at = COALESCE(completed_at, created_at)
WHERE workflow_execution_id IS NULL
  AND task_id IS NULL
  AND status = 'running';
