-- Every ended run carries the time it ended (orcher#276). Cancellation used
-- to record only cancelled_at, so runs cancelled before this have no
-- completed_at; give them the time they were cancelled.
UPDATE workflow_executions
SET completed_at = cancelled_at
WHERE status = 'cancelled'
  AND completed_at IS NULL
  AND cancelled_at IS NOT NULL;
