-- Add eager claim expiry to task_executions.
--
-- When a task is returned eagerly in a CompleteTaskExecution or CompleteWorkflowExecution
-- response, it is immediately marked as 'started' with a short TTL (eager_claim_expires_at).
-- If the SDK drops the eager task for any reason (crash, network loss, etc.), the reclaimer
-- resets these tasks back to 'scheduled' once the TTL expires, so a normal poller can pick
-- them up.  For tasks claimed through the normal poll path this column is always NULL.

ALTER TABLE task_executions
    ADD COLUMN eager_claim_expires_at TIMESTAMPTZ;

-- Partial index used by the reclaimer to efficiently find expired eager claims.
CREATE INDEX idx_task_executions_eager_claim
    ON task_executions (status, eager_claim_expires_at)
    WHERE eager_claim_expires_at IS NOT NULL;
