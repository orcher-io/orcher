-- V045: a failure the retry policy gave up on is marked final, explicitly
--
-- A failed run is retried while retry_count < max_retries. When the policy
-- decides a failure is final before the cap is reached — the error type is one
-- it does not retry — nothing on the row said so: the retry scheduler picked
-- the run up on every poll to reach the same decision again, and nothing else
-- could tell the failure was final. Lowering max_retries to record it would
-- lose the policy the run was started with, which a reset carries forward.
--
-- retries_settled_at records the decision instead. NULL on every existing row,
-- which reads as before: such a run is final once its retries are used up.
--
-- While a deploy rolls out, a replica still running the previous engine does
-- not read this column, and its retry scheduler may retry a failure a new
-- replica has already settled. That lasts as long as the rollout.
ALTER TABLE workflow_executions
    ADD COLUMN IF NOT EXISTS retries_settled_at TIMESTAMPTZ;
