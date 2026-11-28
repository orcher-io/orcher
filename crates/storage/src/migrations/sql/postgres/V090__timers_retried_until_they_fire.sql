-- V090: a timer's attempts are not capped
--
-- A timer whose fire fails is put back to be retried after a backoff, until
-- it fires: its workflow is parked on it, and what makes a fire fail is the
-- engine's own storage, not anything the workflow did (orcher-io/orcher#45).
-- The check held a timer's attempts to `max_retries + 1`, so a timer that had
-- failed that often would make every claim of the batch it was in fail.
--
-- Attempts are still never negative. Dropping the old check and adding the
-- new one take an exclusive lock on the queue only for a moment, as neither
-- reads the table; the new check is then validated under a lock that blocks
-- no read or write.
ALTER TABLE timer_queue DROP CONSTRAINT IF EXISTS timer_queue_attempt_check;
ALTER TABLE timer_queue
    ADD CONSTRAINT timer_queue_attempt_check CHECK (attempt_count >= 0) NOT VALID;
ALTER TABLE timer_queue VALIDATE CONSTRAINT timer_queue_attempt_check;
