-- V087: where in its journal each activation reads to
--
-- claimed_through_entry: the run's last journal entry when its current
-- activation was claimed. The activation carries the journal up to that
-- entry and no further; an outcome recorded after the claim comes with the
-- next activation, as its deferred wake-up already ensured.
--
-- observed_through_entry: the same, for the last activation whose
-- completion was applied. Every outcome at or before it was observed by the
-- run in an earlier activation, which a worker reports to the code it re-runs.
--
-- NULL: not known, as for every row before this column.
ALTER TABLE workflow_executions
    ADD COLUMN IF NOT EXISTS claimed_through_entry BIGINT,
    ADD COLUMN IF NOT EXISTS observed_through_entry BIGINT;
