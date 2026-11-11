-- V060: know which worker process holds each claim, and when it was last seen
--
-- A worker process that died while it held an activation left the workflow
-- claimed until the claim timed out — five minutes in production — although
-- the engine stopped hearing from the process within seconds. Engine deaths
-- were already handled that way (V051); a worker's were not, because nothing
-- recorded which process a claim was handed to. claimed_by names the poller,
-- and a poller's name is often reused by the next process (a fixed identity
-- restarted in the same pod), so it cannot say whether the holder is gone.
--
-- Every poll from a current SDK names its process (worker_instance_id), so a
-- claim now records it: claimed_by_instance on the workflow execution, and
-- worker_instance_id on the task attempt it starts. worker_instances records,
-- per organization and namespace, when each process was last seen polling;
-- every engine writes the processes it sees every few seconds. A process not
-- seen for long enough has its outstanding claims released by whichever
-- engine notices first. Rows are deleted once their claims would have timed
-- out anyway, so the table holds roughly the processes of the last minutes.
--
-- organization_id is the nil UUID for a poll that names no organization, as
-- in worker_shutdowns, so the key needs no NULL handling: two tenants'
-- processes that happen to send the same instance id are distinct rows.
--
-- Nullable columns with no default change only the catalogue. The locks the
-- ALTERs take are bounded, as in V051; SET LOCAL holds for this migration
-- only, which the runner executes inside a transaction. The indexes the
-- release reads are built concurrently, in V061.
SET LOCAL lock_timeout = '5s';

ALTER TABLE workflow_executions
    ADD COLUMN IF NOT EXISTS claimed_by_instance TEXT;

ALTER TABLE task_executions
    ADD COLUMN IF NOT EXISTS worker_instance_id TEXT;

CREATE TABLE IF NOT EXISTS worker_instances (
    organization_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000',
    namespace TEXT NOT NULL,
    worker_instance_id TEXT NOT NULL,
    identity TEXT NOT NULL DEFAULT '',
    last_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (organization_id, namespace, worker_instance_id)
);

CREATE INDEX IF NOT EXISTS idx_worker_instances_last_seen
    ON worker_instances (last_seen);
