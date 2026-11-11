-- V053: remember, for a while, which workers have said they are shutting down
--
-- A worker that is shutting down tells the engine so (ShutdownWorker), and from
-- then on its polls are answered with no work. Those polls may land on any
-- replica, not only the one that took the call, so the fact has to be where
-- every replica can see it: a row here, announced to the others as it is
-- written (pg_notify on orcher_worker_shutdown) and read in full by a replica
-- whenever its listener (re)connects, so a missed announcement is recovered.
--
-- A shutdown may name the process that is stopping (worker_instance_id), in
-- which case only that process's polls are refused and a later process polling
-- under the same identity is served. An empty worker_instance_id means the
-- worker named none, and every poll under the identity is refused. The id is
-- part of the key, so two processes sharing an identity that both stop are two
-- rows, and neither undoes the other.
--
-- task_queues limits the shutdown to polls on those queues; empty means every
-- queue. A credential limited to some queues can stop polls on those only.
--
-- Rows expire. Past expires_at a row means nothing, and the write that records
-- a shutdown deletes the expired ones, so the table holds roughly the workers
-- that stopped in the last few minutes.
--
-- organization_id is the nil UUID for a caller that names no organization, as
-- in `workers`, so the key needs no NULL handling: two tenants' workers that
-- happen to share an identity are distinct rows.
CREATE TABLE IF NOT EXISTS worker_shutdowns (
    organization_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000',
    namespace TEXT NOT NULL,
    identity TEXT NOT NULL,
    worker_instance_id TEXT NOT NULL DEFAULT '',
    task_queues TEXT[] NOT NULL DEFAULT '{}',
    shutdown_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (organization_id, namespace, identity, worker_instance_id)
);

CREATE INDEX IF NOT EXISTS idx_worker_shutdowns_expires_at
    ON worker_shutdowns (expires_at);
