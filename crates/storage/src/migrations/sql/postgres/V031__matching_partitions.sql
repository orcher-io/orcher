-- V031: matching partitions — the delivery-side ready-queue, partitioned by
-- (namespace, task_queue).
--
-- Persistence is sharded by (namespace, workflow_id): a workflow's ledger, journal,
-- outbox, and its dispatch transfer rows live on that workflow's shard database.
-- Task DELIVERY is a separate, cluster-local concern: a worker polls by
-- (namespace, task_queue), not by workflow, so it must not scan every shard. Each
-- shard's transfer emitter hands a ready task into this table's matching partition;
-- a worker poll consults exactly one partition — O(1), no cross-shard scan. The
-- in-memory sync-match fast path serves the common (worker-waiting) case with zero
-- reads here, so this table is the durable backstop: no waiter parked, crash
-- recovery, and fairness.
--
-- This lives on the coordination database. Sharding the matching partitions
-- themselves (to remove the coordination-DB funnel at very high scale) is a later,
-- additive step behind the MatchingStore trait — it does not change this schema's
-- contract. Design: orcher.io/matching-layer-design.md.
--
-- Creating this table changes zero runtime behavior; the matching path is enabled by
-- a flag in a later increment.

CREATE TABLE matching_partitions (
    -- The task's own id. PRIMARY KEY so the transfer (shard outbox → matching) is
    -- idempotent: a re-delivered transfer row is a no-op via ON CONFLICT.
    task_id UUID PRIMARY KEY,

    -- Partition key: a worker polls (namespace, task_queue). Names, not ids, to match
    -- the poll request and the in-process sync-match registry keying.
    namespace TEXT NOT NULL,
    task_queue TEXT NOT NULL,

    -- Correlation back to the durable record (the workflow on its shard).
    workflow_id UUID NOT NULL,

    -- Everything the worker needs to run the task (mirrors the transfer payload).
    payload JSONB NOT NULL,

    -- ready  → available to claim; claimed → leased to a worker pending ack.
    status VARCHAR(20) NOT NULL DEFAULT 'ready',

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Lease-based claiming: a claim sets an expiry + owner; a crashed worker's lease
    -- expires and the reclaimer returns the row to `ready` (same pattern as the
    -- outbox queue).
    lease_expires_at TIMESTAMPTZ,
    lease_owner TEXT
);

-- The claim query: oldest ready task in a partition. FIFO by created_at; partial
-- index so only ready rows are scanned.
CREATE INDEX idx_matching_ready
    ON matching_partitions (namespace, task_queue, created_at)
    WHERE status = 'ready';

-- Reclaimer: find claimed rows whose lease has expired.
CREATE INDEX idx_matching_expired_leases
    ON matching_partitions (lease_expires_at)
    WHERE status = 'claimed';

COMMENT ON TABLE matching_partitions IS
    'Delivery-side ready-queue partitioned by (namespace, task_queue); the durable backstop for the in-memory sync-match fast path.';
