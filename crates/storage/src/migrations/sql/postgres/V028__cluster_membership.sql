-- V028: cluster membership + shard fencing (the HRW-ownership + fencing substrate).
--
-- This is the storage foundation for horizontal scaling (design:
-- orcher.io/cluster-crate.md §4, §7). It is deliberately a NEW, flat model,
-- distinct from the legacy `clusters`/`nodes` tables (V009) which encode a
-- coordinator/leader-election design the rendezvous-hash model explicitly avoids
-- ("no coordinator, no election"). The legacy tables are dead scaffolding and
-- will be consolidated out in a later cleanup.
--
-- Two tables, each doing exactly one job:
--   cluster_members — who is in the cluster and alive (DB-heartbeat membership).
--   cluster_shards  — the fencing token per virtual shard (the write arbiter).
--
-- Nothing reads these yet: this migration only creates the substrate. Ownership
-- (rendezvous hashing) is computed in the `orcher-cluster` crate from the live
-- member set; the generation guard on the engine write path lands in a later
-- increment. Creating the tables changes zero runtime behavior.

-- ── Membership ──────────────────────────────────────────────────────────────
-- A node is a member the moment it registers; it is *live* if its heartbeat is
-- recent (now() - heartbeat_at < K*T, evaluated at read time — NOT stored, so
-- liveness never goes stale in the row). No parent cluster, no leader: the flat
-- live set feeds the rendezvous-hash ownership map directly.
CREATE TABLE IF NOT EXISTS cluster_members (
    node_id      TEXT PRIMARY KEY,                       -- stable NodeId (host:port or uuid)
    address      TEXT        NOT NULL,                   -- gRPC address (for cross-node routing)
    status       TEXT        NOT NULL DEFAULT 'active',  -- active | draining | offline
    heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Liveness scans filter by recency of heartbeat, so index it.
CREATE INDEX IF NOT EXISTS idx_cluster_members_heartbeat
    ON cluster_members (heartbeat_at);

-- ── Shard fencing ───────────────────────────────────────────────────────────
-- Each virtual shard has a monotonic `generation` — the fencing token. It is the
-- SOLE write arbiter: a per-shard write commits iff the writer holds the current
-- generation. This decouples "who thinks it owns the shard" (membership, which can
-- be wrong) from "who is allowed to write" (this token, which cannot). A node that
-- lost ownership has its writes rejected by the DB even if it still believes it owns
-- the shard — the split-brain safety net (a monotonic per-shard fencing token).
--
-- Rows are created lazily on first acquire (no need to seed all VSHARDS). A missing
-- row = unowned, generation 0. Deliberately NO foreign key to cluster_members:
-- ownership + generation must survive membership churn (stale owners are handled by
-- the acquire predicate's liveness check, not by row deletion/cascade).
CREATE TABLE IF NOT EXISTS cluster_shards (
    shard_id   INTEGER     PRIMARY KEY,                  -- virtual shard in [0, VSHARDS)
    owner_node TEXT,                                     -- NULL = unowned
    generation BIGINT      NOT NULL DEFAULT 0,           -- monotonic fencing token
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- A node enumerates the shards it owns (to know what to process); index the owner.
CREATE INDEX IF NOT EXISTS idx_cluster_shards_owner
    ON cluster_shards (owner_node);
