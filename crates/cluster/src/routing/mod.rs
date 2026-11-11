//! Shard → database routing — the seam that turns shared-Postgres (all shards in
//! one DB) into partitioned persistence (shards spread across many DBs) *without*
//! rewriting the engine. Consumers acquire a pool through the router instead of
//! holding a single `PgPool`, so going multi-DB later is a routing-table change,
//! not a code change.
//!
//! Design: `orcher.io/cluster-v2-partitioned-persistence.md`.
//!
//! This module is gated on the `storage` feature because it deals in real
//! `PgPool`s. [`SinglePoolRouter`] (every shard → the one pool) is the no-op
//! default, byte-identical to holding a single pool — it lets the seam land ahead
//! of any multi-DB code. [`MultiPoolRouter`] maps contiguous shard ranges to
//! distinct database pools (the actual partitioning). Neither routes anything yet —
//! the coordinator write-path conversion is the next, deliberate step.

use sqlx::PgPool;

use crate::shard::DEFAULT_VSHARDS;
use crate::types::ShardId;

/// Resolves the database pool that owns a shard's data. A workflow lives entirely
/// in one shard, so **all** of its state (executions, journal, timers, outbox) is
/// reachable through `pool_for_workflow` — a single DB, a single transaction.
pub trait ShardRouter: Send + Sync {
    /// The pool holding `shard`'s data.
    fn pool_for_shard(&self, shard: ShardId) -> &PgPool;

    /// The virtual-shard count this router hashes workflows against.
    fn vshards(&self) -> u32;

    /// Every pool behind the router — for scatter-gather reads (list/search across
    /// shards) and running migrations against each database.
    fn all_pools(&self) -> Vec<&PgPool>;

    /// The pool holding `workflow_id`'s data: `pool_for_shard(shard_of(workflow_id))`.
    fn pool_for_workflow(&self, workflow_id: &str) -> &PgPool {
        self.pool_for_shard(ShardId::of(workflow_id, self.vshards()))
    }
}

/// The no-op router: every shard resolves to the same single pool. Behaviour is
/// byte-identical to holding a bare `PgPool`, so it is the default until a
/// multi-database deployment supplies a real [`ShardRouter`].
#[derive(Debug)]
pub struct SinglePoolRouter {
    pool: PgPool,
    vshards: u32,
}

impl SinglePoolRouter {
    /// Route every shard to `pool`, using the default virtual-shard count.
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            vshards: DEFAULT_VSHARDS,
        }
    }

    /// Route every shard to `pool` with an explicit virtual-shard count.
    pub fn with_vshards(pool: PgPool, vshards: u32) -> Self {
        Self { pool, vshards }
    }
}

impl ShardRouter for SinglePoolRouter {
    fn pool_for_shard(&self, _shard: ShardId) -> &PgPool {
        &self.pool
    }
    fn vshards(&self) -> u32 {
        self.vshards
    }
    fn all_pools(&self) -> Vec<&PgPool> {
        vec![&self.pool]
    }
}

/// Which pool (of `n_pools`) owns `shard`: the shard space `[0, vshards)` is split
/// into `n_pools` contiguous, near-equal ranges. Contiguous (vs hash-striped) so a
/// range's data is co-located and a future range can be moved to a new database as
/// one block during rebalancing. Deterministic and pure — the routing invariant.
fn pool_index(shard: ShardId, n_pools: usize, vshards: u32) -> usize {
    debug_assert!(n_pools > 0 && vshards > 0);
    // shard * n / vshards, saturating at the last pool.
    let idx = (shard.0 as u64 * n_pools as u64 / vshards as u64) as usize;
    idx.min(n_pools - 1)
}

/// Routes shard ranges to distinct database pools — the partitioned-persistence
/// router. The shard space is split into contiguous ranges, one per pool, so
/// aggregate write capacity grows with the number of databases. Adding a database
/// (and reassigning a range to it) is the "add capacity" operation; live movement
/// of a range's data between databases (rebalancing) is a later step, made safe by
/// the shard fencing token.
#[derive(Debug)]
pub struct MultiPoolRouter {
    pools: Vec<PgPool>,
    vshards: u32,
}

impl MultiPoolRouter {
    /// Build a router over `pools` (one Postgres per entry), using the default
    /// virtual-shard count. Panics if `pools` is empty — a router needs a database.
    pub fn new(pools: Vec<PgPool>) -> Self {
        Self::with_vshards(pools, DEFAULT_VSHARDS)
    }

    /// Build a router with an explicit virtual-shard count.
    pub fn with_vshards(pools: Vec<PgPool>, vshards: u32) -> Self {
        assert!(
            !pools.is_empty(),
            "MultiPoolRouter requires at least one pool"
        );
        assert!(vshards > 0, "vshards must be > 0");
        Self { pools, vshards }
    }
}

impl ShardRouter for MultiPoolRouter {
    fn pool_for_shard(&self, shard: ShardId) -> &PgPool {
        &self.pools[pool_index(shard, self.pools.len(), self.vshards)]
    }
    fn vshards(&self) -> u32 {
        self.vshards
    }
    fn all_pools(&self) -> Vec<&PgPool> {
        self.pools.iter().collect()
    }
}

// Bridge the routers to the storage-layer resolver seam so repositories can route a
// workflow's writes/reads to its shard's pool without depending on this crate. The
// routing key is the `workflow_executions.id` UUID (stringified), matching how the
// coordinator hashes it. `SinglePoolRouter`/`MultiPoolRouter` are namespace-agnostic
// (one shared set), so they ignore `namespace_id`; [`NamespacePlacement`] adds
// per-namespace pool-sets on top.
impl orcher_storage::sharding::pool_routing::ShardPoolResolver for SinglePoolRouter {
    fn pool_for_workflow(
        &self,
        _namespace_id: Option<uuid::Uuid>,
        workflow_id: uuid::Uuid,
    ) -> &PgPool {
        ShardRouter::pool_for_workflow(self, &workflow_id.to_string())
    }
    fn all_pools(&self) -> Vec<&PgPool> {
        ShardRouter::all_pools(self)
    }

    fn shard_of(&self, workflow_id: uuid::Uuid) -> i32 {
        // Same derivation as pool_for_workflow above — see the trait doc.
        ShardId::of(&workflow_id.to_string(), ShardRouter::vshards(self)).0 as i32
    }

    fn pools_for_shard(&self, shard_id: i32) -> Vec<&PgPool> {
        vec![ShardRouter::pool_for_shard(self, ShardId(shard_id as u32))]
    }
}

impl orcher_storage::sharding::pool_routing::ShardPoolResolver for MultiPoolRouter {
    fn pool_for_workflow(
        &self,
        _namespace_id: Option<uuid::Uuid>,
        workflow_id: uuid::Uuid,
    ) -> &PgPool {
        ShardRouter::pool_for_workflow(self, &workflow_id.to_string())
    }
    fn all_pools(&self) -> Vec<&PgPool> {
        ShardRouter::all_pools(self)
    }

    fn shard_of(&self, workflow_id: uuid::Uuid) -> i32 {
        // Same derivation as pool_for_workflow above — see the trait doc.
        ShardId::of(&workflow_id.to_string(), ShardRouter::vshards(self)).0 as i32
    }

    fn pools_for_shard(&self, shard_id: i32) -> Vec<&PgPool> {
        vec![ShardRouter::pool_for_shard(self, ShardId(shard_id as u32))]
    }
}

/// A set of database pools that a namespace's workflows are distributed across by
/// `hash(workflow_id)` — the unit of persistence **placement**. One `PoolSet` is the
/// shared default; a namespace pinned to its own set gets dedicated capacity.
#[derive(Debug)]
pub struct PoolSet {
    pools: Vec<PgPool>,
    vshards: u32,
}

impl PoolSet {
    /// A pool-set over `pools`, using the default virtual-shard count. Panics if empty.
    pub fn new(pools: Vec<PgPool>) -> Self {
        Self::with_vshards(pools, DEFAULT_VSHARDS)
    }

    /// A pool-set with an explicit virtual-shard count.
    pub fn with_vshards(pools: Vec<PgPool>, vshards: u32) -> Self {
        assert!(!pools.is_empty(), "PoolSet requires at least one pool");
        assert!(vshards > 0, "vshards must be > 0");
        Self { pools, vshards }
    }

    fn pool_for_workflow(&self, workflow_id: &str) -> &PgPool {
        let shard = ShardId::of(workflow_id, self.vshards);
        &self.pools[pool_index(shard, self.pools.len(), self.vshards)]
    }

    fn pools(&self) -> impl Iterator<Item = &PgPool> {
        self.pools.iter()
    }
}

/// Namespace-aware persistence placement: every namespace's workflows live in a
/// [`PoolSet`]. The `default` set holds every namespace unless an `override` pins one
/// to a dedicated set (per-tenant capacity / isolation / residency — a **Cloud**
/// policy; OSS ships only the default, so routing is namespace-agnostic and identical
/// to a bare [`MultiPoolRouter`]). This is the shape the matching layer and future
/// `namespace → cluster` replication also key off. Design:
/// `orcher.io/cluster-v3-namespace-aware-sharding.md`.
#[derive(Debug)]
pub struct NamespacePlacement {
    default_set: PoolSet,
    overrides: std::collections::HashMap<uuid::Uuid, PoolSet>,
}

impl NamespacePlacement {
    /// Placement with a single shared default set over `default_pools` and no
    /// per-namespace overrides — the OSS configuration.
    pub fn new(default_pools: Vec<PgPool>) -> Self {
        Self {
            default_set: PoolSet::new(default_pools),
            overrides: std::collections::HashMap::new(),
        }
    }

    /// Pin `namespace_id` to a dedicated pool-set (a Cloud placement policy).
    pub fn with_override(mut self, namespace_id: uuid::Uuid, pools: Vec<PgPool>) -> Self {
        self.overrides.insert(namespace_id, PoolSet::new(pools));
        self
    }

    fn set_for(&self, namespace_id: Option<uuid::Uuid>) -> &PoolSet {
        namespace_id
            .and_then(|ns| self.overrides.get(&ns))
            .unwrap_or(&self.default_set)
    }
}

impl orcher_storage::sharding::pool_routing::ShardPoolResolver for NamespacePlacement {
    fn pool_for_workflow(
        &self,
        namespace_id: Option<uuid::Uuid>,
        workflow_id: uuid::Uuid,
    ) -> &PgPool {
        self.set_for(namespace_id)
            .pool_for_workflow(&workflow_id.to_string())
    }
    fn all_pools(&self) -> Vec<&PgPool> {
        let mut pools: Vec<&PgPool> = self.default_set.pools().collect();
        for set in self.overrides.values() {
            pools.extend(set.pools());
        }
        pools
    }

    fn shard_of(&self, workflow_id: uuid::Uuid) -> i32 {
        // Same derivation as pool_for_workflow above — see the trait doc.
        ShardId::of(&workflow_id.to_string(), ShardRouter::vshards(self)).0 as i32
    }

    fn pools_for_shard(&self, shard_id: i32) -> Vec<&PgPool> {
        // The shard's pool in the default set and in every pinned set: a
        // namespace pinned elsewhere writes the same shard id to its own set.
        let shard = ShardId(shard_id as u32);
        std::iter::once(&self.default_set)
            .chain(self.overrides.values())
            .map(|set| &set.pools[pool_index(shard, set.pools.len(), set.vshards)])
            .collect()
    }
}

// The coordinator's write path uses `ShardRouter` (no namespace in scope yet), so it
// routes the default set. This agrees with the resolver for every namespace on the
// default set — i.e. all of OSS. Threading `namespace_id` through the coordinator is
// required only once dedicated pool-sets exist (Cloud) and is tracked as a follow-up.
impl ShardRouter for NamespacePlacement {
    fn pool_for_shard(&self, shard: ShardId) -> &PgPool {
        let set = &self.default_set;
        &set.pools[pool_index(shard, set.pools.len(), set.vshards)]
    }
    fn vshards(&self) -> u32 {
        self.default_set.vshards
    }
    fn all_pools(&self) -> Vec<&PgPool> {
        orcher_storage::sharding::pool_routing::ShardPoolResolver::all_pools(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn test_pool() -> PgPool {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgresql://localhost/orcher_cluster_test".to_string());
        PgPool::connect(&url).await.expect("connect")
    }

    #[tokio::test]
    #[ignore] // requires DATABASE_URL
    async fn single_pool_routes_every_shard_and_workflow_to_the_one_pool() {
        let router = SinglePoolRouter::new(test_pool().await);
        let base = router.pool_for_shard(ShardId(0));
        // Every shard → the same pool (pointer-identical).
        for i in [1u32, 7, 100, 4095] {
            assert!(std::ptr::eq(router.pool_for_shard(ShardId(i)), base));
        }
        // pool_for_workflow resolves via shard_of → the same pool.
        for wf in ["ord_1", "abc-123", ""] {
            assert!(std::ptr::eq(router.pool_for_workflow(wf), base));
        }
        assert_eq!(router.all_pools().len(), 1);
    }

    // ── pure range-mapping logic (no database) ────────────────────────────────

    #[test]
    fn pool_index_splits_the_shard_space_into_contiguous_equal_ranges() {
        let vshards = 4096;
        // 4 pools → 4 contiguous ranges of 1024.
        assert_eq!(pool_index(ShardId(0), 4, vshards), 0);
        assert_eq!(pool_index(ShardId(1023), 4, vshards), 0);
        assert_eq!(pool_index(ShardId(1024), 4, vshards), 1);
        assert_eq!(pool_index(ShardId(2048), 4, vshards), 2);
        assert_eq!(pool_index(ShardId(3072), 4, vshards), 3);
        // The very last shard stays in-bounds (saturates at the last pool).
        assert_eq!(pool_index(ShardId(vshards - 1), 4, vshards), 3);
        // One pool → everything routes to it.
        for s in [0u32, 1, 2048, vshards - 1] {
            assert_eq!(pool_index(ShardId(s), 1, vshards), 0);
        }
    }

    #[test]
    fn pool_index_covers_every_pool_and_never_out_of_bounds() {
        let vshards = 4096;
        for n in [1usize, 2, 3, 5, 8] {
            let mut seen = vec![false; n];
            for s in 0..vshards {
                let idx = pool_index(ShardId(s), n, vshards);
                assert!(idx < n, "index {idx} out of bounds for {n} pools");
                seen[idx] = true;
            }
            assert!(
                seen.iter().all(|&b| b),
                "every one of {n} pools owns some shard"
            );
        }
    }

    #[tokio::test]
    #[ignore] // requires DATABASE_URL
    async fn multi_pool_distributes_shards_across_pools() {
        // Two distinct pool objects (same DB is fine — we assert on routing, not data).
        let router = MultiPoolRouter::new(vec![test_pool().await, test_pool().await]);
        let lo = router.pool_for_shard(ShardId(0));
        let hi = router.pool_for_shard(ShardId(DEFAULT_VSHARDS - 1));
        // Low and high shards land on different pools.
        assert!(
            !std::ptr::eq(lo, hi),
            "low and high shards must route to different pools"
        );
        assert!(std::ptr::eq(router.pool_for_shard(ShardId(1)), lo));
        assert!(std::ptr::eq(
            router.pool_for_shard(ShardId(DEFAULT_VSHARDS - 2)),
            hi
        ));
        assert_eq!(router.all_pools().len(), 2);
    }
}
