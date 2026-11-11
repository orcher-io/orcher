//! Shard → database-pool routing seam for partitioned persistence.
//!
//! A [`ShardPoolResolver`] maps a workflow's routing id to the database pool that
//! holds that workflow's shard. It lets repositories co-locate a workflow's whole
//! state on one pool without this crate depending on the routing/cluster layer: the
//! trait is defined here and implemented by `orcher-cluster`'s routers, then injected
//! into repositories at startup. Absent (the default), a repository uses its own
//! single pool — byte-identical to a non-partitioned deployment.

use sqlx::PgPool;
use uuid::Uuid;

/// Resolves the database pool that owns a workflow's shard.
///
/// The routing inputs are `(namespace_id, workflow_id)`. `workflow_id` is the
/// `workflow_executions.id` UUID — the same key the workflow coordinator routes by,
/// so a workflow's ledger, journal, outbox, and task rows all co-locate on one pool.
/// `namespace_id` selects the namespace's **pool-set**: `None` (or a namespace with
/// no override) resolves to the shared default set, so single-tenant / OSS routing is
/// namespace-agnostic; a namespace pinned to a dedicated pool-set (per-tenant capacity
/// or residency — a Cloud policy) routes into that set. Within the resolved set the
/// pool is chosen by `hash(workflow_id)`.
pub trait ShardPoolResolver: Send + Sync + std::fmt::Debug {
    /// The pool holding `(namespace_id, workflow_id)`'s shard.
    fn pool_for_workflow(&self, namespace_id: Option<Uuid>, workflow_id: Uuid) -> &PgPool;

    /// Every pool across every set, for lookups that aren't keyed by the routing id
    /// (e.g. by `execution_id`) and must scatter-gather across shards. A single-pool
    /// deployment returns just that pool.
    fn all_pools(&self) -> Vec<&PgPool>;

    /// The virtual shard `workflow_id` belongs to.
    ///
    /// Deliberately on the *same* trait as [`Self::pool_for_workflow`] and derived
    /// from the same hash: a row's persisted `shard_id` and the pool it was routed
    /// to can then never disagree. A second, independent derivation — a SQL
    /// generated column, say — could drift from this one and would send work to a
    /// node that does not own it, which is unrecoverable without a backfill.
    ///
    /// Note the routing key is the execution's internal id, not the user-supplied
    /// workflow id; see the callers in the execution and journal repositories.
    fn shard_of(&self, workflow_id: Uuid) -> i32;

    /// Every pool that can hold `shard_id`'s rows — one per pool-set.
    ///
    /// A shard's fencing generation has to be readable where its rows are
    /// written: the write guard checks it inside the writing transaction, and
    /// a check against another database would not be atomic with the write.
    fn pools_for_shard(&self, shard_id: i32) -> Vec<&PgPool>;
}
