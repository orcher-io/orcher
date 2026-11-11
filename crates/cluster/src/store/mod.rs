//! The storage seam behind live membership + shard fencing. [`DbCluster`] and
//! [`ShardOwnership`] read/write through this trait; the Postgres implementation
//! lives in [`pg`] (behind the `storage` feature) and wraps `orcher-storage`'s
//! `ClusterMembershipRepository`.
//!
//! Keeping this a trait means the ownership logic is testable with an in-memory
//! fake — no database — while the real deployment binds it to Postgres.
//!
//! [`DbCluster`]: crate::membership::DbCluster
//! [`ShardOwnership`]: crate::fencing::ShardOwnership

// The Postgres `ClusterStore` impl for `orcher-storage`'s repository.
#[cfg(feature = "storage")]
mod pg;

use crate::types::{Generation, NodeId, ShardId};

/// Result of a store operation. A boxed error keeps this crate free of any
/// storage-specific error type (the adapter maps its errors into this).
pub type StoreResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Persistence the cluster runtime needs: membership (register/heartbeat, read the
/// live set) and shard fencing (acquire/renew/release a shard's generation). The
/// Postgres implementation wraps `orcher-storage`'s `ClusterMembershipRepository`.
#[async_trait::async_trait]
pub trait ClusterStore: Send + Sync {
    /// Register (or refresh) this node with its reachable address, stamping a
    /// fresh heartbeat. Called on join and idempotently on every heartbeat tick.
    async fn register(&self, node: &NodeId, address: &str) -> StoreResult<()>;

    /// Refresh only this node's heartbeat (the steady-state tick).
    async fn heartbeat(&self, node: &NodeId) -> StoreResult<()>;

    /// The live member set: nodes whose heartbeat is within `stale_after_secs`,
    /// each with the address it registered. This — not a raw table read — is
    /// what feeds the ownership map, and the address is how another node reaches
    /// the owner of a shard it does not hold.
    async fn live_members(&self, stale_after_secs: i64) -> StoreResult<Vec<(NodeId, String)>>;

    /// Acquire (or reclaim) `shard` for `node`, bumping and returning the new
    /// fencing generation — or `None` if a *live* other node still owns it. The
    /// returned generation is the token every subsequent per-shard write presents.
    async fn acquire_shard(
        &self,
        shard: ShardId,
        node: &NodeId,
        stale_after_secs: i64,
    ) -> StoreResult<Option<Generation>>;

    /// Renew ownership of `shard` at `held` without bumping the generation (the
    /// steady-state keep-alive). Returns `false` if this node has been fenced out.
    async fn renew_shard(
        &self,
        shard: ShardId,
        node: &NodeId,
        held: Generation,
    ) -> StoreResult<bool>;

    /// Relinquish `shard` (graceful drain) — only if this node still holds it at
    /// `held`. Returns `false` if it no longer does.
    async fn release_shard(
        &self,
        shard: ShardId,
        node: &NodeId,
        held: Generation,
    ) -> StoreResult<bool>;
}
