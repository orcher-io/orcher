//! Cluster storage types: the rows of `cluster_members` and `cluster_shards`
//! (V028), the engine's cluster membership and shard ownership.
//!
//! A deliberately flat model: no parent cluster, no leader. Membership is a
//! live set; ownership is derived from it by contiguous range partitioning in
//! the `orcher-cluster` crate. See `repositories/domains/cluster/membership.rs`.

use orcher_common::types::time::Timestamptz;
use serde::{Deserialize, Serialize};

/// A row in `cluster_members`: one orchestrator node's membership + last heartbeat.
///
/// Liveness is NOT stored — a member is *live* iff `heartbeat_at` is recent
/// (evaluated at read time), so a row never carries a stale "alive" bit.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "postgres", derive(sqlx::FromRow))]
pub struct ClusterMember {
    /// Stable node identity (e.g. `host:port` or a uuid string).
    pub node_id: String,
    /// gRPC address other nodes reach this one at (used for cross-node routing).
    pub address: String,
    /// `active` | `draining` | `offline`.
    pub status: String,
    pub heartbeat_at: Timestamptz,
    pub created_at: Timestamptz,
}

/// A row in `cluster_shards`: the fencing token for one virtual shard.
///
/// `generation` is monotonic and is the *sole write arbiter* — a per-shard write
/// commits only if the writer holds the current generation. `owner_node` is
/// advisory (who last acquired); it may point at a node whose membership row has
/// since expired, which is exactly why the generation, not the owner field, is the
/// authority.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "postgres", derive(sqlx::FromRow))]
pub struct ClusterShard {
    /// Virtual shard id in `[0, VSHARDS)`.
    pub shard_id: i32,
    /// Node that last acquired the shard, or `None` if unowned.
    pub owner_node: Option<String>,
    /// Monotonic fencing token (bumped on every ownership takeover).
    pub generation: i64,
    pub updated_at: Timestamptz,
}
