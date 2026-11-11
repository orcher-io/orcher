//! # Cluster Domain Module
//!
//! Persistence for the engine's cluster: the nodes that are members
//! (`cluster_members`) and the shards each owns under a lease
//! (`cluster_shards`), with the transaction that fences a write to the
//! shard's current owner. Membership and ownership logic lives in the
//! `orcher-cluster` crate.

/// Cluster membership and shard fencing (V028)
pub mod membership;

#[cfg(test)]
mod membership_tests;

// Re-export main types for convenience
pub use membership::{begin_shard_guarded_tx, ClusterMembershipRepository};
