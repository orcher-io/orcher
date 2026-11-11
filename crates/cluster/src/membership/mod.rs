//! Concrete cluster views — implementations of [`Membership`] and [`ShardMap`].
//!
//! - [`StaticCluster`] — a fixed member set (the single-node / test default).
//! - [`DbCluster`] — a live view whose member set comes from a
//!   [`ClusterStore`](crate::store::ClusterStore), refreshed on a cadence the
//!   caller drives.
//! - [`ClusterClaimScope`] — the adapter that exposes a live view's owned shard
//!   ranges to the storage layer's claim filter (feature `storage`).
//!
//! [`Membership`]: crate::traits::Membership
//! [`ShardMap`]: crate::traits::ShardMap

#[cfg(feature = "storage")]
mod claim_scope;
mod db;
mod static_set;

#[cfg(feature = "storage")]
pub use claim_scope::ClusterClaimScope;
pub use db::DbCluster;
pub use static_set::StaticCluster;
