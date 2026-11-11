//! Cluster membership and shard ownership for ORCHER horizontal scaling.
//!
//! A pure ownership core — shard identity, a stable hash, contiguous-range
//! ownership, and the trait seam that `dispatch`/`core` consume — plus a
//! live, database-backed view of it. With clustering on, a node writes a
//! shard's rows only while it holds that shard's current `generation`.
//!
//! Others depend on `cluster`; `cluster` never depends on `core`/`dispatch`.
//! The ownership math and traits are dependency-free; the Postgres-backed view is
//! gated behind the `storage` feature so pure consumers stay light.
//!
//! ## Module layout
//! - [`types`] — the identity types (`NodeId`, `ShardId`, `Generation`).
//! - [`traits`] — the seam consumers depend on (`Membership`, `ShardMap`,
//!   `PeerDirectory`).
//! - [`address`] — whether a registered peer address can actually be dialed.
//! - [`shard`] — how a workflow maps to a shard and a shard to its owner
//!   (stable hash, `ShardId::of`, contiguous-range ownership read both ways).
//! - [`store`] — the [`ClusterStore`] seam behind live membership + shard fencing
//!   (trait + Postgres adapter, the latter gated on `storage`).
//! - [`membership`] — concrete cluster views implementing the traits
//!   (`StaticCluster`; `DbCluster` over a `ClusterStore`).
//! - [`fencing`] — [`ShardOwnership`], this node's held shards + generations
//!   (lazy acquire / renew / release); the piece the write-path guard consults.
//! - [`routing`] — the [`ShardRouter`](routing::ShardRouter) seam mapping a shard
//!   to the database pool that holds its data (gated on `storage`).

#![forbid(unsafe_code)]

pub mod address;
pub mod fencing;
pub mod membership;
pub mod shard;
pub mod store;
pub mod traits;
pub mod types;

// The shard→pool routing seam deals in real `PgPool`s (needs the `storage` feature).
#[cfg(feature = "storage")]
pub mod routing;

pub use fencing::ShardOwnership;
#[cfg(feature = "storage")]
pub use membership::ClusterClaimScope;
pub use membership::{DbCluster, StaticCluster};
pub use shard::{owned_ranges, range_owner, ShardRange, DEFAULT_VSHARDS};
pub use store::{ClusterStore, StoreResult};
pub use traits::{Membership, PeerDirectory, ShardMap};
pub use types::{Generation, NodeId, ShardId};

#[cfg(feature = "storage")]
pub use routing::{MultiPoolRouter, NamespacePlacement, PoolSet, ShardRouter, SinglePoolRouter};
