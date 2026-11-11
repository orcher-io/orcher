//! Shard mapping and ownership: how a workflow id becomes a [`ShardId`], and how
//! a shard resolves to its owning node.
//!
//! - [`hash`] — the stable, cross-node/version hash primitives (private).
//! - [`id`] — `ShardId::of` (workflow → shard) and [`DEFAULT_VSHARDS`].
//! - [`ranges`] — [`owned_ranges`] (member set → the contiguous range a node
//!   owns) and [`range_owner`] (shard → its owning node). One assignment, read
//!   in both directions, so ownership cannot be answered two ways that differ.
//!
//! [`ShardId`]: crate::types::ShardId

mod hash;
mod id;
mod ranges;

pub use id::DEFAULT_VSHARDS;
pub use ranges::{owned_ranges, range_owner, ShardRange};
