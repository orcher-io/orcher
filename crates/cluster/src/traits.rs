//! The cluster seam: the two traits consumers depend on. `dispatch` routes a task
//! to `owner_of(shard_of(wf))`; `core` processes/guards only the shards it `owns`.
//! Concrete implementations live in [`crate::membership`].

use crate::types::{NodeId, ShardId};

/// Which nodes are live, and which one is me.
pub trait Membership: Send + Sync {
    fn this_node(&self) -> &NodeId;
    fn live_nodes(&self) -> Vec<NodeId>;
}

/// Shard → owner resolution, derived from membership. Consumed by `dispatch`
/// (route a task to `owner_of(shard_of(wf))`) and `core` (process/guard only
/// shards this node `owns`).
pub trait ShardMap: Send + Sync {
    fn shard_of(&self, workflow_id: &str) -> ShardId;
    fn owner_of(&self, shard: ShardId) -> Option<NodeId>;
    fn owns(&self, shard: ShardId) -> bool;
}

/// Where a live node can be reached, for relaying a request to the owner of a
/// shard this node does not hold.
///
/// `None` means "cannot be dialed", not only "unknown": a node that registered
/// its bind address (`0.0.0.0:…`) answers `None` too, so a caller never
/// connects to an address that from here means itself.
pub trait PeerDirectory: Send + Sync {
    fn address_of(&self, node: &NodeId) -> Option<String>;
}
