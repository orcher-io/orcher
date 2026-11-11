//! A static (non-gossiping) cluster view: a fixed member set + this node.

use crate::shard::{range_owner, DEFAULT_VSHARDS};
use crate::traits::{Membership, ShardMap};
use crate::types::{NodeId, ShardId};

/// A static (non-gossiping) cluster view: a fixed member set + this node. Used as
/// the single-node default today ("1 member owns all shards"); later the member set
/// is swapped for a live, DB-heartbeat-backed one behind the same traits.
pub struct StaticCluster {
    this: NodeId,
    members: Vec<NodeId>,
    vshards: u32,
    /// Shard databases the ownership ranges must align to. 1 unless partitioned
    /// persistence is on.
    n_pools: usize,
}

impl StaticCluster {
    /// Single-node cluster: this node owns every shard. Behaviour-preserving
    /// default for a non-clustered deployment.
    pub fn single_node(this: NodeId) -> Self {
        StaticCluster {
            members: vec![this.clone()],
            this,
            vshards: DEFAULT_VSHARDS,
            n_pools: 1,
        }
    }

    /// Multi-member view (test/bootstrap). `members` must include `this`.
    pub fn new(this: NodeId, members: Vec<NodeId>, vshards: u32) -> Self {
        StaticCluster {
            this,
            members,
            vshards,
            n_pools: 1,
        }
    }

    /// Align ownership to `n_pools` shard databases, so a node's range stays
    /// inside one of them. Defaults to 1 (single database).
    pub fn with_pools(mut self, n_pools: usize) -> Self {
        self.n_pools = n_pools.max(1);
        self
    }

    pub fn vshards(&self) -> u32 {
        self.vshards
    }
}

impl Membership for StaticCluster {
    fn this_node(&self) -> &NodeId {
        &self.this
    }
    fn live_nodes(&self) -> Vec<NodeId> {
        self.members.clone()
    }
}

impl ShardMap for StaticCluster {
    fn shard_of(&self, workflow_id: &str) -> ShardId {
        ShardId::of(workflow_id, self.vshards)
    }
    fn owner_of(&self, shard: ShardId) -> Option<NodeId> {
        range_owner(shard, &self.members, self.vshards, self.n_pools).cloned()
    }
    fn owns(&self, shard: ShardId) -> bool {
        self.owner_of(shard).as_ref() == Some(&self.this)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_node_owns_all_shards() {
        let c = StaticCluster::single_node(NodeId::new("node-a"));
        for i in (0..DEFAULT_VSHARDS).step_by(97) {
            assert!(c.owns(ShardId(i)));
            assert_eq!(c.owner_of(ShardId(i)), Some(NodeId::new("node-a")));
        }
    }
}
