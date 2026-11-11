//! A live, database-backed cluster view: the ownership map derives from the
//! current live member set (via [`ClusterStore`]), refreshed on a cadence the
//! caller drives.
//!
//! The [`Membership`]/[`ShardMap`] trait methods are synchronous, but reading the
//! live set is a database call — so [`DbCluster`] serves the traits from a cached
//! **snapshot** and exposes async `register`/`heartbeat`/`refresh` for the caller
//! (the orchestrator) to drive on its own timer. Ownership is computed from
//! whatever the last `refresh` observed, so all nodes converge on the same map as
//! their snapshots converge on the same live set.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::shard::{range_owner, DEFAULT_VSHARDS};
use crate::store::{ClusterStore, StoreResult};
use crate::traits::{Membership, PeerDirectory, ShardMap};
use crate::types::{NodeId, ShardId};

/// One observation of the live set: who is in it, and where each can be
/// reached. Kept together so a reader never pairs one refresh's members with
/// another refresh's addresses.
#[derive(Default)]
struct Snapshot {
    members: Arc<Vec<NodeId>>,
    addresses: HashMap<NodeId, String>,
}

/// A live cluster view backed by a [`ClusterStore`]. Cheap to clone the handle is
/// not required — hold it behind an `Arc` if shared.
pub struct DbCluster {
    this: NodeId,
    store: Arc<dyn ClusterStore>,
    vshards: u32,
    /// Shard databases the ownership ranges must align to, so a node's range
    /// stays inside one of them. 1 unless partitioned persistence is on.
    n_pools: usize,
    stale_after_secs: i64,
    /// Cached live set (and each member's registered address) the sync trait
    /// methods read. Replaced wholesale by `refresh`.
    snapshot: RwLock<Snapshot>,
}

impl DbCluster {
    /// Build a live view for `this` node. The initial snapshot optimistically
    /// contains only `this` (a freshly-started node considers itself the sole
    /// member until its first `refresh` learns the real live set).
    pub fn new(
        this: NodeId,
        store: Arc<dyn ClusterStore>,
        vshards: u32,
        stale_after_secs: i64,
    ) -> Self {
        let snapshot = RwLock::new(Snapshot {
            members: Arc::new(vec![this.clone()]),
            addresses: HashMap::new(),
        });
        DbCluster {
            this,
            store,
            vshards,
            n_pools: 1,
            stale_after_secs,
            snapshot,
        }
    }

    /// Convenience constructor with the default virtual-shard count.
    pub fn with_defaults(
        this: NodeId,
        store: Arc<dyn ClusterStore>,
        stale_after_secs: i64,
    ) -> Self {
        Self::new(this, store, DEFAULT_VSHARDS, stale_after_secs)
    }

    /// Align ownership to `n_pools` shard databases. Defaults to 1; set this when
    /// partitioned persistence is on, or a node's range can straddle two
    /// databases and its claims fan out across both.
    pub fn with_pools(mut self, n_pools: usize) -> Self {
        self.n_pools = n_pools.max(1);
        self
    }

    /// Register (or refresh) this node's membership + heartbeat with its address.
    /// Call on startup and idempotently on every heartbeat tick.
    pub async fn register(&self, address: &str) -> StoreResult<()> {
        self.store.register(&self.this, address).await
    }

    /// Steady-state heartbeat (keeps this node in the live set without re-sending
    /// the address).
    pub async fn heartbeat(&self) -> StoreResult<()> {
        self.store.heartbeat(&self.this).await
    }

    /// Re-read the live member set and swap it into the snapshot the trait methods
    /// serve. The caller drives this on the membership cadence (alongside heartbeat).
    ///
    /// The DB live set is the source of truth — this node is included iff its own
    /// heartbeat is still fresh there, so a node that stopped heartbeating correctly
    /// stops owning shards (it will fail to appear in its own refresh).
    pub async fn refresh(&self) -> StoreResult<()> {
        let live = self.store.live_members(self.stale_after_secs).await?;
        let mut members: Vec<NodeId> = live.iter().map(|(node, _)| node.clone()).collect();
        members.sort();
        members.dedup();
        let addresses = live.into_iter().collect();
        *self.snapshot.write().unwrap() = Snapshot {
            members: Arc::new(members),
            addresses,
        };
        Ok(())
    }

    /// The current cached live set (cheap clone of the `Arc`).
    fn members(&self) -> Arc<Vec<NodeId>> {
        self.snapshot.read().unwrap().members.clone()
    }

    /// Where `node` can be dialed, as it registered in the last refresh.
    ///
    /// `None` if the node was not live at the last refresh, or registered an
    /// address no peer can connect to — a bind address such as `0.0.0.0:50051`,
    /// which is what every node registered before an advertised address could
    /// be configured. Such a node is live and owns shards; it just cannot be
    /// reached, and the caller must handle the request itself.
    pub fn address_of(&self, node: &NodeId) -> Option<String> {
        self.snapshot
            .read()
            .unwrap()
            .addresses
            .get(node)
            .and_then(|address| crate::address::normalize(address))
    }
}

impl PeerDirectory for DbCluster {
    fn address_of(&self, node: &NodeId) -> Option<String> {
        DbCluster::address_of(self, node)
    }
}

impl DbCluster {
    /// The contiguous shard ranges this node owns under the current snapshot.
    ///
    /// The claim-side view of ownership, and the same assignment
    /// [`ShardMap::owns`] answers point lookups from — a node cannot filter its
    /// claims by one set of shards while fencing acquires another.
    ///
    /// Empty while this node is not in the live set (it has not yet registered,
    /// or its heartbeat went stale). Callers must read that as *ownership
    /// unknown*, not *owns nothing*: see the fail-open note on `ClaimScope`.
    pub fn owned_ranges(&self) -> Vec<crate::shard::ShardRange> {
        crate::shard::owned_ranges(&self.this, &self.members(), self.vshards, self.n_pools)
    }
}

impl Membership for DbCluster {
    fn this_node(&self) -> &NodeId {
        &self.this
    }
    fn live_nodes(&self) -> Vec<NodeId> {
        self.members().as_ref().clone()
    }
}

impl ShardMap for DbCluster {
    fn shard_of(&self, workflow_id: &str) -> ShardId {
        ShardId::of(workflow_id, self.vshards)
    }
    fn owner_of(&self, shard: ShardId) -> Option<NodeId> {
        range_owner(shard, &self.members(), self.vshards, self.n_pools).cloned()
    }
    fn owns(&self, shard: ShardId) -> bool {
        self.owner_of(shard).as_ref() == Some(&self.this)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Generation;
    use std::sync::Mutex;

    /// In-memory ClusterStore: a controllable live set, no database. Each node
    /// registers as `<name>.peer:50051` unless it registers itself otherwise.
    #[derive(Default)]
    struct FakeStore {
        live: Mutex<Vec<(NodeId, String)>>,
    }
    impl FakeStore {
        fn with(nodes: &[&str]) -> Arc<Self> {
            let store = Arc::new(FakeStore::default());
            store.set(nodes);
            store
        }
        fn set(&self, nodes: &[&str]) {
            *self.live.lock().unwrap() = nodes
                .iter()
                .map(|s| (NodeId::new(*s), format!("{s}.peer:50051")))
                .collect();
        }
    }
    #[async_trait::async_trait]
    impl ClusterStore for FakeStore {
        async fn register(&self, node: &NodeId, address: &str) -> StoreResult<()> {
            let mut live = self.live.lock().unwrap();
            live.retain(|(n, _)| n != node);
            live.push((node.clone(), address.to_string()));
            Ok(())
        }
        async fn heartbeat(&self, _node: &NodeId) -> StoreResult<()> {
            Ok(())
        }
        async fn live_members(&self, _stale_after_secs: i64) -> StoreResult<Vec<(NodeId, String)>> {
            Ok(self.live.lock().unwrap().clone())
        }
        // Fencing ops are unused by the membership tests.
        async fn acquire_shard(
            &self,
            _shard: ShardId,
            _node: &NodeId,
            _stale: i64,
        ) -> StoreResult<Option<Generation>> {
            Ok(Some(1))
        }
        async fn renew_shard(
            &self,
            _shard: ShardId,
            _node: &NodeId,
            _held: Generation,
        ) -> StoreResult<bool> {
            Ok(true)
        }
        async fn release_shard(
            &self,
            _shard: ShardId,
            _node: &NodeId,
            _held: Generation,
        ) -> StoreResult<bool> {
            Ok(true)
        }
    }

    #[tokio::test]
    async fn refresh_reflects_the_live_set_into_ownership() {
        let store = FakeStore::with(&["a", "b", "c"]);
        let c = DbCluster::new(NodeId::new("a"), store.clone(), 4096, 30);
        c.refresh().await.unwrap();

        // Every shard is owned by exactly one live node, matching the pure
        // range assignment over the same set — the live view and the assignment
        // must not be two different answers.
        let nodes: Vec<NodeId> = ["a", "b", "c"].iter().map(|s| NodeId::new(*s)).collect();
        for i in (0..4096u32).step_by(101) {
            let owner = c.owner_of(ShardId(i)).unwrap();
            assert_eq!(owner, *range_owner(ShardId(i), &nodes, 4096, 1).unwrap());
            assert_eq!(c.owns(ShardId(i)), owner == NodeId::new("a"));
        }
    }

    #[tokio::test]
    async fn ownership_shifts_when_a_node_leaves() {
        let store = FakeStore::with(&["a", "b", "c"]);
        let c = DbCluster::new(NodeId::new("a"), store.clone(), 4096, 30);
        c.refresh().await.unwrap();

        // Find a shard "a" does NOT own (owned by b or c).
        let foreign = (0..4096u32)
            .map(ShardId)
            .find(|s| !c.owns(*s))
            .expect("some shard is owned by another node");
        let prev_owner = c.owner_of(foreign).unwrap();
        assert_ne!(prev_owner, NodeId::new("a"));

        // That owner dies; after refresh its shards must reassign to a survivor,
        // and some now land on "a".
        let survivors: Vec<&str> = ["a", "b", "c"]
            .into_iter()
            .filter(|n| *n != prev_owner.as_str())
            .collect();
        store.set(&survivors);
        c.refresh().await.unwrap();

        let new_owner = c.owner_of(foreign).unwrap();
        assert_ne!(new_owner, prev_owner, "dead node's shard must move");
        assert!(survivors.contains(&new_owner.as_str()));
    }

    #[tokio::test]
    async fn register_then_refresh_includes_self() {
        let store = FakeStore::with(&["b"]); // self ("a") not yet present
        let c = DbCluster::new(NodeId::new("a"), store.clone(), 4096, 30);
        c.register("a:1").await.unwrap();
        c.refresh().await.unwrap();
        assert!(c.live_nodes().contains(&NodeId::new("a")));
        // With self present, "a" owns a non-trivial share of shards.
        let owned = (0..4096u32).map(ShardId).filter(|s| c.owns(*s)).count();
        assert!(owned > 0, "a registered → must own some shards");
    }

    #[tokio::test]
    async fn address_of_serves_the_registered_address_of_a_live_peer() {
        let store = FakeStore::with(&["b"]);
        let c = DbCluster::new(NodeId::new("a"), store.clone(), 4096, 30);
        c.register("10.0.0.1:50051").await.unwrap();

        // Before the first refresh nothing is known about anyone.
        assert_eq!(c.address_of(&NodeId::new("b")), None);

        c.refresh().await.unwrap();
        assert_eq!(
            c.address_of(&NodeId::new("b")).as_deref(),
            Some("b.peer:50051")
        );
        assert_eq!(
            c.address_of(&NodeId::new("a")).as_deref(),
            Some("10.0.0.1:50051")
        );
        // A node that is not live has no address, however it once registered.
        assert_eq!(c.address_of(&NodeId::new("gone")), None);
    }

    #[tokio::test]
    async fn a_bind_address_is_not_served_as_a_peer_address() {
        // What every node registered before it could advertise anything else.
        let store = FakeStore::with(&[]);
        store
            .register(&NodeId::new("b"), "0.0.0.0:50051")
            .await
            .unwrap();
        let c = DbCluster::new(NodeId::new("a"), store.clone(), 4096, 30);
        c.refresh().await.unwrap();

        assert!(
            c.live_nodes().contains(&NodeId::new("b")),
            "b is live and owns shards"
        );
        assert_eq!(
            c.address_of(&NodeId::new("b")),
            None,
            "but 0.0.0.0 from here means ourselves, so it must not be dialed"
        );
    }

    #[tokio::test]
    async fn a_refresh_replaces_addresses_along_with_members() {
        let store = FakeStore::with(&["a", "b"]);
        let c = DbCluster::new(NodeId::new("a"), store.clone(), 4096, 30);
        c.refresh().await.unwrap();
        assert!(c.address_of(&NodeId::new("b")).is_some());

        store.set(&["a"]);
        c.refresh().await.unwrap();
        assert_eq!(c.address_of(&NodeId::new("b")), None);
    }
}
