//! Shard-ownership runtime: the node-local record of which shards this node holds
//! and at which fencing generation. This is the piece the engine write path will
//! consult — "give me the generation I hold for this workflow's shard, acquiring
//! it if I don't hold it yet" — and reject a write when the answer is "you don't
//! hold it."
//!
//! Acquisition is **lazy**: a shard's generation is acquired on first touch (when
//! a workflow in that shard is first written), not by pre-acquiring all `VSHARDS`.
//! Held shards are kept fresh by [`ShardOwnership::maintain`] on the membership
//! cadence, which also releases shards this node no longer owns.
//!
//! Still inert — nothing in the engine consults this yet. The write-path guard
//! that does is a separate, independently-gated step.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::store::{ClusterStore, StoreResult};
use crate::traits::{PeerDirectory, ShardMap};
use crate::types::{Generation, NodeId, ShardId};

/// This node's held shards and their generations, plus lazy acquire / periodic
/// renew / release against a [`ClusterStore`].
pub struct ShardOwnership {
    this: NodeId,
    store: Arc<dyn ClusterStore>,
    /// Ownership map (contiguous ranges over the live set) — decides which
    /// shards we *should* hold, driving renew-vs-release during maintenance.
    /// The same assignment the claim filter uses, so a node cannot hold a lease
    /// on one set of shards while claiming rows from another.
    shards: Arc<dyn ShardMap>,
    stale_after_secs: i64,
    /// Set when range-strict: where peers can be reached, which decides whether
    /// a shard in another node's range is left for it. See
    /// [`ShardOwnership::with_range_strict`].
    range_strict: Option<Arc<dyn PeerDirectory>>,
    held: RwLock<HashMap<ShardId, Generation>>,
}

impl std::fmt::Debug for ShardOwnership {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardOwnership")
            .field("node", &self.this)
            .field("held_shards", &self.held_count())
            .finish()
    }
}

impl ShardOwnership {
    pub fn new(
        this: NodeId,
        store: Arc<dyn ClusterStore>,
        shards: Arc<dyn ShardMap>,
        stale_after_secs: i64,
    ) -> Self {
        ShardOwnership {
            this,
            store,
            shards,
            stale_after_secs,
            range_strict: None,
            held: RwLock::new(HashMap::new()),
        }
    }

    /// Acquire only the shards this node's range covers.
    ///
    /// Without this, acquisition is first-touch: whichever node writes a
    /// workflow first takes its shard, wherever the range says it belongs, and
    /// holds it until maintenance releases it a few seconds later. Meanwhile the
    /// range owner — the node that claims that shard's work — is fenced out of
    /// its own shard. With requests for one workflow spread over several nodes
    /// that happens constantly: the shard moves back and forth and the owner's
    /// writes are refused.
    ///
    /// Strict, a node declines a shard whose range owner is another live node
    /// it could relay the request to, so the owner can always take it. That is
    /// only safe when something does relay the request, which is why it is
    /// switched on together with owner forwarding and off otherwise; `peers` is
    /// the same directory the forwarder dials from.
    ///
    /// It still takes a shard when it could not have relayed:
    /// - it knows of no live member, or of only itself — a node starting up, or
    ///   one whose view of the cluster is unknown, and refusing then would
    ///   refuse everything;
    /// - the range owner has no address it can dial — a node that registered
    ///   its bind address, as every node did before it could advertise one.
    ///   Refusing there would refuse the request everywhere but on the owner,
    ///   so for that shard this falls back to first-touch.
    pub fn with_range_strict(mut self, peers: Arc<dyn PeerDirectory>) -> Self {
        self.range_strict = Some(peers);
        self
    }

    /// Whether this node may take `shard`: `false` only when range-strict and
    /// the range gives it to another node that can be reached.
    fn may_take(&self, shard: ShardId) -> bool {
        let Some(peers) = &self.range_strict else {
            return true;
        };
        match self.shards.owner_of(shard) {
            Some(owner) if owner != self.this => peers.address_of(&owner).is_none(),
            // No owner (an empty live view) is unknown; this node as owner,
            // including as the only member it knows of, is ours to take.
            _ => true,
        }
    }

    /// The generation this node currently holds for `shard`, if any (no I/O).
    pub fn held_generation(&self, shard: ShardId) -> Option<Generation> {
        self.held.read().unwrap().get(&shard).copied()
    }

    /// Ensure this node holds the shard owning `workflow_id`, returning
    /// `(shard, generation)` — the token to guard the write with. Returns `None`
    /// if a *live* other node owns the shard (we must not process this workflow),
    /// or, when range-strict, if another live node's range covers it.
    ///
    /// Lazy: hits the store only on the first acquire for a shard; subsequent
    /// calls return the cached generation until the shard is released or fenced.
    pub async fn acquire_for(
        &self,
        workflow_id: &str,
    ) -> StoreResult<Option<(ShardId, Generation)>> {
        let shard = self.shards.shard_of(workflow_id);
        // Checked before the cache as well: a shard held from before the range
        // moved stays in the map until maintenance releases it, and writing
        // under it in that window is exactly what fences the new owner out.
        if !self.may_take(shard) {
            return Ok(None);
        }
        if let Some(gen) = self.held_generation(shard) {
            return Ok(Some((shard, gen)));
        }
        match self
            .store
            .acquire_shard(shard, &self.this, self.stale_after_secs)
            .await?
        {
            Some(gen) => {
                self.held.write().unwrap().insert(shard, gen);
                Ok(Some((shard, gen)))
            }
            None => Ok(None), // a live other node owns it
        }
    }

    /// Keep held shards fresh: renew those we still own (no generation bump),
    /// release those we no longer own. Drops any shard we were fenced out of.
    /// Driven on the membership cadence, alongside heartbeat/refresh.
    pub async fn maintain(&self) -> StoreResult<()> {
        let held: Vec<(ShardId, Generation)> = self
            .held
            .read()
            .unwrap()
            .iter()
            .map(|(s, g)| (*s, *g))
            .collect();

        for (shard, gen) in held {
            if self.shards.owns(shard) {
                // Still ours — renew. If renew fails, someone fenced us out.
                if !self.store.renew_shard(shard, &self.this, gen).await? {
                    self.held.write().unwrap().remove(&shard);
                }
            } else {
                // No longer ours (membership changed) — relinquish and forget.
                let _ = self.store.release_shard(shard, &self.this, gen).await;
                self.held.write().unwrap().remove(&shard);
            }
        }
        Ok(())
    }

    /// Relinquish every held shard (graceful drain / shutdown).
    pub async fn release_all(&self) -> StoreResult<()> {
        let held: Vec<(ShardId, Generation)> = self
            .held
            .read()
            .unwrap()
            .iter()
            .map(|(s, g)| (*s, *g))
            .collect();
        for (shard, gen) in held {
            let _ = self.store.release_shard(shard, &self.this, gen).await;
        }
        self.held.write().unwrap().clear();
        Ok(())
    }

    /// Count of shards currently held (diagnostics/tests).
    pub fn held_count(&self) -> usize {
        self.held.read().unwrap().len()
    }
}

// Bridge to the storage-layer write fence, so repositories can refuse a write
// from a superseded owner without depending on this crate. `acquire_for` is
// already lazy — it hits the store once per shard and serves the generation
// from memory afterwards — so this is a map lookup and an integer compare on
// the hot path, not a query per write.
#[cfg(feature = "storage")]
#[async_trait::async_trait]
impl orcher_storage::sharding::WriteFence for ShardOwnership {
    async fn guard(&self, workflow_id: uuid::Uuid) -> Option<orcher_storage::sharding::FenceToken> {
        match self.acquire_for(&workflow_id.to_string()).await {
            Ok(Some((shard, generation))) => Some(orcher_storage::sharding::FenceToken {
                shard_id: shard.0 as i32,
                generation: generation as i64,
            }),
            // A live other node owns the shard: we are superseded, so the caller
            // must refuse the write rather than proceed unguarded.
            Ok(None) => None,
            // A store error is not evidence that we still hold the shard, and
            // this fence fails closed — so it is also a refusal.
            Err(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::membership::StaticCluster;
    use std::sync::Mutex;

    /// A fake fencing store: a `shard -> (owner, generation)` map with the same
    /// acquire/renew/release semantics as the real Postgres CAS, but in memory.
    #[derive(Default)]
    struct FakeFencing {
        // shard -> (owner, generation)
        shards: Mutex<HashMap<ShardId, (NodeId, Generation)>>,
        // which nodes are "live" for the stale check
        live: Mutex<Vec<NodeId>>,
    }
    impl FakeFencing {
        fn new(live: &[&str]) -> Arc<Self> {
            Arc::new(FakeFencing {
                shards: Mutex::new(HashMap::new()),
                live: Mutex::new(live.iter().map(|s| NodeId::new(*s)).collect()),
            })
        }
        fn kill(&self, node: &str) {
            self.live.lock().unwrap().retain(|n| n.as_str() != node);
        }
        fn owner_of(&self, shard: ShardId) -> Option<NodeId> {
            self.shards
                .lock()
                .unwrap()
                .get(&shard)
                .map(|(n, _)| n.clone())
        }
    }
    #[async_trait::async_trait]
    impl ClusterStore for FakeFencing {
        async fn register(&self, _node: &NodeId, _addr: &str) -> StoreResult<()> {
            Ok(())
        }
        async fn heartbeat(&self, _node: &NodeId) -> StoreResult<()> {
            Ok(())
        }
        async fn live_members(&self, _stale: i64) -> StoreResult<Vec<(NodeId, String)>> {
            Ok(self
                .live
                .lock()
                .unwrap()
                .iter()
                .map(|n| (n.clone(), format!("{}:50051", n.as_str())))
                .collect())
        }
        async fn acquire_shard(
            &self,
            shard: ShardId,
            node: &NodeId,
            _stale: i64,
        ) -> StoreResult<Option<Generation>> {
            let live = self.live.lock().unwrap().clone();
            let mut shards = self.shards.lock().unwrap();
            match shards.get(&shard) {
                // Owned by a live *other* node → fenced out.
                Some((owner, _)) if owner != node && live.contains(owner) => Ok(None),
                // Unowned, ours, or a dead owner → take it, bumping the generation.
                Some((_, gen)) => {
                    let ng = gen + 1;
                    shards.insert(shard, (node.clone(), ng));
                    Ok(Some(ng))
                }
                None => {
                    shards.insert(shard, (node.clone(), 1));
                    Ok(Some(1))
                }
            }
        }
        async fn renew_shard(
            &self,
            shard: ShardId,
            node: &NodeId,
            held: Generation,
        ) -> StoreResult<bool> {
            let shards = self.shards.lock().unwrap();
            Ok(matches!(shards.get(&shard), Some((o, g)) if o == node && *g == held))
        }
        async fn release_shard(
            &self,
            shard: ShardId,
            node: &NodeId,
            held: Generation,
        ) -> StoreResult<bool> {
            let mut shards = self.shards.lock().unwrap();
            if matches!(shards.get(&shard), Some((o, g)) if o == node && *g == held) {
                shards.remove(&shard);
                Ok(true)
            } else {
                Ok(false)
            }
        }
    }

    fn ownership(this: &str, members: &[&str], store: Arc<dyn ClusterStore>) -> ShardOwnership {
        let nodes: Vec<NodeId> = members.iter().map(|s| NodeId::new(*s)).collect();
        let map: Arc<dyn ShardMap> = Arc::new(StaticCluster::new(
            NodeId::new(this),
            nodes,
            crate::DEFAULT_VSHARDS,
        ));
        ShardOwnership::new(NodeId::new(this), store, map, 30)
    }

    #[tokio::test]
    async fn acquire_is_lazy_and_cached() {
        let store = FakeFencing::new(&["a"]);
        let own = ownership("a", &["a"], store.clone());
        assert_eq!(own.held_count(), 0);

        let (shard, gen) = own.acquire_for("wf-1").await.unwrap().unwrap();
        assert_eq!(gen, 1);
        assert_eq!(own.held_count(), 1);
        // Second call for the same shard → cached, no bump.
        let again = own.acquire_for("wf-1").await.unwrap().unwrap();
        assert_eq!(again, (shard, 1));
        assert_eq!(own.held_generation(shard), Some(1));
    }

    #[tokio::test]
    async fn a_live_other_owner_fences_us_out() {
        let store = FakeFencing::new(&["a", "b"]);
        let b = ownership("b", &["a", "b"], store.clone());
        let a = ownership("a", &["a", "b"], store.clone());
        // b acquires wf-x's shard first, and b is live.
        assert!(b.acquire_for("wf-x").await.unwrap().is_some());
        // a asks for the SAME workflow → same shard (shard_of is deterministic) →
        // fenced out, because a live node (b) owns it.
        assert_eq!(
            a.acquire_for("wf-x").await.unwrap(),
            None,
            "a must be fenced out of b's live shard"
        );
    }

    #[tokio::test]
    async fn maintain_renews_owned_and_releases_unowned() {
        // Single-member view for "a": a owns every shard → maintain renews.
        let store = FakeFencing::new(&["a"]);
        let own = ownership("a", &["a"], store.clone());
        let (shard, gen) = own.acquire_for("wf-1").await.unwrap().unwrap();
        own.maintain().await.unwrap();
        // Still held at the same generation (renew doesn't bump).
        assert_eq!(own.held_generation(shard), Some(gen));
        assert_eq!(
            store.owner_of(shard).as_ref().map(|n| n.as_str()),
            Some("a")
        );
    }

    /// Every peer answers on some address, as when all advertise one.
    struct Reachable;
    impl PeerDirectory for Reachable {
        fn address_of(&self, node: &NodeId) -> Option<String> {
            Some(format!("{}.peer:50051", node.as_str()))
        }
    }
    fn reachable() -> Arc<dyn PeerDirectory> {
        Arc::new(Reachable)
    }

    /// No peer can be dialed, as when they registered bind addresses.
    struct Unreachable;
    impl PeerDirectory for Unreachable {
        fn address_of(&self, _node: &NodeId) -> Option<String> {
            None
        }
    }

    /// A workflow in a shard another live node's range covers.
    fn foreign_workflow(own: &ShardOwnership) -> String {
        (0..10_000)
            .map(|i| format!("wf-{i}"))
            .find(|wf| {
                let shard = own.shards.shard_of(wf);
                own.shards.owner_of(shard) != Some(own.this.clone())
            })
            .expect("some workflow hashes into the other node's range")
    }

    #[tokio::test]
    async fn first_touch_takes_a_shard_outside_the_range_when_not_strict() {
        // Today's behaviour, kept when forwarding is off: whoever touches first
        // holds the shard, wherever its range lies.
        let store = FakeFencing::new(&["a", "b"]);
        let a = ownership("a", &["a", "b"], store.clone());
        let wf = foreign_workflow(&a);
        assert!(a.acquire_for(&wf).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn range_strict_declines_a_shard_another_live_node_owns() {
        let store = FakeFencing::new(&["a", "b"]);
        let a = ownership("a", &["a", "b"], store.clone()).with_range_strict(reachable());
        let wf = foreign_workflow(&a);

        assert_eq!(
            a.acquire_for(&wf).await.unwrap(),
            None,
            "a must leave b's shard for b"
        );
        let shard = a.shards.shard_of(&wf);
        assert_eq!(store.owner_of(shard), None, "and must not have taken it");

        // So the range owner can always take its own shard.
        let b = ownership("b", &["a", "b"], store.clone()).with_range_strict(reachable());
        assert!(b.acquire_for(&wf).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn range_strict_takes_a_shard_whose_owner_cannot_be_reached() {
        // An owner with no dialable address cannot be relayed to, so refusing
        // would refuse the request on every node but the owner.
        let store = FakeFencing::new(&["a", "b"]);
        let a = ownership("a", &["a", "b"], store.clone()).with_range_strict(Arc::new(Unreachable));
        let wf = foreign_workflow(&a);
        assert!(a.acquire_for(&wf).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn range_strict_still_acquires_inside_its_own_range() {
        let store = FakeFencing::new(&["a", "b"]);
        let a = ownership("a", &["a", "b"], store.clone()).with_range_strict(reachable());
        let own = (0..10_000)
            .map(|i| format!("wf-{i}"))
            .find(|wf| a.shards.owns(a.shards.shard_of(wf)))
            .unwrap();
        assert!(a.acquire_for(&own).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn range_strict_takes_anything_while_it_knows_only_itself() {
        // A node that has not yet seen its peers — the view DbCluster starts
        // with — must not refuse everything.
        let store = FakeFencing::new(&["a", "b"]);
        let a = ownership("a", &["a"], store.clone()).with_range_strict(reachable());
        for i in 0..50 {
            assert!(a.acquire_for(&format!("wf-{i}")).await.unwrap().is_some());
        }
    }

    #[tokio::test]
    async fn range_strict_takes_anything_when_the_live_set_is_unknown() {
        let store = FakeFencing::new(&["a"]);
        let a = ownership("a", &[], store.clone()).with_range_strict(reachable());
        assert!(a.acquire_for("wf-1").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn range_strict_stops_using_a_held_shard_once_the_range_moves() {
        use crate::membership::DbCluster;
        // A live view, so the range can move under a shard already held.
        let store = FakeFencing::new(&["a"]);
        let view = Arc::new(DbCluster::new(
            NodeId::new("a"),
            store.clone(),
            crate::DEFAULT_VSHARDS,
            30,
        ));
        view.refresh().await.unwrap();
        let a = ShardOwnership::new(NodeId::new("a"), store.clone(), view.clone(), 30)
            .with_range_strict(reachable());

        // Alone, a takes a shard that will belong to b once b joins.
        let wf = foreign_workflow(&ownership("a", &["a", "b"], store.clone()));
        assert!(a.acquire_for(&wf).await.unwrap().is_some());

        // b joins; a's cached generation must no longer be used for writes.
        store.live.lock().unwrap().push(NodeId::new("b"));
        view.refresh().await.unwrap();
        assert_eq!(a.acquire_for(&wf).await.unwrap(), None);
    }

    #[tokio::test]
    async fn maintain_drops_a_shard_we_were_fenced_out_of() {
        let store = FakeFencing::new(&["a", "b"]);
        let a = ownership("a", &["a"], store.clone()); // a's view: a owns all
        let (shard, _) = a.acquire_for("wf-1").await.unwrap().unwrap();
        assert_eq!(a.held_count(), 1);

        // b forcibly takes the shard (a is considered dead by the store's live set).
        store.kill("a");
        let b = NodeId::new("b");
        let ng = store.acquire_shard(shard, &b, 30).await.unwrap();
        assert!(ng.is_some(), "b reclaims a's shard");

        // a maintains: it still *thinks* it owns the shard (single-member view),
        // so it renews — but the store rejects (b holds it now) → a drops it.
        a.maintain().await.unwrap();
        assert_eq!(a.held_count(), 0, "a drops the shard it was fenced out of");
    }
}
