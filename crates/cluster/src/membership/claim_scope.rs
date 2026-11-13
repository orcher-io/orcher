//! Bridges a live cluster view to the storage layer's claim filter.
//!
//! [`DbCluster`] knows which shard ranges this node owns; `orcher-storage` knows
//! how to turn ranges into a predicate on a claim query. This is the adapter
//! between them, and it is where the *poll counter* behind the unfiltered sweep
//! lives — the cluster view itself stays a pure function of the member set.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use orcher_storage::sharding::ClaimScope;

use super::DbCluster;

/// How often a poll ignores the shard filter and scans everything.
///
/// Not tuned so much as bounded: it caps how long a wrongly-owned range can
/// go unclaimed, and costs one global scan in every `N` polls. Low enough that a
/// stall is measured in seconds at normal poll rates, high enough that the extra
/// scans do not show up.
const DEFAULT_SWEEP_INTERVAL: u32 = 20;

/// Claim filtering driven by a live cluster view.
///
/// Holds the cluster behind an `Arc` and reads its ranges per poll rather than
/// caching them, so a membership change takes effect on the next poll with no
/// invalidation step of its own.
pub struct ClusterClaimScope {
    cluster: Arc<DbCluster>,
    sweep_every: u32,
    polls: AtomicU32,
}

// Hand-written rather than derived: `DbCluster` holds an `Arc<dyn ClusterStore>`,
// and requiring `Debug` on that trait to satisfy this would push the bound onto
// every store implementation. Reports the current ranges, which is what is
// actually useful when this shows up in a log line.
impl std::fmt::Debug for ClusterClaimScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterClaimScope")
            .field("owned_ranges", &self.cluster.owned_ranges())
            .field("sweep_every", &self.sweep_every)
            .finish()
    }
}

impl ClusterClaimScope {
    pub fn new(cluster: Arc<DbCluster>) -> Self {
        ClusterClaimScope {
            cluster,
            sweep_every: DEFAULT_SWEEP_INTERVAL,
            polls: AtomicU32::new(0),
        }
    }

    /// Override the sweep cadence. `1` makes every poll unfiltered (filtering
    /// off); `0` disables the sweep, which trades the stall guard away and is
    /// only safe where membership is known to be exact.
    pub fn with_sweep_interval(mut self, every: u32) -> Self {
        self.sweep_every = every;
        self
    }
}

impl ClaimScope for ClusterClaimScope {
    fn owned_ranges(&self) -> Vec<(i32, i32)> {
        self.cluster
            .owned_ranges()
            .into_iter()
            .map(|r| (r.lo as i32, r.hi as i32))
            .collect()
    }

    fn sweep_interval(&self) -> u32 {
        self.sweep_every
    }

    fn should_sweep(&self) -> bool {
        match self.sweep_every {
            0 => false,
            1 => true,
            n => {
                // Count first, compare after, so the very first poll is filtered
                // rather than swept — a fresh node should start doing its own
                // work immediately, not scan the whole table.
                let seen = self.polls.fetch_add(1, Ordering::Relaxed).wrapping_add(1);
                seen.is_multiple_of(n)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{ClusterStore, StoreResult};
    use crate::types::{Generation, NodeId, ShardId};
    use std::sync::Mutex;

    #[derive(Default)]
    struct FakeStore {
        live: Mutex<Vec<String>>,
    }

    impl FakeStore {
        fn with(ids: &[&str]) -> Arc<Self> {
            Arc::new(FakeStore {
                live: Mutex::new(ids.iter().map(|s| s.to_string()).collect()),
            })
        }
    }

    #[async_trait::async_trait]
    impl ClusterStore for FakeStore {
        async fn register(&self, node: &NodeId, _address: &str) -> StoreResult<()> {
            let mut live = self.live.lock().unwrap();
            if !live.iter().any(|n| n == node.as_str()) {
                live.push(node.0.clone());
            }
            Ok(())
        }
        async fn heartbeat(&self, _node: &NodeId) -> StoreResult<()> {
            Ok(())
        }
        async fn live_members(&self, _stale_after_secs: i64) -> StoreResult<Vec<(NodeId, String)>> {
            Ok(self
                .live
                .lock()
                .unwrap()
                .iter()
                .map(|s| (NodeId::new(s.clone()), format!("{s}:50051")))
                .collect())
        }
        async fn acquire_shard(
            &self,
            _shard: ShardId,
            _node: &NodeId,
            _stale_after_secs: i64,
        ) -> StoreResult<Option<Generation>> {
            Ok(None)
        }
        async fn renew_shard(
            &self,
            _shard: ShardId,
            _node: &NodeId,
            _gen: Generation,
        ) -> StoreResult<bool> {
            Ok(true)
        }
        async fn release_shard(
            &self,
            _shard: ShardId,
            _node: &NodeId,
            _gen: Generation,
        ) -> StoreResult<bool> {
            Ok(true)
        }
    }

    fn scope(members: &[&str], this: &str) -> ClusterClaimScope {
        let store = FakeStore::with(members);
        let cluster = Arc::new(DbCluster::new(NodeId::new(this), store, 4096, 30));
        ClusterClaimScope::new(cluster)
    }

    #[tokio::test]
    async fn a_sole_node_owns_the_whole_space() {
        let s = scope(&["a"], "a");
        s.cluster.refresh().await.unwrap();
        assert_eq!(s.owned_ranges(), vec![(0, 4095)]);
    }

    /// Ranges must partition the space across the live nodes, so a filtered
    /// claim on each node together covers every row.
    #[tokio::test]
    async fn members_split_the_space_without_gaps() {
        let members = ["a", "b", "c"];
        let mut covered: Vec<(i32, i32)> = Vec::new();
        for this in members {
            let s = scope(&members, this);
            s.cluster.refresh().await.unwrap();
            covered.extend(s.owned_ranges());
        }
        covered.sort();
        assert_eq!(covered.len(), 3, "each node owns exactly one range");
        assert_eq!(covered[0].0, 0);
        assert_eq!(covered[2].1, 4095);
        for w in covered.windows(2) {
            assert_eq!(w[1].0, w[0].1 + 1, "gap or overlap between {:?}", w);
        }
    }

    /// The fail-open case: before the first refresh a node's snapshot holds only
    /// itself, so it owns everything. It must never report an empty range set
    /// *and* keep working — either it owns all, or the empty set is read by the
    /// storage layer as "claim unfiltered".
    #[tokio::test]
    async fn a_node_absent_from_the_live_set_reports_no_ranges() {
        let s = scope(&["b", "c"], "a"); // "a" is not a member
        s.cluster.refresh().await.unwrap();
        assert!(
            s.owned_ranges().is_empty(),
            "a non-member must report nothing, which storage reads as claim-everything"
        );
    }

    #[test]
    fn the_first_poll_is_filtered_and_every_nth_sweeps() {
        let s = scope(&["a"], "a").with_sweep_interval(5);
        let swept: Vec<bool> = (0..10).map(|_| s.should_sweep()).collect();
        assert_eq!(
            swept,
            vec![false, false, false, false, true, false, false, false, false, true],
            "a fresh node must do its own work first, then sweep on every 5th poll"
        );
    }

    #[test]
    fn sweep_interval_of_one_disables_filtering() {
        let s = scope(&["a"], "a").with_sweep_interval(1);
        assert!((0..5).all(|_| s.should_sweep()));
    }

    #[test]
    fn sweep_interval_of_zero_never_sweeps() {
        let s = scope(&["a"], "a").with_sweep_interval(0);
        assert!(!(0..5).any(|_| s.should_sweep()));
    }
}
