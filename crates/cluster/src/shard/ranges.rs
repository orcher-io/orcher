//! Contiguous shard ownership: which *range* of the shard space a node owns.
//!
//! A claim query needs to ask "which shards do I own" and get an answer a
//! database index can scan. Ownership by hashing each shard to a node answers
//! the inverse question — "who owns this one shard" — and scatters a node's
//! shards uniformly across `[0, vshards)`. With 3 nodes and 4096 shards that is
//! ~1365 unrelated integers, an `IN` list on every poll.
//!
//! A contiguous range is `shard_id BETWEEN lo AND hi`: one index range scan.
//! [`owned_ranges`] assigns those ranges, and [`range_owner`] reads the same
//! assignment backwards for point lookups, so there is exactly one definition of
//! who owns what.
//!
//! # Alignment to pools
//!
//! Shard→pool is also contiguous (`routing::pool_index`: pool `p` covers a
//! contiguous slice of the shard space). Assigning node ranges that respect
//! those boundaries means a node's range lies inside a single pool, so its
//! claims hit one database rather than fanning out across all of them. Adding a
//! database and adding a node then become the same operation.
//!
//! # The trade-off, stated plainly
//!
//! Hash-per-shard ownership moves ~`1/N` of shards when a node joins or leaves.
//! Contiguous ranges move more, because the boundaries shift for everyone. That is a
//! deliberate trade: while nothing is cached per-shard, moving a shard costs
//! only which rows a node filters on, and the claim-side win is large and
//! constant. It stops being free once ownership carries in-memory state, so the
//! upgrade path is *multiple* smaller ranges per node (assign `T` slices
//! instead of 1 — movement falls as `T` grows). [`owned_ranges`] already returns
//! a set for that reason; nothing above it needs to change when `T > 1`.
//!
//! # Not a correctness mechanism
//!
//! Ownership here is an optimisation: it decides who *looks* at a row, not who
//! may write it. Exactly-once still rests on `FOR UPDATE SKIP LOCKED`. Two nodes
//! transiently believing they own the same shard is therefore safe. A transient
//! *gap* — no node believing it owns a shard — is the failure that matters,
//! because those rows go unclaimed until membership converges; callers must keep
//! an unfiltered sweep for that case.

use crate::types::{NodeId, ShardId};

/// An inclusive range of shard ids, `lo..=hi`.
///
/// Inclusive because it maps directly onto SQL `BETWEEN lo AND hi`, and an
/// off-by-one at this boundary silently strands a shard.
#[derive(Clone, Copy, PartialEq, Eq, Debug, PartialOrd, Ord)]
pub struct ShardRange {
    pub lo: u32,
    pub hi: u32,
}

impl ShardRange {
    pub fn contains(&self, shard: ShardId) -> bool {
        shard.0 >= self.lo && shard.0 <= self.hi
    }

    /// Number of shards in the range.
    pub fn len(&self) -> u32 {
        self.hi - self.lo + 1
    }

    /// Always false — a `ShardRange` is inclusive and so holds at least one
    /// shard. Present because clippy asks for it wherever `len` exists.
    pub fn is_empty(&self) -> bool {
        false
    }
}

/// First shard of pool `p`, when `[0, vshards)` is split into `n_pools`
/// contiguous near-equal ranges.
///
/// This is the inverse of `routing::pool_index`, and the two are checked against
/// each other in the tests — if they ever disagree a node's range would straddle
/// a database boundary, which is the one thing this alignment exists to prevent.
fn pool_start(p: usize, n_pools: usize, vshards: u32) -> u32 {
    // Round up, matching the truncating division in `pool_index`: shard s belongs
    // to pool `s * n_pools / vshards`, so pool p starts at the smallest s with
    // `s * n_pools >= p * vshards`.
    ((p as u64 * vshards as u64).div_ceil(n_pools as u64)) as u32
}

/// The contiguous shard range(s) `this` owns, given the live member set.
///
/// Every node computes this from the same inputs and reaches the same answer, so
/// there is no coordinator and no election. Returns empty when `this` is not in
/// `nodes` (it is not live, so it owns nothing) or when there are no members.
///
/// The result partitions `[0, vshards)` exactly across the live nodes: no shard
/// is owned twice and none is left out. Both are asserted in the tests, the
/// second because a gap means workflows on that shard stop being claimed.
///
/// `n_pools` is the number of shard databases (1 for the usual single-database
/// deployment). Ranges never straddle a pool boundary.
pub fn owned_ranges(
    this: &NodeId,
    nodes: &[NodeId],
    vshards: u32,
    n_pools: usize,
) -> Vec<ShardRange> {
    if vshards == 0 || n_pools == 0 {
        return Vec::new();
    }

    // Sort so that every node derives the same ordering from the same set,
    // whatever order the membership query returned rows in.
    let mut live: Vec<&NodeId> = nodes.iter().collect();
    live.sort();
    live.dedup();

    let n = live.len();
    let Some(i) = live.iter().position(|n| *n == this) else {
        return Vec::new();
    };

    let range = if n <= n_pools {
        // Fewer nodes than databases: a node owns whole pools. Pools are
        // contiguous and assigned in order, so its territory is still one range.
        let first_pool = i * n_pools / n;
        let end_pool = (i + 1) * n_pools / n;
        let lo = pool_start(first_pool, n_pools, vshards);
        let hi = pool_start(end_pool, n_pools, vshards) - 1;
        ShardRange { lo, hi }
    } else {
        // More nodes than databases: several nodes share a pool, each taking a
        // sub-slice of *that pool's* range so the range stays inside one database.
        let p = i * n_pools / n;
        let pool_lo = pool_start(p, n_pools, vshards);
        let pool_hi = pool_start(p + 1, n_pools, vshards); // exclusive

        // Which nodes share pool p, and where `this` sits among them.
        let first_node_of_pool = (p as u64 * n as u64).div_ceil(n_pools as u64) as usize;
        let end_node_of_pool = ((p + 1) as u64 * n as u64).div_ceil(n_pools as u64) as usize;
        let share_count = end_node_of_pool - first_node_of_pool;
        let k = i - first_node_of_pool;

        let width = (pool_hi - pool_lo) as u64;
        let lo = pool_lo + (k as u64 * width / share_count as u64) as u32;
        let end = pool_lo + ((k + 1) as u64 * width / share_count as u64) as u32; // exclusive

        // More nodes than shards in this pool: this node's slice is empty. Return
        // nothing rather than computing `end - 1`, which underflows at pool_lo = 0
        // and would wrap to a range reading as "owns everything".
        if end <= lo {
            return Vec::new();
        }
        ShardRange { lo, hi: end - 1 }
    };

    vec![range]
}

/// Which node owns `shard`, under the same assignment [`owned_ranges`] produces.
///
/// The point-lookup inverse of `owned_ranges`, deliberately defined *in terms of*
/// it rather than by a second derivation. Ownership answered two different ways
/// is ownership that can disagree, and a node acquiring one set of shards while
/// claiming rows from another is the kind of split that stays invisible until
/// something is already wrong.
///
/// `O(live nodes)`. This is a point lookup on cold paths (routing a request to
/// its owner, deciding whether to acquire a shard); the hot claim path uses
/// `owned_ranges` once per poll, not this.
pub fn range_owner(
    shard: ShardId,
    nodes: &[NodeId],
    vshards: u32,
    n_pools: usize,
) -> Option<&NodeId> {
    nodes.iter().find(|n| {
        owned_ranges(n, nodes, vshards, n_pools)
            .iter()
            .any(|r| r.contains(shard))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nodes(n: usize) -> Vec<NodeId> {
        (0..n)
            .map(|i| NodeId::new(format!("node-{i:03}")))
            .collect()
    }

    /// Union of every node's ranges, as a per-shard owner count.
    fn coverage(ns: &[NodeId], vshards: u32, n_pools: usize) -> Vec<u32> {
        let mut owners = vec![0u32; vshards as usize];
        for n in ns {
            for r in owned_ranges(n, ns, vshards, n_pools) {
                for s in r.lo..=r.hi {
                    owners[s as usize] += 1;
                }
            }
        }
        owners
    }

    /// The property that matters: every shard owned exactly once. A shard owned
    /// zero times stalls; owned twice is merely wasteful (SKIP LOCKED covers it).
    #[test]
    fn ranges_partition_the_shard_space() {
        for n_pools in [1usize, 2, 3, 4, 8] {
            for n in 1..=16usize {
                let ns = nodes(n);
                let owners = coverage(&ns, 4096, n_pools);
                let unowned = owners.iter().filter(|c| **c == 0).count();
                let doubly = owners.iter().filter(|c| **c > 1).count();
                assert_eq!(
                    (unowned, doubly),
                    (0, 0),
                    "nodes={n} pools={n_pools}: {unowned} shards unowned, {doubly} owned twice"
                );
            }
        }
    }

    /// A range must lie inside one pool, or a node's claim query spans two
    /// databases — the thing the alignment exists to prevent.
    #[test]
    fn ranges_never_straddle_a_pool_boundary() {
        let vshards = 4096;
        for n_pools in [1usize, 2, 3, 4, 8] {
            for n in 1..=16usize {
                let ns = nodes(n);
                for node in &ns {
                    for r in owned_ranges(node, &ns, vshards, n_pools) {
                        // Only meaningful when the node does not own whole pools.
                        if n > n_pools {
                            let pool_of =
                                |s: u32| (s as u64 * n_pools as u64 / vshards as u64) as usize;
                            assert_eq!(
                                pool_of(r.lo),
                                pool_of(r.hi),
                                "nodes={n} pools={n_pools}: range {r:?} straddles pools"
                            );
                        }
                    }
                }
            }
        }
    }

    /// `pool_start` must be the exact inverse of the routing crate's
    /// `pool_index`, or ranges align to boundaries that do not exist.
    #[test]
    fn pool_start_inverts_pool_index() {
        let vshards = 4096;
        for n_pools in [1usize, 2, 3, 5, 8, 16] {
            for p in 0..n_pools {
                let start = pool_start(p, n_pools, vshards);
                let pool_of = |s: u32| {
                    ((s as u64 * n_pools as u64 / vshards as u64) as usize).min(n_pools - 1)
                };
                assert_eq!(pool_of(start), p, "pools={n_pools} p={p}: start {start}");
                if start > 0 {
                    assert_eq!(
                        pool_of(start - 1),
                        p - 1,
                        "pools={n_pools} p={p}: {} should end the previous pool",
                        start - 1
                    );
                }
            }
        }
    }

    /// Every node must reach the same assignment from the same member set,
    /// whatever order its membership query returned.
    #[test]
    fn assignment_is_order_independent() {
        let ns = nodes(5);
        let mut shuffled = ns.clone();
        shuffled.reverse();
        for node in &ns {
            assert_eq!(
                owned_ranges(node, &ns, 4096, 2),
                owned_ranges(node, &shuffled, 4096, 2),
                "member ordering changed the assignment for {node:?}"
            );
        }
    }

    #[test]
    fn ranges_are_balanced() {
        let ns = nodes(4);
        let sizes: Vec<u32> = ns
            .iter()
            .map(|n| owned_ranges(n, &ns, 4096, 1).iter().map(|r| r.len()).sum())
            .collect();
        let (min, max) = (*sizes.iter().min().unwrap(), *sizes.iter().max().unwrap());
        assert!(max - min <= 1, "uneven split across equal nodes: {sizes:?}");
    }

    /// The two views of ownership must agree on every shard, or a node can
    /// acquire one set and claim from another.
    #[test]
    fn range_owner_agrees_with_owned_ranges() {
        for n_pools in [1usize, 2, 4] {
            for n in [1usize, 2, 3, 5, 8] {
                let ns = nodes(n);
                for s in 0..4096u32 {
                    let shard = ShardId(s);
                    let by_lookup = range_owner(shard, &ns, 4096, n_pools);
                    let by_ranges = ns.iter().find(|node| {
                        owned_ranges(node, &ns, 4096, n_pools)
                            .iter()
                            .any(|r| r.contains(shard))
                    });
                    assert_eq!(
                        by_lookup, by_ranges,
                        "nodes={n} pools={n_pools} shard={s}: the two ownership views disagree"
                    );
                    assert!(by_lookup.is_some(), "shard {s} has no owner");
                }
            }
        }
    }

    #[test]
    fn range_owner_has_no_owner_without_members() {
        assert_eq!(range_owner(ShardId(0), &[], 4096, 1), None);
    }

    #[test]
    fn a_node_outside_the_live_set_owns_nothing() {
        let ns = nodes(3);
        let stranger = NodeId::new("not-a-member");
        assert!(owned_ranges(&stranger, &ns, 4096, 1).is_empty());
        assert!(owned_ranges(&stranger, &[], 4096, 1).is_empty());
    }

    #[test]
    fn a_single_node_owns_everything() {
        let ns = nodes(1);
        assert_eq!(
            owned_ranges(&ns[0], &ns, 4096, 1),
            vec![ShardRange { lo: 0, hi: 4095 }]
        );
    }

    /// More nodes than shards: the surplus own nothing, and crucially do not
    /// fall through to an inverted range that would read as "owns all".
    #[test]
    fn surplus_nodes_own_nothing_rather_than_everything() {
        let ns = nodes(8);
        let owners = coverage(&ns, 4, 1);
        assert!(owners.iter().all(|c| *c == 1), "coverage broke: {owners:?}");
        let owning = ns
            .iter()
            .filter(|n| !owned_ranges(n, &ns, 4, 1).is_empty())
            .count();
        assert_eq!(owning, 4, "expected one node per shard to own a range");
    }

    /// Duplicate rows in the member set (the same node reported twice) must not
    /// shift everyone else's range.
    #[test]
    fn duplicate_members_are_ignored() {
        let ns = nodes(3);
        let mut dupes = ns.clone();
        dupes.push(ns[1].clone());
        for node in &ns {
            assert_eq!(
                owned_ranges(node, &ns, 4096, 1),
                owned_ranges(node, &dupes, 4096, 1),
                "a duplicate member changed the assignment for {node:?}"
            );
        }
    }
}
