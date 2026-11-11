//! Which shards this node claims from — the seam that turns cluster ownership
//! into a predicate on a claim query.
//!
//! Rows now carry a `shard_id`, and the cluster layer can say which contiguous
//! ranges of shards a node owns. This trait joins the two without this crate
//! depending on the cluster layer: it is defined here, implemented by
//! `orcher-cluster`, and injected into repositories at startup. Absent (the
//! default, and every single-node deployment), claims are unfiltered — byte for
//! byte the query that ran before.
//!
//! # Fail open, always
//!
//! Ownership decides who *looks* at a row, never who may write it; exactly-once
//! rests on `FOR UPDATE SKIP LOCKED` and is unaffected by anything here. That
//! asymmetry sets the safe direction for every ambiguous case:
//!
//! - Two nodes both believing they own a shard is **harmless** — they contend on
//!   the row lock and one wins.
//! - No node believing it owns a shard is **not** — those rows go unclaimed, and
//!   the workflows on them stop making progress.
//!
//! So every uncertain path here widens the claim rather than narrowing it: an
//! empty range set means "ownership unknown, claim everything", and rows with a
//! NULL `shard_id` (written before the column was populated, or by a path with
//! no router) stay claimable by every node.
//!
//! # Why an unfiltered sweep is mandatory
//!
//! Fail-open on *unknown* ownership is not enough, because the dangerous case is
//! ownership that is confidently **wrong**: a node whose heartbeat row lingers
//! after it dies still appears live, keeps its range, and claims nothing from it.
//! Nothing self-corrects — those shards stall indefinitely.
//!
//! [`ClaimScope::sweep_interval`] is the guard. One poll in every `N` ignores the
//! filter entirely, so no range can stall for longer than `N` polls no matter how
//! wrong the membership view is. It costs `1/N` of polls scanning globally, which
//! is the price of not having a stall mode that requires an operator to notice.

/// The shard ranges this node claims from.
pub trait ClaimScope: Send + Sync + std::fmt::Debug {
    /// Inclusive `(lo, hi)` shard ranges owned by this node.
    ///
    /// **Empty means claim everything**, not "claim nothing" — see the fail-open
    /// note above. A node that has not yet registered its heartbeat, or that has
    /// dropped out of the live set, must keep working rather than go silent.
    fn owned_ranges(&self) -> Vec<(i32, i32)>;

    /// Run one poll in every `N` unfiltered, so a stale membership view cannot
    /// strand a range forever. `1` disables filtering entirely; `0` disables the
    /// sweep and is only safe when membership is known to be exact.
    fn sweep_interval(&self) -> u32 {
        20
    }

    /// True when this particular poll should ignore the filter. Implementations
    /// count their own polls; the default never sweeps.
    fn should_sweep(&self) -> bool {
        false
    }
}

/// Bind parameters for the shard predicate, or `None` to run unfiltered.
///
/// `min`/`max` bound every range and exist so the planner can prune with the
/// `shard_id` index before evaluating the exact test. With one range per node —
/// what the cluster layer assigns today — the bound *is* the exact answer and the
/// `unnest` test is a no-op. With several ranges per node (the movement
/// optimisation) the bound stays a useful prefilter and the exact test does the
/// rest, so the SQL does not change when that lands.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClaimBounds {
    /// Lower bound of each owned range, paired positionally with `his`.
    pub los: Vec<i32>,
    /// Upper bound of each owned range, paired positionally with `los`.
    pub his: Vec<i32>,
    /// Lowest shard across every range, for the index prefilter.
    pub min: i32,
    /// Highest shard across every range, for the index prefilter.
    pub max: i32,
}

impl ClaimBounds {
    /// Build bounds from a scope, or `None` when this poll must not filter:
    /// no scope injected, ownership unknown, or a sweep poll.
    pub fn for_poll(scope: Option<&std::sync::Arc<dyn ClaimScope>>) -> Option<Self> {
        let scope = scope?;
        if scope.should_sweep() {
            return None;
        }
        let ranges = scope.owned_ranges();
        if ranges.is_empty() {
            return None;
        }
        let los: Vec<i32> = ranges.iter().map(|r| r.0).collect();
        let his: Vec<i32> = ranges.iter().map(|r| r.1).collect();
        let min = *los.iter().min()?;
        let max = *his.iter().max()?;
        Some(ClaimBounds { los, his, min, max })
    }

    /// Build bounds from exactly the ranges this node owns, or `None` when no
    /// scope is injected or ownership is unknown. No sweep: for a pass that
    /// only this node's shards should run — every node runs it over its own
    /// ranges, and between them they cover every row.
    pub fn for_owned(scope: Option<&std::sync::Arc<dyn ClaimScope>>) -> Option<Self> {
        let ranges = scope?.owned_ranges();
        if ranges.is_empty() {
            return None;
        }
        let los: Vec<i32> = ranges.iter().map(|r| r.0).collect();
        let his: Vec<i32> = ranges.iter().map(|r| r.1).collect();
        let min = *los.iter().min()?;
        let max = *his.iter().max()?;
        Some(ClaimBounds { los, his, min, max })
    }

    /// Split into the four bind values, using `None` for an unfiltered poll.
    /// Returned rather than bound here so call sites keep their parameter order
    /// explicit and visible next to the SQL.
    pub fn params(
        this: Option<&Self>,
    ) -> (Option<Vec<i32>>, Option<Vec<i32>>, Option<i32>, Option<i32>) {
        match this {
            Some(b) => (
                Some(b.los.clone()),
                Some(b.his.clone()),
                Some(b.min),
                Some(b.max),
            ),
            None => (None, None, None, None),
        }
    }
}

/// The shard predicate, parameterised by the alias of the table being claimed
/// from and the index of the first of its four bind parameters.
///
/// Kept as one function so the three claim queries cannot drift apart — a filter
/// that is subtly stricter in one of them strands rows only on that path, which
/// is close to undetectable in a passing test suite.
///
/// Reads as: no filter requested, **or** the row predates shard routing, **or**
/// the row is inside this node's ranges.
pub fn shard_predicate(alias: &str, first_param: usize) -> String {
    let los = first_param;
    let his = first_param + 1;
    let min = first_param + 2;
    let max = first_param + 3;
    format!(
        "(${los}::int[] IS NULL \
           OR {alias}.shard_id IS NULL \
           OR ({alias}.shard_id BETWEEN ${min} AND ${max} \
               AND EXISTS (SELECT 1 FROM unnest(${los}::int[], ${his}::int[]) AS _r(lo, hi) \
                           WHERE {alias}.shard_id BETWEEN _r.lo AND _r.hi)))"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[derive(Debug)]
    struct Fixed {
        ranges: Vec<(i32, i32)>,
        sweeps: AtomicU32,
        sweep_every: u32,
    }

    impl ClaimScope for Fixed {
        fn owned_ranges(&self) -> Vec<(i32, i32)> {
            self.ranges.clone()
        }
        fn sweep_interval(&self) -> u32 {
            self.sweep_every
        }
        fn should_sweep(&self) -> bool {
            match self.sweep_every {
                0 => false, // sweep disabled
                1 => true,  // every poll unfiltered
                n => self.sweeps.fetch_add(1, Ordering::Relaxed) % n == 0,
            }
        }
    }

    fn scope(ranges: Vec<(i32, i32)>, sweep_every: u32) -> std::sync::Arc<dyn ClaimScope> {
        std::sync::Arc::new(Fixed {
            ranges,
            sweeps: AtomicU32::new(1),
            sweep_every,
        })
    }

    #[test]
    fn no_scope_means_no_filter() {
        assert_eq!(ClaimBounds::for_poll(None), None);
    }

    /// The whole fail-open contract in one assertion: unknown ownership must
    /// widen the claim, never silence the node.
    #[test]
    fn empty_ranges_mean_claim_everything_not_nothing() {
        let s = scope(vec![], 0);
        assert_eq!(
            ClaimBounds::for_poll(Some(&s)),
            None,
            "a node with no known ownership must claim unfiltered, or it stops working entirely"
        );
    }

    #[test]
    fn bounds_span_every_range() {
        let s = scope(vec![(100, 199), (400, 499)], 0);
        let b = ClaimBounds::for_poll(Some(&s)).unwrap();
        assert_eq!((b.min, b.max), (100, 499));
        assert_eq!(b.los, vec![100, 400]);
        assert_eq!(b.his, vec![199, 499]);
    }

    /// The stall guard: however wrong the membership view is, a range cannot go
    /// unclaimed for more than `sweep_interval` polls.
    #[test]
    fn one_poll_in_n_is_unfiltered() {
        let s = scope(vec![(0, 10)], 5);
        let unfiltered = (0..20)
            .filter(|_| ClaimBounds::for_poll(Some(&s)).is_none())
            .count();
        assert_eq!(unfiltered, 4, "expected every 5th poll to sweep");
    }

    #[test]
    fn predicate_numbers_its_parameters_in_order() {
        let sql = shard_predicate("oq", 4);
        assert!(sql.contains("$4::int[] IS NULL"), "{sql}");
        assert!(sql.contains("oq.shard_id BETWEEN $6 AND $7"), "{sql}");
        assert!(sql.contains("unnest($4::int[], $5::int[])"), "{sql}");
    }

    /// A row written before shard routing existed must stay claimable by
    /// everyone, or it is stranded permanently.
    #[test]
    fn predicate_admits_null_shards() {
        assert!(shard_predicate("oq", 4).contains("oq.shard_id IS NULL"));
    }
}
