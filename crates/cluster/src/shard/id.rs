//! Workflow → shard mapping: [`ShardId::of`] and the default virtual-shard count.

use super::hash::fnv1a;
use crate::types::ShardId;

/// Default virtual-shard count (design O1). Shards are cheap ownership tokens, so
/// a generous fixed count is effectively never the ceiling for one OSS cluster;
/// horizontal scale comes from distributing shards across nodes, not changing this.
pub const DEFAULT_VSHARDS: u32 = 4096;

impl ShardId {
    /// Map a workflow id to its virtual shard: `fnv1a(workflow_id) % vshards`.
    ///
    /// Uses a stable hash so every node — and every version — computes the same
    /// shard for the same workflow (required for consistent ownership).
    pub fn of(workflow_id: &str, vshards: u32) -> ShardId {
        debug_assert!(vshards > 0, "vshards must be > 0");
        ShardId((fnv1a(workflow_id.as_bytes()) % vshards as u64) as u32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_of_is_deterministic_and_in_range() {
        for wf in ["ord_1", "ord_2", "abc-123", ""] {
            let s = ShardId::of(wf, DEFAULT_VSHARDS);
            assert!(s.0 < DEFAULT_VSHARDS);
            assert_eq!(s, ShardId::of(wf, DEFAULT_VSHARDS), "must be stable");
        }
    }
}
