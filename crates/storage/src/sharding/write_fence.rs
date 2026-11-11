//! Which shard generation this node may write under — the seam that lets a
//! repository refuse a write from an owner that has been superseded.
//!
//! Third of the sharding seams, and the one with the opposite failure
//! direction to the other two. [`ShardPoolResolver`](super::ShardPoolResolver)
//! picks where a row lives; [`ClaimScope`](super::ClaimScope) picks which rows
//! this node looks at and **fails open**. This one decides whether a write is
//! still authorised, and **fails closed**.
//!
//! # The failure it exists for
//!
//! `FOR UPDATE SKIP LOCKED` stops two nodes claiming the same row at the same
//! instant. It cannot stop this:
//!
//! 1. node A claims a workflow and begins processing
//! 2. A stalls — a long GC pause, a partition, a frozen VM
//! 3. A's claim expires, node B reclaims the workflow and processes it
//! 4. A resumes and writes
//!
//! A's write is a *new* transaction. Nothing is locked any more, so SKIP LOCKED
//! has no opinion about it, and A can overwrite status and step progress with a
//! view of the world that is several transitions out of date.
//!
//! A generation check rejects it: ownership moved, A's generation is stale, the
//! write fails. That is the whole job.
//!
//! # Why it fails closed
//!
//! A claim filter that is wrong costs throughput. A write fence that is wrong
//! costs correctness — the thing it exists to protect. So when this seam is
//! injected and cannot confirm ownership, the write is refused rather than
//! allowed through unguarded. Absent entirely (single node, clustering off) it
//! is not consulted at all and writes proceed exactly as they did before.
//!
//! # Cost
//!
//! Deliberately not a query per write. Implementations are expected to hold the
//! generation for a shard in memory after acquiring it once, so the hot path is
//! a map lookup and an integer compare; the database is still the arbiter,
//! because the generation rides into the transaction and is checked there.
//!
//! # What this becomes
//!
//! Ownership epochs are also what make an owner-local cache safe: a node may
//! cache state for a shard precisely because it holds that shard, and must drop
//! the cache when it loses it. [`WriteFence::guard`] returning `None` is that
//! signal, which is why this is worth having as a seam rather than a predicate
//! bolted onto one query.

use uuid::Uuid;

/// Carried in the message of every refusal the fence produces, wherever it is
/// raised, so a caller several layers up can tell "this node does not own the
/// workflow" from a genuine failure.
///
/// The distinction matters to whoever sent the request. A refusal is not an
/// error in the request: sent again — to the owner, or here once ownership has
/// settled — the same request succeeds. So the gRPC layer answers it as
/// `UNAVAILABLE`, which clients retry, rather than `INTERNAL`, which they do
/// not. The refusals reach it as `OrcherError::Internal` wrapped in further
/// context by each layer they pass through, and a marker in the text survives
/// that where a type would not.
pub const OWNERSHIP_LOST: &str = "shard ownership lost";

/// Whether an error message carries a fence refusal (see [`OWNERSHIP_LOST`]).
pub fn is_ownership_lost(message: &str) -> bool {
    message.contains(OWNERSHIP_LOST)
}

/// The shard and generation a write must be stamped with to be accepted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FenceToken {
    /// The shard the write belongs to.
    pub shard_id: i32,
    /// The ownership generation this node holds; a write stamped with an older
    /// one is refused, which is how a superseded owner is fenced off.
    pub generation: i64,
}

/// Authorises writes for the shards this node currently owns.
#[async_trait::async_trait]
pub trait WriteFence: Send + Sync + std::fmt::Debug {
    /// The token for `workflow_id`'s shard, or `None` when this node does not
    /// hold it — in which case the caller must **refuse the write**, not fall
    /// back to an unguarded one.
    ///
    /// Expected to be cheap and in-memory after the first acquisition for a
    /// given shard.
    async fn guard(&self, workflow_id: Uuid) -> Option<FenceToken>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct Fixed(Option<FenceToken>);

    #[async_trait::async_trait]
    impl WriteFence for Fixed {
        async fn guard(&self, _workflow_id: Uuid) -> Option<FenceToken> {
            self.0
        }
    }

    /// The contract callers depend on: `None` means refuse, and it must be
    /// distinguishable from "no fence injected" (which the caller models as
    /// `Option<Arc<dyn WriteFence>>` being absent).
    #[tokio::test]
    async fn a_lost_shard_yields_no_token() {
        let f = Fixed(None);
        assert!(f.guard(Uuid::new_v4()).await.is_none());
    }

    #[test]
    fn a_refusal_is_recognised_through_added_context() {
        let refusal = format!("{OWNERSHIP_LOST}: shard 12 held generation 1, current Some(2)");
        let wrapped = format!("Failed to process commands: Internal error: {refusal}");
        assert!(is_ownership_lost(&wrapped));
        assert!(!is_ownership_lost(
            "Failed to begin transaction: pool timed out"
        ));
    }

    #[tokio::test]
    async fn a_held_shard_yields_its_generation() {
        let f = Fixed(Some(FenceToken {
            shard_id: 2611,
            generation: 7,
        }));
        let t = f.guard(Uuid::new_v4()).await.unwrap();
        assert_eq!((t.shard_id, t.generation), (2611, 7));
    }
}
