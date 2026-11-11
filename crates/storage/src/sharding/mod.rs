//! # Sharding Module
//!
//! The seams through which cluster shard ownership reaches persistence, without
//! this crate depending on the cluster layer.
//!
//! A workflow hashes to a virtual shard. Two independent questions follow from
//! that shard, and each gets a trait here — defined in this crate, implemented by
//! `orcher-cluster`, and injected into repositories at startup:
//!
//! | Question | Trait | Answers |
//! |---|---|---|
//! | *Where does this row live?* | [`ShardPoolResolver`] | shard → database pool |
//! | *Which rows may this node claim?* | [`ClaimScope`] | node → owned shard ranges |
//! | *May this node still write it?* | [`WriteFence`] | shard → ownership generation |
//!
//! ```text
//! sharding/
//! ├── pool_routing.rs   # ShardPoolResolver — shard → database pool (writes)
//! ├── claim_scope.rs    # ClaimScope        — node → owned shards (claims)
//! └── write_fence.rs    # WriteFence        — shard → generation (authority)
//! ```
//!
//! # Both are absent by default
//!
//! With neither injected — every single-node, single-database deployment — a
//! repository uses its own pool and claims unfiltered, byte for byte the
//! behaviour that predates partitioning. Nothing here is on by default.
//!
//! # They share one derivation, deliberately
//!
//! [`ShardPoolResolver::shard_of`] is the single place a shard is computed, and
//! the value it returns is what gets persisted to a row's `shard_id`. Routing and
//! claiming therefore cannot disagree about which shard a row belongs to. A
//! second, independent derivation — a SQL generated column, a re-hash on the
//! claim side — could drift, and drift here sends work to a node that does not
//! own it, unrecoverable without a backfill.
//!
//! # Different safety directions
//!
//! Routing must be *exact*: send a write to the wrong pool and the row is
//! effectively lost. Claiming is an *optimisation* — it decides who looks at a
//! row, never who may write it. So [`ClaimScope`] fails **open** wherever it is
//! uncertain, which is why its module carries the longer safety note.
//!
//! [`WriteFence`] is the opposite and fails **closed**. It answers whether this
//! node still holds the authority its in-flight work assumed, which
//! `FOR UPDATE SKIP LOCKED` cannot: a lock only exists inside a live
//! transaction, so it says nothing about a stalled owner that resumes and
//! writes after its claim has already been reassigned.

pub mod claim_scope;
pub mod pool_routing;
pub mod write_fence;

pub use claim_scope::{shard_predicate, ClaimBounds, ClaimScope};
pub use pool_routing::ShardPoolResolver;
pub use write_fence::{is_ownership_lost, FenceToken, WriteFence, OWNERSHIP_LOST};
