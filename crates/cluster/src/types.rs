//! Core cluster identity types: the node id, the virtual shard id, and the
//! ownership generation. Deliberately plain data — mapping/ownership logic lives
//! in [`crate::shard`], and the resolution seam in [`crate::traits`].

/// Monotonic per-shard ownership generation (the fencing token; used once
/// ownership gates the write path).
pub type Generation = u64;

/// Stable identity of an orchestrator node (e.g. `host:port` or a UUID string).
#[derive(Clone, PartialEq, Eq, Hash, Debug, PartialOrd, Ord)]
pub struct NodeId(pub String);

impl NodeId {
    pub fn new(id: impl Into<String>) -> Self {
        NodeId(id.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A virtual shard id in `[0, vshards)`. The unit of ownership and serialization.
///
/// Construction from a workflow id (`ShardId::of`) and the default shard count
/// live in [`crate::shard::id`].
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, PartialOrd, Ord)]
pub struct ShardId(pub u32);
