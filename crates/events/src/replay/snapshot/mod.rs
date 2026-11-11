//! Snapshot management for optimizing event replay by saving/restoring state checkpoints.

pub mod types;

// Re-export main types
pub use types::{Snapshot, SnapshotConfig, SnapshotManager, SnapshotStore, SnapshotStrategy};
