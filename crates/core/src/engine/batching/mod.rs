//! Journal write batching for throughput.

pub mod batcher;
pub mod types;

pub use batcher::{WriteBatcher, WriteBatcherConfig, WriteBatcherHandle};
pub use types::{BatchedWrite, WriteOperation};
