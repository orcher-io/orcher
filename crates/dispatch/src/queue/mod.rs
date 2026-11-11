//! Partitioned task queue with work stealing.

pub mod partitioned;

pub use partitioned::{PartitionConfig, PartitionStats, PartitionedTaskQueue, TaskQueuePartition};
