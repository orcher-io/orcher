//! Task dispatch infrastructure — polling, capacity, routing, queues, and eager dispatch.

#![allow(missing_docs)]

pub mod buffer;
pub mod capacity;
pub mod eager;
pub mod polling;
pub mod queue;
pub mod routing;
pub mod sharding;
pub mod support;
pub mod traits;

pub mod prelude {
    pub use orcher_common::types::dispatch::*;

    pub use crate::buffer::{BufferPool, BufferPoolConfig, PooledBuffer};
    pub use crate::capacity::{CapacityRegistry, TaskPermits};
    pub use crate::eager::{EagerTaskDispatcher, WorkflowPredictor};
    pub use crate::polling::{Claimer, Notifier, Reclaimer};
    pub use crate::queue::{PartitionConfig, PartitionedTaskQueue};
    pub use crate::routing::{WorkerAffinityConfig, WorkerAffinityManager};
}

pub use buffer::{BufferPool, BufferPoolConfig, BufferPoolStats, PooledBuffer};
pub use capacity::{CapacityRegistry, TaskPermits};
pub use eager::{
    EagerDispatchConfig, EagerDispatchIntegration, EagerDispatchIntegrationBuilder,
    EagerDispatchable, EagerTask, EagerTaskDispatcher, PredictedTask, SelectionReason,
    WorkflowPredictor,
};
pub use polling::{Claimer, Notifier, Reclaimer};
pub use queue::{PartitionConfig, PartitionStats, PartitionedTaskQueue};
pub use routing::{WorkerAffinityConfig, WorkerAffinityManager};
pub use sharding::{
    HorizontalShardRouter, HorizontalShardingConfig, ShardConfig, ShardConfigRegistry,
    ShardCoordinator, ShardRouter, WorkerShardAssignment,
};
pub use support::AsyncWriter;
pub use traits::{JournalCache, TaskOutbox};
