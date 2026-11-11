//! Sharding for distributing work across orchestrator instances.

pub mod config_registry;
pub mod coordinator;
pub mod horizontal;
pub mod worker;

#[cfg(test)]
mod integration_test;

pub use config_registry::ShardConfigRegistry;
pub use coordinator::{ShardCoordinator, ShardCoordinatorError};
pub use horizontal::{
    compute_ownership_change, HorizontalShardMetrics, HorizontalShardRouter,
    HorizontalShardingConfig, ShardOwnershipChange, ShardRedirectInfo,
};
pub use worker::{ShardConfig, ShardRouter, WorkerShardAssignment};
