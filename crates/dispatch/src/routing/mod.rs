//! Task routing and worker affinity.

pub mod affinity;
pub mod config;

pub use orcher_common::types::dispatch::routing::{AffinityConfig, RoutingConfig, ShardingConfig};

pub use affinity::{AffinityLease, AffinityMetrics, WorkerAffinityConfig, WorkerAffinityManager};
