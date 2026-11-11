//! Task dispatch subsystem.

/// Polling manager - orchestrates long-polling for workflow execution
pub mod polling_manager;

pub mod dead_node_claims;

/// Handing out a delayed start's run once it is due
pub mod delayed_starts;

/// Releasing the claims of a worker process nobody sees polling any more
pub mod lapsed_worker_claims;

/// Converter - transforms DB types to polling response types
pub mod converter;

/// Waiters registry - sync-match dispatch with sub-ms latency
pub mod waiters;

/// Task polling manager - long-polling for task execution from outbox
pub mod task_manager;

/// Cached workflow runner - optimized execution with state caching
pub mod cached_runner;

// Re-export key types for convenience
pub use cached_runner::CachedWorkflowRunner;
pub use converter::Converter;
pub use dead_node_claims::{DeadNodeClaims, Swept};
pub use delayed_starts::DelayedStarts;
pub use lapsed_worker_claims::{LapsedSwept, LapsedWorkerClaims, LivenessSettings};
pub use polling_manager::PollingManager;
pub use task_manager::TaskPollingManager;
pub use waiters::{RoutedTask, WaitersRegistry};
