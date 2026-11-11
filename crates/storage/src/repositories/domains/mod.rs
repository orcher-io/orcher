//! # Domain-Specific Repositories Module
//!
//! Domain-specific repository implementations for data persistence.
//!
//! ## Structure
//!
//! Each domain has its own directory with specialized repository implementations:
//!
//! - `workflow/`: Workflow persistence (orcher-workflow owns logic)
//! - `scheduling/`: Schedule persistence (reserved for future use)
//! - `auth/`: Auth data persistence (orcher-security owns logic)
//! - `monitoring/`: Monitoring data persistence (orcher-monitoring owns logic)
//! - `cluster/`: Cluster data persistence (orcher-cluster owns logic)
//!
//! ## Design Principles
//!
//! 1. **Data Persistence Only**: No business logic in repositories
//! 2. **Domain Ownership**: Each domain crate owns its logic
//! 3. **Repository Responsibility**: Only handle CRUD and queries
//! 4. **Type Safety**: Use orcher-common types throughout

/// Actor state persistence (orcher-core owns logic, V026 migration; virtual-objects pattern)
pub mod actor;

/// Matching store: delivery-side task queue partitioned by (namespace, task_queue)
pub mod matching;

/// Journal entry persistence (orcher-core owns logic, V024 migration)
pub mod journal;

/// Child workflow execution persistence (orcher-core owns logic, V024 migration)
pub mod child_workflow;

/// Coordination queue persistence (orcher-core owns logic)
pub mod coordination;

/// Task execution persistence (orcher-core/orcher-worker own logic)
pub mod task_execution;

/// Workflow execution persistence (orcher-workflow owns logic)
pub mod workflow_execution;

/// Worker persistence (orcher-worker owns logic)
pub mod worker;

/// Schedule persistence (reserved for future use)
pub mod schedule;

/// Cluster persistence (orcher-cluster owns logic)
pub mod cluster;

/// Namespace persistence (orcher-storage owns namespace management)
pub mod namespace;

// Re-export domain repositories for convenience
pub use actor::ActorStateRepository;
pub use child_workflow::ChildWorkflowRepository;
pub use cluster::{begin_shard_guarded_tx, ClusterMembershipRepository};
pub use coordination::CoordinationRepository;
pub use journal::JournalRepository;
pub use matching::{MatchTask, MatchingStore, PgMatchingStore};
pub use namespace::NamespaceRepository;
pub use schedule::ScheduleRepository;
pub use task_execution::TaskExecutionRepository;
pub use worker::WorkerRepository;
pub use workflow_execution::WorkflowExecutionRepository;

// Re-export S3 types from backends (if feature enabled)
#[cfg(feature = "s3")]
pub use crate::backends::s3::{S3Client, S3Config, S3Storage};

// Backward compatibility aliases
#[cfg(feature = "s3")]
pub use S3Client as S3StorageClient;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_domains_module_exists() {
        // Compile-time test to ensure module structure is correct
        // Module existence is verified by successful compilation
    }

    #[test]
    fn test_task_execution_repository_export() {
        // Compile-time test to ensure TaskExecutionRepository is properly exported
        fn _assert_task_execution_repo<T: Clone + Send + Sync + std::fmt::Debug>() {}
        _assert_task_execution_repo::<TaskExecutionRepository>();
    }

    #[test]
    fn test_workflow_execution_repository_export() {
        // Compile-time test to ensure WorkflowExecutionRepository is properly exported
        fn _assert_workflow_execution_repo<T: Clone + Send + Sync + std::fmt::Debug>() {}
        _assert_workflow_execution_repo::<WorkflowExecutionRepository>();
    }

    #[test]
    fn test_worker_repository_export() {
        // Compile-time test to ensure WorkerRepository is properly exported
        fn _assert_worker_repo<T: Clone + Send + Sync + std::fmt::Debug>() {}
        _assert_worker_repo::<WorkerRepository>();
    }

    #[test]
    fn test_schedule_repository_export() {
        // Compile-time test to ensure ScheduleRepository is properly exported
        fn _assert_schedule_repo<T: Clone + Send + Sync + std::fmt::Debug>() {}
        _assert_schedule_repo::<ScheduleRepository>();
    }
}
