//! # Worker Domain Module
//!
//! Worker persistence layer for ORCHER worker management.
//!
//! This module provides data persistence for workers only. Business logic
//! for worker orchestration lives in `orcher-worker` crate.
//!
//! ## Responsibilities
//!
//! - **Data Persistence Only**: CRUD operations for worker entities
//! - **Query Support**: Worker-specific queries (by status, capability, capacity)
//! - **No Business Logic**: Worker scheduling, task assignment logic lives in domain crates
//!
//! ## Structure
//!
//! - `repository.rs`: Main worker repository implementation
//! - Future expansions for worker-specific data models

/// Worker repository implementation
pub mod repository;

/// The workers that have said they are shutting down
pub mod shutdowns;

/// When each worker process was last seen polling
pub mod instances;

// Re-export main types for convenience
pub use instances::{LapsedInstance, SeenInstance, WorkerInstanceRepository};
pub use repository::WorkerRepository;
pub use shutdowns::{
    ttl_for, ShutdownLimits, WorkerShutdown, WorkerShutdownRepository, MAX_NOTICE_BYTES,
    WORKER_SHUTDOWN_CHANNEL,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_worker_module_exports() {
        // Compile-time test to ensure WorkerRepository is properly exported
        fn _assert_worker_repository_available<T>()
        where
            T: Clone + Send + Sync + std::fmt::Debug,
        {
        }
        _assert_worker_repository_available::<WorkerRepository>();
    }
}
