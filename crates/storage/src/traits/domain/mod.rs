//! # Domain Storage Traits
//!
//! This module contains domain-specific repository trait definitions using orcher-common
//! types for maximum compatibility and type safety across the ORCHER ecosystem.

/// Worker repository trait with domain-specific operations
pub mod worker_repository;

/// Schedule repository trait with domain-specific operations
pub mod schedule_repository;

/// Namespace repository trait with domain-specific operations
pub mod namespace_repository;

/// Coordination queue repository trait with domain-specific operations
pub mod coordination_repository;

// Re-export domain traits for convenience
pub use coordination_repository::CoordinationRepositoryTrait;
pub use namespace_repository::NamespaceRepositoryTrait;
pub use schedule_repository::ScheduleRepositoryTrait;
pub use worker_repository::WorkerRepositoryTrait;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_traits_module_exports() {
        // Compile-time test to ensure all traits are properly exported
        fn _assert_traits_available<T, WorkR, SR, CoordR>()
        where
            T: Clone + Send + Sync + orcher_common::validation::Validatable + 'static,
            WorkR: WorkerRepositoryTrait,
            SR: ScheduleRepositoryTrait,
            CoordR: CoordinationRepositoryTrait,
        {
        }
    }
}
