//! # Repositories Module
//!
//! Data access layer using the repository pattern.
//!
//! ## Structure
//!
//! - `base/`: Base repository implementations with generic CRUD operations
//! - `domains/`: Domain-specific repositories for data persistence only
//!
//! ## Design Principles
//!
//! 1. **Single Responsibility**: Repositories handle data persistence ONLY
//! 2. **No Business Logic**: Domain logic lives in orcher-workflow, orcher-core, etc.
//! 3. **Integration Over Implementation**: Use orcher-common types throughout
//! 4. **Separation of Concerns**: Base provides generic operations, domains provide specific ones

/// Base repository implementations
pub mod base;

/// The column lists repositories read back, in place of `*`
pub(crate) mod columns;

/// Domain-specific repositories
pub mod domains;

/// ExecutionLog repository (v2.2 durable execution)
pub mod execution_log;

// Re-export commonly used base types
pub use base::{
    crud::CrudOperations,
    pagination::{Page, Pagination},
    query::{FilterOperator, QueryFilter, QueryOptions, SortDirection, SortField},
    validation::validate_entity,
};

// Re-export ExecutionLog repository types
pub use execution_log::{
    ExecutionLogEntry, ExecutionLogFilter, ExecutionLogRepository, ExecutionLogStatus,
    ExecutionStepType, PostgresExecutionLogRepository, WorkflowExecutionStats,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_repositories_module_exports() {
        // Compile-time test to ensure all repository types are properly exported
        fn _assert_types_available() {
            fn _check_crud<T, Id, C>()
            where
                T: Send + Sync,
                Id: Send + Sync,
                C: CrudOperations<T, Id>,
            {
            }

            fn _check_pagination() {
                let _page = Pagination::default();
                let _options = QueryOptions::new();
            }
        }
    }

    #[test]
    fn test_base_types() {
        let pagination = Pagination::default();
        assert_eq!(pagination.page, 1);

        let options = QueryOptions::new();
        assert!(options.filters.is_empty());
    }
}
