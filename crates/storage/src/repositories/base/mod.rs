//! # Base Repository Module
//!
//! Base repository implementations and generic operations.

/// Generic CRUD operations
pub mod crud;

/// Batch operations for bulk data manipulation
pub mod batch;

/// Query builder and filtering
pub mod query;

/// Pagination support
pub mod pagination;

/// Data validation before persistence
pub mod validation;

// Re-export main types for convenience
pub use batch::BatchOperations;
pub use crud::CrudOperations;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_base_module_exports() {
        // Compile-time test to ensure all base types are properly exported
        fn _assert_crud_available<T, Id, C>()
        where
            T: Send + Sync,
            Id: Send + Sync,
            C: CrudOperations<T, Id>,
        {
        }
    }
}
