//! # Core Repository Traits
//!
//! Storage repository abstractions using orcher-common types for type safety and consistency.

use async_trait::async_trait;
use orcher_common::{
    types::identifiers::traits::Identifier, validation::Validatable, OrcherResult,
};
use std::fmt::Debug;

/// Generic repository trait for CRUD operations with orcher-common integration
#[async_trait]
pub trait Repository<T>: Send + Sync
where
    T: Clone + Send + Sync + Validatable + 'static,
{
    /// The identifier type for this entity
    type Id: Send + Sync + Debug + Clone + Identifier + 'static;

    /// Create a new entity with validation
    async fn create(&self, entity: T) -> OrcherResult<T> {
        // Pre-persist validation using orcher-common framework
        let validation_result = entity.validate();
        if !validation_result.is_valid() {
            return Err(crate::error::StorageError::ValidationFailed {
                field: "entity".to_string(),
                message: format!(
                    "Validation failed with {} errors",
                    validation_result.error_count()
                ),
            }
            .into());
        }
        self.create_validated(entity).await
    }

    /// Create entity without validation (assumes pre-validated)
    async fn create_validated(&self, entity: T) -> OrcherResult<T>;

    /// Find entity by ID
    async fn find_by_id(&self, id: Self::Id) -> OrcherResult<Option<T>>;

    /// Update an existing entity with validation
    async fn update(&self, entity: T) -> OrcherResult<T> {
        // Pre-update validation using orcher-common framework
        let validation_result = entity.validate();
        if !validation_result.is_valid() {
            return Err(crate::error::StorageError::ValidationFailed {
                field: "entity".to_string(),
                message: format!(
                    "Validation failed with {} errors",
                    validation_result.error_count()
                ),
            }
            .into());
        }
        self.update_validated(entity).await
    }

    /// Update entity without validation (assumes pre-validated)
    async fn update_validated(&self, entity: T) -> OrcherResult<T>;

    /// Delete entity by ID
    async fn delete(&self, id: Self::Id) -> OrcherResult<bool>;

    /// Check if entity exists
    async fn exists(&self, id: Self::Id) -> OrcherResult<bool> {
        Ok(self.find_by_id(id).await?.is_some())
    }
}

/// Batch operations trait for efficient bulk operations
#[async_trait]
pub trait BatchRepository<T>: Repository<T>
where
    T: Clone + Send + Sync + Validatable + 'static,
{
    /// Create multiple entities in a batch
    async fn create_batch(&self, entities: Vec<T>) -> OrcherResult<Vec<T>>;

    /// Update multiple entities in a batch
    async fn update_batch(&self, entities: Vec<T>) -> OrcherResult<Vec<T>>;

    /// Delete multiple entities by IDs
    async fn delete_batch(&self, ids: Vec<Self::Id>) -> OrcherResult<usize>;

    /// Find multiple entities by IDs
    async fn find_batch(&self, ids: Vec<Self::Id>) -> OrcherResult<Vec<T>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_repository_trait_bounds() {
        // Compile-time test to ensure trait bounds work correctly
        fn _assert_repository_bounds<T, R>()
        where
            T: Clone + Send + Sync + Validatable + 'static,
            R: Repository<T>,
        {
        }
    }

    #[test]
    fn test_batch_repository_trait_bounds() {
        // Compile-time test for batch repository trait bounds
        fn _assert_batch_repository_bounds<T, R>()
        where
            T: Clone + Send + Sync + Validatable + 'static,
            R: BatchRepository<T>,
        {
        }
    }
}
