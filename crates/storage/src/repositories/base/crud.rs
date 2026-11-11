//! # Base CRUD Repository
//!
//! Generic CRUD operations for all repository implementations.

use async_trait::async_trait;
use orcher_common::OrcherResult;

/// Base CRUD operations trait
#[async_trait]
pub trait CrudOperations<T, Id>: Send + Sync
where
    T: Send + Sync,
    Id: Send + Sync,
{
    /// Create a new entity
    async fn create(&self, entity: T) -> OrcherResult<T>;

    /// Find an entity by ID
    async fn find_by_id(&self, id: Id) -> OrcherResult<Option<T>>;

    /// Update an existing entity
    async fn update(&self, entity: T) -> OrcherResult<T>;

    /// Delete an entity by ID
    async fn delete(&self, id: Id) -> OrcherResult<bool>;

    /// Check if an entity exists
    async fn exists(&self, id: Id) -> OrcherResult<bool>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crud_trait_bounds() {
        // Compile-time test to ensure trait bounds work correctly
        fn _assert_crud_bounds<T, Id, C>()
        where
            T: Send + Sync,
            Id: Send + Sync,
            C: CrudOperations<T, Id>,
        {
        }
    }
}
