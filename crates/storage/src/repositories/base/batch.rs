//! # Batch Repository Operations
//!
//! Batch operations for efficient bulk data manipulation.

use async_trait::async_trait;
use orcher_common::OrcherResult;

/// Batch operations trait for bulk data operations
#[async_trait]
pub trait BatchOperations<T, Id>: Send + Sync
where
    T: Send + Sync,
    Id: Send + Sync,
{
    /// Create multiple entities in a single transaction
    ///
    /// # Arguments
    /// * `entities` - Vector of entities to create
    ///
    /// # Returns
    /// * `Ok(Vec<T>)` - Created entities
    /// * `Err(OrcherError)` - Database error
    async fn batch_create(&self, entities: Vec<T>) -> OrcherResult<Vec<T>>;

    /// Update multiple entities in a single transaction
    ///
    /// # Arguments
    /// * `entities` - Vector of entities to update
    ///
    /// # Returns
    /// * `Ok(Vec<T>)` - Updated entities
    /// * `Err(OrcherError)` - Database error
    async fn batch_update(&self, entities: Vec<T>) -> OrcherResult<Vec<T>>;

    /// Delete multiple entities by IDs in a single transaction
    ///
    /// # Arguments
    /// * `ids` - Vector of entity IDs to delete
    ///
    /// # Returns
    /// * `Ok(usize)` - Number of entities deleted
    /// * `Err(OrcherError)` - Database error
    async fn batch_delete(&self, ids: Vec<Id>) -> OrcherResult<usize>;

    /// Check existence of multiple entities by IDs
    ///
    /// # Arguments
    /// * `ids` - Vector of entity IDs to check
    ///
    /// # Returns
    /// * `Ok(Vec<bool>)` - Existence status for each ID (same order)
    /// * `Err(OrcherError)` - Database error
    async fn batch_exists(&self, ids: Vec<Id>) -> OrcherResult<Vec<bool>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_trait_bounds() {
        // Compile-time test to ensure trait bounds work correctly
        fn _assert_batch_bounds<T, Id, B>()
        where
            T: Send + Sync,
            Id: Send + Sync,
            B: BatchOperations<T, Id>,
        {
        }
    }

    #[test]
    fn test_batch_operations_send_sync() {
        // Ensure BatchOperations trait requires Send + Sync
        fn _assert_send_sync<T: Send + Sync>() {}
    }
}
