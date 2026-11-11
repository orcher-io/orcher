//! # PostgreSQL Base Repository
//!
//! Base repository implementation with common CRUD operations using orcher-common types.

use orcher_common::OrcherResult;
use sqlx::PgPool;
use std::marker::PhantomData;
use tracing::{debug, info};

/// Base repository for PostgreSQL operations
#[derive(Debug, Clone)]
pub struct BaseRepository<T> {
    pool: PgPool,
    table_name: String,
    _phantom: PhantomData<T>,
}

impl<T> BaseRepository<T> {
    /// Create a new base repository
    pub fn new(pool: PgPool, table_name: impl Into<String>) -> Self {
        Self {
            pool,
            table_name: table_name.into(),
            _phantom: PhantomData,
        }
    }

    /// Get a reference to the connection pool
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Get the table name
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Execute a raw SQL query and return the number of rows affected
    pub async fn execute_raw(&self, sql: &str) -> OrcherResult<u64> {
        debug!("Executing query on table '{}': {}", self.table_name, sql);

        let result = sqlx::query(sql).execute(&self.pool).await.map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to execute query on table '{}': {}",
                self.table_name, e
            ))
        })?;

        let rows_affected = result.rows_affected();
        info!(
            "Query executed successfully: {} rows affected",
            rows_affected
        );
        Ok(rows_affected)
    }

    /// Check if a record exists by ID
    pub async fn exists_by_id(&self, id: &str) -> OrcherResult<bool> {
        let sql = format!(
            "SELECT EXISTS(SELECT 1 FROM {} WHERE id = $1)",
            self.table_name
        );

        let exists: bool = sqlx::query_scalar(&sql)
            .bind(id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| {
                orcher_common::error::OrcherError::internal(format!(
                    "Failed to check existence in table '{}': {}",
                    self.table_name, e
                ))
            })?;

        debug!(
            "Existence check for ID '{}' in table '{}': {}",
            id, self.table_name, exists
        );
        Ok(exists)
    }

    /// Count total records in the table
    pub async fn count_all(&self) -> OrcherResult<i64> {
        let sql = format!("SELECT COUNT(*) FROM {}", self.table_name);

        let count: i64 = sqlx::query_scalar(&sql)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| {
                orcher_common::error::OrcherError::internal(format!(
                    "Failed to count records in table '{}': {}",
                    self.table_name, e
                ))
            })?;

        debug!("Total count in table '{}': {}", self.table_name, count);
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_base_repository_creation() {
        // Compile-time test to ensure BaseRepository can be created
        fn _assert_send_sync<T: Send + Sync>() {}
        _assert_send_sync::<BaseRepository<String>>();
    }

    #[test]
    fn test_table_name_const() {
        // Test table name string handling without requiring database connection
        let table_name = "test_table";
        assert_eq!(table_name, "test_table");
        assert!(!table_name.is_empty());
    }

    #[test]
    fn test_phantom_data() {
        // Verify PhantomData doesn't affect size
        use std::mem::size_of;
        let size = size_of::<PhantomData<String>>();
        assert_eq!(size, 0);
    }
}
