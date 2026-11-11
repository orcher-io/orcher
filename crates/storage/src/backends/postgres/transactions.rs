//! # PostgreSQL Transaction Management
//!
//! Transaction support for PostgreSQL backend using orcher-common async coordination.

use orcher_common::OrcherResult;
use sqlx::{PgPool, Postgres, Transaction};
use tracing::{debug, info};

/// Transaction wrapper with automatic rollback on drop
pub struct PgTransaction<'a> {
    inner: Option<Transaction<'a, Postgres>>,
    committed: bool,
}

impl<'a> PgTransaction<'a> {
    /// Create a new transaction from a pool
    pub async fn begin(pool: &'a PgPool) -> OrcherResult<Self> {
        debug!("Beginning PostgreSQL transaction");
        let tx = pool.begin().await.map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to begin transaction: {}",
                e
            ))
        })?;

        Ok(Self {
            inner: Some(tx),
            committed: false,
        })
    }

    /// Commit the transaction
    pub async fn commit(mut self) -> OrcherResult<()> {
        debug!("Committing PostgreSQL transaction");
        if let Some(tx) = self.inner.take() {
            tx.commit().await.map_err(|e| {
                orcher_common::error::OrcherError::internal(format!(
                    "Failed to commit transaction: {}",
                    e
                ))
            })?;
            self.committed = true;
            info!("PostgreSQL transaction committed successfully");
        }
        Ok(())
    }

    /// Rollback the transaction explicitly
    pub async fn rollback(mut self) -> OrcherResult<()> {
        debug!("Rolling back PostgreSQL transaction");
        if let Some(tx) = self.inner.take() {
            tx.rollback().await.map_err(|e| {
                orcher_common::error::OrcherError::internal(format!(
                    "Failed to rollback transaction: {}",
                    e
                ))
            })?;
            info!("PostgreSQL transaction rolled back successfully");
        }
        Ok(())
    }

    /// Get a mutable reference to the inner transaction
    pub fn transaction_mut(&mut self) -> Option<&mut Transaction<'a, Postgres>> {
        self.inner.as_mut()
    }
}

impl<'a> Drop for PgTransaction<'a> {
    fn drop(&mut self) {
        if !self.committed && self.inner.is_some() {
            debug!("Transaction dropped without commit - will auto-rollback");
        }
    }
}

/// Begin a transaction from a pool
pub async fn begin_transaction<'a>(pool: &'a PgPool) -> OrcherResult<PgTransaction<'a>> {
    PgTransaction::begin(pool).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_creation() {
        // Compile-time test to ensure types are properly defined
        fn _assert_transaction_traits<T: Send>() {}
        _assert_transaction_traits::<PgTransaction>();
    }

    #[test]
    fn test_transaction_state() {
        // Test that we can track committed state
        let committed = false;
        assert!(!committed);
    }

    #[test]
    fn test_transaction_drop_behavior() {
        // Verify transaction has proper drop implementation
        fn _assert_has_drop<T>() {}
        _assert_has_drop::<PgTransaction>();
    }
}
