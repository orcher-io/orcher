//! # Migration Rollback Capabilities
//!
//! This module provides rollback functionality for database migrations.
//! It supports rolling back to specific versions and handles rollback
//! validation and safety checks.

use crate::migrations::versioning::VersionManager;
use crate::migrations::{Migration, MigrationVersion};
use async_trait::async_trait;
use std::time::Instant;

/// Rollback execution context
#[async_trait]
pub trait RollbackContext: Send + Sync {
    /// Execute rollback SQL and return affected rows
    async fn execute_rollback_sql(&mut self, sql: &str) -> Result<u64, String>;

    /// Check if rollback is safe for given version
    async fn validate_rollback(&mut self, version: &MigrationVersion) -> Result<bool, String>;
}

/// Rollback strategy for migrations
#[derive(Debug, Clone)]
pub enum RollbackStrategy {
    /// Roll back to specific version
    ToVersion(MigrationVersion),
    /// Roll back single step
    SingleStep,
    /// Roll back all migrations
    Complete,
}

/// Rollback operation result
#[derive(Debug)]
pub enum RollbackResult {
    /// Rollback was successful
    Success(MigrationVersion),
    /// Rollback was skipped (already at target)
    Skipped,
    /// Rollback failed with error
    Failed(String),
}

/// Migration rollback manager
pub struct RollbackManager {
    version_manager: VersionManager,
    rollback_migrations: Vec<Migration>,
}

impl RollbackManager {
    /// Create new rollback manager
    pub fn new() -> Self {
        Self {
            version_manager: VersionManager::new(),
            rollback_migrations: Vec::new(),
        }
    }

    /// Add rollback migration
    pub fn add_rollback(&mut self, migration: Migration) {
        self.rollback_migrations.push(migration);
        self.rollback_migrations
            .sort_by(|a, b| b.version.cmp(&a.version)); // Reverse order for rollback
    }

    /// Execute rollback with given strategy
    pub async fn rollback(
        &mut self,
        strategy: RollbackStrategy,
        context: &mut dyn RollbackContext,
    ) -> Result<Vec<RollbackResult>, String> {
        match strategy {
            RollbackStrategy::ToVersion(target) => self.rollback_to_version(target, context).await,
            RollbackStrategy::SingleStep => self.rollback_single_step(context).await,
            RollbackStrategy::Complete => self.rollback_complete(context).await,
        }
    }

    /// Roll back to specific version
    async fn rollback_to_version(
        &mut self,
        target: MigrationVersion,
        context: &mut dyn RollbackContext,
    ) -> Result<Vec<RollbackResult>, String> {
        let current = self.version_manager.current_version();

        if current.is_none() || current.unwrap() <= &target {
            return Ok(vec![RollbackResult::Skipped]);
        }

        let mut results = Vec::new();

        for migration in &self.rollback_migrations.clone() {
            if migration.version <= target {
                break;
            }

            if !context.validate_rollback(&migration.version).await? {
                return Ok(vec![RollbackResult::Failed(
                    "Rollback validation failed".to_string(),
                )]);
            }

            let result = self.execute_rollback(migration, context).await?;
            results.push(result);
        }

        Ok(results)
    }

    /// Roll back single migration step
    async fn rollback_single_step(
        &mut self,
        context: &mut dyn RollbackContext,
    ) -> Result<Vec<RollbackResult>, String> {
        if let Some(migration) = self.rollback_migrations.first().cloned() {
            let result = self.execute_rollback(&migration, context).await?;
            Ok(vec![result])
        } else {
            Ok(vec![RollbackResult::Skipped])
        }
    }

    /// Roll back all migrations
    async fn rollback_complete(
        &mut self,
        context: &mut dyn RollbackContext,
    ) -> Result<Vec<RollbackResult>, String> {
        let mut results = Vec::new();

        for migration in &self.rollback_migrations.clone() {
            let result = self.execute_rollback(migration, context).await?;
            results.push(result);
        }

        Ok(results)
    }

    /// Execute single rollback operation
    async fn execute_rollback(
        &mut self,
        migration: &Migration,
        context: &mut dyn RollbackContext,
    ) -> Result<RollbackResult, String> {
        let start_time = Instant::now();

        match context.execute_rollback_sql(migration.sql).await {
            Ok(_) => {
                let _execution_time = start_time.elapsed().as_millis() as u64;
                Ok(RollbackResult::Success(migration.version.clone()))
            }
            Err(error) => Ok(RollbackResult::Failed(error)),
        }
    }
}

impl Default for RollbackManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockRollbackContext {
        should_fail: bool,
        validation_result: bool,
    }

    #[async_trait]
    impl RollbackContext for MockRollbackContext {
        async fn execute_rollback_sql(&mut self, _sql: &str) -> Result<u64, String> {
            if self.should_fail {
                Err("Mock rollback failed".to_string())
            } else {
                Ok(1)
            }
        }

        async fn validate_rollback(&mut self, _version: &MigrationVersion) -> Result<bool, String> {
            Ok(self.validation_result)
        }
    }

    #[tokio::test]
    async fn test_rollback_manager_creation() {
        let manager = RollbackManager::new();
        assert_eq!(manager.rollback_migrations.len(), 0);
    }

    #[tokio::test]
    async fn test_add_rollback() {
        let mut manager = RollbackManager::new();
        let migration = Migration::new(1, "rollback_test", "DROP TABLE test;");

        manager.add_rollback(migration);
        assert_eq!(manager.rollback_migrations.len(), 1);
    }

    #[tokio::test]
    async fn test_single_step_rollback() {
        let mut manager = RollbackManager::new();
        let migration = Migration::new(1, "rollback_test", "DROP TABLE test;");
        manager.add_rollback(migration);

        let mut context = MockRollbackContext {
            should_fail: false,
            validation_result: true,
        };

        let results = manager
            .rollback(RollbackStrategy::SingleStep, &mut context)
            .await
            .unwrap();

        assert_eq!(results.len(), 1);
        match &results[0] {
            RollbackResult::Success(_) => (),
            _ => panic!("Expected Success result"),
        }
    }

    #[tokio::test]
    async fn test_rollback_failure() {
        let mut manager = RollbackManager::new();
        let migration = Migration::new(1, "rollback_test", "DROP TABLE test;");
        manager.add_rollback(migration);

        let mut context = MockRollbackContext {
            should_fail: true,
            validation_result: true,
        };

        let results = manager
            .rollback(RollbackStrategy::SingleStep, &mut context)
            .await
            .unwrap();

        assert_eq!(results.len(), 1);
        match &results[0] {
            RollbackResult::Failed(_) => (),
            _ => panic!("Expected Failed result"),
        }
    }
}
