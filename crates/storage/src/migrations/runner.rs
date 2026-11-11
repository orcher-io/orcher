//! # Migration Execution Engine
//!
//! This module provides the core migration execution engine for running
//! database schema migrations. It handles migration ordering, execution,
//! and result tracking.

use crate::migrations::versioning::VersionManager;
use crate::migrations::{Migration, MigrationResult, MigrationVersion};
use async_trait::async_trait;
use std::time::Instant;

/// Migration execution context and database connection
#[async_trait]
pub trait MigrationContext: Send + Sync {
    /// Execute raw SQL and return affected rows
    async fn execute_sql(&mut self, sql: &str) -> Result<u64, String>;

    /// Check if schema_migrations table exists
    async fn schema_table_exists(&mut self) -> Result<bool, String>;

    /// Create schema_migrations table
    async fn create_schema_table(&mut self) -> Result<(), String>;
}

/// Migration runner for executing database migrations
pub struct MigrationRunner {
    version_manager: VersionManager,
    migrations: Vec<Migration>,
}

impl MigrationRunner {
    /// Create new migration runner
    pub fn new() -> Self {
        Self {
            version_manager: VersionManager::new(),
            migrations: Vec::new(),
        }
    }

    /// Add migration to runner
    pub fn add_migration(&mut self, migration: Migration) {
        self.migrations.push(migration);
        self.migrations.sort_by(|a, b| a.version.cmp(&b.version));
    }

    /// Run all pending migrations
    pub async fn run_migrations(
        &mut self,
        context: &mut dyn MigrationContext,
    ) -> Result<Vec<MigrationResult>, String> {
        // Ensure schema table exists
        if !context.schema_table_exists().await? {
            context.create_schema_table().await?;
        }

        let mut results = Vec::new();
        let migrations = self.migrations.clone();

        for migration in &migrations {
            let result = self.run_single_migration(migration, context).await?;
            results.push(result);
        }

        Ok(results)
    }

    /// Execute a single migration
    async fn run_single_migration(
        &mut self,
        migration: &Migration,
        context: &mut dyn MigrationContext,
    ) -> Result<MigrationResult, String> {
        // Skip if already applied
        if self.version_manager.is_applied(&migration.version) {
            return Ok(MigrationResult::Skipped);
        }

        let start_time = Instant::now();

        match context.execute_sql(migration.sql).await {
            Ok(_) => {
                let execution_time = start_time.elapsed().as_millis() as u64;
                self.version_manager
                    .record_migration(migration, execution_time);
                Ok(MigrationResult::Applied)
            }
            Err(error) => Ok(MigrationResult::Failed(error)),
        }
    }

    /// Get current schema version
    pub fn current_version(&self) -> Option<&MigrationVersion> {
        self.version_manager.current_version()
    }
}

impl Default for MigrationRunner {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockContext {
        schema_exists: bool,
        should_fail: bool,
    }

    #[async_trait]
    impl MigrationContext for MockContext {
        async fn execute_sql(&mut self, _sql: &str) -> Result<u64, String> {
            if self.should_fail {
                Err("Mock execution failed".to_string())
            } else {
                Ok(1)
            }
        }

        async fn schema_table_exists(&mut self) -> Result<bool, String> {
            Ok(self.schema_exists)
        }

        async fn create_schema_table(&mut self) -> Result<(), String> {
            self.schema_exists = true;
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_runner_creation() {
        let runner = MigrationRunner::new();
        assert!(runner.current_version().is_none());
        assert_eq!(runner.migrations.len(), 0);
    }

    #[tokio::test]
    async fn test_add_migration() {
        let mut runner = MigrationRunner::new();
        let migration = Migration::new(1, "test", "CREATE TABLE test;");

        runner.add_migration(migration);
        assert_eq!(runner.migrations.len(), 1);
    }

    #[tokio::test]
    async fn test_migration_execution() {
        let mut runner = MigrationRunner::new();
        let migration = Migration::new(1, "test", "CREATE TABLE test;");
        runner.add_migration(migration);

        let mut context = MockContext {
            schema_exists: false,
            should_fail: false,
        };

        let results = runner.run_migrations(&mut context).await.unwrap();
        assert_eq!(results.len(), 1);

        match &results[0] {
            MigrationResult::Applied => (),
            _ => panic!("Expected Applied result"),
        }
    }

    #[tokio::test]
    async fn test_migration_failure() {
        let mut runner = MigrationRunner::new();
        let migration = Migration::new(1, "test", "CREATE TABLE test;");
        runner.add_migration(migration);

        let mut context = MockContext {
            schema_exists: true,
            should_fail: true,
        };

        let results = runner.run_migrations(&mut context).await.unwrap();
        assert_eq!(results.len(), 1);

        match &results[0] {
            MigrationResult::Failed(_) => (),
            _ => panic!("Expected Failed result"),
        }
    }
}
