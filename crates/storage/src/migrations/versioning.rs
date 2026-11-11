//! # Schema Version Management
//!
//! This module handles tracking and managing database schema versions.
//! It provides functionality to check current schema version, track applied migrations,
//! and ensure migration consistency across environments.

use crate::migrations::{Migration, MigrationVersion};
use orcher_common::types::time::Timestamptz;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Schema version information stored in database
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaVersion {
    /// Current schema version
    pub version: MigrationVersion,
    /// When this version was applied
    pub applied_at: Timestamptz,
    /// Checksum of the migration that created this version
    pub checksum: String,
    /// Whether this version is dirty (migration failed)
    pub dirty: bool,
}

/// Migration history record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationRecord {
    /// Migration version
    pub version: MigrationVersion,
    /// Migration name/description
    pub name: String,
    /// When migration was applied
    pub applied_at: Timestamptz,
    /// Execution time in milliseconds
    pub execution_time_ms: u64,
    /// Migration checksum
    pub checksum: String,
    /// Success status
    pub success: bool,
}

/// Version manager for tracking schema state
pub struct VersionManager {
    applied_migrations: HashMap<MigrationVersion, MigrationRecord>,
    current_version: Option<MigrationVersion>,
}

impl VersionManager {
    /// Create new version manager
    pub fn new() -> Self {
        Self {
            applied_migrations: HashMap::new(),
            current_version: None,
        }
    }

    /// Get current schema version
    pub fn current_version(&self) -> Option<&MigrationVersion> {
        self.current_version.as_ref()
    }

    /// Check if migration has been applied
    pub fn is_applied(&self, version: &MigrationVersion) -> bool {
        self.applied_migrations.contains_key(version)
    }

    /// Record successful migration application
    pub fn record_migration(&mut self, migration: &Migration, execution_time_ms: u64) {
        let record = MigrationRecord {
            version: migration.version.clone(),
            name: migration.name.clone(),
            applied_at: Timestamptz::now(),
            execution_time_ms,
            checksum: migration.checksum.clone(),
            success: true,
        };

        self.applied_migrations
            .insert(migration.version.clone(), record);
        self.current_version = Some(migration.version.clone());
    }
}

impl Default for VersionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrations::Migration;

    #[test]
    fn test_version_manager_new() {
        let manager = VersionManager::new();
        assert!(manager.current_version().is_none());
        assert!(!manager.is_applied(&MigrationVersion(1)));
    }

    #[test]
    fn test_record_migration() {
        let mut manager = VersionManager::new();
        let migration = Migration::new(1, "test", "CREATE TABLE test;");

        manager.record_migration(&migration, 100);

        assert_eq!(manager.current_version(), Some(&MigrationVersion(1)));
        assert!(manager.is_applied(&MigrationVersion(1)));
    }

    #[test]
    fn test_migration_record_fields() {
        let mut manager = VersionManager::new();
        let migration = Migration::new(5, "auth_tables", "CREATE TABLE users;");

        manager.record_migration(&migration, 250);

        let record = &manager.applied_migrations[&MigrationVersion(5)];
        assert_eq!(record.version.0, 5);
        assert_eq!(record.name, "auth_tables");
        assert_eq!(record.execution_time_ms, 250);
        assert!(record.success);
        assert!(!record.checksum.is_empty());
    }
}
