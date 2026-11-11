//! # Migration Validation and Testing
//!
//! This module provides validation functionality for database migrations.
//! It includes SQL syntax validation, safety checks, and migration testing
//! capabilities to ensure migration reliability.

use crate::migrations::{Migration, MigrationVersion};
use std::collections::HashSet;
use thiserror::Error;

/// Migration validation errors
#[derive(Error, Debug)]
pub enum ValidationError {
    /// SQL syntax error
    #[error("SQL syntax error: {0}")]
    SyntaxError(String),
    /// Unsafe operation detected
    #[error("Unsafe operation detected: {0}")]
    UnsafeOperation(String),
    /// Missing rollback migration
    #[error("Missing rollback migration for version {0}")]
    MissingRollback(MigrationVersion),
    /// Duplicate version
    #[error("Duplicate migration version: {0}")]
    DuplicateVersion(MigrationVersion),
    /// Invalid version sequence
    #[error("Invalid version sequence at {0}")]
    InvalidSequence(MigrationVersion),
}

/// Migration safety checker
pub struct MigrationValidator {
    unsafe_keywords: HashSet<String>,
    #[allow(dead_code)]
    required_patterns: Vec<String>,
}

impl MigrationValidator {
    /// Create new migration validator
    pub fn new() -> Self {
        let mut unsafe_keywords = HashSet::new();
        unsafe_keywords.insert("DROP DATABASE".to_string());
        unsafe_keywords.insert("TRUNCATE".to_string());
        unsafe_keywords.insert("DELETE FROM".to_string());

        Self {
            unsafe_keywords,
            required_patterns: vec!["CREATE".to_string(), "ALTER".to_string()],
        }
    }

    /// Validate single migration
    pub fn validate_migration(&self, migration: &Migration) -> Result<(), ValidationError> {
        self.check_sql_safety(migration.sql)?;
        self.validate_sql_structure(migration.sql)?;
        Ok(())
    }

    /// Validate migration sequence
    pub fn validate_sequence(&self, migrations: &[Migration]) -> Result<(), ValidationError> {
        let mut versions = HashSet::new();
        let mut sorted_versions = Vec::new();

        for migration in migrations {
            if versions.contains(&migration.version) {
                return Err(ValidationError::DuplicateVersion(migration.version.clone()));
            }
            versions.insert(migration.version.clone());
            sorted_versions.push(migration.version.0);
        }

        sorted_versions.sort();
        for (i, version) in sorted_versions.iter().enumerate() {
            if i > 0 && *version != sorted_versions[i - 1] + 1 {
                return Err(ValidationError::InvalidSequence(MigrationVersion(*version)));
            }
        }

        Ok(())
    }

    /// Check SQL for unsafe operations
    fn check_sql_safety(&self, sql: &str) -> Result<(), ValidationError> {
        let upper_sql = sql.to_uppercase();

        for keyword in &self.unsafe_keywords {
            if upper_sql.contains(keyword) {
                return Err(ValidationError::UnsafeOperation(keyword.clone()));
            }
        }

        Ok(())
    }

    /// Validate SQL structure
    fn validate_sql_structure(&self, sql: &str) -> Result<(), ValidationError> {
        if sql.trim().is_empty() {
            return Err(ValidationError::SyntaxError("Empty SQL".to_string()));
        }

        if !sql.trim().ends_with(';') {
            return Err(ValidationError::SyntaxError(
                "SQL must end with semicolon".to_string(),
            ));
        }

        Ok(())
    }
}

impl Default for MigrationValidator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validator_creation() {
        let validator = MigrationValidator::new();
        assert!(!validator.unsafe_keywords.is_empty());
        assert!(!validator.required_patterns.is_empty());
    }

    #[test]
    fn test_valid_migration() {
        let validator = MigrationValidator::new();
        let migration = Migration::new(1, "test", "CREATE TABLE test (id INT);");

        assert!(validator.validate_migration(&migration).is_ok());
    }

    #[test]
    fn test_unsafe_operation_detection() {
        let validator = MigrationValidator::new();
        let migration = Migration::new(1, "unsafe", "DROP DATABASE test;");

        let result = validator.validate_migration(&migration);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ValidationError::UnsafeOperation(_)
        ));
    }

    #[test]
    fn test_empty_sql_validation() {
        let validator = MigrationValidator::new();
        let migration = Migration::new(1, "empty", "");

        let result = validator.validate_migration(&migration);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ValidationError::SyntaxError(_)
        ));
    }

    #[test]
    fn test_missing_semicolon() {
        let validator = MigrationValidator::new();
        let migration = Migration::new(1, "no_semicolon", "CREATE TABLE test (id INT)");

        let result = validator.validate_migration(&migration);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ValidationError::SyntaxError(_)
        ));
    }

    #[test]
    fn test_sequence_validation() {
        let validator = MigrationValidator::new();
        let migrations = vec![
            Migration::new(1, "first", "CREATE TABLE test1;"),
            Migration::new(2, "second", "CREATE TABLE test2;"),
            Migration::new(3, "third", "CREATE TABLE test3;"),
        ];

        assert!(validator.validate_sequence(&migrations).is_ok());
    }

    #[test]
    fn test_duplicate_version_detection() {
        let validator = MigrationValidator::new();
        let migrations = vec![
            Migration::new(1, "first", "CREATE TABLE test1;"),
            Migration::new(1, "duplicate", "CREATE TABLE test2;"),
        ];

        let result = validator.validate_sequence(&migrations);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ValidationError::DuplicateVersion(_)
        ));
    }

    #[test]
    fn test_invalid_sequence_detection() {
        let validator = MigrationValidator::new();
        let migrations = vec![
            Migration::new(1, "first", "CREATE TABLE test1;"),
            Migration::new(3, "skipped", "CREATE TABLE test3;"), // Skips version 2
        ];

        let result = validator.validate_sequence(&migrations);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ValidationError::InvalidSequence(_)
        ));
    }
}
