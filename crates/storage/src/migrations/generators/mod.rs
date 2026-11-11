//! # Migration Generation Tools
//!
//! This module provides code generation tools for creating database migrations.
//! It includes generators for tables, indexes, constraints, and other database
//! schema elements with proper SQL generation and validation.

use crate::migrations::Migration;
use std::collections::HashMap;
use thiserror::Error;

/// Generator errors
#[derive(Error, Debug)]
pub enum GeneratorError {
    /// Invalid configuration
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),
    /// SQL generation failed
    #[error("SQL generation failed: {0}")]
    SqlGenerationFailed(String),
    /// Template not found
    #[error("Template not found: {0}")]
    TemplateNotFound(String),
}

/// Database column definition
#[derive(Debug, Clone)]
pub struct ColumnDefinition {
    /// Column name
    pub name: String,
    /// Column data type
    pub data_type: String,
    /// Whether column is nullable
    pub nullable: bool,
    /// Default value if any
    pub default: Option<String>,
    /// Whether column is primary key
    pub primary_key: bool,
    /// Whether column is unique
    pub unique: bool,
}

/// Database table definition
#[derive(Debug, Clone)]
pub struct TableDefinition {
    /// Table name
    pub name: String,
    /// Table columns
    pub columns: Vec<ColumnDefinition>,
    /// Table constraints
    pub constraints: Vec<String>,
    /// Table indexes
    pub indexes: Vec<String>,
}

/// Migration generator trait
pub trait MigrationGenerator {
    /// Generate SQL for the migration
    fn generate_sql(&self) -> Result<String, GeneratorError>;

    /// Get migration name
    fn get_name(&self) -> String;

    /// Get migration description
    fn get_description(&self) -> String;
}

/// Generator factory for creating migrations
pub struct GeneratorFactory {
    templates: HashMap<String, String>,
}

impl GeneratorFactory {
    /// Create new generator factory
    pub fn new() -> Self {
        let mut templates = HashMap::new();

        // Add basic SQL templates
        templates.insert(
            "create_table".to_string(),
            "CREATE TABLE {table_name} (\n{columns}\n);".to_string(),
        );

        templates.insert(
            "create_index".to_string(),
            "CREATE INDEX {index_name} ON {table_name} ({columns});".to_string(),
        );

        templates.insert(
            "add_constraint".to_string(),
            "ALTER TABLE {table_name} ADD CONSTRAINT {constraint_name} {constraint_definition};"
                .to_string(),
        );

        Self { templates }
    }

    /// Create migration from template
    pub fn create_migration(
        &self,
        version: u32,
        name: &str,
        template: &str,
        params: HashMap<String, String>,
    ) -> Result<Migration, GeneratorError> {
        let template_sql = self
            .templates
            .get(template)
            .ok_or_else(|| GeneratorError::TemplateNotFound(template.to_string()))?;

        let mut sql = template_sql.clone();
        for (key, value) in params {
            sql = sql.replace(&format!("{{{}}}", key), &value);
        }

        Ok(Migration::new(
            version,
            name,
            Box::leak(sql.into_boxed_str()),
        ))
    }

    /// Add custom template
    pub fn add_template(&mut self, name: String, template: String) {
        self.templates.insert(name, template);
    }
}

impl Default for GeneratorFactory {
    fn default() -> Self {
        Self::new()
    }
}

pub mod constraint;
pub mod index;
/// Generator modules
pub mod table;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generator_factory_creation() {
        let factory = GeneratorFactory::new();
        assert!(!factory.templates.is_empty());
        assert!(factory.templates.contains_key("create_table"));
    }

    #[test]
    fn test_column_definition() {
        let column = ColumnDefinition {
            name: "id".to_string(),
            data_type: "UUID".to_string(),
            nullable: false,
            default: Some("uuid_generate_v4()".to_string()),
            primary_key: true,
            unique: false,
        };

        assert_eq!(column.name, "id");
        assert_eq!(column.data_type, "UUID");
        assert!(!column.nullable);
        assert!(column.primary_key);
    }

    #[test]
    fn test_table_definition() {
        let column = ColumnDefinition {
            name: "id".to_string(),
            data_type: "UUID".to_string(),
            nullable: false,
            default: Some("uuid_generate_v4()".to_string()),
            primary_key: true,
            unique: false,
        };

        let table = TableDefinition {
            name: "test_table".to_string(),
            columns: vec![column],
            constraints: vec![],
            indexes: vec![],
        };

        assert_eq!(table.name, "test_table");
        assert_eq!(table.columns.len(), 1);
    }

    #[test]
    fn test_migration_creation() {
        let factory = GeneratorFactory::new();
        let mut params = HashMap::new();
        params.insert("table_name".to_string(), "users".to_string());
        params.insert("columns".to_string(), "id UUID PRIMARY KEY".to_string());

        let migration = factory.create_migration(1, "create_users", "create_table", params);
        assert!(migration.is_ok());

        let migration = migration.unwrap();
        assert_eq!(migration.version.0, 1);
        assert_eq!(migration.name, "create_users");
        assert!(migration.sql.contains("CREATE TABLE users"));
    }

    #[test]
    fn test_template_not_found() {
        let factory = GeneratorFactory::new();
        let params = HashMap::new();

        let result = factory.create_migration(1, "test", "nonexistent", params);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            GeneratorError::TemplateNotFound(_)
        ));
    }

    #[test]
    fn test_add_custom_template() {
        let mut factory = GeneratorFactory::new();
        factory.add_template(
            "custom_template".to_string(),
            "CUSTOM SQL {param}".to_string(),
        );

        assert!(factory.templates.contains_key("custom_template"));
    }
}
