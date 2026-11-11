//! # Table Generator
//!
//! This module provides table generation capabilities for database migrations.
//! It handles table creation, modification, and deletion with proper SQL
//! generation and validation.

use super::{ColumnDefinition, GeneratorError, MigrationGenerator, TableDefinition};

/// Table generator for creating table migration SQL
pub struct TableGenerator {
    table_definition: TableDefinition,
    operation: TableOperation,
}

/// Table operation types
#[derive(Debug, Clone)]
pub enum TableOperation {
    /// Create new table
    Create,
    /// Drop existing table
    Drop,
    /// Add column to existing table
    AddColumn(ColumnDefinition),
    /// Drop column from existing table
    DropColumn(String),
    /// Modify existing column
    ModifyColumn(ColumnDefinition),
}

impl TableGenerator {
    /// Create new table generator
    pub fn new(table_definition: TableDefinition, operation: TableOperation) -> Self {
        Self {
            table_definition,
            operation,
        }
    }

    /// Create table generator for new table
    pub fn create_table(table_definition: TableDefinition) -> Self {
        Self::new(table_definition, TableOperation::Create)
    }

    /// Create table generator for dropping table
    pub fn drop_table(table_name: String) -> Self {
        let table_definition = TableDefinition {
            name: table_name,
            columns: Vec::new(),
            constraints: Vec::new(),
            indexes: Vec::new(),
        };
        Self::new(table_definition, TableOperation::Drop)
    }

    /// Generate column definition SQL
    fn generate_column_sql(&self, column: &ColumnDefinition) -> String {
        let mut sql = format!("    {} {}", column.name, column.data_type);

        if !column.nullable {
            sql.push_str(" NOT NULL");
        }

        if let Some(default) = &column.default {
            sql.push_str(&format!(" DEFAULT {}", default));
        }

        if column.primary_key {
            sql.push_str(" PRIMARY KEY");
        }

        if column.unique && !column.primary_key {
            sql.push_str(" UNIQUE");
        }

        sql
    }

    /// Generate CREATE TABLE SQL
    fn generate_create_table_sql(&self) -> Result<String, GeneratorError> {
        let mut sql = format!("CREATE TABLE {} (\n", self.table_definition.name);

        // Generate column definitions
        let mut column_sqls = Vec::new();
        for column in &self.table_definition.columns {
            column_sqls.push(self.generate_column_sql(column));
        }

        // Add constraints
        for constraint in &self.table_definition.constraints {
            column_sqls.push(format!("    {}", constraint));
        }

        sql.push_str(&column_sqls.join(",\n"));
        sql.push_str("\n);");

        // Add indexes if any
        for index in &self.table_definition.indexes {
            sql.push_str(&format!("\n{}", index));
        }

        Ok(sql)
    }

    /// Generate DROP TABLE SQL
    fn generate_drop_table_sql(&self) -> Result<String, GeneratorError> {
        Ok(format!(
            "DROP TABLE IF EXISTS {};",
            self.table_definition.name
        ))
    }

    /// Generate ADD COLUMN SQL
    fn generate_add_column_sql(&self, column: &ColumnDefinition) -> Result<String, GeneratorError> {
        let column_sql = self.generate_column_sql(column);
        Ok(format!(
            "ALTER TABLE {} ADD COLUMN {};",
            self.table_definition.name,
            column_sql.trim_start()
        ))
    }

    /// Generate DROP COLUMN SQL
    fn generate_drop_column_sql(&self, column_name: &str) -> Result<String, GeneratorError> {
        Ok(format!(
            "ALTER TABLE {} DROP COLUMN {};",
            self.table_definition.name, column_name
        ))
    }

    /// Generate MODIFY COLUMN SQL
    fn generate_modify_column_sql(
        &self,
        column: &ColumnDefinition,
    ) -> Result<String, GeneratorError> {
        let column_sql = self.generate_column_sql(column);
        Ok(format!(
            "ALTER TABLE {} ALTER COLUMN {};",
            self.table_definition.name,
            column_sql.trim_start()
        ))
    }
}

impl MigrationGenerator for TableGenerator {
    fn generate_sql(&self) -> Result<String, GeneratorError> {
        match &self.operation {
            TableOperation::Create => self.generate_create_table_sql(),
            TableOperation::Drop => self.generate_drop_table_sql(),
            TableOperation::AddColumn(column) => self.generate_add_column_sql(column),
            TableOperation::DropColumn(column_name) => self.generate_drop_column_sql(column_name),
            TableOperation::ModifyColumn(column) => self.generate_modify_column_sql(column),
        }
    }

    fn get_name(&self) -> String {
        match &self.operation {
            TableOperation::Create => format!("create_{}", self.table_definition.name),
            TableOperation::Drop => format!("drop_{}", self.table_definition.name),
            TableOperation::AddColumn(column) => {
                format!("add_column_{}_{}", self.table_definition.name, column.name)
            }
            TableOperation::DropColumn(column_name) => {
                format!("drop_column_{}_{}", self.table_definition.name, column_name)
            }
            TableOperation::ModifyColumn(column) => {
                format!(
                    "modify_column_{}_{}",
                    self.table_definition.name, column.name
                )
            }
        }
    }

    fn get_description(&self) -> String {
        match &self.operation {
            TableOperation::Create => format!("Create {} table", self.table_definition.name),
            TableOperation::Drop => format!("Drop {} table", self.table_definition.name),
            TableOperation::AddColumn(column) => {
                format!(
                    "Add {} column to {} table",
                    column.name, self.table_definition.name
                )
            }
            TableOperation::DropColumn(column_name) => {
                format!(
                    "Drop {} column from {} table",
                    column_name, self.table_definition.name
                )
            }
            TableOperation::ModifyColumn(column) => {
                format!(
                    "Modify {} column in {} table",
                    column.name, self.table_definition.name
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_column() -> ColumnDefinition {
        ColumnDefinition {
            name: "id".to_string(),
            data_type: "UUID".to_string(),
            nullable: false,
            default: Some("uuid_generate_v4()".to_string()),
            primary_key: true,
            unique: false,
        }
    }

    fn create_test_table() -> TableDefinition {
        TableDefinition {
            name: "users".to_string(),
            columns: vec![
                create_test_column(),
                ColumnDefinition {
                    name: "username".to_string(),
                    data_type: "VARCHAR(100)".to_string(),
                    nullable: false,
                    default: None,
                    primary_key: false,
                    unique: true,
                },
            ],
            constraints: vec![
                "CONSTRAINT users_username_check CHECK (length(username) > 0)".to_string(),
            ],
            indexes: vec!["CREATE INDEX idx_users_username ON users (username);".to_string()],
        }
    }

    #[test]
    fn test_create_table_generator() {
        let table_def = create_test_table();
        let generator = TableGenerator::create_table(table_def);

        let sql = generator.generate_sql().unwrap();
        assert!(sql.contains("CREATE TABLE users"));
        assert!(sql.contains("id UUID"));
        assert!(sql.contains("PRIMARY KEY"));
        assert!(sql.contains("username VARCHAR(100)"));
        assert!(sql.contains("UNIQUE"));
        assert!(sql.contains("users_username_check"));
        assert!(sql.contains("CREATE INDEX idx_users_username"));
    }

    #[test]
    fn test_drop_table_generator() {
        let generator = TableGenerator::drop_table("users".to_string());

        let sql = generator.generate_sql().unwrap();
        assert_eq!(sql, "DROP TABLE IF EXISTS users;");
    }

    #[test]
    fn test_add_column_generator() {
        let table_def = TableDefinition {
            name: "users".to_string(),
            columns: Vec::new(),
            constraints: Vec::new(),
            indexes: Vec::new(),
        };

        let column = ColumnDefinition {
            name: "email".to_string(),
            data_type: "VARCHAR(255)".to_string(),
            nullable: true,
            default: None,
            primary_key: false,
            unique: false,
        };

        let generator = TableGenerator::new(table_def, TableOperation::AddColumn(column));

        let sql = generator.generate_sql().unwrap();
        assert!(sql.contains("ALTER TABLE users ADD COLUMN"));
        assert!(sql.contains("email VARCHAR(255)"));
    }

    #[test]
    fn test_drop_column_generator() {
        let table_def = TableDefinition {
            name: "users".to_string(),
            columns: Vec::new(),
            constraints: Vec::new(),
            indexes: Vec::new(),
        };

        let generator = TableGenerator::new(
            table_def,
            TableOperation::DropColumn("old_column".to_string()),
        );

        let sql = generator.generate_sql().unwrap();
        assert_eq!(sql, "ALTER TABLE users DROP COLUMN old_column;");
    }

    #[test]
    fn test_column_sql_generation() {
        let generator = TableGenerator::create_table(create_test_table());

        let column = ColumnDefinition {
            name: "test_col".to_string(),
            data_type: "INTEGER".to_string(),
            nullable: false,
            default: Some("0".to_string()),
            primary_key: false,
            unique: true,
        };

        let sql = generator.generate_column_sql(&column);
        assert!(sql.contains("test_col INTEGER"));
        assert!(sql.contains("NOT NULL"));
        assert!(sql.contains("DEFAULT 0"));
        assert!(sql.contains("UNIQUE"));
        assert!(!sql.contains("PRIMARY KEY"));
    }

    #[test]
    fn test_generator_name_generation() {
        let table_def = create_test_table();
        let generator = TableGenerator::create_table(table_def);

        assert_eq!(generator.get_name(), "create_users");
        assert_eq!(generator.get_description(), "Create users table");

        let drop_generator = TableGenerator::drop_table("users".to_string());
        assert_eq!(drop_generator.get_name(), "drop_users");
        assert_eq!(drop_generator.get_description(), "Drop users table");
    }
}
