//! # Constraint Generator
//!
//! This module provides constraint generation capabilities for database migrations.
//! It handles constraint creation, modification, and deletion with support for
//! primary keys, foreign keys, unique constraints, check constraints, and exclusion constraints.

use super::{GeneratorError, MigrationGenerator};

/// Constraint generator for creating constraint migration SQL
pub struct ConstraintGenerator {
    constraint_definition: ConstraintDefinition,
    operation: ConstraintOperation,
}

/// Constraint definition structure
#[derive(Debug, Clone)]
pub struct ConstraintDefinition {
    /// Constraint name
    pub name: String,
    /// Table name the constraint belongs to
    pub table_name: String,
    /// Constraint type
    pub constraint_type: ConstraintType,
    /// Columns involved in the constraint
    pub columns: Vec<String>,
    /// Additional constraint options
    pub options: Vec<String>,
}

/// Constraint type enumeration
#[derive(Debug, Clone)]
pub enum ConstraintType {
    /// Primary key constraint
    PrimaryKey,
    /// Foreign key constraint
    ForeignKey {
        /// Referenced table
        referenced_table: String,
        /// Referenced columns
        referenced_columns: Vec<String>,
        /// On delete action
        on_delete: Option<ForeignKeyAction>,
        /// On update action
        on_update: Option<ForeignKeyAction>,
    },
    /// Unique constraint
    Unique,
    /// Check constraint
    Check {
        /// Check expression
        expression: String,
    },
    /// Exclusion constraint
    Exclusion {
        /// Exclusion elements
        elements: Vec<ExclusionElement>,
    },
    /// Not null constraint
    NotNull {
        /// Column name
        column: String,
    },
}

/// Foreign key action enumeration
#[derive(Debug, Clone)]
pub enum ForeignKeyAction {
    /// No action
    NoAction,
    /// Restrict
    Restrict,
    /// Cascade
    Cascade,
    /// Set null
    SetNull,
    /// Set default
    SetDefault,
}

/// Exclusion constraint element
#[derive(Debug, Clone)]
pub struct ExclusionElement {
    /// Element expression
    pub expression: String,
    /// Operator class
    pub operator: String,
}

/// Constraint operation types
#[derive(Debug, Clone)]
pub enum ConstraintOperation {
    /// Add new constraint
    Add,
    /// Drop existing constraint
    Drop,
    /// Modify existing constraint (drop then add)
    Modify,
}

impl ConstraintGenerator {
    /// Create new constraint generator
    pub fn new(
        constraint_definition: ConstraintDefinition,
        operation: ConstraintOperation,
    ) -> Self {
        Self {
            constraint_definition,
            operation,
        }
    }

    /// Create constraint generator for adding constraint
    pub fn add_constraint(constraint_definition: ConstraintDefinition) -> Self {
        Self::new(constraint_definition, ConstraintOperation::Add)
    }

    /// Create constraint generator for dropping constraint
    pub fn drop_constraint(constraint_name: String, table_name: String) -> Self {
        let constraint_definition = ConstraintDefinition {
            name: constraint_name,
            table_name,
            constraint_type: ConstraintType::Check {
                expression: "true".to_string(),
            },
            columns: Vec::new(),
            options: Vec::new(),
        };
        Self::new(constraint_definition, ConstraintOperation::Drop)
    }

    /// Generate constraint type SQL
    fn generate_constraint_type_sql(&self) -> Result<String, GeneratorError> {
        match &self.constraint_definition.constraint_type {
            ConstraintType::PrimaryKey => {
                let columns = self.constraint_definition.columns.join(", ");
                Ok(format!("PRIMARY KEY ({})", columns))
            }
            ConstraintType::ForeignKey {
                referenced_table,
                referenced_columns,
                on_delete,
                on_update,
            } => {
                let columns = self.constraint_definition.columns.join(", ");
                let ref_columns = referenced_columns.join(", ");
                let mut sql = format!(
                    "FOREIGN KEY ({}) REFERENCES {} ({})",
                    columns, referenced_table, ref_columns
                );

                if let Some(action) = on_delete {
                    sql.push_str(&format!(
                        " ON DELETE {}",
                        self.format_foreign_key_action(action)
                    ));
                }

                if let Some(action) = on_update {
                    sql.push_str(&format!(
                        " ON UPDATE {}",
                        self.format_foreign_key_action(action)
                    ));
                }

                Ok(sql)
            }
            ConstraintType::Unique => {
                let columns = self.constraint_definition.columns.join(", ");
                Ok(format!("UNIQUE ({})", columns))
            }
            ConstraintType::Check { expression } => Ok(format!("CHECK ({})", expression)),
            ConstraintType::Exclusion { elements } => {
                let element_specs: Vec<String> = elements
                    .iter()
                    .map(|el| format!("{} WITH {}", el.expression, el.operator))
                    .collect();
                Ok(format!("EXCLUDE ({})", element_specs.join(", ")))
            }
            ConstraintType::NotNull { column } => {
                Ok(format!("ALTER COLUMN {} SET NOT NULL", column))
            }
        }
    }

    /// Format foreign key action
    fn format_foreign_key_action(&self, action: &ForeignKeyAction) -> String {
        match action {
            ForeignKeyAction::NoAction => "NO ACTION".to_string(),
            ForeignKeyAction::Restrict => "RESTRICT".to_string(),
            ForeignKeyAction::Cascade => "CASCADE".to_string(),
            ForeignKeyAction::SetNull => "SET NULL".to_string(),
            ForeignKeyAction::SetDefault => "SET DEFAULT".to_string(),
        }
    }

    /// Generate ADD CONSTRAINT SQL
    fn generate_add_constraint_sql(&self) -> Result<String, GeneratorError> {
        let constraint_sql = self.generate_constraint_type_sql()?;

        // Handle NOT NULL constraints differently
        if matches!(
            self.constraint_definition.constraint_type,
            ConstraintType::NotNull { .. }
        ) {
            return Ok(format!(
                "ALTER TABLE {} {};",
                self.constraint_definition.table_name, constraint_sql
            ));
        }

        let mut sql = format!(
            "ALTER TABLE {} ADD CONSTRAINT {} {}",
            self.constraint_definition.table_name, self.constraint_definition.name, constraint_sql
        );

        // Add options if any
        if !self.constraint_definition.options.is_empty() {
            sql.push(' ');
            sql.push_str(&self.constraint_definition.options.join(" "));
        }

        sql.push(';');
        Ok(sql)
    }

    /// Generate DROP CONSTRAINT SQL
    fn generate_drop_constraint_sql(&self) -> Result<String, GeneratorError> {
        Ok(format!(
            "ALTER TABLE {} DROP CONSTRAINT IF EXISTS {};",
            self.constraint_definition.table_name, self.constraint_definition.name
        ))
    }

    /// Generate MODIFY CONSTRAINT SQL (drop then add)
    fn generate_modify_constraint_sql(&self) -> Result<String, GeneratorError> {
        let drop_sql = self.generate_drop_constraint_sql()?;
        let add_sql = self.generate_add_constraint_sql()?;
        Ok(format!("{}\n{}", drop_sql, add_sql))
    }
}

impl MigrationGenerator for ConstraintGenerator {
    fn generate_sql(&self) -> Result<String, GeneratorError> {
        match &self.operation {
            ConstraintOperation::Add => self.generate_add_constraint_sql(),
            ConstraintOperation::Drop => self.generate_drop_constraint_sql(),
            ConstraintOperation::Modify => self.generate_modify_constraint_sql(),
        }
    }

    fn get_name(&self) -> String {
        match &self.operation {
            ConstraintOperation::Add => {
                format!("add_constraint_{}", self.constraint_definition.name)
            }
            ConstraintOperation::Drop => {
                format!("drop_constraint_{}", self.constraint_definition.name)
            }
            ConstraintOperation::Modify => {
                format!("modify_constraint_{}", self.constraint_definition.name)
            }
        }
    }

    fn get_description(&self) -> String {
        let constraint_type_name = match &self.constraint_definition.constraint_type {
            ConstraintType::PrimaryKey => "primary key",
            ConstraintType::ForeignKey { .. } => "foreign key",
            ConstraintType::Unique => "unique",
            ConstraintType::Check { .. } => "check",
            ConstraintType::Exclusion { .. } => "exclusion",
            ConstraintType::NotNull { .. } => "not null",
        };

        match &self.operation {
            ConstraintOperation::Add => format!(
                "Add {} constraint {} to {} table",
                constraint_type_name,
                self.constraint_definition.name,
                self.constraint_definition.table_name
            ),
            ConstraintOperation::Drop => format!(
                "Drop {} constraint {} from {} table",
                constraint_type_name,
                self.constraint_definition.name,
                self.constraint_definition.table_name
            ),
            ConstraintOperation::Modify => format!(
                "Modify {} constraint {} on {} table",
                constraint_type_name,
                self.constraint_definition.name,
                self.constraint_definition.table_name
            ),
        }
    }
}

impl ConstraintDefinition {
    /// Create new constraint definition
    pub fn new(name: String, table_name: String, constraint_type: ConstraintType) -> Self {
        Self {
            name,
            table_name,
            constraint_type,
            columns: Vec::new(),
            options: Vec::new(),
        }
    }

    /// Add column to constraint
    pub fn add_column(mut self, column: String) -> Self {
        self.columns.push(column);
        self
    }

    /// Add option to constraint
    pub fn with_option(mut self, option: String) -> Self {
        self.options.push(option);
        self
    }

    /// Create primary key constraint
    pub fn primary_key(name: String, table_name: String, columns: Vec<String>) -> Self {
        Self {
            name,
            table_name,
            constraint_type: ConstraintType::PrimaryKey,
            columns,
            options: Vec::new(),
        }
    }

    /// Create foreign key constraint
    pub fn foreign_key(
        name: String,
        table_name: String,
        columns: Vec<String>,
        referenced_table: String,
        referenced_columns: Vec<String>,
    ) -> Self {
        Self {
            name,
            table_name,
            constraint_type: ConstraintType::ForeignKey {
                referenced_table,
                referenced_columns,
                on_delete: None,
                on_update: None,
            },
            columns,
            options: Vec::new(),
        }
    }

    /// Create unique constraint
    pub fn unique(name: String, table_name: String, columns: Vec<String>) -> Self {
        Self {
            name,
            table_name,
            constraint_type: ConstraintType::Unique,
            columns,
            options: Vec::new(),
        }
    }

    /// Create check constraint
    pub fn check(name: String, table_name: String, expression: String) -> Self {
        Self {
            name,
            table_name,
            constraint_type: ConstraintType::Check { expression },
            columns: Vec::new(),
            options: Vec::new(),
        }
    }

    /// Create not null constraint
    pub fn not_null(name: String, table_name: String, column: String) -> Self {
        Self {
            name,
            table_name,
            constraint_type: ConstraintType::NotNull { column },
            columns: Vec::new(),
            options: Vec::new(),
        }
    }

    /// Set foreign key actions
    pub fn with_foreign_key_actions(
        mut self,
        on_delete: Option<ForeignKeyAction>,
        on_update: Option<ForeignKeyAction>,
    ) -> Self {
        if let ConstraintType::ForeignKey {
            on_delete: ref mut ref_on_delete,
            on_update: ref mut ref_on_update,
            ..
        } = self.constraint_type
        {
            *ref_on_delete = on_delete;
            *ref_on_update = on_update;
        }
        self
    }
}

impl ExclusionElement {
    /// Create new exclusion element
    pub fn new(expression: String, operator: String) -> Self {
        Self {
            expression,
            operator,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_primary_key_constraint() {
        let constraint_def = ConstraintDefinition::primary_key(
            "pk_users".to_string(),
            "users".to_string(),
            vec!["id".to_string()],
        );

        let generator = ConstraintGenerator::add_constraint(constraint_def);
        let sql = generator.generate_sql().unwrap();

        assert_eq!(
            sql,
            "ALTER TABLE users ADD CONSTRAINT pk_users PRIMARY KEY (id);"
        );
    }

    #[test]
    fn test_foreign_key_constraint() {
        let constraint_def = ConstraintDefinition::foreign_key(
            "fk_posts_user_id".to_string(),
            "posts".to_string(),
            vec!["user_id".to_string()],
            "users".to_string(),
            vec!["id".to_string()],
        )
        .with_foreign_key_actions(
            Some(ForeignKeyAction::Cascade),
            Some(ForeignKeyAction::Restrict),
        );

        let generator = ConstraintGenerator::add_constraint(constraint_def);
        let sql = generator.generate_sql().unwrap();

        assert_eq!(
            sql,
            "ALTER TABLE posts ADD CONSTRAINT fk_posts_user_id FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE ON UPDATE RESTRICT;"
        );
    }

    #[test]
    fn test_unique_constraint() {
        let constraint_def = ConstraintDefinition::unique(
            "uk_users_email".to_string(),
            "users".to_string(),
            vec!["email".to_string()],
        );

        let generator = ConstraintGenerator::add_constraint(constraint_def);
        let sql = generator.generate_sql().unwrap();

        assert_eq!(
            sql,
            "ALTER TABLE users ADD CONSTRAINT uk_users_email UNIQUE (email);"
        );
    }

    #[test]
    fn test_check_constraint() {
        let constraint_def = ConstraintDefinition::check(
            "chk_users_age".to_string(),
            "users".to_string(),
            "age >= 0 AND age <= 150".to_string(),
        );

        let generator = ConstraintGenerator::add_constraint(constraint_def);
        let sql = generator.generate_sql().unwrap();

        assert_eq!(
            sql,
            "ALTER TABLE users ADD CONSTRAINT chk_users_age CHECK (age >= 0 AND age <= 150);"
        );
    }

    #[test]
    fn test_not_null_constraint() {
        let constraint_def = ConstraintDefinition::not_null(
            "nn_users_email".to_string(),
            "users".to_string(),
            "email".to_string(),
        );

        let generator = ConstraintGenerator::add_constraint(constraint_def);
        let sql = generator.generate_sql().unwrap();

        assert_eq!(sql, "ALTER TABLE users ALTER COLUMN email SET NOT NULL;");
    }

    #[test]
    fn test_composite_unique_constraint() {
        let constraint_def = ConstraintDefinition::unique(
            "uk_users_first_last_name".to_string(),
            "users".to_string(),
            vec!["first_name".to_string(), "last_name".to_string()],
        );

        let generator = ConstraintGenerator::add_constraint(constraint_def);
        let sql = generator.generate_sql().unwrap();

        assert_eq!(
            sql,
            "ALTER TABLE users ADD CONSTRAINT uk_users_first_last_name UNIQUE (first_name, last_name);"
        );
    }

    #[test]
    fn test_constraint_with_options() {
        let constraint_def = ConstraintDefinition::unique(
            "uk_users_email".to_string(),
            "users".to_string(),
            vec!["email".to_string()],
        )
        .with_option("DEFERRABLE INITIALLY DEFERRED".to_string());

        let generator = ConstraintGenerator::add_constraint(constraint_def);
        let sql = generator.generate_sql().unwrap();

        assert_eq!(
            sql,
            "ALTER TABLE users ADD CONSTRAINT uk_users_email UNIQUE (email) DEFERRABLE INITIALLY DEFERRED;"
        );
    }

    #[test]
    fn test_drop_constraint() {
        let generator = ConstraintGenerator::drop_constraint(
            "fk_posts_user_id".to_string(),
            "posts".to_string(),
        );
        let sql = generator.generate_sql().unwrap();

        assert_eq!(
            sql,
            "ALTER TABLE posts DROP CONSTRAINT IF EXISTS fk_posts_user_id;"
        );
    }

    #[test]
    fn test_modify_constraint() {
        let constraint_def = ConstraintDefinition::check(
            "chk_users_age".to_string(),
            "users".to_string(),
            "age >= 18".to_string(),
        );

        let generator = ConstraintGenerator::new(constraint_def, ConstraintOperation::Modify);
        let sql = generator.generate_sql().unwrap();

        assert!(sql.contains("DROP CONSTRAINT IF EXISTS chk_users_age;"));
        assert!(sql.contains("ADD CONSTRAINT chk_users_age CHECK (age >= 18);"));
    }

    #[test]
    fn test_foreign_key_actions() {
        let generator = ConstraintGenerator::add_constraint(ConstraintDefinition::new(
            "test".to_string(),
            "test".to_string(),
            ConstraintType::ForeignKey {
                referenced_table: "ref_table".to_string(),
                referenced_columns: vec!["id".to_string()],
                on_delete: Some(ForeignKeyAction::SetNull),
                on_update: Some(ForeignKeyAction::NoAction),
            },
        ));

        assert_eq!(
            generator.format_foreign_key_action(&ForeignKeyAction::SetNull),
            "SET NULL"
        );
        assert_eq!(
            generator.format_foreign_key_action(&ForeignKeyAction::NoAction),
            "NO ACTION"
        );
        assert_eq!(
            generator.format_foreign_key_action(&ForeignKeyAction::Cascade),
            "CASCADE"
        );
    }

    #[test]
    fn test_generator_name_and_description() {
        let constraint_def = ConstraintDefinition::primary_key(
            "pk_users".to_string(),
            "users".to_string(),
            vec!["id".to_string()],
        );

        let generator = ConstraintGenerator::add_constraint(constraint_def);

        assert_eq!(generator.get_name(), "add_constraint_pk_users");
        assert_eq!(
            generator.get_description(),
            "Add primary key constraint pk_users to users table"
        );

        let drop_generator =
            ConstraintGenerator::drop_constraint("pk_users".to_string(), "users".to_string());
        assert_eq!(drop_generator.get_name(), "drop_constraint_pk_users");
        assert!(drop_generator
            .get_description()
            .contains("Drop check constraint pk_users from users table"));
    }

    #[test]
    fn test_exclusion_element() {
        let element = ExclusionElement::new(
            "daterange(start_date, end_date)".to_string(),
            "&&".to_string(),
        );
        assert_eq!(element.expression, "daterange(start_date, end_date)");
        assert_eq!(element.operator, "&&");
    }
}
