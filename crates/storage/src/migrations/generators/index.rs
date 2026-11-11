//! # Index Generator
//!
//! This module provides index generation capabilities for database migrations.
//! It handles index creation, modification, and deletion with support for
//! various index types including B-tree, hash, GIN, GiST, and partial indexes.

use super::{GeneratorError, MigrationGenerator};

/// Index generator for creating index migration SQL
pub struct IndexGenerator {
    index_definition: IndexDefinition,
    operation: IndexOperation,
}

/// Index definition structure
#[derive(Debug, Clone)]
pub struct IndexDefinition {
    /// Index name
    pub name: String,
    /// Table name the index belongs to
    pub table_name: String,
    /// Columns included in the index
    pub columns: Vec<IndexColumn>,
    /// Index type (btree, hash, gin, gist, etc.)
    pub index_type: IndexType,
    /// Whether the index is unique
    pub unique: bool,
    /// Partial index condition (WHERE clause)
    pub condition: Option<String>,
    /// Additional index options
    pub options: Vec<String>,
}

/// Index column definition
#[derive(Debug, Clone)]
pub struct IndexColumn {
    /// Column name
    pub name: String,
    /// Sort order (ASC, DESC)
    pub order: Option<SortOrder>,
    /// Expression for functional indexes
    pub expression: Option<String>,
}

/// Sort order for index columns
#[derive(Debug, Clone)]
pub enum SortOrder {
    /// Ascending order
    Asc,
    /// Descending order
    Desc,
}

/// Index type enumeration
#[derive(Debug, Clone)]
pub enum IndexType {
    /// B-tree index (default)
    BTree,
    /// Hash index
    Hash,
    /// GIN (Generalized Inverted Index)
    Gin,
    /// GiST (Generalized Search Tree)
    Gist,
    /// BRIN (Block Range Index)
    Brin,
    /// SP-GiST (Space-Partitioned GiST)
    SpGist,
}

/// Index operation types
#[derive(Debug, Clone)]
pub enum IndexOperation {
    /// Create new index
    Create,
    /// Drop existing index
    Drop,
    /// Recreate index (drop then create)
    Recreate,
}

impl IndexGenerator {
    /// Create new index generator
    pub fn new(index_definition: IndexDefinition, operation: IndexOperation) -> Self {
        Self {
            index_definition,
            operation,
        }
    }

    /// Create index generator for new index
    pub fn create_index(index_definition: IndexDefinition) -> Self {
        Self::new(index_definition, IndexOperation::Create)
    }

    /// Create index generator for dropping index
    pub fn drop_index(index_name: String, table_name: String) -> Self {
        let index_definition = IndexDefinition {
            name: index_name,
            table_name,
            columns: Vec::new(),
            index_type: IndexType::BTree,
            unique: false,
            condition: None,
            options: Vec::new(),
        };
        Self::new(index_definition, IndexOperation::Drop)
    }

    /// Generate column specification SQL
    fn generate_column_spec(&self, column: &IndexColumn) -> String {
        let mut spec = if let Some(expression) = &column.expression {
            expression.clone()
        } else {
            column.name.clone()
        };

        if let Some(order) = &column.order {
            spec.push_str(match order {
                SortOrder::Asc => " ASC",
                SortOrder::Desc => " DESC",
            });
        }

        spec
    }

    /// Generate CREATE INDEX SQL
    fn generate_create_index_sql(&self) -> Result<String, GeneratorError> {
        let mut sql = String::new();

        // Start with CREATE
        if self.index_definition.unique {
            sql.push_str("CREATE UNIQUE INDEX ");
        } else {
            sql.push_str("CREATE INDEX ");
        }

        // Index name
        sql.push_str(&self.index_definition.name);

        // Table name
        sql.push_str(" ON ");
        sql.push_str(&self.index_definition.table_name);

        // Index type (if not default B-tree)
        if !matches!(self.index_definition.index_type, IndexType::BTree) {
            sql.push_str(" USING ");
            sql.push_str(&self.get_index_type_sql());
        }

        // Columns
        sql.push_str(" (");
        let column_specs: Vec<String> = self
            .index_definition
            .columns
            .iter()
            .map(|col| self.generate_column_spec(col))
            .collect();
        sql.push_str(&column_specs.join(", "));
        sql.push(')');

        // Partial index condition
        if let Some(condition) = &self.index_definition.condition {
            sql.push_str(" WHERE ");
            sql.push_str(condition);
        }

        // Additional options
        if !self.index_definition.options.is_empty() {
            sql.push(' ');
            sql.push_str(&self.index_definition.options.join(" "));
        }

        sql.push(';');
        Ok(sql)
    }

    /// Generate DROP INDEX SQL
    fn generate_drop_index_sql(&self) -> Result<String, GeneratorError> {
        Ok(format!(
            "DROP INDEX IF EXISTS {};",
            self.index_definition.name
        ))
    }

    /// Generate RECREATE INDEX SQL
    fn generate_recreate_index_sql(&self) -> Result<String, GeneratorError> {
        let drop_sql = self.generate_drop_index_sql()?;
        let create_sql = self.generate_create_index_sql()?;
        Ok(format!("{}\n{}", drop_sql, create_sql))
    }

    /// Get index type SQL string
    fn get_index_type_sql(&self) -> String {
        match self.index_definition.index_type {
            IndexType::BTree => "btree".to_string(),
            IndexType::Hash => "hash".to_string(),
            IndexType::Gin => "gin".to_string(),
            IndexType::Gist => "gist".to_string(),
            IndexType::Brin => "brin".to_string(),
            IndexType::SpGist => "spgist".to_string(),
        }
    }
}

impl MigrationGenerator for IndexGenerator {
    fn generate_sql(&self) -> Result<String, GeneratorError> {
        match &self.operation {
            IndexOperation::Create => self.generate_create_index_sql(),
            IndexOperation::Drop => self.generate_drop_index_sql(),
            IndexOperation::Recreate => self.generate_recreate_index_sql(),
        }
    }

    fn get_name(&self) -> String {
        match &self.operation {
            IndexOperation::Create => format!("create_index_{}", self.index_definition.name),
            IndexOperation::Drop => format!("drop_index_{}", self.index_definition.name),
            IndexOperation::Recreate => format!("recreate_index_{}", self.index_definition.name),
        }
    }

    fn get_description(&self) -> String {
        match &self.operation {
            IndexOperation::Create => format!(
                "Create {} index on {} table",
                self.index_definition.name, self.index_definition.table_name
            ),
            IndexOperation::Drop => format!(
                "Drop {} index from {} table",
                self.index_definition.name, self.index_definition.table_name
            ),
            IndexOperation::Recreate => format!(
                "Recreate {} index on {} table",
                self.index_definition.name, self.index_definition.table_name
            ),
        }
    }
}

impl IndexColumn {
    /// Create new index column
    pub fn new(name: String) -> Self {
        Self {
            name,
            order: None,
            expression: None,
        }
    }

    /// Create index column with sort order
    pub fn with_order(mut self, order: SortOrder) -> Self {
        self.order = Some(order);
        self
    }

    /// Create functional index column with expression
    pub fn with_expression(mut self, expression: String) -> Self {
        self.expression = Some(expression);
        self
    }
}

impl IndexDefinition {
    /// Create new index definition
    pub fn new(name: String, table_name: String) -> Self {
        Self {
            name,
            table_name,
            columns: Vec::new(),
            index_type: IndexType::BTree,
            unique: false,
            condition: None,
            options: Vec::new(),
        }
    }

    /// Add column to index
    pub fn add_column(mut self, column: IndexColumn) -> Self {
        self.columns.push(column);
        self
    }

    /// Set index type
    pub fn with_type(mut self, index_type: IndexType) -> Self {
        self.index_type = index_type;
        self
    }

    /// Make index unique
    pub fn unique(mut self) -> Self {
        self.unique = true;
        self
    }

    /// Add partial index condition
    pub fn with_condition(mut self, condition: String) -> Self {
        self.condition = Some(condition);
        self
    }

    /// Add index option
    pub fn with_option(mut self, option: String) -> Self {
        self.options.push(option);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_index_creation() {
        let index_def = IndexDefinition::new("idx_users_email".to_string(), "users".to_string())
            .add_column(IndexColumn::new("email".to_string()));

        let generator = IndexGenerator::create_index(index_def);
        let sql = generator.generate_sql().unwrap();

        assert_eq!(sql, "CREATE INDEX idx_users_email ON users (email);");
    }

    #[test]
    fn test_unique_index_creation() {
        let index_def = IndexDefinition::new("idx_users_username".to_string(), "users".to_string())
            .add_column(IndexColumn::new("username".to_string()))
            .unique();

        let generator = IndexGenerator::create_index(index_def);
        let sql = generator.generate_sql().unwrap();

        assert_eq!(
            sql,
            "CREATE UNIQUE INDEX idx_users_username ON users (username);"
        );
    }

    #[test]
    fn test_composite_index_creation() {
        let index_def =
            IndexDefinition::new("idx_users_name_email".to_string(), "users".to_string())
                .add_column(IndexColumn::new("last_name".to_string()).with_order(SortOrder::Asc))
                .add_column(IndexColumn::new("first_name".to_string()).with_order(SortOrder::Asc))
                .add_column(IndexColumn::new("email".to_string()).with_order(SortOrder::Desc));

        let generator = IndexGenerator::create_index(index_def);
        let sql = generator.generate_sql().unwrap();

        assert_eq!(sql, "CREATE INDEX idx_users_name_email ON users (last_name ASC, first_name ASC, email DESC);");
    }

    #[test]
    fn test_gin_index_creation() {
        let index_def = IndexDefinition::new("idx_users_tags".to_string(), "users".to_string())
            .add_column(IndexColumn::new("tags".to_string()))
            .with_type(IndexType::Gin);

        let generator = IndexGenerator::create_index(index_def);
        let sql = generator.generate_sql().unwrap();

        assert_eq!(
            sql,
            "CREATE INDEX idx_users_tags ON users USING gin (tags);"
        );
    }

    #[test]
    fn test_partial_index_creation() {
        let index_def =
            IndexDefinition::new("idx_users_active_email".to_string(), "users".to_string())
                .add_column(IndexColumn::new("email".to_string()))
                .with_condition("active = true".to_string());

        let generator = IndexGenerator::create_index(index_def);
        let sql = generator.generate_sql().unwrap();

        assert_eq!(
            sql,
            "CREATE INDEX idx_users_active_email ON users (email) WHERE active = true;"
        );
    }

    #[test]
    fn test_functional_index_creation() {
        let index_def =
            IndexDefinition::new("idx_users_lower_email".to_string(), "users".to_string())
                .add_column(
                    IndexColumn::new("email".to_string())
                        .with_expression("lower(email)".to_string()),
                );

        let generator = IndexGenerator::create_index(index_def);
        let sql = generator.generate_sql().unwrap();

        assert_eq!(
            sql,
            "CREATE INDEX idx_users_lower_email ON users (lower(email));"
        );
    }

    #[test]
    fn test_index_with_options() {
        let index_def = IndexDefinition::new("idx_users_email".to_string(), "users".to_string())
            .add_column(IndexColumn::new("email".to_string()))
            .with_option("WITH (fillfactor = 70)".to_string());

        let generator = IndexGenerator::create_index(index_def);
        let sql = generator.generate_sql().unwrap();

        assert_eq!(
            sql,
            "CREATE INDEX idx_users_email ON users (email) WITH (fillfactor = 70);"
        );
    }

    #[test]
    fn test_drop_index() {
        let generator =
            IndexGenerator::drop_index("idx_users_email".to_string(), "users".to_string());
        let sql = generator.generate_sql().unwrap();

        assert_eq!(sql, "DROP INDEX IF EXISTS idx_users_email;");
    }

    #[test]
    fn test_recreate_index() {
        let index_def = IndexDefinition::new("idx_users_email".to_string(), "users".to_string())
            .add_column(IndexColumn::new("email".to_string()));

        let generator = IndexGenerator::new(index_def, IndexOperation::Recreate);
        let sql = generator.generate_sql().unwrap();

        assert!(sql.contains("DROP INDEX IF EXISTS idx_users_email;"));
        assert!(sql.contains("CREATE INDEX idx_users_email ON users (email);"));
    }

    #[test]
    fn test_index_type_sql_generation() {
        let index_def = IndexDefinition::new("test".to_string(), "test".to_string());
        let generator = IndexGenerator::create_index(index_def);

        assert_eq!(generator.get_index_type_sql(), "btree");

        let gin_def =
            IndexDefinition::new("test".to_string(), "test".to_string()).with_type(IndexType::Gin);
        let gin_generator = IndexGenerator::create_index(gin_def);
        assert_eq!(gin_generator.get_index_type_sql(), "gin");

        let hash_def =
            IndexDefinition::new("test".to_string(), "test".to_string()).with_type(IndexType::Hash);
        let hash_generator = IndexGenerator::create_index(hash_def);
        assert_eq!(hash_generator.get_index_type_sql(), "hash");
    }

    #[test]
    fn test_generator_name_and_description() {
        let index_def = IndexDefinition::new("idx_test".to_string(), "test_table".to_string());
        let generator = IndexGenerator::create_index(index_def);

        assert_eq!(generator.get_name(), "create_index_idx_test");
        assert_eq!(
            generator.get_description(),
            "Create idx_test index on test_table table"
        );

        let drop_generator =
            IndexGenerator::drop_index("idx_test".to_string(), "test_table".to_string());
        assert_eq!(drop_generator.get_name(), "drop_index_idx_test");
        assert_eq!(
            drop_generator.get_description(),
            "Drop idx_test index from test_table table"
        );
    }
}
