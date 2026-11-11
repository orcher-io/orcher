//! # Queryable Trait
//!
//! Core trait for advanced querying capabilities using orcher-common types
//! for maximum compatibility and type safety across the ORCHER ecosystem.

use async_trait::async_trait;
use orcher_common::OrcherResult;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;

use super::persistable::Persistable;

/// Query filter operations
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FilterOperation {
    /// Equality check
    Equals(String),
    /// Not equals check
    NotEquals(String),
    /// Greater than
    GreaterThan(String),
    /// Greater than or equal
    GreaterThanOrEqual(String),
    /// Less than
    LessThan(String),
    /// Less than or equal
    LessThanOrEqual(String),
    /// Value in list
    In(Vec<String>),
    /// Value not in list
    NotIn(Vec<String>),
    /// Pattern matching (SQL LIKE)
    Like(String),
    /// Pattern not matching
    NotLike(String),
    /// Regular expression match
    Regex(String),
    /// Null check
    IsNull,
    /// Not null check
    IsNotNull,
    /// Between two values
    Between(String, String),
    /// Full-text search
    FullText(String),
}

/// Query filter condition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryFilter {
    /// Field name to filter on
    pub field: String,
    /// Filter operation
    pub operation: FilterOperation,
    /// Logical operator for combining with next filter
    pub logical_op: Option<LogicalOperator>,
}

/// Logical operators for combining filters
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum LogicalOperator {
    /// AND operation
    And,
    /// OR operation
    Or,
    /// NOT operation
    Not,
}

/// Sort direction for ordering results
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SortDirection {
    /// Ascending order
    Ascending,
    /// Descending order
    Descending,
}

/// Sort specification for query results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuerySort {
    /// Field name to sort by
    pub field: String,
    /// Sort direction
    pub direction: SortDirection,
}

/// Pagination parameters for query results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryPagination {
    /// Number of records to skip
    pub offset: u64,
    /// Maximum number of records to return
    pub limit: u64,
}

impl QueryPagination {
    /// Create new pagination parameters
    pub fn new(offset: u64, limit: u64) -> Self {
        Self { offset, limit }
    }

    /// Create pagination for a specific page
    pub fn page(page: u64, page_size: u64) -> Self {
        Self {
            offset: page * page_size,
            limit: page_size,
        }
    }

    /// Get the page number (0-based)
    pub fn page_number(&self) -> u64 {
        if self.limit == 0 {
            0
        } else {
            self.offset / self.limit
        }
    }
}

/// Query aggregation functions
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AggregateFunction {
    /// Count records
    Count,
    /// Sum numeric values
    Sum(String),
    /// Average numeric values
    Average(String),
    /// Minimum value
    Min(String),
    /// Maximum value
    Max(String),
    /// Group by field
    GroupBy(String),
}

/// Complete query specification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Query {
    /// Filter conditions
    pub filters: Vec<QueryFilter>,
    /// Sort specifications
    pub sorts: Vec<QuerySort>,
    /// Pagination parameters
    pub pagination: Option<QueryPagination>,
    /// Fields to include in results (None = all fields)
    pub fields: Option<Vec<String>>,
    /// Aggregation functions
    pub aggregates: Vec<AggregateFunction>,
    /// Additional query options
    pub options: HashMap<String, String>,
}

impl Query {
    /// Create a new empty query
    pub fn new() -> Self {
        Self {
            filters: Vec::new(),
            sorts: Vec::new(),
            pagination: None,
            fields: None,
            aggregates: Vec::new(),
            options: HashMap::new(),
        }
    }

    /// Add a filter condition
    pub fn filter(mut self, field: String, operation: FilterOperation) -> Self {
        self.filters.push(QueryFilter {
            field,
            operation,
            logical_op: None,
        });
        self
    }

    /// Add a filter with logical operator
    pub fn filter_with_op(
        mut self,
        field: String,
        operation: FilterOperation,
        logical_op: LogicalOperator,
    ) -> Self {
        self.filters.push(QueryFilter {
            field,
            operation,
            logical_op: Some(logical_op),
        });
        self
    }

    /// Add sorting
    pub fn sort(mut self, field: String, direction: SortDirection) -> Self {
        self.sorts.push(QuerySort { field, direction });
        self
    }

    /// Add ascending sort
    pub fn sort_asc(self, field: String) -> Self {
        self.sort(field, SortDirection::Ascending)
    }

    /// Add descending sort
    pub fn sort_desc(self, field: String) -> Self {
        self.sort(field, SortDirection::Descending)
    }

    /// Set pagination
    pub fn paginate(mut self, offset: u64, limit: u64) -> Self {
        self.pagination = Some(QueryPagination::new(offset, limit));
        self
    }

    /// Set page-based pagination
    pub fn page(mut self, page: u64, page_size: u64) -> Self {
        self.pagination = Some(QueryPagination::page(page, page_size));
        self
    }

    /// Limit results
    pub fn limit(mut self, limit: u64) -> Self {
        if let Some(ref mut pagination) = self.pagination {
            pagination.limit = limit;
        } else {
            self.pagination = Some(QueryPagination::new(0, limit));
        }
        self
    }

    /// Skip records
    pub fn offset(mut self, offset: u64) -> Self {
        if let Some(ref mut pagination) = self.pagination {
            pagination.offset = offset;
        } else {
            self.pagination = Some(QueryPagination::new(offset, 50)); // Default limit
        }
        self
    }

    /// Select specific fields
    pub fn select(mut self, fields: Vec<String>) -> Self {
        self.fields = Some(fields);
        self
    }

    /// Add aggregation function
    pub fn aggregate(mut self, function: AggregateFunction) -> Self {
        self.aggregates.push(function);
        self
    }

    /// Add query option
    pub fn option(mut self, key: String, value: String) -> Self {
        self.options.insert(key, value);
        self
    }
}

impl Default for Query {
    fn default() -> Self {
        Self::new()
    }
}

/// Query results with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult<T> {
    /// Result data
    pub data: Vec<T>,
    /// Total count (before pagination)
    pub total_count: Option<u64>,
    /// Current page information
    pub pagination: Option<QueryPagination>,
    /// Query execution metadata
    pub metadata: QueryMetadata,
}

/// Query execution metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryMetadata {
    /// Query execution time in milliseconds
    pub execution_time_ms: u64,
    /// Whether result was served from cache
    pub from_cache: bool,
    /// Database/storage engine used
    pub engine: Option<String>,
    /// Query complexity score
    pub complexity_score: Option<u32>,
}

impl<T> QueryResult<T> {
    /// Create new query result
    pub fn new(data: Vec<T>, total_count: Option<u64>) -> Self {
        Self {
            data,
            total_count,
            pagination: None,
            metadata: QueryMetadata {
                execution_time_ms: 0,
                from_cache: false,
                engine: None,
                complexity_score: None,
            },
        }
    }

    /// Check if there are more pages available
    pub fn has_next_page(&self) -> bool {
        if let (Some(pagination), Some(total)) = (&self.pagination, self.total_count) {
            pagination.offset + pagination.limit < total
        } else {
            false
        }
    }

    /// Check if there is a previous page
    pub fn has_prev_page(&self) -> bool {
        if let Some(pagination) = &self.pagination {
            pagination.offset > 0
        } else {
            false
        }
    }

    /// Get next page pagination parameters
    pub fn next_page(&self) -> Option<QueryPagination> {
        if self.has_next_page() {
            self.pagination.as_ref().map(|pagination| {
                QueryPagination::new(pagination.offset + pagination.limit, pagination.limit)
            })
        } else {
            None
        }
    }

    /// Get previous page pagination parameters
    pub fn prev_page(&self) -> Option<QueryPagination> {
        if self.has_prev_page() {
            if let Some(pagination) = &self.pagination {
                let new_offset = pagination.offset.saturating_sub(pagination.limit);
                Some(QueryPagination::new(new_offset, pagination.limit))
            } else {
                None
            }
        } else {
            None
        }
    }
}

/// Core trait for entities that support advanced querying
#[async_trait]
pub trait Queryable: Persistable {
    /// Custom filter type for domain-specific filtering
    type Filter: Send + Sync + Debug;

    /// Custom sort type for domain-specific sorting
    type Sort: Send + Sync + Debug;

    /// Apply filters to query
    fn apply_filters(query: Query, filters: &Self::Filter) -> Query;

    /// Apply sorting to query
    fn apply_sort(query: Query, sort: &Self::Sort) -> Query;

    /// Get searchable fields for this entity
    fn searchable_fields() -> Vec<&'static str> {
        vec![] // Default: no searchable fields
    }

    /// Get filterable fields for this entity
    fn filterable_fields() -> Vec<&'static str> {
        vec![] // Default: no filterable fields
    }

    /// Get sortable fields for this entity
    fn sortable_fields() -> Vec<&'static str> {
        vec![] // Default: no sortable fields
    }

    /// Build query for finding entities by text search
    fn search_query(search_term: &str) -> Query {
        let mut query = Query::new();
        for field in Self::searchable_fields() {
            query = query.filter_with_op(
                field.to_string(),
                FilterOperation::Like(format!("%{}%", search_term)),
                LogicalOperator::Or,
            );
        }
        query
    }

    /// Validate query before execution
    fn validate_query(query: &Query) -> OrcherResult<()> {
        let filterable = Self::filterable_fields();
        let sortable = Self::sortable_fields();

        // Validate filter fields
        for filter in &query.filters {
            if !filterable.contains(&filter.field.as_str()) {
                return Err(crate::error::StorageError::ValidationFailed {
                    field: "filter".to_string(),
                    message: format!("Field '{}' is not filterable", filter.field),
                }
                .into());
            }
        }

        // Validate sort fields
        for sort in &query.sorts {
            if !sortable.contains(&sort.field.as_str()) {
                return Err(crate::error::StorageError::ValidationFailed {
                    field: "sort".to_string(),
                    message: format!("Field '{}' is not sortable", sort.field),
                }
                .into());
            }
        }

        Ok(())
    }

    /// Get query complexity score (for optimization and caching decisions)
    fn query_complexity(query: &Query) -> u32 {
        let mut score = 0;

        // Base complexity
        score += query.filters.len() as u32 * 10;
        score += query.sorts.len() as u32 * 5;
        score += query.aggregates.len() as u32 * 20;

        // Complexity modifiers
        for filter in &query.filters {
            score += match &filter.operation {
                FilterOperation::Like(_) | FilterOperation::Regex(_) => 30,
                FilterOperation::FullText(_) => 50,
                FilterOperation::In(values) | FilterOperation::NotIn(values) => {
                    values.len() as u32 * 2
                }
                _ => 5,
            };
        }

        score
    }
}

/// Advanced queryable trait with additional capabilities
#[async_trait]
pub trait AdvancedQueryable: Queryable {
    /// Execute raw query string
    async fn execute_raw_query(query: &str) -> OrcherResult<QueryResult<Self>>;

    /// Get query execution plan
    async fn explain_query(query: &Query) -> OrcherResult<String>;

    /// Create database index for query optimization
    async fn create_index(fields: Vec<String>, unique: bool) -> OrcherResult<String>;

    /// Drop database index
    async fn drop_index(index_name: &str) -> OrcherResult<()>;

    /// Analyze query performance
    async fn analyze_query_performance(query: &Query) -> OrcherResult<QueryMetadata>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use orcher_common::{
        types::identifiers::TaskId,
        validation::{Validatable, ValidationResult},
    };
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TestQueryableEntity {
        id: TaskId,
        name: String,
        status: String,
        priority: i32,
        created_at: String,
    }

    impl TestQueryableEntity {
        #[allow(dead_code)]
        fn new(name: String, status: String, priority: i32) -> Self {
            Self {
                id: TaskId::new_v4(),
                name,
                status,
                priority,
                created_at: "2023-01-01T00:00:00Z".to_string(),
            }
        }
    }

    impl Validatable for TestQueryableEntity {
        fn validate(&self) -> ValidationResult {
            ValidationResult::new()
        }
    }

    impl Persistable for TestQueryableEntity {
        type Id = TaskId;

        fn id(&self) -> &Self::Id {
            &self.id
        }

        fn object_type() -> &'static str {
            "test_queryable"
        }
    }

    #[derive(Debug)]
    struct TestFilter {
        status: Option<String>,
        min_priority: Option<i32>,
    }

    #[derive(Debug)]
    struct TestSort {
        by_priority: bool,
        by_name: bool,
    }

    impl Queryable for TestQueryableEntity {
        type Filter = TestFilter;
        type Sort = TestSort;

        fn apply_filters(mut query: Query, filters: &Self::Filter) -> Query {
            if let Some(ref status) = filters.status {
                query = query.filter(
                    "status".to_string(),
                    FilterOperation::Equals(status.clone()),
                );
            }
            if let Some(min_priority) = filters.min_priority {
                query = query.filter(
                    "priority".to_string(),
                    FilterOperation::GreaterThanOrEqual(min_priority.to_string()),
                );
            }
            query
        }

        fn apply_sort(mut query: Query, sort: &Self::Sort) -> Query {
            if sort.by_priority {
                query = query.sort_desc("priority".to_string());
            }
            if sort.by_name {
                query = query.sort_asc("name".to_string());
            }
            query
        }

        fn searchable_fields() -> Vec<&'static str> {
            vec!["name", "status"]
        }

        fn filterable_fields() -> Vec<&'static str> {
            vec!["name", "status", "priority", "created_at"]
        }

        fn sortable_fields() -> Vec<&'static str> {
            vec!["name", "priority", "created_at"]
        }
    }

    #[test]
    fn test_query_builder() {
        let query = Query::new()
            .filter(
                "name".to_string(),
                FilterOperation::Like("test%".to_string()),
            )
            .filter_with_op(
                "status".to_string(),
                FilterOperation::Equals("active".to_string()),
                LogicalOperator::And,
            )
            .sort_desc("priority".to_string())
            .sort_asc("name".to_string())
            .paginate(0, 50)
            .select(vec!["id".to_string(), "name".to_string()])
            .option("include_deleted".to_string(), "false".to_string());

        assert_eq!(query.filters.len(), 2);
        assert_eq!(query.sorts.len(), 2);
        assert!(query.pagination.is_some());
        assert!(query.fields.is_some());
        assert_eq!(query.options.len(), 1);
    }

    #[test]
    fn test_pagination() {
        let page_pagination = QueryPagination::page(2, 25);
        assert_eq!(page_pagination.offset, 50);
        assert_eq!(page_pagination.limit, 25);
        assert_eq!(page_pagination.page_number(), 2);

        let offset_pagination = QueryPagination::new(100, 20);
        assert_eq!(offset_pagination.page_number(), 5);
    }

    #[test]
    fn test_query_result_pagination() {
        let data: Vec<TestQueryableEntity> = vec![];
        let mut result = QueryResult::new(data, Some(100));
        result.pagination = Some(QueryPagination::new(0, 25));

        assert!(result.has_next_page());
        assert!(!result.has_prev_page());

        let next_page = result.next_page().unwrap();
        assert_eq!(next_page.offset, 25);
        assert_eq!(next_page.limit, 25);

        // Test middle page
        result.pagination = Some(QueryPagination::new(50, 25));
        assert!(result.has_next_page());
        assert!(result.has_prev_page());

        let prev_page = result.prev_page().unwrap();
        assert_eq!(prev_page.offset, 25);
        assert_eq!(prev_page.limit, 25);

        // Test last page
        result.pagination = Some(QueryPagination::new(75, 25));
        assert!(!result.has_next_page());
        assert!(result.has_prev_page());
    }

    #[test]
    fn test_filter_operations_serialization() {
        let operations = vec![
            FilterOperation::Equals("test".to_string()),
            FilterOperation::In(vec!["a".to_string(), "b".to_string()]),
            FilterOperation::Like("%test%".to_string()),
            FilterOperation::Between("1".to_string(), "10".to_string()),
            FilterOperation::IsNull,
        ];

        for op in operations {
            let serialized = serde_json::to_string(&op).unwrap();
            let deserialized: FilterOperation = serde_json::from_str(&serialized).unwrap();
            assert_eq!(op, deserialized);
        }
    }

    #[test]
    fn test_queryable_implementation() {
        let filters = TestFilter {
            status: Some("active".to_string()),
            min_priority: Some(10),
        };

        let sort = TestSort {
            by_priority: true,
            by_name: false,
        };

        let query = Query::new();
        let filtered_query = TestQueryableEntity::apply_filters(query, &filters);
        let sorted_query = TestQueryableEntity::apply_sort(filtered_query, &sort);

        assert_eq!(sorted_query.filters.len(), 2);
        assert_eq!(sorted_query.sorts.len(), 1);
        assert_eq!(sorted_query.sorts[0].direction, SortDirection::Descending);
    }

    #[test]
    fn test_search_query() {
        let search_query = TestQueryableEntity::search_query("test");
        assert_eq!(search_query.filters.len(), 2); // name and status fields

        for filter in &search_query.filters {
            if let FilterOperation::Like(pattern) = &filter.operation {
                assert_eq!(pattern, "%test%");
            }
            if let Some(LogicalOperator::Or) = filter.logical_op {
                // Expected for combining search terms
            }
        }
    }

    #[test]
    fn test_query_validation() {
        // Valid query
        let valid_query = Query::new()
            .filter(
                "name".to_string(),
                FilterOperation::Equals("test".to_string()),
            )
            .sort_asc("priority".to_string());

        assert!(TestQueryableEntity::validate_query(&valid_query).is_ok());

        // Invalid filter field
        let invalid_filter_query = Query::new().filter(
            "invalid_field".to_string(),
            FilterOperation::Equals("test".to_string()),
        );

        assert!(TestQueryableEntity::validate_query(&invalid_filter_query).is_err());

        // Invalid sort field
        let invalid_sort_query = Query::new().sort_asc("invalid_field".to_string());

        assert!(TestQueryableEntity::validate_query(&invalid_sort_query).is_err());
    }

    #[test]
    fn test_query_complexity() {
        let simple_query = Query::new().filter(
            "name".to_string(),
            FilterOperation::Equals("test".to_string()),
        );
        let simple_complexity = TestQueryableEntity::query_complexity(&simple_query);

        let complex_query = Query::new()
            .filter(
                "name".to_string(),
                FilterOperation::Like("%test%".to_string()),
            )
            .filter(
                "description".to_string(),
                FilterOperation::FullText("search term".to_string()),
            )
            .sort_desc("priority".to_string())
            .aggregate(AggregateFunction::Count);

        let complex_complexity = TestQueryableEntity::query_complexity(&complex_query);

        assert!(complex_complexity > simple_complexity);
    }

    #[test]
    fn test_aggregate_functions_serialization() {
        let functions = vec![
            AggregateFunction::Count,
            AggregateFunction::Sum("amount".to_string()),
            AggregateFunction::Average("score".to_string()),
            AggregateFunction::Min("date".to_string()),
            AggregateFunction::Max("date".to_string()),
            AggregateFunction::GroupBy("category".to_string()),
        ];

        for func in functions {
            let serialized = serde_json::to_string(&func).unwrap();
            let deserialized: AggregateFunction = serde_json::from_str(&serialized).unwrap();
            assert_eq!(func, deserialized);
        }
    }
}
