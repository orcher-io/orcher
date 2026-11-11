//! # Query Builder and Filtering
//!
//! Query building utilities and filtering operations for repositories.

use serde::{Deserialize, Serialize};

/// Query filter for repository operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryFilter {
    /// Field to filter on
    pub field: String,
    /// Operator to use (eq, ne, gt, lt, like, in)
    pub operator: FilterOperator,
    /// Value to compare against
    pub value: serde_json::Value,
}

/// Filter operators for queries
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum FilterOperator {
    /// Equal to
    Eq,
    /// Not equal to
    Ne,
    /// Greater than
    Gt,
    /// Greater than or equal to
    Gte,
    /// Less than
    Lt,
    /// Less than or equal to
    Lte,
    /// SQL LIKE pattern matching
    Like,
    /// In a set of values
    In,
    /// Not in a set of values
    NotIn,
}

/// Query options for filtering, sorting, and pagination
#[derive(Debug, Clone, Default)]
pub struct QueryOptions {
    /// Filters to apply
    pub filters: Vec<QueryFilter>,
    /// Fields to sort by
    pub sort_by: Vec<SortField>,
    /// Maximum number of results
    pub limit: Option<u64>,
    /// Number of results to skip
    pub offset: Option<u64>,
}

/// Sort field configuration
#[derive(Debug, Clone)]
pub struct SortField {
    /// Field name to sort by
    pub field: String,
    /// Sort direction
    pub direction: SortDirection,
}

/// Sort direction
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    /// Ascending order
    Asc,
    /// Descending order
    Desc,
}

impl QueryOptions {
    /// Create a new empty query options
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a filter to the query
    pub fn with_filter(mut self, filter: QueryFilter) -> Self {
        self.filters.push(filter);
        self
    }

    /// Add a sort field to the query
    pub fn with_sort(mut self, field: impl Into<String>, direction: SortDirection) -> Self {
        self.sort_by.push(SortField {
            field: field.into(),
            direction,
        });
        self
    }

    /// Set the limit for the query
    pub fn with_limit(mut self, limit: u64) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Set the offset for the query
    pub fn with_offset(mut self, offset: u64) -> Self {
        self.offset = Some(offset);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_options_builder() {
        let options = QueryOptions::new().with_limit(10).with_offset(0);

        assert_eq!(options.limit, Some(10));
        assert_eq!(options.offset, Some(0));
    }

    #[test]
    fn test_filter_operator_equality() {
        assert_eq!(FilterOperator::Eq, FilterOperator::Eq);
        assert_ne!(FilterOperator::Eq, FilterOperator::Ne);
    }

    #[test]
    fn test_sort_direction() {
        let asc = SortDirection::Asc;
        let desc = SortDirection::Desc;
        assert_ne!(asc, desc);
    }
}
