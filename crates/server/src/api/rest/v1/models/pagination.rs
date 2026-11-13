//! Pagination Models
//!
//! This module contains reusable pagination models for REST API responses.
//! Supports both offset-based and cursor-based pagination patterns.

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Paginated response wrapper
///
/// Generic wrapper for paginated API responses that includes the data
/// and pagination metadata.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PaginatedResponse<T> {
    /// The actual data items
    pub data: Vec<T>,

    /// Pagination metadata
    pub pagination: PaginationMeta,
}

/// Pagination metadata
///
/// Contains information about the current page, total items, and navigation.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PaginationMeta {
    /// Current page number (1-indexed)
    pub page: u32,

    /// Number of items per page
    pub limit: u32,

    /// Total number of items across all pages
    pub total: u64,

    /// Total number of pages
    pub total_pages: u32,

    /// Whether there is a next page
    pub has_next: bool,

    /// Whether there is a previous page
    pub has_prev: bool,

    /// Next page number (if available)
    pub next_page: Option<u32>,

    /// Previous page number (if available)
    pub prev_page: Option<u32>,
}

/// Cursor-based pagination response
///
/// Used for infinite scrolling and real-time data streams.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CursorPaginatedResponse<T> {
    /// The actual data items
    pub data: Vec<T>,

    /// Cursor pagination metadata
    pub pagination: CursorPaginationMeta,
}

/// Cursor pagination metadata
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CursorPaginationMeta {
    /// Cursor for the next page
    pub next_cursor: Option<String>,

    /// Cursor for the previous page
    pub prev_cursor: Option<String>,

    /// Whether there are more items after this page
    pub has_more: bool,

    /// Number of items in this response
    pub count: u32,
}

/// Pagination query parameters
///
/// Common query parameters for paginated endpoints.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PaginationQuery {
    /// Page number (1-indexed, default: 1)
    #[serde(default = "default_page")]
    pub page: u32,

    /// Items per page (default: 20, max: 100)
    #[serde(default = "default_limit")]
    pub limit: u32,
}

/// Cursor pagination query parameters
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CursorPaginationQuery {
    /// Cursor for the next page
    pub cursor: Option<String>,

    /// Number of items to fetch (default: 20, max: 100)
    #[serde(default = "default_limit")]
    pub limit: u32,
}

fn default_page() -> u32 {
    1
}

fn default_limit() -> u32 {
    20
}

impl<T> PaginatedResponse<T> {
    /// Create a new paginated response
    ///
    /// # Arguments
    ///
    /// * `data` - The items for this page
    /// * `page` - Current page number (1-indexed)
    /// * `limit` - Items per page
    /// * `total` - Total number of items across all pages
    #[must_use]
    pub fn new(data: Vec<T>, page: u32, limit: u32, total: u64) -> Self {
        let total_pages = if limit > 0 {
            ((total as f64) / f64::from(limit)).ceil() as u32
        } else {
            0
        };

        let has_next = page < total_pages;
        let has_prev = page > 1;

        let next_page = if has_next { Some(page + 1) } else { None };
        let prev_page = if has_prev { Some(page - 1) } else { None };

        Self {
            data,
            pagination: PaginationMeta {
                page,
                limit,
                total,
                total_pages,
                has_next,
                has_prev,
                next_page,
                prev_page,
            },
        }
    }
}

impl<T> CursorPaginatedResponse<T> {
    /// Create a new cursor-based paginated response
    ///
    /// # Arguments
    ///
    /// * `data` - The items for this page
    /// * `next_cursor` - Cursor for the next page
    /// * `prev_cursor` - Cursor for the previous page
    /// * `has_more` - Whether there are more items
    #[must_use]
    pub fn new(
        data: Vec<T>,
        next_cursor: Option<String>,
        prev_cursor: Option<String>,
        has_more: bool,
    ) -> Self {
        let count = data.len() as u32;

        Self {
            data,
            pagination: CursorPaginationMeta {
                next_cursor,
                prev_cursor,
                has_more,
                count,
            },
        }
    }
}

impl Default for PaginationQuery {
    fn default() -> Self {
        Self {
            page: default_page(),
            limit: default_limit(),
        }
    }
}

impl Default for CursorPaginationQuery {
    fn default() -> Self {
        Self {
            cursor: None,
            limit: default_limit(),
        }
    }
}

impl PaginationQuery {
    /// Validate and clamp pagination parameters
    ///
    /// Ensures page >= 1 and limit is between 1 and 100.
    pub fn validate(&mut self) {
        if self.page < 1 {
            self.page = 1;
        }
        self.limit = self.limit.clamp(1, 100);
    }

    /// Calculate the offset for database queries
    #[must_use]
    pub fn offset(&self) -> u64 {
        u64::from(self.page - 1) * u64::from(self.limit)
    }
}

impl CursorPaginationQuery {
    /// Validate and clamp pagination parameters
    ///
    /// Ensures limit is between 1 and 100.
    pub fn validate(&mut self) {
        self.limit = self.limit.clamp(1, 100);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_paginated_response_creation() {
        let data = vec![1, 2, 3, 4, 5];
        let response = PaginatedResponse::new(data.clone(), 1, 5, 20);

        assert_eq!(response.data, data);
        assert_eq!(response.pagination.page, 1);
        assert_eq!(response.pagination.limit, 5);
        assert_eq!(response.pagination.total, 20);
        assert_eq!(response.pagination.total_pages, 4);
        assert!(response.pagination.has_next);
        assert!(!response.pagination.has_prev);
    }

    #[test]
    fn test_paginated_response_last_page() {
        let data = vec![1, 2, 3];
        let response = PaginatedResponse::new(data, 4, 5, 20);

        assert_eq!(response.pagination.page, 4);
        assert!(!response.pagination.has_next);
        assert!(response.pagination.has_prev);
        assert_eq!(response.pagination.next_page, None);
        assert_eq!(response.pagination.prev_page, Some(3));
    }

    #[test]
    fn test_cursor_paginated_response() {
        let data = vec![1, 2, 3];
        let response = CursorPaginatedResponse::new(
            data,
            Some("next_cursor".to_string()),
            Some("prev_cursor".to_string()),
            true,
        );

        assert_eq!(response.pagination.count, 3);
        assert!(response.pagination.has_more);
        assert_eq!(
            response.pagination.next_cursor,
            Some("next_cursor".to_string())
        );
    }

    #[test]
    fn test_pagination_query_default() {
        let query = PaginationQuery::default();
        assert_eq!(query.page, 1);
        assert_eq!(query.limit, 20);
    }

    #[test]
    fn test_pagination_query_validate() {
        let mut query = PaginationQuery {
            page: 0,
            limit: 200,
        };
        query.validate();

        assert_eq!(query.page, 1);
        assert_eq!(query.limit, 100);
    }

    #[test]
    fn test_pagination_query_offset() {
        let query = PaginationQuery { page: 3, limit: 20 };
        assert_eq!(query.offset(), 40);

        let query = PaginationQuery { page: 1, limit: 20 };
        assert_eq!(query.offset(), 0);
    }

    #[test]
    fn test_cursor_pagination_query_validate() {
        let mut query = CursorPaginationQuery {
            cursor: None,
            limit: 200,
        };
        query.validate();

        assert_eq!(query.limit, 100);
    }

    #[test]
    fn test_total_pages_calculation() {
        // Exact division
        let response = PaginatedResponse::new(vec![1, 2, 3], 1, 10, 100);
        assert_eq!(response.pagination.total_pages, 10);

        // With remainder
        let response = PaginatedResponse::new(vec![1, 2, 3], 1, 10, 95);
        assert_eq!(response.pagination.total_pages, 10);

        // Less than one page
        let response = PaginatedResponse::new(vec![1, 2, 3], 1, 10, 5);
        assert_eq!(response.pagination.total_pages, 1);
    }
}
