//! Request Models for REST API v1
//!
//! This module contains common request models used across API endpoints.

use serde::{Deserialize, Serialize};

/// Generic request wrapper for paginated queries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaginatedRequest {
    /// Page number (starting from 1)
    pub page: Option<u32>,
    /// Number of items per page
    pub limit: Option<u32>,
    /// Offset for cursor-based pagination
    pub offset: Option<u32>,
}

impl Default for PaginatedRequest {
    fn default() -> Self {
        Self {
            page: Some(1),
            limit: Some(20),
            offset: None,
        }
    }
}

/// Generic filter request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterRequest {
    /// Filter field name
    pub field: String,
    /// Filter operator (eq, ne, gt, lt, gte, lte, contains, in)
    pub operator: String,
    /// Filter value
    pub value: serde_json::Value,
}

/// Sort order for queries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SortOrder {
    /// Ascending order
    Asc,
    /// Descending order
    Desc,
}

/// Sort request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortRequest {
    /// Field to sort by
    pub field: String,
    /// Sort order
    pub order: SortOrder,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_paginated_request_default() {
        let request = PaginatedRequest::default();
        assert_eq!(request.page, Some(1));
        assert_eq!(request.limit, Some(20));
    }

    #[test]
    fn test_filter_request_serialization() {
        let filter = FilterRequest {
            field: "status".to_string(),
            operator: "eq".to_string(),
            value: serde_json::json!("running"),
        };
        let serialized = serde_json::to_string(&filter).unwrap();
        assert!(serialized.contains("status"));
    }
}
