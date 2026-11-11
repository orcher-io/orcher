//! Response Models for REST API v1
//!
//! This module contains common response models used across API endpoints.

use serde::{Deserialize, Serialize};

/// Generic API response wrapper
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiResponse<T> {
    /// Response data
    pub data: T,
    /// Response status message
    pub message: Option<String>,
    /// Response timestamp
    pub timestamp: String,
}

/// Paginated response wrapper
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaginatedResponse<T> {
    /// Response data items
    pub items: Vec<T>,
    /// Total number of items
    pub total: u64,
    /// Current page
    pub page: u32,
    /// Items per page
    pub limit: u32,
    /// Total number of pages
    pub total_pages: u32,
}
