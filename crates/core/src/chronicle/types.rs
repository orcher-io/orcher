//! Chronicle types and filters.

#![allow(missing_docs)]

use orcher_common::types::time::Timestamptz;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// Workflow status for filtering
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "workflow_status", rename_all = "lowercase")]
pub enum WorkflowStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Canceled,
    TimedOut,
}

/// Request to list workflows with filtering and pagination
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListWorkflowsRequest {
    /// Namespace for isolation (required)
    pub namespace: String,

    /// Filter by workflow status
    pub status: Option<WorkflowStatus>,

    /// Filter by workflow name (exact match)
    pub workflow_name: Option<String>,

    /// Filter by workflow name prefix
    pub workflow_name_prefix: Option<String>,

    /// Filter by who started the workflow
    pub started_by: Option<String>,

    /// Filter by parent workflow (child workflows only)
    pub parent_workflow_id: Option<Uuid>,

    /// Time range: workflows started after this time
    pub started_after: Option<Timestamptz>,

    /// Time range: workflows started before this time
    pub started_before: Option<Timestamptz>,

    /// Time range: workflows completed after this time
    pub completed_after: Option<Timestamptz>,

    /// Time range: workflows completed before this time
    pub completed_before: Option<Timestamptz>,

    /// Labels filter (JSONB containment query)
    /// Example: {"customer_id": "123", "order_type": "premium"}
    pub labels: Option<HashMap<String, serde_json::Value>>,

    /// Tag filters (must have ALL these tags)
    pub tags: Option<Vec<String>>,

    /// Pagination: page size (default 100, max 1000)
    pub page_size: Option<i32>,

    /// Pagination: cursor token from previous response
    pub page_token: Option<String>,

    /// Sort order
    pub order_by: Option<OrderBy>,

    /// Whether to include total count (can be expensive)
    pub include_count: bool,
}

impl ListWorkflowsRequest {
    /// Create a new request with just namespace
    pub fn new(namespace: String) -> Self {
        Self {
            namespace,
            status: None,
            workflow_name: None,
            workflow_name_prefix: None,
            started_by: None,
            parent_workflow_id: None,
            started_after: None,
            started_before: None,
            completed_after: None,
            completed_before: None,
            labels: None,
            tags: None,
            page_size: None,
            page_token: None,
            order_by: None,
            include_count: false,
        }
    }

    /// Get effective page size (default 100, max 1000)
    pub fn effective_page_size(&self) -> i32 {
        self.page_size.unwrap_or(100).clamp(1, 1000)
    }

    /// Get effective order by
    pub fn effective_order_by(&self) -> OrderBy {
        self.order_by.unwrap_or_default()
    }
}

/// Sort order for workflow listing
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderBy {
    /// Most recent first (default)
    StartTimeDesc,
    /// Oldest first
    StartTimeAsc,
    /// Recently completed first
    CompletedTimeDesc,
    /// Alphabetical by workflow name
    WorkflowNameAsc,
}

impl Default for OrderBy {
    fn default() -> Self {
        Self::StartTimeDesc
    }
}

impl OrderBy {
    /// Get SQL ORDER BY clause
    pub fn to_sql(&self) -> &'static str {
        match self {
            OrderBy::StartTimeDesc => "started_at DESC, workflow_id DESC",
            OrderBy::StartTimeAsc => "started_at ASC, workflow_id ASC",
            OrderBy::CompletedTimeDesc => "completed_at DESC NULLS LAST, workflow_id DESC",
            OrderBy::WorkflowNameAsc => "workflow_name ASC, workflow_id ASC",
        }
    }

    /// Get timestamp field for cursor pagination
    pub fn cursor_field(&self) -> &'static str {
        match self {
            OrderBy::StartTimeDesc | OrderBy::StartTimeAsc => "started_at",
            OrderBy::CompletedTimeDesc => "completed_at",
            OrderBy::WorkflowNameAsc => "started_at", // Fallback to started_at
        }
    }
}

/// Response from listing workflows
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListWorkflowsResponse {
    /// Workflows matching the filter
    pub workflows: Vec<WorkflowInfo>,

    /// Cursor for next page (opaque token)
    pub next_page_token: Option<String>,

    /// Total count (if requested, may be expensive)
    pub total_count: Option<i64>,
}

/// Workflow information (summary view)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowInfo {
    pub workflow_id: Uuid,
    pub workflow_name: String,
    pub namespace: String,
    pub status: WorkflowStatus,
    pub started_at: Timestamptz,
    pub completed_at: Option<Timestamptz>,
    pub started_by: Option<String>,
    pub parent_workflow_id: Option<Uuid>,
    pub labels: Option<serde_json::Value>,
    pub tags: Option<Vec<String>>,
    pub last_error: Option<String>,
}

/// Request to count workflows
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CountWorkflowsRequest {
    pub namespace: String,
    pub status: Option<WorkflowStatus>,
    pub workflow_name: Option<String>,
    pub started_after: Option<Timestamptz>,
    pub started_before: Option<Timestamptz>,
    pub labels: Option<HashMap<String, serde_json::Value>>,
    pub tags: Option<Vec<String>>,
}

/// Response with workflow count
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CountWorkflowsResponse {
    pub count: i64,
}

/// Request to get workflow details
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetWorkflowRequest {
    pub namespace: String,
    pub workflow_id: Uuid,
}

/// Request to get workflow statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetWorkflowStatsRequest {
    pub namespace: String,
    pub start_date: Option<Timestamptz>,
    pub end_date: Option<Timestamptz>,
}

/// Workflow statistics response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowStatsResponse {
    pub namespace: String,
    pub total_started: i64,
    pub total_completed: i64,
    pub total_failed: i64,
    pub total_canceled: i64,
    pub currently_running: i64,
    pub avg_duration_seconds: Option<f64>,
    pub p50_duration_seconds: Option<f64>,
    pub p95_duration_seconds: Option<f64>,
    pub p99_duration_seconds: Option<f64>,
}

/// Cursor for pagination (encodes position)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageCursor {
    /// Last seen timestamp (for ordering)
    pub last_timestamp: Timestamptz,

    /// Last seen workflow_id (for stable ordering)
    pub last_workflow_id: Uuid,

    /// Original order by (to validate consistency)
    pub order_by: OrderBy,
}

impl PageCursor {
    /// Create a new cursor
    pub fn new(last_timestamp: Timestamptz, last_workflow_id: Uuid, order_by: OrderBy) -> Self {
        Self {
            last_timestamp,
            last_workflow_id,
            order_by,
        }
    }

    /// Encode cursor as opaque base64 string
    pub fn encode(&self) -> Result<String, String> {
        use base64::Engine;
        let json = serde_json::to_string(self).map_err(|e| e.to_string())?;
        Ok(base64::engine::general_purpose::STANDARD.encode(json))
    }

    /// Decode cursor from base64 string
    pub fn decode(token: &str) -> Result<Self, String> {
        use base64::Engine;
        let json = base64::engine::general_purpose::STANDARD
            .decode(token)
            .map_err(|e| e.to_string())?;
        let cursor: Self = serde_json::from_slice(&json).map_err(|e| e.to_string())?;
        Ok(cursor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_list_request_defaults() {
        let req = ListWorkflowsRequest::new("test-ns".to_string());
        assert_eq!(req.namespace, "test-ns");
        assert_eq!(req.effective_page_size(), 100);
        assert_eq!(req.effective_order_by(), OrderBy::StartTimeDesc);
    }

    #[test]
    fn test_page_size_limits() {
        let mut req = ListWorkflowsRequest::new("test-ns".to_string());

        // Max limit
        req.page_size = Some(2000);
        assert_eq!(req.effective_page_size(), 1000);

        // Min limit
        req.page_size = Some(0);
        assert_eq!(req.effective_page_size(), 1);

        // Normal
        req.page_size = Some(50);
        assert_eq!(req.effective_page_size(), 50);
    }

    #[test]
    fn test_order_by_sql() {
        assert_eq!(
            OrderBy::StartTimeDesc.to_sql(),
            "started_at DESC, workflow_id DESC"
        );
        assert_eq!(
            OrderBy::StartTimeAsc.to_sql(),
            "started_at ASC, workflow_id ASC"
        );
        assert_eq!(
            OrderBy::CompletedTimeDesc.to_sql(),
            "completed_at DESC NULLS LAST, workflow_id DESC"
        );
    }

    #[test]
    fn test_cursor_encode_decode() {
        let now = Timestamptz::now();
        let id = Uuid::new_v4();
        let cursor = PageCursor::new(now, id, OrderBy::StartTimeDesc);

        let encoded = cursor.encode().unwrap();
        assert!(!encoded.is_empty());

        let decoded = PageCursor::decode(&encoded).unwrap();
        assert_eq!(decoded.last_workflow_id, id);
        assert_eq!(decoded.order_by, OrderBy::StartTimeDesc);
    }

    #[test]
    fn test_cursor_decode_invalid() {
        assert!(PageCursor::decode("invalid-base64!!!").is_err());
        assert!(PageCursor::decode("YWJjZGVm").is_err()); // Valid base64 but invalid JSON
    }
}
