//! Workflow chronicle — search and query layer for execution history.

pub mod query_builder;
pub mod service;
pub mod types;

// Re-export main types for convenience
pub use service::{ChronicleMetrics, ChronicleService};
pub use types::{
    CountWorkflowsRequest, CountWorkflowsResponse, GetWorkflowRequest, GetWorkflowStatsRequest,
    ListWorkflowsRequest, ListWorkflowsResponse, OrderBy, PageCursor, WorkflowInfo,
    WorkflowStatsResponse, WorkflowStatus,
};
