//! Live query dispatch infrastructure for querying running workflows.
//!
//! This module provides the shared state between `WorkflowService` (which receives
//! `QueryWorkflow` requests from clients) and `ExecutionService` (which attaches queries
//! to poll responses and receives results from workers).

use dashmap::DashMap;
use orcher_proto::orcher::v1::{QueryRequest, QueryResult};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

/// A pending query waiting for a worker to answer it.
///
/// Created by `WorkflowService::query_workflow()`, attached to poll responses
/// by `ExecutionService::poll_workflow_execution()`, and resolved by
/// `ExecutionService::respond_query_task()` or via `CompleteWorkflowExecution.query_results`.
pub struct PendingQuery {
    /// The query request to send to the worker
    pub request: QueryRequest,

    /// The execution_id of the workflow this query targets
    pub execution_id: String,

    /// Notification handle — fired when the result is written
    pub notify: Arc<Notify>,

    /// The result, written by the worker response handler
    pub result: Arc<Mutex<Option<QueryResult>>>,
}

/// Shared registry of pending queries.
///
/// Keyed by `query_id`. Shared between `WorkflowService` and `ExecutionService`.
pub type PendingQueryRegistry = Arc<DashMap<String, PendingQuery>>;

/// Create a new empty pending query registry.
pub fn new_pending_query_registry() -> PendingQueryRegistry {
    Arc::new(DashMap::new())
}
