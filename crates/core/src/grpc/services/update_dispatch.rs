//! Live update dispatch infrastructure for synchronous workflow mutations.
//!
//! This module provides the shared state between `WorkflowService` (which receives
//! `UpdateWorkflow` requests from clients) and `ExecutionService` (which attaches updates
//! to poll responses, journals the results, and notifies the waiting client).

use dashmap::DashMap;
use orcher_proto::orcher::v1::{UpdateRequest, UpdateResult};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

/// A pending update waiting for a worker to process it.
///
/// Created by `WorkflowService::update_workflow()`, attached to poll responses
/// by `ExecutionService::poll_workflow_execution()`, and resolved by
/// `ExecutionService::respond_update_task()` or via `CompleteWorkflowExecution.update_results`.
pub struct PendingUpdate {
    /// The update request to send to the worker
    pub request: UpdateRequest,

    /// The execution_id of the workflow this update targets
    pub execution_id: String,

    /// The internal workflow_id (DB UUID) for journal writes
    pub workflow_id: String,

    /// Notification handle — fired when the result is written
    pub notify: Arc<Notify>,

    /// The result, written by the worker response handler
    pub result: Arc<Mutex<Option<UpdateResult>>>,
}

/// Shared registry of pending updates.
///
/// Keyed by `update_id`. Shared between `WorkflowService` and `ExecutionService`.
pub type PendingUpdateRegistry = Arc<DashMap<String, PendingUpdate>>;

/// Create a new empty pending update registry.
pub fn new_pending_update_registry() -> PendingUpdateRegistry {
    Arc::new(DashMap::new())
}
