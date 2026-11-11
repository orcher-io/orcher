//! Child workflow lifecycle management.

use orcher_common::OrcherResult;
use orcher_storage::repositories::domains::child_workflow::ChildWorkflowRepository;
use orcher_storage::schema::types::child_workflow::{ChildWorkflowExecution, OrphanPolicy};
use serde_json::Value;
use std::sync::Arc;
use tracing::{debug, error, info, instrument, warn};

use crate::workflows::JournalService;
use crate::workflows::WorkflowExecutionService;

/// What a parent asked for its child's runs, beyond what to run.
///
/// A child used to be started with none of this: its retry policy was dropped,
/// so a child the parent asked to be retried failed for good on its first
/// failure.
#[derive(Debug, Clone, Default)]
pub struct ChildRunSettings {
    /// Retry cap, as a top-level start stores it (`None` = no retries).
    pub max_retries: Option<i32>,
    /// Retry policy JSON, as a top-level start stores it.
    pub retry_policy: Option<Value>,
    /// Labels for the child's runs.
    pub labels: Option<Value>,
    /// Annotations for the child's runs, as a top-level start stores them.
    pub annotations: Option<Value>,
    /// The time limits the child's start command asked for. A child used to
    /// be started with none: its own were dropped, and its parent's are not
    /// inherited.
    pub timeouts: crate::workflows::RunTimeouts,
}

/// Child workflow manager for orchestrating child workflow lifecycle
///
/// Manages the creation, state transitions, and lifecycle events of child
/// workflows, coordinating between the parent and child executions.
pub struct ChildWorkflowManager {
    /// Repository for child workflow execution records
    repository: Arc<ChildWorkflowRepository>,

    /// Journal service for appending parent workflow events
    journal_service: Arc<JournalService>,

    /// Workflow execution service for starting child workflow executions
    workflow_execution_service: Arc<WorkflowExecutionService>,
    // TODO: Add EventBus for publishing lifecycle events
    // event_bus: Arc<EventBus>,
}

impl ChildWorkflowManager {
    /// Create a new child workflow manager
    ///
    /// # Arguments
    ///
    /// * `repository` - Repository for child workflow records
    /// * `journal_service` - Service for appending journal entries
    /// * `workflow_execution_service` - Service for starting workflow executions
    pub fn new(
        repository: Arc<ChildWorkflowRepository>,
        journal_service: Arc<JournalService>,
        workflow_execution_service: Arc<WorkflowExecutionService>,
    ) -> Self {
        Self {
            repository,
            journal_service,
            workflow_execution_service,
        }
    }

    /// Handle StartChildWorkflow command
    ///
    /// Creates a child workflow execution record and appends the initiated event
    /// to the parent workflow's journal.
    ///
    /// # Arguments
    ///
    /// * `parent_workflow_id` - The parent workflow identifier
    /// * `parent_execution_id` - The parent execution identifier
    /// * `child_workflow_id` - The child workflow identifier (user-provided)
    /// * `child_workflow_type` - The child workflow type/name
    /// * `task_queue` - The task queue for child execution
    /// * `input` - The input data for child workflow
    /// * `orphan_policy` - Policy for when parent closes
    /// * `settings` - What the parent asked for the child's runs: retries, labels
    ///
    /// # Returns
    ///
    /// The created child workflow execution
    #[instrument(skip(self, input, settings), fields(
        parent_workflow_id = %parent_workflow_id,
        parent_execution_id = %parent_execution_id,
        child_workflow_id = %child_workflow_id,
        child_workflow_type = %child_workflow_type
    ))]
    #[allow(clippy::too_many_arguments)]
    pub async fn handle_start_child_workflow(
        &self,
        parent_workflow_id: &str,
        parent_execution_id: &str,
        child_workflow_id: &str,
        child_workflow_type: &str,
        namespace: &str,
        task_queue: &str,
        input: Value,
        orphan_policy: OrphanPolicy,
        settings: ChildRunSettings,
    ) -> OrcherResult<ChildWorkflowExecution> {
        info!(
            parent_workflow_id = %parent_workflow_id,
            parent_execution_id = %parent_execution_id,
            child_workflow_id = %child_workflow_id,
            child_workflow_type = %child_workflow_type,
            "Starting child workflow"
        );

        // Idempotency: a parent that started several children and then suspended
        // awaiting them replays from the top whenever ANY child completes, and
        // re-emits StartChildWorkflow for the children that are still pending.
        // The business child_workflow_id is parent-assigned and unique within the
        // parent, so if a child already exists under it, return that one rather
        // than minting a second execution (the mechanism behind the historical
        // duplicate-child bug). This makes start-then-await-all durable and
        // keeps single-child starts safe against retried command batches.
        if let Some(existing) = self
            .repository
            .get_by_parent_and_child_id(parent_execution_id, child_workflow_id)
            .await?
        {
            // The record is written before the execution is created, so a
            // failure between the two left a record with no child behind it.
            // Every replay then landed here, reported the child as started,
            // and the parent waited on a workflow that did not exist. Create
            // the execution if it is missing; the start converges instead of
            // depending on having succeeded all at once. It is created under
            // the id the record already holds, so the record names it.
            if self
                .workflow_execution_service
                .find_execution(&existing.child_execution_id)
                .await?
                .is_none()
            {
                match self
                    .workflow_execution_service
                    .find_child(parent_execution_id, child_workflow_id)
                    .await?
                {
                    // A record that never learned its run's id (written before
                    // records held the real one): point it there rather than
                    // start the child a second time.
                    Some(run) => {
                        warn!(
                            parent_execution_id = %parent_execution_id,
                            child_workflow_id = %child_workflow_id,
                            child_execution_id = %run.execution_id,
                            "Child record did not name its run; pointing it there"
                        );
                        self.repository
                            .repoint_child_execution(
                                &existing.child_execution_id,
                                &run.execution_id,
                            )
                            .await?;
                    }
                    None => {
                        warn!(
                            parent_execution_id = %parent_execution_id,
                            child_workflow_id = %child_workflow_id,
                            "Child was recorded but its execution was never created; creating it now"
                        );
                        self.workflow_execution_service
                            .start_child_workflow(
                                &existing.child_execution_id,
                                child_workflow_id,
                                child_workflow_type,
                                "1.0",
                                namespace,
                                task_queue,
                                input,
                                parent_execution_id,
                                settings,
                            )
                            .await?;
                    }
                }
            }
            info!(
                parent_workflow_id = %parent_workflow_id,
                parent_execution_id = %parent_execution_id,
                child_workflow_id = %child_workflow_id,
                child_execution_id = %existing.child_execution_id,
                "Child workflow already started for this parent; returning existing (idempotent start)"
            );
            return Ok(existing);
        }

        // The child's execution id, chosen here so the record and the
        // execution carry the same one. The record used to get an id of its
        // own that no execution ever had, so a finished run could not be
        // traced back to its record — or through it, to its parent.
        let child_execution_id = uuid::Uuid::new_v4().to_string();

        // Append ChildWorkflowExecutionInitiated to parent journal
        // This gets the initiated_event_id we need
        let initiated_event_id = self
            .journal_service
            .append_child_workflow_initiated(
                parent_workflow_id,
                parent_execution_id,
                child_workflow_id,
                &child_execution_id,
                child_workflow_type,
                task_queue,
                input.clone(),
            )
            .await?;

        debug!(
            parent_workflow_id = %parent_workflow_id,
            parent_execution_id = %parent_execution_id,
            child_workflow_id = %child_workflow_id,
            initiated_event_id = initiated_event_id,
            "Appended ChildWorkflowExecutionInitiated event"
        );

        // Create child workflow execution record
        let child = ChildWorkflowExecution::new(
            parent_workflow_id.to_string(),
            parent_execution_id.to_string(),
            initiated_event_id,
            child_workflow_id.to_string(),
            child_execution_id.clone(),
            child_workflow_type.to_string(),
            task_queue.to_string(),
            input.clone(),
            orphan_policy,
        );

        let created = self.repository.create(&child).await?;

        info!(
            parent_workflow_id = %parent_workflow_id,
            parent_execution_id = %parent_execution_id,
            child_workflow_id = %child_workflow_id,
            child_execution_id = %child_execution_id,
            initiated_event_id = initiated_event_id,
            "Child workflow initiated"
        );

        // Start the child workflow execution
        // This creates the workflow execution record and queues it for service pickup
        self.workflow_execution_service
            .start_child_workflow(
                &child_execution_id,
                child_workflow_id,
                child_workflow_type,
                "1.0",
                namespace,
                task_queue,
                input,
                parent_execution_id,
                settings,
            )
            .await
            .map_err(|e| {
                error!(
                    parent_workflow_id = %parent_workflow_id,
                    parent_execution_id = %parent_execution_id,
                    child_workflow_id = %child_workflow_id,
                    error = %e,
                    "Failed to start child workflow execution"
                );
                e
            })?;

        info!(
            parent_workflow_id = %parent_workflow_id,
            parent_execution_id = %parent_execution_id,
            child_workflow_id = %child_workflow_id,
            child_execution_id = %child_execution_id,
            "Child workflow execution started and queued for service pickup"
        );

        Ok(created)
    }

    /// Ask for the cancellation of the child `parent_execution_id` started as
    /// `child_workflow_id`.
    ///
    /// The child's run is cancelled exactly as an API cancellation would
    /// cancel it. The parent hears about it the way it hears about any other
    /// ending, once the reconciliation sweep sees the cancelled run. This used
    /// to append a cancellation to the parent's journal directly, looking the
    /// child up by a run id the SDKs do not send — so it found nothing, failed
    /// the command, and cancelled nothing.
    ///
    /// Returns whether a run was cancelled: `false` when there is no such
    /// child, or it had already ended.
    pub async fn request_cancel_child(
        &self,
        parent_execution_id: &str,
        child_workflow_id: &str,
    ) -> OrcherResult<bool> {
        let Some(child) = self
            .repository
            .get_by_parent_and_child_id(parent_execution_id, child_workflow_id)
            .await?
        else {
            warn!(
                parent_execution_id = %parent_execution_id,
                child_workflow_id = %child_workflow_id,
                "Cancellation requested for a child this parent never started"
            );
            return Ok(false);
        };
        self.workflow_execution_service
            .cancel_child_workflow(
                &child.child_execution_id,
                "Cancellation requested by the parent workflow",
            )
            .await
    }

    /// A child's run `old` has been replaced by `new` — retried, continued as
    /// new, or reset. Move its tracking row along, so the outcome the parent
    /// eventually receives is the successor's, not that of the run it replaced.
    ///
    /// A no-op for a run that is not a child. A failure is logged, not
    /// returned: the successor already exists, failing the caller would only
    /// make it try to create another, and the reconciliation sweep makes the
    /// same move when it next finds the row pointing at a replaced run.
    pub async fn successor_started(&self, old: &str, new: &str) {
        if let Err(e) = self.repository.repoint_child_execution(old, new).await {
            warn!(
                old_execution_id = %old,
                new_execution_id = %new,
                error = %e,
                "Could not move child tracking to the successor run; the sweep will"
            );
        }
    }

    /// Get a specific child workflow
    ///
    /// # Arguments
    ///
    /// * `child_workflow_id` - The child workflow identifier
    /// * `child_execution_id` - The child execution identifier
    pub async fn get_child(
        &self,
        child_workflow_id: &str,
        child_execution_id: &str,
    ) -> OrcherResult<Option<ChildWorkflowExecution>> {
        self.repository
            .get_by_child(child_workflow_id, child_execution_id)
            .await
    }
}

// ================================================================================================
// TESTS
// ================================================================================================

#[cfg(test)]
mod tests {

    #[test]
    fn test_child_workflow_manager_creation() {
        // Basic compilation test
        // Real tests require database connection
    }

    // Database-backed coverage lives in crates/core/tests/child_start.rs and
    // crates/core/tests/child_lifecycle.rs.
}
