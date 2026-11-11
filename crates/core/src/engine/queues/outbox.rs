//! Outbox queue for atomic task dispatch.

use crate::engine::queues::types::{OutboxTask, QueueMetrics};
use jiff::SignedDuration;
use orcher_common::error::{OrcherError, OrcherResult};
use orcher_storage::repositories::domains::coordination::OutboxRepository;
use std::sync::Arc;
use tokio::sync::Notify;
use tracing::{debug, warn};
use uuid::Uuid;

/// Outbox queue for atomic task dispatch
///
/// This is a thin wrapper around OutboxRepository that maintains the existing
/// public API while delegating to the repository layer for all database operations.
#[derive(Clone)]
pub struct OutboxQueue {
    repository: Arc<OutboxRepository>,
    /// Shared wake signal — triggers the OutboxProcessor to process tasks immediately
    /// instead of waiting for the next poll interval. Set via `with_wake_signal`.
    wake: Arc<Notify>,
}

impl OutboxQueue {
    /// Create a new outbox queue
    ///
    /// # Arguments
    ///
    /// * `repository` - The outbox repository for database operations
    pub fn new(repository: Arc<OutboxRepository>) -> Self {
        Self {
            repository,
            wake: Arc::new(Notify::new()),
        }
    }

    /// Get the wake signal for sharing with the OutboxProcessor.
    pub fn wake_signal(&self) -> Arc<Notify> {
        self.wake.clone()
    }

    /// Notify the processor that new tasks are available.
    /// Call this after inserting tasks into the outbox to eliminate poll-interval latency.
    /// Uses notify_waiters() so multiple rapid inserts coalesce into a single wake-up.
    pub fn notify_new_tasks(&self) {
        self.wake.notify_waiters();
    }

    /// Create a new task in the outbox (atomic with transaction)
    ///
    /// This should be called within the same transaction as the workflow state change
    /// to ensure atomic dispatch.
    ///
    /// # Arguments
    ///
    /// * `tx` - The active transaction
    /// * `task` - The task to create
    ///
    /// # Returns
    ///
    /// The task ID of the created (or existing if duplicate) task
    pub async fn create_task(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        task: OutboxTask,
    ) -> OrcherResult<Uuid> {
        self.repository.create_in_tx(tx, task).await
    }

    /// Create a task in the outbox within `tx`, or nothing when one with the
    /// same idempotency key is already there — `None` in that case.
    pub async fn create_task_if_new(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        task: OutboxTask,
    ) -> OrcherResult<Option<Uuid>> {
        self.repository.create_in_tx_if_new(tx, task).await
    }

    /// Claim a batch of tasks for processing (for a specific workflow)
    ///
    /// Returns tasks in FIFO order for the given workflow to maintain ordering guarantees.
    ///
    /// # Arguments
    ///
    /// * `organization_id` - Optional organization ID for multi-tenant filtering
    /// * `workflow_id` - The workflow to claim tasks for
    /// * `batch_size` - Maximum number of tasks to claim
    ///
    /// # Returns
    ///
    /// List of claimed tasks ready for processing
    pub async fn claim_tasks_for_workflow(
        &self,
        organization_id: Option<Uuid>,
        workflow_id: Uuid,
        batch_size: usize,
    ) -> OrcherResult<Vec<OutboxTask>> {
        self.repository
            .claim_tasks_for_workflow(organization_id, workflow_id, batch_size)
            .await
    }

    /// Claim the oldest ready tasks, EXCLUDING specified task types
    ///
    /// Used by OutboxProcessor to claim orchestration tasks (DispatchStep, etc.)
    /// while excluding DispatchTask which is handled by TaskPollingManager; see
    /// [`OutboxRepository::claim_tasks_excluding_types`] for the order they
    /// leave in and why one workflow's tasks may be claimed together.
    ///
    /// # Arguments
    ///
    /// * `organization_id` - Optional organization ID for multi-tenant filtering
    /// * `batch_size` - Maximum number of tasks to claim
    /// * `exclude_types` - Task types to EXCLUDE from claiming (e.g., ["dispatch_task"])
    ///
    /// # Returns
    ///
    /// List of claimed tasks, oldest first, excluding specified types
    pub async fn claim_tasks_excluding_types(
        &self,
        organization_id: Option<Uuid>,
        batch_size: usize,
        exclude_types: &[&str],
    ) -> OrcherResult<Vec<OutboxTask>> {
        self.repository
            .claim_tasks_excluding_types(organization_id, batch_size, exclude_types)
            .await
    }

    /// Claim the oldest pending task dispatches of one `(namespace, task_queue)`.
    ///
    /// Used by TaskPollingManager to claim only DispatchTask tasks, and only
    /// those its poll can deliver; see
    /// [`OutboxRepository::claim_dispatch_tasks`] for the order they leave in.
    ///
    /// # Arguments
    ///
    /// * `organization_id` - Optional organization ID for multi-tenant filtering
    /// * `batch_size` - Maximum number of tasks to claim
    /// * `task_queue` - The task queue the poll serves
    /// * `namespace` - The namespace the poll serves
    pub async fn claim_dispatch_tasks(
        &self,
        organization_id: Option<Uuid>,
        batch_size: usize,
        task_queue: &str,
        namespace: &str,
    ) -> OrcherResult<Vec<OutboxTask>> {
        self.repository
            .claim_dispatch_tasks(organization_id, batch_size, task_queue, namespace)
            .await
    }

    /// Mark a task as completed
    ///
    /// # Arguments
    ///
    /// * `task_id` - The task ID to mark as completed
    pub async fn mark_completed(&self, task_id: Uuid) -> OrcherResult<()> {
        self.repository.complete(task_id).await
    }

    /// Complete a dispatch this engine claimed but could not deliver, its task
    /// no longer waiting; see [`OutboxRepository::settle_claimed`].
    pub async fn settle_claimed(&self, task_id: Uuid) -> OrcherResult<()> {
        self.repository.settle_claimed(task_id).await
    }

    /// Release a claimed task back to pending status
    ///
    /// Used when a worker claims a task but determines it's not the right match
    /// (e.g., wrong task_type, wrong queue, etc.)
    ///
    /// # Arguments
    ///
    /// * `task_id` - The task ID to release
    pub async fn release_claimed_task(&self, task_id: Uuid) -> OrcherResult<()> {
        self.repository.reset_task_to_pending(task_id).await
    }

    /// Mark a task as failed
    ///
    /// If the task has remaining retries, it will be scheduled for retry with
    /// exponential backoff. Otherwise, it will be marked as permanently failed.
    ///
    /// # Arguments
    ///
    /// * `task_id` - The task ID to mark as failed
    /// * `error_message` - The error message
    /// * `error_details` - Optional detailed error information
    ///
    /// # Returns
    ///
    /// `true` if the task should be moved to DLQ (no more retries), `false` if it was scheduled for retry
    pub async fn mark_failed(
        &self,
        task_id: Uuid,
        error_message: String,
        error_details: Option<serde_json::Value>,
    ) -> OrcherResult<bool> {
        // Get the task to check if it can be retried
        let task = self.repository.find_by_id(task_id).await?;

        let task = match task {
            Some(t) => t,
            None => {
                warn!(task_id = %task_id, "Task not found for failure marking");
                return Ok(false);
            }
        };

        let should_move_to_dlq = !task.can_retry();

        // Delegate to repository to handle the failure logic
        self.repository
            .fail(task_id, error_message, error_details)
            .await?;

        Ok(should_move_to_dlq)
    }

    /// Move a task to the dead letter queue
    ///
    /// This is called when a task has exhausted all retries.
    ///
    /// # Arguments
    ///
    /// * `task_id` - The task ID to move to DLQ
    ///
    /// # Returns
    ///
    /// The DLQ entry ID
    pub async fn move_to_dlq(&self, task_id: Uuid) -> OrcherResult<Uuid> {
        // Get the failed task
        let task = self.repository.find_by_id(task_id).await?;

        let task = match task {
            Some(t) => t,
            None => {
                return Err(OrcherError::Internal {
                    message: format!("Task {} not found for DLQ move", task_id),
                    source: None,
                });
            }
        };

        // Construct error details
        let _error_details = serde_json::json!({
            "last_error": task.last_error,
            "error_details": task.error_details,
            "attempt_count": task.attempt_count,
            "max_retries": task.max_retries,
        });

        // Move to DLQ using the repository
        // Note: We need to import CoordinationRepository for DLQ operations
        // For now, we'll delegate this to a higher-level coordinator
        // that has access to both outbox and DLQ repositories

        debug!(
            task_id = %task_id,
            workflow_id = %task.workflow_id,
            "Task should be moved to DLQ (exhausted all retries)"
        );

        // Return the task_id as DLQ operation will be handled by coordinator
        Ok(task_id)
    }

    /// Cleanup stuck tasks that have been processing for too long
    ///
    /// Resets stuck tasks back to pending status for retry.
    ///
    /// # Arguments
    ///
    /// * `stuck_threshold_secs` - Tasks processing longer than this (in seconds) are considered stuck
    ///
    /// # Returns
    ///
    /// Number of tasks that were reset
    pub async fn cleanup_stuck_tasks(&self, stuck_threshold_secs: i64) -> OrcherResult<usize> {
        let stuck_duration = SignedDuration::from_secs(stuck_threshold_secs);
        let count = self.repository.reset_stuck_tasks(stuck_duration).await?;
        Ok(count as usize)
    }

    /// Get queue metrics for monitoring
    ///
    /// # Returns
    ///
    /// Queue metrics including pending, processing, completed, and failed counts
    pub async fn get_metrics(&self) -> OrcherResult<QueueMetrics> {
        self.repository.get_metrics().await
    }

    /// Get the number of pending tasks for a specific workflow
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - The workflow ID
    ///
    /// # Returns
    ///
    /// Number of pending tasks
    pub async fn get_pending_count_for_workflow(&self, workflow_id: Uuid) -> OrcherResult<i64> {
        let tasks = self
            .repository
            .list_pending_for_workflow(workflow_id)
            .await?;
        Ok(tasks.len() as i64)
    }

    /// List pending tasks for a workflow
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - The workflow ID
    ///
    /// # Returns
    ///
    /// List of pending tasks in FIFO order
    pub async fn list_pending_tasks(&self, workflow_id: Uuid) -> OrcherResult<Vec<OutboxTask>> {
        self.repository.list_pending_for_workflow(workflow_id).await
    }

    /// Find a task by ID
    ///
    /// # Arguments
    ///
    /// * `task_id` - The task ID
    ///
    /// # Returns
    ///
    /// The task if found
    pub async fn find_task(&self, task_id: Uuid) -> OrcherResult<Option<OutboxTask>> {
        self.repository.find_by_id(task_id).await
    }
}

// ============================================================================
// TaskOutbox Trait Implementation
// ============================================================================

#[async_trait::async_trait]
impl orcher_dispatch::traits::TaskOutbox for OutboxQueue {
    async fn claim_dispatch_tasks(
        &self,
        organization_id: Option<Uuid>,
        batch_size: usize,
        task_queue: &str,
        namespace: &str,
    ) -> OrcherResult<Vec<OutboxTask>> {
        self.claim_dispatch_tasks(organization_id, batch_size, task_queue, namespace)
            .await
    }

    async fn release_claimed_task(&self, task_id: Uuid) -> OrcherResult<()> {
        self.release_claimed_task(task_id).await
    }

    async fn mark_completed(&self, task_id: Uuid) -> OrcherResult<()> {
        self.mark_completed(task_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: These tests require a database connection and are typically
    // run as integration tests. Unit tests would mock the repository.

    #[test]
    fn test_outbox_queue_creation() {
        // Compile-time test to ensure OutboxQueue can be created
        // with a repository
        fn _assert_can_create(repo: Arc<OutboxRepository>) {
            let _queue = OutboxQueue::new(repo);
        }
    }
}
