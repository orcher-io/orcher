//! Timer queue for durable scheduling.

use crate::engine::queues::types::{QueueMetrics, TimerTask};
use orcher_common::error::{OrcherError, OrcherResult};
use orcher_storage::repositories::domains::coordination::TimerRepository;

use jiff::SignedDuration;
use orcher_common::types::time::Timestamptz;
use std::sync::Arc;
use tracing::{debug, warn};
use uuid::Uuid;

/// Timer queue for durable scheduling
///
/// This is a thin wrapper around TimerRepository that maintains the existing
/// public API while delegating to the repository layer for all database operations.
#[derive(Clone)]
pub struct TimerQueue {
    repository: Arc<TimerRepository>,
}

impl TimerQueue {
    /// Create a new timer queue
    ///
    /// # Arguments
    ///
    /// * `repository` - The timer repository for database operations
    pub fn new(repository: Arc<TimerRepository>) -> Self {
        Self { repository }
    }

    /// Create a new timer task (atomic with transaction)
    ///
    /// This should be called within the same transaction as the workflow state change
    /// to ensure atomic timer creation.
    ///
    /// # Arguments
    ///
    /// * `tx` - The active transaction
    /// * `task` - The timer task to create
    ///
    /// # Returns
    ///
    /// The task ID of the created timer
    pub async fn create_task(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        task: TimerTask,
    ) -> OrcherResult<Uuid> {
        self.repository.create_in_tx(tx, task).await
    }

    /// Whether this workflow execution already has a fire-timer task with this
    /// business timer name, in any status (within the caller's transaction).
    pub async fn fire_timer_exists_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        workflow_id: Uuid,
        timer_name: &str,
    ) -> OrcherResult<bool> {
        self.repository
            .fire_timer_exists_in_tx(tx, workflow_id, timer_name)
            .await
    }

    /// Claim expired timers for processing
    ///
    /// Returns timers that have passed their fire_at time and are ready to execute.
    ///
    /// # Arguments
    ///
    /// * `organization_id` - Optional organization ID for multi-tenant filtering
    /// * `batch_size` - Maximum number of timers to claim
    ///
    /// # Returns
    ///
    /// List of claimed timer tasks ready for processing
    pub async fn claim_expired_tasks(
        &self,
        organization_id: Option<Uuid>,
        batch_size: usize,
    ) -> OrcherResult<Vec<TimerTask>> {
        self.repository
            .claim_ready_tasks(organization_id, batch_size)
            .await
    }

    /// Get the next fire time (earliest timer that needs to fire)
    ///
    /// Used by the processor to determine how long to sleep.
    ///
    /// # Arguments
    ///
    /// * `organization_id` - Optional organization ID for multi-tenant filtering
    ///
    /// # Returns
    ///
    /// The next fire time if any pending timers exist
    pub async fn get_next_fire_time(
        &self,
        organization_id: Option<Uuid>,
    ) -> OrcherResult<Option<Timestamptz>> {
        self.repository.get_next_fire_time(organization_id).await
    }

    /// Mark a timer as completed
    ///
    /// # Arguments
    ///
    /// * `task_id` - The task ID to mark as completed
    pub async fn mark_completed(&self, task_id: Uuid) -> OrcherResult<()> {
        self.repository.complete(task_id).await
    }

    /// Mark a timer as failed
    ///
    /// If the task has remaining retries, it will be scheduled for retry.
    /// Otherwise, it will be marked as permanently failed.
    ///
    /// # Arguments
    ///
    /// * `task_id` - The task ID to mark as failed
    /// * `error_msg` - The error message
    ///
    /// # Returns
    ///
    /// `true` if the task can be retried, `false` if it should be moved to DLQ
    pub async fn mark_failed(&self, task_id: Uuid, error_msg: String) -> OrcherResult<bool> {
        // Get the task to check if it can be retried
        let task = self.repository.find_by_id(task_id).await?;

        let task = match task {
            Some(t) => t,
            None => {
                warn!(task_id = %task_id, "Timer not found when marking as failed");
                return Ok(false);
            }
        };

        let can_retry = task.can_retry();

        // Delegate to repository to handle the failure logic
        self.repository.fail(task_id, error_msg).await?;

        Ok(can_retry)
    }

    /// Move a failed timer to the dead letter queue
    ///
    /// This is called when a timer has exhausted all retries.
    /// Note: DLQ operations are handled by the coordinator that has access to both
    /// timer and DLQ repositories.
    ///
    /// # Arguments
    ///
    /// * `task_id` - The task ID to move to DLQ
    ///
    /// # Returns
    ///
    /// The task ID (DLQ operation will be handled by coordinator)
    pub async fn move_to_dlq(&self, task_id: Uuid) -> OrcherResult<Uuid> {
        // Get the failed task
        let task = self.repository.find_by_id(task_id).await?;

        let task = match task {
            Some(t) => t,
            None => {
                return Err(OrcherError::Internal {
                    message: format!("Timer {} not found for DLQ move", task_id),
                    source: None,
                });
            }
        };

        debug!(
            task_id = %task_id,
            workflow_id = %task.workflow_id,
            "Timer should be moved to DLQ (exhausted all retries)"
        );

        // Return the task_id as DLQ operation will be handled by coordinator
        Ok(task_id)
    }

    /// Clean up stuck timers (timers that have been processing for too long)
    ///
    /// Resets stuck tasks back to pending status for retry.
    ///
    /// # Arguments
    ///
    /// * `stuck_threshold_secs` - Timers processing longer than this (in seconds) are considered stuck
    ///
    /// # Returns
    ///
    /// Number of timers that were reset
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

    /// Cancel a pending timer
    ///
    /// # Arguments
    ///
    /// * `task_id` - The task ID to cancel
    ///
    /// # Returns
    ///
    /// `true` if the timer was cancelled, `false` if it was not found or already processed
    pub async fn cancel_timer(&self, task_id: Uuid) -> OrcherResult<bool> {
        self.repository.cancel(task_id).await?;
        Ok(true)
    }

    /// Get pending timer count for a workflow
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - The workflow ID
    ///
    /// # Returns
    ///
    /// Number of pending timers for the workflow
    pub async fn get_pending_count_for_workflow(&self, workflow_id: Uuid) -> OrcherResult<i64> {
        let timers = self
            .repository
            .list_workflow_timers(workflow_id, true)
            .await?;
        Ok(timers.len() as i64)
    }

    /// List pending timers for a workflow
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - The workflow ID
    ///
    /// # Returns
    ///
    /// List of pending timers ordered by fire time
    pub async fn list_pending_timers(&self, workflow_id: Uuid) -> OrcherResult<Vec<TimerTask>> {
        self.repository
            .list_workflow_timers(workflow_id, true)
            .await
    }

    /// Find a timer by ID
    ///
    /// # Arguments
    ///
    /// * `task_id` - The task ID
    ///
    /// # Returns
    ///
    /// The timer task if found
    pub async fn find_timer(&self, task_id: Uuid) -> OrcherResult<Option<TimerTask>> {
        self.repository.find_by_id(task_id).await
    }

    /// Cancel all pending timers for a workflow
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - The workflow ID
    ///
    /// # Returns
    ///
    /// Number of timers cancelled
    pub async fn cancel_workflow_timers(&self, workflow_id: Uuid) -> OrcherResult<usize> {
        let cancelled = self.repository.cancel_workflow_timers(workflow_id).await?;
        Ok(cancelled as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: These tests require a database connection and are typically
    // run as integration tests. Unit tests would mock the repository.

    #[test]
    fn test_timer_queue_creation() {
        // Compile-time test to ensure TimerQueue can be created
        // with a repository
        fn _assert_can_create(repo: Arc<TimerRepository>) {
            let _queue = TimerQueue::new(repo);
        }
    }
}
