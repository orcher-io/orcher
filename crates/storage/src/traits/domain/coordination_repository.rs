//! # Coordination Queue Repository Domain Trait
//!
//! Domain-specific repository operations for coordination queues in the
//! workflow orchestration model.
//!
//! This trait provides persistence operations for:
//! - Outbox pattern (atomic task dispatch)
//! - Timer queue (durable scheduling)
//! - Dead letter queue (failed task handling)
//!
//! These queues support Coordination Queues features for reliable
//! workflow coordination with guaranteed delivery semantics.

use async_trait::async_trait;
use orcher_common::types::time::Timestamptz;
use orcher_common::OrcherResult;
use uuid::Uuid;

use crate::schema::types::coordination::{DeadLetterTask, OutboxTask, QueueMetrics, TimerTask};

/// Domain-specific repository trait for Coordination Queue entities
///
/// This trait provides operations for managing outbox tasks, timer tasks,
/// and dead letter queue entries in a distributed architecture.
///
/// ## Design Principles
///
/// - **Atomicity**: Outbox tasks created within same transaction as workflow state
/// - **At-least-once delivery**: Guaranteed task processing with retries
/// - **Ordering**: Per-workflow FIFO ordering for outbox tasks
/// - **Durability**: Timer tasks survive coordinator crashes
/// - **Observability**: Dead letter queue for failed tasks
///
/// ## Usage
///
/// ```rust,ignore
/// use orcher_storage::traits::domain::CoordinationRepositoryTrait;
///
/// async fn example(repo: &impl CoordinationRepositoryTrait) -> OrcherResult<()> {
///     // Create outbox task within transaction
///     let task = OutboxTask::new(...);
///     repo.create_outbox_task(task).await?;
///
///     // Schedule timer
///     let timer = TimerTask::new(...);
///     repo.create_timer_task(timer).await?;
///
///     // Claim tasks for processing
///     let tasks = repo.claim_outbox_tasks_for_workflow(workflow_id, 10).await?;
///
///     Ok(())
/// }
/// ```
#[async_trait]
pub trait CoordinationRepositoryTrait: Send + Sync {
    // ========================================================================
    // Outbox Queue Operations
    // ========================================================================

    /// Create a new outbox task
    ///
    /// This should be called within the same transaction as the workflow state change
    /// to ensure atomic dispatch.
    ///
    /// # Arguments
    ///
    /// * `task` - The outbox task to create
    ///
    /// # Returns
    ///
    /// The created task with database-generated fields populated
    async fn create_outbox_task(&self, task: OutboxTask) -> OrcherResult<OutboxTask>;

    /// Create a new outbox task within an existing transaction
    ///
    /// This is the primary method for atomic outbox pattern - task is created
    /// atomically with the workflow state change.
    ///
    /// # Arguments
    ///
    /// * `tx` - The active transaction
    /// * `task` - The outbox task to create
    ///
    /// # Returns
    ///
    /// The task ID of the created (or existing if duplicate) task
    async fn create_outbox_task_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        task: OutboxTask,
    ) -> OrcherResult<Uuid>;

    /// Find an outbox task by ID
    ///
    /// # Arguments
    ///
    /// * `task_id` - The task ID
    async fn find_outbox_task(&self, task_id: Uuid) -> OrcherResult<Option<OutboxTask>>;

    /// Claim a batch of tasks for a specific workflow
    ///
    /// Returns tasks in FIFO order for the given workflow to maintain ordering guarantees.
    /// Tasks are atomically marked as 'processing'.
    ///
    /// # Arguments
    ///
    /// * `organization_id` - Optional organization ID for multi-tenant filtering
    /// * `workflow_id` - The workflow ID
    /// * `batch_size` - Maximum number of tasks to claim
    ///
    /// # Returns
    ///
    /// List of claimed tasks ready for processing
    async fn claim_outbox_tasks_for_workflow(
        &self,
        organization_id: Option<Uuid>,
        workflow_id: Uuid,
        batch_size: usize,
    ) -> OrcherResult<Vec<OutboxTask>>;

    /// Mark an outbox task as completed
    ///
    /// # Arguments
    ///
    /// * `task_id` - The task ID
    async fn complete_outbox_task(&self, task_id: Uuid) -> OrcherResult<()>;

    /// Mark an outbox task as failed
    ///
    /// If the task has remaining retries, it will be scheduled for retry with
    /// exponential backoff. Otherwise, it will be moved to the dead letter queue.
    ///
    /// # Arguments
    ///
    /// * `task_id` - The task ID
    /// * `error_message` - The error message
    /// * `error_details` - Optional error details as JSON
    async fn fail_outbox_task(
        &self,
        task_id: Uuid,
        error_message: String,
        error_details: Option<serde_json::Value>,
    ) -> OrcherResult<()>;

    /// Reset stuck processing tasks back to pending
    ///
    /// Finds tasks that have been in 'processing' state for too long and
    /// resets them to 'pending' for retry.
    ///
    /// # Arguments
    ///
    /// * `stuck_threshold` - Tasks processing longer than this are considered stuck
    ///
    /// # Returns
    ///
    /// Number of tasks reset
    async fn reset_stuck_outbox_tasks(
        &self,
        stuck_threshold: jiff::SignedDuration,
    ) -> OrcherResult<i64>;

    /// Get pending outbox tasks for a workflow
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - The workflow ID
    ///
    /// # Returns
    ///
    /// List of pending tasks in FIFO order
    async fn list_pending_outbox_tasks(&self, workflow_id: Uuid) -> OrcherResult<Vec<OutboxTask>>;

    /// Get outbox queue metrics
    ///
    /// # Returns
    ///
    /// Queue metrics for monitoring
    async fn get_outbox_metrics(&self) -> OrcherResult<QueueMetrics>;

    /// Cleanup old completed outbox tasks
    ///
    /// # Arguments
    ///
    /// * `retention_days` - Delete tasks completed before this many days ago
    ///
    /// # Returns
    ///
    /// Number of tasks deleted
    async fn cleanup_completed_outbox_tasks(&self, retention_days: i32) -> OrcherResult<i64>;

    /// Claim a single task for eager return during task completion.
    ///
    /// This enables eager task return: when a worker completes a task,
    /// we can immediately return another pending task from the same queue,
    /// eliminating a poll round-trip.
    ///
    /// # Arguments
    ///
    /// * `organization_id` - Optional organization ID for multi-tenant filtering
    /// * `task_queue` - The task queue to claim from
    /// * `namespace` - The namespace to claim from
    ///
    /// # Returns
    ///
    /// * `Ok(Some(task))` - A claimed task ready for execution
    /// * `Ok(None)` - No pending tasks available
    async fn claim_task_for_eager_return(
        &self,
        organization_id: Option<Uuid>,
        task_queue: &str,
        namespace: &str,
    ) -> OrcherResult<Option<OutboxTask>>;

    // ========================================================================
    // Timer Queue Operations
    // ========================================================================

    /// Create a new timer task
    ///
    /// # Arguments
    ///
    /// * `task` - The timer task to create
    ///
    /// # Returns
    ///
    /// The created task with database-generated fields populated
    async fn create_timer_task(&self, task: TimerTask) -> OrcherResult<TimerTask>;

    /// Create a new timer task within an existing transaction
    ///
    /// # Arguments
    ///
    /// * `tx` - The active transaction
    /// * `task` - The timer task to create
    ///
    /// # Returns
    ///
    /// The task ID of the created task
    async fn create_timer_task_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        task: TimerTask,
    ) -> OrcherResult<Uuid>;

    /// Find a timer task by ID
    ///
    /// # Arguments
    ///
    /// * `task_id` - The task ID
    async fn find_timer_task(&self, task_id: Uuid) -> OrcherResult<Option<TimerTask>>;

    /// Claim timer tasks that are ready to fire
    ///
    /// Returns timers that have reached their fire_at time, in chronological order.
    /// Tasks are atomically marked as 'processing'.
    ///
    /// # Arguments
    ///
    /// * `organization_id` - Optional organization ID for multi-tenant filtering
    /// * `batch_size` - Maximum number of timers to claim
    ///
    /// # Returns
    ///
    /// List of timers ready to fire
    async fn claim_ready_timer_tasks(
        &self,
        organization_id: Option<Uuid>,
        batch_size: usize,
    ) -> OrcherResult<Vec<TimerTask>>;

    /// Get the next timer fire time
    ///
    /// Returns the fire_at timestamp of the earliest pending timer,
    /// which helps the timer processor know when to wake up.
    ///
    /// # Arguments
    ///
    /// * `organization_id` - Optional organization ID for multi-tenant filtering
    ///
    /// # Returns
    ///
    /// The next fire time, or None if no pending timers
    async fn get_next_timer_fire_time(
        &self,
        organization_id: Option<Uuid>,
    ) -> OrcherResult<Option<Timestamptz>>;

    /// Mark a timer task as completed
    ///
    /// # Arguments
    ///
    /// * `task_id` - The task ID
    async fn complete_timer_task(&self, task_id: Uuid) -> OrcherResult<()>;

    /// Mark a timer task as failed
    ///
    /// If the task has remaining retries, it will be rescheduled.
    /// Otherwise, it will be moved to the dead letter queue.
    ///
    /// # Arguments
    ///
    /// * `task_id` - The task ID
    /// * `error_message` - The error message
    async fn fail_timer_task(&self, task_id: Uuid, error_message: String) -> OrcherResult<()>;

    /// Cancel a timer task
    ///
    /// Marks the timer as cancelled so it won't fire.
    ///
    /// # Arguments
    ///
    /// * `task_id` - The task ID
    async fn cancel_timer_task(&self, task_id: Uuid) -> OrcherResult<()>;

    /// Cancel all timer tasks for a workflow
    ///
    /// Used when a workflow completes or fails.
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - The workflow ID
    ///
    /// # Returns
    ///
    /// Number of timers cancelled
    async fn cancel_workflow_timers(&self, workflow_id: Uuid) -> OrcherResult<i64>;

    /// Reset stuck processing timer tasks back to pending
    ///
    /// # Arguments
    ///
    /// * `stuck_threshold` - Tasks processing longer than this are considered stuck
    ///
    /// # Returns
    ///
    /// Number of tasks reset
    async fn reset_stuck_timer_tasks(
        &self,
        stuck_threshold: jiff::SignedDuration,
    ) -> OrcherResult<i64>;

    /// List all timer tasks for a workflow
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - The workflow ID
    /// * `include_completed` - Whether to include completed/cancelled timers
    ///
    /// # Returns
    ///
    /// List of timer tasks
    async fn list_workflow_timers(
        &self,
        workflow_id: Uuid,
        include_completed: bool,
    ) -> OrcherResult<Vec<TimerTask>>;

    /// Get timer queue metrics
    ///
    /// # Returns
    ///
    /// Queue metrics for monitoring
    async fn get_timer_metrics(&self) -> OrcherResult<QueueMetrics>;

    /// Cleanup old completed timer tasks
    ///
    /// # Arguments
    ///
    /// * `retention_days` - Delete tasks completed before this many days ago
    ///
    /// # Returns
    ///
    /// Number of tasks deleted
    async fn cleanup_completed_timer_tasks(&self, retention_days: i32) -> OrcherResult<i64>;

    // ========================================================================
    // Dead Letter Queue Operations
    // ========================================================================

    /// Move a failed outbox task to the dead letter queue
    ///
    /// This is called automatically when a task exhausts all retries.
    ///
    /// # Arguments
    ///
    /// * `task` - The failed outbox task
    /// * `error_details` - Complete error details
    ///
    /// # Returns
    ///
    /// The created dead letter task
    async fn move_outbox_task_to_dlq(
        &self,
        task: &OutboxTask,
        error_details: serde_json::Value,
    ) -> OrcherResult<DeadLetterTask>;

    /// Move a failed timer task to the dead letter queue
    ///
    /// # Arguments
    ///
    /// * `task` - The failed timer task
    /// * `error_details` - Complete error details
    ///
    /// # Returns
    ///
    /// The created dead letter task
    async fn move_timer_task_to_dlq(
        &self,
        task: &TimerTask,
        error_details: serde_json::Value,
    ) -> OrcherResult<DeadLetterTask>;

    /// Find a dead letter task by ID
    ///
    /// # Arguments
    ///
    /// * `dlq_id` - The DLQ entry ID
    async fn find_dead_letter_task(&self, dlq_id: Uuid) -> OrcherResult<Option<DeadLetterTask>>;

    /// List dead letter tasks for a workflow
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - The workflow ID
    /// * `limit` - Maximum number of entries to return
    ///
    /// # Returns
    ///
    /// List of dead letter tasks
    async fn list_workflow_dead_letter_tasks(
        &self,
        workflow_id: Uuid,
        limit: usize,
    ) -> OrcherResult<Vec<DeadLetterTask>>;

    /// List all dead letter tasks
    ///
    /// # Arguments
    ///
    /// * `limit` - Maximum number of entries to return
    /// * `offset` - Offset for pagination
    ///
    /// # Returns
    ///
    /// List of dead letter tasks
    async fn list_dead_letter_tasks(
        &self,
        limit: usize,
        offset: usize,
    ) -> OrcherResult<Vec<DeadLetterTask>>;

    /// Add a note to a dead letter task
    ///
    /// Used for manual intervention tracking.
    ///
    /// # Arguments
    ///
    /// * `dlq_id` - The DLQ entry ID
    /// * `note` - The note to add
    async fn add_dlq_note(&self, dlq_id: Uuid, note: String) -> OrcherResult<()>;

    /// Delete a dead letter task
    ///
    /// Used after manual resolution.
    ///
    /// # Arguments
    ///
    /// * `dlq_id` - The DLQ entry ID
    async fn delete_dead_letter_task(&self, dlq_id: Uuid) -> OrcherResult<()>;

    /// Get count of dead letter tasks by workflow
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - The workflow ID
    ///
    /// # Returns
    ///
    /// Count of DLQ entries for this workflow
    async fn count_workflow_dead_letter_tasks(&self, workflow_id: Uuid) -> OrcherResult<i64>;

    /// Get count of all dead letter tasks
    ///
    /// # Returns
    ///
    /// Total count of DLQ entries
    async fn count_dead_letter_tasks(&self) -> OrcherResult<i64>;

    // ========================================================================
    // Batch Operations
    // ========================================================================

    /// Cleanup all old completed tasks (outbox + timer)
    ///
    /// Convenience method to cleanup both queues at once.
    ///
    /// # Arguments
    ///
    /// * `retention_days` - Delete tasks completed before this many days ago
    ///
    /// # Returns
    ///
    /// Tuple of (outbox_deleted, timer_deleted)
    async fn cleanup_completed_tasks(&self, retention_days: i32) -> OrcherResult<(i64, i64)>;

    /// Get health status of all coordination queues
    ///
    /// Returns metrics for monitoring and alerting.
    ///
    /// # Returns
    ///
    /// Tuple of (outbox_metrics, timer_metrics, dlq_count)
    async fn get_queue_health(&self) -> OrcherResult<(QueueMetrics, QueueMetrics, i64)>;
}
