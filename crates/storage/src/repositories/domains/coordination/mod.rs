//! # Coordination Queue Repository Module
//!
//! This module provides the main coordination repository implementation that combines
//! outbox, timer, and dead letter queue repositories into a single unified interface.
//!
//! The coordination repository implements the `CoordinationRepositoryTrait` and delegates
//! operations to specialized sub-repositories for better separation of concerns.

mod dlq_repository;
mod outbox_repository;
mod timer_repository;

use async_trait::async_trait;
use orcher_common::types::time::Timestamptz;
use orcher_common::OrcherResult;
use sqlx::{PgPool, Postgres, Transaction};
use std::sync::Arc;
use uuid::Uuid;

use crate::schema::types::coordination::{DeadLetterTask, OutboxTask, QueueMetrics, TimerTask};
use crate::traits::domain::CoordinationRepositoryTrait;

pub use dlq_repository::DeadLetterQueueRepository;
pub use outbox_repository::OutboxRepository;
pub use timer_repository::TimerRepository;

/// Main coordination repository that combines all coordination queue operations
///
/// This repository acts as a facade over the specialized repositories and implements
/// the `CoordinationRepositoryTrait` for unified access to all coordination features.
#[derive(Clone)]
pub struct CoordinationRepository {
    /// Database connection pool
    _pool: Arc<PgPool>,
    /// Outbox queue repository
    outbox: OutboxRepository,
    /// Timer queue repository
    timer: TimerRepository,
    /// Dead letter queue repository
    dlq: DeadLetterQueueRepository,
}

impl CoordinationRepository {
    /// Create a new coordination repository
    ///
    /// # Arguments
    ///
    /// * `pool` - Database connection pool
    pub fn new(pool: Arc<PgPool>) -> Self {
        Self {
            _pool: pool.clone(),
            outbox: OutboxRepository::new(pool.clone()),
            timer: TimerRepository::new(pool.clone()),
            dlq: DeadLetterQueueRepository::new(pool),
        }
    }

    /// Get reference to outbox repository
    pub fn outbox(&self) -> &OutboxRepository {
        &self.outbox
    }

    /// Get reference to timer repository
    pub fn timer(&self) -> &TimerRepository {
        &self.timer
    }

    /// Get reference to dead letter queue repository
    pub fn dlq(&self) -> &DeadLetterQueueRepository {
        &self.dlq
    }
}

#[async_trait]
impl CoordinationRepositoryTrait for CoordinationRepository {
    // ========================================================================
    // Outbox Queue Operations
    // ========================================================================

    async fn create_outbox_task(&self, task: OutboxTask) -> OrcherResult<OutboxTask> {
        self.outbox.create(task).await
    }

    async fn create_outbox_task_in_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        task: OutboxTask,
    ) -> OrcherResult<Uuid> {
        self.outbox.create_in_tx(tx, task).await
    }

    async fn find_outbox_task(&self, task_id: Uuid) -> OrcherResult<Option<OutboxTask>> {
        self.outbox.find_by_id(task_id).await
    }

    async fn claim_outbox_tasks_for_workflow(
        &self,
        organization_id: Option<Uuid>,
        workflow_id: Uuid,
        batch_size: usize,
    ) -> OrcherResult<Vec<OutboxTask>> {
        self.outbox
            .claim_tasks_for_workflow(organization_id, workflow_id, batch_size)
            .await
    }

    async fn complete_outbox_task(&self, task_id: Uuid) -> OrcherResult<()> {
        self.outbox.complete(task_id).await
    }

    async fn fail_outbox_task(
        &self,
        task_id: Uuid,
        error_message: String,
        error_details: Option<serde_json::Value>,
    ) -> OrcherResult<()> {
        self.outbox
            .fail(task_id, error_message, error_details)
            .await
    }

    async fn reset_stuck_outbox_tasks(
        &self,
        stuck_threshold: jiff::SignedDuration,
    ) -> OrcherResult<i64> {
        self.outbox.reset_stuck_tasks(stuck_threshold).await
    }

    async fn list_pending_outbox_tasks(&self, workflow_id: Uuid) -> OrcherResult<Vec<OutboxTask>> {
        self.outbox.list_pending_for_workflow(workflow_id).await
    }

    async fn get_outbox_metrics(&self) -> OrcherResult<QueueMetrics> {
        self.outbox.get_metrics().await
    }

    async fn cleanup_completed_outbox_tasks(&self, retention_days: i32) -> OrcherResult<i64> {
        self.outbox.cleanup_completed(retention_days).await
    }

    async fn claim_task_for_eager_return(
        &self,
        organization_id: Option<Uuid>,
        task_queue: &str,
        namespace: &str,
    ) -> OrcherResult<Option<OutboxTask>> {
        self.outbox
            .claim_task_for_eager_return(organization_id, task_queue, namespace)
            .await
    }

    // ========================================================================
    // Timer Queue Operations
    // ========================================================================

    async fn create_timer_task(&self, task: TimerTask) -> OrcherResult<TimerTask> {
        self.timer.create(task).await
    }

    async fn create_timer_task_in_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        task: TimerTask,
    ) -> OrcherResult<Uuid> {
        self.timer.create_in_tx(tx, task).await
    }

    async fn find_timer_task(&self, task_id: Uuid) -> OrcherResult<Option<TimerTask>> {
        self.timer.find_by_id(task_id).await
    }

    async fn claim_ready_timer_tasks(
        &self,
        organization_id: Option<Uuid>,
        batch_size: usize,
    ) -> OrcherResult<Vec<TimerTask>> {
        self.timer
            .claim_ready_tasks(organization_id, batch_size)
            .await
    }

    async fn get_next_timer_fire_time(
        &self,
        organization_id: Option<Uuid>,
    ) -> OrcherResult<Option<Timestamptz>> {
        self.timer.get_next_fire_time(organization_id).await
    }

    async fn complete_timer_task(&self, task_id: Uuid) -> OrcherResult<()> {
        self.timer.complete(task_id).await
    }

    async fn fail_timer_task(&self, task_id: Uuid, error_message: String) -> OrcherResult<()> {
        self.timer.fail(task_id, error_message).await
    }

    async fn cancel_timer_task(&self, task_id: Uuid) -> OrcherResult<()> {
        self.timer.cancel(task_id).await
    }

    async fn cancel_workflow_timers(&self, workflow_id: Uuid) -> OrcherResult<i64> {
        self.timer.cancel_workflow_timers(workflow_id).await
    }

    async fn reset_stuck_timer_tasks(
        &self,
        stuck_threshold: jiff::SignedDuration,
    ) -> OrcherResult<i64> {
        self.timer.reset_stuck_tasks(stuck_threshold).await
    }

    async fn list_workflow_timers(
        &self,
        workflow_id: Uuid,
        include_completed: bool,
    ) -> OrcherResult<Vec<TimerTask>> {
        self.timer
            .list_workflow_timers(workflow_id, include_completed)
            .await
    }

    async fn get_timer_metrics(&self) -> OrcherResult<QueueMetrics> {
        self.timer.get_metrics().await
    }

    async fn cleanup_completed_timer_tasks(&self, retention_days: i32) -> OrcherResult<i64> {
        self.timer.cleanup_completed(retention_days).await
    }

    // ========================================================================
    // Dead Letter Queue Operations
    // ========================================================================

    async fn move_outbox_task_to_dlq(
        &self,
        task: &OutboxTask,
        error_details: serde_json::Value,
    ) -> OrcherResult<DeadLetterTask> {
        self.dlq.move_outbox_task_to_dlq(task, error_details).await
    }

    async fn move_timer_task_to_dlq(
        &self,
        task: &TimerTask,
        error_details: serde_json::Value,
    ) -> OrcherResult<DeadLetterTask> {
        self.dlq.move_timer_task_to_dlq(task, error_details).await
    }

    async fn find_dead_letter_task(&self, dlq_id: Uuid) -> OrcherResult<Option<DeadLetterTask>> {
        self.dlq.find_by_id(dlq_id).await
    }

    async fn list_workflow_dead_letter_tasks(
        &self,
        workflow_id: Uuid,
        limit: usize,
    ) -> OrcherResult<Vec<DeadLetterTask>> {
        self.dlq.list_workflow_tasks(workflow_id, limit).await
    }

    async fn list_dead_letter_tasks(
        &self,
        limit: usize,
        offset: usize,
    ) -> OrcherResult<Vec<DeadLetterTask>> {
        self.dlq.list_all(limit, offset).await
    }

    async fn add_dlq_note(&self, dlq_id: Uuid, note: String) -> OrcherResult<()> {
        self.dlq.add_note(dlq_id, note).await
    }

    async fn delete_dead_letter_task(&self, dlq_id: Uuid) -> OrcherResult<()> {
        self.dlq.delete(dlq_id).await
    }

    async fn count_workflow_dead_letter_tasks(&self, workflow_id: Uuid) -> OrcherResult<i64> {
        self.dlq.count_workflow_tasks(workflow_id).await
    }

    async fn count_dead_letter_tasks(&self) -> OrcherResult<i64> {
        self.dlq.count_all().await
    }

    // ========================================================================
    // Batch Operations
    // ========================================================================

    async fn cleanup_completed_tasks(&self, retention_days: i32) -> OrcherResult<(i64, i64)> {
        let outbox_deleted = self.cleanup_completed_outbox_tasks(retention_days).await?;
        let timer_deleted = self.cleanup_completed_timer_tasks(retention_days).await?;
        Ok((outbox_deleted, timer_deleted))
    }

    async fn get_queue_health(&self) -> OrcherResult<(QueueMetrics, QueueMetrics, i64)> {
        let outbox_metrics = self.get_outbox_metrics().await?;
        let timer_metrics = self.get_timer_metrics().await?;
        let dlq_count = self.count_dead_letter_tasks().await?;
        Ok((outbox_metrics, timer_metrics, dlq_count))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_coordination_repository_structure() {
        // Compile-time test to ensure repository can be created
        fn _assert_can_create(pool: Arc<PgPool>) {
            let _repo = CoordinationRepository::new(pool);
        }
    }

    #[test]
    fn test_coordination_repository_trait_impl() {
        // Compile-time test to ensure trait is properly implemented
        fn _assert_trait_impl<T: CoordinationRepositoryTrait>() {}
        _assert_trait_impl::<CoordinationRepository>();
    }

    #[test]
    fn test_sub_repository_access() {
        // Ensure sub-repositories are accessible
        fn _assert_sub_repos(pool: Arc<PgPool>) {
            let repo = CoordinationRepository::new(pool);
            let _outbox = repo.outbox();
            let _timer = repo.timer();
            let _dlq = repo.dlq();
        }
    }
}
