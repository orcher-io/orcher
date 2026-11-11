//! Schedule queue for cron-based workflow scheduling.

use orcher_common::error::OrcherResult;
use orcher_common::types::identifiers::ScheduleId;
use orcher_common::types::time::Timestamptz;
use orcher_storage::repositories::domains::schedule::ScheduleRepository;
use orcher_storage::schema::types::Schedule;
use orcher_storage::traits::domain::ScheduleRepositoryTrait;

use jiff::SignedDuration;
use std::sync::Arc;

/// Schedule queue for cron-based workflow scheduling.
///
/// Thin wrapper around ScheduleRepository that provides the interface
/// used by the ScheduleProcessor. Mirrors the TimerQueue pattern.
#[derive(Clone)]
pub struct ScheduleQueue {
    repository: Arc<ScheduleRepository>,
}

impl ScheduleQueue {
    /// Create a new schedule queue.
    pub fn new(repository: Arc<ScheduleRepository>) -> Self {
        Self { repository }
    }

    /// Claim due schedules for processing using FOR UPDATE SKIP LOCKED.
    ///
    /// Returns schedules whose `next_run_at` has passed and are active + enabled.
    /// Multiple processors can safely call this concurrently.
    pub async fn claim_ready_schedules(&self, batch_size: usize) -> OrcherResult<Vec<Schedule>> {
        self.repository.claim_ready_schedules(batch_size).await
    }

    /// Get the earliest next_run_at across all active schedules.
    ///
    /// Used to determine how long to sleep before the next check.
    pub async fn get_next_fire_time(&self) -> OrcherResult<Option<Timestamptz>> {
        self.repository.get_next_fire_time().await
    }

    /// Mark a schedule as fired: update timestamps, increment run_count,
    /// and reset status to active.
    pub async fn mark_fired(
        &self,
        schedule_id: ScheduleId,
        next_run_at: Option<Timestamptz>,
    ) -> OrcherResult<()> {
        self.repository.mark_fired(schedule_id, next_run_at).await
    }

    /// Increment the failure count for a schedule and reset status to active.
    pub async fn mark_failed(&self, schedule_id: ScheduleId) -> OrcherResult<()> {
        self.repository.increment_failure_count(schedule_id).await?;
        self.repository
            .update_status(
                schedule_id,
                orcher_storage::schema::types::ScheduleStatus::Active,
            )
            .await
    }

    /// Reset schedules stuck in `processing` state for longer than the threshold.
    pub async fn cleanup_stuck_schedules(&self, stuck_threshold_secs: i64) -> OrcherResult<usize> {
        let threshold = SignedDuration::from_secs(stuck_threshold_secs);
        let count = self.repository.reset_stuck_schedules(threshold).await?;
        Ok(count as usize)
    }

    /// Count active (running) instances for a schedule.
    pub async fn count_active_instances(&self, schedule_id: ScheduleId) -> OrcherResult<i64> {
        self.repository.count_active_instances(schedule_id).await
    }

    /// Insert a schedule execution record.
    pub async fn insert_execution(
        &self,
        schedule_id: ScheduleId,
        execution_number: i64,
        scheduled_at: Timestamptz,
        workflow_execution_id: Option<uuid::Uuid>,
    ) -> OrcherResult<uuid::Uuid> {
        self.repository
            .insert_execution(
                schedule_id,
                execution_number,
                scheduled_at,
                workflow_execution_id,
            )
            .await
    }

    /// Upsert a schedule by name (for SDK registration).
    pub async fn upsert(&self, schedule: Schedule) -> OrcherResult<Schedule> {
        self.repository.upsert_by_name(schedule).await
    }

    /// Find a schedule by ID.
    pub async fn find_by_id(&self, id: ScheduleId) -> OrcherResult<Option<Schedule>> {
        use orcher_storage::repositories::base::CrudOperations;
        self.repository.find_by_id(id).await
    }

    /// Find schedules that should be marked as completed.
    pub async fn find_schedules_to_complete(&self) -> OrcherResult<Vec<Schedule>> {
        use orcher_storage::traits::domain::ScheduleRepositoryTrait;
        self.repository.find_schedules_to_complete().await
    }

    /// Update schedule status.
    pub async fn update_status(
        &self,
        schedule_id: ScheduleId,
        status: orcher_storage::schema::types::ScheduleStatus,
    ) -> OrcherResult<()> {
        use orcher_storage::traits::domain::ScheduleRepositoryTrait;
        self.repository.update_status(schedule_id, status).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schedule_queue_creation() {
        fn _assert_can_create(repo: Arc<ScheduleRepository>) {
            let _queue = ScheduleQueue::new(repo);
        }
    }

    #[test]
    fn test_schedule_queue_is_clone() {
        fn _assert_clone<T: Clone>() {}
        _assert_clone::<ScheduleQueue>();
    }
}
