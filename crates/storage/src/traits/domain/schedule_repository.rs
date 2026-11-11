//! # Schedule Repository Domain Trait
//!
//! Domain-specific repository operations for schedules using orcher-common types
//! for maximum compatibility and type safety across the ORCHER ecosystem.

use async_trait::async_trait;
use orcher_common::types::time::Timestamptz;
use orcher_common::{types::identifiers::ScheduleId, OrcherResult};

use crate::{
    schema::types::{Schedule, ScheduleStatus},
    traits::core::Repository,
};

/// Domain-specific repository trait for Schedule entities
///
/// This trait extends the base Repository trait with schedule-specific operations
/// leveraging orcher-common types for consistency across the ORCHER ecosystem.
#[async_trait]
pub trait ScheduleRepositoryTrait: Repository<Schedule> {
    /// Find all schedules regardless of status
    async fn find_all_schedules(&self) -> OrcherResult<Vec<Schedule>>;

    /// Find schedules by status
    async fn find_by_status(&self, status: ScheduleStatus) -> OrcherResult<Vec<Schedule>>;

    /// Find enabled schedules that are ready to run
    async fn find_ready_to_run(&self) -> OrcherResult<Vec<Schedule>>;

    /// Find schedules by name pattern (case-insensitive)
    async fn find_by_name_pattern(&self, pattern: &str) -> OrcherResult<Vec<Schedule>>;

    /// Update schedule status
    async fn update_status(
        &self,
        schedule_id: ScheduleId,
        status: ScheduleStatus,
    ) -> OrcherResult<()>;

    /// Update schedule next run time
    async fn update_next_run(
        &self,
        schedule_id: ScheduleId,
        next_run_at: Timestamptz,
    ) -> OrcherResult<()>;

    /// Update schedule last run time
    async fn update_last_run(
        &self,
        schedule_id: ScheduleId,
        last_run_at: Timestamptz,
    ) -> OrcherResult<()>;

    /// Increment schedule run count
    async fn increment_run_count(&self, schedule_id: ScheduleId) -> OrcherResult<()>;

    /// Increment schedule success count
    async fn increment_success_count(&self, schedule_id: ScheduleId) -> OrcherResult<()>;

    /// Increment schedule failure count
    async fn increment_failure_count(&self, schedule_id: ScheduleId) -> OrcherResult<()>;

    /// Enable a schedule
    async fn enable_schedule(&self, schedule_id: ScheduleId) -> OrcherResult<()>;

    /// Disable a schedule
    async fn disable_schedule(&self, schedule_id: ScheduleId) -> OrcherResult<()>;

    /// Find schedules by tag
    async fn find_by_tag(&self, tag: &str) -> OrcherResult<Vec<Schedule>>;

    /// Find schedules by cron expression
    async fn find_by_cron_expression(&self, cron_expression: &str) -> OrcherResult<Vec<Schedule>>;

    /// Find schedules created by a specific user
    async fn find_by_creator(&self, created_by: &str) -> OrcherResult<Vec<Schedule>>;

    /// Find schedules that have reached their max runs
    async fn find_completed_schedules(&self) -> OrcherResult<Vec<Schedule>>;

    /// Find schedules that should be completed (reached max_runs or passed end_date)
    async fn find_schedules_to_complete(&self) -> OrcherResult<Vec<Schedule>>;

    /// Count schedules by status
    async fn count_by_status(&self, status: ScheduleStatus) -> OrcherResult<u64>;

    /// Find schedules by timezone
    async fn find_by_timezone(&self, timezone: &str) -> OrcherResult<Vec<Schedule>>;

    /// Update schedule statistics (run count, success count, failure count)
    async fn update_statistics(
        &self,
        schedule_id: ScheduleId,
        run_count: i64,
        success_count: i64,
        failure_count: i64,
    ) -> OrcherResult<()>;

    /// Find all active schedules (enabled and status is active)
    async fn find_active_schedules(&self) -> OrcherResult<Vec<Schedule>>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use orcher_common::types::identifiers::traits::Identifier;

    #[test]
    fn test_schedule_repository_trait_bounds() {
        // Compile-time test to ensure trait bounds work correctly
        fn _assert_schedule_repository_bounds<R>()
        where
            R: ScheduleRepositoryTrait,
        {
            // This function just needs to compile
        }
    }

    #[test]
    fn test_schedule_repository_trait_is_send_sync() {
        // Ensure the trait is Send + Sync
        fn _assert_send_sync<T: Send + Sync>() {}
        fn _assert_repo<R: ScheduleRepositoryTrait>() {
            _assert_send_sync::<R>();
        }
    }

    #[test]
    fn test_schedule_id_usage() {
        // Test that ScheduleId works correctly
        let schedule_id = ScheduleId::new_v4();
        assert!(schedule_id.is_valid());
        assert!(!schedule_id.is_nil());
    }
}
