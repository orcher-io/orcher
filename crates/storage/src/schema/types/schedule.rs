//! Schedule storage types.

use orcher_common::types::time::Timestamptz;
use orcher_common::{
    types::identifiers::ScheduleId,
    validation::{Validatable, ValidationResult},
};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "postgres", derive(sqlx::FromRow))]
pub struct Schedule {
    pub id: ScheduleId,
    pub organization_id: Option<uuid::Uuid>,
    /// Must be unique
    pub name: String,
    pub description: Option<String>,
    pub cron_expression: String,
    /// IANA timezone identifier
    pub timezone: String,
    pub enabled: bool,
    pub task_template: JsonValue,
    pub max_instances: i32,
    pub overlap_policy: OverlapPolicy,
    pub timeout_seconds: Option<i32>,
    pub retry_policy: JsonValue,
    pub start_date: Option<Timestamptz>,
    pub end_date: Option<Timestamptz>,
    /// NULL for unlimited
    pub max_runs: Option<i64>,
    pub status: ScheduleStatus,
    pub next_run_at: Option<Timestamptz>,
    pub last_run_at: Option<Timestamptz>,
    pub run_count: i64,
    pub success_count: i64,
    pub failure_count: i64,
    #[cfg_attr(feature = "postgres", sqlx(default))]
    pub tags: Vec<String>,
    pub metadata: JsonValue,
    pub created_at: Timestamptz,
    pub updated_at: Timestamptz,
    pub created_by: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "postgres", derive(sqlx::Type))]
#[cfg_attr(
    feature = "postgres",
    sqlx(type_name = "varchar", rename_all = "lowercase")
)]
pub enum ScheduleStatus {
    Active,
    Paused,
    Disabled,
    /// Reached max_runs or end_date
    Completed,
    Failed,
    /// Claimed by a ScheduleProcessor
    Processing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "postgres", derive(sqlx::Type))]
#[cfg_attr(
    feature = "postgres",
    sqlx(type_name = "varchar", rename_all = "lowercase")
)]
pub enum OverlapPolicy {
    /// Skip if previous is still running
    Skip,
    /// Run after previous completes
    Queue,
    /// Cancel previous, start new
    Replace,
    Parallel,
}

impl Schedule {
    pub fn new(
        name: String,
        cron_expression: String,
        timezone: String,
        task_template: JsonValue,
    ) -> Self {
        let now = Timestamptz::now();
        Self {
            id: ScheduleId::new_v4(),
            organization_id: None,
            name,
            description: None,
            cron_expression,
            timezone,
            enabled: true,
            task_template,
            max_instances: 1,
            overlap_policy: OverlapPolicy::Skip,
            timeout_seconds: None,
            retry_policy: serde_json::json!({"max_attempts": 3, "delay_seconds": 60}),
            start_date: None,
            end_date: None,
            max_runs: None,
            status: ScheduleStatus::Active,
            next_run_at: None,
            last_run_at: None,
            run_count: 0,
            success_count: 0,
            failure_count: 0,
            tags: Vec::new(),
            metadata: serde_json::json!({}),
            created_at: now,
            updated_at: now,
            created_by: None,
        }
    }

    pub fn new_with_org(
        organization_id: uuid::Uuid,
        name: String,
        cron_expression: String,
        timezone: String,
        task_template: JsonValue,
    ) -> Self {
        let now = Timestamptz::now();
        Self {
            id: ScheduleId::new_v4(),
            organization_id: Some(organization_id),
            name,
            description: None,
            cron_expression,
            timezone,
            enabled: true,
            task_template,
            max_instances: 1,
            overlap_policy: OverlapPolicy::Skip,
            timeout_seconds: None,
            retry_policy: serde_json::json!({"max_attempts": 3, "delay_seconds": 60}),
            start_date: None,
            end_date: None,
            max_runs: None,
            status: ScheduleStatus::Active,
            next_run_at: None,
            last_run_at: None,
            run_count: 0,
            success_count: 0,
            failure_count: 0,
            tags: Vec::new(),
            metadata: serde_json::json!({}),
            created_at: now,
            updated_at: now,
            created_by: None,
        }
    }

    pub fn should_run(&self) -> bool {
        self.enabled
            && self.status == ScheduleStatus::Active
            && self.next_run_at.is_some()
            && self.next_run_at.unwrap() <= Timestamptz::now()
    }

    pub fn has_reached_max_runs(&self) -> bool {
        if let Some(max) = self.max_runs {
            self.run_count >= max
        } else {
            false
        }
    }
}

impl Default for ScheduleStatus {
    fn default() -> Self {
        Self::Active
    }
}

impl Default for OverlapPolicy {
    fn default() -> Self {
        Self::Skip
    }
}

impl Validatable for Schedule {
    fn validate(&self) -> ValidationResult {
        let mut result = ValidationResult::new();

        if self.name.is_empty() {
            result.add_error("name", "Schedule name cannot be empty");
        }

        if self.cron_expression.is_empty() {
            result.add_error("cron_expression", "Cron expression cannot be empty");
        }

        if self.timezone.is_empty() {
            result.add_error("timezone", "Timezone cannot be empty");
        }

        if self.max_instances <= 0 {
            result.add_error("max_instances", "Max instances must be greater than 0");
        }

        if let (Some(start), Some(end)) = (self.start_date, self.end_date) {
            if start > end {
                result.add_error(
                    "date_range",
                    "Start date must be before or equal to end date",
                );
            }
        }

        if let Some(max) = self.max_runs {
            if max <= 0 {
                result.add_error("max_runs", "Max runs must be greater than 0");
            }
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schedule_creation() {
        let schedule = Schedule::new(
            "daily-backup".to_string(),
            "0 0 0 * * *".to_string(),
            "UTC".to_string(),
            serde_json::json!({"task_type": "backup"}),
        );

        assert_eq!(schedule.name, "daily-backup");
        assert_eq!(schedule.cron_expression, "0 0 0 * * *");
        assert_eq!(schedule.timezone, "UTC");
        assert!(schedule.enabled);
        assert_eq!(schedule.status, ScheduleStatus::Active);
        assert_eq!(schedule.overlap_policy, OverlapPolicy::Skip);
        assert_eq!(schedule.max_instances, 1);
    }

    #[test]
    fn test_schedule_should_run() {
        let mut schedule = Schedule::new(
            "test".to_string(),
            "0 0 0 * * *".to_string(),
            "UTC".to_string(),
            serde_json::json!({}),
        );

        // Should not run without next_run_at
        assert!(!schedule.should_run());

        // Should run when next_run_at is in the past
        schedule.next_run_at = Some(Timestamptz::now() - jiff::SignedDuration::from_hours(1));
        assert!(schedule.should_run());

        // Should not run when disabled
        schedule.enabled = false;
        assert!(!schedule.should_run());
    }

    #[test]
    fn test_schedule_max_runs() {
        let mut schedule = Schedule::new(
            "test".to_string(),
            "0 0 0 * * *".to_string(),
            "UTC".to_string(),
            serde_json::json!({}),
        );

        schedule.max_runs = Some(5);
        assert!(!schedule.has_reached_max_runs());

        schedule.run_count = 5;
        assert!(schedule.has_reached_max_runs());

        schedule.run_count = 6;
        assert!(schedule.has_reached_max_runs());
    }

    #[test]
    fn test_schedule_validation() {
        let mut schedule = Schedule::new(
            "test".to_string(),
            "0 0 0 * * *".to_string(),
            "UTC".to_string(),
            serde_json::json!({}),
        );

        let result = schedule.validate();
        assert!(result.is_valid());

        // Test empty name
        schedule.name = String::new();
        let result = schedule.validate();
        assert!(!result.is_valid());

        // Test invalid date range
        schedule.name = "test".to_string();
        schedule.start_date = Some(Timestamptz::now());
        schedule.end_date = Some(Timestamptz::now() - jiff::SignedDuration::from_hours(1));
        let result = schedule.validate();
        assert!(!result.is_valid());
    }
}
