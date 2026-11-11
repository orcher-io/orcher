//! Coordination queue storage types.

use orcher_common::types::time::Timestamptz;
use orcher_common::validation::{Validatable, ValidationResult};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "postgres", derive(sqlx::Type))]
#[cfg_attr(
    feature = "postgres",
    sqlx(type_name = "VARCHAR", rename_all = "lowercase")
)]
#[serde(rename_all = "lowercase")]
pub enum TaskStatus {
    Pending,
    Processing,
    Completed,
    Failed,
    /// Timer queue only.
    Cancelled,
}

impl std::fmt::Display for TaskStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskStatus::Pending => write!(f, "pending"),
            TaskStatus::Processing => write!(f, "processing"),
            TaskStatus::Completed => write!(f, "completed"),
            TaskStatus::Failed => write!(f, "failed"),
            TaskStatus::Cancelled => write!(f, "cancelled"),
        }
    }
}

impl Default for TaskStatus {
    fn default() -> Self {
        TaskStatus::Pending
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "postgres", derive(sqlx::Type))]
#[cfg_attr(
    feature = "postgres",
    sqlx(type_name = "VARCHAR", rename_all = "snake_case")
)]
#[serde(rename_all = "snake_case")]
pub enum OutboxTaskType {
    DispatchStep,
    DispatchTask,
    SendEventWorkflow,
    UpdateVisibility,
    FireEventGate,
}

impl OutboxTaskType {
    /// Every outbox task type, as the `task_type` column's constraint lists
    /// them.
    pub const ALL: [OutboxTaskType; 5] = [
        OutboxTaskType::DispatchStep,
        OutboxTaskType::DispatchTask,
        OutboxTaskType::SendEventWorkflow,
        OutboxTaskType::UpdateVisibility,
        OutboxTaskType::FireEventGate,
    ];
}

impl std::fmt::Display for OutboxTaskType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OutboxTaskType::DispatchStep => write!(f, "dispatch_step"),
            OutboxTaskType::DispatchTask => write!(f, "dispatch_task"),
            OutboxTaskType::SendEventWorkflow => write!(f, "send_event_workflow"),
            OutboxTaskType::UpdateVisibility => write!(f, "update_visibility"),
            OutboxTaskType::FireEventGate => write!(f, "fire_event_gate"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "postgres", derive(sqlx::Type))]
#[cfg_attr(
    feature = "postgres",
    sqlx(type_name = "VARCHAR", rename_all = "snake_case")
)]
#[serde(rename_all = "snake_case")]
pub enum TimerTaskType {
    FireTimer,
    TimeoutStep,
    TimeoutTask,
    RetryStep,
    /// Task was not picked up in time.
    ScheduleToStartTimeout,
}

impl std::fmt::Display for TimerTaskType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TimerTaskType::FireTimer => write!(f, "fire_timer"),
            TimerTaskType::TimeoutStep => write!(f, "timeout_step"),
            TimerTaskType::TimeoutTask => write!(f, "timeout_task"),
            TimerTaskType::RetryStep => write!(f, "retry_step"),
            TimerTaskType::ScheduleToStartTimeout => write!(f, "schedule_to_start_timeout"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "postgres", derive(sqlx::FromRow))]
pub struct OutboxTask {
    pub task_id: Uuid,
    pub organization_id: Option<Uuid>,
    pub workflow_id: Uuid,
    pub task_type: OutboxTaskType,
    #[cfg_attr(feature = "postgres", sqlx(json))]
    pub payload: serde_json::Value,
    /// Prevents duplicate task creation.
    pub idempotency_key: Option<String>,
    pub status: TaskStatus,
    pub created_at: Timestamptz,
    pub processing_started_at: Option<Timestamptz>,
    pub completed_at: Option<Timestamptz>,
    pub attempt_count: i32,
    pub max_retries: i32,
    pub next_retry_at: Option<Timestamptz>,
    pub backoff_multiplier: f64,
    pub last_error: Option<String>,
    #[cfg_attr(feature = "postgres", sqlx(json))]
    pub error_details: Option<serde_json::Value>,
    /// NULL means the task is not currently leased.
    pub lease_expires_at: Option<Timestamptz>,
    pub lease_owner: Option<String>,
    #[serde(default)]
    pub shard_id: Option<i32>,
}

impl OutboxTask {
    pub fn new(
        workflow_id: Uuid,
        task_type: OutboxTaskType,
        payload: serde_json::Value,
        idempotency_key: Option<String>,
    ) -> Self {
        Self {
            task_id: Uuid::new_v4(),
            organization_id: None,
            workflow_id,
            task_type,
            payload,
            idempotency_key,
            status: TaskStatus::Pending,
            created_at: Timestamptz::now(),
            processing_started_at: None,
            completed_at: None,
            attempt_count: 0,
            max_retries: 3,
            next_retry_at: None,
            backoff_multiplier: 2.0,
            last_error: None,
            error_details: None,
            lease_expires_at: None,
            lease_owner: None,
            shard_id: None,
        }
    }

    pub fn new_with_org(
        organization_id: Uuid,
        workflow_id: Uuid,
        task_type: OutboxTaskType,
        payload: serde_json::Value,
        idempotency_key: Option<String>,
    ) -> Self {
        Self {
            task_id: Uuid::new_v4(),
            organization_id: Some(organization_id),
            workflow_id,
            task_type,
            payload,
            idempotency_key,
            status: TaskStatus::Pending,
            created_at: Timestamptz::now(),
            processing_started_at: None,
            completed_at: None,
            attempt_count: 0,
            max_retries: 3,
            next_retry_at: None,
            backoff_multiplier: 2.0,
            last_error: None,
            error_details: None,
            lease_expires_at: None,
            lease_owner: None,
            shard_id: None,
        }
    }

    pub fn is_pending(&self) -> bool {
        self.status == TaskStatus::Pending
    }

    pub fn is_processing(&self) -> bool {
        self.status == TaskStatus::Processing
    }

    pub fn is_completed(&self) -> bool {
        self.status == TaskStatus::Completed
    }

    pub fn is_failed(&self) -> bool {
        self.status == TaskStatus::Failed
    }

    pub fn can_retry(&self) -> bool {
        self.attempt_count < self.max_retries
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self.status, TaskStatus::Completed | TaskStatus::Failed)
    }
}

impl Validatable for OutboxTask {
    fn validate(&self) -> ValidationResult {
        let mut result = ValidationResult::new();

        if self.attempt_count < 0 {
            result.add_error("attempt_count", "Attempt count cannot be negative");
        }

        if self.max_retries < 0 {
            result.add_error("max_retries", "Max retries cannot be negative");
        }

        if self.attempt_count > self.max_retries + 1 {
            result.add_error("attempt_count", "Attempt count exceeds max retries + 1");
        }

        if self.backoff_multiplier < 1.0 || self.backoff_multiplier > 10.0 {
            result.add_error(
                "backoff_multiplier",
                "Backoff multiplier must be between 1.0 and 10.0",
            );
        }

        if self.processing_started_at.is_some() && self.status == TaskStatus::Pending {
            result.add_error(
                "processing_started_at",
                "Pending task cannot have processing_started_at",
            );
        }

        if self.completed_at.is_some() && !self.is_terminal() {
            result.add_error("completed_at", "Only terminal tasks can have completed_at");
        }

        result
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "postgres", derive(sqlx::FromRow))]
pub struct TimerTask {
    pub task_id: Uuid,
    pub organization_id: Option<Uuid>,
    pub workflow_id: Uuid,
    pub task_type: TimerTaskType,
    #[cfg_attr(feature = "postgres", sqlx(json))]
    pub payload: serde_json::Value,
    pub fire_at: Timestamptz,
    pub created_at: Timestamptz,
    pub status: TaskStatus,
    pub processing_started_at: Option<Timestamptz>,
    pub completed_at: Option<Timestamptz>,
    pub attempt_count: i32,
    pub max_retries: i32,
    pub last_error: Option<String>,
    #[serde(default)]
    pub shard_id: Option<i32>,
}

impl TimerTask {
    pub fn new(
        workflow_id: Uuid,
        task_type: TimerTaskType,
        payload: serde_json::Value,
        fire_at: Timestamptz,
    ) -> Self {
        Self {
            task_id: Uuid::new_v4(),
            organization_id: None,
            workflow_id,
            task_type,
            payload,
            fire_at,
            created_at: Timestamptz::now(),
            status: TaskStatus::Pending,
            processing_started_at: None,
            completed_at: None,
            attempt_count: 0,
            max_retries: 3,
            last_error: None,
            shard_id: None,
        }
    }

    pub fn new_with_org(
        organization_id: Uuid,
        workflow_id: Uuid,
        task_type: TimerTaskType,
        payload: serde_json::Value,
        fire_at: Timestamptz,
    ) -> Self {
        Self {
            task_id: Uuid::new_v4(),
            organization_id: Some(organization_id),
            workflow_id,
            task_type,
            payload,
            fire_at,
            created_at: Timestamptz::now(),
            status: TaskStatus::Pending,
            processing_started_at: None,
            completed_at: None,
            attempt_count: 0,
            max_retries: 3,
            last_error: None,
            shard_id: None,
        }
    }

    pub fn is_ready(&self) -> bool {
        self.fire_at <= Timestamptz::now() && self.status == TaskStatus::Pending
    }

    pub fn is_pending(&self) -> bool {
        self.status == TaskStatus::Pending
    }

    pub fn is_processing(&self) -> bool {
        self.status == TaskStatus::Processing
    }

    pub fn is_completed(&self) -> bool {
        self.status == TaskStatus::Completed
    }

    pub fn is_failed(&self) -> bool {
        self.status == TaskStatus::Failed
    }

    pub fn is_cancelled(&self) -> bool {
        self.status == TaskStatus::Cancelled
    }

    pub fn can_retry(&self) -> bool {
        self.attempt_count < self.max_retries
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self.status,
            TaskStatus::Completed | TaskStatus::Failed | TaskStatus::Cancelled
        )
    }

    pub fn seconds_until_fire(&self) -> i64 {
        (self.fire_at - Timestamptz::now()).as_secs()
    }
}

impl Validatable for TimerTask {
    fn validate(&self) -> ValidationResult {
        let mut result = ValidationResult::new();

        if self.attempt_count < 0 {
            result.add_error("attempt_count", "Attempt count cannot be negative");
        }

        if self.max_retries < 0 {
            result.add_error("max_retries", "Max retries cannot be negative");
        }

        if self.attempt_count > self.max_retries + 1 {
            result.add_error("attempt_count", "Attempt count exceeds max retries + 1");
        }

        // A fire time before the created time is not an error: a short timer
        // is recorded after it falls due when its transaction waited longer
        // than its duration, and it fires at once.

        if self.processing_started_at.is_some() && self.status == TaskStatus::Pending {
            result.add_error(
                "processing_started_at",
                "Pending task cannot have processing_started_at",
            );
        }

        if self.completed_at.is_some() && !self.is_terminal() {
            result.add_error("completed_at", "Only terminal tasks can have completed_at");
        }

        result
    }
}

/// Task that failed after all retry attempts.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "postgres", derive(sqlx::FromRow))]
pub struct DeadLetterTask {
    pub dlq_id: Uuid,
    pub organization_id: Option<Uuid>,
    pub original_task_id: Uuid,
    pub original_task_type: String,
    pub workflow_id: Uuid,
    #[cfg_attr(feature = "postgres", sqlx(json))]
    pub payload: serde_json::Value,
    pub attempt_count: i32,
    pub last_error: String,
    #[cfg_attr(feature = "postgres", sqlx(json))]
    pub error_details: serde_json::Value,
    pub failed_at: Timestamptz,
    /// For manual intervention.
    pub note: Option<String>,
}

impl DeadLetterTask {
    pub fn new(
        original_task_id: Uuid,
        original_task_type: String,
        workflow_id: Uuid,
        payload: serde_json::Value,
        attempt_count: i32,
        last_error: String,
        error_details: serde_json::Value,
    ) -> Self {
        Self {
            dlq_id: Uuid::new_v4(),
            organization_id: None,
            original_task_id,
            original_task_type,
            workflow_id,
            payload,
            attempt_count,
            last_error,
            error_details,
            failed_at: Timestamptz::now(),
            note: None,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_org(
        organization_id: Uuid,
        original_task_id: Uuid,
        original_task_type: String,
        workflow_id: Uuid,
        payload: serde_json::Value,
        attempt_count: i32,
        last_error: String,
        error_details: serde_json::Value,
    ) -> Self {
        Self {
            dlq_id: Uuid::new_v4(),
            organization_id: Some(organization_id),
            original_task_id,
            original_task_type,
            workflow_id,
            payload,
            attempt_count,
            last_error,
            error_details,
            failed_at: Timestamptz::now(),
            note: None,
        }
    }

    pub fn from_outbox_task(task: &OutboxTask, error_details: serde_json::Value) -> Self {
        let mut dlq = Self::new(
            task.task_id,
            task.task_type.to_string(),
            task.workflow_id,
            task.payload.clone(),
            task.attempt_count,
            task.last_error
                .clone()
                .unwrap_or_else(|| "Unknown error".to_string()),
            error_details,
        );
        dlq.organization_id = task.organization_id;
        dlq
    }

    pub fn from_timer_task(task: &TimerTask, error_details: serde_json::Value) -> Self {
        let mut dlq = Self::new(
            task.task_id,
            task.task_type.to_string(),
            task.workflow_id,
            task.payload.clone(),
            task.attempt_count,
            task.last_error
                .clone()
                .unwrap_or_else(|| "Unknown error".to_string()),
            error_details,
        );
        dlq.organization_id = task.organization_id;
        dlq
    }

    pub fn with_note(mut self, note: impl Into<String>) -> Self {
        self.note = Some(note.into());
        self
    }
}

impl Validatable for DeadLetterTask {
    fn validate(&self) -> ValidationResult {
        let mut result = ValidationResult::new();

        if self.original_task_type.is_empty() {
            result.add_error("original_task_type", "Original task type cannot be empty");
        }

        if self.last_error.is_empty() {
            result.add_error("last_error", "Last error cannot be empty");
        }

        if self.attempt_count < 0 {
            result.add_error("attempt_count", "Attempt count cannot be negative");
        }

        result
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct QueueMetrics {
    pub pending_count: i64,
    pub processing_count: i64,
    pub completed_count: i64,
    pub failed_count: i64,
    pub avg_processing_time_ms: Option<i64>,
    pub oldest_pending_age_seconds: Option<i64>,
}

impl Default for QueueMetrics {
    fn default() -> Self {
        Self {
            pending_count: 0,
            processing_count: 0,
            completed_count: 0,
            failed_count: 0,
            avg_processing_time_ms: None,
            oldest_pending_age_seconds: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jiff::SignedDuration;

    #[test]
    fn test_task_status_display() {
        assert_eq!(TaskStatus::Pending.to_string(), "pending");
        assert_eq!(TaskStatus::Processing.to_string(), "processing");
        assert_eq!(TaskStatus::Completed.to_string(), "completed");
        assert_eq!(TaskStatus::Failed.to_string(), "failed");
        assert_eq!(TaskStatus::Cancelled.to_string(), "cancelled");
    }

    #[test]
    fn test_outbox_task_creation() {
        let task = OutboxTask::new(
            Uuid::new_v4(),
            OutboxTaskType::DispatchStep,
            serde_json::json!({"test": "data"}),
            Some("idempotency-key".to_string()),
        );

        assert!(task.is_pending());
        assert!(!task.is_processing());
        assert!(!task.is_completed());
        assert!(!task.is_failed());
        assert!(task.can_retry());
        assert!(!task.is_terminal());
    }

    #[test]
    fn a_timer_already_due_when_recorded_is_valid_and_ready() {
        let due = Timestamptz::now() - SignedDuration::from_millis(5);
        let task = TimerTask::new(
            Uuid::new_v4(),
            TimerTaskType::FireTimer,
            serde_json::json!({"timer_name": "t"}),
            due,
        );
        assert!(task.validate().is_valid());
        assert!(task.is_ready());
    }

    #[test]
    fn test_timer_task_creation() {
        let fire_at = Timestamptz::now() + SignedDuration::from_secs(60);
        let task = TimerTask::new(
            Uuid::new_v4(),
            TimerTaskType::FireTimer,
            serde_json::json!({"timer_id": "test"}),
            fire_at,
        );

        assert!(task.is_pending());
        assert!(!task.is_ready()); // Not ready yet (60 seconds in future)
        assert!(task.can_retry());
        assert!(!task.is_terminal());
    }

    #[test]
    fn test_outbox_task_validation() {
        let mut task = OutboxTask::new(
            Uuid::new_v4(),
            OutboxTaskType::DispatchStep,
            serde_json::json!({}),
            None,
        );

        assert!(task.validate().is_valid());

        // Invalid backoff multiplier
        task.backoff_multiplier = 15.0;
        assert!(!task.validate().is_valid());
    }

    #[test]
    fn test_dead_letter_task_from_outbox() {
        let outbox_task = OutboxTask::new(
            Uuid::new_v4(),
            OutboxTaskType::DispatchTask,
            serde_json::json!({"task": "data"}),
            None,
        );

        let dlq_task =
            DeadLetterTask::from_outbox_task(&outbox_task, serde_json::json!({"error": "details"}));

        assert_eq!(dlq_task.original_task_id, outbox_task.task_id);
        assert_eq!(dlq_task.workflow_id, outbox_task.workflow_id);
        assert_eq!(dlq_task.payload, outbox_task.payload);
    }

    #[test]
    fn test_queue_metrics_default() {
        let metrics = QueueMetrics::default();
        assert_eq!(metrics.pending_count, 0);
        assert_eq!(metrics.processing_count, 0);
        assert_eq!(metrics.completed_count, 0);
        assert_eq!(metrics.failed_count, 0);
    }

    /// The outbox processor claims every type but the ones it excludes by
    /// reading `OutboxTaskType::ALL`; a type missing from it would never be
    /// claimed.
    #[test]
    fn all_outbox_task_types_are_listed() {
        for t in OutboxTaskType::ALL {
            // A new variant fails to compile here until it is listed.
            match t {
                OutboxTaskType::DispatchStep
                | OutboxTaskType::DispatchTask
                | OutboxTaskType::SendEventWorkflow
                | OutboxTaskType::UpdateVisibility
                | OutboxTaskType::FireEventGate => {}
            }
        }
        let mut names: Vec<String> = OutboxTaskType::ALL.iter().map(|t| t.to_string()).collect();
        names.sort();
        names.dedup();
        assert_eq!(names.len(), 5, "every type listed once");
    }
}
