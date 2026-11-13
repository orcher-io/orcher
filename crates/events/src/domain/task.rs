//! Task lifecycle events: submitted -> assigned -> started -> completed/failed/cancelled.

use orcher_common::types::identifiers::{TaskId, WorkerId};
use orcher_common::types::time::Timestamptz;
use serde::{Deserialize, Serialize};

/// Every state transition in a task's life.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TaskEvent {
    /// Task submitted to the orchestrator.
    Submitted {
        task_id: TaskId,
        submitted_at: Timestamptz,
        /// higher = more urgent
        priority: i32,
    },

    /// Task assigned to a worker for execution.
    Assigned {
        task_id: TaskId,
        worker_id: WorkerId,
        assigned_at: Timestamptz,
    },

    /// Worker began executing the task.
    Started {
        task_id: TaskId,
        worker_id: WorkerId,
        started_at: Timestamptz,
    },

    /// Task finished execution.
    Completed {
        task_id: TaskId,
        result: TaskResult,
        completed_at: Timestamptz,
    },

    /// Task failed during execution.
    Failed {
        task_id: TaskId,
        error: String,
        retry_count: u32,
        failed_at: Timestamptz,
    },

    /// Task was cancelled and will not be retried.
    Cancelled {
        task_id: TaskId,
        reason: String,
        cancelled_at: Timestamptz,
    },

    /// Failed task scheduled for retry.
    Retrying {
        task_id: TaskId,
        retry_count: u32,
        next_retry_at: Timestamptz,
    },

    /// Task exceeded its execution timeout.
    TimedOut {
        task_id: TaskId,
        timeout_seconds: u64,
        timed_out_at: Timestamptz,
    },

    /// Task dispatched to an external message queue.
    Dispatched {
        task_id: TaskId,
        queue_name: String,
        message_id: String,
        routing_key: String,
        dispatched_at: Timestamptz,
    },
}

impl TaskEvent {
    /// The task this event is about.
    pub fn task_id(&self) -> &TaskId {
        match self {
            Self::Submitted { task_id, .. } => task_id,
            Self::Assigned { task_id, .. } => task_id,
            Self::Started { task_id, .. } => task_id,
            Self::Completed { task_id, .. } => task_id,
            Self::Failed { task_id, .. } => task_id,
            Self::Cancelled { task_id, .. } => task_id,
            Self::Retrying { task_id, .. } => task_id,
            Self::TimedOut { task_id, .. } => task_id,
            Self::Dispatched { task_id, .. } => task_id,
        }
    }

    /// When this event happened.
    pub fn timestamp(&self) -> &Timestamptz {
        match self {
            Self::Submitted { submitted_at, .. } => submitted_at,
            Self::Assigned { assigned_at, .. } => assigned_at,
            Self::Started { started_at, .. } => started_at,
            Self::Completed { completed_at, .. } => completed_at,
            Self::Failed { failed_at, .. } => failed_at,
            Self::Cancelled { cancelled_at, .. } => cancelled_at,
            Self::Retrying { next_retry_at, .. } => next_retry_at,
            Self::TimedOut { timed_out_at, .. } => timed_out_at,
            Self::Dispatched { dispatched_at, .. } => dispatched_at,
        }
    }

    /// Dotted type name, e.g. "task.submitted".
    pub fn event_type(&self) -> &str {
        match self {
            Self::Submitted { .. } => "task.submitted",
            Self::Assigned { .. } => "task.assigned",
            Self::Started { .. } => "task.started",
            Self::Completed { .. } => "task.completed",
            Self::Failed { .. } => "task.failed",
            Self::Cancelled { .. } => "task.cancelled",
            Self::Retrying { .. } => "task.retrying",
            Self::TimedOut { .. } => "task.timed_out",
            Self::Dispatched { .. } => "task.dispatched",
        }
    }

    /// True if the task won't transition further (completed, cancelled, timed out).
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Completed { .. } | Self::Cancelled { .. } | Self::TimedOut { .. }
        )
    }

    /// True for Failed or TimedOut.
    pub fn is_failure(&self) -> bool {
        matches!(self, Self::Failed { .. } | Self::TimedOut { .. })
    }

    /// True if Completed with success=true.
    pub fn is_success(&self) -> bool {
        matches!(
            self,
            Self::Completed {
                result: TaskResult { success: true, .. },
                ..
            }
        )
    }
}

/// Output payload from a completed task.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TaskResult {
    pub success: bool,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<serde_json::Value>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
}

impl TaskResult {
    /// Build a success result.
    pub fn success(output: Option<serde_json::Value>, duration_ms: Option<u64>) -> Self {
        Self {
            success: true,
            output,
            error: None,
            duration_ms,
        }
    }

    /// Build a failure result.
    pub fn failure(error: String, duration_ms: Option<u64>) -> Self {
        Self {
            success: false,
            output: None,
            error: Some(error),
            duration_ms,
        }
    }

    pub fn has_output(&self) -> bool {
        self.output.is_some()
    }

    pub fn has_error(&self) -> bool {
        self.error.is_some()
    }

    /// Duration converted to seconds.
    pub fn duration_seconds(&self) -> Option<f64> {
        self.duration_ms.map(|ms| ms as f64 / 1000.0)
    }
}

impl Default for TaskResult {
    fn default() -> Self {
        Self::success(None, None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orcher_common::types::identifiers::Identifier;
    use serde_json::json;

    #[test]
    fn test_task_event_submitted() {
        let task_id = TaskId::generate();
        let submitted_at = Timestamptz::now();
        let event = TaskEvent::Submitted {
            task_id,
            submitted_at,
            priority: 5,
        };

        assert_eq!(event.task_id(), &task_id);
        assert_eq!(event.timestamp(), &submitted_at);
        assert_eq!(event.event_type(), "task.submitted");
        assert!(!event.is_terminal());
        assert!(!event.is_failure());
        assert!(!event.is_success());
    }

    #[test]
    fn test_task_event_assigned() {
        let task_id = TaskId::generate();
        let worker_id = WorkerId::generate();
        let assigned_at = Timestamptz::now();

        let event = TaskEvent::Assigned {
            task_id,
            worker_id,
            assigned_at,
        };

        assert_eq!(event.task_id(), &task_id);
        assert_eq!(event.event_type(), "task.assigned");
    }

    #[test]
    fn test_task_event_completed() {
        let task_id = TaskId::generate();
        let result = TaskResult::success(Some(json!({"result": "data"})), Some(150));
        let completed_at = Timestamptz::now();

        let event = TaskEvent::Completed {
            task_id,
            result,
            completed_at,
        };

        assert_eq!(event.task_id(), &task_id);
        assert_eq!(event.event_type(), "task.completed");
        assert!(event.is_terminal());
        assert!(event.is_success());
        assert!(!event.is_failure());
    }

    #[test]
    fn test_task_event_failed() {
        let task_id = TaskId::generate();
        let error = "Connection timeout".to_string();
        let failed_at = Timestamptz::now();

        let event = TaskEvent::Failed {
            task_id,
            error,
            retry_count: 1,
            failed_at,
        };

        assert_eq!(event.task_id(), &task_id);
        assert_eq!(event.event_type(), "task.failed");
        assert!(!event.is_terminal());
        assert!(event.is_failure());
    }

    #[test]
    fn test_task_event_cancelled() {
        let task_id = TaskId::generate();
        let event = TaskEvent::Cancelled {
            task_id,
            reason: "User requested".to_string(),
            cancelled_at: Timestamptz::now(),
        };

        assert!(event.is_terminal());
        assert!(!event.is_failure());
    }

    #[test]
    fn test_task_event_timed_out() {
        let event = TaskEvent::TimedOut {
            task_id: TaskId::generate(),
            timeout_seconds: 300,
            timed_out_at: Timestamptz::now(),
        };

        assert!(event.is_terminal());
        assert!(event.is_failure());
    }

    #[test]
    fn test_task_event_dispatched() {
        let event = TaskEvent::Dispatched {
            task_id: TaskId::generate(),
            queue_name: "task-queue".to_string(),
            message_id: "msg-123".to_string(),
            routing_key: "tasks.execute".to_string(),
            dispatched_at: Timestamptz::now(),
        };

        assert_eq!(event.event_type(), "task.dispatched");
        assert!(!event.is_terminal());
    }

    #[test]
    fn test_task_result_success() {
        let output = Some(json!({"key": "value"}));
        let result = TaskResult::success(output.clone(), Some(100));

        assert!(result.success);
        assert_eq!(result.output, output);
        assert!(result.error.is_none());
        assert_eq!(result.duration_ms, Some(100));
        assert!(result.has_output());
        assert!(!result.has_error());
        assert_eq!(result.duration_seconds(), Some(0.1));
    }

    #[test]
    fn test_task_result_failure() {
        let error = "Task failed".to_string();
        let result = TaskResult::failure(error.clone(), Some(200));

        assert!(!result.success);
        assert!(result.output.is_none());
        assert_eq!(result.error, Some(error));
        assert_eq!(result.duration_ms, Some(200));
        assert!(!result.has_output());
        assert!(result.has_error());
        assert_eq!(result.duration_seconds(), Some(0.2));
    }

    #[test]
    fn test_task_result_default() {
        let result = TaskResult::default();
        assert!(result.success);
        assert!(result.output.is_none());
        assert!(result.error.is_none());
        assert!(result.duration_ms.is_none());
    }

    #[test]
    fn test_task_event_serialization() {
        let event = TaskEvent::Submitted {
            task_id: TaskId::generate(),
            submitted_at: Timestamptz::now(),
            priority: 5,
        };

        let json = serde_json::to_string(&event).unwrap();
        let deserialized: TaskEvent = serde_json::from_str(&json).unwrap();

        assert_eq!(event, deserialized);
    }

    #[test]
    fn test_task_result_serialization() {
        let result = TaskResult::success(Some(json!({"test": "data"})), Some(100));

        let json = serde_json::to_string(&result).unwrap();
        let deserialized: TaskResult = serde_json::from_str(&json).unwrap();

        assert_eq!(result, deserialized);
    }

    #[test]
    fn test_task_event_clone() {
        let event = TaskEvent::Submitted {
            task_id: TaskId::generate(),
            submitted_at: Timestamptz::now(),
            priority: 5,
        };

        let cloned = event.clone();
        assert_eq!(event, cloned);
    }

    #[test]
    fn test_task_result_clone() {
        let result = TaskResult::success(Some(json!({"key": "value"})), Some(100));
        let cloned = result.clone();
        assert_eq!(result, cloned);
    }

    #[test]
    fn test_all_event_types() {
        let task_id = TaskId::generate();
        let worker_id = WorkerId::generate();
        let now = Timestamptz::now();

        let events = vec![
            TaskEvent::Submitted {
                task_id,
                submitted_at: now,
                priority: 5,
            },
            TaskEvent::Assigned {
                task_id,
                worker_id,
                assigned_at: now,
            },
            TaskEvent::Started {
                task_id,
                worker_id,
                started_at: now,
            },
            TaskEvent::Completed {
                task_id,
                result: TaskResult::success(None, None),
                completed_at: now,
            },
            TaskEvent::Failed {
                task_id,
                error: "error".to_string(),
                retry_count: 1,
                failed_at: now,
            },
            TaskEvent::Cancelled {
                task_id,
                reason: "reason".to_string(),
                cancelled_at: now,
            },
            TaskEvent::Retrying {
                task_id,
                retry_count: 1,
                next_retry_at: now,
            },
            TaskEvent::TimedOut {
                task_id,
                timeout_seconds: 300,
                timed_out_at: now,
            },
            TaskEvent::Dispatched {
                task_id,
                queue_name: "queue".to_string(),
                message_id: "msg".to_string(),
                routing_key: "key".to_string(),
                dispatched_at: now,
            },
        ];

        // All events should have the same task_id
        for event in &events {
            assert_eq!(event.task_id(), &task_id);
        }

        // All events should have a valid event_type
        for event in &events {
            assert!(!event.event_type().is_empty());
        }
    }
}
