//! Task execution status types.

use crate::types::identifiers::TaskExecutionId;
use crate::types::time::Timestamptz;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Lifecycle status of a task.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum TaskStatus {
    #[default]
    Pending,
    Queued,
    Running {
        started_at: Timestamptz,
        execution_id: TaskExecutionId,
    },
    Completed {
        completed_at: Timestamptz,
        duration_ms: u64,
        result: Option<String>,
    },
    Failed {
        failed_at: Timestamptz,
        error: String,
        error_code: Option<String>,
        retryable: bool,
    },
    Cancelled {
        cancelled_at: Timestamptz,
        reason: String,
    },
    Suspended {
        suspended_at: Timestamptz,
        reason: String,
    },
    TimedOut {
        timed_out_at: Timestamptz,
        timeout_duration_ms: u64,
    },
}

impl TaskStatus {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            TaskStatus::Completed { .. }
                | TaskStatus::Failed { .. }
                | TaskStatus::Cancelled { .. }
                | TaskStatus::TimedOut { .. }
        )
    }

    pub fn is_active(&self) -> bool {
        matches!(self, TaskStatus::Running { .. })
    }

    pub fn is_retryable(&self) -> bool {
        match self {
            TaskStatus::Failed { retryable, .. } => *retryable,
            TaskStatus::TimedOut { .. } => true,
            _ => false,
        }
    }

    pub fn timestamp(&self) -> Option<Timestamptz> {
        match self {
            TaskStatus::Running { started_at, .. } => Some(*started_at),
            TaskStatus::Completed { completed_at, .. } => Some(*completed_at),
            TaskStatus::Failed { failed_at, .. } => Some(*failed_at),
            TaskStatus::Cancelled { cancelled_at, .. } => Some(*cancelled_at),
            TaskStatus::Suspended { suspended_at, .. } => Some(*suspended_at),
            TaskStatus::TimedOut { timed_out_at, .. } => Some(*timed_out_at),
            _ => None,
        }
    }

    pub fn description(&self) -> String {
        match self {
            TaskStatus::Pending => "Task is waiting to be queued".to_string(),
            TaskStatus::Queued => "Task is queued for execution".to_string(),
            TaskStatus::Running { started_at, .. } => {
                format!(
                    "Task started running at {}",
                    started_at.format("%Y-%m-%d %H:%M:%S UTC")
                )
            }
            TaskStatus::Completed {
                completed_at,
                duration_ms,
                ..
            } => {
                format!(
                    "Task completed successfully at {} (duration: {}ms)",
                    completed_at.format("%Y-%m-%d %H:%M:%S UTC"),
                    duration_ms
                )
            }
            TaskStatus::Failed {
                failed_at, error, ..
            } => {
                format!(
                    "Task failed at {} with error: {}",
                    failed_at.format("%Y-%m-%d %H:%M:%S UTC"),
                    error
                )
            }
            TaskStatus::Cancelled {
                cancelled_at,
                reason,
            } => {
                format!(
                    "Task cancelled at {} - {}",
                    cancelled_at.format("%Y-%m-%d %H:%M:%S UTC"),
                    reason
                )
            }
            TaskStatus::Suspended {
                suspended_at,
                reason,
            } => {
                format!(
                    "Task suspended at {} - {}",
                    suspended_at.format("%Y-%m-%d %H:%M:%S UTC"),
                    reason
                )
            }
            TaskStatus::TimedOut {
                timed_out_at,
                timeout_duration_ms,
            } => {
                format!(
                    "Task timed out at {} after {}ms",
                    timed_out_at.format("%Y-%m-%d %H:%M:%S UTC"),
                    timeout_duration_ms
                )
            }
        }
    }
}

impl fmt::Display for TaskStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let status_name = match self {
            TaskStatus::Pending => "PENDING",
            TaskStatus::Queued => "QUEUED",
            TaskStatus::Running { .. } => "RUNNING",
            TaskStatus::Completed { .. } => "COMPLETED",
            TaskStatus::Failed { .. } => "FAILED",
            TaskStatus::Cancelled { .. } => "CANCELLED",
            TaskStatus::Suspended { .. } => "SUSPENDED",
            TaskStatus::TimedOut { .. } => "TIMED_OUT",
        };
        write!(f, "{}", status_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::identifiers::traits::Identifier;

    #[test]
    fn test_task_status_creation() {
        let status = TaskStatus::default();
        assert_eq!(status, TaskStatus::Pending);
        assert!(!status.is_terminal());
        assert!(!status.is_active());
    }

    #[test]
    fn test_task_status_terminal_states() {
        let completed = TaskStatus::Completed {
            completed_at: Timestamptz::now(),
            duration_ms: 1000,
            result: None,
        };
        assert!(completed.is_terminal());

        let failed = TaskStatus::Failed {
            failed_at: Timestamptz::now(),
            error: "Test error".to_string(),
            error_code: None,
            retryable: false,
        };
        assert!(failed.is_terminal());

        let running = TaskStatus::Running {
            started_at: Timestamptz::now(),
            execution_id: TaskExecutionId::generate(),
        };
        assert!(!running.is_terminal());
        assert!(running.is_active());
    }

    #[test]
    fn test_task_status_display() {
        let pending = TaskStatus::Pending;
        assert_eq!(pending.to_string(), "PENDING");

        let running = TaskStatus::Running {
            started_at: Timestamptz::now(),
            execution_id: TaskExecutionId::generate(),
        };
        assert_eq!(running.to_string(), "RUNNING");
    }

    #[test]
    fn test_task_status_retryable() {
        let retryable_failed = TaskStatus::Failed {
            failed_at: Timestamptz::now(),
            error: "Retryable error".to_string(),
            error_code: None,
            retryable: true,
        };
        assert!(retryable_failed.is_retryable());

        let non_retryable_failed = TaskStatus::Failed {
            failed_at: Timestamptz::now(),
            error: "Fatal error".to_string(),
            error_code: None,
            retryable: false,
        };
        assert!(!non_retryable_failed.is_retryable());

        let timed_out = TaskStatus::TimedOut {
            timed_out_at: Timestamptz::now(),
            timeout_duration_ms: 5000,
        };
        assert!(timed_out.is_retryable());
    }
}
