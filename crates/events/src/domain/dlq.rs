//! Dead letter queue events: retry attempts, DLQ moves, replays, purges, alerts.

use orcher_common::types::identifiers::{TaskId, WorkerId};
use orcher_common::types::time::Timestamptz;
use serde::{Deserialize, Serialize};

/// How a task failed — used for DLQ categorization.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum FailureType {
    /// Execution error (function threw exception)
    ExecutionError,
    /// Timeout during execution
    Timeout,
    /// Worker crash or disconnect
    WorkerFailure,
    /// Deserialization or serialization error
    SerializationError,
    /// Network or connection error
    NetworkError,
    /// Resource exhaustion (memory, CPU, etc.)
    ResourceExhaustion,
    /// Unknown or unclassified error
    Unknown,
}

impl std::fmt::Display for FailureType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FailureType::ExecutionError => write!(f, "ExecutionError"),
            FailureType::Timeout => write!(f, "Timeout"),
            FailureType::WorkerFailure => write!(f, "WorkerFailure"),
            FailureType::SerializationError => write!(f, "SerializationError"),
            FailureType::NetworkError => write!(f, "NetworkError"),
            FailureType::ResourceExhaustion => write!(f, "ResourceExhaustion"),
            FailureType::Unknown => write!(f, "Unknown"),
        }
    }
}

impl FailureType {
    /// Best-effort classification from an error string.
    pub fn from_error_message(error: &str) -> Self {
        let error_lower = error.to_lowercase();

        if error_lower.contains("timeout") || error_lower.contains("deadline") {
            FailureType::Timeout
        } else if error_lower.contains("deserialize")
            || error_lower.contains("serialize")
            || error_lower.contains("parse")
        {
            FailureType::SerializationError
        } else if error_lower.contains("connection")
            || error_lower.contains("disconnect")
            || error_lower.contains("worker")
        {
            FailureType::WorkerFailure
        } else if error_lower.contains("network") || error_lower.contains("unreachable") {
            FailureType::NetworkError
        } else if error_lower.contains("memory")
            || error_lower.contains("oom")
            || error_lower.contains("resource")
        {
            FailureType::ResourceExhaustion
        } else if error_lower.contains("execution")
            || error_lower.contains("panic")
            || error_lower.contains("error")
        {
            FailureType::ExecutionError
        } else {
            FailureType::Unknown
        }
    }
}

/// DLQ operations and retry tracking.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DlqEvent {
    /// A failed task was scheduled for retry.
    TaskRetryAttempted {
        task_id: TaskId,
        execution_id: String,
        retry_count: u32,
        worker_id: Option<WorkerId>,
        error: String,
        next_retry_delay_ms: u64,
        timestamp: Timestamptz,
    },

    /// Task exhausted retries and landed in the DLQ.
    TaskMovedToDlq {
        task_id: TaskId,
        execution_id: String,
        partition_id: usize,
        dlq_entry_id: String,
        failure_type: FailureType,
        final_error: String,
        retry_count: u32,
        timestamp: Timestamptz,
    },

    /// A DLQ entry was replayed back to the task queue.
    TaskReplayedFromDlq {
        task_id: TaskId,
        execution_id: String,
        partition_id: usize,
        dlq_entry_id: String,
        target_queue: String,
        operator: Option<String>,
        replay_count: u32,
        timestamp: Timestamptz,
    },

    /// A DLQ entry was permanently removed.
    TaskPurgedFromDlq {
        task_id: TaskId,
        execution_id: String,
        partition_id: usize,
        dlq_entry_id: String,
        reason: String,
        operator: Option<String>,
        timestamp: Timestamptz,
    },

    /// A batch of DLQ entries was replayed in one operation.
    DlqBatchReplayCompleted {
        partition_id: usize,
        total_entries: usize,
        replayed_count: usize,
        failed_count: usize,
        failure_type_filter: Option<FailureType>,
        operator: Option<String>,
        timestamp: Timestamptz,
    },

    /// DLQ depth exceeded a configured threshold.
    DlqDepthThresholdExceeded {
        partition_id: usize,
        current_depth: u64,
        threshold: u64,
        severity: AlertSeverity,
        oldest_entry_age_secs: Option<u64>,
        timestamp: Timestamptz,
    },

    /// Retry metadata hit its TTL and was cleaned up.
    RetryMetadataExpired {
        execution_id: String,
        final_retry_count: u32,
        age_secs: u64,
        timestamp: Timestamptz,
    },
}

/// How urgently the DLQ alert needs attention.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AlertSeverity {
    /// Informational alert
    Info,
    /// Warning - attention recommended
    Warning,
    /// Critical - immediate action required
    Critical,
}

impl DlqEvent {
    /// Get the timestamp for any DLQ event
    pub fn timestamp(&self) -> Timestamptz {
        match self {
            DlqEvent::TaskRetryAttempted { timestamp, .. } => *timestamp,
            DlqEvent::TaskMovedToDlq { timestamp, .. } => *timestamp,
            DlqEvent::TaskReplayedFromDlq { timestamp, .. } => *timestamp,
            DlqEvent::TaskPurgedFromDlq { timestamp, .. } => *timestamp,
            DlqEvent::DlqBatchReplayCompleted { timestamp, .. } => *timestamp,
            DlqEvent::DlqDepthThresholdExceeded { timestamp, .. } => *timestamp,
            DlqEvent::RetryMetadataExpired { timestamp, .. } => *timestamp,
        }
    }

    /// Get the execution ID if present
    pub fn execution_id(&self) -> Option<&str> {
        match self {
            DlqEvent::TaskRetryAttempted { execution_id, .. } => Some(execution_id),
            DlqEvent::TaskMovedToDlq { execution_id, .. } => Some(execution_id),
            DlqEvent::TaskReplayedFromDlq { execution_id, .. } => Some(execution_id),
            DlqEvent::TaskPurgedFromDlq { execution_id, .. } => Some(execution_id),
            DlqEvent::RetryMetadataExpired { execution_id, .. } => Some(execution_id),
            _ => None,
        }
    }

    /// Get the task ID if present
    pub fn task_id(&self) -> Option<&TaskId> {
        match self {
            DlqEvent::TaskRetryAttempted { task_id, .. } => Some(task_id),
            DlqEvent::TaskMovedToDlq { task_id, .. } => Some(task_id),
            DlqEvent::TaskReplayedFromDlq { task_id, .. } => Some(task_id),
            DlqEvent::TaskPurgedFromDlq { task_id, .. } => Some(task_id),
            _ => None,
        }
    }

    /// Get the partition ID if present
    pub fn partition_id(&self) -> Option<usize> {
        match self {
            DlqEvent::TaskMovedToDlq { partition_id, .. } => Some(*partition_id),
            DlqEvent::TaskReplayedFromDlq { partition_id, .. } => Some(*partition_id),
            DlqEvent::TaskPurgedFromDlq { partition_id, .. } => Some(*partition_id),
            DlqEvent::DlqBatchReplayCompleted { partition_id, .. } => Some(*partition_id),
            DlqEvent::DlqDepthThresholdExceeded { partition_id, .. } => Some(*partition_id),
            _ => None,
        }
    }

    /// Check if this is an alert event
    pub fn is_alert(&self) -> bool {
        matches!(self, DlqEvent::DlqDepthThresholdExceeded { .. })
    }

    /// Check if this indicates a failure
    pub fn is_failure(&self) -> bool {
        matches!(
            self,
            DlqEvent::TaskMovedToDlq { .. } | DlqEvent::TaskRetryAttempted { .. }
        )
    }

    /// Check if this indicates a recovery action
    pub fn is_recovery(&self) -> bool {
        matches!(
            self,
            DlqEvent::TaskReplayedFromDlq { .. } | DlqEvent::DlqBatchReplayCompleted { .. }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orcher_common::types::identifiers::Identifier;

    #[test]
    fn test_dlq_event_serialization() {
        let event = DlqEvent::TaskMovedToDlq {
            task_id: TaskId::generate(),
            execution_id: "exec-123".to_string(),
            partition_id: 0,
            dlq_entry_id: "1234567890-0".to_string(),
            failure_type: FailureType::ExecutionError,
            final_error: "Max retries exceeded".to_string(),
            retry_count: 3,
            timestamp: Timestamptz::now(),
        };

        let json = serde_json::to_string(&event).unwrap();
        let deserialized: DlqEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(event, deserialized);
    }

    #[test]
    fn test_failure_type_display() {
        assert_eq!(FailureType::ExecutionError.to_string(), "ExecutionError");
        assert_eq!(FailureType::Timeout.to_string(), "Timeout");
        assert_eq!(FailureType::NetworkError.to_string(), "NetworkError");
    }

    #[test]
    fn test_failure_type_from_error_message() {
        assert_eq!(
            FailureType::from_error_message("Connection timeout"),
            FailureType::Timeout
        );
        assert_eq!(
            FailureType::from_error_message("Failed to deserialize"),
            FailureType::SerializationError
        );
        assert_eq!(
            FailureType::from_error_message("Worker disconnected"),
            FailureType::WorkerFailure
        );
        assert_eq!(
            FailureType::from_error_message("Network unreachable"),
            FailureType::NetworkError
        );
        assert_eq!(
            FailureType::from_error_message("Out of memory"),
            FailureType::ResourceExhaustion
        );
        assert_eq!(
            FailureType::from_error_message("Execution failed"),
            FailureType::ExecutionError
        );
        assert_eq!(
            FailureType::from_error_message("Something weird"),
            FailureType::Unknown
        );
    }

    #[test]
    fn test_event_timestamp_accessor() {
        let timestamp = Timestamptz::now();
        let event = DlqEvent::TaskRetryAttempted {
            task_id: TaskId::generate(),
            execution_id: "exec-123".to_string(),
            retry_count: 1,
            worker_id: None,
            error: "test error".to_string(),
            next_retry_delay_ms: 1000,
            timestamp,
        };

        assert_eq!(event.timestamp(), timestamp);
    }

    #[test]
    fn test_event_classifiers() {
        let alert_event = DlqEvent::DlqDepthThresholdExceeded {
            partition_id: 0,
            current_depth: 150,
            threshold: 100,
            severity: AlertSeverity::Warning,
            oldest_entry_age_secs: Some(3600),
            timestamp: Timestamptz::now(),
        };
        assert!(alert_event.is_alert());
        assert!(!alert_event.is_failure());
        assert!(!alert_event.is_recovery());

        let failure_event = DlqEvent::TaskMovedToDlq {
            task_id: TaskId::generate(),
            execution_id: "exec-123".to_string(),
            partition_id: 0,
            dlq_entry_id: "1234567890-0".to_string(),
            failure_type: FailureType::ExecutionError,
            final_error: "error".to_string(),
            retry_count: 3,
            timestamp: Timestamptz::now(),
        };
        assert!(!failure_event.is_alert());
        assert!(failure_event.is_failure());
        assert!(!failure_event.is_recovery());

        let recovery_event = DlqEvent::TaskReplayedFromDlq {
            task_id: TaskId::generate(),
            execution_id: "exec-123".to_string(),
            partition_id: 0,
            dlq_entry_id: "1234567890-0".to_string(),
            target_queue: "orcher:tasks:0".to_string(),
            operator: Some("admin".to_string()),
            replay_count: 1,
            timestamp: Timestamptz::now(),
        };
        assert!(!recovery_event.is_alert());
        assert!(!recovery_event.is_failure());
        assert!(recovery_event.is_recovery());
    }

    #[test]
    fn test_execution_id_accessor() {
        let event = DlqEvent::TaskRetryAttempted {
            task_id: TaskId::generate(),
            execution_id: "exec-456".to_string(),
            retry_count: 1,
            worker_id: None,
            error: "test".to_string(),
            next_retry_delay_ms: 1000,
            timestamp: Timestamptz::now(),
        };

        assert_eq!(event.execution_id(), Some("exec-456"));
    }

    #[test]
    fn test_partition_id_accessor() {
        let event = DlqEvent::DlqBatchReplayCompleted {
            partition_id: 2,
            total_entries: 50,
            replayed_count: 45,
            failed_count: 5,
            failure_type_filter: Some(FailureType::Timeout),
            operator: Some("system".to_string()),
            timestamp: Timestamptz::now(),
        };

        assert_eq!(event.partition_id(), Some(2));
    }
}
