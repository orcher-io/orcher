//! Worker-side task execution events — the wire format between workers and orchestrator.

use orcher_common::types::identifiers::{TaskId, WorkerId};
use orcher_common::types::time::Timestamptz;
use serde::{Deserialize, Serialize};

/// Task execution event from the worker's point of view.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkerTaskEvent {
    pub event_type: WorkerTaskEventType,
    pub task_id: TaskId,
    pub worker_id: WorkerId,
    pub timestamp: Timestamptz,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
}

/// What happened during execution.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WorkerTaskEventType {
    /// Worker began executing the task.
    ExecutionStarted,

    /// Task finished successfully with output.
    ExecutionCompleted {
        output: serde_json::Value,
        duration_ms: u64,
    },

    /// Task failed with an error.
    ExecutionFailed { error: String, duration_ms: u64 },

    /// Task exceeded its configured timeout.
    ExecutionTimedOut { timeout_seconds: u64 },
}

impl WorkerTaskEvent {
    /// Create a new execution started event.
    pub fn execution_started(task_id: TaskId, worker_id: WorkerId) -> Self {
        Self {
            event_type: WorkerTaskEventType::ExecutionStarted,
            task_id,
            worker_id,
            timestamp: Timestamptz::now(),
            metadata: None,
        }
    }

    /// Create a new execution completed event.
    pub fn execution_completed(
        task_id: TaskId,
        worker_id: WorkerId,
        output: serde_json::Value,
        duration_ms: u64,
    ) -> Self {
        Self {
            event_type: WorkerTaskEventType::ExecutionCompleted {
                output,
                duration_ms,
            },
            task_id,
            worker_id,
            timestamp: Timestamptz::now(),
            metadata: None,
        }
    }

    /// Create a new execution failed event.
    pub fn execution_failed(
        task_id: TaskId,
        worker_id: WorkerId,
        error: String,
        duration_ms: u64,
    ) -> Self {
        Self {
            event_type: WorkerTaskEventType::ExecutionFailed { error, duration_ms },
            task_id,
            worker_id,
            timestamp: Timestamptz::now(),
            metadata: None,
        }
    }

    /// Create a new execution timed out event.
    pub fn execution_timed_out(task_id: TaskId, worker_id: WorkerId, timeout_seconds: u64) -> Self {
        Self {
            event_type: WorkerTaskEventType::ExecutionTimedOut { timeout_seconds },
            task_id,
            worker_id,
            timestamp: Timestamptz::now(),
            metadata: None,
        }
    }

    /// Attach extra context (trace IDs, etc.).
    pub fn with_metadata(mut self, metadata: serde_json::Value) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Dotted type name, e.g. "worker_task.execution_started".
    pub fn event_type_name(&self) -> &str {
        match &self.event_type {
            WorkerTaskEventType::ExecutionStarted => "worker_task.execution_started",
            WorkerTaskEventType::ExecutionCompleted { .. } => "worker_task.execution_completed",
            WorkerTaskEventType::ExecutionFailed { .. } => "worker_task.execution_failed",
            WorkerTaskEventType::ExecutionTimedOut { .. } => "worker_task.execution_timed_out",
        }
    }

    /// Returns `true` if the task completed successfully.
    pub fn is_success(&self) -> bool {
        matches!(
            self.event_type,
            WorkerTaskEventType::ExecutionCompleted { .. }
        )
    }

    /// Returns `true` if the task failed or timed out.
    pub fn is_failure(&self) -> bool {
        matches!(
            self.event_type,
            WorkerTaskEventType::ExecutionFailed { .. }
                | WorkerTaskEventType::ExecutionTimedOut { .. }
        )
    }

    /// Returns `true` if the task execution has ended (any outcome).
    pub fn is_terminal(&self) -> bool {
        !matches!(self.event_type, WorkerTaskEventType::ExecutionStarted)
    }

    /// Execution wall-clock time, if the event carries it.
    pub fn duration_ms(&self) -> Option<u64> {
        match &self.event_type {
            WorkerTaskEventType::ExecutionCompleted { duration_ms, .. } => Some(*duration_ms),
            WorkerTaskEventType::ExecutionFailed { duration_ms, .. } => Some(*duration_ms),
            _ => None,
        }
    }

    /// Task output from a completed event, if any.
    pub fn output(&self) -> Option<&serde_json::Value> {
        match &self.event_type {
            WorkerTaskEventType::ExecutionCompleted { output, .. } => Some(output),
            _ => None,
        }
    }

    /// Error message from a failed event, if any.
    pub fn error(&self) -> Option<&String> {
        match &self.event_type {
            WorkerTaskEventType::ExecutionFailed { error, .. } => Some(error),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orcher_common::types::identifiers::Identifier;
    use serde_json::json;

    #[test]
    fn test_execution_started_event() {
        let task_id = TaskId::generate();
        let worker_id = WorkerId::generate();

        let event = WorkerTaskEvent::execution_started(task_id, worker_id);

        assert_eq!(event.task_id, task_id);
        assert_eq!(event.worker_id, worker_id);
        assert!(matches!(
            event.event_type,
            WorkerTaskEventType::ExecutionStarted
        ));
        assert!(event.metadata.is_none());
        assert_eq!(event.event_type_name(), "worker_task.execution_started");
        assert!(!event.is_success());
        assert!(!event.is_failure());
        assert!(!event.is_terminal());
        assert_eq!(event.duration_ms(), None);
        assert_eq!(event.output(), None);
        assert_eq!(event.error(), None);
    }

    #[test]
    fn test_execution_completed_event() {
        let task_id = TaskId::generate();
        let worker_id = WorkerId::generate();
        let output = json!({"result": "success"});
        let duration_ms = 1500;

        let event =
            WorkerTaskEvent::execution_completed(task_id, worker_id, output.clone(), duration_ms);

        assert_eq!(event.task_id, task_id);
        assert_eq!(event.worker_id, worker_id);
        assert_eq!(event.event_type_name(), "worker_task.execution_completed");
        assert!(event.is_success());
        assert!(!event.is_failure());
        assert!(event.is_terminal());
        assert_eq!(event.duration_ms(), Some(duration_ms));
        assert_eq!(event.output(), Some(&output));
        assert_eq!(event.error(), None);

        match event.event_type {
            WorkerTaskEventType::ExecutionCompleted {
                output: out,
                duration_ms: dur,
            } => {
                assert_eq!(out, output);
                assert_eq!(dur, duration_ms);
            }
            _ => panic!("Expected ExecutionCompleted event"),
        }
    }

    #[test]
    fn test_execution_failed_event() {
        let task_id = TaskId::generate();
        let worker_id = WorkerId::generate();
        let error_msg = "Task execution failed".to_string();
        let duration_ms = 500;

        let event =
            WorkerTaskEvent::execution_failed(task_id, worker_id, error_msg.clone(), duration_ms);

        assert_eq!(event.task_id, task_id);
        assert_eq!(event.worker_id, worker_id);
        assert_eq!(event.event_type_name(), "worker_task.execution_failed");
        assert!(!event.is_success());
        assert!(event.is_failure());
        assert!(event.is_terminal());
        assert_eq!(event.duration_ms(), Some(duration_ms));
        assert_eq!(event.output(), None);
        assert_eq!(event.error(), Some(&error_msg));

        match event.event_type {
            WorkerTaskEventType::ExecutionFailed {
                error,
                duration_ms: dur,
            } => {
                assert_eq!(error, error_msg);
                assert_eq!(dur, duration_ms);
            }
            _ => panic!("Expected ExecutionFailed event"),
        }
    }

    #[test]
    fn test_execution_timed_out_event() {
        let task_id = TaskId::generate();
        let worker_id = WorkerId::generate();
        let timeout_seconds = 300;

        let event = WorkerTaskEvent::execution_timed_out(task_id, worker_id, timeout_seconds);

        assert_eq!(event.task_id, task_id);
        assert_eq!(event.worker_id, worker_id);
        assert_eq!(event.event_type_name(), "worker_task.execution_timed_out");
        assert!(!event.is_success());
        assert!(event.is_failure());
        assert!(event.is_terminal());
        assert_eq!(event.duration_ms(), None);

        match event.event_type {
            WorkerTaskEventType::ExecutionTimedOut {
                timeout_seconds: timeout,
            } => {
                assert_eq!(timeout, timeout_seconds);
            }
            _ => panic!("Expected ExecutionTimedOut event"),
        }
    }

    #[test]
    fn test_event_with_metadata() {
        let task_id = TaskId::generate();
        let worker_id = WorkerId::generate();
        let metadata = json!({"trace_id": "abc123", "env": "production"});

        let event =
            WorkerTaskEvent::execution_started(task_id, worker_id).with_metadata(metadata.clone());

        assert_eq!(event.metadata, Some(metadata));
    }

    #[test]
    fn test_event_serialization() {
        let task_id = TaskId::generate();
        let worker_id = WorkerId::generate();
        let event = WorkerTaskEvent::execution_started(task_id, worker_id);

        let json_str = serde_json::to_string(&event).unwrap();
        let deserialized: WorkerTaskEvent = serde_json::from_str(&json_str).unwrap();

        assert_eq!(event, deserialized);
    }

    #[test]
    fn test_event_serialization_completed() {
        let task_id = TaskId::generate();
        let worker_id = WorkerId::generate();
        let output = json!({"result": "success", "count": 42});
        let event = WorkerTaskEvent::execution_completed(task_id, worker_id, output, 1500);

        let json_str = serde_json::to_string(&event).unwrap();
        let deserialized: WorkerTaskEvent = serde_json::from_str(&json_str).unwrap();

        assert_eq!(event, deserialized);
    }

    #[test]
    fn test_event_clone() {
        let task_id = TaskId::generate();
        let worker_id = WorkerId::generate();
        let event = WorkerTaskEvent::execution_started(task_id, worker_id);

        let cloned = event.clone();
        assert_eq!(event, cloned);
    }

    #[test]
    fn test_all_event_types_serialize() {
        let task_id = TaskId::generate();
        let worker_id = WorkerId::generate();

        let events = vec![
            WorkerTaskEvent::execution_started(task_id, worker_id),
            WorkerTaskEvent::execution_completed(task_id, worker_id, json!({"test": true}), 100),
            WorkerTaskEvent::execution_failed(task_id, worker_id, "error".to_string(), 50),
            WorkerTaskEvent::execution_timed_out(task_id, worker_id, 300),
        ];

        for event in events {
            let json_str = serde_json::to_string(&event).unwrap();
            let deserialized: WorkerTaskEvent = serde_json::from_str(&json_str).unwrap();
            assert_eq!(event, deserialized);
        }
    }

    #[test]
    fn test_all_event_types_methods() {
        let task_id = TaskId::generate();
        let worker_id = WorkerId::generate();

        let started = WorkerTaskEvent::execution_started(task_id, worker_id);
        assert!(!started.is_terminal());
        assert!(!started.is_success());
        assert!(!started.is_failure());

        let completed =
            WorkerTaskEvent::execution_completed(task_id, worker_id, json!({"result": "ok"}), 100);
        assert!(completed.is_terminal());
        assert!(completed.is_success());
        assert!(!completed.is_failure());
        assert_eq!(completed.duration_ms(), Some(100));

        let failed = WorkerTaskEvent::execution_failed(task_id, worker_id, "error".to_string(), 50);
        assert!(failed.is_terminal());
        assert!(!failed.is_success());
        assert!(failed.is_failure());
        assert_eq!(failed.duration_ms(), Some(50));
        assert_eq!(failed.error(), Some(&"error".to_string()));

        let timed_out = WorkerTaskEvent::execution_timed_out(task_id, worker_id, 300);
        assert!(timed_out.is_terminal());
        assert!(!timed_out.is_success());
        assert!(timed_out.is_failure());
    }
}
