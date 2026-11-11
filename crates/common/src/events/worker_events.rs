//! Worker event types for task execution lifecycle.

use crate::types::time::Timestamptz;
use serde::{Deserialize, Serialize};

#[cfg(feature = "uuid")]
use crate::types::identifiers::{TaskId, WorkerId};

/// Wire format for worker-to-orchestrator task lifecycle events.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "test-utils", derive(Eq))]
pub struct WorkerTaskEvent {
    pub event_type: WorkerTaskEventType,
    #[cfg(feature = "uuid")]
    pub task_id: TaskId,
    #[cfg(not(feature = "uuid"))]
    pub task_id: String,
    #[cfg(feature = "uuid")]
    pub worker_id: WorkerId,
    #[cfg(not(feature = "uuid"))]
    pub worker_id: String,
    pub timestamp: Timestamptz,
    pub metadata: Option<serde_json::Value>,
}

/// Types of events workers can emit during task execution.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "test-utils", derive(Eq))]
pub enum WorkerTaskEventType {
    ExecutionStarted,
    ExecutionCompleted {
        output: serde_json::Value,
        duration_ms: u64,
    },
    ExecutionFailed {
        error: String,
        duration_ms: u64,
    },
    ExecutionTimedOut {
        timeout_seconds: u64,
    },
}

#[cfg(feature = "uuid")]
impl WorkerTaskEvent {
    pub fn execution_started(task_id: TaskId, worker_id: WorkerId) -> Self {
        Self {
            event_type: WorkerTaskEventType::ExecutionStarted,
            task_id,
            worker_id,
            timestamp: Timestamptz::now(),
            metadata: None,
        }
    }

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

    pub fn execution_timed_out(task_id: TaskId, worker_id: WorkerId, timeout_seconds: u64) -> Self {
        Self {
            event_type: WorkerTaskEventType::ExecutionTimedOut { timeout_seconds },
            task_id,
            worker_id,
            timestamp: Timestamptz::now(),
            metadata: None,
        }
    }

    pub fn with_metadata(mut self, metadata: serde_json::Value) -> Self {
        self.metadata = Some(metadata);
        self
    }
}

#[cfg(all(test, feature = "uuid"))]
mod tests {
    use super::*;
    use crate::types::identifiers::Identifier;

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
    }

    #[test]
    fn test_execution_completed_event() {
        let task_id = TaskId::generate();
        let worker_id = WorkerId::generate();
        let output = serde_json::json!({"result": "success"});
        let duration_ms = 1500;

        let event =
            WorkerTaskEvent::execution_completed(task_id, worker_id, output.clone(), duration_ms);

        assert_eq!(event.task_id, task_id);
        assert_eq!(event.worker_id, worker_id);

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
        let metadata = serde_json::json!({"trace_id": "abc123", "env": "production"});

        let event =
            WorkerTaskEvent::execution_started(task_id, worker_id).with_metadata(metadata.clone());

        assert_eq!(event.metadata, Some(metadata));
    }

    #[test]
    fn test_event_serialization() {
        let task_id = TaskId::generate();
        let worker_id = WorkerId::generate();
        let event = WorkerTaskEvent::execution_started(task_id, worker_id);

        let json = serde_json::to_string(&event).unwrap();
        let deserialized: WorkerTaskEvent = serde_json::from_str(&json).unwrap();

        assert_eq!(event, deserialized);
    }

    #[test]
    fn test_event_serialization_completed() {
        let task_id = TaskId::generate();
        let worker_id = WorkerId::generate();
        let output = serde_json::json!({"result": "success", "count": 42});
        let event = WorkerTaskEvent::execution_completed(task_id, worker_id, output, 1500);

        let json = serde_json::to_string(&event).unwrap();
        let deserialized: WorkerTaskEvent = serde_json::from_str(&json).unwrap();

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
            WorkerTaskEvent::execution_completed(
                task_id,
                worker_id,
                serde_json::json!({"test": true}),
                100,
            ),
            WorkerTaskEvent::execution_failed(task_id, worker_id, "error".to_string(), 50),
            WorkerTaskEvent::execution_timed_out(task_id, worker_id, 300),
        ];

        for event in events {
            let json = serde_json::to_string(&event).unwrap();
            let deserialized: WorkerTaskEvent = serde_json::from_str(&json).unwrap();
            assert_eq!(event, deserialized);
        }
    }
}
