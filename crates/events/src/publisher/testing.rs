//! Mock publisher for tests — records every event for later assertions.

use crate::domain::{
    DlqEvent, ExecutionEvent, SchedulerEvent, TaskEvent, WorkerEvent, WorkerTaskEvent,
    WorkflowEvent,
};
use crate::error::Result;
use crate::publisher::traits::EventPublisher;
use async_trait::async_trait;
use std::sync::{Arc, Mutex};

/// Records published events so tests can inspect what happened.
/// Call `set_should_fail(true)` to simulate publish failures.
#[derive(Clone)]
pub struct MockPublisher {
    task_events: Arc<Mutex<Vec<TaskEvent>>>,
    worker_events: Arc<Mutex<Vec<WorkerEvent>>>,
    workflow_events: Arc<Mutex<Vec<WorkflowEvent>>>,
    worker_task_events: Arc<Mutex<Vec<WorkerTaskEvent>>>,
    execution_events: Arc<Mutex<Vec<ExecutionEvent>>>,
    scheduler_events: Arc<Mutex<Vec<SchedulerEvent>>>,
    dlq_events: Arc<Mutex<Vec<DlqEvent>>>,
    should_fail: Arc<Mutex<bool>>,
}

impl MockPublisher {
    /// Fresh mock — empty event lists, won't fail by default.
    pub fn new() -> Self {
        Self {
            task_events: Arc::new(Mutex::new(Vec::new())),
            worker_events: Arc::new(Mutex::new(Vec::new())),
            workflow_events: Arc::new(Mutex::new(Vec::new())),
            worker_task_events: Arc::new(Mutex::new(Vec::new())),
            execution_events: Arc::new(Mutex::new(Vec::new())),
            scheduler_events: Arc::new(Mutex::new(Vec::new())),
            dlq_events: Arc::new(Mutex::new(Vec::new())),
            should_fail: Arc::new(Mutex::new(false)),
        }
    }

    /// When set to `true`, all subsequent publishes will return an error.
    pub fn set_should_fail(&self, fail: bool) {
        *self.should_fail.lock().unwrap() = fail;
    }

    /// All recorded task events.
    pub fn task_events(&self) -> Vec<TaskEvent> {
        self.task_events.lock().unwrap().clone()
    }

    /// All recorded worker events.
    pub fn worker_events(&self) -> Vec<WorkerEvent> {
        self.worker_events.lock().unwrap().clone()
    }

    /// All recorded workflow events.
    pub fn workflow_events(&self) -> Vec<WorkflowEvent> {
        self.workflow_events.lock().unwrap().clone()
    }

    /// All recorded worker-task events.
    pub fn worker_task_events(&self) -> Vec<WorkerTaskEvent> {
        self.worker_task_events.lock().unwrap().clone()
    }

    /// All recorded execution events.
    pub fn execution_events(&self) -> Vec<ExecutionEvent> {
        self.execution_events.lock().unwrap().clone()
    }

    /// All recorded scheduler events.
    pub fn scheduler_events(&self) -> Vec<SchedulerEvent> {
        self.scheduler_events.lock().unwrap().clone()
    }

    /// Count of recorded task events.
    pub fn task_event_count(&self) -> usize {
        self.task_events.lock().unwrap().len()
    }

    /// Count of recorded worker events.
    pub fn worker_event_count(&self) -> usize {
        self.worker_events.lock().unwrap().len()
    }

    /// Count of recorded workflow events.
    pub fn workflow_event_count(&self) -> usize {
        self.workflow_events.lock().unwrap().len()
    }

    /// Count of recorded worker-task events.
    pub fn worker_task_event_count(&self) -> usize {
        self.worker_task_events.lock().unwrap().len()
    }

    /// Count of recorded execution events.
    pub fn execution_event_count(&self) -> usize {
        self.execution_events.lock().unwrap().len()
    }

    /// Count of recorded scheduler events.
    pub fn scheduler_event_count(&self) -> usize {
        self.scheduler_events.lock().unwrap().len()
    }

    /// Sum across all event types.
    pub fn total_event_count(&self) -> usize {
        self.task_event_count()
            + self.worker_event_count()
            + self.workflow_event_count()
            + self.worker_task_event_count()
            + self.execution_event_count()
            + self.scheduler_event_count()
    }

    /// Wipe all recorded events.
    pub fn clear(&self) {
        self.task_events.lock().unwrap().clear();
        self.worker_events.lock().unwrap().clear();
        self.workflow_events.lock().unwrap().clear();
        self.worker_task_events.lock().unwrap().clear();
        self.execution_events.lock().unwrap().clear();
        self.scheduler_events.lock().unwrap().clear();
    }

    /// Panics if no recorded task event matches the predicate.
    pub fn assert_task_event_published<F>(&self, predicate: F)
    where
        F: Fn(&TaskEvent) -> bool,
    {
        let events = self.task_events();
        assert!(events.iter().any(predicate), "No matching task event found");
    }

    /// Panics if no recorded worker event matches the predicate.
    pub fn assert_worker_event_published<F>(&self, predicate: F)
    where
        F: Fn(&WorkerEvent) -> bool,
    {
        let events = self.worker_events();
        assert!(
            events.iter().any(predicate),
            "No matching worker event found"
        );
    }

    /// Panics if no recorded workflow event matches the predicate.
    pub fn assert_workflow_event_published<F>(&self, predicate: F)
    where
        F: Fn(&WorkflowEvent) -> bool,
    {
        let events = self.workflow_events();
        assert!(
            events.iter().any(predicate),
            "No matching workflow event found"
        );
    }

    /// Panics if anything was published at all.
    pub fn assert_no_events(&self) {
        assert_eq!(
            self.total_event_count(),
            0,
            "Expected no events, but {} were published",
            self.total_event_count()
        );
    }

    /// Panics if the task event count doesn't match `expected`.
    pub fn assert_task_event_count(&self, expected: usize) {
        let actual = self.task_event_count();
        assert_eq!(
            actual, expected,
            "Expected {} task events, but {} were published",
            expected, actual
        );
    }
}

impl Default for MockPublisher {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl EventPublisher for MockPublisher {
    async fn publish_task_event(&self, event: TaskEvent) -> Result<()> {
        if *self.should_fail.lock().unwrap() {
            return Err(crate::error::EventError::PublishError {
                message: "Mock publisher configured to fail".to_string(),
            });
        }
        self.task_events.lock().unwrap().push(event);
        Ok(())
    }

    async fn publish_worker_event(&self, event: WorkerEvent) -> Result<()> {
        if *self.should_fail.lock().unwrap() {
            return Err(crate::error::EventError::PublishError {
                message: "Mock publisher configured to fail".to_string(),
            });
        }
        self.worker_events.lock().unwrap().push(event);
        Ok(())
    }

    async fn publish_workflow_event(&self, event: WorkflowEvent) -> Result<()> {
        if *self.should_fail.lock().unwrap() {
            return Err(crate::error::EventError::PublishError {
                message: "Mock publisher configured to fail".to_string(),
            });
        }
        self.workflow_events.lock().unwrap().push(event);
        Ok(())
    }

    async fn publish_worker_task_event(&self, event: WorkerTaskEvent) -> Result<()> {
        if *self.should_fail.lock().unwrap() {
            return Err(crate::error::EventError::PublishError {
                message: "Mock publisher configured to fail".to_string(),
            });
        }
        self.worker_task_events.lock().unwrap().push(event);
        Ok(())
    }

    async fn publish_execution_event(&self, event: ExecutionEvent) -> Result<()> {
        if *self.should_fail.lock().unwrap() {
            return Err(crate::error::EventError::PublishError {
                message: "Mock publisher configured to fail".to_string(),
            });
        }
        self.execution_events.lock().unwrap().push(event);
        Ok(())
    }

    async fn publish_scheduler_event(&self, event: SchedulerEvent) -> Result<()> {
        if *self.should_fail.lock().unwrap() {
            return Err(crate::error::EventError::PublishError {
                message: "Mock publisher configured to fail".to_string(),
            });
        }
        self.scheduler_events.lock().unwrap().push(event);
        Ok(())
    }

    async fn publish_dlq_event(&self, event: DlqEvent) -> Result<()> {
        if *self.should_fail.lock().unwrap() {
            return Err(crate::error::EventError::PublishError {
                message: "Mock publisher configured to fail".to_string(),
            });
        }
        self.dlq_events.lock().unwrap().push(event);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orcher_common::types::identifiers::{Identifier, TaskId, WorkerId, WorkflowId};
    use orcher_common::types::time::Timestamptz;

    #[tokio::test]
    async fn test_mock_publisher_creation() {
        let publisher = MockPublisher::new();
        assert_eq!(publisher.total_event_count(), 0);
    }

    #[tokio::test]
    async fn test_publish_and_retrieve_task_event() {
        let publisher = MockPublisher::new();

        let task_id = TaskId::generate();
        let event = TaskEvent::Submitted {
            task_id,
            submitted_at: Timestamptz::now(),
            priority: 5,
        };

        publisher.publish_task_event(event).await.unwrap();

        assert_eq!(publisher.task_event_count(), 1);
        let events = publisher.task_events();
        assert!(matches!(events[0], TaskEvent::Submitted { .. }));
    }

    #[tokio::test]
    async fn test_publish_multiple_event_types() {
        let publisher = MockPublisher::new();

        let task_id = TaskId::generate();
        let task_event = TaskEvent::Submitted {
            task_id,
            submitted_at: Timestamptz::now(),
            priority: 5,
        };

        let worker_id = WorkerId::generate();
        let worker_event = WorkerEvent::Registered {
            worker_id,
            capabilities: vec!["compute".to_string()],
            registered_at: Timestamptz::now(),
        };

        let workflow_id = WorkflowId::generate();
        let workflow_event = WorkflowEvent::Started {
            workflow_id,
            started_at: Timestamptz::now(),
        };

        publisher.publish_task_event(task_event).await.unwrap();
        publisher.publish_worker_event(worker_event).await.unwrap();
        publisher
            .publish_workflow_event(workflow_event)
            .await
            .unwrap();

        assert_eq!(publisher.task_event_count(), 1);
        assert_eq!(publisher.worker_event_count(), 1);
        assert_eq!(publisher.workflow_event_count(), 1);
        assert_eq!(publisher.total_event_count(), 3);
    }

    #[tokio::test]
    async fn test_clear_events() {
        let publisher = MockPublisher::new();

        let task_id = TaskId::generate();
        let event = TaskEvent::Submitted {
            task_id,
            submitted_at: Timestamptz::now(),
            priority: 5,
        };

        publisher.publish_task_event(event).await.unwrap();
        assert_eq!(publisher.task_event_count(), 1);

        publisher.clear();
        assert_eq!(publisher.task_event_count(), 0);
        assert_eq!(publisher.total_event_count(), 0);
    }

    #[tokio::test]
    async fn test_assert_task_event_published() {
        let publisher = MockPublisher::new();
        let task_id = TaskId::generate();

        let event = TaskEvent::Submitted {
            task_id,
            submitted_at: Timestamptz::now(),
            priority: 5,
        };

        publisher.publish_task_event(event).await.unwrap();

        publisher.assert_task_event_published(
            |e| matches!(e, TaskEvent::Submitted { task_id: id, .. } if *id == task_id),
        );
    }

    #[tokio::test]
    async fn test_assert_no_events() {
        let publisher = MockPublisher::new();
        publisher.assert_no_events();
    }

    #[tokio::test]
    async fn test_assert_task_event_count() {
        let publisher = MockPublisher::new();

        let task_id1 = TaskId::generate();
        let event1 = TaskEvent::Submitted {
            task_id: task_id1,
            submitted_at: Timestamptz::now(),
            priority: 5,
        };

        let task_id2 = TaskId::generate();
        let event2 = TaskEvent::Submitted {
            task_id: task_id2,
            submitted_at: Timestamptz::now(),
            priority: 8,
        };

        publisher.publish_task_event(event1).await.unwrap();
        publisher.publish_task_event(event2).await.unwrap();

        publisher.assert_task_event_count(2);
    }

    #[tokio::test]
    async fn test_should_fail() {
        let publisher = MockPublisher::new();
        publisher.set_should_fail(true);

        let task_id = TaskId::generate();
        let event = TaskEvent::Submitted {
            task_id,
            submitted_at: Timestamptz::now(),
            priority: 5,
        };

        let result = publisher.publish_task_event(event).await;
        assert!(result.is_err());
        assert_eq!(publisher.task_event_count(), 0);
    }

    #[tokio::test]
    async fn test_mock_publisher_clone() {
        let publisher1 = MockPublisher::new();

        let task_id = TaskId::generate();
        let event = TaskEvent::Submitted {
            task_id,
            submitted_at: Timestamptz::now(),
            priority: 5,
        };

        publisher1.publish_task_event(event).await.unwrap();

        let publisher2 = publisher1.clone();
        assert_eq!(publisher2.task_event_count(), 1);
    }
}
