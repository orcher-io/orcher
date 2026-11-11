//! Simplest publisher — just forwards events to the in-memory bus.

use crate::bus::EventBus;
use crate::domain::{
    DlqEvent, ExecutionEvent, SchedulerEvent, TaskEvent, WorkerEvent, WorkerTaskEvent,
    WorkflowEvent,
};
use crate::error::Result;
use crate::publisher::traits::EventPublisher;
use async_trait::async_trait;
use std::sync::Arc;

/// Thin wrapper that publishes directly to an [`EventBus`]. No persistence, no middleware.
pub struct LocalPublisher {
    bus: Arc<EventBus>,
}

impl LocalPublisher {
    /// Wrap an existing bus.
    pub fn new(bus: Arc<EventBus>) -> Self {
        Self { bus }
    }

    /// Access the underlying bus.
    pub fn bus(&self) -> &Arc<EventBus> {
        &self.bus
    }
}

#[async_trait]
impl EventPublisher for LocalPublisher {
    async fn publish_task_event(&self, event: TaskEvent) -> Result<()> {
        self.bus.publish_task_event(event).await
    }

    async fn publish_worker_event(&self, event: WorkerEvent) -> Result<()> {
        self.bus.publish_worker_event(event).await
    }

    async fn publish_workflow_event(&self, event: WorkflowEvent) -> Result<()> {
        self.bus.publish_workflow_event(event).await
    }

    async fn publish_worker_task_event(&self, event: WorkerTaskEvent) -> Result<()> {
        self.bus.publish_worker_task_event(event).await
    }

    async fn publish_execution_event(&self, event: ExecutionEvent) -> Result<()> {
        self.bus.publish_execution_event(event).await
    }

    async fn publish_scheduler_event(&self, event: SchedulerEvent) -> Result<()> {
        self.bus.publish_scheduler_event(event).await
    }

    async fn publish_dlq_event(&self, event: DlqEvent) -> Result<()> {
        self.bus.publish_dlq_event(event).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::publisher::EventPublisher;
    use orcher_common::types::identifiers::{Identifier, TaskId, WorkerId, WorkflowId};
    use orcher_common::types::time::Timestamptz;

    #[tokio::test]
    async fn test_local_publisher_creation() {
        let bus = EventBus::with_defaults();
        let publisher = LocalPublisher::new(bus);
        assert!(publisher.bus().task_subscriber_count() == 0);
    }

    #[tokio::test]
    async fn test_publish_task_event() {
        let bus = EventBus::with_defaults();
        let publisher = LocalPublisher::new(Arc::clone(&bus));
        let mut receiver = bus.subscribe_task_events();

        let task_id = TaskId::generate();
        let event = TaskEvent::Submitted {
            task_id,
            submitted_at: Timestamptz::now(),
            priority: 5,
        };

        publisher.publish_task_event(event.clone()).await.unwrap();

        let received = receiver.recv().await.unwrap();
        match received {
            TaskEvent::Submitted {
                task_id: id,
                priority,
                ..
            } => {
                assert_eq!(id, task_id);
                assert_eq!(priority, 5);
            }
            _ => panic!("Expected TaskEvent::Submitted"),
        }
    }

    #[tokio::test]
    async fn test_publish_worker_event() {
        let bus = EventBus::with_defaults();
        let publisher = LocalPublisher::new(Arc::clone(&bus));
        let mut receiver = bus.subscribe_worker_events();

        let worker_id = WorkerId::generate();
        let event = WorkerEvent::Registered {
            worker_id,
            capabilities: vec!["compute".to_string()],
            registered_at: Timestamptz::now(),
        };

        publisher.publish_worker_event(event.clone()).await.unwrap();

        let received = receiver.recv().await.unwrap();
        assert!(matches!(received, WorkerEvent::Registered { .. }));
    }

    #[tokio::test]
    async fn test_publish_workflow_event() {
        let bus = EventBus::with_defaults();
        let publisher = LocalPublisher::new(Arc::clone(&bus));
        let mut receiver = bus.subscribe_workflow_events();

        let workflow_id = WorkflowId::generate();
        let event = WorkflowEvent::Started {
            workflow_id,
            started_at: Timestamptz::now(),
        };

        publisher
            .publish_workflow_event(event.clone())
            .await
            .unwrap();

        let received = receiver.recv().await.unwrap();
        assert!(matches!(received, WorkflowEvent::Started { .. }));
    }

    #[tokio::test]
    async fn test_publish_worker_task_event() {
        let bus = EventBus::with_defaults();
        let publisher = LocalPublisher::new(Arc::clone(&bus));
        let mut receiver = bus.subscribe_worker_task_events();

        let task_id = TaskId::generate();
        let worker_id = WorkerId::generate();
        let event = WorkerTaskEvent::execution_started(task_id, worker_id);

        publisher
            .publish_worker_task_event(event.clone())
            .await
            .unwrap();

        let received = receiver.recv().await.unwrap();
        assert_eq!(received.task_id, task_id);
        assert_eq!(received.worker_id, worker_id);
    }

    #[tokio::test]
    async fn test_publish_execution_event() {
        let bus = EventBus::with_defaults();
        let publisher = LocalPublisher::new(Arc::clone(&bus));
        let mut receiver = bus.subscribe_execution_events();

        let event = ExecutionEvent::Started {
            execution_id: "exec_123".to_string(),
            started_at: Timestamptz::now(),
        };

        publisher
            .publish_execution_event(event.clone())
            .await
            .unwrap();

        let received = receiver.recv().await.unwrap();
        assert_eq!(received.execution_id(), "exec_123");
    }

    #[tokio::test]
    async fn test_publish_scheduler_event() {
        let bus = EventBus::with_defaults();
        let publisher = LocalPublisher::new(Arc::clone(&bus));
        let mut receiver = bus.subscribe_scheduler_events();

        let event = SchedulerEvent::ScheduleEnabled {
            schedule_id: "sched_123".to_string(),
            enabled_at: Timestamptz::now(),
        };

        publisher
            .publish_scheduler_event(event.clone())
            .await
            .unwrap();

        let received = receiver.recv().await.unwrap();
        assert_eq!(received.schedule_id(), "sched_123");
    }

    #[tokio::test]
    async fn test_multiple_publishers_same_bus() {
        let bus = EventBus::with_defaults();
        let publisher1 = LocalPublisher::new(Arc::clone(&bus));
        let publisher2 = LocalPublisher::new(Arc::clone(&bus));
        let mut receiver = bus.subscribe_task_events();

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
            priority: 10,
        };

        publisher1.publish_task_event(event1).await.unwrap();
        publisher2.publish_task_event(event2).await.unwrap();

        let received1 = receiver.recv().await.unwrap();
        let received2 = receiver.recv().await.unwrap();

        assert!(matches!(received1, TaskEvent::Submitted { .. }));
        assert!(matches!(received2, TaskEvent::Submitted { .. }));
    }

    #[tokio::test]
    async fn test_publisher_with_no_subscribers() {
        let bus = EventBus::with_defaults();
        let publisher = LocalPublisher::new(bus);

        let task_id = TaskId::generate();
        let event = TaskEvent::Submitted {
            task_id,
            submitted_at: Timestamptz::now(),
            priority: 5,
        };

        // Should not error even with no subscribers
        let result = publisher.publish_task_event(event).await;
        assert!(result.is_ok());
    }
}
