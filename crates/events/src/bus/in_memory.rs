//! The main `EventBus` — one broadcast channel per domain event type.

use crate::bus::channel::TypedChannel;
use crate::bus::config::EventBusConfig;
use crate::domain::{
    DlqEvent, ExecutionEvent, SchedulerEvent, TaskEvent, WorkerEvent, WorkerTaskEvent,
    WorkflowEvent,
};
use crate::error::Result;
use std::sync::Arc;
use tokio::sync::broadcast;

/// Central hub for in-process event distribution.
///
/// Each event kind gets its own broadcast channel so subscribers only
/// see the types they care about. Publish is non-blocking; if nobody
/// is listening the event is silently dropped.
pub struct EventBus {
    task_events: TypedChannel<TaskEvent>,
    worker_events: TypedChannel<WorkerEvent>,
    workflow_events: TypedChannel<WorkflowEvent>,
    worker_task_events: TypedChannel<WorkerTaskEvent>,
    execution_events: TypedChannel<ExecutionEvent>,
    scheduler_events: TypedChannel<SchedulerEvent>,
    dlq_events: TypedChannel<DlqEvent>,
    config: EventBusConfig,
}

impl EventBus {
    /// Build a bus from the given config. Returns `Arc` so it can be shared across tasks.
    pub fn new(config: EventBusConfig) -> Arc<Self> {
        Arc::new(Self {
            task_events: TypedChannel::new(config.task_channel_capacity),
            worker_events: TypedChannel::new(config.worker_channel_capacity),
            workflow_events: TypedChannel::new(config.workflow_channel_capacity),
            worker_task_events: TypedChannel::new(config.worker_task_channel_capacity),
            execution_events: TypedChannel::new(config.execution_channel_capacity),
            scheduler_events: TypedChannel::new(config.scheduler_channel_capacity),
            dlq_events: TypedChannel::new(config.task_channel_capacity), // Use same capacity as tasks
            config,
        })
    }

    /// Shorthand for `new(EventBusConfig::default())`.
    pub fn with_defaults() -> Arc<Self> {
        Self::new(EventBusConfig::default())
    }

    // -- Task events --

    pub async fn publish_task_event(&self, event: TaskEvent) -> Result<()> {
        self.task_events.publish(event).await
    }

    pub fn subscribe_task_events(&self) -> broadcast::Receiver<TaskEvent> {
        self.task_events.subscribe()
    }

    pub fn task_subscriber_count(&self) -> usize {
        self.task_events.subscriber_count()
    }

    // -- Worker events --

    pub async fn publish_worker_event(&self, event: WorkerEvent) -> Result<()> {
        self.worker_events.publish(event).await
    }

    pub fn subscribe_worker_events(&self) -> broadcast::Receiver<WorkerEvent> {
        self.worker_events.subscribe()
    }

    pub fn worker_subscriber_count(&self) -> usize {
        self.worker_events.subscriber_count()
    }

    // -- Workflow events --

    pub async fn publish_workflow_event(&self, event: WorkflowEvent) -> Result<()> {
        self.workflow_events.publish(event).await
    }

    pub fn subscribe_workflow_events(&self) -> broadcast::Receiver<WorkflowEvent> {
        self.workflow_events.subscribe()
    }

    pub fn workflow_subscriber_count(&self) -> usize {
        self.workflow_events.subscriber_count()
    }

    // -- Worker-task events --

    pub async fn publish_worker_task_event(&self, event: WorkerTaskEvent) -> Result<()> {
        self.worker_task_events.publish(event).await
    }

    pub fn subscribe_worker_task_events(&self) -> broadcast::Receiver<WorkerTaskEvent> {
        self.worker_task_events.subscribe()
    }

    pub fn worker_task_subscriber_count(&self) -> usize {
        self.worker_task_events.subscriber_count()
    }

    // -- Execution events --

    pub async fn publish_execution_event(&self, event: ExecutionEvent) -> Result<()> {
        self.execution_events.publish(event).await
    }

    pub fn subscribe_execution_events(&self) -> broadcast::Receiver<ExecutionEvent> {
        self.execution_events.subscribe()
    }

    pub fn execution_subscriber_count(&self) -> usize {
        self.execution_events.subscriber_count()
    }

    // -- Scheduler events --

    pub async fn publish_scheduler_event(&self, event: SchedulerEvent) -> Result<()> {
        self.scheduler_events.publish(event).await
    }

    pub fn subscribe_scheduler_events(&self) -> broadcast::Receiver<SchedulerEvent> {
        self.scheduler_events.subscribe()
    }

    pub fn scheduler_subscriber_count(&self) -> usize {
        self.scheduler_events.subscriber_count()
    }

    // -- DLQ events --

    pub async fn publish_dlq_event(&self, event: DlqEvent) -> Result<()> {
        self.dlq_events.publish(event).await
    }

    pub fn subscribe_dlq_events(&self) -> broadcast::Receiver<DlqEvent> {
        self.dlq_events.subscribe()
    }

    pub fn dlq_subscriber_count(&self) -> usize {
        self.dlq_events.subscriber_count()
    }

    // -- Management --

    pub fn config(&self) -> &EventBusConfig {
        &self.config
    }

    /// Sum of subscribers across all channel types.
    pub fn total_subscriber_count(&self) -> usize {
        self.task_subscriber_count()
            + self.worker_subscriber_count()
            + self.workflow_subscriber_count()
            + self.worker_task_subscriber_count()
            + self.execution_subscriber_count()
            + self.scheduler_subscriber_count()
            + self.dlq_subscriber_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orcher_common::types::identifiers::{Identifier, TaskId, WorkerId, WorkflowId};
    use orcher_common::types::time::Timestamptz;

    #[test]
    fn test_event_bus_creation() {
        let config = EventBusConfig::default();
        let bus = EventBus::new(config);
        assert_eq!(bus.task_subscriber_count(), 0);
        assert_eq!(bus.total_subscriber_count(), 0);
    }

    #[test]
    fn test_event_bus_with_defaults() {
        let bus = EventBus::with_defaults();
        assert_eq!(bus.config().task_channel_capacity, 10_000);
    }

    #[tokio::test]
    async fn test_task_event_publish_subscribe() {
        let bus = EventBus::with_defaults();
        let mut receiver = bus.subscribe_task_events();

        let task_id = TaskId::generate();
        let event = TaskEvent::Submitted {
            task_id,
            submitted_at: Timestamptz::now(),
            priority: 5,
        };

        bus.publish_task_event(event.clone()).await.unwrap();

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
    async fn test_multiple_subscribers() {
        let bus = EventBus::with_defaults();
        let mut receiver1 = bus.subscribe_task_events();
        let mut receiver2 = bus.subscribe_task_events();

        assert_eq!(bus.task_subscriber_count(), 2);

        let task_id = TaskId::generate();
        let event = TaskEvent::Submitted {
            task_id,
            submitted_at: Timestamptz::now(),
            priority: 5,
        };

        bus.publish_task_event(event.clone()).await.unwrap();

        let received1 = receiver1.recv().await.unwrap();
        let received2 = receiver2.recv().await.unwrap();

        match (received1, received2) {
            (
                TaskEvent::Submitted {
                    task_id: id1,
                    priority: p1,
                    ..
                },
                TaskEvent::Submitted {
                    task_id: id2,
                    priority: p2,
                    ..
                },
            ) => {
                assert_eq!(id1, task_id);
                assert_eq!(id2, task_id);
                assert_eq!(p1, 5);
                assert_eq!(p2, 5);
            }
            _ => panic!("Expected TaskEvent::Submitted"),
        }
    }

    #[tokio::test]
    async fn test_worker_event_publish_subscribe() {
        let bus = EventBus::with_defaults();
        let mut receiver = bus.subscribe_worker_events();

        let worker_id = WorkerId::generate();
        let event = WorkerEvent::Registered {
            worker_id,
            capabilities: vec!["compute".to_string()],
            registered_at: Timestamptz::now(),
        };

        bus.publish_worker_event(event.clone()).await.unwrap();

        let received = receiver.recv().await.unwrap();
        assert!(matches!(received, WorkerEvent::Registered { .. }));
    }

    #[tokio::test]
    async fn test_workflow_event_publish_subscribe() {
        let bus = EventBus::with_defaults();
        let mut receiver = bus.subscribe_workflow_events();

        let workflow_id = WorkflowId::generate();
        let event = WorkflowEvent::Started {
            workflow_id,
            started_at: Timestamptz::now(),
        };

        bus.publish_workflow_event(event.clone()).await.unwrap();

        let received = receiver.recv().await.unwrap();
        assert!(matches!(received, WorkflowEvent::Started { .. }));
    }

    #[tokio::test]
    async fn test_worker_task_event_publish_subscribe() {
        let bus = EventBus::with_defaults();
        let mut receiver = bus.subscribe_worker_task_events();

        let task_id = TaskId::generate();
        let worker_id = WorkerId::generate();
        let event = WorkerTaskEvent::execution_started(task_id, worker_id);

        bus.publish_worker_task_event(event.clone()).await.unwrap();

        let received = receiver.recv().await.unwrap();
        assert_eq!(received.task_id, task_id);
        assert_eq!(received.worker_id, worker_id);
    }

    #[tokio::test]
    async fn test_execution_event_publish_subscribe() {
        let bus = EventBus::with_defaults();
        let mut receiver = bus.subscribe_execution_events();

        let event = ExecutionEvent::Started {
            execution_id: "exec_123".to_string(),
            started_at: Timestamptz::now(),
        };

        bus.publish_execution_event(event.clone()).await.unwrap();

        let received = receiver.recv().await.unwrap();
        assert_eq!(received.execution_id(), "exec_123");
    }

    #[tokio::test]
    async fn test_scheduler_event_publish_subscribe() {
        let bus = EventBus::with_defaults();
        let mut receiver = bus.subscribe_scheduler_events();

        let event = SchedulerEvent::ScheduleEnabled {
            schedule_id: "sched_123".to_string(),
            enabled_at: Timestamptz::now(),
        };

        bus.publish_scheduler_event(event.clone()).await.unwrap();

        let received = receiver.recv().await.unwrap();
        assert_eq!(received.schedule_id(), "sched_123");
    }

    #[tokio::test]
    async fn test_subscriber_count() {
        let bus = EventBus::with_defaults();

        assert_eq!(bus.task_subscriber_count(), 0);
        assert_eq!(bus.total_subscriber_count(), 0);

        let _receiver1 = bus.subscribe_task_events();
        assert_eq!(bus.task_subscriber_count(), 1);
        assert_eq!(bus.total_subscriber_count(), 1);

        let _receiver2 = bus.subscribe_worker_events();
        assert_eq!(bus.total_subscriber_count(), 2);
    }

    #[tokio::test]
    async fn test_multiple_event_types_independent() {
        let bus = EventBus::with_defaults();
        let mut task_receiver = bus.subscribe_task_events();
        let mut worker_receiver = bus.subscribe_worker_events();

        // Publish task event
        let task_id = TaskId::generate();
        let task_event = TaskEvent::Submitted {
            task_id,
            submitted_at: Timestamptz::now(),
            priority: 5,
        };
        bus.publish_task_event(task_event).await.unwrap();

        // Publish worker event
        let worker_id = WorkerId::generate();
        let worker_event = WorkerEvent::Registered {
            worker_id,
            capabilities: vec!["compute".to_string()],
            registered_at: Timestamptz::now(),
        };
        bus.publish_worker_event(worker_event).await.unwrap();

        // Each receiver should only get their event type
        let received_task = task_receiver.recv().await.unwrap();
        let received_worker = worker_receiver.recv().await.unwrap();

        assert!(matches!(received_task, TaskEvent::Submitted { .. }));
        assert!(matches!(received_worker, WorkerEvent::Registered { .. }));
    }

    #[tokio::test]
    async fn test_publish_without_subscribers() {
        let bus = EventBus::with_defaults();

        let task_id = TaskId::generate();
        let event = TaskEvent::Submitted {
            task_id,
            submitted_at: Timestamptz::now(),
            priority: 5,
        };

        // Should not error even with no subscribers
        let result = bus.publish_task_event(event).await;
        assert!(result.is_ok());
    }
}
