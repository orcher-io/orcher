//! Fan-out publisher: send events to multiple downstream publishers.

use crate::domain::{
    DlqEvent, ExecutionEvent, SchedulerEvent, TaskEvent, WorkerEvent, WorkerTaskEvent,
    WorkflowEvent,
};
use crate::error::{EventError, Result};
use crate::publisher::traits::EventPublisher;
use async_trait::async_trait;
use std::sync::Arc;

/// How the multi-publisher handles failures across its targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PublishStrategy {
    /// Try all targets; return error if any failed.
    #[default]
    All,

    /// Try targets in order; stop on the first success.
    FirstSuccess,

    /// Fire-and-forget to all targets; always returns Ok.
    Broadcast,

    /// Try targets in order; stop on the first failure.
    FailFast,
}

/// Publishes each event to a list of downstream publishers.
pub struct MultiPublisher {
    publishers: Vec<Arc<dyn EventPublisher>>,
    strategy: PublishStrategy,
}

impl MultiPublisher {
    /// Create with a given strategy. Starts with no publishers attached.
    pub fn new(strategy: PublishStrategy) -> Self {
        Self {
            publishers: Vec::new(),
            strategy,
        }
    }

    /// Shorthand for `new(PublishStrategy::All)`.
    pub fn with_defaults() -> Self {
        Self::new(PublishStrategy::default())
    }

    /// Append a downstream publisher.
    pub fn add_publisher(&mut self, publisher: Arc<dyn EventPublisher>) {
        self.publishers.push(publisher);
    }

    /// How many downstream publishers are attached.
    pub fn publisher_count(&self) -> usize {
        self.publishers.len()
    }

    /// Current strategy.
    pub fn strategy(&self) -> PublishStrategy {
        self.strategy
    }

    /// Switch to a different strategy.
    pub fn set_strategy(&mut self, strategy: PublishStrategy) {
        self.strategy = strategy;
    }

    /// Remove all downstream publishers.
    pub fn clear_publishers(&mut self) {
        self.publishers.clear();
    }

    /// Run the publish closure against all targets according to `self.strategy`.
    async fn execute_strategy<F, Fut>(&self, mut publish_fn: F) -> Result<()>
    where
        F: FnMut(&Arc<dyn EventPublisher>) -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        if self.publishers.is_empty() {
            return Ok(());
        }

        match self.strategy {
            PublishStrategy::All => {
                let mut errors = Vec::new();
                for publisher in &self.publishers {
                    if let Err(e) = publish_fn(publisher).await {
                        errors.push(e);
                    }
                }
                if !errors.is_empty() {
                    return Err(EventError::PublishError {
                        message: format!(
                            "Failed to publish to {} out of {} publishers",
                            errors.len(),
                            self.publishers.len()
                        ),
                    });
                }
                Ok(())
            }
            PublishStrategy::FirstSuccess => {
                let mut last_error = None;
                for publisher in &self.publishers {
                    match publish_fn(publisher).await {
                        Ok(_) => return Ok(()),
                        Err(e) => last_error = Some(e),
                    }
                }
                Err(last_error.unwrap_or_else(|| EventError::PublishError {
                    message: "No publishers available".to_string(),
                }))
            }
            PublishStrategy::Broadcast => {
                for publisher in &self.publishers {
                    let _ = publish_fn(publisher).await;
                }
                Ok(())
            }
            PublishStrategy::FailFast => {
                for publisher in &self.publishers {
                    publish_fn(publisher).await?;
                }
                Ok(())
            }
        }
    }
}

#[async_trait]
impl EventPublisher for MultiPublisher {
    async fn publish_task_event(&self, event: TaskEvent) -> Result<()> {
        self.execute_strategy(|publisher| {
            let event = event.clone();
            let publisher = Arc::clone(publisher);
            async move { publisher.publish_task_event(event).await }
        })
        .await
    }

    async fn publish_worker_event(&self, event: WorkerEvent) -> Result<()> {
        self.execute_strategy(|publisher| {
            let event = event.clone();
            let publisher = Arc::clone(publisher);
            async move { publisher.publish_worker_event(event).await }
        })
        .await
    }

    async fn publish_workflow_event(&self, event: WorkflowEvent) -> Result<()> {
        self.execute_strategy(|publisher| {
            let event = event.clone();
            let publisher = Arc::clone(publisher);
            async move { publisher.publish_workflow_event(event).await }
        })
        .await
    }

    async fn publish_worker_task_event(&self, event: WorkerTaskEvent) -> Result<()> {
        self.execute_strategy(|publisher| {
            let event = event.clone();
            let publisher = Arc::clone(publisher);
            async move { publisher.publish_worker_task_event(event).await }
        })
        .await
    }

    async fn publish_execution_event(&self, event: ExecutionEvent) -> Result<()> {
        self.execute_strategy(|publisher| {
            let event = event.clone();
            let publisher = Arc::clone(publisher);
            async move { publisher.publish_execution_event(event).await }
        })
        .await
    }

    async fn publish_scheduler_event(&self, event: SchedulerEvent) -> Result<()> {
        self.execute_strategy(|publisher| {
            let event = event.clone();
            let publisher = Arc::clone(publisher);
            async move { publisher.publish_scheduler_event(event).await }
        })
        .await
    }

    async fn publish_dlq_event(&self, event: DlqEvent) -> Result<()> {
        self.execute_strategy(|publisher| {
            let event = event.clone();
            let publisher = Arc::clone(publisher);
            async move { publisher.publish_dlq_event(event).await }
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::EventBus;
    use crate::publisher::LocalPublisher;
    use orcher_common::types::identifiers::{Identifier, TaskId, WorkerId, WorkflowId};
    use orcher_common::types::time::Timestamptz;

    #[test]
    fn test_multi_publisher_creation() {
        let publisher = MultiPublisher::new(PublishStrategy::All);
        assert_eq!(publisher.publisher_count(), 0);
        assert_eq!(publisher.strategy(), PublishStrategy::All);
    }

    #[test]
    fn test_multi_publisher_with_defaults() {
        let publisher = MultiPublisher::with_defaults();
        assert_eq!(publisher.strategy(), PublishStrategy::All);
    }

    #[test]
    fn test_add_publisher() {
        let mut multi = MultiPublisher::with_defaults();
        let bus = EventBus::with_defaults();
        let local = Arc::new(LocalPublisher::new(bus));

        multi.add_publisher(local);
        assert_eq!(multi.publisher_count(), 1);
    }

    #[test]
    fn test_clear_publishers() {
        let mut multi = MultiPublisher::with_defaults();
        let bus1 = EventBus::with_defaults();
        let bus2 = EventBus::with_defaults();

        multi.add_publisher(Arc::new(LocalPublisher::new(bus1)));
        multi.add_publisher(Arc::new(LocalPublisher::new(bus2)));
        assert_eq!(multi.publisher_count(), 2);

        multi.clear_publishers();
        assert_eq!(multi.publisher_count(), 0);
    }

    #[test]
    fn test_set_strategy() {
        let mut multi = MultiPublisher::with_defaults();
        assert_eq!(multi.strategy(), PublishStrategy::All);

        multi.set_strategy(PublishStrategy::FirstSuccess);
        assert_eq!(multi.strategy(), PublishStrategy::FirstSuccess);
    }

    #[tokio::test]
    async fn test_publish_to_multiple_targets() {
        let bus1 = EventBus::with_defaults();
        let bus2 = EventBus::with_defaults();

        let mut receiver1 = bus1.subscribe_task_events();
        let mut receiver2 = bus2.subscribe_task_events();

        let mut multi = MultiPublisher::new(PublishStrategy::All);
        multi.add_publisher(Arc::new(LocalPublisher::new(Arc::clone(&bus1))));
        multi.add_publisher(Arc::new(LocalPublisher::new(Arc::clone(&bus2))));

        let task_id = TaskId::generate();
        let event = TaskEvent::Submitted {
            task_id,
            submitted_at: Timestamptz::now(),
            priority: 5,
        };

        multi.publish_task_event(event).await.unwrap();

        // Both receivers should get the event
        let received1 = receiver1.recv().await.unwrap();
        let received2 = receiver2.recv().await.unwrap();

        assert!(matches!(received1, TaskEvent::Submitted { .. }));
        assert!(matches!(received2, TaskEvent::Submitted { .. }));
    }

    #[tokio::test]
    async fn test_publish_strategy_broadcast() {
        let bus1 = EventBus::with_defaults();
        let bus2 = EventBus::with_defaults();

        let mut receiver1 = bus1.subscribe_task_events();
        let mut receiver2 = bus2.subscribe_task_events();

        let mut multi = MultiPublisher::new(PublishStrategy::Broadcast);
        multi.add_publisher(Arc::new(LocalPublisher::new(Arc::clone(&bus1))));
        multi.add_publisher(Arc::new(LocalPublisher::new(Arc::clone(&bus2))));

        let task_id = TaskId::generate();
        let event = TaskEvent::Submitted {
            task_id,
            submitted_at: Timestamptz::now(),
            priority: 5,
        };

        // Should succeed even if some publishers fail
        let result = multi.publish_task_event(event).await;
        assert!(result.is_ok());

        // Both receivers should get the event
        let received1 = receiver1.recv().await.unwrap();
        let received2 = receiver2.recv().await.unwrap();

        assert!(matches!(received1, TaskEvent::Submitted { .. }));
        assert!(matches!(received2, TaskEvent::Submitted { .. }));
    }

    #[tokio::test]
    async fn test_publish_with_no_publishers() {
        let multi = MultiPublisher::with_defaults();

        let task_id = TaskId::generate();
        let event = TaskEvent::Submitted {
            task_id,
            submitted_at: Timestamptz::now(),
            priority: 5,
        };

        // Should not error with no publishers
        let result = multi.publish_task_event(event).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_publish_worker_event() {
        let bus1 = EventBus::with_defaults();
        let bus2 = EventBus::with_defaults();

        let mut receiver1 = bus1.subscribe_worker_events();
        let mut receiver2 = bus2.subscribe_worker_events();

        let mut multi = MultiPublisher::new(PublishStrategy::All);
        multi.add_publisher(Arc::new(LocalPublisher::new(Arc::clone(&bus1))));
        multi.add_publisher(Arc::new(LocalPublisher::new(Arc::clone(&bus2))));

        let worker_id = WorkerId::generate();
        let event = WorkerEvent::Registered {
            worker_id,
            capabilities: vec!["compute".to_string()],
            registered_at: Timestamptz::now(),
        };

        multi.publish_worker_event(event).await.unwrap();

        let received1 = receiver1.recv().await.unwrap();
        let received2 = receiver2.recv().await.unwrap();

        assert!(matches!(received1, WorkerEvent::Registered { .. }));
        assert!(matches!(received2, WorkerEvent::Registered { .. }));
    }

    #[tokio::test]
    async fn test_publish_workflow_event() {
        let bus1 = EventBus::with_defaults();
        let bus2 = EventBus::with_defaults();

        let mut receiver1 = bus1.subscribe_workflow_events();
        let mut receiver2 = bus2.subscribe_workflow_events();

        let mut multi = MultiPublisher::new(PublishStrategy::All);
        multi.add_publisher(Arc::new(LocalPublisher::new(Arc::clone(&bus1))));
        multi.add_publisher(Arc::new(LocalPublisher::new(Arc::clone(&bus2))));

        let workflow_id = WorkflowId::generate();
        let event = WorkflowEvent::Started {
            workflow_id,
            started_at: Timestamptz::now(),
        };

        multi.publish_workflow_event(event).await.unwrap();

        let received1 = receiver1.recv().await.unwrap();
        let received2 = receiver2.recv().await.unwrap();

        assert!(matches!(received1, WorkflowEvent::Started { .. }));
        assert!(matches!(received2, WorkflowEvent::Started { .. }));
    }

    #[tokio::test]
    async fn test_publish_worker_task_event() {
        let bus = EventBus::with_defaults();
        let mut receiver = bus.subscribe_worker_task_events();

        let mut multi = MultiPublisher::new(PublishStrategy::All);
        multi.add_publisher(Arc::new(LocalPublisher::new(Arc::clone(&bus))));

        let task_id = TaskId::generate();
        let worker_id = WorkerId::generate();
        let event = WorkerTaskEvent::execution_started(task_id, worker_id);

        multi.publish_worker_task_event(event).await.unwrap();

        let received = receiver.recv().await.unwrap();
        assert_eq!(received.task_id, task_id);
        assert_eq!(received.worker_id, worker_id);
    }

    #[tokio::test]
    async fn test_publish_execution_event() {
        let bus = EventBus::with_defaults();
        let mut receiver = bus.subscribe_execution_events();

        let mut multi = MultiPublisher::new(PublishStrategy::All);
        multi.add_publisher(Arc::new(LocalPublisher::new(Arc::clone(&bus))));

        let event = ExecutionEvent::Started {
            execution_id: "exec_123".to_string(),
            started_at: Timestamptz::now(),
        };

        multi.publish_execution_event(event).await.unwrap();

        let received = receiver.recv().await.unwrap();
        assert_eq!(received.execution_id(), "exec_123");
    }

    #[tokio::test]
    async fn test_publish_scheduler_event() {
        let bus = EventBus::with_defaults();
        let mut receiver = bus.subscribe_scheduler_events();

        let mut multi = MultiPublisher::new(PublishStrategy::All);
        multi.add_publisher(Arc::new(LocalPublisher::new(Arc::clone(&bus))));

        let event = SchedulerEvent::ScheduleEnabled {
            schedule_id: "sched_123".to_string(),
            enabled_at: Timestamptz::now(),
        };

        multi.publish_scheduler_event(event).await.unwrap();

        let received = receiver.recv().await.unwrap();
        assert_eq!(received.schedule_id(), "sched_123");
    }
}
