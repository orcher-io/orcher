//! The core `EventPublisher` trait and its typed variants.

use crate::domain::{
    DlqEvent, ExecutionEvent, SchedulerEvent, TaskEvent, WorkerEvent, WorkerTaskEvent,
    WorkflowEvent,
};
use crate::error::Result;
use async_trait::async_trait;

/// Main publishing interface — one method per domain event type.
///
/// Implementations decide where events actually go (bus, DB, external queue, etc).
#[async_trait]
pub trait EventPublisher: Send + Sync {
    /// Publish a task lifecycle event.
    async fn publish_task_event(&self, event: TaskEvent) -> Result<()>;

    /// Publish a worker status event.
    async fn publish_worker_event(&self, event: WorkerEvent) -> Result<()>;

    /// Publish a workflow lifecycle event.
    async fn publish_workflow_event(&self, event: WorkflowEvent) -> Result<()>;

    /// Publish a worker-task execution event.
    async fn publish_worker_task_event(&self, event: WorkerTaskEvent) -> Result<()>;

    /// Publish an execution state change.
    async fn publish_execution_event(&self, event: ExecutionEvent) -> Result<()>;

    /// Publish a scheduler event.
    async fn publish_scheduler_event(&self, event: SchedulerEvent) -> Result<()>;

    /// Publish a dead-letter-queue event.
    async fn publish_dlq_event(&self, event: DlqEvent) -> Result<()>;

    /// Batch-publish task events. Default just loops; override for bulk optimizations.
    async fn publish_task_events_batch(&self, events: Vec<TaskEvent>) -> Result<()> {
        for event in events {
            self.publish_task_event(event).await?;
        }
        Ok(())
    }

    /// Batch-publish worker events.
    async fn publish_worker_events_batch(&self, events: Vec<WorkerEvent>) -> Result<()> {
        for event in events {
            self.publish_worker_event(event).await?;
        }
        Ok(())
    }

    /// Batch-publish workflow events.
    async fn publish_workflow_events_batch(&self, events: Vec<WorkflowEvent>) -> Result<()> {
        for event in events {
            self.publish_workflow_event(event).await?;
        }
        Ok(())
    }
}

/// Generic publisher for domain events (event-sourcing style).
#[async_trait]
pub trait DomainEventPublisher: Send + Sync {
    /// Publish a single domain event.
    async fn publish_domain_event<E>(&self, event: E) -> Result<()>
    where
        E: Send + Sync + 'static;
}

/// Publisher for events destined for external systems.
#[async_trait]
pub trait IntegrationEventPublisher: Send + Sync {
    /// Publish a single integration event.
    async fn publish_integration_event<E>(&self, event: E) -> Result<()>
    where
        E: Send + Sync + 'static;
}

#[cfg(test)]
mod tests {
    use super::*;
    use orcher_common::types::identifiers::{Identifier, TaskId};
    use orcher_common::types::time::Timestamptz;

    // Mock publisher for testing trait implementation
    struct TestPublisher;

    #[async_trait]
    impl EventPublisher for TestPublisher {
        async fn publish_task_event(&self, _event: TaskEvent) -> Result<()> {
            Ok(())
        }

        async fn publish_worker_event(&self, _event: WorkerEvent) -> Result<()> {
            Ok(())
        }

        async fn publish_workflow_event(&self, _event: WorkflowEvent) -> Result<()> {
            Ok(())
        }

        async fn publish_worker_task_event(&self, _event: WorkerTaskEvent) -> Result<()> {
            Ok(())
        }

        async fn publish_execution_event(&self, _event: ExecutionEvent) -> Result<()> {
            Ok(())
        }

        async fn publish_scheduler_event(&self, _event: SchedulerEvent) -> Result<()> {
            Ok(())
        }

        async fn publish_dlq_event(&self, _event: DlqEvent) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_event_publisher_trait() {
        let publisher = TestPublisher;

        let event = TaskEvent::Submitted {
            task_id: TaskId::generate(),
            submitted_at: Timestamptz::now(),
            priority: 5,
        };

        let result = publisher.publish_task_event(event).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_batch_publishing_default_impl() {
        let publisher = TestPublisher;

        let events = vec![
            TaskEvent::Submitted {
                task_id: TaskId::generate(),
                submitted_at: Timestamptz::now(),
                priority: 5,
            },
            TaskEvent::Submitted {
                task_id: TaskId::generate(),
                submitted_at: Timestamptz::now(),
                priority: 8,
            },
        ];

        let result = publisher.publish_task_events_batch(events).await;
        assert!(result.is_ok());
    }
}
