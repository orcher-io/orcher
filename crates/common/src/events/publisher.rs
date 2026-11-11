//! Event publisher trait for worker event emission.
//!
//! # Design
//!
//! This trait enables **dependency inversion**: both worker and core
//! depend on this abstraction in orcher-common, eliminating the need
//! for worker to directly depend on core.
//!
//! # Implementations
//!
//! - **EventBridge** (orcher-core): In-process publishing to event bus and store
//! - **GrpcEventPublisher** (orcher-worker): Remote publishing via gRPC
//! - **QueueEventPublisher** (orcher-worker): Queue-based publishing
//! - **MockEventPublisher** (tests): Testing implementation
//!
//! # Example
//!
//! ```rust,ignore
//! use orcher_common::events::{EventPublisher, WorkerTaskEvent};
//! use orcher_common::types::identifiers::{TaskId, WorkerId, Identifier};
//! use std::sync::Arc;
//!
//! // In core: EventBridge implements EventPublisher
//! let event_bridge = Arc::new(EventBridge::new(...));
//! let publisher: Arc<dyn EventPublisher> = event_bridge;
//!
//! // Publish event
//! let event = WorkerTaskEvent::execution_started(
//!     TaskId::generate(),
//!     WorkerId::generate(),
//! );
//! publisher.publish_task_event(event).await?;
//! ```

use super::WorkerTaskEvent;
use crate::OrcherResult;
use async_trait::async_trait;

#[async_trait]
pub trait EventPublisher: Send + Sync {
    async fn publish_task_event(&self, event: WorkerTaskEvent) -> OrcherResult<()>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::identifiers::{Identifier, TaskId, WorkerId};
    use std::sync::{Arc, Mutex};

    /// Mock EventPublisher for testing
    #[derive(Clone)]
    struct MockEventPublisher {
        events: Arc<Mutex<Vec<WorkerTaskEvent>>>,
    }

    impl MockEventPublisher {
        fn new() -> Self {
            Self {
                events: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn published_events(&self) -> Vec<WorkerTaskEvent> {
            self.events.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl EventPublisher for MockEventPublisher {
        async fn publish_task_event(&self, event: WorkerTaskEvent) -> OrcherResult<()> {
            self.events.lock().unwrap().push(event);
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_mock_publisher() {
        let mock = MockEventPublisher::new();
        let task_id = TaskId::generate();
        let worker_id = WorkerId::generate();

        let event = WorkerTaskEvent::execution_started(task_id, worker_id);
        mock.publish_task_event(event.clone()).await.unwrap();

        let published = mock.published_events();
        assert_eq!(published.len(), 1);
        assert_eq!(published[0], event);
    }

    #[tokio::test]
    async fn test_mock_publisher_multiple_events() {
        let mock = MockEventPublisher::new();
        let task_id = TaskId::generate();
        let worker_id = WorkerId::generate();

        // Publish multiple events
        mock.publish_task_event(WorkerTaskEvent::execution_started(task_id, worker_id))
            .await
            .unwrap();

        mock.publish_task_event(WorkerTaskEvent::execution_completed(
            task_id,
            worker_id,
            serde_json::json!({"result": "success"}),
            1000,
        ))
        .await
        .unwrap();

        let published = mock.published_events();
        assert_eq!(published.len(), 2);
    }

    #[tokio::test]
    async fn test_trait_object() {
        let mock = MockEventPublisher::new();
        let publisher: Arc<dyn EventPublisher> = Arc::new(mock.clone());

        let task_id = TaskId::generate();
        let worker_id = WorkerId::generate();
        let event = WorkerTaskEvent::execution_started(task_id, worker_id);

        publisher.publish_task_event(event).await.unwrap();

        let published = mock.published_events();
        assert_eq!(published.len(), 1);
    }
}
