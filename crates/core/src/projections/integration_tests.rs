//! Projection integration tests.

#[cfg(test)]
mod tests {
    use crate::events::{EventBridge, InMemoryEventStore};
    use orcher_common::types::identifiers::{TaskId, WorkerId};
    use orcher_common::types::time::Timestamptz;
    use orcher_events::prelude::{EventBus, TaskEvent, WorkerEvent};
    use orcher_events::publisher::EventPublisher;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_task_projection_with_event_bridge() {
        // Setup
        let event_bus = Arc::new(EventBus::with_defaults());
        let event_store = Arc::new(InMemoryEventStore::new());
        let (event_bridge, _worker_handle) =
            EventBridge::with_defaults_and_background(Arc::clone(&event_bus), event_store.clone());
        let event_bridge = Arc::new(event_bridge);

        // Register task projection
        event_bridge
            .register_projection("task_state".to_string())
            .await;

        // Publish task events
        let task_id = TaskId::new_v4();

        // Submit task
        event_bridge
            .publish_task_event(TaskEvent::Submitted {
                task_id,
                submitted_at: Timestamptz::now(),
                priority: 5,
            })
            .await
            .unwrap();

        // Assign task
        let worker_id = WorkerId::new_v4();
        event_bridge
            .publish_task_event(TaskEvent::Assigned {
                task_id,
                worker_id,
                assigned_at: Timestamptz::now(),
            })
            .await
            .unwrap();

        // Complete task
        event_bridge
            .publish_task_event(TaskEvent::Completed {
                task_id,
                completed_at: Timestamptz::now(),
                result: orcher_events::prelude::TaskResult::success(None, None),
            })
            .await
            .unwrap();

        // Give time for background persistence
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Verify events were persisted
        let event_count = event_store.total_event_count().await;
        assert!(
            event_count >= 3,
            "Expected at least 3 events, found {}",
            event_count
        );

        // Verify projection count
        assert_eq!(event_bridge.projection_count().await, 1);
    }

    #[tokio::test]
    async fn test_worker_projection_with_event_bridge() {
        // Setup
        let event_bus = Arc::new(EventBus::with_defaults());
        let event_store = Arc::new(InMemoryEventStore::new());
        let (event_bridge, _worker_handle) =
            EventBridge::with_defaults_and_background(Arc::clone(&event_bus), event_store.clone());
        let event_bridge = Arc::new(event_bridge);

        // Register worker projection
        event_bridge
            .register_projection("worker_state".to_string())
            .await;

        // Publish worker events
        let worker_id = WorkerId::new_v4();

        // Register worker
        event_bridge
            .publish_worker_event(WorkerEvent::Registered {
                worker_id,
                capabilities: vec!["compute".to_string()],
                registered_at: Timestamptz::now(),
            })
            .await
            .unwrap();

        // Worker heartbeat
        event_bridge
            .publish_worker_event(WorkerEvent::HeartbeatReceived {
                worker_id,
                load: 0.5,
                timestamp: Timestamptz::now(),
            })
            .await
            .unwrap();

        // Give time for background persistence
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Verify events were persisted
        let event_count = event_store.total_event_count().await;
        assert!(
            event_count >= 2,
            "Expected at least 2 events, found {}",
            event_count
        );

        // Verify projection count
        assert_eq!(event_bridge.projection_count().await, 1);
    }

    #[tokio::test]
    async fn test_multiple_projections_with_event_bridge() {
        // Setup
        let event_bus = Arc::new(EventBus::with_defaults());
        let event_store = Arc::new(InMemoryEventStore::new());
        let (event_bridge, _worker_handle) =
            EventBridge::with_defaults_and_background(Arc::clone(&event_bus), event_store.clone());
        let event_bridge = Arc::new(event_bridge);

        // Register multiple projections
        event_bridge
            .register_projection("task_state".to_string())
            .await;
        event_bridge
            .register_projection("worker_state".to_string())
            .await;
        event_bridge
            .register_projection("task_statistics".to_string())
            .await;

        // Verify projection count
        assert_eq!(event_bridge.projection_count().await, 3);

        // Publish mixed events
        let task_id = TaskId::new_v4();
        let worker_id = WorkerId::new_v4();

        event_bridge
            .publish_task_event(TaskEvent::Submitted {
                task_id,
                submitted_at: Timestamptz::now(),
                priority: 5,
            })
            .await
            .unwrap();

        event_bridge
            .publish_worker_event(WorkerEvent::Registered {
                worker_id,
                capabilities: vec!["compute".to_string()],
                registered_at: Timestamptz::now(),
            })
            .await
            .unwrap();

        event_bridge
            .publish_task_event(TaskEvent::Assigned {
                task_id,
                worker_id,
                assigned_at: Timestamptz::now(),
            })
            .await
            .unwrap();

        // Give time for background persistence
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Verify events were persisted
        let event_count = event_store.total_event_count().await;
        assert!(
            event_count >= 3,
            "Expected at least 3 events, found {}",
            event_count
        );
    }

    #[tokio::test]
    async fn test_projection_clear() {
        // Setup
        let event_bus = Arc::new(EventBus::with_defaults());
        let event_store = Arc::new(InMemoryEventStore::new());
        let (event_bridge, _worker_handle) =
            EventBridge::with_defaults_and_background(Arc::clone(&event_bus), event_store);
        let event_bridge = Arc::new(event_bridge);

        // Register projections
        event_bridge
            .register_projection("projection1".to_string())
            .await;
        event_bridge
            .register_projection("projection2".to_string())
            .await;

        assert_eq!(event_bridge.projection_count().await, 2);

        // Clear projections
        event_bridge.clear_projections().await;

        assert_eq!(event_bridge.projection_count().await, 0);
    }

    #[tokio::test]
    async fn test_event_ordering_preserved() {
        // Setup
        let event_bus = Arc::new(EventBus::with_defaults());
        let event_store = Arc::new(InMemoryEventStore::new());
        let (event_bridge, _worker_handle) =
            EventBridge::with_defaults_and_background(Arc::clone(&event_bus), event_store.clone());
        let event_bridge = Arc::new(event_bridge);

        // Publish multiple events in sequence
        let task_id = TaskId::new_v4();

        event_bridge
            .publish_task_event(TaskEvent::Submitted {
                task_id,
                submitted_at: Timestamptz::now(),
                priority: 5,
            })
            .await
            .unwrap();

        event_bridge
            .publish_task_event(TaskEvent::Assigned {
                task_id,
                worker_id: WorkerId::new_v4(),
                assigned_at: Timestamptz::now(),
            })
            .await
            .unwrap();

        event_bridge
            .publish_task_event(TaskEvent::Started {
                task_id,
                worker_id: WorkerId::new_v4(),
                started_at: Timestamptz::now(),
            })
            .await
            .unwrap();

        event_bridge
            .publish_task_event(TaskEvent::Completed {
                task_id,
                completed_at: Timestamptz::now(),
                result: orcher_events::prelude::TaskResult::success(None, None),
            })
            .await
            .unwrap();

        // Give time for background persistence
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Verify all events were persisted
        let event_count = event_store.total_event_count().await;
        assert_eq!(event_count, 4, "Expected 4 events to be persisted");
    }

    #[tokio::test]
    async fn test_concurrent_event_publishing() {
        // Setup
        let event_bus = Arc::new(EventBus::with_defaults());
        let event_store = Arc::new(InMemoryEventStore::new());
        let (event_bridge, _worker_handle) =
            EventBridge::with_defaults_and_background(Arc::clone(&event_bus), event_store.clone());
        let event_bridge = Arc::new(event_bridge);

        // Spawn multiple concurrent publishers
        let mut handles = vec![];
        for i in 0..10 {
            let bridge = Arc::clone(&event_bridge);
            let handle = tokio::spawn(async move {
                let task_id = TaskId::new_v4();
                bridge
                    .publish_task_event(TaskEvent::Submitted {
                        task_id,
                        submitted_at: Timestamptz::now(),
                        priority: i,
                    })
                    .await
                    .unwrap();
            });
            handles.push(handle);
        }

        // Wait for all publishers
        for handle in handles {
            handle.await.unwrap();
        }

        // Give time for background persistence
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        // Verify all events were persisted
        let event_count = event_store.total_event_count().await;
        assert_eq!(event_count, 10, "Expected 10 events to be persisted");
    }

    #[tokio::test]
    async fn test_projection_with_bus_and_store() {
        // Setup
        let event_bus = Arc::new(EventBus::with_defaults());
        let event_store = Arc::new(InMemoryEventStore::new());
        let (event_bridge, _worker_handle) =
            EventBridge::with_defaults_and_background(Arc::clone(&event_bus), event_store.clone());
        let event_bridge = Arc::new(event_bridge);

        // Subscribe to bus events
        let mut task_receiver = event_bus.subscribe_task_events();

        // Register projection
        event_bridge
            .register_projection("test_projection".to_string())
            .await;

        // Publish event
        let task_id = TaskId::new_v4();
        event_bridge
            .publish_task_event(TaskEvent::Submitted {
                task_id,
                submitted_at: Timestamptz::now(),
                priority: 5,
            })
            .await
            .unwrap();

        // Verify event was published to bus
        let bus_event = tokio::time::timeout(
            tokio::time::Duration::from_millis(100),
            task_receiver.recv(),
        )
        .await
        .expect("Should receive event from bus")
        .expect("Should receive valid event");

        match bus_event {
            TaskEvent::Submitted {
                task_id: received_id,
                ..
            } => {
                assert_eq!(received_id, task_id);
            }
            _ => panic!("Expected TaskEvent::Submitted"),
        }

        // Give time for background persistence
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Verify event was persisted to store
        let event_count = event_store.total_event_count().await;
        assert_eq!(event_count, 1);

        // Verify projection is registered
        assert_eq!(event_bridge.projection_count().await, 1);
    }
}
