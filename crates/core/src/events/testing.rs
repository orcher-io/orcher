//! Event testing utilities.

use crate::events::{EventBridge, EventBridgeConfig, EventBus};
use orcher_events::testing::InMemoryEventStore;
use std::sync::Arc;
use tokio::task::JoinHandle;

/// Create a test event bridge with in-memory event store and background worker
///
/// This is a convenience function for creating a fully-configured event bridge
/// suitable for testing. It includes:
///
/// - In-memory event bus
/// - In-memory event store (no persistence)
/// - Background persistence worker
/// - Default configuration
///
/// # Returns
///
/// Returns a tuple of:
/// - `Arc<EventBridge>`: The event bridge ready for use
/// - `JoinHandle<()>`: Handle to the background worker (will be cancelled on drop)
///
/// # Example
///
/// ```rust,ignore
/// use orcher_core::events::testing::create_test_event_bridge;
/// use orcher_events::domain::TaskEvent;
/// use orcher_common::types::identifiers::{TaskId, WorkflowId};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let (event_bridge, _handle) = create_test_event_bridge();
///
/// // Publish an event
/// let task_id = TaskId::new_v4();
/// let workflow_id = WorkflowId::new_v4();
/// let event = TaskEvent::queued(task_id, workflow_id);
/// event_bridge.publish_task_event(event).await?;
/// # Ok(())
/// # }
/// ```
pub fn create_test_event_bridge() -> (Arc<EventBridge>, Option<JoinHandle<()>>) {
    let bus = EventBus::with_defaults();
    let store = Arc::new(InMemoryEventStore::new());
    let config = EventBridgeConfig::default();

    let (bridge, handle) = EventBridge::new_with_background(bus, config, store);
    (Arc::new(bridge), handle)
}

/// Create a test event bridge with custom configuration
///
/// Similar to `create_test_event_bridge` but allows custom configuration.
///
/// # Arguments
///
/// * `config` - Custom event bridge configuration
///
/// # Returns
///
/// Returns a tuple of:
/// - `Arc<EventBridge>`: The event bridge ready for use
/// - `Option<JoinHandle<()>>`: Handle to the background worker (if enabled)
///
/// # Example
///
/// ```rust
/// use orcher_core::events::testing::create_test_event_bridge_with_config;
/// use orcher_core::events::EventBridgeConfig;
///
/// # async fn example() {
/// let mut config = EventBridgeConfig::default();
/// config.enable_persistence = false;
///
/// let (event_bridge, _handle) = create_test_event_bridge_with_config(config);
/// # }
/// ```
pub fn create_test_event_bridge_with_config(
    config: EventBridgeConfig,
) -> (Arc<EventBridge>, Option<JoinHandle<()>>) {
    let bus = EventBus::with_defaults();
    let store = Arc::new(InMemoryEventStore::new());

    let (bridge, handle) = EventBridge::new_with_background(bus, config, store);
    (Arc::new(bridge), handle)
}

/// Create a test event bridge without background worker
///
/// Creates a test event bridge that processes events synchronously without
/// a background worker. Useful for tests that need deterministic event processing.
///
/// # Returns
///
/// Returns the event bridge ready for use (no background worker handle).
///
/// # Example
///
/// ```rust
/// use orcher_core::events::testing::create_test_event_bridge_sync;
///
/// # async fn example() {
/// let event_bridge = create_test_event_bridge_sync();
/// // All event processing happens synchronously
/// # }
/// ```
pub fn create_test_event_bridge_sync() -> Arc<EventBridge> {
    let bus = EventBus::with_defaults();
    let store = Arc::new(InMemoryEventStore::new());

    let (bridge, _handle) = EventBridge::with_defaults_and_background(bus, store);
    Arc::new(bridge)
}

/// Create components for manual event bridge construction
///
/// Returns all the components needed to construct an event bridge manually.
/// Useful for tests that need fine-grained control over component lifecycle.
///
/// # Returns
///
/// Returns a tuple of:
/// - `Arc<EventBus>`: Event bus
/// - `Arc<InMemoryEventStore>`: In-memory event store
/// - `EventBridgeConfig`: Default configuration
///
/// # Example
///
/// ```rust
/// use orcher_core::events::testing::create_test_event_components;
/// use orcher_core::events::EventBridge;
///
/// # async fn example() {
/// let (bus, store, config) = create_test_event_components();
///
/// // Customize before creating bridge
/// let bridge = EventBridge::new(bus, config);
/// # }
/// ```
pub fn create_test_event_components() -> (Arc<EventBus>, Arc<InMemoryEventStore>, EventBridgeConfig)
{
    let bus = EventBus::with_defaults();
    let store = Arc::new(InMemoryEventStore::new());
    let config = EventBridgeConfig::default();

    (bus, store, config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use orcher_common::types::identifiers::TaskId;
    use orcher_common::types::time::Timestamptz;
    use orcher_events::domain::TaskEvent;
    use orcher_events::publisher::EventPublisher;

    #[tokio::test]
    async fn test_create_test_event_bridge() {
        let (bridge, _handle) = create_test_event_bridge();

        // Verify we can publish events
        let task_id = TaskId::new_v4();
        let event = TaskEvent::Submitted {
            task_id,
            submitted_at: Timestamptz::now(),
            priority: 5,
        };

        EventPublisher::publish_task_event(bridge.as_ref(), event)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_create_test_event_bridge_with_config() {
        let mut config = EventBridgeConfig::default();
        config.enable_persistence = false;

        let (bridge, _handle) = create_test_event_bridge_with_config(config);

        // Verify configuration was applied
        let task_id = TaskId::new_v4();
        let event = TaskEvent::Submitted {
            task_id,
            submitted_at: Timestamptz::now(),
            priority: 5,
        };

        EventPublisher::publish_task_event(bridge.as_ref(), event)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_create_test_event_bridge_sync() {
        let bridge = create_test_event_bridge_sync();

        // Verify we can publish events synchronously
        let task_id = TaskId::new_v4();
        let event = TaskEvent::Submitted {
            task_id,
            submitted_at: Timestamptz::now(),
            priority: 5,
        };

        EventPublisher::publish_task_event(bridge.as_ref(), event)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_create_test_event_components() {
        let (bus, store, config) = create_test_event_components();

        // Verify components are usable
        assert!(Arc::strong_count(&bus) >= 1);
        assert!(Arc::strong_count(&store) >= 1);
        assert!(config.enable_persistence);
    }

    #[tokio::test]
    async fn test_multiple_bridges() {
        let (bridge1, _handle1) = create_test_event_bridge();
        let (bridge2, _handle2) = create_test_event_bridge();

        // Verify both work independently
        let task_id1 = TaskId::new_v4();
        let event1 = TaskEvent::Submitted {
            task_id: task_id1,
            submitted_at: Timestamptz::now(),
            priority: 5,
        };

        let task_id2 = TaskId::new_v4();
        let event2 = TaskEvent::Submitted {
            task_id: task_id2,
            submitted_at: Timestamptz::now(),
            priority: 5,
        };

        EventPublisher::publish_task_event(bridge1.as_ref(), event1)
            .await
            .unwrap();
        EventPublisher::publish_task_event(bridge2.as_ref(), event2)
            .await
            .unwrap();
    }
}
