//! Event sourcing and subscriptions.

// =============================================================================
// RE-EXPORTS FROM orcher-events
// =============================================================================

/// EventBridge - Dual-write pattern (bus + storage) with background persistence
///
/// This is an alias to `orcher_events::publisher::BridgePublisher`.
///
/// Features:
/// - Background persistence worker (non-blocking)
/// - Projection management
/// - Retry logic with exponential backoff
/// - Configurable queue size
///
/// Use `new_with_background()` for production (returns worker handle).
pub use orcher_events::publisher::BridgePublisher as EventBridge;

/// Configuration for EventBridge
///
/// This is an alias to `orcher_events::publisher::BridgeConfig`.
pub use orcher_events::publisher::BridgeConfig as EventBridgeConfig;

/// Event bus for in-memory pub/sub
pub use orcher_events::bus::EventBus;

/// Event types
pub use orcher_events::domain::{
    ExecutionEvent, SchedulerEvent, TaskEvent, WorkerEvent, WorkerTaskEvent, WorkflowEvent,
};

/// Event store traits and implementations
pub use orcher_events::replay::{EventStore, StoredEvent};

/// Stream-based event types
pub use orcher_events::replay::{StreamId, StreamPosition};

/// Event publisher trait
pub use orcher_events::publisher::EventPublisher;

/// Testing utilities
///
/// Note: InMemoryEventStore is available for all builds to support
/// testing in downstream crates and examples.
pub use orcher_events::testing::InMemoryEventStore;

// =============================================================================
// ORCHESTRATION-SPECIFIC COMPONENTS
// =============================================================================

mod subscriptions;

pub use subscriptions::{SubscriptionHandle, SubscriptionManager};

// =============================================================================
// TESTING UTILITIES
// =============================================================================

pub mod testing;

// =============================================================================
// TESTS
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_module_exports() {
        // Verify orchestration components are accessible
        let _ = EventBridgeConfig::default();
    }

    #[tokio::test]
    async fn test_event_bridge_creation() {
        use orcher_events::bus::EventBus;
        use std::sync::Arc;

        let bus = Arc::new(EventBus::with_defaults());
        let config = EventBridgeConfig::default();

        // Can create without event store (no persistence)
        let _bridge = EventBridge::new(Arc::clone(&bus), config);
    }

    #[tokio::test]
    async fn test_projection_management() {
        use orcher_events::bus::EventBus;
        use std::sync::Arc;

        let bus = Arc::new(EventBus::with_defaults());
        let bridge = EventBridge::with_defaults(Arc::clone(&bus));

        // Test projection management
        bridge
            .register_projection("test_projection".to_string())
            .await;
        assert_eq!(bridge.projection_count().await, 1);

        bridge.clear_projections().await;
        assert_eq!(bridge.projection_count().await, 0);
    }
}
