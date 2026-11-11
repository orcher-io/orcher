//! gRPC streaming utilities.

use orcher_events::prelude::EventBus;
use std::sync::Arc;

/// Event stream manager for converting domain events to gRPC streams
///
/// Subscribes to the internal EventBus and provides gRPC-compatible
/// streams of task and worker events.
pub struct EventStreamManager {
    /// Internal event bus for subscribing to domain events
    event_bus: Arc<EventBus>,
}

impl EventStreamManager {
    /// Create a new EventStreamManager
    ///
    /// # Arguments
    /// * `event_bus` - The internal event bus to subscribe to
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use orcher_core::grpc::streaming::EventStreamManager;
    /// use orcher_core::events::EventBus;
    /// use std::sync::Arc;
    ///
    /// let event_bus = EventBus::with_defaults();
    /// let manager = EventStreamManager::new(event_bus);
    /// ```
    pub fn new(event_bus: Arc<EventBus>) -> Self {
        Self { event_bus }
    }

    /// Create a new EventStreamManager with custom buffer size
    ///
    /// # Arguments
    /// * `event_bus` - The internal event bus to subscribe to
    /// * `_buffer_size` - Maximum number of events to buffer (reserved for future use)
    pub fn with_buffer_size(event_bus: Arc<EventBus>, _buffer_size: usize) -> Self {
        Self { event_bus }
    }

    /// Get a reference to the event bus
    ///
    /// # Returns
    /// Reference to the internal event bus
    pub fn event_bus(&self) -> Arc<EventBus> {
        self.event_bus.clone()
    }

    // REMOVED: stream_task_events() - used orchestrator proto types (removed)
    // REMOVED: stream_worker_events() - used orchestrator proto types (removed)
}

// ================================================================================================
// REMOVED: HELPER FUNCTIONS (OrchestratorService proto removed)
// ================================================================================================
// REMOVED: extract_task_id_from_event() - only used by stream_task_events
// REMOVED: extract_worker_id_from_event() - only used by stream_worker_events
// REMOVED: convert_task_event_to_proto() - converted to orchestrator proto types
// REMOVED: convert_worker_event_to_proto() - converted to orchestrator proto types
// REMOVED: convert_datetime_to_proto() - only used by above conversion functions

#[cfg(test)]
mod tests {
    use super::*;
    use orcher_events::prelude::EventBus;

    #[test]
    fn test_stream_manager_creation() {
        let event_bus = EventBus::with_defaults();
        let _manager = EventStreamManager::new(event_bus);
        // Manager created successfully
    }

    #[test]
    fn test_stream_manager_with_custom_buffer() {
        let event_bus = EventBus::with_defaults();
        let _manager = EventStreamManager::with_buffer_size(event_bus, 500);
        // Manager created successfully with custom buffer size
    }

    // REMOVED: test_extract_task_id_from_event - extract_task_id_from_event removed
    // REMOVED: test_extract_worker_id_from_event - extract_worker_id_from_event removed
    // REMOVED: test_task_event_conversion - convert_task_event_to_proto removed
    // REMOVED: test_worker_event_conversion - convert_worker_event_to_proto removed

    // REMOVED: test_convert_datetime_to_proto - convert_datetime_to_proto removed
    // REMOVED: test_stream_task_events_creation - stream_task_events method removed
    // REMOVED: test_stream_worker_events_creation - stream_worker_events method removed
}
