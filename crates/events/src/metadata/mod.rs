//! Event tracking and correlation — IDs, context, headers, and timestamps.

pub mod context;
pub mod headers;
pub mod identifiers;
pub mod timestamp;

pub use context::EventContext;
pub use headers::{EventHeaders, HeaderValue};
pub use identifiers::{CausationId, CorrelationId, EventId, StreamId};
pub use timestamp::EventTimestamp;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metadata_types_available() {
        // Verify all types are accessible
        let _event_id = EventId::generate();
        let _correlation_id = CorrelationId::generate();
        let _causation_id = CausationId::generate();
        let _stream_id = StreamId::task("task_123");
        let _context = EventContext::new();
        let _headers = EventHeaders::new();
        let _timestamp = EventTimestamp::now();
    }

    #[test]
    fn test_complete_event_metadata() {
        // Create complete event metadata
        let _event_id = EventId::generate();
        let correlation_id = CorrelationId::generate();
        let stream_id = StreamId::task("task_123");

        let mut headers = EventHeaders::new();
        headers.set("request_id", "req_123");
        headers.set("priority", 5);

        let context = EventContext::new()
            .with_correlation(correlation_id)
            .with_stream(stream_id)
            .with_user_id("user_123")
            .with_tenant_id("tenant_456")
            .with_metadata("source", "api");

        let mut timestamp = EventTimestamp::now();
        timestamp.mark_processed();
        timestamp.mark_published();

        // All metadata is properly constructed
        assert_eq!(context.user_id, Some("user_123".to_string()));
        assert_eq!(headers.len(), 2);
        assert!(timestamp.is_published());
    }

    #[test]
    fn test_event_chain() {
        // Test creating event chains
        let parent = EventContext::new();
        let child = parent.create_child();
        let sibling = parent.create_sibling();

        // Child has causation pointing to parent
        assert_eq!(child.causation_id.unwrap().to_event_id(), parent.event_id);

        // Sibling has same causation as parent
        assert_eq!(sibling.causation_id, parent.causation_id);

        // All have different event IDs
        assert_ne!(child.event_id, parent.event_id);
        assert_ne!(sibling.event_id, parent.event_id);
        assert_ne!(child.event_id, sibling.event_id);
    }
}
