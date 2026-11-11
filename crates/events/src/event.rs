//! Serialized event representation for storage and replay.

use orcher_common::types::time::Timestamptz;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A serialized event ready for persistence.
///
/// This is the wire/storage format — domain events get serialized into this
/// before being written to the event store.
///
/// ```rust
/// use orcher_events::Event;
/// use serde_json::json;
///
/// let event = Event::new(
///     "TaskSubmitted".to_string(),
///     json!({"task_id": "123", "priority": 5})
/// );
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    /// e.g. "TaskSubmitted", "WorkflowCompleted"
    pub event_type: String,

    /// The actual payload, serialized as JSON.
    pub data: serde_json::Value,

    /// Arbitrary key-value pairs (correlation IDs, trace context, etc).
    #[serde(default)]
    pub metadata: HashMap<String, String>,

    /// When the event happened (business time, not storage time).
    #[serde(default = "Timestamptz::now")]
    pub timestamp: Timestamptz,
}

impl Event {
    /// Build a new event with the given type and JSON payload.
    pub fn new(event_type: String, data: serde_json::Value) -> Self {
        Self {
            event_type,
            data,
            metadata: HashMap::new(),
            timestamp: Timestamptz::now(),
        }
    }

    /// Attach a single metadata key-value pair.
    pub fn with_metadata_item(mut self, key: String, value: String) -> Self {
        self.metadata.insert(key, value);
        self
    }

    /// Replace all metadata at once.
    pub fn with_metadata(mut self, metadata: HashMap<String, String>) -> Self {
        self.metadata = metadata;
        self
    }

    /// Override the default timestamp.
    pub fn with_timestamp(mut self, timestamp: Timestamptz) -> Self {
        self.timestamp = timestamp;
        self
    }

    /// Tag this event with a correlation ID (stored in metadata).
    pub fn with_correlation_id(mut self, correlation_id: String) -> Self {
        self.metadata
            .insert("correlation_id".to_string(), correlation_id);
        self
    }

    /// Tag this event with a causation ID (stored in metadata).
    pub fn with_causation_id(mut self, causation_id: String) -> Self {
        self.metadata
            .insert("causation_id".to_string(), causation_id);
        self
    }

    /// Read back the correlation ID, if one was set.
    pub fn correlation_id(&self) -> Option<&str> {
        self.metadata.get("correlation_id").map(|s| s.as_str())
    }

    /// Read back the causation ID, if one was set.
    pub fn causation_id(&self) -> Option<&str> {
        self.metadata.get("causation_id").map(|s| s.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_event_creation() {
        let event = Event::new("TestEvent".to_string(), json!({"key": "value"}));

        assert_eq!(event.event_type, "TestEvent");
        assert_eq!(event.data, json!({"key": "value"}));
        assert!(event.metadata.is_empty());
    }

    #[test]
    fn test_event_with_metadata_item() {
        let event = Event::new("TestEvent".to_string(), json!({}))
            .with_metadata_item("key1".to_string(), "value1".to_string())
            .with_metadata_item("key2".to_string(), "value2".to_string());

        assert_eq!(event.metadata.len(), 2);
        assert_eq!(event.metadata.get("key1").unwrap(), "value1");
        assert_eq!(event.metadata.get("key2").unwrap(), "value2");
    }

    #[test]
    fn test_event_with_metadata() {
        let mut metadata = HashMap::new();
        metadata.insert("key1".to_string(), "value1".to_string());
        metadata.insert("key2".to_string(), "value2".to_string());

        let event = Event::new("TestEvent".to_string(), json!({})).with_metadata(metadata);

        assert_eq!(event.metadata.len(), 2);
    }

    #[test]
    fn test_event_with_timestamp() {
        let timestamp = Timestamptz::now();
        let event = Event::new("TestEvent".to_string(), json!({})).with_timestamp(timestamp);

        assert_eq!(event.timestamp, timestamp);
    }

    #[test]
    fn test_event_with_correlation_id() {
        let event = Event::new("TestEvent".to_string(), json!({}))
            .with_correlation_id("corr-123".to_string());

        assert_eq!(event.correlation_id(), Some("corr-123"));
        assert_eq!(event.metadata.get("correlation_id").unwrap(), "corr-123");
    }

    #[test]
    fn test_event_with_causation_id() {
        let event = Event::new("TestEvent".to_string(), json!({}))
            .with_causation_id("cause-123".to_string());

        assert_eq!(event.causation_id(), Some("cause-123"));
        assert_eq!(event.metadata.get("causation_id").unwrap(), "cause-123");
    }

    #[test]
    fn test_event_serialization() {
        let event = Event::new("TestEvent".to_string(), json!({"key": "value"}))
            .with_metadata_item("meta_key".to_string(), "meta_value".to_string());

        let json = serde_json::to_string(&event).unwrap();
        let deserialized: Event = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.event_type, event.event_type);
        assert_eq!(deserialized.data, event.data);
        assert_eq!(deserialized.metadata, event.metadata);
    }

    #[test]
    fn test_event_default_timestamp() {
        let event = Event::new("TestEvent".to_string(), json!({}));
        let now = Timestamptz::now();

        // Timestamp should be close to now (within 1 second)
        let diff = (event.timestamp - now).as_secs().abs();
        assert!(diff < 1);
    }
}
