//! Per-event context: who, when, why, and how events relate to each other.

use crate::metadata::identifiers::{CausationId, CorrelationId, EventId, StreamId};
use orcher_common::types::time::Timestamptz;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Metadata carried alongside an event — IDs, user/tenant info, and custom k/v pairs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventContext {
    pub event_id: EventId,
    pub correlation_id: Option<CorrelationId>,
    pub causation_id: Option<CausationId>,
    pub stream_id: Option<StreamId>,
    pub timestamp: Timestamptz,
    pub user_id: Option<String>,
    pub tenant_id: Option<String>,
    pub session_id: Option<String>,
    pub source: Option<String>,
    pub metadata: HashMap<String, String>,
}

impl EventContext {
    /// Fresh context with a new EventId and timestamp = now.
    pub fn new() -> Self {
        Self {
            event_id: EventId::generate(),
            correlation_id: None,
            causation_id: None,
            stream_id: None,
            timestamp: Timestamptz::now(),
            user_id: None,
            tenant_id: None,
            session_id: None,
            source: None,
            metadata: HashMap::new(),
        }
    }

    pub fn with_event_id(event_id: EventId) -> Self {
        Self {
            event_id,
            correlation_id: None,
            causation_id: None,
            stream_id: None,
            timestamp: Timestamptz::now(),
            user_id: None,
            tenant_id: None,
            session_id: None,
            source: None,
            metadata: HashMap::new(),
        }
    }

    pub fn with_correlation(mut self, correlation_id: CorrelationId) -> Self {
        self.correlation_id = Some(correlation_id);
        self
    }

    pub fn with_causation(mut self, causation_id: CausationId) -> Self {
        self.causation_id = Some(causation_id);
        self
    }

    pub fn with_stream(mut self, stream_id: StreamId) -> Self {
        self.stream_id = Some(stream_id);
        self
    }

    pub fn with_user_id(mut self, user_id: impl Into<String>) -> Self {
        self.user_id = Some(user_id.into());
        self
    }

    pub fn with_tenant_id(mut self, tenant_id: impl Into<String>) -> Self {
        self.tenant_id = Some(tenant_id.into());
        self
    }

    pub fn with_session_id(mut self, session_id: impl Into<String>) -> Self {
        self.session_id = Some(session_id.into());
        self
    }

    pub fn with_source(mut self, source: impl Into<String>) -> Self {
        self.source = Some(source.into());
        self
    }

    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    pub fn with_timestamp(mut self, timestamp: Timestamptz) -> Self {
        self.timestamp = timestamp;
        self
    }

    pub fn add_metadata(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.metadata.insert(key.into(), value.into());
    }

    pub fn get_metadata(&self, key: &str) -> Option<&String> {
        self.metadata.get(key)
    }

    pub fn remove_metadata(&mut self, key: &str) -> Option<String> {
        self.metadata.remove(key)
    }

    pub fn has_metadata(&self, key: &str) -> bool {
        self.metadata.contains_key(key)
    }

    /// New context whose causation points back to this event. Preserves correlation.
    pub fn create_child(&self) -> Self {
        let new_event_id = EventId::generate();
        let causation_id = CausationId::from_event_id(self.event_id);

        Self {
            event_id: new_event_id,
            correlation_id: self.correlation_id,
            causation_id: Some(causation_id),
            stream_id: self.stream_id.clone(),
            timestamp: Timestamptz::now(),
            user_id: self.user_id.clone(),
            tenant_id: self.tenant_id.clone(),
            session_id: self.session_id.clone(),
            source: self.source.clone(),
            metadata: self.metadata.clone(),
        }
    }

    /// New context with the same correlation and causation — for parallel/sibling events.
    pub fn create_sibling(&self) -> Self {
        Self {
            event_id: EventId::generate(),
            correlation_id: self.correlation_id,
            causation_id: self.causation_id,
            stream_id: self.stream_id.clone(),
            timestamp: Timestamptz::now(),
            user_id: self.user_id.clone(),
            tenant_id: self.tenant_id.clone(),
            session_id: self.session_id.clone(),
            source: self.source.clone(),
            metadata: self.metadata.clone(),
        }
    }
}

impl Default for EventContext {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_context_creation() {
        let context = EventContext::new();
        assert!(!context.event_id.is_nil());
        assert!(context.correlation_id.is_none());
        assert!(context.causation_id.is_none());
    }

    #[test]
    fn test_context_with_event_id() {
        let event_id = EventId::generate();
        let context = EventContext::with_event_id(event_id);
        assert_eq!(context.event_id, event_id);
    }

    #[test]
    fn test_context_with_correlation() {
        let correlation_id = CorrelationId::generate();
        let context = EventContext::new().with_correlation(correlation_id);
        assert_eq!(context.correlation_id, Some(correlation_id));
    }

    #[test]
    fn test_context_with_causation() {
        let causation_id = CausationId::generate();
        let context = EventContext::new().with_causation(causation_id);
        assert_eq!(context.causation_id, Some(causation_id));
    }

    #[test]
    fn test_context_with_stream() {
        let stream_id = StreamId::task("task_123");
        let context = EventContext::new().with_stream(stream_id.clone());
        assert_eq!(context.stream_id, Some(stream_id));
    }

    #[test]
    fn test_context_with_user() {
        let context = EventContext::new().with_user_id("user_123");
        assert_eq!(context.user_id, Some("user_123".to_string()));
    }

    #[test]
    fn test_context_with_tenant() {
        let context = EventContext::new().with_tenant_id("tenant_456");
        assert_eq!(context.tenant_id, Some("tenant_456".to_string()));
    }

    #[test]
    fn test_context_with_session() {
        let context = EventContext::new().with_session_id("session_789");
        assert_eq!(context.session_id, Some("session_789".to_string()));
    }

    #[test]
    fn test_context_with_source() {
        let context = EventContext::new().with_source("api");
        assert_eq!(context.source, Some("api".to_string()));
    }

    #[test]
    fn test_context_with_metadata() {
        let context = EventContext::new()
            .with_metadata("key1", "value1")
            .with_metadata("key2", "value2");

        assert_eq!(context.get_metadata("key1"), Some(&"value1".to_string()));
        assert_eq!(context.get_metadata("key2"), Some(&"value2".to_string()));
    }

    #[test]
    fn test_context_add_metadata() {
        let mut context = EventContext::new();
        context.add_metadata("key", "value");
        assert_eq!(context.get_metadata("key"), Some(&"value".to_string()));
    }

    #[test]
    fn test_context_has_metadata() {
        let context = EventContext::new().with_metadata("key", "value");
        assert!(context.has_metadata("key"));
        assert!(!context.has_metadata("missing"));
    }

    #[test]
    fn test_context_remove_metadata() {
        let mut context = EventContext::new().with_metadata("key", "value");
        let removed = context.remove_metadata("key");
        assert_eq!(removed, Some("value".to_string()));
        assert!(!context.has_metadata("key"));
    }

    #[test]
    fn test_context_create_child() {
        let parent = EventContext::new().with_user_id("user_123");
        let child = parent.create_child();

        // Different event ID
        assert_ne!(child.event_id, parent.event_id);

        // Causation points to parent
        assert_eq!(child.causation_id.unwrap().to_event_id(), parent.event_id);

        // User context preserved
        assert_eq!(child.user_id, parent.user_id);
    }

    #[test]
    fn test_context_create_child_with_correlation() {
        let correlation_id = CorrelationId::generate();
        let parent = EventContext::new().with_correlation(correlation_id);
        let child = parent.create_child();

        // Same correlation
        assert_eq!(child.correlation_id, Some(correlation_id));
    }

    #[test]
    fn test_context_create_sibling() {
        let causation_id = CausationId::generate();
        let context1 = EventContext::new().with_causation(causation_id);
        let context2 = context1.create_sibling();

        // Different event IDs
        assert_ne!(context1.event_id, context2.event_id);

        // Same causation
        assert_eq!(context1.causation_id, context2.causation_id);
    }

    #[test]
    fn test_context_builder_pattern() {
        let context = EventContext::new()
            .with_user_id("user_123")
            .with_tenant_id("tenant_456")
            .with_session_id("session_789")
            .with_source("api")
            .with_metadata("env", "prod");

        assert_eq!(context.user_id, Some("user_123".to_string()));
        assert_eq!(context.tenant_id, Some("tenant_456".to_string()));
        assert_eq!(context.session_id, Some("session_789".to_string()));
        assert_eq!(context.source, Some("api".to_string()));
        assert_eq!(context.get_metadata("env"), Some(&"prod".to_string()));
    }

    #[test]
    fn test_context_serialization() {
        let context = EventContext::new()
            .with_user_id("user_123")
            .with_metadata("key", "value");

        let json = serde_json::to_string(&context).unwrap();
        let deserialized: EventContext = serde_json::from_str(&json).unwrap();

        assert_eq!(context.event_id, deserialized.event_id);
        assert_eq!(context.user_id, deserialized.user_id);
        assert_eq!(context.metadata, deserialized.metadata);
    }
}
