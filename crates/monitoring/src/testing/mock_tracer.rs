//! Mock tracer for testing distributed tracing
//!
//! This module provides a mock tracer that captures spans and traces
//! for testing purposes without requiring external tracing infrastructure.

use orcher_common::uuid::Uuid;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Mock tracer for capturing traces in tests
#[derive(Debug, Clone)]
pub struct MockTracer {
    inner: Arc<RwLock<MockTracerInner>>,
}

#[derive(Debug, Default)]
struct MockTracerInner {
    spans: Vec<MockSpan>,
    active_spans: HashMap<String, usize>,
}

/// A captured span from the mock tracer
#[derive(Debug, Clone)]
pub struct MockSpan {
    /// Span ID
    pub span_id: String,
    /// Parent span ID if this is a child span
    pub parent_id: Option<String>,
    /// Span name
    pub name: String,
    /// Span attributes/tags
    pub attributes: HashMap<String, String>,
    /// Span events
    pub events: Vec<MockSpanEvent>,
    /// Span start time
    pub start_time: Instant,
    /// Span end time
    pub end_time: Option<Instant>,
    /// Span status
    pub status: SpanStatus,
}

/// Span event captured during tracing
#[derive(Debug, Clone)]
pub struct MockSpanEvent {
    /// Event name
    pub name: String,
    /// Event attributes
    pub attributes: HashMap<String, String>,
    /// Event timestamp
    pub timestamp: Instant,
}

/// Span status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpanStatus {
    /// Span is still active
    Active,
    /// Span completed successfully
    Ok,
    /// Span completed with an error
    Error,
}

impl MockTracer {
    /// Create a new mock tracer
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(MockTracerInner::default())),
        }
    }

    /// Start a new span
    pub fn start_span(&self, name: impl Into<String>) -> String {
        let span_id = format!("span-{}", Uuid::new_v4());
        let span = MockSpan {
            span_id: span_id.clone(),
            parent_id: None,
            name: name.into(),
            attributes: HashMap::new(),
            events: Vec::new(),
            start_time: Instant::now(),
            end_time: None,
            status: SpanStatus::Active,
        };

        let mut inner = self.inner.write();
        let index = inner.spans.len();
        inner.spans.push(span);
        inner.active_spans.insert(span_id.clone(), index);

        span_id
    }

    /// Start a child span
    pub fn start_child_span(
        &self,
        parent_id: impl Into<String>,
        name: impl Into<String>,
    ) -> String {
        let span_id = format!("span-{}", Uuid::new_v4());
        let span = MockSpan {
            span_id: span_id.clone(),
            parent_id: Some(parent_id.into()),
            name: name.into(),
            attributes: HashMap::new(),
            events: Vec::new(),
            start_time: Instant::now(),
            end_time: None,
            status: SpanStatus::Active,
        };

        let mut inner = self.inner.write();
        let index = inner.spans.len();
        inner.spans.push(span);
        inner.active_spans.insert(span_id.clone(), index);

        span_id
    }

    /// Add an attribute to a span
    pub fn add_attribute(&self, span_id: &str, key: impl Into<String>, value: impl Into<String>) {
        let mut inner = self.inner.write();
        if let Some(&index) = inner.active_spans.get(span_id) {
            if let Some(span) = inner.spans.get_mut(index) {
                span.attributes.insert(key.into(), value.into());
            }
        }
    }

    /// Add an event to a span
    pub fn add_event(&self, span_id: &str, name: impl Into<String>) {
        let mut inner = self.inner.write();
        if let Some(&index) = inner.active_spans.get(span_id) {
            if let Some(span) = inner.spans.get_mut(index) {
                span.events.push(MockSpanEvent {
                    name: name.into(),
                    attributes: HashMap::new(),
                    timestamp: Instant::now(),
                });
            }
        }
    }

    /// End a span successfully
    pub fn end_span(&self, span_id: &str) {
        let mut inner = self.inner.write();
        if let Some(&index) = inner.active_spans.get(span_id) {
            if let Some(span) = inner.spans.get_mut(index) {
                span.end_time = Some(Instant::now());
                span.status = SpanStatus::Ok;
            }
            inner.active_spans.remove(span_id);
        }
    }

    /// End a span with an error
    pub fn end_span_with_error(&self, span_id: &str) {
        let mut inner = self.inner.write();
        if let Some(&index) = inner.active_spans.get(span_id) {
            if let Some(span) = inner.spans.get_mut(index) {
                span.end_time = Some(Instant::now());
                span.status = SpanStatus::Error;
            }
            inner.active_spans.remove(span_id);
        }
    }

    /// Get a span by ID
    pub fn get_span(&self, span_id: &str) -> Option<MockSpan> {
        let inner = self.inner.read();
        inner.spans.iter().find(|s| s.span_id == span_id).cloned()
    }

    /// Get all spans
    pub fn get_all_spans(&self) -> Vec<MockSpan> {
        let inner = self.inner.read();
        inner.spans.clone()
    }

    /// Get spans by name
    pub fn get_spans_by_name(&self, name: &str) -> Vec<MockSpan> {
        let inner = self.inner.read();
        inner
            .spans
            .iter()
            .filter(|s| s.name == name)
            .cloned()
            .collect()
    }

    /// Get child spans of a parent
    pub fn get_child_spans(&self, parent_id: &str) -> Vec<MockSpan> {
        let inner = self.inner.read();
        inner
            .spans
            .iter()
            .filter(|s| s.parent_id.as_deref() == Some(parent_id))
            .cloned()
            .collect()
    }

    /// Get the number of active spans
    pub fn active_span_count(&self) -> usize {
        let inner = self.inner.read();
        inner.active_spans.len()
    }

    /// Get the total number of spans
    pub fn total_span_count(&self) -> usize {
        let inner = self.inner.read();
        inner.spans.len()
    }

    /// Clear all recorded spans
    pub fn clear(&self) {
        let mut inner = self.inner.write();
        inner.spans.clear();
        inner.active_spans.clear();
    }
}

impl Default for MockTracer {
    fn default() -> Self {
        Self::new()
    }
}

impl MockSpan {
    /// Get the span duration
    pub fn duration(&self) -> Option<Duration> {
        self.end_time.map(|end| end.duration_since(self.start_time))
    }

    /// Check if the span is completed
    pub fn is_completed(&self) -> bool {
        self.end_time.is_some()
    }

    /// Check if the span has an error
    pub fn has_error(&self) -> bool {
        self.status == SpanStatus::Error
    }

    /// Get an attribute value
    pub fn get_attribute(&self, key: &str) -> Option<&str> {
        self.attributes.get(key).map(|s| s.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_start_and_end_span() {
        let tracer = MockTracer::new();

        let span_id = tracer.start_span("test_span");
        assert_eq!(tracer.active_span_count(), 1);

        tracer.end_span(&span_id);
        assert_eq!(tracer.active_span_count(), 0);

        let span = tracer.get_span(&span_id).unwrap();
        assert_eq!(span.name, "test_span");
        assert_eq!(span.status, SpanStatus::Ok);
        assert!(span.is_completed());
    }

    #[test]
    fn test_child_spans() {
        let tracer = MockTracer::new();

        let parent_id = tracer.start_span("parent");
        let child_id = tracer.start_child_span(&parent_id, "child");

        assert_eq!(tracer.total_span_count(), 2);

        let child = tracer.get_span(&child_id).unwrap();
        assert_eq!(child.parent_id.as_deref(), Some(parent_id.as_str()));

        let children = tracer.get_child_spans(&parent_id);
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].span_id, child_id);
    }

    #[test]
    fn test_span_attributes() {
        let tracer = MockTracer::new();

        let span_id = tracer.start_span("test");
        tracer.add_attribute(&span_id, "key1", "value1");
        tracer.add_attribute(&span_id, "key2", "value2");

        let span = tracer.get_span(&span_id).unwrap();
        assert_eq!(span.get_attribute("key1"), Some("value1"));
        assert_eq!(span.get_attribute("key2"), Some("value2"));
    }

    #[test]
    fn test_span_events() {
        let tracer = MockTracer::new();

        let span_id = tracer.start_span("test");
        tracer.add_event(&span_id, "event1");
        tracer.add_event(&span_id, "event2");

        let span = tracer.get_span(&span_id).unwrap();
        assert_eq!(span.events.len(), 2);
        assert_eq!(span.events[0].name, "event1");
        assert_eq!(span.events[1].name, "event2");
    }

    #[test]
    fn test_span_with_error() {
        let tracer = MockTracer::new();

        let span_id = tracer.start_span("test");
        tracer.end_span_with_error(&span_id);

        let span = tracer.get_span(&span_id).unwrap();
        assert_eq!(span.status, SpanStatus::Error);
        assert!(span.has_error());
    }

    #[test]
    fn test_get_spans_by_name() {
        let tracer = MockTracer::new();

        tracer.start_span("span1");
        tracer.start_span("span2");
        tracer.start_span("span1");

        let spans = tracer.get_spans_by_name("span1");
        assert_eq!(spans.len(), 2);
    }

    #[test]
    fn test_clear() {
        let tracer = MockTracer::new();

        tracer.start_span("span1");
        tracer.start_span("span2");

        assert_eq!(tracer.total_span_count(), 2);

        tracer.clear();

        assert_eq!(tracer.total_span_count(), 0);
        assert_eq!(tracer.active_span_count(), 0);
    }
}
