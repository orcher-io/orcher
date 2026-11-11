//! Event store trait and stored event types.

use crate::error::Result;
use crate::metadata::StreamId;

use async_trait::async_trait;
use orcher_common::types::time::Timestamptz;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// An event as persisted in the store, with full metadata for replay and ordering.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredEvent {
    pub event_id: u64,
    pub stream_id: StreamId,
    pub event_type: String,
    pub version: u32,
    /// 1-based, monotonically increasing within the stream.
    pub stream_position: u64,
    pub global_position: u64,
    pub data: serde_json::Value,
    pub metadata: serde_json::Value,
    pub timestamp: Timestamptz,
    pub stored_at: Timestamptz,
    pub correlation_id: Option<String>,
    pub causation_id: Option<String>,
}

impl StoredEvent {
    pub fn new(
        event_id: u64,
        stream_id: StreamId,
        event_type: String,
        data: serde_json::Value,
    ) -> Self {
        Self {
            event_id,
            stream_id,
            event_type,
            version: 1,
            stream_position: 0, // Will be set by store
            global_position: 0, // Will be set by store
            data,
            metadata: serde_json::json!({}),
            timestamp: Timestamptz::now(),
            stored_at: Timestamptz::now(),
            correlation_id: None,
            causation_id: None,
        }
    }

    pub fn with_version(mut self, version: u32) -> Self {
        self.version = version;
        self
    }

    pub fn with_metadata(mut self, metadata: serde_json::Value) -> Self {
        self.metadata = metadata;
        self
    }

    pub fn with_correlation_id(mut self, correlation_id: String) -> Self {
        self.correlation_id = Some(correlation_id);
        self
    }

    pub fn with_causation_id(mut self, causation_id: String) -> Self {
        self.causation_id = Some(causation_id);
        self
    }

    pub fn with_timestamp(mut self, timestamp: Timestamptz) -> Self {
        self.timestamp = timestamp;
        self
    }
}

/// Used for optimistic concurrency control on stream appends.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    Default,
)]
pub enum StreamPosition {
    #[default]
    Start,
    Position(u64),
    /// Append without version check.
    End,
}

impl StreamPosition {
    pub fn as_u64(&self) -> Option<u64> {
        match self {
            StreamPosition::Position(pos) => Some(*pos),
            _ => None,
        }
    }

    pub fn is_start(&self) -> bool {
        matches!(self, StreamPosition::Start)
    }

    pub fn is_end(&self) -> bool {
        matches!(self, StreamPosition::End)
    }
}

impl From<u64> for StreamPosition {
    fn from(pos: u64) -> Self {
        StreamPosition::Position(pos)
    }
}

#[derive(Debug, Clone)]
pub struct EventQuery {
    pub stream_id: Option<StreamId>,
    pub category: Option<String>,
    pub event_types: Vec<String>,
    pub from_position: Option<u64>,
    pub to_position: Option<u64>,
    pub from_timestamp: Option<Timestamptz>,
    pub to_timestamp: Option<Timestamptz>,
    pub limit: Option<usize>,
    pub reverse: bool,
    pub direction: ReadDirection,
}

impl EventQuery {
    pub fn new() -> Self {
        Self {
            stream_id: None,
            category: None,
            event_types: Vec::new(),
            from_position: None,
            to_position: None,
            from_timestamp: None,
            to_timestamp: None,
            limit: None,
            reverse: false,
            direction: ReadDirection::Forward,
        }
    }

    pub fn for_stream(mut self, stream_id: StreamId) -> Self {
        self.stream_id = Some(stream_id);
        self
    }

    pub fn for_category(mut self, category: String) -> Self {
        self.category = Some(category);
        self
    }

    pub fn with_event_types(mut self, event_types: Vec<String>) -> Self {
        self.event_types = event_types;
        self
    }

    pub fn from_position(mut self, position: u64) -> Self {
        self.from_position = Some(position);
        self
    }

    pub fn to_position(mut self, position: u64) -> Self {
        self.to_position = Some(position);
        self
    }

    pub fn from_timestamp(mut self, timestamp: Timestamptz) -> Self {
        self.from_timestamp = Some(timestamp);
        self
    }

    pub fn to_timestamp(mut self, timestamp: Timestamptz) -> Self {
        self.to_timestamp = Some(timestamp);
        self
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn direction(mut self, direction: ReadDirection) -> Self {
        self.direction = direction;
        self
    }
}

impl Default for EventQuery {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadDirection {
    Forward,
    Backward,
}

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub events: Vec<StoredEvent>,
    pub has_more: bool,
}

#[derive(Debug, Clone)]
pub struct AppendResult {
    pub stream_position: u64,
    pub global_position: u64,
    pub events_appended: usize,
}

/// Persistent storage for events, organized by streams with optimistic concurrency.
#[async_trait]
pub trait EventStore: Send + Sync {
    /// Append events to a stream. Returns error on concurrency conflict.
    async fn append_to_stream(
        &self,
        stream_id: &StreamId,
        expected_position: StreamPosition,
        events: Vec<crate::Event>,
    ) -> Result<AppendResult>;

    async fn read_stream(
        &self,
        stream_id: &StreamId,
        from_position: StreamPosition,
        max_count: usize,
    ) -> Result<Vec<StoredEvent>>;

    async fn read_stream_backwards(
        &self,
        stream_id: &StreamId,
        from_position: Option<StreamPosition>,
        max_count: usize,
    ) -> Result<Vec<StoredEvent>>;

    /// Returns 0 if the stream doesn't exist.
    async fn get_stream_position(&self, stream_id: &StreamId) -> Result<u64>;

    async fn stream_exists(&self, stream_id: &StreamId) -> Result<bool>;

    async fn query_events(&self, query: &EventQuery) -> Result<QueryResult>;

    /// Read all events across all streams, ordered by global position.
    async fn read_all(&self, from_position: u64, max_count: usize) -> Result<Vec<StoredEvent>>;

    async fn get_global_position(&self) -> Result<u64>;

    async fn delete_stream(&self, stream_id: &StreamId) -> Result<()>;

    async fn get_event_by_id(&self, event_id: u64) -> Result<Option<StoredEvent>>;

    /// Query all streams matching a category (e.g. "task" matches task-123, task-456).
    async fn query_category(
        &self,
        category: &str,
        from_position: u64,
        max_count: usize,
    ) -> Result<Vec<StoredEvent>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stored_event_creation() {
        let event_id = 123u64;
        let stream_id = StreamId::task("test-123");
        let data = serde_json::json!({"key": "value"});

        let event = StoredEvent::new(
            event_id,
            stream_id.clone(),
            "TestEvent".to_string(),
            data.clone(),
        );

        assert_eq!(event.event_id, event_id);
        assert_eq!(event.stream_id, stream_id);
        assert_eq!(event.event_type, "TestEvent");
        assert_eq!(event.data, data);
    }

    #[test]
    fn test_stored_event_builder() {
        let event = StoredEvent::new(
            456u64,
            StreamId::task("test-123"),
            "TestEvent".to_string(),
            serde_json::json!({}),
        )
        .with_version(2)
        .with_correlation_id("corr-123".to_string())
        .with_causation_id("cause-123".to_string());

        assert_eq!(event.version, 2);
        assert_eq!(event.correlation_id, Some("corr-123".to_string()));
        assert_eq!(event.causation_id, Some("cause-123".to_string()));
    }

    #[test]
    fn test_stream_position() {
        assert!(StreamPosition::Start.is_start());
        assert!(!StreamPosition::Start.is_end());

        assert!(StreamPosition::End.is_end());
        assert!(!StreamPosition::End.is_start());

        let pos = StreamPosition::Position(5);
        assert_eq!(pos.as_u64(), Some(5));

        let pos_from_u64: StreamPosition = 10.into();
        assert_eq!(pos_from_u64.as_u64(), Some(10));
    }

    #[test]
    fn test_stream_position_ordering() {
        assert!(StreamPosition::Position(1) < StreamPosition::Position(2));
        assert!(StreamPosition::Position(2) > StreamPosition::Position(1));
        assert_eq!(StreamPosition::Position(5), StreamPosition::Position(5));
    }

    #[test]
    fn test_event_query_builder() {
        let stream_id = StreamId::task("test-123");
        let query = EventQuery::new()
            .for_stream(stream_id.clone())
            .from_position(10)
            .to_position(20)
            .limit(50)
            .direction(ReadDirection::Backward);

        assert_eq!(query.stream_id, Some(stream_id));
        assert_eq!(query.from_position, Some(10));
        assert_eq!(query.to_position, Some(20));
        assert_eq!(query.limit, Some(50));
        assert_eq!(query.direction, ReadDirection::Backward);
    }

    #[test]
    fn test_event_query_with_event_types() {
        let query = EventQuery::new()
            .with_event_types(vec!["TaskEvent".to_string(), "WorkflowEvent".to_string()]);

        assert_eq!(query.event_types.len(), 2);
    }

    #[test]
    fn test_read_direction() {
        assert_eq!(ReadDirection::Forward, ReadDirection::Forward);
        assert_ne!(ReadDirection::Forward, ReadDirection::Backward);
    }

    #[test]
    fn test_append_result() {
        let result = AppendResult {
            stream_position: 5,
            global_position: 100,
            events_appended: 3,
        };

        assert_eq!(result.stream_position, 5);
        assert_eq!(result.global_position, 100);
        assert_eq!(result.events_appended, 3);
    }
}
