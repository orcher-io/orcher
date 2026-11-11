//! UUID-based identifiers for event tracking and correlation.

use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

/// UUID v4 wrapper for uniquely identifying an event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EventId(Uuid);

impl EventId {
    pub fn generate() -> Self {
        Self(Uuid::new_v4())
    }

    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }

    pub fn into_uuid(self) -> Uuid {
        self.0
    }

    pub fn parse(s: &str) -> Result<Self, uuid::Error> {
        Ok(Self(Uuid::parse_str(s)?))
    }

    /// All zeros — useful for tests or placeholders.
    pub fn nil() -> Self {
        Self(Uuid::nil())
    }

    pub fn is_nil(&self) -> bool {
        self.0.is_nil()
    }
}

impl fmt::Display for EventId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<Uuid> for EventId {
    fn from(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

impl From<EventId> for Uuid {
    fn from(id: EventId) -> Self {
        id.0
    }
}

impl std::str::FromStr for EventId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

/// Shared across all events in the same logical operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CorrelationId(Uuid);

impl CorrelationId {
    pub fn generate() -> Self {
        Self(Uuid::new_v4())
    }

    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }

    pub fn into_uuid(self) -> Uuid {
        self.0
    }

    pub fn parse(s: &str) -> Result<Self, uuid::Error> {
        Ok(Self(Uuid::parse_str(s)?))
    }

    pub fn nil() -> Self {
        Self(Uuid::nil())
    }

    pub fn is_nil(&self) -> bool {
        self.0.is_nil()
    }
}

impl fmt::Display for CorrelationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<Uuid> for CorrelationId {
    fn from(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

impl From<CorrelationId> for Uuid {
    fn from(id: CorrelationId) -> Self {
        id.0
    }
}

impl std::str::FromStr for CorrelationId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

/// Points back to the event that directly caused this one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CausationId(Uuid);

impl CausationId {
    pub fn generate() -> Self {
        Self(Uuid::new_v4())
    }

    /// Wrap the causing event's ID.
    pub fn from_event_id(event_id: EventId) -> Self {
        Self(event_id.into_uuid())
    }

    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }

    pub fn into_uuid(self) -> Uuid {
        self.0
    }

    /// Look up the event that caused this one.
    pub fn to_event_id(&self) -> EventId {
        EventId::from_uuid(self.0)
    }

    pub fn parse(s: &str) -> Result<Self, uuid::Error> {
        Ok(Self(Uuid::parse_str(s)?))
    }

    pub fn nil() -> Self {
        Self(Uuid::nil())
    }

    pub fn is_nil(&self) -> bool {
        self.0.is_nil()
    }
}

impl fmt::Display for CausationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<Uuid> for CausationId {
    fn from(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

impl From<CausationId> for Uuid {
    fn from(id: CausationId) -> Self {
        id.0
    }
}

impl From<EventId> for CausationId {
    fn from(event_id: EventId) -> Self {
        Self::from_event_id(event_id)
    }
}

impl std::str::FromStr for CausationId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

/// Identifies an event stream — `category-id` (e.g. "task-task_123").
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StreamId {
    pub category: String,
    pub id: String,
}

impl StreamId {
    pub fn new(category: impl Into<String>, id: impl Into<String>) -> Self {
        Self {
            category: category.into(),
            id: id.into(),
        }
    }

    pub fn task(task_id: impl Into<String>) -> Self {
        Self::new("task", task_id)
    }

    pub fn workflow(workflow_id: impl Into<String>) -> Self {
        Self::new("workflow", workflow_id)
    }

    pub fn worker(worker_id: impl Into<String>) -> Self {
        Self::new("worker", worker_id)
    }

    pub fn execution(execution_id: impl Into<String>) -> Self {
        Self::new("execution", execution_id)
    }

    /// `"category-id"` form.
    pub fn full_name(&self) -> String {
        format!("{}-{}", self.category, self.id)
    }

    /// Parse from `"category-id"` format.
    pub fn parse(s: &str) -> Result<Self, String> {
        let parts: Vec<&str> = s.splitn(2, '-').collect();
        if parts.len() != 2 {
            return Err(format!("Invalid stream ID format: {}", s));
        }
        Ok(Self {
            category: parts[0].to_string(),
            id: parts[1].to_string(),
        })
    }

    pub fn category(&self) -> &str {
        &self.category
    }

    pub fn id_part(&self) -> &str {
        &self.id
    }

    /// Alias for `new` — useful when you want to emphasize the category.
    pub fn new_with_category(category: impl Into<String>, id: impl Into<String>) -> Self {
        Self::new(category, id)
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.full_name())
    }
}

impl std::str::FromStr for StreamId {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_id_generation() {
        let id1 = EventId::generate();
        let id2 = EventId::generate();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_event_id_from_uuid() {
        let uuid = Uuid::new_v4();
        let id = EventId::from_uuid(uuid);
        assert_eq!(id.as_uuid(), &uuid);
    }

    #[test]
    fn test_event_id_into_uuid() {
        let uuid = Uuid::new_v4();
        let id = EventId::from_uuid(uuid);
        let uuid2: Uuid = id.into_uuid();
        assert_eq!(uuid, uuid2);
    }

    #[test]
    fn test_event_id_parse() {
        let uuid_str = "550e8400-e29b-41d4-a716-446655440000";
        let id = EventId::parse(uuid_str).unwrap();
        assert_eq!(id.to_string(), uuid_str);
    }

    #[test]
    fn test_event_id_nil() {
        let nil_id = EventId::nil();
        assert!(nil_id.is_nil());

        let real_id = EventId::generate();
        assert!(!real_id.is_nil());
    }

    #[test]
    fn test_event_id_display() {
        let id = EventId::generate();
        let display = format!("{}", id);
        assert!(!display.is_empty());
    }

    #[test]
    fn test_correlation_id_generation() {
        let id1 = CorrelationId::generate();
        let id2 = CorrelationId::generate();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_correlation_id_from_uuid() {
        let uuid = Uuid::new_v4();
        let id = CorrelationId::from_uuid(uuid);
        assert_eq!(id.as_uuid(), &uuid);
    }

    #[test]
    fn test_causation_id_from_event_id() {
        let event_id = EventId::generate();
        let causation_id = CausationId::from_event_id(event_id);
        assert_eq!(causation_id.to_event_id(), event_id);
    }

    #[test]
    fn test_causation_id_generation() {
        let id1 = CausationId::generate();
        let id2 = CausationId::generate();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_stream_id_creation() {
        let stream_id = StreamId::new("task", "task_123");
        assert_eq!(stream_id.category, "task");
        assert_eq!(stream_id.id, "task_123");
    }

    #[test]
    fn test_stream_id_task() {
        let stream_id = StreamId::task("task_123");
        assert_eq!(stream_id.category, "task");
        assert_eq!(stream_id.id, "task_123");
    }

    #[test]
    fn test_stream_id_workflow() {
        let stream_id = StreamId::workflow("workflow_456");
        assert_eq!(stream_id.category, "workflow");
        assert_eq!(stream_id.id, "workflow_456");
    }

    #[test]
    fn test_stream_id_full_name() {
        let stream_id = StreamId::task("task_123");
        assert_eq!(stream_id.full_name(), "task-task_123");
    }

    #[test]
    fn test_stream_id_parse() {
        let stream_id = StreamId::parse("task-task_123").unwrap();
        assert_eq!(stream_id.category, "task");
        assert_eq!(stream_id.id, "task_123");
    }

    #[test]
    fn test_stream_id_parse_invalid() {
        let result = StreamId::parse("invalid");
        assert!(result.is_err());
    }

    #[test]
    fn test_stream_id_display() {
        let stream_id = StreamId::task("task_123");
        assert_eq!(stream_id.to_string(), "task-task_123");
    }

    #[test]
    fn test_event_id_serialization() {
        let id = EventId::generate();
        let json = serde_json::to_string(&id).unwrap();
        let deserialized: EventId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, deserialized);
    }

    #[test]
    fn test_correlation_id_serialization() {
        let id = CorrelationId::generate();
        let json = serde_json::to_string(&id).unwrap();
        let deserialized: CorrelationId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, deserialized);
    }

    #[test]
    fn test_causation_id_serialization() {
        let id = CausationId::generate();
        let json = serde_json::to_string(&id).unwrap();
        let deserialized: CausationId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, deserialized);
    }

    #[test]
    fn test_stream_id_serialization() {
        let stream_id = StreamId::task("task_123");
        let json = serde_json::to_string(&stream_id).unwrap();
        let deserialized: StreamId = serde_json::from_str(&json).unwrap();
        assert_eq!(stream_id, deserialized);
    }
}
