//! Event identifier types.

use super::traits::{Identifier, IdentifierError, IdentifierPrefix, IdentifierValidation};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

// Re-export identifiers from other modules for aggregate ID enum
use super::{NodeId, ScheduleId, TaskId, UserId, WorkerId, WorkflowId};

/// Type-safe aggregate identifier for events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AggregateId {
    Task(TaskId),
    Workflow(WorkflowId),
    Worker(WorkerId),
    Schedule(ScheduleId),
    User(UserId),
    Node(NodeId),
}

impl AggregateId {
    pub fn aggregate_type(&self) -> &'static str {
        match self {
            Self::Task(_) => "task",
            Self::Workflow(_) => "workflow",
            Self::Worker(_) => "worker",
            Self::Schedule(_) => "schedule",
            Self::User(_) => "user",
            Self::Node(_) => "node",
        }
    }

    pub fn as_uuid(&self) -> uuid::Uuid {
        match self {
            Self::Task(id) => id.as_uuid(),
            Self::Workflow(id) => id.as_uuid(),
            Self::Worker(id) => id.as_uuid(),
            Self::Schedule(id) => id.as_uuid(),
            Self::User(id) => id.as_uuid(),
            Self::Node(id) => id.as_uuid(),
        }
    }

    pub fn from_uuid_and_type(uuid: uuid::Uuid, aggregate_type: &str) -> Option<Self> {
        match aggregate_type {
            "task" => Some(Self::Task(TaskId::from_uuid(uuid))),
            "workflow" => Some(Self::Workflow(WorkflowId::from_uuid(uuid))),
            "worker" => Some(Self::Worker(WorkerId::from_uuid(uuid))),
            "schedule" => Some(Self::Schedule(ScheduleId::from_uuid(uuid))),
            "user" => Some(Self::User(UserId::from_uuid(uuid))),
            "node" => Some(Self::Node(NodeId::from_uuid(uuid))),
            _ => None,
        }
    }
}

impl fmt::Display for AggregateId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Task(id) => write!(f, "{}", id),
            Self::Workflow(id) => write!(f, "{}", id),
            Self::Worker(id) => write!(f, "{}", id),
            Self::Schedule(id) => write!(f, "{}", id),
            Self::User(id) => write!(f, "{}", id),
            Self::Node(id) => write!(f, "{}", id),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EventId(uuid::Uuid);

impl EventId {
    pub fn new_v4() -> Self {
        Self(uuid::Uuid::new_v4())
    }

    pub fn as_uuid(&self) -> uuid::Uuid {
        self.0
    }

    pub fn from_uuid(uuid: uuid::Uuid) -> Self {
        Self(uuid)
    }
}

impl Identifier for EventId {
    fn generate() -> Self {
        Self::new_v4()
    }

    fn is_valid(&self) -> bool {
        !self.0.is_nil()
    }

    fn as_str(&self) -> String {
        self.0.to_string()
    }

    fn type_name() -> &'static str {
        "EventId"
    }

    fn is_nil(&self) -> bool {
        self.0.is_nil()
    }
}

impl IdentifierPrefix for EventId {
    fn prefix() -> &'static str {
        "evt_"
    }

    fn without_prefix(&self) -> String {
        self.0.simple().to_string()
    }

    fn from_prefixed_str(s: &str) -> Result<Self, IdentifierError> {
        if !s.starts_with(Self::prefix()) {
            return Err(IdentifierError::InvalidPrefix {
                expected: Self::prefix().to_string(),
                found: s.chars().take(4).collect(),
            });
        }

        let uuid_str = &s[Self::prefix().len()..];
        let uuid = uuid::Uuid::parse_str(uuid_str)
            .map_err(|_| IdentifierError::InvalidFormat(format!("Invalid UUID: {}", uuid_str)))?;

        Ok(Self(uuid))
    }
}

impl IdentifierValidation for EventId {
    fn validate_format(&self) -> Result<(), IdentifierError> {
        if self.is_nil() {
            return Err(IdentifierError::NilIdentifier);
        }
        Ok(())
    }

    fn validate_business_rules(&self) -> Result<(), IdentifierError> {
        Ok(())
    }
}

impl fmt::Display for EventId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", Self::prefix(), self.0.simple())
    }
}

impl FromStr for EventId {
    type Err = IdentifierError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.starts_with(Self::prefix()) {
            Self::from_prefixed_str(s)
        } else {
            let uuid = uuid::Uuid::parse_str(s)
                .map_err(|_| IdentifierError::InvalidFormat(format!("Invalid UUID: {}", s)))?;
            Ok(Self(uuid))
        }
    }
}

impl From<uuid::Uuid> for EventId {
    fn from(uuid: uuid::Uuid) -> Self {
        Self(uuid)
    }
}

impl From<EventId> for uuid::Uuid {
    fn from(id: EventId) -> Self {
        id.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SnapshotId(uuid::Uuid);

impl SnapshotId {
    pub fn new_v4() -> Self {
        Self(uuid::Uuid::new_v4())
    }

    pub fn as_uuid(&self) -> uuid::Uuid {
        self.0
    }

    pub fn from_uuid(uuid: uuid::Uuid) -> Self {
        Self(uuid)
    }
}

impl Identifier for SnapshotId {
    fn generate() -> Self {
        Self::new_v4()
    }

    fn is_valid(&self) -> bool {
        !self.0.is_nil()
    }

    fn as_str(&self) -> String {
        self.0.to_string()
    }

    fn type_name() -> &'static str {
        "SnapshotId"
    }

    fn is_nil(&self) -> bool {
        self.0.is_nil()
    }
}

impl IdentifierPrefix for SnapshotId {
    fn prefix() -> &'static str {
        "snap_"
    }

    fn without_prefix(&self) -> String {
        self.0.simple().to_string()
    }

    fn from_prefixed_str(s: &str) -> Result<Self, IdentifierError> {
        if !s.starts_with(Self::prefix()) {
            return Err(IdentifierError::InvalidPrefix {
                expected: Self::prefix().to_string(),
                found: s.chars().take(5).collect(),
            });
        }

        let uuid_str = &s[Self::prefix().len()..];
        let uuid = uuid::Uuid::parse_str(uuid_str)
            .map_err(|_| IdentifierError::InvalidFormat(format!("Invalid UUID: {}", uuid_str)))?;

        Ok(Self(uuid))
    }
}

impl IdentifierValidation for SnapshotId {
    fn validate_format(&self) -> Result<(), IdentifierError> {
        if self.is_nil() {
            return Err(IdentifierError::NilIdentifier);
        }
        Ok(())
    }

    fn validate_business_rules(&self) -> Result<(), IdentifierError> {
        Ok(())
    }
}

impl fmt::Display for SnapshotId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", Self::prefix(), self.0.simple())
    }
}

impl FromStr for SnapshotId {
    type Err = IdentifierError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.starts_with(Self::prefix()) {
            Self::from_prefixed_str(s)
        } else {
            let uuid = uuid::Uuid::parse_str(s)
                .map_err(|_| IdentifierError::InvalidFormat(format!("Invalid UUID: {}", s)))?;
            Ok(Self(uuid))
        }
    }
}

impl From<uuid::Uuid> for SnapshotId {
    fn from(uuid: uuid::Uuid) -> Self {
        Self(uuid)
    }
}

impl From<SnapshotId> for uuid::Uuid {
    fn from(id: SnapshotId) -> Self {
        id.0
    }
}

/// Correlation identifier for distributed tracing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CorrelationId(uuid::Uuid);

impl CorrelationId {
    pub fn new_v4() -> Self {
        Self(uuid::Uuid::new_v4())
    }

    pub fn as_uuid(&self) -> uuid::Uuid {
        self.0
    }

    pub fn from_uuid(uuid: uuid::Uuid) -> Self {
        Self(uuid)
    }
}

impl Identifier for CorrelationId {
    fn generate() -> Self {
        Self::new_v4()
    }

    fn is_valid(&self) -> bool {
        !self.0.is_nil()
    }

    fn as_str(&self) -> String {
        self.0.to_string()
    }

    fn type_name() -> &'static str {
        "CorrelationId"
    }

    fn is_nil(&self) -> bool {
        self.0.is_nil()
    }
}

impl IdentifierPrefix for CorrelationId {
    fn prefix() -> &'static str {
        "corr_"
    }

    fn without_prefix(&self) -> String {
        self.0.simple().to_string()
    }

    fn from_prefixed_str(s: &str) -> Result<Self, IdentifierError> {
        if !s.starts_with(Self::prefix()) {
            return Err(IdentifierError::InvalidPrefix {
                expected: Self::prefix().to_string(),
                found: s.chars().take(5).collect(),
            });
        }

        let uuid_str = &s[Self::prefix().len()..];
        let uuid = uuid::Uuid::parse_str(uuid_str)
            .map_err(|_| IdentifierError::InvalidFormat(format!("Invalid UUID: {}", uuid_str)))?;

        Ok(Self(uuid))
    }
}

impl IdentifierValidation for CorrelationId {
    fn validate_format(&self) -> Result<(), IdentifierError> {
        if self.is_nil() {
            return Err(IdentifierError::NilIdentifier);
        }
        Ok(())
    }

    fn validate_business_rules(&self) -> Result<(), IdentifierError> {
        Ok(())
    }
}

impl fmt::Display for CorrelationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", Self::prefix(), self.0.simple())
    }
}

impl FromStr for CorrelationId {
    type Err = IdentifierError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.starts_with(Self::prefix()) {
            Self::from_prefixed_str(s)
        } else {
            let uuid = uuid::Uuid::parse_str(s)
                .map_err(|_| IdentifierError::InvalidFormat(format!("Invalid UUID: {}", s)))?;
            Ok(Self(uuid))
        }
    }
}

impl From<uuid::Uuid> for CorrelationId {
    fn from(uuid: uuid::Uuid) -> Self {
        Self(uuid)
    }
}

impl From<CorrelationId> for uuid::Uuid {
    fn from(id: CorrelationId) -> Self {
        id.0
    }
}

/// Request tracking identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RequestId(uuid::Uuid);

impl RequestId {
    pub fn new_v4() -> Self {
        Self(uuid::Uuid::new_v4())
    }

    pub fn as_uuid(&self) -> uuid::Uuid {
        self.0
    }

    pub fn from_uuid(uuid: uuid::Uuid) -> Self {
        Self(uuid)
    }
}

impl Identifier for RequestId {
    fn generate() -> Self {
        Self::new_v4()
    }

    fn is_valid(&self) -> bool {
        !self.0.is_nil()
    }

    fn as_str(&self) -> String {
        self.0.to_string()
    }

    fn type_name() -> &'static str {
        "RequestId"
    }

    fn is_nil(&self) -> bool {
        self.0.is_nil()
    }
}

impl IdentifierPrefix for RequestId {
    fn prefix() -> &'static str {
        "req_"
    }

    fn without_prefix(&self) -> String {
        self.0.simple().to_string()
    }

    fn from_prefixed_str(s: &str) -> Result<Self, IdentifierError> {
        if !s.starts_with(Self::prefix()) {
            return Err(IdentifierError::InvalidPrefix {
                expected: Self::prefix().to_string(),
                found: s.chars().take(4).collect(),
            });
        }

        let uuid_str = &s[Self::prefix().len()..];
        let uuid = uuid::Uuid::parse_str(uuid_str)
            .map_err(|_| IdentifierError::InvalidFormat(format!("Invalid UUID: {}", uuid_str)))?;

        Ok(Self(uuid))
    }
}

impl IdentifierValidation for RequestId {
    fn validate_format(&self) -> Result<(), IdentifierError> {
        if self.is_nil() {
            return Err(IdentifierError::NilIdentifier);
        }
        Ok(())
    }

    fn validate_business_rules(&self) -> Result<(), IdentifierError> {
        Ok(())
    }
}

impl fmt::Display for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", Self::prefix(), self.0.simple())
    }
}

impl FromStr for RequestId {
    type Err = IdentifierError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.starts_with(Self::prefix()) {
            Self::from_prefixed_str(s)
        } else {
            let uuid = uuid::Uuid::parse_str(s)
                .map_err(|_| IdentifierError::InvalidFormat(format!("Invalid UUID: {}", s)))?;
            Ok(Self(uuid))
        }
    }
}

impl From<uuid::Uuid> for RequestId {
    fn from(uuid: uuid::Uuid) -> Self {
        Self(uuid)
    }
}

impl From<RequestId> for uuid::Uuid {
    fn from(id: RequestId) -> Self {
        id.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_id_creation() {
        let id = EventId::new_v4();
        assert!(id.is_valid());
        assert!(!id.is_nil());
        assert_eq!(EventId::type_name(), "EventId");
    }

    #[test]
    fn test_event_id_display() {
        let id = EventId::new_v4();
        let display_str = id.to_string();
        assert!(display_str.starts_with("evt_"));
    }

    #[test]
    fn test_event_id_from_str() {
        let id = EventId::new_v4();
        let id_str = id.to_string();
        let parsed_id = EventId::from_str(&id_str).unwrap();
        assert_eq!(id, parsed_id);
    }

    #[test]
    fn test_snapshot_id_creation() {
        let id = SnapshotId::new_v4();
        assert!(id.is_valid());
        assert_eq!(SnapshotId::type_name(), "SnapshotId");
    }

    #[test]
    fn test_snapshot_id_display() {
        let id = SnapshotId::new_v4();
        let display_str = id.to_string();
        assert!(display_str.starts_with("snap_"));
    }

    #[test]
    fn test_correlation_id_creation() {
        let id = CorrelationId::new_v4();
        assert!(id.is_valid());
        assert_eq!(CorrelationId::type_name(), "CorrelationId");
    }

    #[test]
    fn test_correlation_id_display() {
        let id = CorrelationId::new_v4();
        let display_str = id.to_string();
        assert!(display_str.starts_with("corr_"));
    }

    #[test]
    fn test_request_id_creation() {
        let id = RequestId::new_v4();
        assert!(id.is_valid());
        assert_eq!(RequestId::type_name(), "RequestId");
    }

    #[test]
    fn test_request_id_display() {
        let id = RequestId::new_v4();
        let display_str = id.to_string();
        assert!(display_str.starts_with("req_"));
    }

    #[test]
    fn test_identifier_validation() {
        let id = EventId::new_v4();
        assert!(id.validate().is_ok());

        let nil_id = EventId::from_uuid(uuid::Uuid::nil());
        assert!(nil_id.validate().is_err());
    }

    #[test]
    fn test_all_identifier_types_serialization() {
        let event_id = EventId::new_v4();
        let snapshot_id = SnapshotId::new_v4();
        let correlation_id = CorrelationId::new_v4();
        let request_id = RequestId::new_v4();

        // Test EventId serialization
        let json = serde_json::to_string(&event_id).unwrap();
        let deserialized: EventId = serde_json::from_str(&json).unwrap();
        assert_eq!(event_id, deserialized);

        // Test SnapshotId serialization
        let json = serde_json::to_string(&snapshot_id).unwrap();
        let deserialized: SnapshotId = serde_json::from_str(&json).unwrap();
        assert_eq!(snapshot_id, deserialized);

        // Test CorrelationId serialization
        let json = serde_json::to_string(&correlation_id).unwrap();
        let deserialized: CorrelationId = serde_json::from_str(&json).unwrap();
        assert_eq!(correlation_id, deserialized);

        // Test RequestId serialization
        let json = serde_json::to_string(&request_id).unwrap();
        let deserialized: RequestId = serde_json::from_str(&json).unwrap();
        assert_eq!(request_id, deserialized);
    }

    #[test]
    fn test_aggregate_id_creation() {
        let task_id = TaskId::generate();
        let workflow_id = WorkflowId::generate();
        let worker_id = WorkerId::generate();

        let task_aggregate = AggregateId::Task(task_id);
        let workflow_aggregate = AggregateId::Workflow(workflow_id);
        let worker_aggregate = AggregateId::Worker(worker_id);

        assert_eq!(task_aggregate.aggregate_type(), "task");
        assert_eq!(workflow_aggregate.aggregate_type(), "workflow");
        assert_eq!(worker_aggregate.aggregate_type(), "worker");

        assert_eq!(task_aggregate.as_uuid(), task_id.as_uuid());
        assert_eq!(workflow_aggregate.as_uuid(), workflow_id.as_uuid());
        assert_eq!(worker_aggregate.as_uuid(), worker_id.as_uuid());
    }

    #[test]
    fn test_aggregate_id_from_uuid_and_type() {
        let uuid = uuid::Uuid::new_v4();

        let task_aggregate = AggregateId::from_uuid_and_type(uuid, "task").unwrap();
        assert_eq!(task_aggregate.aggregate_type(), "task");
        assert_eq!(task_aggregate.as_uuid(), uuid);

        let workflow_aggregate = AggregateId::from_uuid_and_type(uuid, "workflow").unwrap();
        assert_eq!(workflow_aggregate.aggregate_type(), "workflow");
        assert_eq!(workflow_aggregate.as_uuid(), uuid);

        let invalid_aggregate = AggregateId::from_uuid_and_type(uuid, "invalid");
        assert!(invalid_aggregate.is_none());
    }

    #[test]
    fn test_aggregate_id_display() {
        let task_id = TaskId::generate();
        let task_aggregate = AggregateId::Task(task_id);

        let display_str = task_aggregate.to_string();
        // No prefix - just UUID
        assert_eq!(display_str.len(), 32); // UUID without hyphens
        assert_eq!(display_str, task_id.to_string());
    }

    #[test]
    fn test_aggregate_id_serialization() {
        let task_id = TaskId::generate();
        let task_aggregate = AggregateId::Task(task_id);

        let json = serde_json::to_string(&task_aggregate).unwrap();
        let deserialized: AggregateId = serde_json::from_str(&json).unwrap();
        assert_eq!(task_aggregate, deserialized);
    }

    #[test]
    fn test_event_id_from_uuid() {
        let uuid = uuid::Uuid::new_v4();
        let id: EventId = uuid.into();
        assert_eq!(id.as_uuid(), uuid);

        let uuid_back: uuid::Uuid = id.into();
        assert_eq!(uuid, uuid_back);
    }

    #[test]
    fn test_snapshot_id_from_uuid() {
        let uuid = uuid::Uuid::new_v4();
        let id: SnapshotId = uuid.into();
        assert_eq!(id.as_uuid(), uuid);

        let uuid_back: uuid::Uuid = id.into();
        assert_eq!(uuid, uuid_back);
    }

    #[test]
    fn test_correlation_id_from_uuid() {
        let uuid = uuid::Uuid::new_v4();
        let id: CorrelationId = uuid.into();
        assert_eq!(id.as_uuid(), uuid);

        let uuid_back: uuid::Uuid = id.into();
        assert_eq!(uuid, uuid_back);
    }

    #[test]
    fn test_request_id_from_uuid() {
        let uuid = uuid::Uuid::new_v4();
        let id: RequestId = uuid.into();
        assert_eq!(id.as_uuid(), uuid);

        let uuid_back: uuid::Uuid = id.into();
        assert_eq!(uuid, uuid_back);
    }
}
