//! Workflow identifier types.

use super::traits::{Identifier, IdentifierError, IdentifierPrefix, IdentifierValidation};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkflowId(uuid::Uuid);

impl WorkflowId {
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

impl Identifier for WorkflowId {
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
        "WorkflowId"
    }

    fn is_nil(&self) -> bool {
        self.0.is_nil()
    }
}

impl IdentifierPrefix for WorkflowId {
    fn prefix() -> &'static str {
        ""
    }

    fn without_prefix(&self) -> String {
        self.0.hyphenated().to_string()
    }

    fn from_prefixed_str(s: &str) -> Result<Self, IdentifierError> {
        if !s.starts_with(Self::prefix()) {
            return Err(IdentifierError::InvalidPrefix {
                expected: Self::prefix().to_string(),
                found: s.chars().take(3).collect(),
            });
        }

        let uuid_str = &s[Self::prefix().len()..];
        let uuid = uuid::Uuid::parse_str(uuid_str)
            .map_err(|_| IdentifierError::InvalidFormat(format!("Invalid UUID: {}", uuid_str)))?;

        Ok(Self(uuid))
    }
}

impl IdentifierValidation for WorkflowId {
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

/// The one rendering of a workflow execution's id: the hyphenated UUID, the same string as
/// `as_uuid().to_string()` and `as_str()`. Journal rows, task tokens and every
/// other string key are written in this form, so it must not differ from how
/// the id is read back. It once rendered without hyphens, and an event a
/// workflow sent to another was journaled under a key its target never read.
impl fmt::Display for WorkflowId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", Self::prefix(), self.0.hyphenated())
    }
}

impl FromStr for WorkflowId {
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

impl From<uuid::Uuid> for WorkflowId {
    fn from(uuid: uuid::Uuid) -> Self {
        Self(uuid)
    }
}

impl From<WorkflowId> for uuid::Uuid {
    fn from(id: WorkflowId) -> Self {
        id.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ExecutionId(uuid::Uuid);

impl ExecutionId {
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

impl Identifier for ExecutionId {
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
        "ExecutionId"
    }

    fn is_nil(&self) -> bool {
        self.0.is_nil()
    }
}

impl IdentifierPrefix for ExecutionId {
    fn prefix() -> &'static str {
        ""
    }

    fn without_prefix(&self) -> String {
        self.0.hyphenated().to_string()
    }

    fn from_prefixed_str(s: &str) -> Result<Self, IdentifierError> {
        if !s.starts_with(Self::prefix()) {
            return Err(IdentifierError::InvalidPrefix {
                expected: Self::prefix().to_string(),
                found: s.chars().take(3).collect(),
            });
        }

        let uuid_str = &s[Self::prefix().len()..];
        let uuid = uuid::Uuid::parse_str(uuid_str)
            .map_err(|_| IdentifierError::InvalidFormat(format!("Invalid UUID: {}", uuid_str)))?;

        Ok(Self(uuid))
    }
}

impl IdentifierValidation for ExecutionId {
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

/// The one rendering of an execution's id: the hyphenated UUID, the same string as
/// `as_uuid().to_string()` and `as_str()`. Journal rows, task tokens and every
/// other string key are written in this form, so it must not differ from how
/// the id is read back. It once rendered without hyphens, and an event a
/// workflow sent to another was journaled under a key its target never read.
impl fmt::Display for ExecutionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", Self::prefix(), self.0.hyphenated())
    }
}

impl FromStr for ExecutionId {
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

impl From<uuid::Uuid> for ExecutionId {
    fn from(uuid: uuid::Uuid) -> Self {
        Self(uuid)
    }
}

impl From<ExecutionId> for uuid::Uuid {
    fn from(id: ExecutionId) -> Self {
        id.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_workflow_id_creation() {
        let id = WorkflowId::new_v4();
        assert!(id.is_valid());
        assert!(!id.is_nil());
        assert_eq!(WorkflowId::type_name(), "WorkflowId");
    }

    #[test]
    fn test_workflow_id_display() {
        let id = WorkflowId::new_v4();
        // The canonical form: identical to the UUID's own rendering, so a key
        // written with one and read with the other names the same rows.
        assert_eq!(id.to_string(), id.as_uuid().to_string());
        assert_eq!(id.to_string(), id.as_str());
        assert_eq!(id.to_string(), id.without_prefix());
        assert_eq!(format!("{id}"), id.as_uuid().hyphenated().to_string());
    }

    #[test]
    fn test_workflow_id_from_str() {
        let id = WorkflowId::new_v4();
        let id_str = id.to_string();
        let parsed_id = WorkflowId::from_str(&id_str).unwrap();
        assert_eq!(id, parsed_id);
    }

    #[test]
    fn test_execution_id_creation() {
        let id = ExecutionId::new_v4();
        assert!(id.is_valid());
        assert_eq!(ExecutionId::type_name(), "ExecutionId");
    }

    #[test]
    fn test_execution_id_display() {
        let id = ExecutionId::new_v4();
        // The canonical form: identical to the UUID's own rendering, so a key
        // written with one and read with the other names the same rows.
        assert_eq!(id.to_string(), id.as_uuid().to_string());
        assert_eq!(id.to_string(), id.as_str());
        assert_eq!(id.to_string(), id.without_prefix());
        assert_eq!(format!("{id}"), id.as_uuid().hyphenated().to_string());
    }

    #[test]
    fn test_identifier_validation() {
        let id = WorkflowId::new_v4();
        assert!(id.validate().is_ok());

        let nil_id = WorkflowId::from_uuid(uuid::Uuid::nil());
        assert!(nil_id.validate().is_err());
    }

    #[test]
    fn test_workflow_id_from_prefixed_str() {
        let id = WorkflowId::new_v4();
        let prefixed = id.to_string();
        let parsed = WorkflowId::from_prefixed_str(&prefixed).unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn test_execution_id_from_prefixed_str() {
        let id = ExecutionId::new_v4();
        let prefixed = id.to_string();
        let parsed = ExecutionId::from_prefixed_str(&prefixed).unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn test_workflow_id_from_uuid() {
        let uuid = uuid::Uuid::new_v4();
        let id: WorkflowId = uuid.into();
        assert_eq!(id.as_uuid(), uuid);

        let uuid_back: uuid::Uuid = id.into();
        assert_eq!(uuid, uuid_back);
    }

    #[test]
    fn test_execution_id_from_uuid() {
        let uuid = uuid::Uuid::new_v4();
        let id: ExecutionId = uuid.into();
        assert_eq!(id.as_uuid(), uuid);

        let uuid_back: uuid::Uuid = id.into();
        assert_eq!(uuid, uuid_back);
    }
}
