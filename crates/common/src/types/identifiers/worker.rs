//! Worker identifier types.

use super::traits::{Identifier, IdentifierError, IdentifierPrefix, IdentifierValidation};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkerId(uuid::Uuid);

impl WorkerId {
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

impl Identifier for WorkerId {
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
        "WorkerId"
    }

    fn is_nil(&self) -> bool {
        self.0.is_nil()
    }
}

impl IdentifierPrefix for WorkerId {
    fn prefix() -> &'static str {
        "worker_"
    }

    fn without_prefix(&self) -> String {
        self.0.simple().to_string()
    }

    fn from_prefixed_str(s: &str) -> Result<Self, IdentifierError> {
        if !s.starts_with(Self::prefix()) {
            return Err(IdentifierError::InvalidPrefix {
                expected: Self::prefix().to_string(),
                found: s.chars().take(7).collect(),
            });
        }

        let uuid_str = &s[Self::prefix().len()..];
        let uuid = uuid::Uuid::parse_str(uuid_str)
            .map_err(|_| IdentifierError::InvalidFormat(format!("Invalid UUID: {}", uuid_str)))?;

        Ok(Self(uuid))
    }
}

impl IdentifierValidation for WorkerId {
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

impl fmt::Display for WorkerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", Self::prefix(), self.0.simple())
    }
}

impl FromStr for WorkerId {
    type Err = IdentifierError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_prefixed_str(s)
    }
}

impl From<uuid::Uuid> for WorkerId {
    fn from(uuid: uuid::Uuid) -> Self {
        Self(uuid)
    }
}

impl From<WorkerId> for uuid::Uuid {
    fn from(id: WorkerId) -> Self {
        id.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_worker_id_generation() {
        let id = WorkerId::new_v4();
        assert!(id.is_valid());
        assert!(!id.is_nil());
    }

    #[test]
    fn test_worker_id_from_uuid() {
        let uuid = uuid::Uuid::new_v4();
        let id = WorkerId::from_uuid(uuid);
        assert_eq!(id.as_uuid(), uuid);
    }

    #[test]
    fn test_worker_id_display() {
        let id = WorkerId::new_v4();
        let display = format!("{}", id);
        assert!(display.starts_with("worker_"));
    }

    #[test]
    fn test_worker_id_from_str() {
        let uuid = uuid::Uuid::new_v4();
        let id_str = format!("worker_{}", uuid.simple());
        let id: WorkerId = id_str.parse().unwrap();
        assert_eq!(id.as_uuid(), uuid);
    }

    #[test]
    fn test_worker_id_invalid_prefix() {
        let result: Result<WorkerId, _> = "task_123e4567e89b12d3a456426614174000".parse();
        assert!(result.is_err());
    }

    #[test]
    fn test_worker_id_copy() {
        let id1 = WorkerId::new_v4();
        let id2 = id1;
        assert_eq!(id1, id2);
    }

    #[test]
    fn test_worker_id_serialization() {
        let id = WorkerId::new_v4();
        let json = serde_json::to_string(&id).unwrap();
        let deserialized: WorkerId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, deserialized);
    }

    #[test]
    fn test_identifier_traits() {
        fn _assert_identifier<T: Identifier>() {}
        _assert_identifier::<WorkerId>();
    }
}
