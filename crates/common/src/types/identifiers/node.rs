//! Node identifier types.

use super::traits::{Identifier, IdentifierError, IdentifierPrefix, IdentifierValidation};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(uuid::Uuid);

impl NodeId {
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

impl Identifier for NodeId {
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
        "NodeId"
    }

    fn is_nil(&self) -> bool {
        self.0.is_nil()
    }
}

impl IdentifierPrefix for NodeId {
    fn prefix() -> &'static str {
        "node_"
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

impl IdentifierValidation for NodeId {
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

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", Self::prefix(), self.0.simple())
    }
}

impl FromStr for NodeId {
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

impl From<uuid::Uuid> for NodeId {
    fn from(uuid: uuid::Uuid) -> Self {
        Self(uuid)
    }
}

impl From<NodeId> for uuid::Uuid {
    fn from(id: NodeId) -> Self {
        id.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ClusterId(uuid::Uuid);

impl ClusterId {
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

impl Identifier for ClusterId {
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
        "ClusterId"
    }

    fn is_nil(&self) -> bool {
        self.0.is_nil()
    }
}

impl IdentifierPrefix for ClusterId {
    fn prefix() -> &'static str {
        "cluster_"
    }

    fn without_prefix(&self) -> String {
        self.0.simple().to_string()
    }

    fn from_prefixed_str(s: &str) -> Result<Self, IdentifierError> {
        if !s.starts_with(Self::prefix()) {
            return Err(IdentifierError::InvalidPrefix {
                expected: Self::prefix().to_string(),
                found: s.chars().take(8).collect(),
            });
        }

        let uuid_str = &s[Self::prefix().len()..];
        let uuid = uuid::Uuid::parse_str(uuid_str)
            .map_err(|_| IdentifierError::InvalidFormat(format!("Invalid UUID: {}", uuid_str)))?;

        Ok(Self(uuid))
    }
}

impl IdentifierValidation for ClusterId {
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

impl fmt::Display for ClusterId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", Self::prefix(), self.0.simple())
    }
}

impl FromStr for ClusterId {
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

impl From<uuid::Uuid> for ClusterId {
    fn from(uuid: uuid::Uuid) -> Self {
        Self(uuid)
    }
}

impl From<ClusterId> for uuid::Uuid {
    fn from(id: ClusterId) -> Self {
        id.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_id_creation() {
        let id = NodeId::new_v4();
        assert!(id.is_valid());
        assert!(!id.is_nil());
        assert_eq!(NodeId::type_name(), "NodeId");
    }

    #[test]
    fn test_node_id_display() {
        let id = NodeId::new_v4();
        let display_str = id.to_string();
        assert!(display_str.starts_with("node_"));
    }

    #[test]
    fn test_node_id_from_str() {
        let id = NodeId::new_v4();
        let id_str = id.to_string();
        let parsed_id = NodeId::from_str(&id_str).unwrap();
        assert_eq!(id, parsed_id);
    }

    #[test]
    fn test_cluster_id_creation() {
        let id = ClusterId::new_v4();
        assert!(id.is_valid());
        assert!(!id.is_nil());
        assert_eq!(ClusterId::type_name(), "ClusterId");
    }

    #[test]
    fn test_cluster_id_display() {
        let id = ClusterId::new_v4();
        let display_str = id.to_string();
        assert!(display_str.starts_with("cluster_"));
    }

    #[test]
    fn test_cluster_id_from_str() {
        let id = ClusterId::new_v4();
        let id_str = id.to_string();
        let parsed_id = ClusterId::from_str(&id_str).unwrap();
        assert_eq!(id, parsed_id);
    }

    #[test]
    fn test_identifier_validation() {
        let node_id = NodeId::new_v4();
        assert!(node_id.validate().is_ok());

        let nil_node_id = NodeId::from_uuid(uuid::Uuid::nil());
        assert!(nil_node_id.validate().is_err());

        let cluster_id = ClusterId::new_v4();
        assert!(cluster_id.validate().is_ok());

        let nil_cluster_id = ClusterId::from_uuid(uuid::Uuid::nil());
        assert!(nil_cluster_id.validate().is_err());
    }

    #[test]
    fn test_node_id_from_prefixed_str() {
        let id = NodeId::new_v4();
        let prefixed = id.to_string();
        let parsed = NodeId::from_prefixed_str(&prefixed).unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn test_cluster_id_from_prefixed_str() {
        let id = ClusterId::new_v4();
        let prefixed = id.to_string();
        let parsed = ClusterId::from_prefixed_str(&prefixed).unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn test_node_id_from_uuid() {
        let uuid = uuid::Uuid::new_v4();
        let id: NodeId = uuid.into();
        assert_eq!(id.as_uuid(), uuid);

        let uuid_back: uuid::Uuid = id.into();
        assert_eq!(uuid, uuid_back);
    }

    #[test]
    fn test_cluster_id_from_uuid() {
        let uuid = uuid::Uuid::new_v4();
        let id: ClusterId = uuid.into();
        assert_eq!(id.as_uuid(), uuid);

        let uuid_back: uuid::Uuid = id.into();
        assert_eq!(uuid, uuid_back);
    }
}
