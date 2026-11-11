//! Organization identifier types.

use super::traits::{Identifier, IdentifierError, IdentifierPrefix, IdentifierValidation};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OrganizationId(Uuid);

impl OrganizationId {
    pub fn new_v4() -> Self {
        Self(Uuid::new_v4())
    }

    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    pub fn as_uuid(&self) -> Uuid {
        self.0
    }

    /// Get a reference to the underlying UUID.
    pub fn uuid(&self) -> &Uuid {
        &self.0
    }
}

impl Identifier for OrganizationId {
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
        "OrganizationId"
    }

    fn is_nil(&self) -> bool {
        self.0.is_nil()
    }
}

impl IdentifierPrefix for OrganizationId {
    fn prefix() -> &'static str {
        "org_"
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

impl IdentifierValidation for OrganizationId {
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

impl fmt::Display for OrganizationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", Self::prefix(), self.0.simple())
    }
}

impl FromStr for OrganizationId {
    type Err = IdentifierError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Try prefixed format first
        if s.starts_with(Self::prefix()) {
            return Self::from_prefixed_str(s);
        }

        // Fallback to parsing as plain UUID
        uuid::Uuid::parse_str(s).map(Self).map_err(|_| {
            IdentifierError::InvalidFormat(format!("Invalid UUID or prefixed format: {}", s))
        })
    }
}

#[cfg(feature = "postgres")]
impl sqlx::Type<sqlx::Postgres> for OrganizationId {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <uuid::Uuid as sqlx::Type<sqlx::Postgres>>::type_info()
    }
}

#[cfg(feature = "postgres")]
impl<'r> sqlx::Decode<'r, sqlx::Postgres> for OrganizationId {
    fn decode(
        value: sqlx::postgres::PgValueRef<'r>,
    ) -> Result<Self, Box<dyn std::error::Error + 'static + Send + Sync>> {
        let uuid = <uuid::Uuid as sqlx::Decode<sqlx::Postgres>>::decode(value)?;
        Ok(Self(uuid))
    }
}

#[cfg(feature = "postgres")]
impl<'q> sqlx::Encode<'q, sqlx::Postgres> for OrganizationId {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, Box<dyn std::error::Error + Send + Sync>> {
        <uuid::Uuid as sqlx::Encode<sqlx::Postgres>>::encode_by_ref(&self.0, buf)
    }
}

/// Organization member relationship identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OrganizationMemberId(Uuid);

impl OrganizationMemberId {
    pub fn new_v4() -> Self {
        Self(Uuid::new_v4())
    }

    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    pub fn as_uuid(&self) -> Uuid {
        self.0
    }

    /// Get a reference to the underlying UUID.
    pub fn uuid(&self) -> &Uuid {
        &self.0
    }
}

impl Identifier for OrganizationMemberId {
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
        "OrganizationMemberId"
    }

    fn is_nil(&self) -> bool {
        self.0.is_nil()
    }
}

impl IdentifierPrefix for OrganizationMemberId {
    fn prefix() -> &'static str {
        "orgm_"
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

impl IdentifierValidation for OrganizationMemberId {
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

impl fmt::Display for OrganizationMemberId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", Self::prefix(), self.0.simple())
    }
}

impl FromStr for OrganizationMemberId {
    type Err = IdentifierError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.starts_with(Self::prefix()) {
            return Self::from_prefixed_str(s);
        }

        uuid::Uuid::parse_str(s).map(Self).map_err(|_| {
            IdentifierError::InvalidFormat(format!("Invalid UUID or prefixed format: {}", s))
        })
    }
}

#[cfg(feature = "postgres")]
impl sqlx::Type<sqlx::Postgres> for OrganizationMemberId {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <uuid::Uuid as sqlx::Type<sqlx::Postgres>>::type_info()
    }
}

#[cfg(feature = "postgres")]
impl<'r> sqlx::Decode<'r, sqlx::Postgres> for OrganizationMemberId {
    fn decode(
        value: sqlx::postgres::PgValueRef<'r>,
    ) -> Result<Self, Box<dyn std::error::Error + 'static + Send + Sync>> {
        let uuid = <uuid::Uuid as sqlx::Decode<sqlx::Postgres>>::decode(value)?;
        Ok(Self(uuid))
    }
}

#[cfg(feature = "postgres")]
impl<'q> sqlx::Encode<'q, sqlx::Postgres> for OrganizationMemberId {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, Box<dyn std::error::Error + Send + Sync>> {
        <uuid::Uuid as sqlx::Encode<sqlx::Postgres>>::encode_by_ref(&self.0, buf)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OrganizationInvitationId(Uuid);

impl OrganizationInvitationId {
    pub fn new_v4() -> Self {
        Self(Uuid::new_v4())
    }

    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    pub fn as_uuid(&self) -> Uuid {
        self.0
    }

    /// Get a reference to the underlying UUID.
    pub fn uuid(&self) -> &Uuid {
        &self.0
    }
}

impl Identifier for OrganizationInvitationId {
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
        "OrganizationInvitationId"
    }

    fn is_nil(&self) -> bool {
        self.0.is_nil()
    }
}

impl IdentifierPrefix for OrganizationInvitationId {
    fn prefix() -> &'static str {
        "orgi_"
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

impl IdentifierValidation for OrganizationInvitationId {
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

impl fmt::Display for OrganizationInvitationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", Self::prefix(), self.0.simple())
    }
}

impl FromStr for OrganizationInvitationId {
    type Err = IdentifierError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.starts_with(Self::prefix()) {
            return Self::from_prefixed_str(s);
        }

        uuid::Uuid::parse_str(s).map(Self).map_err(|_| {
            IdentifierError::InvalidFormat(format!("Invalid UUID or prefixed format: {}", s))
        })
    }
}

#[cfg(feature = "postgres")]
impl sqlx::Type<sqlx::Postgres> for OrganizationInvitationId {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <uuid::Uuid as sqlx::Type<sqlx::Postgres>>::type_info()
    }
}

#[cfg(feature = "postgres")]
impl<'r> sqlx::Decode<'r, sqlx::Postgres> for OrganizationInvitationId {
    fn decode(
        value: sqlx::postgres::PgValueRef<'r>,
    ) -> Result<Self, Box<dyn std::error::Error + 'static + Send + Sync>> {
        let uuid = <uuid::Uuid as sqlx::Decode<sqlx::Postgres>>::decode(value)?;
        Ok(Self(uuid))
    }
}

#[cfg(feature = "postgres")]
impl<'q> sqlx::Encode<'q, sqlx::Postgres> for OrganizationInvitationId {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, Box<dyn std::error::Error + Send + Sync>> {
        <uuid::Uuid as sqlx::Encode<sqlx::Postgres>>::encode_by_ref(&self.0, buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_organization_id_creation() {
        let id = OrganizationId::new_v4();
        assert!(id.is_valid());
        assert!(!id.as_uuid().is_nil());
    }

    #[test]
    fn test_organization_id_from_uuid() {
        let uuid = Uuid::new_v4();
        let id = OrganizationId::from_uuid(uuid);
        assert_eq!(id.as_uuid(), uuid);
    }

    #[test]
    fn test_organization_id_display() {
        let id = OrganizationId::new_v4();
        let display = format!("{}", id);
        assert!(!display.is_empty());
    }

    #[test]
    fn test_organization_id_from_str() {
        let uuid = uuid::Uuid::new_v4();
        let uuid_str = uuid.to_string();

        // Test parsing UUID string
        let id = OrganizationId::from_str(&uuid_str).unwrap();
        assert_eq!(id.as_uuid(), uuid);

        // Test parsing prefixed string
        let prefixed = format!("org_{}", uuid.simple());
        let id2 = OrganizationId::from_str(&prefixed).unwrap();
        assert_eq!(id2.as_uuid(), uuid);
    }

    #[test]
    fn test_organization_id_invalid_format() {
        let result = OrganizationId::from_str("invalid");
        assert!(result.is_err());
    }

    #[test]
    fn test_organization_member_id_creation() {
        let id = OrganizationMemberId::new_v4();
        assert!(id.is_valid());
        assert!(!id.as_uuid().is_nil());
    }

    #[test]
    fn test_organization_invitation_id_creation() {
        let id = OrganizationInvitationId::new_v4();
        assert!(id.is_valid());
        assert!(!id.as_uuid().is_nil());
    }

    #[test]
    fn test_organization_id_equality() {
        let uuid = uuid::Uuid::new_v4();
        let id1 = OrganizationId::from_uuid(uuid);
        let id2 = OrganizationId::from_uuid(uuid);
        assert_eq!(id1, id2);
    }

    #[test]
    fn test_organization_id_hash() {
        use std::collections::HashSet;

        let id1 = OrganizationId::new_v4();
        let id2 = OrganizationId::new_v4();

        let mut set = HashSet::new();
        set.insert(id1);
        set.insert(id2);

        assert_eq!(set.len(), 2);
    }

    #[test]
    fn test_organization_id_serialization() {
        let id = OrganizationId::new_v4();
        let json = serde_json::to_string(&id).unwrap();
        let deserialized: OrganizationId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, deserialized);
    }

    #[test]
    fn test_identifier_trait_implementation() {
        let id = OrganizationId::generate();
        assert!(id.is_valid());
        assert_eq!(OrganizationId::prefix(), "org_");
    }
}
