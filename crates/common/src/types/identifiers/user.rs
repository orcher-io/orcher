//! User identifier types.

use super::traits::{Identifier, IdentifierError, IdentifierPrefix, IdentifierValidation};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct UserId(uuid::Uuid);

impl UserId {
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

impl Identifier for UserId {
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
        "UserId"
    }

    fn is_nil(&self) -> bool {
        self.0.is_nil()
    }
}

impl IdentifierPrefix for UserId {
    fn prefix() -> &'static str {
        "user_"
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

impl IdentifierValidation for UserId {
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

impl fmt::Display for UserId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", Self::prefix(), self.0.simple())
    }
}

impl FromStr for UserId {
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

impl From<uuid::Uuid> for UserId {
    fn from(uuid: uuid::Uuid) -> Self {
        Self(uuid)
    }
}

#[cfg(feature = "postgres")]
impl sqlx::Type<sqlx::Postgres> for UserId {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <uuid::Uuid as sqlx::Type<sqlx::Postgres>>::type_info()
    }
}

#[cfg(feature = "postgres")]
impl<'r> sqlx::Decode<'r, sqlx::Postgres> for UserId {
    fn decode(
        value: sqlx::postgres::PgValueRef<'r>,
    ) -> Result<Self, Box<dyn std::error::Error + 'static + Send + Sync>> {
        let uuid = <uuid::Uuid as sqlx::Decode<sqlx::Postgres>>::decode(value)?;
        Ok(Self(uuid))
    }
}

#[cfg(feature = "postgres")]
impl<'q> sqlx::Encode<'q, sqlx::Postgres> for UserId {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, Box<dyn std::error::Error + Send + Sync>> {
        <uuid::Uuid as sqlx::Encode<sqlx::Postgres>>::encode_by_ref(&self.0, buf)
    }
}

impl From<UserId> for uuid::Uuid {
    fn from(id: UserId) -> Self {
        id.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SessionId(uuid::Uuid);

impl SessionId {
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

impl Identifier for SessionId {
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
        "SessionId"
    }

    fn is_nil(&self) -> bool {
        self.0.is_nil()
    }
}

impl IdentifierPrefix for SessionId {
    fn prefix() -> &'static str {
        "sess_"
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

impl IdentifierValidation for SessionId {
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

impl fmt::Display for SessionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", Self::prefix(), self.0.simple())
    }
}

impl FromStr for SessionId {
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

impl From<uuid::Uuid> for SessionId {
    fn from(uuid: uuid::Uuid) -> Self {
        Self(uuid)
    }
}

impl From<SessionId> for uuid::Uuid {
    fn from(id: SessionId) -> Self {
        id.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ApiKeyId(uuid::Uuid);

impl ApiKeyId {
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

impl Identifier for ApiKeyId {
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
        "ApiKeyId"
    }

    fn is_nil(&self) -> bool {
        self.0.is_nil()
    }
}

impl IdentifierPrefix for ApiKeyId {
    fn prefix() -> &'static str {
        "key_"
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

impl IdentifierValidation for ApiKeyId {
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

impl fmt::Display for ApiKeyId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", Self::prefix(), self.0.simple())
    }
}

impl FromStr for ApiKeyId {
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

impl From<uuid::Uuid> for ApiKeyId {
    fn from(uuid: uuid::Uuid) -> Self {
        Self(uuid)
    }
}

impl From<ApiKeyId> for uuid::Uuid {
    fn from(id: ApiKeyId) -> Self {
        id.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_user_id_creation() {
        let id = UserId::new_v4();
        assert!(id.is_valid());
        assert!(!id.is_nil());
        assert_eq!(UserId::type_name(), "UserId");
    }

    #[test]
    fn test_user_id_display() {
        let id = UserId::new_v4();
        let display_str = id.to_string();
        assert!(display_str.starts_with("user_"));
    }

    #[test]
    fn test_user_id_from_str() {
        let id = UserId::new_v4();
        let id_str = id.to_string();
        let parsed_id = UserId::from_str(&id_str).unwrap();
        assert_eq!(id, parsed_id);
    }

    #[test]
    fn test_session_id_creation() {
        let id = SessionId::new_v4();
        assert!(id.is_valid());
        assert_eq!(SessionId::type_name(), "SessionId");
    }

    #[test]
    fn test_session_id_display() {
        let id = SessionId::new_v4();
        let display_str = id.to_string();
        assert!(display_str.starts_with("sess_"));
    }

    #[test]
    fn test_api_key_id_creation() {
        let id = ApiKeyId::new_v4();
        assert!(id.is_valid());
        assert_eq!(ApiKeyId::type_name(), "ApiKeyId");
    }

    #[test]
    fn test_api_key_id_display() {
        let id = ApiKeyId::new_v4();
        let display_str = id.to_string();
        assert!(display_str.starts_with("key_"));
    }

    #[test]
    fn test_identifier_validation() {
        let id = UserId::new_v4();
        assert!(id.validate().is_ok());

        let nil_id = UserId::from_uuid(uuid::Uuid::nil());
        assert!(nil_id.validate().is_err());
    }

    #[test]
    fn test_user_id_from_prefixed_str() {
        let id = UserId::new_v4();
        let prefixed = id.to_string();
        let parsed = UserId::from_prefixed_str(&prefixed).unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn test_session_id_from_prefixed_str() {
        let id = SessionId::new_v4();
        let prefixed = id.to_string();
        let parsed = SessionId::from_prefixed_str(&prefixed).unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn test_api_key_id_from_prefixed_str() {
        let id = ApiKeyId::new_v4();
        let prefixed = id.to_string();
        let parsed = ApiKeyId::from_prefixed_str(&prefixed).unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn test_user_id_from_uuid() {
        let uuid = uuid::Uuid::new_v4();
        let id: UserId = uuid.into();
        assert_eq!(id.as_uuid(), uuid);

        let uuid_back: uuid::Uuid = id.into();
        assert_eq!(uuid, uuid_back);
    }

    #[test]
    fn test_session_id_from_uuid() {
        let uuid = uuid::Uuid::new_v4();
        let id: SessionId = uuid.into();
        assert_eq!(id.as_uuid(), uuid);

        let uuid_back: uuid::Uuid = id.into();
        assert_eq!(uuid, uuid_back);
    }

    #[test]
    fn test_api_key_id_from_uuid() {
        let uuid = uuid::Uuid::new_v4();
        let id: ApiKeyId = uuid.into();
        assert_eq!(id.as_uuid(), uuid);

        let uuid_back: uuid::Uuid = id.into();
        assert_eq!(uuid, uuid_back);
    }
}
