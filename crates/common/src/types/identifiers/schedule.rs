//! Schedule identifier types.

use super::traits::{Identifier, IdentifierError, IdentifierPrefix, IdentifierValidation};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ScheduleId(uuid::Uuid);

impl ScheduleId {
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

impl Identifier for ScheduleId {
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
        "ScheduleId"
    }

    fn is_nil(&self) -> bool {
        self.0.is_nil()
    }
}

impl IdentifierPrefix for ScheduleId {
    fn prefix() -> &'static str {
        "sch_"
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

impl IdentifierValidation for ScheduleId {
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

impl fmt::Display for ScheduleId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", Self::prefix(), self.0.simple())
    }
}

impl FromStr for ScheduleId {
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

impl From<uuid::Uuid> for ScheduleId {
    fn from(uuid: uuid::Uuid) -> Self {
        Self(uuid)
    }
}

impl From<ScheduleId> for uuid::Uuid {
    fn from(id: ScheduleId) -> Self {
        id.0
    }
}

// PostgreSQL support: Type, Decode, Encode for sqlx
#[cfg(feature = "postgres")]
impl sqlx::Type<sqlx::Postgres> for ScheduleId {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <uuid::Uuid as sqlx::Type<sqlx::Postgres>>::type_info()
    }
}

#[cfg(feature = "postgres")]
impl<'r> sqlx::Decode<'r, sqlx::Postgres> for ScheduleId {
    fn decode(
        value: sqlx::postgres::PgValueRef<'r>,
    ) -> Result<Self, Box<dyn std::error::Error + 'static + Send + Sync>> {
        let uuid = <uuid::Uuid as sqlx::Decode<sqlx::Postgres>>::decode(value)?;
        Ok(Self(uuid))
    }
}

#[cfg(feature = "postgres")]
impl<'q> sqlx::Encode<'q, sqlx::Postgres> for ScheduleId {
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
    fn test_schedule_id_creation() {
        let id = ScheduleId::new_v4();
        assert!(id.is_valid());
        assert!(!id.is_nil());
    }

    #[test]
    fn test_schedule_id_display() {
        let id = ScheduleId::new_v4();
        let display = id.to_string();
        assert!(display.starts_with("sch_"));
        assert_eq!(display.len(), 36); // "sch_" + 32 hex digits
    }

    #[test]
    fn test_schedule_id_from_str() {
        let id = ScheduleId::new_v4();
        let id_str = id.to_string();
        let parsed = ScheduleId::from_str(&id_str).unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn test_schedule_id_validation() {
        // Valid ID
        let id = ScheduleId::new_v4();
        assert!(id.validate_format().is_ok());
        assert!(id.validate().is_ok());

        // Nil ID
        let nil_id = ScheduleId::from_uuid(uuid::Uuid::nil());
        assert!(nil_id.validate_format().is_err());
    }

    #[test]
    fn test_identifier_trait_schedule_id() {
        let id1 = ScheduleId::generate();
        let id2 = ScheduleId::generate();
        assert_ne!(id1, id2);
        assert!(id1.is_valid());
        assert_eq!(ScheduleId::type_name(), "ScheduleId");
    }
}
