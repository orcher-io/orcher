//! Authentication and authorization identifier types.

use super::traits::{Identifier, IdentifierError, IdentifierPrefix, IdentifierValidation};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RoleId(uuid::Uuid);

impl RoleId {
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

impl Identifier for RoleId {
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
        "RoleId"
    }

    fn is_nil(&self) -> bool {
        self.0.is_nil()
    }
}

impl IdentifierPrefix for RoleId {
    fn prefix() -> &'static str {
        "rol_"
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

impl IdentifierValidation for RoleId {
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

impl fmt::Display for RoleId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", Self::prefix(), self.0.simple())
    }
}

impl FromStr for RoleId {
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

impl From<uuid::Uuid> for RoleId {
    fn from(uuid: uuid::Uuid) -> Self {
        Self(uuid)
    }
}

impl From<RoleId> for uuid::Uuid {
    fn from(id: RoleId) -> Self {
        id.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PermissionId(uuid::Uuid);

impl PermissionId {
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

impl Identifier for PermissionId {
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
        "PermissionId"
    }

    fn is_nil(&self) -> bool {
        self.0.is_nil()
    }
}

impl IdentifierPrefix for PermissionId {
    fn prefix() -> &'static str {
        "prm_"
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

impl IdentifierValidation for PermissionId {
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

impl fmt::Display for PermissionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", Self::prefix(), self.0.simple())
    }
}

impl FromStr for PermissionId {
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

impl From<uuid::Uuid> for PermissionId {
    fn from(uuid: uuid::Uuid) -> Self {
        Self(uuid)
    }
}

impl From<PermissionId> for uuid::Uuid {
    fn from(id: PermissionId) -> Self {
        id.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_role_id_creation() {
        let id = RoleId::new_v4();
        assert!(id.is_valid());
        assert!(!id.is_nil());
    }

    #[test]
    fn test_role_id_display() {
        let id = RoleId::new_v4();
        let display = id.to_string();
        assert!(display.starts_with("rol_"));
        assert_eq!(display.len(), 36); // "rol_" + 32 hex digits
    }

    #[test]
    fn test_role_id_from_str() {
        let id = RoleId::new_v4();
        let id_str = id.to_string();
        let parsed = RoleId::from_str(&id_str).unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn test_role_id_validation() {
        let id = RoleId::new_v4();
        assert!(id.validate_format().is_ok());
        assert!(id.validate().is_ok());

        let nil_id = RoleId::from_uuid(uuid::Uuid::nil());
        assert!(nil_id.validate_format().is_err());
    }

    #[test]
    fn test_permission_id_creation() {
        let id = PermissionId::new_v4();
        assert!(id.is_valid());
        assert!(!id.is_nil());
    }

    #[test]
    fn test_permission_id_display() {
        let id = PermissionId::new_v4();
        let display = id.to_string();
        assert!(display.starts_with("prm_"));
        assert_eq!(display.len(), 36); // "prm_" + 32 hex digits
    }

    #[test]
    fn test_permission_id_from_str() {
        let id = PermissionId::new_v4();
        let id_str = id.to_string();
        let parsed = PermissionId::from_str(&id_str).unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn test_identifier_trait_role_id() {
        let id1 = RoleId::generate();
        let id2 = RoleId::generate();
        assert_ne!(id1, id2);
        assert!(id1.is_valid());
        assert_eq!(RoleId::type_name(), "RoleId");
    }

    #[test]
    fn test_identifier_trait_permission_id() {
        let id1 = PermissionId::generate();
        let id2 = PermissionId::generate();
        assert_ne!(id1, id2);
        assert!(id1.is_valid());
        assert_eq!(PermissionId::type_name(), "PermissionId");
    }

    #[test]
    fn test_policy_id_creation() {
        let id = PolicyId::new_v4();
        assert!(id.is_valid());
        assert!(!id.is_nil());
    }

    #[test]
    fn test_policy_id_display() {
        let id = PolicyId::new_v4();
        let display = id.to_string();
        assert!(display.starts_with("pol_"));
        assert_eq!(display.len(), 36); // "pol_" + 32 hex digits
    }

    #[test]
    fn test_policy_id_from_str() {
        let id = PolicyId::new_v4();
        let id_str = id.to_string();
        let parsed = PolicyId::from_str(&id_str).unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn test_policy_id_validation() {
        let id = PolicyId::new_v4();
        assert!(id.validate_format().is_ok());
        assert!(id.validate().is_ok());

        let nil_id = PolicyId::from_uuid(uuid::Uuid::nil());
        assert!(nil_id.validate_format().is_err());
    }

    #[test]
    fn test_identifier_trait_policy_id() {
        let id1 = PolicyId::generate();
        let id2 = PolicyId::generate();
        assert_ne!(id1, id2);
        assert!(id1.is_valid());
        assert_eq!(PolicyId::type_name(), "PolicyId");
    }

    #[test]
    fn test_crypto_key_id_creation() {
        let id = CryptoKeyId::new_v4();
        assert!(id.is_valid());
        assert!(!id.is_nil());
    }

    #[test]
    fn test_crypto_key_id_display() {
        let id = CryptoKeyId::new_v4();
        let display = id.to_string();
        assert!(display.starts_with("key_"));
        assert_eq!(display.len(), 36); // "key_" + 32 hex digits
    }

    #[test]
    fn test_crypto_key_id_from_str() {
        let id = CryptoKeyId::new_v4();
        let id_str = id.to_string();
        let parsed = CryptoKeyId::from_str(&id_str).unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn test_crypto_key_id_validation() {
        let id = CryptoKeyId::new_v4();
        assert!(id.validate_format().is_ok());
        assert!(id.validate().is_ok());

        let nil_id = CryptoKeyId::from_uuid(uuid::Uuid::nil());
        assert!(nil_id.validate_format().is_err());
    }

    #[test]
    fn test_identifier_trait_crypto_key_id() {
        let id1 = CryptoKeyId::generate();
        let id2 = CryptoKeyId::generate();
        assert_ne!(id1, id2);
        assert!(id1.is_valid());
        assert_eq!(CryptoKeyId::type_name(), "CryptoKeyId");
    }

    #[test]
    fn test_certificate_id_creation() {
        let id = CertificateId::new_v4();
        assert!(id.is_valid());
        assert!(!id.is_nil());
    }

    #[test]
    fn test_certificate_id_display() {
        let id = CertificateId::new_v4();
        let display = id.to_string();
        assert!(display.starts_with("crt_"));
        assert_eq!(display.len(), 36); // "crt_" + 32 hex digits
    }

    #[test]
    fn test_certificate_id_from_str() {
        let id = CertificateId::new_v4();
        let id_str = id.to_string();
        let parsed = CertificateId::from_str(&id_str).unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn test_certificate_id_validation() {
        let id = CertificateId::new_v4();
        assert!(id.validate_format().is_ok());
        assert!(id.validate().is_ok());

        let nil_id = CertificateId::from_uuid(uuid::Uuid::nil());
        assert!(nil_id.validate_format().is_err());
    }

    #[test]
    fn test_identifier_trait_certificate_id() {
        let id1 = CertificateId::generate();
        let id2 = CertificateId::generate();
        assert_ne!(id1, id2);
        assert!(id1.is_valid());
        assert_eq!(CertificateId::type_name(), "CertificateId");
    }

    #[test]
    fn test_refresh_token_id_creation() {
        let id = RefreshTokenId::new_v4();
        assert!(id.is_valid());
        assert!(!id.is_nil());
    }

    #[test]
    fn test_refresh_token_id_display() {
        let id = RefreshTokenId::new_v4();
        let display = id.to_string();
        assert!(display.starts_with("rtk_"));
        assert_eq!(display.len(), 36); // "rtk_" + 32 hex digits
    }

    #[test]
    fn test_refresh_token_id_from_str() {
        let id = RefreshTokenId::new_v4();
        let id_str = id.to_string();
        let parsed = RefreshTokenId::from_str(&id_str).unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn test_refresh_token_id_validation() {
        let id = RefreshTokenId::new_v4();
        assert!(id.validate_format().is_ok());
        assert!(id.validate().is_ok());

        let nil_id = RefreshTokenId::from_uuid(uuid::Uuid::nil());
        assert!(nil_id.validate_format().is_err());
    }

    #[test]
    fn test_identifier_trait_refresh_token_id() {
        let id1 = RefreshTokenId::generate();
        let id2 = RefreshTokenId::generate();
        assert_ne!(id1, id2);
        assert!(id1.is_valid());
        assert_eq!(RefreshTokenId::type_name(), "RefreshTokenId");
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PolicyId(uuid::Uuid);

impl PolicyId {
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

impl Identifier for PolicyId {
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
        "PolicyId"
    }

    fn is_nil(&self) -> bool {
        self.0.is_nil()
    }
}

impl IdentifierPrefix for PolicyId {
    fn prefix() -> &'static str {
        "pol_"
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

impl IdentifierValidation for PolicyId {
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

impl fmt::Display for PolicyId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", Self::prefix(), self.0.simple())
    }
}

impl FromStr for PolicyId {
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

impl From<uuid::Uuid> for PolicyId {
    fn from(uuid: uuid::Uuid) -> Self {
        Self(uuid)
    }
}

impl From<PolicyId> for uuid::Uuid {
    fn from(id: PolicyId) -> Self {
        id.0
    }
}

/// Cryptographic key identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CryptoKeyId(uuid::Uuid);

impl CryptoKeyId {
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

impl Identifier for CryptoKeyId {
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
        "CryptoKeyId"
    }

    fn is_nil(&self) -> bool {
        self.0.is_nil()
    }
}

impl IdentifierPrefix for CryptoKeyId {
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

impl IdentifierValidation for CryptoKeyId {
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

impl fmt::Display for CryptoKeyId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", Self::prefix(), self.0.simple())
    }
}

impl FromStr for CryptoKeyId {
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

impl From<uuid::Uuid> for CryptoKeyId {
    fn from(uuid: uuid::Uuid) -> Self {
        Self(uuid)
    }
}

impl From<CryptoKeyId> for uuid::Uuid {
    fn from(id: CryptoKeyId) -> Self {
        id.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CertificateId(uuid::Uuid);

impl CertificateId {
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

impl Identifier for CertificateId {
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
        "CertificateId"
    }

    fn is_nil(&self) -> bool {
        self.0.is_nil()
    }
}

impl IdentifierPrefix for CertificateId {
    fn prefix() -> &'static str {
        "crt_"
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

impl IdentifierValidation for CertificateId {
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

impl fmt::Display for CertificateId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", Self::prefix(), self.0.simple())
    }
}

impl FromStr for CertificateId {
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

impl From<uuid::Uuid> for CertificateId {
    fn from(uuid: uuid::Uuid) -> Self {
        Self(uuid)
    }
}

impl From<CertificateId> for uuid::Uuid {
    fn from(id: CertificateId) -> Self {
        id.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RefreshTokenId(uuid::Uuid);

impl RefreshTokenId {
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

impl Identifier for RefreshTokenId {
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
        "RefreshTokenId"
    }

    fn is_nil(&self) -> bool {
        self.0.is_nil()
    }
}

impl IdentifierPrefix for RefreshTokenId {
    fn prefix() -> &'static str {
        "rtk_"
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

impl IdentifierValidation for RefreshTokenId {
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

impl fmt::Display for RefreshTokenId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", Self::prefix(), self.0.simple())
    }
}

impl FromStr for RefreshTokenId {
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

impl From<uuid::Uuid> for RefreshTokenId {
    fn from(uuid: uuid::Uuid) -> Self {
        Self(uuid)
    }
}

impl From<RefreshTokenId> for uuid::Uuid {
    fn from(id: RefreshTokenId) -> Self {
        id.0
    }
}
