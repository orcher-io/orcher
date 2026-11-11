//! Core traits for all ORCHER identifier types.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::hash::Hash;
use std::str::FromStr;

pub trait Identifier:
    Clone
    + Copy
    + PartialEq
    + Eq
    + Hash
    + Send
    + Sync
    + fmt::Display
    + fmt::Debug
    + FromStr
    + Serialize
    + for<'de> Deserialize<'de>
{
    fn generate() -> Self;
    fn is_valid(&self) -> bool;
    fn as_str(&self) -> String;
    fn type_name() -> &'static str;
    fn is_nil(&self) -> bool;
}

pub trait IdentifierPrefix: Identifier {
    fn prefix() -> &'static str;
    fn without_prefix(&self) -> String;
    fn from_prefixed_str(s: &str) -> Result<Self, IdentifierError>;
}

pub trait IdentifierValidation: Identifier {
    fn validate_format(&self) -> Result<(), IdentifierError>;
    fn validate_business_rules(&self) -> Result<(), IdentifierError>;

    fn validate(&self) -> Result<(), IdentifierError> {
        self.validate_format()?;
        self.validate_business_rules()?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum IdentifierError {
    InvalidFormat(String),
    InvalidPrefix { expected: String, found: String },
    InvalidLength { expected: usize, found: usize },
    InvalidCharacters(String),
    NilIdentifier,
    ValidationFailed(String),
}

impl fmt::Display for IdentifierError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidFormat(msg) => write!(f, "Invalid identifier format: {}", msg),
            Self::InvalidPrefix { expected, found } => {
                write!(
                    f,
                    "Invalid prefix: expected '{}', found '{}'",
                    expected, found
                )
            }
            Self::InvalidLength { expected, found } => {
                write!(f, "Invalid length: expected {}, found {}", expected, found)
            }
            Self::InvalidCharacters(chars) => {
                write!(f, "Invalid characters in identifier: {}", chars)
            }
            Self::NilIdentifier => write!(f, "Identifier cannot be nil/empty"),
            Self::ValidationFailed(msg) => write!(f, "Identifier validation failed: {}", msg),
        }
    }
}

impl std::error::Error for IdentifierError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_identifier_error_display() {
        let error = IdentifierError::InvalidFormat("test".to_string());
        assert!(error.to_string().contains("Invalid identifier format"));

        let error = IdentifierError::NilIdentifier;
        assert!(error.to_string().contains("cannot be nil"));
    }

    #[test]
    fn test_identifier_error_serialization() {
        let error = IdentifierError::InvalidFormat("test".to_string());
        let serialized = serde_json::to_string(&error).unwrap();
        let deserialized: IdentifierError = serde_json::from_str(&serialized).unwrap();
        assert_eq!(error, deserialized);
    }
}
