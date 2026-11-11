//! # Storage Error Types
//!
//! Storage-specific error handling with orcher-common integration.

use orcher_common::error::OrcherError;
use thiserror::Error;

/// Storage result type alias
pub type StorageResult<T> = Result<T, StorageError>;

/// Storage-specific error types
#[derive(Debug, Error)]
pub enum StorageError {
    /// Database connection error
    #[error("Database connection failed: {message}")]
    ConnectionFailed {
        /// Error message describing the connection failure
        message: String,
    },

    /// Entity not found error (simple message)
    #[error("Not found: {message}")]
    NotFound {
        /// Error message
        message: String,
    },

    /// Validation error
    #[error("Validation failed: {field} - {message}")]
    ValidationFailed {
        /// Field that failed validation
        field: String,
        /// Validation error message
        message: String,
    },

    /// Generic database error
    #[error("Database error: {0}")]
    DatabaseError(String),

    /// Database error with source
    #[error("Database error: {message}")]
    Database {
        /// Error message
        message: String,
        /// Source error
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// Conflict error (duplicate key, constraint violation, etc.)
    #[error("Conflict: {message}")]
    Conflict {
        /// Error message
        message: String,
    },
}

impl From<StorageError> for OrcherError {
    fn from(err: StorageError) -> Self {
        match err {
            StorageError::ConnectionFailed { message } => {
                OrcherError::internal(format!("Database connection failed: {}", message))
            }
            StorageError::NotFound { message } => {
                OrcherError::internal(format!("Not found: {}", message))
            }
            StorageError::ValidationFailed { field, message } => OrcherError::Validation {
                field,
                message,
                rule: None,
            },
            StorageError::DatabaseError(msg) => {
                OrcherError::internal(format!("Database error: {}", msg))
            }
            StorageError::Database { message, .. } => {
                OrcherError::internal(format!("Database error: {}", message))
            }
            StorageError::Conflict { message } => {
                OrcherError::internal(format!("Conflict: {}", message))
            }
        }
    }
}
