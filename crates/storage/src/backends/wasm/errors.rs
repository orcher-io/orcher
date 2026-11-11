//! Storage-specific errors for WASM binary operations
//!
//! This module defines error types specific to WASM storage operations,
//! which are then converted to the standard `OrcherError` type.

use thiserror::Error;

/// Errors that can occur during WASM storage operations
#[derive(Debug, Error)]
pub enum StorageError {
    /// WASM binary not found
    #[error("WASM binary not found: {key}")]
    NotFound {
        /// Storage key that was not found
        key: String,
    },

    /// Storage operation failed
    #[error("Storage operation failed: {message}")]
    OperationFailed {
        /// Error message
        message: String,
        /// Optional source error
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },

    /// Invalid WASM binary
    #[error("Invalid WASM binary: {reason}")]
    InvalidWasm {
        /// Reason for invalidity
        reason: String,
    },

    /// Storage backend unavailable
    #[error("Storage backend unavailable: {backend}")]
    BackendUnavailable {
        /// Backend identifier
        backend: String,
        /// Optional error details
        details: Option<String>,
    },

    /// Checksum mismatch
    #[error("Checksum mismatch: expected {expected}, got {actual}")]
    ChecksumMismatch {
        /// Expected checksum
        expected: String,
        /// Actual checksum
        actual: String,
    },

    /// Invalid storage key
    #[error("Invalid storage key: {key} - {reason}")]
    InvalidKey {
        /// Invalid key
        key: String,
        /// Reason for invalidity
        reason: String,
    },

    /// Storage quota exceeded
    #[error("Storage quota exceeded: {message}")]
    QuotaExceeded {
        /// Error message
        message: String,
        /// Current usage in bytes
        current_usage: Option<u64>,
        /// Quota limit in bytes
        quota_limit: Option<u64>,
    },

    /// Metadata error
    #[error("Metadata error: {message}")]
    MetadataError {
        /// Error message
        message: String,
    },

    /// Serialization error
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// I/O error
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

impl StorageError {
    /// Create a not found error
    pub fn not_found(key: impl Into<String>) -> Self {
        Self::NotFound { key: key.into() }
    }

    /// Create an operation failed error
    pub fn operation_failed(message: impl Into<String>) -> Self {
        Self::OperationFailed {
            message: message.into(),
            source: None,
        }
    }

    /// Create an operation failed error with source
    pub fn operation_failed_with_source(
        message: impl Into<String>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self::OperationFailed {
            message: message.into(),
            source: Some(Box::new(source)),
        }
    }

    /// Create an invalid WASM error
    pub fn invalid_wasm(reason: impl Into<String>) -> Self {
        Self::InvalidWasm {
            reason: reason.into(),
        }
    }

    /// Create a backend unavailable error
    pub fn backend_unavailable(backend: impl Into<String>) -> Self {
        Self::BackendUnavailable {
            backend: backend.into(),
            details: None,
        }
    }

    /// Create a backend unavailable error with details
    pub fn backend_unavailable_with_details(
        backend: impl Into<String>,
        details: impl Into<String>,
    ) -> Self {
        Self::BackendUnavailable {
            backend: backend.into(),
            details: Some(details.into()),
        }
    }

    /// Create a checksum mismatch error
    pub fn checksum_mismatch(expected: impl Into<String>, actual: impl Into<String>) -> Self {
        Self::ChecksumMismatch {
            expected: expected.into(),
            actual: actual.into(),
        }
    }

    /// Create an invalid key error
    pub fn invalid_key(key: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::InvalidKey {
            key: key.into(),
            reason: reason.into(),
        }
    }

    /// Create a quota exceeded error
    pub fn quota_exceeded(message: impl Into<String>) -> Self {
        Self::QuotaExceeded {
            message: message.into(),
            current_usage: None,
            quota_limit: None,
        }
    }

    /// Create a metadata error
    pub fn metadata_error(message: impl Into<String>) -> Self {
        Self::MetadataError {
            message: message.into(),
        }
    }
}

impl From<StorageError> for orcher_common::error::OrcherError {
    fn from(err: StorageError) -> Self {
        use orcher_common::error::OrcherError;

        match err {
            StorageError::NotFound { key } => OrcherError::NotFound {
                resource_type: "WASM binary".to_string(),
                id: key,
            },
            StorageError::OperationFailed { message, .. } => OrcherError::Storage {
                operation: "storage".to_string(),
                message,
            },
            StorageError::InvalidWasm { reason } => OrcherError::InvalidInput {
                message: reason,
                field: Some("wasm_binary".to_string()),
            },
            StorageError::BackendUnavailable { backend, details } => {
                let message = if let Some(details) = details {
                    format!("Backend {} unavailable: {}", backend, details)
                } else {
                    format!("Backend {} unavailable", backend)
                };
                OrcherError::Storage {
                    operation: "backend_check".to_string(),
                    message,
                }
            }
            StorageError::ChecksumMismatch { expected, actual } => OrcherError::InvalidInput {
                message: format!("Checksum mismatch: expected {}, got {}", expected, actual),
                field: Some("checksum".to_string()),
            },
            StorageError::InvalidKey { key, reason } => OrcherError::InvalidInput {
                message: format!("Invalid key '{}': {}", key, reason),
                field: Some("key".to_string()),
            },
            StorageError::QuotaExceeded {
                message,
                current_usage,
                quota_limit,
            } => {
                let full_message = if let (Some(usage), Some(limit)) = (current_usage, quota_limit)
                {
                    format!(
                        "{} (usage: {} bytes, limit: {} bytes)",
                        message, usage, limit
                    )
                } else {
                    message
                };
                OrcherError::Storage {
                    operation: "quota_check".to_string(),
                    message: full_message,
                }
            }
            StorageError::MetadataError { message } => OrcherError::Storage {
                operation: "metadata".to_string(),
                message: format!("Metadata error: {}", message),
            },
            StorageError::Serialization(err) => OrcherError::Serialization {
                format: "json".to_string(),
                message: err.to_string(),
            },
            StorageError::Io(err) => OrcherError::Internal {
                message: format!("I/O error: {}", err),
                source: Some(err.to_string()),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_not_found_error() {
        let err = StorageError::not_found("workflows/test/1.0.0/abc.wasm");
        assert!(matches!(err, StorageError::NotFound { .. }));
        assert_eq!(
            err.to_string(),
            "WASM binary not found: workflows/test/1.0.0/abc.wasm"
        );
    }

    #[test]
    fn test_operation_failed_error() {
        let err = StorageError::operation_failed("Failed to upload");
        assert!(matches!(err, StorageError::OperationFailed { .. }));
        assert_eq!(
            err.to_string(),
            "Storage operation failed: Failed to upload"
        );
    }

    #[test]
    fn test_invalid_wasm_error() {
        let err = StorageError::invalid_wasm("Missing WASM magic number");
        assert!(matches!(err, StorageError::InvalidWasm { .. }));
        assert_eq!(
            err.to_string(),
            "Invalid WASM binary: Missing WASM magic number"
        );
    }

    #[test]
    fn test_backend_unavailable_error() {
        let err = StorageError::backend_unavailable("s3");
        assert!(matches!(err, StorageError::BackendUnavailable { .. }));
        assert_eq!(err.to_string(), "Storage backend unavailable: s3");
    }

    #[test]
    fn test_checksum_mismatch_error() {
        let err = StorageError::checksum_mismatch("abc123", "def456");
        assert!(matches!(err, StorageError::ChecksumMismatch { .. }));
        assert_eq!(
            err.to_string(),
            "Checksum mismatch: expected abc123, got def456"
        );
    }

    #[test]
    fn test_invalid_key_error() {
        let err = StorageError::invalid_key("invalid//key", "contains double slashes");
        assert!(matches!(err, StorageError::InvalidKey { .. }));
        assert_eq!(
            err.to_string(),
            "Invalid storage key: invalid//key - contains double slashes"
        );
    }

    #[test]
    fn test_quota_exceeded_error() {
        let err = StorageError::quota_exceeded("Storage limit reached");
        assert!(matches!(err, StorageError::QuotaExceeded { .. }));
        assert_eq!(
            err.to_string(),
            "Storage quota exceeded: Storage limit reached"
        );
    }

    #[test]
    fn test_conversion_to_orcher_error() {
        let storage_err = StorageError::not_found("test.wasm");
        let orcher_err: orcher_common::error::OrcherError = storage_err.into();
        assert!(matches!(
            orcher_err,
            orcher_common::error::OrcherError::NotFound { .. }
        ));
    }
}
