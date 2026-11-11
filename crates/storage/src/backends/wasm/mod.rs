//! # WASM Binary Storage Abstraction
//!
//! Unified interface for WASM binary storage across different backends.
//!
//! ## Overview
//!
//! This module provides a trait-based abstraction for storing and retrieving WASM binaries.
//! It supports multiple backend implementations:
//!
//! - **Filesystem**: Local file storage (for development)
//! - **S3**: AWS S3 storage (for production)
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────┐
//! │         WasmStorage Trait                   │
//! │  (Common interface for all backends)        │
//! └──────────────┬──────────────────────────────┘
//!                │
//!      ┌─────────┴─────────┐
//!      │                   │
//!      ▼                   ▼
//! ┌──────────┐      ┌──────────┐
//! │Filesystem│      │    S3    │
//! │ Storage  │      │ Storage  │
//! └──────────┘      └──────────┘
//! ```
//!
//! ## Usage
//!
//! ```rust,no_run
//! use orcher_storage::backends::wasm::{WasmStorage, StorageFactory, StorageMetadata};
//!
//! # async fn example() -> orcher_common::OrcherResult<()> {
//! // Create storage backend from environment
//! let storage = StorageFactory::from_env().await?;
//!
//! // Store a WASM binary
//! let metadata = StorageMetadata {
//!     package_name: "my-workflow".to_string(),
//!     package_version: "1.0.0".to_string(),
//!     sha256: "abc123...".to_string(),
//!     size_bytes: 1024,
//!     content_type: "application/wasm".to_string(),
//!     manifest: None,
//! };
//!
//! let key = storage.generate_key("my-workflow", "1.0.0", "abc123");
//! let wasm_bytes = vec![0x00, 0x61, 0x73, 0x6d]; // WASM magic number
//! storage.store(&key, &wasm_bytes, metadata).await?;
//!
//! // Retrieve the WASM binary
//! let binary = storage.retrieve(&key).await?;
//! # Ok(())
//! # }
//! ```

pub mod errors;
pub mod factory;
pub mod types;

use async_trait::async_trait;
use orcher_common::OrcherResult;
use std::fmt::Debug;

pub use errors::*;
pub use factory::*;
pub use types::*;

/// WASM binary storage trait
///
/// This trait defines the interface that all WASM storage backends must implement.
/// It provides methods for storing, retrieving, listing, and managing WASM binaries.
///
/// # Implementations
///
/// - [`FilesystemStorage`](crate::backends::filesystem::FilesystemStorage) - Local filesystem storage
/// - [`S3Storage`](crate::backends::s3::S3Storage) - AWS S3 storage
///
/// # Thread Safety
///
/// All implementations must be `Send + Sync` to support concurrent access.
#[async_trait]
pub trait WasmStorage: Send + Sync + Debug {
    /// Store a WASM binary with metadata
    ///
    /// # Arguments
    ///
    /// * `key` - Storage key (e.g., "workflows/my-workflow/1.0.0/abc123.wasm")
    /// * `wasm_bytes` - WASM binary data
    /// * `metadata` - Metadata about the WASM binary
    ///
    /// # Returns
    ///
    /// Returns information about the stored WASM binary including storage path and timestamp.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The WASM binary is empty
    /// - The WASM binary is invalid
    /// - Storage operation fails
    async fn store(
        &self,
        key: &str,
        wasm_bytes: &[u8],
        metadata: StorageMetadata,
    ) -> OrcherResult<StoredWasm>;

    /// Retrieve a WASM binary
    ///
    /// # Arguments
    ///
    /// * `key` - Storage key
    ///
    /// # Returns
    ///
    /// Returns the WASM binary data along with its metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The key does not exist
    /// - Storage operation fails
    async fn retrieve(&self, key: &str) -> OrcherResult<WasmBinary>;

    /// Check if a WASM binary exists
    ///
    /// # Arguments
    ///
    /// * `key` - Storage key
    ///
    /// # Returns
    ///
    /// Returns `true` if the WASM binary exists, `false` otherwise.
    async fn exists(&self, key: &str) -> OrcherResult<bool>;

    /// Delete a WASM binary
    ///
    /// # Arguments
    ///
    /// * `key` - Storage key
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The key does not exist
    /// - Storage operation fails
    async fn delete(&self, key: &str) -> OrcherResult<()>;

    /// List WASM binaries with optional prefix filter
    ///
    /// # Arguments
    ///
    /// * `prefix` - Optional prefix to filter results (e.g., "workflows/my-workflow/")
    ///
    /// # Returns
    ///
    /// Returns a list of WASM binary information.
    ///
    /// # Errors
    ///
    /// Returns an error if storage operation fails.
    async fn list(&self, prefix: Option<&str>) -> OrcherResult<Vec<WasmInfo>>;

    /// Get metadata for a WASM binary
    ///
    /// # Arguments
    ///
    /// * `key` - Storage key
    ///
    /// # Returns
    ///
    /// Returns the metadata for the WASM binary.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The key does not exist
    /// - Storage operation fails
    async fn get_metadata(&self, key: &str) -> OrcherResult<StorageMetadata>;

    /// Generate a storage key from workflow information
    ///
    /// The default implementation generates keys in the format:
    /// `workflows/{package_name}/{package_version}/{sha256}.wasm`
    ///
    /// # Arguments
    ///
    /// * `package_name` - Workflow package name
    /// * `package_version` - Workflow package version
    /// * `sha256` - SHA256 checksum of the WASM binary
    ///
    /// # Returns
    ///
    /// Returns a storage key string.
    fn generate_key(&self, package_name: &str, package_version: &str, sha256: &str) -> String {
        format!(
            "workflows/{}/{}/{}.wasm",
            package_name, package_version, sha256
        )
    }

    /// Perform a health check on the storage backend
    ///
    /// # Returns
    ///
    /// Returns the health status of the storage backend.
    ///
    /// # Errors
    ///
    /// Returns an error if the health check fails.
    async fn health_check(&self) -> OrcherResult<HealthStatus>;

    /// Get the backend type
    ///
    /// # Returns
    ///
    /// Returns the type of storage backend (Filesystem or S3).
    fn backend_type(&self) -> StorageBackendType;
}

/// Storage backend type identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StorageBackendType {
    /// Filesystem storage backend
    Filesystem,
    /// S3 storage backend
    S3,
}

impl std::fmt::Display for StorageBackendType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageBackendType::Filesystem => write!(f, "filesystem"),
            StorageBackendType::S3 => write!(f, "s3"),
        }
    }
}

/// Health status for storage backend
#[derive(Debug, Clone)]
pub struct HealthStatus {
    /// Whether the backend is healthy
    pub healthy: bool,

    /// Backend type
    pub backend_type: StorageBackendType,

    /// Response time in milliseconds
    pub response_time_ms: u64,

    /// Available storage space in bytes (if available)
    pub available_space_bytes: Option<u64>,

    /// Human-readable status message
    pub message: String,

    /// Additional backend-specific information
    pub details: std::collections::HashMap<String, String>,
}

impl HealthStatus {
    /// Create a healthy status
    pub fn healthy(backend_type: StorageBackendType, response_time_ms: u64) -> Self {
        Self {
            healthy: true,
            backend_type,
            response_time_ms,
            available_space_bytes: None,
            message: format!("{} storage is healthy", backend_type),
            details: std::collections::HashMap::new(),
        }
    }

    /// Create an unhealthy status
    pub fn unhealthy(
        backend_type: StorageBackendType,
        response_time_ms: u64,
        message: String,
    ) -> Self {
        Self {
            healthy: false,
            backend_type,
            response_time_ms,
            available_space_bytes: None,
            message,
            details: std::collections::HashMap::new(),
        }
    }

    /// Add a detail field
    pub fn with_detail(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.details.insert(key.into(), value.into());
        self
    }

    /// Set available space
    pub fn with_available_space(mut self, bytes: u64) -> Self {
        self.available_space_bytes = Some(bytes);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backend_type_display() {
        assert_eq!(StorageBackendType::Filesystem.to_string(), "filesystem");
        assert_eq!(StorageBackendType::S3.to_string(), "s3");
    }

    #[test]
    fn test_health_status_healthy() {
        let status = HealthStatus::healthy(StorageBackendType::Filesystem, 100);
        assert!(status.healthy);
        assert_eq!(status.backend_type, StorageBackendType::Filesystem);
        assert_eq!(status.response_time_ms, 100);
    }

    #[test]
    fn test_health_status_unhealthy() {
        let status =
            HealthStatus::unhealthy(StorageBackendType::S3, 500, "Connection failed".to_string());
        assert!(!status.healthy);
        assert_eq!(status.backend_type, StorageBackendType::S3);
        assert_eq!(status.response_time_ms, 500);
        assert_eq!(status.message, "Connection failed");
    }

    #[test]
    fn test_health_status_with_details() {
        let status = HealthStatus::healthy(StorageBackendType::Filesystem, 100)
            .with_detail("path", "/var/orcher/workflows")
            .with_available_space(1024 * 1024 * 1024);

        assert_eq!(
            status.details.get("path"),
            Some(&"/var/orcher/workflows".to_string())
        );
        assert_eq!(status.available_space_bytes, Some(1024 * 1024 * 1024));
    }
}
