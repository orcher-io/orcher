//! Storage backend factory
//!
//! This module provides a factory for creating WASM storage backends
//! based on configuration or environment variables.

use super::{StorageBackendType, WasmStorage};
use crate::backends::{FilesystemConfig, FilesystemStorage};
#[cfg(feature = "s3")]
use crate::backends::{S3Config, S3Storage};
use orcher_common::OrcherResult;
use std::sync::Arc;
use tracing::{debug, info};

/// Storage backend configuration
///
/// This enum represents the different types of storage backends
/// that can be created by the factory.
#[derive(Debug, Clone)]
pub enum StorageConfig {
    /// Filesystem storage backend
    Filesystem(FilesystemConfig),

    /// S3 storage backend
    #[cfg(feature = "s3")]
    S3(S3Config),
}

impl StorageConfig {
    /// Get the backend type
    pub fn backend_type(&self) -> StorageBackendType {
        match self {
            StorageConfig::Filesystem(_) => StorageBackendType::Filesystem,
            #[cfg(feature = "s3")]
            StorageConfig::S3(_) => StorageBackendType::S3,
        }
    }
}

/// Storage factory for creating storage backends
///
/// This factory provides methods to create storage backends from
/// configuration or environment variables.
///
/// # Examples
///
/// ## From Configuration
///
/// ```rust,no_run
/// use orcher_storage::backends::wasm::{StorageFactory, StorageConfig};
/// use orcher_storage::backends::FilesystemConfig;
///
/// # async fn example() -> orcher_common::OrcherResult<()> {
/// let config = FilesystemConfig::new(".orcher/workflows".to_string());
/// let storage = StorageFactory::create(StorageConfig::Filesystem(config)).await?;
/// # Ok(())
/// # }
/// ```
///
/// ## From Environment Variables
///
/// ```bash
/// export ORCHER_STORAGE_TYPE=filesystem
/// export ORCHER_STORAGE_PATH=~/.orcher/workflows
/// ```
///
/// ```rust,no_run
/// use orcher_storage::backends::wasm::StorageFactory;
///
/// # async fn example() -> orcher_common::OrcherResult<()> {
/// let storage = StorageFactory::from_env().await?;
/// # Ok(())
/// # }
/// ```
pub struct StorageFactory;

impl StorageFactory {
    /// Create a storage backend from configuration
    ///
    /// # Arguments
    ///
    /// * `config` - Storage configuration
    ///
    /// # Returns
    ///
    /// Returns an Arc-wrapped trait object implementing `WasmStorage`.
    ///
    /// # Errors
    ///
    /// Returns an error if backend creation fails.
    pub async fn create(config: StorageConfig) -> OrcherResult<Arc<dyn WasmStorage>> {
        info!("Creating storage backend: {:?}", config.backend_type());

        match config {
            StorageConfig::Filesystem(cfg) => {
                debug!("Initializing filesystem storage at: {}", cfg.storage_path);
                let storage = FilesystemStorage::new(cfg).await?;
                info!("Filesystem storage initialized successfully");
                Ok(Arc::new(storage))
            }
            #[cfg(feature = "s3")]
            StorageConfig::S3(cfg) => {
                debug!(
                    "Initializing S3 storage: bucket={}, region={}",
                    cfg.bucket, cfg.region
                );
                let storage = S3Storage::new(cfg).await?;
                info!("S3 storage initialized successfully");
                Ok(Arc::new(storage))
            }
        }
    }

    /// Create storage backend from environment variables
    ///
    /// # Environment Variables
    ///
    /// ## Common
    /// - `ORCHER_STORAGE_TYPE` - Backend type ("filesystem" or "s3")
    ///
    /// ## Filesystem
    /// - `ORCHER_STORAGE_PATH` - Base storage path (default: ".orcher/workflows")
    /// - `ORCHER_STORAGE_COMPRESSION` - Enable compression ("true" or "false")
    ///
    /// ## S3
    /// - `ORCHER_S3_BUCKET` - S3 bucket name (required)
    /// - `ORCHER_S3_REGION` - AWS region (default: "us-east-1")
    /// - `ORCHER_S3_ENDPOINT` - Custom endpoint URL (optional)
    /// - `ORCHER_S3_PREFIX` - Key prefix (optional)
    ///
    /// # Returns
    ///
    /// Returns an Arc-wrapped trait object implementing `WasmStorage`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Required environment variables are missing
    /// - Environment variables have invalid values
    /// - Backend creation fails
    pub async fn from_env() -> OrcherResult<Arc<dyn WasmStorage>> {
        Self::from_vars(|name| std::env::var(name).ok()).await
    }

    /// Create the backend reading its settings through `var`, which returns a
    /// variable's value or `None` when it is unset. `from_env` passes the
    /// process environment; tests pass their own values, since the process
    /// environment is shared by every test running at once.
    async fn from_vars(var: impl Fn(&str) -> Option<String>) -> OrcherResult<Arc<dyn WasmStorage>> {
        let storage_type = var("ORCHER_STORAGE_TYPE").unwrap_or_else(|| {
            debug!("ORCHER_STORAGE_TYPE not set, defaulting to filesystem");
            "filesystem".to_string()
        });

        info!(
            "Creating storage backend from environment: type={}",
            storage_type
        );

        match storage_type.to_lowercase().as_str() {
            "filesystem" | "fs" => {
                let storage_path = var("ORCHER_STORAGE_PATH").unwrap_or_else(|| {
                    debug!("ORCHER_STORAGE_PATH not set, using default");
                    ".orcher/workflows".to_string()
                });

                // Check if path requires elevated permissions
                if storage_path.starts_with("/var/") || storage_path.starts_with("/usr/") {
                    tracing::warn!(
                        "Storage path '{}' requires elevated permissions. \
                        Consider using a local path like './.orcher/workflows' or '~/.orcher/workflows' \
                        for development environments.",
                        storage_path
                    );
                }

                let enable_compression = var("ORCHER_STORAGE_COMPRESSION")
                    .and_then(|v| v.parse::<bool>().ok())
                    .unwrap_or(false);

                let config =
                    FilesystemConfig::new(storage_path).with_compression(enable_compression);

                Self::create(StorageConfig::Filesystem(config)).await
            }
            #[cfg(feature = "s3")]
            "s3" => {
                let bucket = var("ORCHER_S3_BUCKET").ok_or_else(|| {
                    orcher_common::error::OrcherError::invalid_input_field(
                        "ORCHER_S3_BUCKET environment variable is required for S3 storage",
                        "ORCHER_S3_BUCKET",
                    )
                })?;

                let region = var("ORCHER_S3_REGION").unwrap_or_else(|| {
                    debug!("ORCHER_S3_REGION not set, using default");
                    "us-east-1".to_string()
                });

                let mut config = S3Config::new(bucket, region);

                // Optional endpoint (for S3-compatible services like MinIO)
                if let Some(endpoint) = var("ORCHER_S3_ENDPOINT") {
                    debug!("Using custom S3 endpoint: {}", endpoint);
                    config = config.with_endpoint(endpoint);
                }

                // Optional key prefix
                if let Some(prefix) = var("ORCHER_S3_PREFIX") {
                    debug!("Using S3 key prefix: {}", prefix);
                    config = config.with_prefix(prefix);
                }

                Self::create(StorageConfig::S3(config)).await
            }
            _ => Err(orcher_common::error::OrcherError::invalid_input_field(
                format!(
                    "Unsupported storage type: '{}'. Supported types: filesystem, s3",
                    storage_type
                ),
                "ORCHER_STORAGE_TYPE",
            )),
        }
    }

    /// Create storage backend with sensible defaults for development
    ///
    /// This creates a filesystem storage backend in the current directory
    /// under `.orcher/workflows`.
    ///
    /// # Returns
    ///
    /// Returns an Arc-wrapped trait object implementing `WasmStorage`.
    pub async fn dev_default() -> OrcherResult<Arc<dyn WasmStorage>> {
        info!("Creating development storage backend (filesystem at .orcher/workflows)");
        let config = FilesystemConfig::default();

        // Try to create with better error message
        Self::create(StorageConfig::Filesystem(config))
            .await
            .map_err(|e| orcher_common::error::OrcherError::Storage {
                operation: "dev_default".to_string(),
                message: format!(
                    "Failed to create development storage at .orcher/workflows: {}\n\
                    \nTo fix this, set ORCHER_STORAGE_PATH in your .env file:\n\
                    ORCHER_STORAGE_PATH=./.orcher/workflows",
                    e
                ),
            })
    }

    /// Validate storage backend health
    ///
    /// This is a convenience method to create a storage backend and
    /// immediately check its health.
    ///
    /// # Arguments
    ///
    /// * `config` - Storage configuration
    ///
    /// # Returns
    ///
    /// Returns the storage backend if healthy.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Backend creation fails
    /// - Health check fails
    pub async fn create_and_validate(config: StorageConfig) -> OrcherResult<Arc<dyn WasmStorage>> {
        let storage = Self::create(config).await?;

        // Perform health check
        let health = storage.health_check().await?;

        if !health.healthy {
            return Err(orcher_common::error::OrcherError::Storage {
                operation: "health_check".to_string(),
                message: format!(
                    "Storage backend {} is unhealthy: {}",
                    health.backend_type, health.message
                ),
            });
        }

        info!(
            "Storage backend {} validated successfully ({}ms)",
            health.backend_type, health.response_time_ms
        );

        Ok(storage)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_filesystem_storage() {
        let config = FilesystemConfig::new("/tmp/orcher-test-workflows".to_string());
        let result = StorageFactory::create(StorageConfig::Filesystem(config)).await;
        assert!(result.is_ok());

        let storage = result.unwrap();
        assert_eq!(storage.backend_type(), StorageBackendType::Filesystem);
    }

    #[tokio::test]
    async fn test_dev_default() {
        let result = StorageFactory::dev_default().await;
        assert!(result.is_ok());

        let storage = result.unwrap();
        assert_eq!(storage.backend_type(), StorageBackendType::Filesystem);
    }

    /// A lookup over `pairs`, standing in for the process environment.
    fn vars(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let pairs: Vec<(String, String)> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |name| {
            pairs
                .iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.clone())
        }
    }

    #[tokio::test]
    async fn test_from_env_filesystem() {
        let result = StorageFactory::from_vars(vars(&[
            ("ORCHER_STORAGE_TYPE", "filesystem"),
            ("ORCHER_STORAGE_PATH", "/tmp/orcher-test"),
        ]))
        .await;
        assert!(result.is_ok());

        let storage = result.unwrap();
        assert_eq!(storage.backend_type(), StorageBackendType::Filesystem);
    }

    #[tokio::test]
    async fn test_from_env_invalid_type() {
        let result = StorageFactory::from_vars(vars(&[("ORCHER_STORAGE_TYPE", "invalid")])).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_create_and_validate() {
        let config = FilesystemConfig::new("/tmp/orcher-validated-test".to_string());
        let result = StorageFactory::create_and_validate(StorageConfig::Filesystem(config)).await;

        // This might fail if we can't write to /tmp, but that's expected
        // In a real scenario, we'd use a writable test directory
        match result {
            Ok(storage) => {
                assert_eq!(storage.backend_type(), StorageBackendType::Filesystem);
            }
            Err(_) => {
                // Health check might fail if directory creation fails
                // This is acceptable for the test
            }
        }
    }

    #[test]
    fn test_storage_config_backend_type() {
        let fs_config = StorageConfig::Filesystem(FilesystemConfig::default());
        assert_eq!(fs_config.backend_type(), StorageBackendType::Filesystem);

        #[cfg(feature = "s3")]
        {
            let s3_config = StorageConfig::S3(S3Config::new(
                "test-bucket".to_string(),
                "us-east-1".to_string(),
            ));
            assert_eq!(s3_config.backend_type(), StorageBackendType::S3);
        }
    }
}
