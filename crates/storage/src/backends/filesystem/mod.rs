//! Filesystem storage backend for WASM binaries
//!
//! This module provides a filesystem-based storage backend for WASM workflow binaries.
//! It implements the `WasmStorage` trait and provides full support for storing,
//! retrieving, listing, and managing WASM binaries on the local filesystem.
//!
//! ## Features
//!
//! - **Local Storage**: Store WASM binaries on the local filesystem
//! - **Organized Structure**: Hierarchical directory structure for workflows
//! - **Metadata Support**: JSON metadata files alongside WASM binaries
//! - **Checksum Verification**: SHA256 checksums for data integrity
//! - **Optional Compression**: gzip compression to reduce storage space
//! - **Health Checks**: Verify storage availability and capacity
//!
//! ## Directory Structure
//!
//! ```text
//! {storage_path}/
//! └── workflows/
//!     └── {package_name}/
//!         └── {package_version}/
//!             ├── {sha256}.wasm
//!             └── {sha256}.wasm.meta
//! ```
//!
//! ## Usage
//!
//! ```rust,no_run
//! use orcher_storage::backends::filesystem::{FilesystemStorage, FilesystemConfig};
//! use orcher_storage::backends::wasm::{WasmStorage, StorageMetadata};
//!
//! # async fn example() -> orcher_common::OrcherResult<()> {
//! // Create filesystem storage
//! let config = FilesystemConfig::new(".orcher/workflows".to_string());
//! let storage = FilesystemStorage::new(config).await?;
//!
//! // Store a WASM binary
//! let metadata = StorageMetadata::new(
//!     "my-workflow".to_string(),
//!     "1.0.0".to_string(),
//!     "abc123...".to_string(),
//!     1024,
//! );
//!
//! let key = storage.generate_key("my-workflow", "1.0.0", "abc123");
//! let wasm_bytes = vec![0x00, 0x61, 0x73, 0x6d]; // WASM magic number
//! let stored = storage.store(&key, &wasm_bytes, metadata).await?;
//!
//! // Retrieve the WASM binary
//! let binary = storage.retrieve(&key).await?;
//!
//! // List all workflows
//! let workflows = storage.list(None).await?;
//!
//! // Health check
//! let health = storage.health_check().await?;
//! # Ok(())
//! # }
//! ```

pub mod config;
pub mod health;
pub mod operations;

use async_trait::async_trait;
use orcher_common::OrcherResult;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

pub use config::FilesystemConfig;

use crate::backends::wasm::{
    HealthStatus, StorageBackendType, StorageMetadata, StoredWasm, WasmBinary, WasmInfo,
    WasmStorage,
};

/// Filesystem storage backend
///
/// This structure implements WASM binary storage on the local filesystem.
/// It provides thread-safe access to the storage through interior mutability.
///
/// # Thread Safety
///
/// The storage backend uses an `Arc<RwLock<>>` for the base path to allow
/// safe concurrent access across multiple threads.
#[derive(Debug, Clone)]
pub struct FilesystemStorage {
    /// Base path for storage (wrapped for thread safety)
    base_path: Arc<RwLock<PathBuf>>,

    /// Storage configuration
    config: FilesystemConfig,
}

impl FilesystemStorage {
    /// Create a new filesystem storage backend
    ///
    /// This function creates a new filesystem storage backend with the given configuration.
    /// It validates the configuration and ensures the storage directory is accessible.
    ///
    /// # Arguments
    ///
    /// * `config` - Filesystem storage configuration
    ///
    /// # Returns
    ///
    /// Returns a new `FilesystemStorage` instance.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Configuration validation fails
    /// - Storage directory cannot be accessed or created
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use orcher_storage::backends::filesystem::{FilesystemStorage, FilesystemConfig};
    ///
    /// # async fn example() -> orcher_common::OrcherResult<()> {
    /// let config = FilesystemConfig::new("/var/orcher/workflows".to_string());
    /// let storage = FilesystemStorage::new(config).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn new(config: FilesystemConfig) -> OrcherResult<Self> {
        // Validate configuration
        config.validate().map_err(|e| {
            orcher_common::error::OrcherError::invalid_input_field(&e, "FilesystemConfig")
        })?;

        // Expand path (handle ~ and environment variables)
        let base_path = config.storage_path_buf();

        debug!("Initializing filesystem storage at: {:?}", base_path);

        // Create directory if needed
        if config.create_dirs && !base_path.exists() {
            info!("Creating storage directory: {:?}", base_path);
            tokio::fs::create_dir_all(&base_path).await.map_err(|e| {
                let suggestion = if e.kind() == std::io::ErrorKind::PermissionDenied {
                    format!(
                        "\n\nPermission denied when creating directory: {:?}\n\
                        \nSuggestions:\n\
                        1. Use a local directory in .env:\n\
                           ORCHER_STORAGE_PATH=./.orcher/workflows\n\
                        2. Or create the directory with proper permissions:\n\
                           sudo mkdir -p {:?}\n\
                           sudo chown -R $USER:$USER {:?}\n\
                        3. Or use a directory in your home folder:\n\
                           ORCHER_STORAGE_PATH=~/.orcher/workflows\n",
                        base_path, base_path, base_path
                    )
                } else {
                    format!(
                        "\nFailed to create directory: {:?}\nError: {}",
                        base_path, e
                    )
                };

                orcher_common::error::OrcherError::Storage {
                    operation: "create_directory".to_string(),
                    message: suggestion,
                }
            })?;

            // Set directory permissions on Unix
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let perms = std::fs::Permissions::from_mode(config.dir_permissions);
                tokio::fs::set_permissions(&base_path, perms)
                    .await
                    .map_err(|e| orcher_common::error::OrcherError::Storage {
                        operation: "set_permissions".to_string(),
                        message: format!("Failed to set directory permissions: {}", e),
                    })?;
            }
        }

        // Verify directory exists and is accessible
        if !base_path.exists() {
            return Err(orcher_common::error::OrcherError::Storage {
                operation: "validate_directory".to_string(),
                message: format!(
                    "Storage directory does not exist and create_dirs is disabled: {:?}",
                    base_path
                ),
            });
        }

        if !base_path.is_dir() {
            return Err(orcher_common::error::OrcherError::Storage {
                operation: "validate_directory".to_string(),
                message: format!("Storage path is not a directory: {:?}", base_path),
            });
        }

        info!(
            "Filesystem storage initialized successfully at {:?}",
            base_path
        );

        Ok(Self {
            base_path: Arc::new(RwLock::new(base_path)),
            config,
        })
    }

    /// Get the base path
    async fn get_base_path(&self) -> PathBuf {
        self.base_path.read().await.clone()
    }

    /// Get the configuration
    pub fn config(&self) -> &FilesystemConfig {
        &self.config
    }
}

#[async_trait]
impl WasmStorage for FilesystemStorage {
    async fn store(
        &self,
        key: &str,
        wasm_bytes: &[u8],
        metadata: StorageMetadata,
    ) -> OrcherResult<StoredWasm> {
        let base_path = self.get_base_path().await;
        operations::store(&base_path, key, wasm_bytes, metadata, &self.config).await
    }

    async fn retrieve(&self, key: &str) -> OrcherResult<WasmBinary> {
        let base_path = self.get_base_path().await;
        operations::retrieve(&base_path, key, &self.config).await
    }

    async fn exists(&self, key: &str) -> OrcherResult<bool> {
        let base_path = self.get_base_path().await;
        let file_path = base_path.join(key);
        Ok(file_path.exists())
    }

    async fn delete(&self, key: &str) -> OrcherResult<()> {
        let base_path = self.get_base_path().await;
        operations::delete(&base_path, key).await
    }

    async fn list(&self, prefix: Option<&str>) -> OrcherResult<Vec<WasmInfo>> {
        let base_path = self.get_base_path().await;
        operations::list(&base_path, prefix).await
    }

    async fn get_metadata(&self, key: &str) -> OrcherResult<StorageMetadata> {
        let base_path = self.get_base_path().await;
        operations::get_metadata(&base_path, key).await
    }

    async fn health_check(&self) -> OrcherResult<HealthStatus> {
        let base_path = self.get_base_path().await;
        health::check_health(&base_path, &self.config).await
    }

    fn backend_type(&self) -> StorageBackendType {
        StorageBackendType::Filesystem
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};
    use tempfile::TempDir;

    fn create_test_wasm() -> Vec<u8> {
        // WASM magic number + minimal valid content
        vec![0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00]
    }

    fn create_test_metadata(wasm_bytes: &[u8]) -> StorageMetadata {
        let mut hasher = Sha256::new();
        hasher.update(wasm_bytes);
        let sha256 = format!("{:x}", hasher.finalize());

        StorageMetadata::new(
            "test-workflow".to_string(),
            "1.0.0".to_string(),
            sha256,
            wasm_bytes.len() as u64,
        )
    }

    #[tokio::test]
    async fn test_new_storage() {
        let temp_dir = TempDir::new().unwrap();
        let config = FilesystemConfig::new(temp_dir.path().to_string_lossy().to_string());

        let storage = FilesystemStorage::new(config).await;
        assert!(storage.is_ok());

        let storage = storage.unwrap();
        assert_eq!(storage.backend_type(), StorageBackendType::Filesystem);
    }

    #[tokio::test]
    async fn test_new_storage_creates_directory() {
        let temp_dir = TempDir::new().unwrap();
        let storage_path = temp_dir.path().join("workflows");

        // Directory doesn't exist yet
        assert!(!storage_path.exists());

        let config = FilesystemConfig::new(storage_path.to_string_lossy().to_string());
        let storage = FilesystemStorage::new(config).await.unwrap();

        // Directory should now exist
        assert!(storage_path.exists());
        assert_eq!(storage.backend_type(), StorageBackendType::Filesystem);
    }

    #[tokio::test]
    async fn test_new_storage_without_create_dirs() {
        let temp_dir = TempDir::new().unwrap();
        let storage_path = temp_dir.path().join("nonexistent");

        let config = FilesystemConfig::new(storage_path.to_string_lossy().to_string())
            .with_create_dirs(false);

        let result = FilesystemStorage::new(config).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_store_and_retrieve() {
        let temp_dir = TempDir::new().unwrap();
        let config = FilesystemConfig::new(temp_dir.path().to_string_lossy().to_string());
        let storage = FilesystemStorage::new(config).await.unwrap();

        let wasm_bytes = create_test_wasm();
        let metadata = create_test_metadata(&wasm_bytes);
        let key = storage.generate_key("test-workflow", "1.0.0", &metadata.sha256);

        // Store
        let stored = storage.store(&key, &wasm_bytes, metadata).await.unwrap();
        assert_eq!(stored.key, key);
        assert_eq!(stored.size_bytes, wasm_bytes.len() as u64);
        assert_eq!(stored.backend, "filesystem");

        // Retrieve
        let binary = storage.retrieve(&key).await.unwrap();
        assert_eq!(binary.bytes, wasm_bytes);
        assert_eq!(binary.metadata.package_name, "test-workflow");
        assert!(binary.is_valid_wasm());
        assert!(binary.verify_checksum());
    }

    #[tokio::test]
    async fn test_exists() {
        let temp_dir = TempDir::new().unwrap();
        let config = FilesystemConfig::new(temp_dir.path().to_string_lossy().to_string());
        let storage = FilesystemStorage::new(config).await.unwrap();

        let wasm_bytes = create_test_wasm();
        let metadata = create_test_metadata(&wasm_bytes);
        let key = storage.generate_key("test-workflow", "1.0.0", &metadata.sha256);

        // Should not exist initially
        assert!(!storage.exists(&key).await.unwrap());

        // Store
        storage.store(&key, &wasm_bytes, metadata).await.unwrap();

        // Should exist now
        assert!(storage.exists(&key).await.unwrap());
    }

    #[tokio::test]
    async fn test_delete() {
        let temp_dir = TempDir::new().unwrap();
        let config = FilesystemConfig::new(temp_dir.path().to_string_lossy().to_string());
        let storage = FilesystemStorage::new(config).await.unwrap();

        let wasm_bytes = create_test_wasm();
        let metadata = create_test_metadata(&wasm_bytes);
        let key = storage.generate_key("test-workflow", "1.0.0", &metadata.sha256);

        // Store
        storage.store(&key, &wasm_bytes, metadata).await.unwrap();
        assert!(storage.exists(&key).await.unwrap());

        // Delete
        storage.delete(&key).await.unwrap();
        assert!(!storage.exists(&key).await.unwrap());

        // Verify retrieve fails
        let result = storage.retrieve(&key).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_list() {
        let temp_dir = TempDir::new().unwrap();
        let config = FilesystemConfig::new(temp_dir.path().to_string_lossy().to_string());
        let storage = FilesystemStorage::new(config).await.unwrap();

        // Store multiple workflows
        for i in 0..3 {
            let wasm_bytes = create_test_wasm();
            let mut metadata = create_test_metadata(&wasm_bytes);
            metadata.package_name = format!("test-workflow-{}", i);

            let key = storage.generate_key(&metadata.package_name, "1.0.0", &metadata.sha256);
            storage.store(&key, &wasm_bytes, metadata).await.unwrap();
        }

        // List all
        let all_workflows = storage.list(None).await.unwrap();
        assert_eq!(all_workflows.len(), 3);

        // List with prefix
        let filtered = storage
            .list(Some("workflows/test-workflow-0"))
            .await
            .unwrap();
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].package_name, "test-workflow-0");
    }

    #[tokio::test]
    async fn test_get_metadata() {
        let temp_dir = TempDir::new().unwrap();
        let config = FilesystemConfig::new(temp_dir.path().to_string_lossy().to_string());
        let storage = FilesystemStorage::new(config).await.unwrap();

        let wasm_bytes = create_test_wasm();
        let metadata = create_test_metadata(&wasm_bytes);
        let key = storage.generate_key("test-workflow", "1.0.0", &metadata.sha256);

        // Store
        storage
            .store(&key, &wasm_bytes, metadata.clone())
            .await
            .unwrap();

        // Get metadata
        let retrieved = storage.get_metadata(&key).await.unwrap();
        assert_eq!(retrieved.package_name, metadata.package_name);
        assert_eq!(retrieved.sha256, metadata.sha256);
        assert_eq!(retrieved.size_bytes, metadata.size_bytes);
    }

    #[tokio::test]
    async fn test_health_check() {
        let temp_dir = TempDir::new().unwrap();
        let config = FilesystemConfig::new(temp_dir.path().to_string_lossy().to_string());
        let storage = FilesystemStorage::new(config).await.unwrap();

        let health = storage.health_check().await.unwrap();
        assert!(health.healthy);
        assert_eq!(health.backend_type, StorageBackendType::Filesystem);
        assert!(health.details.contains_key("path"));
        assert!(health.details.contains_key("writable"));
        assert!(health.details.contains_key("readable"));
    }

    #[tokio::test]
    async fn test_compression() {
        let temp_dir = TempDir::new().unwrap();
        let config = FilesystemConfig::new(temp_dir.path().to_string_lossy().to_string())
            .with_compression(true);
        let storage = FilesystemStorage::new(config).await.unwrap();

        let wasm_bytes = create_test_wasm();
        let metadata = create_test_metadata(&wasm_bytes);
        let key = storage.generate_key("test-workflow", "1.0.0", &metadata.sha256);

        // Store with compression
        storage.store(&key, &wasm_bytes, metadata).await.unwrap();

        // Retrieve with decompression
        let binary = storage.retrieve(&key).await.unwrap();
        assert_eq!(binary.bytes, wasm_bytes);
        assert!(binary.verify_checksum());
    }

    #[tokio::test]
    async fn test_generate_key() {
        let temp_dir = TempDir::new().unwrap();
        let config = FilesystemConfig::new(temp_dir.path().to_string_lossy().to_string());
        let storage = FilesystemStorage::new(config).await.unwrap();

        let key = storage.generate_key("my-workflow", "1.0.0", "abc123");
        assert_eq!(key, "workflows/my-workflow/1.0.0/abc123.wasm");
    }

    #[tokio::test]
    async fn test_concurrent_access() {
        let temp_dir = TempDir::new().unwrap();
        let config = FilesystemConfig::new(temp_dir.path().to_string_lossy().to_string());
        let storage = Arc::new(FilesystemStorage::new(config).await.unwrap());

        let mut handles = vec![];

        // Spawn multiple tasks to store workflows concurrently
        for i in 0..10 {
            let storage_clone = Arc::clone(&storage);
            let handle = tokio::spawn(async move {
                let wasm_bytes = create_test_wasm();
                let mut metadata = create_test_metadata(&wasm_bytes);
                metadata.package_name = format!("workflow-{}", i);

                let key =
                    storage_clone.generate_key(&metadata.package_name, "1.0.0", &metadata.sha256);
                storage_clone.store(&key, &wasm_bytes, metadata).await
            });
            handles.push(handle);
        }

        // Wait for all tasks to complete
        for handle in handles {
            let result = handle.await.unwrap();
            assert!(result.is_ok());
        }

        // Verify all workflows were stored
        let all_workflows = storage.list(None).await.unwrap();
        assert_eq!(all_workflows.len(), 10);
    }
}
