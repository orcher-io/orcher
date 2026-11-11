//! Filesystem storage configuration
//!
//! This module provides configuration options for the filesystem storage backend.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Filesystem storage configuration
///
/// Configuration options for storing WASM binaries on the local filesystem.
///
/// # Examples
///
/// ```rust
/// use orcher_storage::backends::FilesystemConfig;
///
/// // Basic configuration
/// let config = FilesystemConfig::new(".orcher/workflows".to_string());
///
/// // With compression enabled
/// let config = FilesystemConfig::new(".orcher/workflows".to_string())
///     .with_compression(true);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilesystemConfig {
    /// Base storage path for WASM binaries
    ///
    /// This is the root directory where all workflow WASM binaries will be stored.
    /// The directory structure will be: `{storage_path}/workflows/{name}/{version}/{sha256}.wasm`
    pub storage_path: String,

    /// Automatically create directories if they don't exist
    ///
    /// If true, the storage backend will create the base directory and any
    /// subdirectories as needed. If false, directories must exist before use.
    #[serde(default = "default_create_dirs")]
    pub create_dirs: bool,

    /// Enable WASM binary compression
    ///
    /// If true, WASM binaries will be compressed using gzip before storage.
    /// This can significantly reduce storage space but adds compression overhead.
    #[serde(default)]
    pub enable_compression: bool,

    /// Maximum file size in bytes (optional)
    ///
    /// If set, attempts to store files larger than this size will be rejected.
    /// This can help prevent accidental storage of extremely large files.
    #[serde(default)]
    pub max_file_size_bytes: Option<u64>,

    /// File permissions mode (Unix only)
    ///
    /// The file permissions to set on stored files. Defaults to 0o644 (rw-r--r--).
    /// Only used on Unix-like systems.
    #[serde(default = "default_file_permissions")]
    pub file_permissions: u32,

    /// Directory permissions mode (Unix only)
    ///
    /// The directory permissions to set on created directories. Defaults to 0o755 (rwxr-xr-x).
    /// Only used on Unix-like systems.
    #[serde(default = "default_dir_permissions")]
    pub dir_permissions: u32,
}

/// Default value for create_dirs
fn default_create_dirs() -> bool {
    true
}

/// Default file permissions (0o644 - rw-r--r--)
fn default_file_permissions() -> u32 {
    0o644
}

/// Default directory permissions (0o755 - rwxr-xr-x)
fn default_dir_permissions() -> u32 {
    0o755
}

impl FilesystemConfig {
    /// Create a new filesystem configuration with the specified storage path
    ///
    /// # Arguments
    ///
    /// * `storage_path` - Base directory for storing WASM binaries
    ///
    /// # Examples
    ///
    /// ```rust
    /// use orcher_storage::backends::FilesystemConfig;
    ///
    /// let config = FilesystemConfig::new("/var/orcher/workflows".to_string());
    /// ```
    pub fn new(storage_path: String) -> Self {
        Self {
            storage_path,
            create_dirs: default_create_dirs(),
            enable_compression: false,
            max_file_size_bytes: None,
            file_permissions: default_file_permissions(),
            dir_permissions: default_dir_permissions(),
        }
    }

    /// Enable or disable automatic directory creation
    ///
    /// # Arguments
    ///
    /// * `create` - Whether to create directories automatically
    pub fn with_create_dirs(mut self, create: bool) -> Self {
        self.create_dirs = create;
        self
    }

    /// Enable or disable WASM binary compression
    ///
    /// # Arguments
    ///
    /// * `enable` - Whether to enable compression
    pub fn with_compression(mut self, enable: bool) -> Self {
        self.enable_compression = enable;
        self
    }

    /// Set maximum file size
    ///
    /// # Arguments
    ///
    /// * `max_bytes` - Maximum file size in bytes
    pub fn with_max_file_size(mut self, max_bytes: u64) -> Self {
        self.max_file_size_bytes = Some(max_bytes);
        self
    }

    /// Set file permissions (Unix only)
    ///
    /// # Arguments
    ///
    /// * `mode` - File permission mode (e.g., 0o644)
    pub fn with_file_permissions(mut self, mode: u32) -> Self {
        self.file_permissions = mode;
        self
    }

    /// Set directory permissions (Unix only)
    ///
    /// # Arguments
    ///
    /// * `mode` - Directory permission mode (e.g., 0o755)
    pub fn with_dir_permissions(mut self, mode: u32) -> Self {
        self.dir_permissions = mode;
        self
    }

    /// Get the storage path as a PathBuf
    ///
    /// Expands environment variables and tildes in the path.
    pub fn storage_path_buf(&self) -> PathBuf {
        expand_path(&self.storage_path)
    }

    /// Validate the configuration
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Storage path is empty
    /// - File permissions are invalid
    /// - Directory permissions are invalid
    pub fn validate(&self) -> Result<(), String> {
        if self.storage_path.is_empty() {
            return Err("Storage path cannot be empty".to_string());
        }

        // Validate permissions (should be octal values <= 0o777)
        if self.file_permissions > 0o777 {
            return Err(format!(
                "Invalid file permissions: 0o{:o} (must be <= 0o777)",
                self.file_permissions
            ));
        }

        if self.dir_permissions > 0o777 {
            return Err(format!(
                "Invalid directory permissions: 0o{:o} (must be <= 0o777)",
                self.dir_permissions
            ));
        }

        Ok(())
    }
}

impl Default for FilesystemConfig {
    /// Create default filesystem configuration
    ///
    /// Uses `.orcher/workflows` as the default storage path in the current directory.
    fn default() -> Self {
        Self::new(".orcher/workflows".to_string())
    }
}

/// Expand environment variables and tilde in path
///
/// This function expands:
/// - `~` to the user's home directory
/// - `$VAR` and `${VAR}` to environment variables
///
/// # Arguments
///
/// * `path` - Path string to expand
///
/// # Returns
///
/// Returns a PathBuf with expanded path
fn expand_path(path: &str) -> PathBuf {
    let path = if path.starts_with("~/") {
        if let Some(home) = dirs::home_dir() {
            path.replacen("~", &home.to_string_lossy(), 1)
        } else {
            path.to_string()
        }
    } else {
        path.to_string()
    };

    // Expand environment variables
    let path = shellexpand::env(&path)
        .unwrap_or(std::borrow::Cow::Borrowed(path.as_str()))
        .to_string();

    PathBuf::from(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let config = FilesystemConfig::new("/tmp/workflows".to_string());
        assert_eq!(config.storage_path, "/tmp/workflows");
        assert!(config.create_dirs);
        assert!(!config.enable_compression);
        assert_eq!(config.file_permissions, 0o644);
        assert_eq!(config.dir_permissions, 0o755);
    }

    #[test]
    fn test_default() {
        let config = FilesystemConfig::default();
        assert_eq!(config.storage_path, ".orcher/workflows");
        assert!(config.create_dirs);
    }

    #[test]
    fn test_builder_pattern() {
        let config = FilesystemConfig::new("/tmp/workflows".to_string())
            .with_compression(true)
            .with_create_dirs(false)
            .with_max_file_size(1024 * 1024 * 100) // 100MB
            .with_file_permissions(0o600)
            .with_dir_permissions(0o700);

        assert_eq!(config.storage_path, "/tmp/workflows");
        assert!(!config.create_dirs);
        assert!(config.enable_compression);
        assert_eq!(config.max_file_size_bytes, Some(1024 * 1024 * 100));
        assert_eq!(config.file_permissions, 0o600);
        assert_eq!(config.dir_permissions, 0o700);
    }

    #[test]
    fn test_validate_success() {
        let config = FilesystemConfig::new("/tmp/workflows".to_string());
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_empty_path() {
        let config = FilesystemConfig::new("".to_string());
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_invalid_file_permissions() {
        let config =
            FilesystemConfig::new("/tmp/workflows".to_string()).with_file_permissions(0o1000);
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_validate_invalid_dir_permissions() {
        let config =
            FilesystemConfig::new("/tmp/workflows".to_string()).with_dir_permissions(0o1000);
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_storage_path_buf() {
        let config = FilesystemConfig::new("/tmp/workflows".to_string());
        let path_buf = config.storage_path_buf();
        assert_eq!(path_buf, PathBuf::from("/tmp/workflows"));
    }

    #[test]
    fn test_expand_path_absolute() {
        let path = expand_path("/tmp/workflows");
        assert_eq!(path, PathBuf::from("/tmp/workflows"));
    }

    #[test]
    fn test_expand_path_relative() {
        let path = expand_path("./workflows");
        assert_eq!(path, PathBuf::from("./workflows"));
    }

    #[test]
    fn test_expand_path_tilde() {
        let path = expand_path("~/workflows");
        // Should expand to home directory
        if let Some(home) = dirs::home_dir() {
            assert_eq!(path, home.join("workflows"));
        }
    }

    #[test]
    fn test_expand_path_env_var() {
        std::env::set_var("TEST_ORCHER_PATH", "/test/path");
        let path = expand_path("$TEST_ORCHER_PATH/workflows");
        assert_eq!(path, PathBuf::from("/test/path/workflows"));
        std::env::remove_var("TEST_ORCHER_PATH");
    }

    #[test]
    fn test_serde_roundtrip() {
        let config = FilesystemConfig::new("/tmp/workflows".to_string())
            .with_compression(true)
            .with_max_file_size(1024 * 1024);

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: FilesystemConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(config.storage_path, deserialized.storage_path);
        assert_eq!(config.create_dirs, deserialized.create_dirs);
        assert_eq!(config.enable_compression, deserialized.enable_compression);
        assert_eq!(config.max_file_size_bytes, deserialized.max_file_size_bytes);
    }
}
