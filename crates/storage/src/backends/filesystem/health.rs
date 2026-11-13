//! Filesystem storage health check
//!
//! This module implements health check functionality for the filesystem storage backend.

use super::config::FilesystemConfig;
use crate::backends::wasm::{HealthStatus, StorageBackendType};
use orcher_common::OrcherResult;
use std::path::Path;
use std::time::Instant;
use tracing::{debug, warn};

/// Perform a health check on the filesystem storage
///
/// This function verifies that:
/// - The storage directory exists (or can be created)
/// - The storage directory is writable
/// - The storage directory is readable
/// - Available space information can be retrieved
///
/// # Arguments
///
/// * `base_path` - Base storage directory
/// * `config` - Filesystem configuration
///
/// # Returns
///
/// Returns a health status indicating whether the storage backend is healthy.
pub async fn check_health(
    base_path: &Path,
    config: &FilesystemConfig,
) -> OrcherResult<HealthStatus> {
    let start = Instant::now();

    debug!(
        "Performing health check on filesystem storage: {:?}",
        base_path
    );

    // Check if directory exists
    let exists = base_path.exists();

    if !exists && !config.create_dirs {
        let elapsed = start.elapsed().as_millis() as u64;
        return Ok(HealthStatus::unhealthy(
            StorageBackendType::Filesystem,
            elapsed,
            format!("Storage directory does not exist: {:?}", base_path),
        ));
    }

    // Create directory if needed
    if !exists && config.create_dirs {
        debug!("Creating storage directory: {:?}", base_path);
        if let Err(e) = std::fs::create_dir_all(base_path) {
            let elapsed = start.elapsed().as_millis() as u64;
            return Ok(HealthStatus::unhealthy(
                StorageBackendType::Filesystem,
                elapsed,
                format!("Failed to create storage directory: {}", e),
            ));
        }

        // Set directory permissions on Unix
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let perms = std::fs::Permissions::from_mode(config.dir_permissions);
            if let Err(e) = std::fs::set_permissions(base_path, perms) {
                warn!("Failed to set directory permissions: {}", e);
            }
        }
    }

    // Check if directory is actually a directory
    if !base_path.is_dir() {
        let elapsed = start.elapsed().as_millis() as u64;
        return Ok(HealthStatus::unhealthy(
            StorageBackendType::Filesystem,
            elapsed,
            format!("Path exists but is not a directory: {:?}", base_path),
        ));
    }

    // Test write access
    let test_file = base_path.join(".orcher_health_check");
    match std::fs::write(&test_file, b"health_check") {
        Ok(_) => {
            // Clean up test file
            let _ = std::fs::remove_file(&test_file);
        }
        Err(e) => {
            let elapsed = start.elapsed().as_millis() as u64;
            return Ok(HealthStatus::unhealthy(
                StorageBackendType::Filesystem,
                elapsed,
                format!("Storage directory is not writable: {}", e),
            ));
        }
    }

    // Test read access
    if let Err(e) = std::fs::read_dir(base_path) {
        let elapsed = start.elapsed().as_millis() as u64;
        return Ok(HealthStatus::unhealthy(
            StorageBackendType::Filesystem,
            elapsed,
            format!("Storage directory is not readable: {}", e),
        ));
    }

    // Get available space
    let available_space = get_available_space(base_path);

    // Check if we have enough space (warn if less than 100MB)
    if let Some(space) = available_space {
        let min_space = 100 * 1024 * 1024; // 100 MB
        if space < min_space {
            warn!(
                "Low storage space: {} bytes available (minimum recommended: {} bytes)",
                space, min_space
            );
        }
    }

    let elapsed = start.elapsed().as_millis() as u64;

    let mut status = HealthStatus::healthy(StorageBackendType::Filesystem, elapsed)
        .with_detail("path", base_path.to_string_lossy().to_string())
        .with_detail("writable", "true")
        .with_detail("readable", "true");

    if let Some(space) = available_space {
        status = status.with_available_space(space);
        status = status.with_detail("available_space_human", format_bytes(space));
    }

    debug!("Filesystem storage health check passed in {}ms", elapsed);

    Ok(status)
}

/// Get available space on the filesystem
///
/// This function attempts to get the available space on the filesystem
/// containing the given path. Returns None if the information cannot be retrieved.
///
/// # Arguments
///
/// * `path` - Path to check
///
/// # Returns
///
/// Returns the available space in bytes, or None if unavailable.
fn get_available_space(path: &Path) -> Option<u64> {
    #[cfg(unix)]
    {
        // Get filesystem stats using statvfs
        match nix::sys::statvfs::statvfs(path) {
            Ok(stats) => {
                let available = stats.blocks_available() * stats.block_size();
                Some(available)
            }
            Err(e) => {
                debug!("Failed to get filesystem stats: {}", e);
                None
            }
        }
    }

    #[cfg(windows)]
    {
        use std::ffi::OsStr;
        use std::os::windows::ffi::OsStrExt;
        use winapi::um::fileapi::GetDiskFreeSpaceExW;

        // Convert path to wide string
        let path_wide: Vec<u16> = OsStr::new(path.to_str().unwrap_or(""))
            .encode_wide()
            .chain(std::iter::once(0))
            .collect();

        let mut free_bytes: u64 = 0;
        let mut total_bytes: u64 = 0;
        let mut total_free_bytes: u64 = 0;

        unsafe {
            if GetDiskFreeSpaceExW(
                path_wide.as_ptr(),
                &mut free_bytes as *mut u64 as *mut _,
                &mut total_bytes as *mut u64 as *mut _,
                &mut total_free_bytes as *mut u64 as *mut _,
            ) != 0
            {
                Some(free_bytes)
            } else {
                debug!("Failed to get disk free space");
                None
            }
        }
    }

    #[cfg(not(any(unix, windows)))]
    {
        // On other platforms, we can't determine available space
        None
    }
}

/// Format bytes into human-readable format
fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    const TB: u64 = GB * 1024;

    if bytes >= TB {
        format!("{:.2} TB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} bytes", bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_health_check_success() {
        let temp_dir = TempDir::new().unwrap();
        let config = FilesystemConfig::new(temp_dir.path().to_string_lossy().to_string());

        let status = check_health(temp_dir.path(), &config).await.unwrap();

        assert!(status.healthy);
        assert_eq!(status.backend_type, StorageBackendType::Filesystem);
        assert!(status.details.contains_key("path"));
        assert!(status.details.contains_key("writable"));
        assert!(status.details.contains_key("readable"));
    }

    #[tokio::test]
    async fn test_health_check_nonexistent_dir_with_create() {
        let temp_dir = TempDir::new().unwrap();
        let test_path = temp_dir.path().join("nonexistent");
        let config =
            FilesystemConfig::new(test_path.to_string_lossy().to_string()).with_create_dirs(true);

        let status = check_health(&test_path, &config).await.unwrap();

        assert!(status.healthy);
        assert!(test_path.exists());
    }

    #[tokio::test]
    async fn test_health_check_nonexistent_dir_without_create() {
        let temp_dir = TempDir::new().unwrap();
        let test_path = temp_dir.path().join("nonexistent");
        let config =
            FilesystemConfig::new(test_path.to_string_lossy().to_string()).with_create_dirs(false);

        let status = check_health(&test_path, &config).await.unwrap();

        assert!(!status.healthy);
        assert!(status.message.contains("does not exist"));
    }

    #[tokio::test]
    async fn test_health_check_file_not_directory() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_file");
        std::fs::write(&file_path, b"test").unwrap();

        let config = FilesystemConfig::new(file_path.to_string_lossy().to_string());

        let status = check_health(&file_path, &config).await.unwrap();

        assert!(!status.healthy);
        assert!(status.message.contains("not a directory"));
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(512), "512 bytes");
        assert_eq!(format_bytes(1024), "1.00 KB");
        assert_eq!(format_bytes(1536), "1.50 KB");
        assert_eq!(format_bytes(1048576), "1.00 MB");
        assert_eq!(format_bytes(1073741824), "1.00 GB");
        assert_eq!(format_bytes(1099511627776), "1.00 TB");
    }

    #[test]
    fn test_get_available_space() {
        let temp_dir = TempDir::new().unwrap();
        let space = get_available_space(temp_dir.path());

        // Should return Some value on most systems
        // We can't assert the exact value, but it should be greater than 0
        if let Some(bytes) = space {
            assert!(bytes > 0);
        }
    }
}
