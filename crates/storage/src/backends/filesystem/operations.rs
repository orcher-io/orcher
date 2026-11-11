//! Filesystem storage operations
//!
//! This module implements the core storage operations for the filesystem backend.

use super::config::FilesystemConfig;
use crate::backends::wasm::{StorageError, StorageMetadata, StoredWasm, WasmBinary, WasmInfo};
use orcher_common::types::time::Timestamptz;
use orcher_common::OrcherResult;
use sha2::{Digest, Sha256};
use std::fs;
use std::io::Write;
use std::path::Path;
use tracing::{debug, info, warn};

/// Store a WASM binary to the filesystem
///
/// # Arguments
///
/// * `base_path` - Base storage directory
/// * `key` - Storage key (e.g., "workflows/my-workflow/1.0.0/abc123.wasm")
/// * `wasm_bytes` - WASM binary data
/// * `metadata` - Storage metadata
/// * `config` - Filesystem configuration
///
/// # Returns
///
/// Returns information about the stored WASM binary.
///
/// # Errors
///
/// Returns an error if:
/// - The WASM binary is empty or invalid
/// - The file exceeds maximum size limit
/// - Directory creation fails
/// - File write fails
pub async fn store(
    base_path: &Path,
    key: &str,
    wasm_bytes: &[u8],
    metadata: StorageMetadata,
    config: &FilesystemConfig,
) -> OrcherResult<StoredWasm> {
    // Validate WASM binary
    if wasm_bytes.is_empty() {
        return Err(StorageError::invalid_wasm("WASM binary is empty").into());
    }

    // Check WASM magic number
    if wasm_bytes.len() < 4 || &wasm_bytes[0..4] != b"\0asm" {
        return Err(StorageError::invalid_wasm("Invalid WASM magic number").into());
    }

    // Check max file size
    if let Some(max_size) = config.max_file_size_bytes {
        if wasm_bytes.len() as u64 > max_size {
            return Err(StorageError::operation_failed(format!(
                "WASM binary size ({} bytes) exceeds maximum allowed size ({} bytes)",
                wasm_bytes.len(),
                max_size
            ))
            .into());
        }
    }

    // Verify checksum
    let mut hasher = Sha256::new();
    hasher.update(wasm_bytes);
    let computed_sha256 = format!("{:x}", hasher.finalize());

    if computed_sha256 != metadata.sha256 {
        warn!(
            "SHA256 mismatch: expected={}, computed={}",
            metadata.sha256, computed_sha256
        );
        return Err(StorageError::checksum_mismatch(&metadata.sha256, &computed_sha256).into());
    }

    // Construct full file path
    let file_path = base_path.join(key);
    let dir_path = file_path
        .parent()
        .ok_or_else(|| StorageError::operation_failed("Invalid file path: no parent directory"))?;

    // Create directories if needed
    if config.create_dirs && !dir_path.exists() {
        debug!("Creating directory: {:?}", dir_path);
        fs::create_dir_all(dir_path).map_err(|e| {
            StorageError::operation_failed_with_source(
                format!("Failed to create directory {:?}", dir_path),
                e,
            )
        })?;

        // Set directory permissions on Unix
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let perms = fs::Permissions::from_mode(config.dir_permissions);
            fs::set_permissions(dir_path, perms).map_err(|e| {
                StorageError::operation_failed_with_source("Failed to set directory permissions", e)
            })?;
        }
    }

    // Prepare data to write (with optional compression)
    let bytes_to_write = if config.enable_compression {
        debug!("Compressing WASM binary");
        compress_data(wasm_bytes)?
    } else {
        wasm_bytes.to_vec()
    };

    // Write WASM binary
    debug!("Writing WASM binary to: {:?}", file_path);
    let mut file = fs::File::create(&file_path).map_err(|e| {
        StorageError::operation_failed_with_source(
            format!("Failed to create file {:?}", file_path),
            e,
        )
    })?;

    file.write_all(&bytes_to_write).map_err(|e| {
        StorageError::operation_failed_with_source("Failed to write WASM binary", e)
    })?;

    file.sync_all()
        .map_err(|e| StorageError::operation_failed_with_source("Failed to sync file", e))?;

    // Set file permissions on Unix
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let perms = fs::Permissions::from_mode(config.file_permissions);
        fs::set_permissions(&file_path, perms).map_err(|e| {
            StorageError::operation_failed_with_source("Failed to set file permissions", e)
        })?;
    }

    // Write metadata file
    let metadata_path = file_path.with_extension("wasm.meta");
    write_metadata(&metadata_path, &metadata)?;

    info!(
        "Stored WASM binary: {} ({} bytes) at {:?}",
        key,
        wasm_bytes.len(),
        file_path
    );

    Ok(StoredWasm::new(
        key.to_string(),
        file_path.to_string_lossy().to_string(),
        wasm_bytes.len() as u64,
        metadata.sha256,
        "filesystem".to_string(),
    ))
}

/// Retrieve a WASM binary from the filesystem
///
/// # Arguments
///
/// * `base_path` - Base storage directory
/// * `key` - Storage key
/// * `config` - Filesystem configuration
///
/// # Returns
///
/// Returns the WASM binary with metadata.
///
/// # Errors
///
/// Returns an error if:
/// - The file does not exist
/// - File read fails
/// - Metadata read fails
/// - Decompression fails (if compressed)
pub async fn retrieve(
    base_path: &Path,
    key: &str,
    config: &FilesystemConfig,
) -> OrcherResult<WasmBinary> {
    let file_path = base_path.join(key);

    // Check if file exists
    if !file_path.exists() {
        return Err(StorageError::not_found(key).into());
    }

    debug!("Reading WASM binary from: {:?}", file_path);

    // Read file
    let bytes = fs::read(&file_path).map_err(|e| {
        StorageError::operation_failed_with_source(
            format!("Failed to read file {:?}", file_path),
            e,
        )
    })?;

    // Decompress if needed
    let wasm_bytes = if config.enable_compression {
        debug!("Decompressing WASM binary");
        decompress_data(&bytes)?
    } else {
        bytes
    };

    // Read metadata
    let metadata_path = file_path.with_extension("wasm.meta");
    let metadata = read_metadata(&metadata_path)?;

    // Verify checksum
    let mut hasher = Sha256::new();
    hasher.update(&wasm_bytes);
    let computed_sha256 = format!("{:x}", hasher.finalize());

    if computed_sha256 != metadata.sha256 {
        warn!(
            "SHA256 mismatch on retrieve: expected={}, computed={}",
            metadata.sha256, computed_sha256
        );
        return Err(StorageError::checksum_mismatch(&metadata.sha256, &computed_sha256).into());
    }

    info!(
        "Retrieved WASM binary: {} ({} bytes)",
        key,
        wasm_bytes.len()
    );

    Ok(WasmBinary::new(wasm_bytes, metadata))
}

/// Delete a WASM binary from the filesystem
///
/// # Arguments
///
/// * `base_path` - Base storage directory
/// * `key` - Storage key
///
/// # Returns
///
/// Returns Ok(()) on success.
///
/// # Errors
///
/// Returns an error if:
/// - The file does not exist
/// - File deletion fails
pub async fn delete(base_path: &Path, key: &str) -> OrcherResult<()> {
    let file_path = base_path.join(key);

    if !file_path.exists() {
        return Err(StorageError::not_found(key).into());
    }

    debug!("Deleting WASM binary: {:?}", file_path);

    // Delete WASM file
    fs::remove_file(&file_path).map_err(|e| {
        StorageError::operation_failed_with_source(
            format!("Failed to delete file {:?}", file_path),
            e,
        )
    })?;

    // Delete metadata file if it exists
    let metadata_path = file_path.with_extension("wasm.meta");
    if metadata_path.exists() {
        fs::remove_file(&metadata_path).map_err(|e| {
            StorageError::operation_failed_with_source(
                format!("Failed to delete metadata file {:?}", metadata_path),
                e,
            )
        })?;
    }

    info!("Deleted WASM binary: {}", key);

    Ok(())
}

/// List WASM binaries in the filesystem
///
/// # Arguments
///
/// * `base_path` - Base storage directory
/// * `prefix` - Optional prefix filter
///
/// # Returns
///
/// Returns a list of WASM binary information.
///
/// # Errors
///
/// Returns an error if directory traversal fails.
pub async fn list(base_path: &Path, prefix: Option<&str>) -> OrcherResult<Vec<WasmInfo>> {
    let search_path = if let Some(prefix) = prefix {
        base_path.join(prefix)
    } else {
        base_path.to_path_buf()
    };

    if !search_path.exists() {
        debug!("Search path does not exist: {:?}", search_path);
        return Ok(Vec::new());
    }

    debug!("Listing WASM binaries in: {:?}", search_path);

    let mut wasm_files = Vec::new();

    // Walk directory tree
    for entry in walkdir::WalkDir::new(&search_path)
        .follow_links(false)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        let path = entry.path();

        // Only process .wasm files
        if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("wasm") {
            // Get relative key
            let key = path
                .strip_prefix(base_path)
                .ok()
                .and_then(|p| p.to_str())
                .unwrap_or("")
                .to_string();

            // Read metadata
            let metadata_path = path.with_extension("wasm.meta");
            if let Ok(metadata) = read_metadata(&metadata_path) {
                let file_metadata = fs::metadata(path).ok();
                let stored_at = file_metadata
                    .and_then(|m| m.modified().ok())
                    .map(Timestamptz::from)
                    .unwrap_or_else(Timestamptz::now);

                wasm_files.push(WasmInfo::new(
                    key,
                    metadata.package_name,
                    metadata.package_version,
                    metadata.size_bytes,
                    metadata.sha256,
                    stored_at,
                    "filesystem".to_string(),
                ));
            }
        }
    }

    info!("Found {} WASM binaries", wasm_files.len());

    Ok(wasm_files)
}

/// Get metadata for a WASM binary
///
/// # Arguments
///
/// * `base_path` - Base storage directory
/// * `key` - Storage key
///
/// # Returns
///
/// Returns the storage metadata.
///
/// # Errors
///
/// Returns an error if:
/// - The file does not exist
/// - Metadata file read fails
pub async fn get_metadata(base_path: &Path, key: &str) -> OrcherResult<StorageMetadata> {
    let file_path = base_path.join(key);

    if !file_path.exists() {
        return Err(StorageError::not_found(key).into());
    }

    let metadata_path = file_path.with_extension("wasm.meta");
    read_metadata(&metadata_path)
}

/// Write metadata to a file
fn write_metadata(path: &Path, metadata: &StorageMetadata) -> OrcherResult<()> {
    let json = serde_json::to_string_pretty(metadata).map_err(StorageError::Serialization)?;

    fs::write(path, json).map_err(|e| {
        StorageError::operation_failed_with_source(
            format!("Failed to write metadata file {:?}", path),
            e,
        )
    })?;

    Ok(())
}

/// Read metadata from a file
fn read_metadata(path: &Path) -> OrcherResult<StorageMetadata> {
    if !path.exists() {
        return Err(StorageError::not_found(path.to_string_lossy()).into());
    }

    let json = fs::read_to_string(path).map_err(|e| {
        StorageError::operation_failed_with_source(
            format!("Failed to read metadata file {:?}", path),
            e,
        )
    })?;

    let metadata: StorageMetadata =
        serde_json::from_str(&json).map_err(StorageError::Serialization)?;

    Ok(metadata)
}

/// Compress data using gzip
fn compress_data(data: &[u8]) -> OrcherResult<Vec<u8>> {
    use flate2::write::GzEncoder;
    use flate2::Compression;

    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(data)
        .map_err(|e| StorageError::operation_failed_with_source("Failed to compress data", e))?;

    encoder.finish().map_err(|e| {
        StorageError::operation_failed_with_source("Failed to finish compression", e).into()
    })
}

/// Decompress gzip data
fn decompress_data(data: &[u8]) -> OrcherResult<Vec<u8>> {
    use flate2::read::GzDecoder;
    use std::io::Read;

    let mut decoder = GzDecoder::new(data);
    let mut decompressed = Vec::new();

    decoder
        .read_to_end(&mut decompressed)
        .map_err(|e| StorageError::operation_failed_with_source("Failed to decompress data", e))?;

    Ok(decompressed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_wasm() -> Vec<u8> {
        // WASM magic number + minimal content
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
    async fn test_store_and_retrieve() {
        let temp_dir = TempDir::new().unwrap();
        let config = FilesystemConfig::new(temp_dir.path().to_string_lossy().to_string());

        let wasm_bytes = create_test_wasm();
        let metadata = create_test_metadata(&wasm_bytes);
        let key = "workflows/test/1.0.0/test.wasm";

        // Store
        let stored = store(temp_dir.path(), key, &wasm_bytes, metadata.clone(), &config)
            .await
            .unwrap();

        assert_eq!(stored.key, key);
        assert_eq!(stored.size_bytes, wasm_bytes.len() as u64);

        // Retrieve
        let binary = retrieve(temp_dir.path(), key, &config).await.unwrap();

        assert_eq!(binary.bytes, wasm_bytes);
        assert_eq!(binary.metadata.package_name, "test-workflow");
    }

    #[tokio::test]
    async fn test_store_invalid_wasm() {
        let temp_dir = TempDir::new().unwrap();
        let config = FilesystemConfig::new(temp_dir.path().to_string_lossy().to_string());

        // Invalid WASM (no magic number)
        let invalid_wasm = vec![0x00, 0x00, 0x00, 0x00];
        let metadata = create_test_metadata(&invalid_wasm);
        let key = "workflows/test/1.0.0/test.wasm";

        let result = store(temp_dir.path(), key, &invalid_wasm, metadata, &config).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_store_with_compression() {
        let temp_dir = TempDir::new().unwrap();
        let config = FilesystemConfig::new(temp_dir.path().to_string_lossy().to_string())
            .with_compression(true);

        let wasm_bytes = create_test_wasm();
        let metadata = create_test_metadata(&wasm_bytes);
        let key = "workflows/test/1.0.0/test.wasm";

        // Store with compression
        store(temp_dir.path(), key, &wasm_bytes, metadata, &config)
            .await
            .unwrap();

        // Retrieve with decompression
        let binary = retrieve(temp_dir.path(), key, &config).await.unwrap();

        assert_eq!(binary.bytes, wasm_bytes);
    }

    #[tokio::test]
    async fn test_delete() {
        let temp_dir = TempDir::new().unwrap();
        let config = FilesystemConfig::new(temp_dir.path().to_string_lossy().to_string());

        let wasm_bytes = create_test_wasm();
        let metadata = create_test_metadata(&wasm_bytes);
        let key = "workflows/test/1.0.0/test.wasm";

        // Store
        store(temp_dir.path(), key, &wasm_bytes, metadata, &config)
            .await
            .unwrap();

        // Delete
        delete(temp_dir.path(), key).await.unwrap();

        // Verify deletion
        let result = retrieve(temp_dir.path(), key, &config).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_list() {
        let temp_dir = TempDir::new().unwrap();
        let config = FilesystemConfig::new(temp_dir.path().to_string_lossy().to_string());

        // Store multiple files
        for i in 0..3 {
            let wasm_bytes = create_test_wasm();
            let metadata = create_test_metadata(&wasm_bytes);
            let key = format!("workflows/test-{}/1.0.0/test.wasm", i);

            store(temp_dir.path(), &key, &wasm_bytes, metadata, &config)
                .await
                .unwrap();
        }

        // List all
        let all_files = list(temp_dir.path(), None).await.unwrap();
        assert_eq!(all_files.len(), 3);

        // List with prefix
        let filtered = list(temp_dir.path(), Some("workflows/test-0"))
            .await
            .unwrap();
        assert_eq!(filtered.len(), 1);
    }

    #[tokio::test]
    async fn test_get_metadata() {
        let temp_dir = TempDir::new().unwrap();
        let config = FilesystemConfig::new(temp_dir.path().to_string_lossy().to_string());

        let wasm_bytes = create_test_wasm();
        let metadata = create_test_metadata(&wasm_bytes);
        let key = "workflows/test/1.0.0/test.wasm";

        // Store
        store(temp_dir.path(), key, &wasm_bytes, metadata.clone(), &config)
            .await
            .unwrap();

        // Get metadata
        let retrieved_metadata = get_metadata(temp_dir.path(), key).await.unwrap();

        assert_eq!(retrieved_metadata.package_name, metadata.package_name);
        assert_eq!(retrieved_metadata.sha256, metadata.sha256);
    }
}
