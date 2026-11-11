//! Common types for WASM storage
//!
//! This module defines the data structures used across all WASM storage backends.

use orcher_common::types::time::Timestamptz;
use serde::{Deserialize, Serialize};

/// Metadata for storing WASM binaries
///
/// This structure contains all the metadata associated with a WASM binary,
/// including package information, checksums, and optional manifest data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageMetadata {
    /// Package name (e.g., "my-workflow")
    pub package_name: String,

    /// Package version (e.g., "1.0.0")
    pub package_version: String,

    /// SHA256 checksum of the WASM binary
    pub sha256: String,

    /// Size of the WASM binary in bytes
    pub size_bytes: u64,

    /// Content type (typically "application/wasm")
    #[serde(default = "default_content_type")]
    pub content_type: String,

    /// Optional manifest JSON data
    pub manifest: Option<serde_json::Value>,

    /// Optional additional metadata
    #[serde(default)]
    pub extra: std::collections::HashMap<String, String>,
}

fn default_content_type() -> String {
    "application/wasm".to_string()
}

impl StorageMetadata {
    /// Create new storage metadata with required fields
    pub fn new(
        package_name: String,
        package_version: String,
        sha256: String,
        size_bytes: u64,
    ) -> Self {
        Self {
            package_name,
            package_version,
            sha256,
            size_bytes,
            content_type: default_content_type(),
            manifest: None,
            extra: std::collections::HashMap::new(),
        }
    }

    /// Add manifest data
    pub fn with_manifest(mut self, manifest: serde_json::Value) -> Self {
        self.manifest = Some(manifest);
        self
    }

    /// Add extra metadata field
    pub fn with_extra(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.extra.insert(key.into(), value.into());
        self
    }

    /// Set content type
    pub fn with_content_type(mut self, content_type: String) -> Self {
        self.content_type = content_type;
        self
    }
}

/// Information about a stored WASM binary (after successful storage)
///
/// This structure is returned after successfully storing a WASM binary
/// and contains the storage location and metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredWasm {
    /// Storage key used to store the binary
    pub key: String,

    /// Full storage path (backend-specific, e.g., file path or S3 URL)
    pub storage_path: String,

    /// Size of the stored binary in bytes
    pub size_bytes: u64,

    /// SHA256 checksum of the stored binary
    pub sha256: String,

    /// When the binary was stored
    pub stored_at: Timestamptz,

    /// Backend that stored the binary
    #[serde(default)]
    pub backend: String,
}

impl StoredWasm {
    /// Create new stored WASM information
    pub fn new(
        key: String,
        storage_path: String,
        size_bytes: u64,
        sha256: String,
        backend: String,
    ) -> Self {
        Self {
            key,
            storage_path,
            size_bytes,
            sha256,
            stored_at: Timestamptz::now(),
            backend,
        }
    }
}

/// WASM binary retrieved from storage
///
/// This structure contains the WASM binary data along with its metadata.
/// It is returned when retrieving a WASM binary from storage.
#[derive(Debug)]
pub struct WasmBinary {
    /// WASM binary data
    pub bytes: Vec<u8>,

    /// Metadata about the WASM binary
    pub metadata: StorageMetadata,

    /// When the binary was retrieved
    pub retrieved_at: Timestamptz,
}

impl WasmBinary {
    /// Create new WASM binary
    pub fn new(bytes: Vec<u8>, metadata: StorageMetadata) -> Self {
        Self {
            bytes,
            metadata,
            retrieved_at: Timestamptz::now(),
        }
    }

    /// Get the size of the WASM binary
    pub fn size(&self) -> usize {
        self.bytes.len()
    }

    /// Check if the WASM binary is valid (has WASM magic number)
    pub fn is_valid_wasm(&self) -> bool {
        self.bytes.len() >= 4 && &self.bytes[0..4] == b"\0asm"
    }

    /// Verify the SHA256 checksum
    pub fn verify_checksum(&self) -> bool {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(&self.bytes);
        let computed = format!("{:x}", hasher.finalize());
        computed == self.metadata.sha256
    }
}

/// WASM binary information (for listing)
///
/// This structure contains summary information about a stored WASM binary,
/// used when listing available binaries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasmInfo {
    /// Storage key
    pub key: String,

    /// Package name
    pub package_name: String,

    /// Package version
    pub package_version: String,

    /// Size in bytes
    pub size_bytes: u64,

    /// SHA256 checksum
    pub sha256: String,

    /// When the binary was stored
    pub stored_at: Timestamptz,

    /// Backend type
    #[serde(default)]
    pub backend: String,
}

impl WasmInfo {
    /// Create new WASM info
    pub fn new(
        key: String,
        package_name: String,
        package_version: String,
        size_bytes: u64,
        sha256: String,
        stored_at: Timestamptz,
        backend: String,
    ) -> Self {
        Self {
            key,
            package_name,
            package_version,
            size_bytes,
            sha256,
            stored_at,
            backend,
        }
    }

    /// Get human-readable size
    pub fn size_human(&self) -> String {
        format_bytes(self.size_bytes)
    }
}

/// Format bytes into human-readable format
fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
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

    #[test]
    fn test_storage_metadata_new() {
        let metadata = StorageMetadata::new(
            "test-workflow".to_string(),
            "1.0.0".to_string(),
            "abc123".to_string(),
            1024,
        );

        assert_eq!(metadata.package_name, "test-workflow");
        assert_eq!(metadata.package_version, "1.0.0");
        assert_eq!(metadata.sha256, "abc123");
        assert_eq!(metadata.size_bytes, 1024);
        assert_eq!(metadata.content_type, "application/wasm");
        assert!(metadata.manifest.is_none());
    }

    #[test]
    fn test_storage_metadata_builder() {
        let manifest = serde_json::json!({"workflows": []});
        let metadata = StorageMetadata::new(
            "test".to_string(),
            "1.0.0".to_string(),
            "abc".to_string(),
            1024,
        )
        .with_manifest(manifest.clone())
        .with_extra("author", "test")
        .with_content_type("application/wasm+gzip".to_string());

        assert_eq!(metadata.manifest, Some(manifest));
        assert_eq!(metadata.extra.get("author"), Some(&"test".to_string()));
        assert_eq!(metadata.content_type, "application/wasm+gzip");
    }

    #[test]
    fn test_stored_wasm_new() {
        let stored = StoredWasm::new(
            "workflows/test/1.0.0/abc.wasm".to_string(),
            "/tmp/workflows/test/1.0.0/abc.wasm".to_string(),
            1024,
            "abc123".to_string(),
            "filesystem".to_string(),
        );

        assert_eq!(stored.key, "workflows/test/1.0.0/abc.wasm");
        assert_eq!(stored.size_bytes, 1024);
        assert_eq!(stored.sha256, "abc123");
        assert_eq!(stored.backend, "filesystem");
    }

    #[test]
    fn test_wasm_binary_valid() {
        let metadata = StorageMetadata::new(
            "test".to_string(),
            "1.0.0".to_string(),
            "abc".to_string(),
            4,
        );

        // Valid WASM magic number: 0x00 0x61 0x73 0x6d (\0asm)
        let binary = WasmBinary::new(vec![0x00, 0x61, 0x73, 0x6d], metadata);

        assert!(binary.is_valid_wasm());
        assert_eq!(binary.size(), 4);
    }

    #[test]
    fn test_wasm_binary_invalid() {
        let metadata = StorageMetadata::new(
            "test".to_string(),
            "1.0.0".to_string(),
            "abc".to_string(),
            4,
        );

        let binary = WasmBinary::new(vec![0x00, 0x00, 0x00, 0x00], metadata);

        assert!(!binary.is_valid_wasm());
    }

    #[test]
    fn test_wasm_binary_verify_checksum() {
        let wasm_bytes = vec![0x00, 0x61, 0x73, 0x6d];

        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(&wasm_bytes);
        let sha256 = format!("{:x}", hasher.finalize());

        let metadata = StorageMetadata::new("test".to_string(), "1.0.0".to_string(), sha256, 4);

        let binary = WasmBinary::new(wasm_bytes, metadata);
        assert!(binary.verify_checksum());
    }

    #[test]
    fn test_wasm_info_new() {
        let info = WasmInfo::new(
            "workflows/test/1.0.0/abc.wasm".to_string(),
            "test".to_string(),
            "1.0.0".to_string(),
            1024,
            "abc123".to_string(),
            Timestamptz::now(),
            "filesystem".to_string(),
        );

        assert_eq!(info.package_name, "test");
        assert_eq!(info.size_bytes, 1024);
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(512), "512 bytes");
        assert_eq!(format_bytes(1024), "1.00 KB");
        assert_eq!(format_bytes(1536), "1.50 KB");
        assert_eq!(format_bytes(1048576), "1.00 MB");
        assert_eq!(format_bytes(1073741824), "1.00 GB");
    }

    #[test]
    fn test_wasm_info_size_human() {
        let info = WasmInfo::new(
            "test".to_string(),
            "test".to_string(),
            "1.0.0".to_string(),
            1048576,
            "abc".to_string(),
            Timestamptz::now(),
            "filesystem".to_string(),
        );

        assert_eq!(info.size_human(), "1.00 MB");
    }
}
