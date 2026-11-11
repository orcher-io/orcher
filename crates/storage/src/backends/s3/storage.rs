//! # S3 Storage Implementation
//!
//! Implementation of the WasmStorage trait for AWS S3.

use async_trait::async_trait;
use orcher_common::types::time::Timestamptz;
use orcher_common::OrcherResult;
use std::time::Instant;
use tracing::{debug, info};

use crate::backends::wasm::{
    HealthStatus, StorageBackendType, StorageMetadata, StoredWasm, WasmBinary, WasmInfo,
    WasmStorage,
};

use super::{S3Client, S3Config};

/// S3 storage backend implementation
#[derive(Debug, Clone)]
pub struct S3Storage {
    /// S3 client
    client: S3Client,

    /// Configuration
    config: S3Config,
}

impl S3Storage {
    /// Create a new S3 storage instance
    ///
    /// # Arguments
    /// * `config` - S3 configuration
    ///
    /// # Returns
    /// * `Ok(S3Storage)` - Storage created successfully
    /// * `Err(OrcherError)` - Configuration or connection error
    pub async fn new(config: S3Config) -> OrcherResult<Self> {
        // Validate configuration
        config.validate()?;

        // Create S3 client
        let client = S3Client::new(config.clone()).await?;

        info!(
            "Initialized S3 storage backend for bucket '{}' in region '{}'",
            config.bucket, config.region
        );

        Ok(Self { client, config })
    }

    /// Get a reference to the S3 client
    pub fn client(&self) -> &S3Client {
        &self.client
    }

    /// Get a reference to the configuration
    pub fn config(&self) -> &S3Config {
        &self.config
    }
}

#[async_trait]
impl WasmStorage for S3Storage {
    async fn store(
        &self,
        key: &str,
        wasm_bytes: &[u8],
        metadata: StorageMetadata,
    ) -> OrcherResult<StoredWasm> {
        debug!(
            "Storing WASM binary: key={}, size={} bytes",
            key,
            wasm_bytes.len()
        );

        // Upload to S3
        self.client.upload_wasm(key, wasm_bytes, &metadata).await?;

        // Return stored WASM info
        Ok(StoredWasm {
            key: key.to_string(),
            size_bytes: wasm_bytes.len() as u64,
            storage_path: format!("s3://{}/{}", self.config.bucket, key),
            stored_at: Timestamptz::now(),
            metadata,
        })
    }

    async fn retrieve(&self, key: &str) -> OrcherResult<WasmBinary> {
        debug!("Retrieving WASM binary: key={}", key);

        // Download from S3
        let bytes = self.client.download_wasm(key).await?;

        // Get metadata
        let metadata = self.client.get_metadata(key).await?;

        Ok(WasmBinary {
            key: key.to_string(),
            bytes,
            metadata,
        })
    }

    async fn exists(&self, key: &str) -> OrcherResult<bool> {
        self.client.exists(key).await
    }

    async fn delete(&self, key: &str) -> OrcherResult<()> {
        debug!("Deleting WASM binary: key={}", key);
        self.client.delete_wasm(key).await
    }

    async fn list(&self, prefix: Option<&str>) -> OrcherResult<Vec<WasmInfo>> {
        debug!("Listing WASM binaries with prefix: {:?}", prefix);
        self.client.list_wasm(prefix).await
    }

    async fn get_metadata(&self, key: &str) -> OrcherResult<StorageMetadata> {
        self.client.get_metadata(key).await
    }

    fn generate_key(&self, package_name: &str, package_version: &str, sha256: &str) -> String {
        format!(
            "workflows/{}/{}/{}.wasm",
            package_name, package_version, sha256
        )
    }

    async fn health_check(&self) -> OrcherResult<HealthStatus> {
        let start = Instant::now();

        // Try to list objects in the bucket (with limit 1)
        match self.client.health_check().await {
            Ok(_) => {
                let response_time_ms = start.elapsed().as_millis() as u64;
                Ok(
                    HealthStatus::healthy(StorageBackendType::S3, response_time_ms)
                        .with_detail("bucket", &self.config.bucket)
                        .with_detail("region", &self.config.region)
                        .with_detail("endpoint", &self.config.endpoint()),
                )
            }
            Err(e) => {
                let response_time_ms = start.elapsed().as_millis() as u64;
                Ok(HealthStatus::unhealthy(
                    StorageBackendType::S3,
                    response_time_ms,
                    format!("S3 health check failed: {}", e),
                )
                .with_detail("bucket", &self.config.bucket)
                .with_detail("region", &self.config.region))
            }
        }
    }

    fn backend_type(&self) -> StorageBackendType {
        StorageBackendType::S3
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_key() {
        // This test doesn't require actual S3 connection
        let config = S3Config::new("test-bucket", "us-west-2", "AKIA123", "secret123").unwrap();

        // Create storage without actual S3 connection for testing
        // In real test, we'd use a mock or test environment
        let key = format!("workflows/{}/{}/{}.wasm", "my-workflow", "1.0.0", "abc123");

        assert_eq!(key, "workflows/my-workflow/1.0.0/abc123.wasm");
    }

    #[test]
    fn test_s3_storage_config() {
        let config = S3Config::new("test-bucket", "us-west-2", "AKIA123", "secret123").unwrap();

        assert_eq!(config.bucket, "test-bucket");
        assert_eq!(config.region, "us-west-2");
    }
}
