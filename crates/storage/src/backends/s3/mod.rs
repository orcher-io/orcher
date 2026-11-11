//! # S3 Storage Backend
//!
//! AWS S3 storage backend implementation for WASM binaries and workflow packages.
//!
//! ## Overview
//!
//! This module provides S3 storage functionality for WASM workflow binaries with:
//! - Upload/download WASM binaries to/from S3
//! - Generate S3 keys with consistent naming
//! - Pre-signed URL generation for secure downloads
//! - Metadata management
//! - Health checks
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────┐
//! │     WasmStorage Trait                   │
//! │  (Defined in backends::wasm)            │
//! └──────────────┬──────────────────────────┘
//!                │
//!                │ implements
//!                ▼
//! ┌─────────────────────────────────────────┐
//! │         S3Storage                       │
//! │  - AWS SDK for Rust (aws-sdk-s3)       │
//! │  - Bucket management                    │
//! │  - Key generation                       │
//! │  - Pre-signed URLs                      │
//! └─────────────────────────────────────────┘
//! ```
//!
//! ## Configuration
//!
//! The S3 backend requires:
//! - AWS credentials (access key, secret key)
//! - S3 bucket name
//! - AWS region
//! - Optional: Custom endpoint URL (for S3-compatible services like MinIO)
//!
//! ## Usage
//!
//! ```rust,no_run
//! use orcher_storage::backends::s3::{S3Storage, S3Config};
//! use orcher_storage::backends::wasm::{WasmStorage, StorageMetadata};
//!
//! # async fn example() -> orcher_common::OrcherResult<()> {
//! // Create S3 storage backend
//! let config = S3Config {
//!     bucket: "orcher-workflows".to_string(),
//!     region: "us-east-1".to_string(),
//!     access_key: "AKIA...".to_string(),
//!     secret_key: "secret...".to_string(),
//!     endpoint_url: None,
//! };
//!
//! let storage = S3Storage::new(config).await?;
//!
//! // Use the WasmStorage trait
//! let key = storage.generate_key("my-workflow", "1.0.0", "abc123");
//! let metadata = StorageMetadata {
//!     package_name: "my-workflow".to_string(),
//!     package_version: "1.0.0".to_string(),
//!     sha256: "abc123".to_string(),
//!     size_bytes: 1024,
//!     content_type: "application/wasm".to_string(),
//!     manifest: None,
//! };
//!
//! storage.store(&key, &wasm_bytes, metadata).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Features
//!
//! - **Concurrent uploads**: Thread-safe concurrent operations
//! - **Pre-signed URLs**: Generate temporary download links
//! - **Metadata storage**: Store workflow metadata alongside WASM binaries
//! - **Health checks**: Monitor S3 connectivity and bucket access
//! - **S3-compatible**: Works with MinIO, LocalStack, and other S3-compatible services
//!
//! ## Environment Variables
//!
//! The S3 backend can be configured via environment variables:
//!
//! - `S3_BUCKET` - S3 bucket name
//! - `S3_REGION` - AWS region
//! - `AWS_ACCESS_KEY_ID` - AWS access key
//! - `AWS_SECRET_ACCESS_KEY` - AWS secret key
//! - `S3_ENDPOINT_URL` - Custom endpoint URL (optional)

pub mod client;
pub mod config;
pub mod storage;

pub use client::S3Client;
pub use config::S3Config;
pub use storage::S3Storage;

// Re-export for convenience
pub use crate::backends::wasm::{StorageMetadata, WasmBinary, WasmInfo, WasmStorage};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_s3_module_exists() {
        // Compile-time test to ensure module structure is correct
        assert!(true);
    }
}
