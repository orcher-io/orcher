//! # Storage Backends Module
//!
//! Backend implementations for PostgreSQL, Redis, and WASM binary storage.

/// PostgreSQL backend implementation
#[cfg(feature = "postgres")]
pub mod postgres;

/// Redis backend implementation
#[cfg(feature = "redis")]
pub mod redis;

/// WASM binary storage abstraction
pub mod wasm;

/// Filesystem storage backend
pub mod filesystem;

/// S3 storage backend
#[cfg(feature = "s3")]
pub mod s3;

// Re-export backend types for convenience
#[cfg(feature = "postgres")]
pub use postgres::{PostgresBackend, PostgresConfig};

#[cfg(feature = "redis")]
pub use redis::{RedisBackend, RedisConfig};

// Re-export filesystem storage types
pub use filesystem::{FilesystemConfig, FilesystemStorage};

// Re-export S3 storage types
#[cfg(feature = "s3")]
pub use s3::{S3Client, S3Config, S3Storage};

// Re-export WASM storage types
pub use wasm::{
    HealthStatus, StorageBackendType, StorageConfig, StorageError, StorageFactory, StorageMetadata,
    StoredWasm, WasmBinary, WasmInfo, WasmStorage,
};
