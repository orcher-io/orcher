//! REST API Implementation
//!
//! This module provides a comprehensive REST API that exposes task, workflow,
//! and system management functionality. All operations communicate with the
//! orchestrator service via gRPC clients.
//!
//! # API Versioning
//!
//! The REST API supports multiple versions for backward compatibility:
//! - `v1/` - Current stable API version
//! - `v2/` - Future API version (planned)

/// API Version 1 (current stable)
pub mod v1;

/// Common REST utilities (pagination, filtering, sorting, bulk operations)
pub mod common;

/// API versioning support (detection, compatibility, migration)
pub mod versioning;

/// Custom extractors with improved error handling
pub mod extractors;

// Re-export extractors for convenience
pub use extractors::ValidatedJson;
