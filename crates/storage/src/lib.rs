//! # ORCHER Storage
//!
//! Data persistence and caching layer for the ORCHER task orchestration platform.
//!
//! PostgreSQL schema, migrations and repositories for the engine, typed with
//! `orcher-common`'s identifiers and errors.

#![warn(missing_docs)]
#![warn(clippy::all)]
#![deny(unsafe_code)]
// Allow manual Default impls - often needed for specific initialization logic
#![allow(clippy::derivable_impls)]
// Allow methods that could be trait impls - often intentional API design
#![allow(clippy::should_implement_trait)]
// Allow deprecated usage in tests - projection system tests are deprecated but kept for coverage
#![cfg_attr(test, allow(deprecated))]

// Re-export orcher-common types for convenience
pub use orcher_common::types::identifiers::WorkflowId;
pub use orcher_common::types::identifiers::{ExecutionId, TaskId, UserId};
pub use orcher_common::types::task::TaskStatus;
pub use orcher_common::OrcherResult;

/// Storage error types
pub mod error;

/// Schema definitions and entity types using orcher-common types
pub mod schema;

/// Core storage traits and abstractions
pub mod traits;

/// Database migrations and schema versioning
pub mod migrations;

/// Backend implementations for PostgreSQL and Redis
pub mod backends;

/// Repository implementations for domain entities
pub mod repositories;

/// Recovery from statements prepared against an older schema
pub mod stale_plan;

/// Shard-ownership seams: shard → database pool, and node → owned shards
pub mod sharding;

/// Configuration management integration with orcher-config
pub mod config;

/// Storage utilities and helpers
pub mod utils;

/// Database setup and migration management
pub mod setup;

/// Testing utilities and framework (requires test-utils feature)
#[cfg(feature = "test-utils")]
pub mod testing;

#[cfg(test)]
mod tests {
    use super::*;
    use orcher_common::types::identifiers::traits::Identifier;

    #[test]
    fn test_type_imports() {
        // Verify orcher-common type integration works
        let task_id = TaskId::new_v4();
        assert!(task_id.is_valid());
    }
}
