//! Central orchestration engine for the ORCHER platform.

#![warn(missing_docs)]
#![warn(clippy::all)]
#![allow(clippy::derivable_impls)]
#![allow(clippy::type_complexity)]
#![allow(clippy::too_many_arguments)]
// tonic's generated service traits return `Result<_, tonic::Status>`, and the
// helpers they call return the same, so the error cannot be boxed where it is
// produced without unboxing it again at every call site.
#![allow(clippy::result_large_err)]
#![deny(unsafe_code)]
#![cfg_attr(docsrs, feature(doc_cfg))]

pub use orcher_common::error::{OrcherError, OrcherResult};
pub use orcher_config::prelude::OrcherConfig;
pub use orcher_monitoring;
pub use orcher_storage;

/// Configuration management.
pub mod config;
/// Core error types.
pub mod error;
mod types;

/// Server-side actor coordination.
pub mod actor;
/// Who a caller is: the one authentication seam for every API.
pub mod auth;
/// Workflow chronicle — search and query layer for execution history.
pub mod chronicle;
/// Coordination engine (outbox, timers, dispatch, state).
pub mod engine;
/// Event sourcing infrastructure.
pub mod events;
/// Runtime feature flags.
pub mod feature_flags;
/// gRPC service implementations.
pub mod grpc;
/// Periodic background jobs (metrics aggregation, data retention).
pub mod jobs;
/// In-memory state projections.
pub mod projections;
/// Retry policies and timeout detection.
pub mod retries;
/// Background services (claim cleanup, maintenance).
pub mod services;
/// Utility functions.
pub mod utils;
/// Worker registration and lifecycle tracking.
pub mod worker;
/// Workflow execution, journal, commands, and retry services.
pub mod workflows;

/// Multi-tenant isolation and resource management.
pub mod tenancy;

/// Common re-exports.
pub mod prelude {
    pub use crate::error::{CoreError, CoreResult};

    pub use crate::types::{
        OrchestrationMetrics, OrchestrationState, OrchestrationStateTransition, OrchestrationStats,
        PerformanceMetrics, PoolCapacity, ResourceAllocation, ResourceMetrics, ResourcePool,
        ResourceQuota, ResourceRequirements, ResourceUsage, SecurityContext, StateTransition,
        TaskExecutionContext, TaskExecutionResult, TaskMetrics, TaskPriority, TaskSubmission,
        TaskSubmissionBuilder, WorkerCapability, WorkerHealthStatus, WorkerMetadata, WorkerMetrics,
        WorkerPool, WorkerRegistration, WorkerStatus,
    };

    pub use orcher_common::prelude::*;
    pub use orcher_config::prelude::*;
}

/// Library version.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Returns the crate version string.
pub fn version() -> &'static str {
    VERSION
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version() {
        let version = version();
        assert!(!version.is_empty());
        assert_eq!(version, VERSION);
    }
}
