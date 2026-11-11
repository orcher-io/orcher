//! In-memory state projections for fast queries.

pub mod queries;
pub mod setup;
pub mod task_state;
pub mod worker_state;
pub mod workflow_state;

#[cfg(test)]
mod integration_tests;

// Re-exports
pub use queries::{ProjectionQuery, QueryBuilder, QueryFilter};
pub use task_state::{TaskPriority, TaskState, TaskStateProjection, TaskStatus};
pub use worker_state::{WorkerState, WorkerStateProjection, WorkerStatus};
pub use workflow_state::{WorkflowState, WorkflowStateProjection, WorkflowStatus};

// Re-export the production-grade ProjectionManager from orcher-events
// This provides automatic event processing, state persistence, and monitoring
pub use orcher_events::projections::ProjectionManager;

// Also re-export projection-related types for convenience
pub use orcher_events::projections::{
    Projection, ProjectionError, ProjectionMetadata, ProjectionStateStore, ProjectionStatus,
};
