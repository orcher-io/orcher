//! # Workflow Execution Domain Repository Module
//!
//! This module provides repository implementations for workflow execution persistence.
//! It handles CRUD operations, hierarchy queries, progress tracking, filtering, and analytics
//! for workflow execution instances.
//!
//! ## Usage
//!
//! ```rust,ignore
//! use orcher_storage::repositories::domains::workflow_execution::WorkflowExecutionRepository;
//! use orcher_storage::schema::types::execution::WorkflowExecution;
//!
//! let repo = WorkflowExecutionRepository::new(pool);
//!
//! // Create execution
//! let execution = WorkflowExecution::new(workflow_id, input_data, ExecutionTrigger::Manual, None);
//! let created = repo.create(execution).await?;
//!
//! // Find by ID
//! let found = repo.find_by_id(execution_id).await?;
//!
//! // Update progress
//! repo.update_progress(execution_id, 5, 10).await?;
//!
//! // Complete successfully
//! repo.complete_success(execution_id, output_data).await?;
//!
//! // Query hierarchy
//! let children = repo.find_children(parent_id).await?;
//! ```

pub mod logs;
pub mod repository;

#[cfg(test)]
mod pending_requeue_race_tests;

pub use logs::{
    init_async_log_writer, LogWriterConfig, WorkflowExecutionLog, WorkflowExecutionLogsRepository,
};
pub use repository::{
    is_start_request_held, is_workflow_id_held, release_claim_to_resume_on, start_dispatch_payload,
    ActivationFence, CancelRequestTarget, ClaimFailureReason, ClaimStateCheck, DeadNode,
    DeferredResume, DueStart, HandOut, ListExecutionsFilters, PendingRequeueInfo,
    PendingRequeueWorkflow, ReleasedClaim, Resume, RunRef, WorkflowExecutionRepository,
    DEFAULT_IN_FLIGHT_BOUND_SECS, DEFAULT_JOURNAL_HOLD, START_REQUEST_INDEX,
    WORKFLOW_ID_HOLD_INDEX,
};
