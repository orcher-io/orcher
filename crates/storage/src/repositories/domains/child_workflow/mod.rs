//! # Child Workflow Execution Domain Repository Module
//!
//! This module provides repository implementations for child workflow executions and
//! parent-child workflow relationships. It enables hierarchical workflow composition
//! and lifecycle management.
//!
//! ## ORCHER Terminology
//!
//! - **Child Workflow**: A workflow started by another workflow (parent)
//! - **Parent Workflow**: A workflow that starts one or more child workflows
//! - **Parent Close Policy**: Defines child behavior when parent closes
//!
//! ## Usage
//!
//! ```rust,ignore
//! use orcher_storage::repositories::domains::child_workflow::ChildWorkflowRepository;
//! use orcher_storage::schema::types::child_workflow::{
//!     ChildWorkflowExecution, ChildWorkflowState, OrphanPolicy,
//! };
//!
//! let repo = ChildWorkflowRepository::new(pool);
//!
//! // Create a child workflow record
//! let child = ChildWorkflowExecution::new(
//!     "parent-wf".to_string(),
//!     "parent-exec".to_string(),
//!     1,  // parent_initiated_event_id
//!     "child-wf".to_string(),
//!     "child-exec".to_string(),
//!     "InventoryWorkflow".to_string(),
//!     "default".to_string(),
//!     serde_json::json!({"product_id": "12345"}),
//!     OrphanPolicy::Wait,
//! );
//! let created = repo.create(&child).await?;
//!
//! // Get all children for a parent
//! let children = repo.get_by_parent("parent-wf", "parent-exec").await?;
//!
//! // Get pending (running) children
//! let pending = repo.get_pending_for_parent("parent-wf", "parent-exec").await?;
//!
//! // Update child state
//! repo.update_state(
//!     child.id,
//!     ChildWorkflowState::Completed,
//!     Some(serde_json::json!({"status": "success"})),
//! ).await?;
//! ```
//!
//! ## Key Features
//!
//! - **Parent-Child Tracking**: Maintains relationships between workflows
//! - **Lifecycle Management**: Track child state from INITIATED to terminal states
//! - **Parent Close Policies**: ABANDON, REQUEST_CANCEL, TERMINATE, WAIT
//! - **Flexible Querying**: Query by parent, child, state, or policy
//! - **Thread-Safe**: Uses database constraints for consistency
//!
//! ## Parent Close Policies
//!
//! - **ABANDON**: Child continues running independently when parent closes
//! - **REQUEST_CANCEL**: Request cancellation of child when parent closes
//! - **TERMINATE**: Force terminate child immediately when parent closes
//! - **WAIT**: Parent waits for child completion before closing
//!
//! ## Child Workflow States
//!
//! - **INITIATED**: Parent has requested child creation
//! - **STARTED**: Child workflow has begun execution
//! - **COMPLETED**: Child workflow completed successfully
//! - **FAILED**: Child workflow failed
//! - **TIMED_OUT**: Child workflow timed out
//! - **CANCELED**: Child workflow was canceled
//! - **TERMINATED**: Child workflow was forcefully terminated
//!
//! ## Relationship with V020 and V022
//!
//! This module complements existing event sourcing infrastructure:
//!
//! - **V020 events**: Platform-wide stream-based event store
//! - **V022 execution_log**: Step-level durable execution tracking
//! - **V024 child_workflow_executions**: Parent-child workflow relationships (this module)

pub mod repository;

pub use repository::ChildWorkflowRepository;
