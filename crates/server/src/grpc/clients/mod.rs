//! gRPC client implementations for ORCHER services
//!
//! This module provides client implementations for communicating with
//! ORCHER services via gRPC using modern pull-based APIs.
//!
//! # Available Clients
//!
//! - [`workflow::WorkflowServiceClient`] - Workflow lifecycle operations (start, cancel, query, etc.)
//! - [`execution::ExecutionServiceClient`] - Task/workflow execution polling and completion
//!
//! # Architecture
//!
//! These clients implement the modern pull-based execution model where:
//! - Workers poll for work instead of receiving push notifications
//! - Workflow operations use WorkflowService for lifecycle management
//! - Task/workflow execution uses ExecutionService for polling and completion
//!
//! # Migration from OrchestratorService
//!
//! The old `OrchestratorService` has been replaced with:
//! - `WorkflowService` - for workflow operations (Start, Status, Cancel, Query)
//! - `ExecutionService` - for execution polling (PollTask, PollWorkflow, Complete)
//!
//! # Example
//!
//! ```rust,no_run
//! use orcher_server::grpc::clients::{
//!     workflow::WorkflowServiceClient,
//!     execution::ExecutionServiceClient,
//! };
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create clients
//! let workflow_client = WorkflowServiceClient::connect("http://localhost:50051").await?;
//! let execution_client = ExecutionServiceClient::connect("http://localhost:50051").await?;
//!
//! // Start a workflow
//! let started = workflow_client
//!     .start_workflow(orcher_proto::orcher::v1::StartWorkflowRequest {
//!         workflow_type: "my-workflow".into(),
//!         task_queue: "default-queue".into(),
//!         namespace: "default".into(),
//!         ..Default::default()
//!     })
//!     .await?;
//!
//! // Poll for task execution work
//! if let Some(work) = execution_client.poll_task_execution("default-queue").await? {
//!     // Execute the task...
//!     execution_client.complete_task_execution(&work.task_token, None).await?;
//! }
//! # Ok(())
//! # }
//! ```

pub mod execution;
pub mod namespace;
pub mod workflow;

// Re-export commonly used types
pub use execution::{ExecutionServiceClient, TaskExecutionWork, WorkflowExecutionWork};
pub use namespace::NamespaceServiceClient;
pub use workflow::{WorkflowServiceClient, WorkflowStatusInfo};
