//! Workflows Models
//!
//! This module defines request and response types for workflow execution operations.

use orcher_common::types::time::Timestamptz;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Request to execute a workflow
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecuteWorkflowRequest {
    /// Workflow name (package name)
    pub workflow_name: String,

    /// Workflow version
    #[serde(default = "default_version")]
    pub workflow_version: String,

    /// Namespace for the workflow
    #[serde(default = "default_namespace")]
    pub namespace: String,

    /// The task queue whose workers run the workflow
    #[serde(default = "default_task_queue")]
    pub task_queue: String,

    /// The workflow's id; the engine chooses one when omitted (derived from
    /// `idempotencyKey`, when one is given, so a repeat names the same
    /// workflow)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<String>,

    /// Input parameters for the workflow (JSON)
    #[serde(default)]
    pub parameters: serde_json::Value,

    /// Optional execution timeout in seconds
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_secs: Option<u64>,

    /// Not supported: runs are handed out in the order they become ready, so
    /// a start that sets this is refused rather than have it ignored
    #[serde(skip_serializing_if = "Option::is_none")]
    pub priority: Option<i32>,

    /// Optional idempotency key: a repeat of a start with the same key
    /// answers the run the first one started
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,

    /// Labels for the execution
    #[serde(default)]
    pub tags: HashMap<String, String>,
}

/// Response after submitting a workflow for execution
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecuteWorkflowResponse {
    /// Unique execution ID
    pub execution_id: String,

    /// The workflow's id
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow_id: Option<String>,

    /// Workflow name
    pub workflow_name: String,

    /// Workflow version
    pub workflow_version: String,

    /// Current execution status
    pub status: ExecutionStatus,

    /// When the execution was submitted
    pub submitted_at: Timestamptz,

    /// Optional idempotency key if provided
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
}

/// Response containing execution status
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionStatusResponse {
    /// Execution ID
    pub execution_id: String,

    /// Workflow name
    pub workflow_name: String,

    /// Workflow version
    pub workflow_version: String,

    /// Namespace the execution runs in
    pub namespace: String,

    /// Task queue its tasks are dispatched on
    pub task_queue: String,

    /// Current execution status
    pub status: ExecutionStatus,

    /// When the execution was submitted
    pub submitted_at: Timestamptz,

    /// When the execution started (if started)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<Timestamptz>,

    /// When the execution completed (if completed)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<Timestamptz>,

    /// Execution progress information
    #[serde(skip_serializing_if = "Option::is_none")]
    pub progress: Option<ExecutionProgress>,

    /// Error message if execution failed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,

    /// Execution tags
    #[serde(default)]
    pub tags: HashMap<String, String>,
}

/// Response containing execution result
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionResultResponse {
    /// Execution ID
    pub execution_id: String,

    /// Workflow name
    pub workflow_name: String,

    /// Workflow version
    pub workflow_version: String,

    /// Final execution status
    pub status: ExecutionStatus,

    /// Execution result (if completed successfully)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,

    /// Error message if execution failed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,

    /// When the execution was submitted
    pub submitted_at: Timestamptz,

    /// When the execution started
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<Timestamptz>,

    /// When the execution completed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<Timestamptz>,

    /// Total execution duration in milliseconds (if completed)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,

    /// Execution statistics
    #[serde(skip_serializing_if = "Option::is_none")]
    pub statistics: Option<ExecutionStatistics>,
}

/// Request to reset an execution to a journal entry
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResetExecutionRequest {
    /// The journal entry to reset to: entries up to and including it are
    /// kept, and the workflow runs again from there
    pub target_event_id: i64,

    /// Why, recorded in the journal
    #[serde(default)]
    pub reason: Option<String>,
}

/// Response after resetting an execution
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResetExecutionResponse {
    /// The execution that was reset
    pub execution_id: String,

    /// The new execution that runs from the journal entry
    pub new_execution_id: String,
}

/// Response after cancelling an execution
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CancelExecutionResponse {
    /// Execution ID
    pub execution_id: String,

    /// New status after cancellation request
    pub status: ExecutionStatus,

    /// Cancellation message
    pub message: String,

    /// When the cancellation was requested
    pub cancelled_at: Timestamptz,
}

/// Response containing list of executions
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListExecutionsResponse {
    /// List of execution summaries
    pub executions: Vec<ExecutionSummary>,

    /// Total number of executions matching the filters
    pub total: u64,

    /// Number of results in this page
    pub count: u32,

    /// Offset used for this page
    pub offset: u32,
}

/// Summary of a workflow execution
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionSummary {
    /// Execution ID
    pub execution_id: String,

    /// Workflow name
    pub workflow_name: String,

    /// Workflow version
    pub workflow_version: String,

    /// Current execution status
    pub status: ExecutionStatus,

    /// When the execution was submitted
    pub submitted_at: Timestamptz,

    /// When the execution completed (if applicable)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<Timestamptz>,

    /// Duration in milliseconds (if completed)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,

    /// Namespace
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,

    /// Task queue
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_queue: Option<String>,

    /// Execution tags
    #[serde(default)]
    pub tags: HashMap<String, String>,
}

/// Execution status enum
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExecutionStatus {
    /// Execution has been submitted but not yet started
    Pending,
    /// Execution is currently running
    Running,
    /// Execution completed successfully
    Completed,
    /// Execution failed with an error
    Failed,
    /// Execution was cancelled: asked to stop, it ran its cleanup and ended
    #[serde(alias = "CANCELED")]
    Cancelled,
    /// Execution was terminated: stopped at once, with no cleanup
    Terminated,
    /// Execution timed out
    TimedOut,
    /// Execution ended by restarting fresh: its workflow goes on in a new run
    RestartedFresh,
    /// Execution was reset: its workflow goes on in a new run from an earlier point
    Reset,
}

/// Execution progress information
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionProgress {
    /// Current step being executed
    pub current_step: String,
    /// Total number of steps in the workflow
    pub total_steps: u32,
    /// Number of completed steps
    pub completed_steps: u32,
    /// Progress percentage (0-100)
    pub progress_percent: f32,
    /// Optional progress message
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// Execution statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionStatistics {
    /// Total number of steps executed
    pub total_steps: u32,
    /// Number of successful steps
    pub successful_steps: u32,
    /// Number of failed steps
    pub failed_steps: u32,
    /// Number of retried steps
    pub retried_steps: u32,
    /// Total execution time in milliseconds
    pub total_duration_ms: u64,
    /// Average step duration in milliseconds
    #[serde(skip_serializing_if = "Option::is_none")]
    pub average_step_duration_ms: Option<u64>,
}

/// Log entry for streaming logs
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogEntry {
    /// Timestamp of the log entry
    pub timestamp: Timestamptz,
    /// Log level (debug, info, warn, error)
    pub level: LogLevel,
    /// Log message
    pub message: String,
    /// Optional step name
    #[serde(skip_serializing_if = "Option::is_none")]
    pub step: Option<String>,
}

/// Log level enum
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

// Default functions for serde
fn default_version() -> String {
    "latest".to_string()
}

fn default_namespace() -> String {
    "default".to_string()
}

fn default_task_queue() -> String {
    "default".to_string()
}

impl ExecutionStatus {
    /// Check if execution is in a terminal state
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            ExecutionStatus::Completed
                | ExecutionStatus::Failed
                | ExecutionStatus::Cancelled
                | ExecutionStatus::Terminated
                | ExecutionStatus::TimedOut
                | ExecutionStatus::RestartedFresh
                | ExecutionStatus::Reset
        )
    }

    /// Check if execution is active
    #[must_use]
    pub fn is_active(&self) -> bool {
        matches!(self, ExecutionStatus::Pending | ExecutionStatus::Running)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_execution_status_is_terminal() {
        assert!(ExecutionStatus::Completed.is_terminal());
        assert!(ExecutionStatus::Failed.is_terminal());
        assert!(!ExecutionStatus::Pending.is_terminal());
    }

    #[test]
    fn test_execution_status_is_active() {
        assert!(ExecutionStatus::Pending.is_active());
        assert!(ExecutionStatus::Running.is_active());
        assert!(!ExecutionStatus::Completed.is_active());
    }
}
