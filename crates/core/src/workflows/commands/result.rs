//! Command processing result types.

use orcher_storage::schema::types::TaskExecution;

/// Result of command processing, including any scheduled tasks for eager dispatch
///
/// This enables the Eager Task Return optimization where scheduled tasks
/// can be returned in the CompleteWorkflowExecutionResponse, avoiding a poll round-trip.
#[derive(Debug, Default)]
pub struct CommandProcessingResult {
    /// Tasks that were scheduled during command processing
    /// These can be eagerly dispatched to the completing worker if it has capacity
    pub scheduled_tasks: Vec<TaskExecution>,

    /// Whether the workflow completed (CompleteWorkflow command was processed)
    pub workflow_completed: bool,

    /// Whether the workflow failed (FailWorkflow command was processed)
    pub workflow_failed: bool,

    /// Whether the workflow ended itself as cancelled (CancelWorkflow command
    /// was processed)
    pub workflow_cancelled: bool,

    /// The failure type the workflow failed with, when it named one: what a
    /// retry policy's non-retryable error types are matched against.
    pub failure_type: Option<String>,

    /// Whether the workflow code raised the failure as non-retryable: such a
    /// failure is not retried, whatever the retry policy allows.
    pub non_retryable: bool,

    /// Number of commands successfully processed
    pub commands_processed: usize,
}

impl CommandProcessingResult {
    /// Create an empty result
    pub fn new() -> Self {
        Self::default()
    }

    /// Check if any tasks were scheduled
    pub fn has_scheduled_tasks(&self) -> bool {
        !self.scheduled_tasks.is_empty()
    }

    /// Check if the workflow has terminated (completed, failed or cancelled)
    pub fn is_terminated(&self) -> bool {
        self.workflow_completed || self.workflow_failed || self.workflow_cancelled
    }
}
