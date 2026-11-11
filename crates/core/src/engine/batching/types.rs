//! Batching types and configuration.

use orcher_common::types::time::Timestamptz;
use orcher_storage::schema::types::journal::JournalEntry;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use tokio::sync::oneshot;
use uuid::Uuid;

/// A write operation that can be batched
#[derive(Debug)]
pub enum WriteOperation {
    /// Append a journal entry
    JournalAppend(JournalAppendOp),

    /// Update workflow execution state
    StateUpdate(StateUpdateOp),

    /// Insert an outbox task
    OutboxInsert(OutboxInsertOp),

    /// Complete an outbox task
    OutboxComplete(OutboxCompleteOp),
}

impl WriteOperation {
    /// Get the operation type as a string (for metrics/logging)
    pub fn operation_type(&self) -> &'static str {
        match self {
            WriteOperation::JournalAppend(_) => "journal_append",
            WriteOperation::StateUpdate(_) => "state_update",
            WriteOperation::OutboxInsert(_) => "outbox_insert",
            WriteOperation::OutboxComplete(_) => "outbox_complete",
        }
    }

    /// Get the workflow_id associated with this operation (for partitioning)
    pub fn workflow_id(&self) -> Option<&str> {
        match self {
            WriteOperation::JournalAppend(op) => Some(&op.workflow_id),
            WriteOperation::StateUpdate(op) => Some(&op.workflow_id),
            WriteOperation::OutboxInsert(op) => Some(&op.workflow_id),
            WriteOperation::OutboxComplete(op) => op.workflow_id.as_deref(),
        }
    }
}

/// Journal append operation
#[derive(Debug, Clone)]
pub struct JournalAppendOp {
    /// Workflow ID
    pub workflow_id: String,
    /// Execution ID
    pub execution_id: String,
    /// Event ID (sequence number)
    pub event_id: i64,
    /// Event type
    pub event_type: String,
    /// Event payload
    pub payload: JsonValue,
    /// Timestamp
    pub timestamp: Timestamptz,
}

impl From<JournalAppendOp> for JournalEntry {
    fn from(op: JournalAppendOp) -> Self {
        JournalEntry::new(
            op.workflow_id,
            op.execution_id,
            op.event_id,
            op.event_type,
            op.payload,
        )
    }
}

/// State update operation
#[derive(Debug, Clone)]
pub struct StateUpdateOp {
    /// Workflow ID
    pub workflow_id: String,
    /// Execution ID
    pub execution_id: String,
    /// New status (optional)
    pub status: Option<String>,
    /// Current step (optional)
    pub current_step: Option<String>,
    /// Result (for completion)
    pub result: Option<JsonValue>,
    /// Error message (for failure)
    pub error: Option<String>,
    /// Completed timestamp
    pub completed_at: Option<Timestamptz>,
}

/// Outbox insert operation
#[derive(Debug, Clone)]
pub struct OutboxInsertOp {
    /// Unique task ID
    pub task_id: Uuid,
    /// Workflow ID
    pub workflow_id: String,
    /// Execution ID
    pub execution_id: String,
    /// Task type (e.g., "dispatch_task", "send_signal")
    pub task_type: String,
    /// Task payload
    pub payload: JsonValue,
    /// Scheduled time (for delayed tasks)
    pub scheduled_at: Option<Timestamptz>,
}

/// Outbox complete operation
#[derive(Debug, Clone)]
pub struct OutboxCompleteOp {
    /// Task ID to complete
    pub task_id: Uuid,
    /// Optional workflow ID (for logging)
    pub workflow_id: Option<String>,
    /// Success or failure
    pub success: bool,
    /// Error message if failed
    pub error: Option<String>,
}

/// A batched write with its completion notification channel
#[derive(Debug)]
pub struct BatchedWrite {
    /// The write operation
    pub operation: WriteOperation,
    /// Channel to notify when write completes
    pub notifier: oneshot::Sender<BatchWriteResult>,
    /// When the write was queued
    pub queued_at: std::time::Instant,
}

impl BatchedWrite {
    /// Create a new batched write
    pub fn new(operation: WriteOperation) -> (Self, oneshot::Receiver<BatchWriteResult>) {
        let (tx, rx) = oneshot::channel();
        (
            Self {
                operation,
                notifier: tx,
                queued_at: std::time::Instant::now(),
            },
            rx,
        )
    }

    /// Notify the caller of completion
    pub fn complete(self, result: BatchWriteResult) {
        // Ignore error if receiver dropped (caller gave up waiting)
        let _ = self.notifier.send(result);
    }
}

/// Result of a batched write operation
#[derive(Debug, Clone)]
pub enum BatchWriteResult {
    /// Write succeeded
    Success,
    /// Write failed with error
    Error(String),
}

impl BatchWriteResult {
    /// Check if the result is successful
    pub fn is_success(&self) -> bool {
        matches!(self, BatchWriteResult::Success)
    }

    /// Get error message if failed
    pub fn error(&self) -> Option<&str> {
        match self {
            BatchWriteResult::Error(msg) => Some(msg),
            BatchWriteResult::Success => None,
        }
    }
}

/// Statistics for batch operations
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BatchStats {
    /// Total writes processed
    pub total_writes: u64,
    /// Total batches executed
    pub total_batches: u64,
    /// Average batch size
    pub avg_batch_size: f64,
    /// Total errors
    pub total_errors: u64,
    /// Average wait time in microseconds
    pub avg_wait_time_us: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_operation_type() {
        let op = WriteOperation::JournalAppend(JournalAppendOp {
            workflow_id: "wf-1".to_string(),
            execution_id: "ex-1".to_string(),
            event_id: 1,
            event_type: "WorkflowStarted".to_string(),
            payload: serde_json::json!({}),
            timestamp: Timestamptz::now(),
        });
        assert_eq!(op.operation_type(), "journal_append");
    }

    #[test]
    fn test_batched_write_completion() {
        let op = WriteOperation::JournalAppend(JournalAppendOp {
            workflow_id: "wf-1".to_string(),
            execution_id: "ex-1".to_string(),
            event_id: 1,
            event_type: "WorkflowStarted".to_string(),
            payload: serde_json::json!({}),
            timestamp: Timestamptz::now(),
        });

        let (write, rx) = BatchedWrite::new(op);
        write.complete(BatchWriteResult::Success);

        let result = rx.blocking_recv().unwrap();
        assert!(result.is_success());
    }

    #[test]
    fn test_batch_write_result() {
        let success = BatchWriteResult::Success;
        assert!(success.is_success());
        assert!(success.error().is_none());

        let error = BatchWriteResult::Error("test error".to_string());
        assert!(!error.is_success());
        assert_eq!(error.error(), Some("test error"));
    }
}
