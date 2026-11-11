//! Standardized span creation helpers for ORCHER components
//!
//! This module provides consistent span creation functions for all ORCHER
//! components, following OpenTelemetry semantic conventions and best practices.

use orcher_common::types::identifiers::*;
use tracing::{info_span, Span};

/// Create a standardized span for task operations
///
/// Creates a span with consistent naming and attributes for task-related operations.
/// Includes task ID, operation type, and other relevant metadata.
///
/// # Arguments
///
/// * `operation` - The operation being performed (e.g., "execute", "submit", "complete")
/// * `task_id` - The unique identifier for the task
/// * `task_type` - Optional task type for categorization
///
/// # Examples
///
/// ```rust
/// use orcher_monitoring::tracing::create_task_span;
/// use orcher_common::types::identifiers::TaskId;
///
/// let task_id = TaskId::new_v4();
/// let span = create_task_span("execute", &task_id, Some("data_processing"));
/// let _guard = span.enter();
/// // Task execution code here
/// ```
pub fn create_task_span(operation: &str, task_id: &TaskId, task_type: Option<&str>) -> Span {
    let span = info_span!(
        "task_operation",
        otel.name = format!("task.{}", operation),
        otel.kind = "internal",
        task.id = %task_id,
        task.operation = operation,
    );

    if let Some(t_type) = task_type {
        span.record("task.type", t_type);
    }

    span
}

/// Create a standardized span for workflow operations
///
/// Creates a span with consistent naming and attributes for workflow-related operations.
/// Includes workflow ID, step information, and execution context.
///
/// # Arguments
///
/// * `operation` - The operation being performed (e.g., "start", "step", "complete")
/// * `workflow_id` - The unique identifier for the workflow
/// * `step_name` - Optional name of the current workflow step
///
/// # Examples
///
/// ```rust
/// use orcher_monitoring::tracing::create_workflow_span;
/// use orcher_common::types::identifiers::WorkflowId;
///
/// let workflow_id = WorkflowId::new_v4();
/// let span = create_workflow_span("step", &workflow_id, Some("data_validation"));
/// let _guard = span.enter();
/// // Workflow step execution code here
/// ```
pub fn create_workflow_span(
    operation: &str,
    workflow_id: &WorkflowId,
    step_name: Option<&str>,
) -> Span {
    let span = info_span!(
        "workflow_operation",
        otel.name = format!("workflow.{}", operation),
        otel.kind = "internal",
        workflow.id = %workflow_id,
        workflow.operation = operation,
    );

    if let Some(step) = step_name {
        span.record("workflow.step", step);
    }

    span
}

/// Create a standardized span for worker operations
///
/// Creates a span with consistent naming and attributes for worker-related operations.
/// Includes worker ID, node information, and operation context.
///
/// # Arguments
///
/// * `operation` - The operation being performed (e.g., "start", "process", "idle")
/// * `worker_id` - The unique identifier for the worker
/// * `node_id` - Optional node identifier where the worker is running
///
/// # Examples
///
/// ```rust
/// use orcher_monitoring::tracing::create_worker_span;
/// use orcher_common::types::identifiers::{WorkerId, NodeId};
///
/// let worker_id = WorkerId::new_v4();
/// let node_id = NodeId::new_v4();
/// let span = create_worker_span("process", &worker_id, Some(&node_id));
/// let _guard = span.enter();
/// // Worker processing code here
/// ```
pub fn create_worker_span(operation: &str, worker_id: &WorkerId, node_id: Option<&NodeId>) -> Span {
    let span = info_span!(
        "worker_operation",
        otel.name = format!("worker.{}", operation),
        otel.kind = "internal",
        worker.id = %worker_id,
        worker.operation = operation,
    );

    if let Some(node) = node_id {
        span.record("worker.node_id", format!("{}", node));
    }

    span
}

/// Create a standardized span for storage operations
///
/// Creates a span with consistent naming and attributes for storage-related operations.
/// Includes operation type, table/collection information, and performance context.
///
/// # Arguments
///
/// * `operation` - The storage operation (e.g., "read", "write", "delete", "query")
/// * `table_name` - Name of the table or collection being accessed
/// * `record_count` - Optional number of records involved in the operation
///
/// # Examples
///
/// ```rust
/// use orcher_monitoring::tracing::create_storage_span;
///
/// let span = create_storage_span("query", "tasks", Some(100));
/// let _guard = span.enter();
/// // Database query code here
/// ```
pub fn create_storage_span(operation: &str, table_name: &str, record_count: Option<u64>) -> Span {
    let span = info_span!(
        "storage_operation",
        otel.name = format!("db.{}", operation),
        otel.kind = "client",
        db.operation = operation,
        db.sql.table = table_name,
        db.system = "orcher_storage",
    );

    if let Some(count) = record_count {
        span.record("db.record_count", count);
    }

    span
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing_subscriber::fmt::TestWriter;

    fn init_test_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_writer(TestWriter::default())
            .with_max_level(tracing::Level::TRACE)
            .try_init();
    }

    #[test]
    fn test_create_task_span() {
        init_test_subscriber();
        let task_id = TaskId::new_v4();
        let span = create_task_span("execute", &task_id, Some("test_task"));

        // Span should be created successfully
        assert!(span.metadata().is_some());
    }

    #[test]
    fn test_create_workflow_span() {
        init_test_subscriber();
        let workflow_id = WorkflowId::new_v4();
        let span = create_workflow_span("step", &workflow_id, Some("validation"));

        // Span should be created successfully
        assert!(span.metadata().is_some());
    }

    #[test]
    fn test_create_worker_span() {
        init_test_subscriber();
        let worker_id = WorkerId::new_v4();
        let node_id = NodeId::new_v4();
        let span = create_worker_span("process", &worker_id, Some(&node_id));

        // Span should be created successfully
        assert!(span.metadata().is_some());
    }

    #[test]
    fn test_create_storage_span() {
        init_test_subscriber();
        let span = create_storage_span("query", "tasks", Some(50));

        // Span should be created successfully
        assert!(span.metadata().is_some());
    }

    #[test]
    fn test_spans_with_none_optional_params() {
        init_test_subscriber();
        let task_id = TaskId::new_v4();
        let workflow_id = WorkflowId::new_v4();
        let worker_id = WorkerId::new_v4();

        // All spans should work with None for optional parameters
        let task_span = create_task_span("execute", &task_id, None);
        let workflow_span = create_workflow_span("start", &workflow_id, None);
        let worker_span = create_worker_span("idle", &worker_id, None);
        let storage_span = create_storage_span("read", "config", None);

        assert!(task_span.metadata().is_some());
        assert!(workflow_span.metadata().is_some());
        assert!(worker_span.metadata().is_some());
        assert!(storage_span.metadata().is_some());
    }
}
