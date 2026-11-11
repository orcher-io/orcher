//! The run a request names.
//!
//! Every RPC that addresses one workflow run — status, result, describe,
//! journal, logs, task executions, cancel, terminate, reset, events, queries,
//! updates — resolves it here, so they all agree on what a workflow id, a run
//! id and a namespace mean, and on who may see the run. The rules themselves
//! are [`WorkflowExecutionRepository::find_run`].

use orcher_storage::repositories::domains::workflow_execution::{
    RunRef, WorkflowExecutionRepository,
};
use orcher_storage::schema::types::execution::WorkflowExecution;
use tonic::Status;
use tracing::error;
use uuid::Uuid;

/// The namespace a request means when it names none.
pub const DEFAULT_NAMESPACE: &str = "default";

/// The namespace a request is in: the one it names, else the default.
pub fn namespace_or_default(namespace: &str) -> &str {
    if namespace.is_empty() {
        DEFAULT_NAMESPACE
    } else {
        namespace
    }
}

/// The run `(namespace, workflow_id, execution_id)` names that `caller` may
/// see, or `None`.
///
/// `None` covers a run that does not exist and one the caller may not see
/// alike; callers report both as not found. A request naming neither id is
/// invalid.
pub async fn find_named_run(
    repository: &WorkflowExecutionRepository,
    caller: Option<Uuid>,
    namespace: &str,
    workflow_id: &str,
    execution_id: &str,
) -> Result<Option<WorkflowExecution>, Status> {
    if workflow_id.is_empty() && execution_id.is_empty() {
        return Err(Status::invalid_argument(
            "Either workflow_id or execution_id is required",
        ));
    }
    repository
        .find_run(RunRef {
            organization_id: caller,
            namespace: namespace_or_default(namespace),
            workflow_id,
            execution_id,
        })
        .await
        .map_err(|e| {
            error!(error = %e, workflow_id, execution_id, "Failed to find the named run");
            Status::internal(format!("Failed to query workflow: {}", e))
        })
}
