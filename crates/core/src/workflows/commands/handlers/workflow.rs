//! Workflow command handlers.

use orcher_common::types::time::Timestamptz;
use orcher_common::{error::OrcherError, OrcherResult};
use orcher_events::bus::EventBus;
use orcher_events::domain::ExecutionEvent;
use orcher_proto::orcher::v1::Command;
use orcher_storage::repositories::domains::workflow_execution::logs::{
    WorkflowExecutionLog, WorkflowExecutionLogsRepository,
};
use orcher_storage::repositories::domains::workflow_execution::WorkflowExecutionRepository;
use orcher_storage::repositories::{ExecutionLogRepository, ExecutionLogStatus};
use orcher_storage::schema::types::WorkflowExecution;
use std::sync::Arc;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::workflows::{ChildWorkflowManager, JournalService};

/// Handle CompleteWorkflow command
///
/// Updates workflow execution status to COMPLETED and appends journal entry.
pub async fn handle_complete_workflow(
    journal_service: &Arc<JournalService>,
    event_bus: &Arc<EventBus>,
    workflow_execution_repository: &Arc<WorkflowExecutionRepository>,
    execution_log_repository: &Arc<dyn ExecutionLogRepository>,
    _logs_repository: &Arc<WorkflowExecutionLogsRepository>,
    workflow_id: &str,
    execution_id: &str,
    command: &Command,
) -> OrcherResult<()> {
    debug!(
        workflow_id = %workflow_id,
        execution_id = %execution_id,
        "Handling CompleteWorkflow command"
    );

    // Extract command attributes
    let attrs = command
        .attributes
        .as_ref()
        .and_then(|a| match a {
            orcher_proto::orcher::v1::command::Attributes::CompleteWorkflow(attrs) => Some(attrs),
            _ => None,
        })
        .ok_or_else(|| OrcherError::invalid_input("Missing CompleteWorkflow attributes"))?;

    // Step counts are derived from the execution_log (read-only compute). A
    // failure here is non-fatal — default to zeros and still finalize the row.
    let (total_steps, completed_steps, failed_steps, retried_steps) =
        compute_step_counts_from_execution_log(execution_log_repository, workflow_id, execution_id)
            .await
            .unwrap_or_else(|e| {
                warn!(
                    workflow_id = %workflow_id,
                    execution_id = %execution_id,
                    error = %e,
                    "Failed to compute step counts (non-fatal); defaulting to zeros"
                );
                (0, 0, 0, 0)
            });

    // Parse the workflow result payload (absent => leave existing result untouched).
    //
    // A result that is not JSON used to become NULL: the workflow completed
    // and its result was gone, and a caller decoding into an optional type
    // could not tell it apart from a workflow that returned nothing. Failing
    // the workflow instead would be worse — a retry re-runs every side effect
    // and fails the same way. It is kept, losslessly, in the engine's form
    // for binary payloads, and said loudly.
    let result_json = if attrs.result.is_empty() {
        None
    } else {
        match serde_json::from_slice(&attrs.result) {
            Ok(value) => Some(value),
            Err(e) => {
                error!(
                    workflow_id = %workflow_id,
                    execution_id = %execution_id,
                    result_bytes = attrs.result.len(),
                    error = %e,
                    "Workflow result is not JSON; storing it base64-encoded as binary_data"
                );
                Some(serde_json::json!({
                    "binary_data": base64::Engine::encode(
                        &base64::engine::general_purpose::STANDARD,
                        &attrs.result,
                    ),
                    "length": attrs.result.len(),
                }))
            }
        }
    };

    // Finalize the execution in a single atomic UPDATE: status + result +
    // completed_at + step counts + duration. Replaces the former 4-UPDATE +
    // 2-SELECT sequence (see the dispatch scaling note on DB-ops-per-transition)
    // and closes the status=completed-but-result=NULL race by construction.
    let recorded = workflow_execution_repository
        .complete_workflow_atomic(
            workflow_id,
            result_json.clone(),
            Timestamptz::now(),
            total_steps,
            completed_steps,
            failed_steps,
            retried_steps,
        )
        .await?;
    if !recorded {
        // Already ended — completed by an earlier copy of this completion, or
        // cancelled, terminated or failed meanwhile. It keeps that ending, and
        // the journal and event stream are not told it ended a second time.
        info!(
            workflow_id = %workflow_id,
            execution_id = %execution_id,
            "Workflow has already ended; its completion is not recorded"
        );
        return Ok(());
    }

    // Append WorkflowExecutionCompleted journal entry
    journal_service
        .append_workflow_completed(
            workflow_id,
            execution_id,
            serde_json::json!({
                "result_size": attrs.result.len(),
            }),
        )
        .await?;

    info!(
        workflow_id = %workflow_id,
        execution_id = %execution_id,
        "Workflow completed and status updated in database"
    );

    // Publish ExecutionEvent::Completed
    if let Err(e) = event_bus
        .publish_execution_event(ExecutionEvent::Completed {
            execution_id: execution_id.to_string(),
            completed_at: Timestamptz::now(),
            result: result_json,
        })
        .await
    {
        warn!(
            error = %e,
            execution_id = %execution_id,
            "Failed to publish execution completed event"
        );
    }

    Ok(())
}

/// Handle FailWorkflow command
///
/// Updates workflow execution status to FAILED and appends journal entry.
pub async fn handle_fail_workflow(
    journal_service: &Arc<JournalService>,
    event_bus: &Arc<EventBus>,
    workflow_execution_repository: &Arc<WorkflowExecutionRepository>,
    logs_repository: &Arc<WorkflowExecutionLogsRepository>,
    workflow_id: &str,
    execution_id: &str,
    command: &Command,
) -> OrcherResult<()> {
    debug!(
        workflow_id = %workflow_id,
        execution_id = %execution_id,
        "Handling FailWorkflow command"
    );

    // Extract command attributes
    let attrs = command
        .attributes
        .as_ref()
        .and_then(|a| match a {
            orcher_proto::orcher::v1::command::Attributes::FailWorkflow(attrs) => Some(attrs),
            _ => None,
        })
        .ok_or_else(|| OrcherError::invalid_input("Missing FailWorkflow attributes"))?;

    let failure_message = attrs
        .failure
        .as_ref()
        .map(|f| f.message.clone())
        .unwrap_or_default();

    // Manually construct failure JSON since proto Failure doesn't implement Serialize
    let failure_json = if let Some(ref failure) = attrs.failure {
        serde_json::json!({
            "message": failure.message,
            "source": failure.source,
            "stack_trace": failure.stack_trace,
            "failure_type": failure.failure_type,
            "details": failure.details,
            // Read back by the retry scheduler, so it decides as the failure
            // path did.
            "non_retryable": failure.non_retryable,
        })
    } else {
        serde_json::json!({
            "message": failure_message,
        })
    };

    // Status and error in one write. Only the status used to be recorded, so
    // the row said "failed" and not why — and the row is what a waiting parent
    // is told from, so it heard "Child workflow failed" and nothing more.
    let recorded = workflow_execution_repository
        .fail_workflow_atomic(
            workflow_id,
            failure_message.clone(),
            Some(failure_json.clone()),
            Timestamptz::now(),
        )
        .await?;
    if !recorded {
        // The run ended while this activation was running — cancelled,
        // terminated, timed out — and keeps that ending. Recording the
        // failure anyway would make it retryable again.
        info!(
            workflow_id = %workflow_id,
            execution_id = %execution_id,
            "Workflow has already ended; its failure is not recorded"
        );
        return Ok(());
    }

    // Append WorkflowExecutionFailed journal entry

    // Append WorkflowExecutionFailed journal entry
    journal_service
        .append_workflow_failed(
            workflow_id,
            execution_id,
            failure_message.clone(),
            Some(failure_json.clone()),
        )
        .await?;

    info!(
        workflow_id = %workflow_id,
        execution_id = %execution_id,
        failure_message = %failure_message,
        "Workflow failed and status updated in database"
    );

    // Write user-facing workflow failure log
    let failure_log = WorkflowExecutionLog::error(
        execution_id.to_string(),
        format!("Workflow failed: {}", failure_message),
    )
    .with_source("CommandProcessor".to_string())
    .with_metadata(failure_json.clone());

    if let Err(e) = logs_repository.add_log(&failure_log).await {
        warn!(
            execution_id = %execution_id,
            error = %e,
            "Failed to write workflow failure log"
        );
    }

    // Publish ExecutionEvent::Failed
    if let Err(e) = event_bus
        .publish_execution_event(ExecutionEvent::Failed {
            execution_id: execution_id.to_string(),
            error: failure_message.clone(),
            failed_at: Timestamptz::now(),
        })
        .await
    {
        warn!(
            error = %e,
            execution_id = %execution_id,
            "Failed to publish execution failed event"
        );
    }

    Ok(())
}

/// Handle CancelWorkflow command
///
/// Records that the workflow ended itself as cancelled, unless it has already
/// ended, and appends the journal entry.
pub async fn handle_cancel_workflow(
    journal_service: &Arc<JournalService>,
    event_bus: &Arc<EventBus>,
    workflow_execution_repository: &Arc<WorkflowExecutionRepository>,
    workflow_id: &str,
    execution_id: &str,
    command: &Command,
) -> OrcherResult<()> {
    debug!(
        workflow_id = %workflow_id,
        execution_id = %execution_id,
        "Handling CancelWorkflow command"
    );

    // Extract cancellation details
    let details = command
        .attributes
        .as_ref()
        .and_then(|a| match a {
            orcher_proto::orcher::v1::command::Attributes::CancelWorkflow(attrs) => Some(attrs),
            _ => None,
        })
        .map(|attrs| String::from_utf8(attrs.details.clone()).unwrap_or_default())
        .unwrap_or_default();

    let reason = if details.is_empty() {
        "Workflow cancelled by SDK".to_string()
    } else {
        details
    };

    let recorded = workflow_execution_repository
        .cancel_workflow_atomic(workflow_id, Timestamptz::now())
        .await?;
    if !recorded {
        // Already ended: completed, failed for good, terminated or cancelled
        // meanwhile. It keeps that ending, and the journal and event stream are
        // not told it ended a second time.
        info!(
            workflow_id = %workflow_id,
            execution_id = %execution_id,
            "Workflow has already ended; its cancellation is not recorded"
        );
        return Ok(());
    }

    // Append WorkflowExecutionCanceled journal entry
    journal_service
        .append_workflow_canceled(workflow_id, execution_id, reason.clone())
        .await?;

    info!(
        workflow_id = %workflow_id,
        execution_id = %execution_id,
        reason = %reason,
        "Workflow cancelled and status updated in database"
    );

    // Publish ExecutionEvent::Cancelled
    if let Err(e) = event_bus
        .publish_execution_event(ExecutionEvent::Cancelled {
            execution_id: execution_id.to_string(),
            reason: reason.clone(),
            cancelled_at: Timestamptz::now(),
        })
        .await
    {
        warn!(
            error = %e,
            execution_id = %execution_id,
            "Failed to publish execution cancelled event"
        );
    }

    Ok(())
}

/// Handle RestartFresh command
///
/// Marks the current execution as "restarted_fresh" and creates a new execution
/// with the same workflow_id but a fresh execution_id and state.
pub async fn handle_restart_fresh(
    journal_service: &Arc<JournalService>,
    workflow_execution_repository: &Arc<WorkflowExecutionRepository>,
    child_workflow_manager: &Arc<ChildWorkflowManager>,
    workflow_id: &str,
    execution_id: &str,
    command: &Command,
) -> OrcherResult<()> {
    debug!(
        workflow_id = %workflow_id,
        execution_id = %execution_id,
        "Handling RestartFresh command"
    );

    // Extract RestartFresh attributes
    let attrs = command
        .attributes
        .as_ref()
        .and_then(|a| match a {
            orcher_proto::orcher::v1::command::Attributes::RestartFresh(attrs) => Some(attrs),
            _ => None,
        })
        .ok_or_else(|| OrcherError::invalid_input("Missing RestartFresh attributes"))?;

    // Look up the current execution to inherit properties
    let current_execution = workflow_execution_repository
        .find_by_execution_id(execution_id)
        .await?
        .ok_or_else(|| OrcherError::not_found("workflow_execution", execution_id.to_string()))?;

    // The execution timeout covers the workflow across all its runs: past it,
    // no run of the workflow starts. This run is left as it is — live, past
    // its execution deadline — and the timeout watcher ends it timed out, as
    // it ends every run past that deadline, never to be retried.
    if current_execution.execution_expired_by(Timestamptz::now()) {
        warn!(
            workflow_id = %workflow_id,
            execution_id = %execution_id,
            "Not restarting the workflow fresh: its execution timeout has run out"
        );
        return Ok(());
    }

    // Determine properties for the new execution (use attrs if provided, else inherit)
    let workflow_type = if attrs.workflow_type.is_empty() {
        current_execution.workflow_name.clone()
    } else {
        attrs.workflow_type.clone()
    };

    let task_queue = if attrs.task_queue.is_empty() {
        current_execution.task_queue.clone()
    } else {
        attrs.task_queue.clone()
    };

    let input: serde_json::Value = if attrs.input.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&attrs.input).unwrap_or(serde_json::Value::Null)
    };

    // Mark the current execution "restarted_fresh" — a distinct terminal state
    // (added to workflow_exec_status_check in V026; the value and its
    // RestartedFresh proto mapping already existed). It produced no result of its
    // own: the logical workflow continues in the new execution below (linked by
    // workflow_id + parent_execution_id), so the result path recognises the chain
    // and follows it to the final execution's result rather than returning this
    // row's empty terminus.
    workflow_execution_repository
        .update_status_by_workflow_id(
            workflow_id,
            "restarted_fresh",
            None,
            Some(Timestamptz::now()),
        )
        .await?;

    // Create new execution with same workflow_id
    let new_execution_id = Uuid::new_v4().to_string();

    let mut new_execution = WorkflowExecution::builder()
        .execution_id(new_execution_id.clone())
        .workflow_name(workflow_type.clone())
        .workflow_version(current_execution.workflow_version.clone())
        .namespace(current_execution.namespace.clone())
        .parameters(input)
        // A restart-fresh execution is triggered by workflow
        // code, so the trigger_type must be "workflow" — one of the
        // workflow_exec_trigger_check set. "restart_fresh" is not in that set
        // and violated the constraint, so the new execution row could never be
        // created (the same defect fixed for child workflows).
        .trigger_type("workflow".to_string())
        .build();

    // Set workflow_id to maintain the logical workflow identity
    if let Some(ref wf_id) = current_execution.workflow_id {
        new_execution.workflow_id = Some(wf_id.clone());
    }
    new_execution.status = "running".to_string();
    new_execution.started_at = Some(Timestamptz::now());
    new_execution.root_execution_id = current_execution.root_execution_id.clone();
    new_execution.parent_execution_id = Some(execution_id.to_string());
    new_execution.task_queue = task_queue.clone();
    // The new run is the same workflow going on, under the retry policy the
    // command names or else this run's. It is a fresh run, so its retries are
    // its own: the count starts again. Built without these, the new run had
    // the builder's default cap and no policy, so a workflow started with no
    // retries was retried once it had continued as new.
    match attrs.retry_policy.as_ref() {
        Some(policy) => {
            let (max_retries, retry_policy) =
                crate::workflows::retry::convert_retry_policy(Some(policy))?;
            new_execution.max_retries = max_retries.unwrap_or(0);
            new_execution.retry_policy = retry_policy;
        }
        None => {
            new_execution.max_retries = current_execution.max_retries;
            new_execution.retry_policy = current_execution.retry_policy.clone();
        }
    }
    new_execution.retry_count = 0;
    // The new run carries the labels and annotations the command names, or
    // else this run's: the workflow's metadata goes on with it. Built
    // without them, a workflow lost both once it had continued as new.
    new_execution.labels = crate::workflows::execution::string_map(&attrs.labels)
        .unwrap_or_else(|| current_execution.labels.clone());
    new_execution.annotations = crate::workflows::execution::string_map(&attrs.annotations)
        .unwrap_or_else(|| current_execution.annotations.clone());
    // The new run is held to the run and task timeouts the command names, or
    // else to this run's; either way its run clock starts with it.
    new_execution.run_timeout_secs =
        crate::workflows::execution::limit_secs(attrs.run_timeout.as_ref())
            .or(current_execution.run_timeout_secs);
    new_execution.task_timeout_secs =
        crate::workflows::execution::limit_secs(attrs.task_timeout.as_ref())
            .or(current_execution.task_timeout_secs);
    // The execution timeout is the workflow's, fixed when it started and not
    // restarted with each run: the new run carries the deadline this one was
    // held to. A command cannot move it, so its execution_timeout is not read.
    new_execution.timeout_secs = current_execution.timeout_secs;
    new_execution.execution_deadline = current_execution.execution_expires_at();
    // The new run is the same workflow going on: it belongs to the same
    // organization, and takes over the workflow id's hold — this one was
    // marked restarted_fresh above and holds nothing.
    new_execution.organization_id = current_execution.organization_id;
    new_execution.holds_workflow_id = current_execution.holds_workflow_id;
    if let (true, Some(wf_id)) = (
        current_execution.holds_workflow_id,
        &current_execution.workflow_id,
    ) {
        new_execution.id = orcher_common::types::identifiers::ExecutionId::from_uuid(
            workflow_execution_repository.id_beside_workflow_id(
                current_execution.organization_id,
                &current_execution.namespace,
                wf_id,
                |_| Uuid::new_v4(),
            ),
        );
    }

    // Create new execution with outbox task for dispatch
    let outbox_payload = serde_json::json!({
        "execution_id": new_execution.execution_id,
        "workflow_name": new_execution.workflow_name,
        "workflow_version": new_execution.workflow_version,
        "namespace": new_execution.namespace,
        "task_queue": task_queue,
        "parameters": new_execution.parameters,
    });

    let idempotency_key = format!("restart_fresh_{}", new_execution.execution_id);

    workflow_execution_repository
        .create_with_outbox_task(&new_execution, outbox_payload, idempotency_key, &task_queue)
        .await?;

    // If this run is somebody's child, the child goes on in the new run: the
    // parent's outcome is the new run's, not this one's.
    child_workflow_manager
        .successor_started(execution_id, &new_execution_id)
        .await;

    // Append journal entry linking old execution to new
    let restart_attrs = serde_json::json!({
        "workflow_type": workflow_type,
        "task_queue": task_queue,
    });

    journal_service
        .append_workflow_restarted_fresh(
            workflow_id,
            execution_id,
            &new_execution_id,
            restart_attrs,
        )
        .await?;

    info!(
        workflow_id = %workflow_id,
        old_execution_id = %execution_id,
        new_execution_id = %new_execution_id,
        workflow_type = %workflow_type,
        task_queue = %task_queue,
        "Workflow restarted fresh with new execution"
    );

    Ok(())
}

// ================================================================================================
// HELPER FUNCTIONS
// ================================================================================================

/// Compute step counts from the execution_log table (read-only).
/// Returns `(total, completed, failed, retried)`; the caller persists them as
/// part of the single atomic completion UPDATE.
async fn compute_step_counts_from_execution_log(
    execution_log_repository: &Arc<dyn ExecutionLogRepository>,
    workflow_id: &str,
    execution_id: &str,
) -> OrcherResult<(i32, i32, i32, i32)> {
    debug!(
        workflow_id = %workflow_id,
        execution_id = %execution_id,
        "Calculating step counts from execution_log"
    );

    // Parse workflow_id as UUID
    let workflow_uuid = Uuid::parse_str(workflow_id)
        .map_err(|e| OrcherError::invalid_input(format!("Invalid workflow_id UUID: {}", e)))?;

    // Get all entries for this workflow
    let entries = execution_log_repository.list_entries(workflow_uuid).await?;

    let total_steps = entries.len() as i32;
    let completed_steps = entries
        .iter()
        .filter(|e| matches!(e.status, ExecutionLogStatus::Completed))
        .count() as i32;
    let failed_steps = entries
        .iter()
        .filter(|e| matches!(e.status, ExecutionLogStatus::Failed))
        .count() as i32;
    let retried_steps = entries.iter().filter(|e| e.retry_count > 0).count() as i32;

    debug!(
        workflow_id = %workflow_id,
        execution_id = %execution_id,
        total_steps = %total_steps,
        completed_steps = %completed_steps,
        failed_steps = %failed_steps,
        retried_steps = %retried_steps,
        "Calculated step counts from execution_log"
    );

    info!(
        workflow_id = %workflow_id,
        execution_id = %execution_id,
        total_steps = %total_steps,
        completed_steps = %completed_steps,
        "Computed step counts from execution_log"
    );

    Ok((total_steps, completed_steps, failed_steps, retried_steps))
}
