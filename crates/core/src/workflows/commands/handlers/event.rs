//! Event command handlers.

use orcher_common::{error::OrcherError, OrcherResult};
use orcher_proto::orcher::v1::Command;
use orcher_storage::repositories::domains::workflow_execution::{
    RunRef, WorkflowExecutionRepository,
};
use std::sync::Arc;
use tracing::{debug, info, warn};

use crate::engine::dispatch::PollingManager;
use crate::workflows::JournalService;

/// Handle SendEvent command
///
/// Sends an event (signal) to another workflow execution. Records the send in
/// the sender's journal and delivers `EventReceived` to the target workflow's
/// journal, then notifies the polling manager so the target is re-queued.
pub async fn handle_send_event(
    journal_service: &Arc<JournalService>,
    workflow_execution_repository: &Arc<WorkflowExecutionRepository>,
    polling_manager: Option<&Arc<PollingManager>>,
    workflow_id: &str,
    execution_id: &str,
    command: &Command,
) -> OrcherResult<()> {
    debug!(
        workflow_id = %workflow_id,
        execution_id = %execution_id,
        "Handling SendEvent command"
    );

    let attrs = command
        .attributes
        .as_ref()
        .and_then(|a| match a {
            orcher_proto::orcher::v1::command::Attributes::SendEvent(attrs) => Some(attrs),
            _ => None,
        })
        .ok_or_else(|| OrcherError::invalid_input("Missing SendEvent attributes"))?;

    // Find the target before recording anything, so a failed lookup leaves
    // nothing half-done for the retried activation to repeat.
    //
    // The target is the run the command names, as every request naming a run
    // resolves it (find_run), seen as the sender: in the sender's organization
    // and, by workflow id, its namespace. A failed lookup is an error, where it
    // read as "no such workflow" and the event was quietly not delivered.
    let sender = workflow_execution_repository
        .find_by_execution_id(execution_id)
        .await?
        .ok_or_else(|| OrcherError::not_found("sending execution", execution_id))?;
    let target = workflow_execution_repository
        .find_run(RunRef {
            organization_id: sender.organization_id,
            namespace: &sender.namespace,
            workflow_id: &attrs.workflow_id,
            execution_id: &attrs.run_id,
        })
        .await?;

    // Record the event send in the sender's journal
    journal_service
        .append_entry(
            workflow_id,
            execution_id,
            "ExternalEventSent",
            serde_json::json!({
                "target_workflow_id": attrs.workflow_id,
                "target_run_id": attrs.run_id,
                "event_name": attrs.event_name,
                "payload_size": attrs.payload.len(),
            }),
        )
        .await?;

    if let Some(target_wf) = target {
        let target_wf_id = target_wf.id.to_string();
        let target_exec_id = target_wf.execution_id.clone();

        if let Err(e) = journal_service
            .append_event_received(
                &target_wf_id,
                &target_exec_id,
                &attrs.event_name,
                &attrs.payload,
                execution_id,
            )
            .await
        {
            warn!(
                error = %e,
                target_workflow_id = %attrs.workflow_id,
                event_name = %attrs.event_name,
                "Failed to append EventReceived to target workflow journal"
            );
        } else {
            // Make the target runnable again. A workflow parked on an event holds
            // its claim (like one waiting on a task or timer), so the event has to
            // release it before the wake-up means anything — the same two steps
            // the SendEvent RPC takes. The release is best-effort: if it fails, the
            // claim-timeout reclaimer frees the target later.
            if let Some(pm) = polling_manager {
                if let Err(e) = pm.release_claim_to_resume(&target_wf.execution_id).await {
                    warn!(
                        target_workflow_id = %attrs.workflow_id,
                        target_execution_id = %target_wf.execution_id,
                        error = %e,
                        "Failed to release the target's claim after delivering an event (non-fatal)"
                    );
                }
                pm.notify_work_available(&target_wf.task_queue, &target_wf.namespace)
                    .await;
            }
            info!(
                source_workflow_id = %workflow_id,
                target_workflow_id = %attrs.workflow_id,
                event_name = %attrs.event_name,
                "EventReceived delivered to target workflow"
            );
        }
    } else {
        warn!(
            target_workflow_id = %attrs.workflow_id,
            event_name = %attrs.event_name,
            "Target workflow not found for SendEvent delivery"
        );
    }

    Ok(())
}

/// Handle RequestCancellation command
///
/// Requests cancellation of the current workflow execution. Records the
/// cancellation request in the journal for replay durability.
pub async fn handle_request_cancellation(
    journal_service: &Arc<JournalService>,
    workflow_id: &str,
    execution_id: &str,
    command: &Command,
) -> OrcherResult<()> {
    debug!(
        workflow_id = %workflow_id,
        execution_id = %execution_id,
        "Handling RequestCancellation command"
    );

    let attrs = command
        .attributes
        .as_ref()
        .and_then(|a| match a {
            orcher_proto::orcher::v1::command::Attributes::RequestCancellation(attrs) => {
                Some(attrs)
            }
            _ => None,
        })
        .ok_or_else(|| OrcherError::invalid_input("Missing RequestCancellation attributes"))?;

    // Record the cancellation request in the journal
    journal_service
        .append_entry(
            workflow_id,
            execution_id,
            "ExternalCancellationRequested",
            serde_json::json!({
                "reason": attrs.reason,
            }),
        )
        .await?;

    info!(
        workflow_id = %workflow_id,
        execution_id = %execution_id,
        reason = %attrs.reason,
        "External cancellation requested"
    );

    Ok(())
}
