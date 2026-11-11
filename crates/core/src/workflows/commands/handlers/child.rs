//! Child workflow command handlers.

use orcher_common::{error::OrcherError, OrcherResult};
use orcher_proto::orcher::v1::{Command, StartChildWorkflowCommandAttributes};
use orcher_storage::schema::types::child_workflow::OrphanPolicy;
use std::sync::Arc;
use tracing::{debug, info};

use crate::engine::dispatch::PollingManager;
use crate::workflows::retry::convert_retry_policy;
use crate::workflows::{ChildRunSettings, ChildWorkflowManager, RunTimeouts};

/// Handle StartChildWorkflow command
///
/// Routes to ChildWorkflowManager to create child workflow execution.
pub async fn handle_start_child_workflow(
    child_workflow_manager: &Arc<ChildWorkflowManager>,
    polling_manager: &Option<Arc<PollingManager>>,
    workflow_id: &str,
    execution_id: &str,
    parent_namespace: &str,
    command: &Command,
) -> OrcherResult<()> {
    debug!(
        workflow_id = %workflow_id,
        execution_id = %execution_id,
        "Handling StartChildWorkflow command"
    );

    // Extract command attributes
    let attrs = command
        .attributes
        .as_ref()
        .and_then(|a| match a {
            orcher_proto::orcher::v1::command::Attributes::StartChildWorkflow(attrs) => Some(attrs),
            _ => None,
        })
        .ok_or_else(|| OrcherError::invalid_input("Missing StartChildWorkflow attributes"))?;

    // Destructured whole, so a field added to the command is a compile error
    // here until it is either carried to the child or stated as not carried.
    let StartChildWorkflowCommandAttributes {
        workflow_id: child_workflow_id,
        workflow_type,
        task_queue,
        input,
        execution_timeout,
        run_timeout,
        task_timeout,
        orphan_policy,
        retry_policy,
        namespace,
        annotations,
        labels,
    } = attrs;

    // Convert parent close policy from proto enum to ORCHER type
    let orphan_policy = match orphan_policy {
        1 => OrphanPolicy::Terminate, // ORPHAN_POLICY_TERMINATE
        2 => OrphanPolicy::Cancel,    // ORPHAN_POLICY_CANCEL
        3 => OrphanPolicy::Abandon,   // ORPHAN_POLICY_ABANDON
        _ => OrphanPolicy::Abandon,   // Default/Unspecified
    };

    // Input that is not JSON is refused, as it is for a top-level start. It
    // used to be replaced with `{}`, so the child ran on an empty object in
    // place of what the parent sent it. Refusing it as invalid input fails the
    // parent with the reason (see CommandProcessor), which replaying cannot
    // change.
    let input = if input.is_empty() {
        serde_json::json!({})
    } else {
        serde_json::from_slice(input).map_err(|e| {
            OrcherError::invalid_input(format!(
                "child workflow '{}' ({}): input is not valid JSON: {}",
                child_workflow_id, workflow_type, e
            ))
        })?
    };

    let namespace = child_namespace(namespace, parent_namespace);

    // The child's retry policy, converted exactly as a top-level start's is.
    // It used to be ignored, so every child ran with no retries whatever its
    // parent asked for.
    let (max_retries, retry_policy) = convert_retry_policy(retry_policy.as_ref())?;
    let labels = if labels.is_empty() {
        None
    } else {
        Some(serde_json::to_value(labels).unwrap_or_default())
    };

    // Route to ChildWorkflowManager
    child_workflow_manager
        .handle_start_child_workflow(
            workflow_id,
            execution_id,
            child_workflow_id,
            workflow_type,
            namespace,
            task_queue,
            input,
            orphan_policy,
            ChildRunSettings {
                max_retries,
                retry_policy,
                labels,
                annotations: crate::workflows::execution::string_map(annotations),
                // The child's own limits, read as a top-level start's are.
                // They used to be dropped, so a child ran with none.
                timeouts: RunTimeouts::from_durations(
                    execution_timeout.as_ref(),
                    run_timeout.as_ref(),
                    task_timeout.as_ref(),
                ),
            },
        )
        .await?;

    info!(
        workflow_id = %workflow_id,
        execution_id = %execution_id,
        child_workflow_id = %attrs.workflow_id,
        child_workflow_type = %attrs.workflow_type,
        "StartChildWorkflow command processed"
    );

    // Notify waiting services that new work is available on the child's task queue
    if let Some(ref polling_manager) = polling_manager {
        polling_manager
            .notify_work_available(&attrs.task_queue, namespace)
            .await;

        debug!(
            child_workflow_id = %attrs.workflow_id,
            task_queue = %attrs.task_queue,
            namespace = %namespace,
            "Notified waiting services of child workflow"
        );
    }

    Ok(())
}

/// Handle CancelChildWorkflow command
///
/// Cancels the child's current run the way an API cancellation does. The
/// parent learns of it like any other ending: the child reconciliation sweep
/// sees the cancelled run and delivers it to the parent's journal.
pub async fn handle_cancel_child_workflow(
    child_workflow_manager: &Arc<ChildWorkflowManager>,
    workflow_id: &str,
    execution_id: &str,
    command: &Command,
) -> OrcherResult<()> {
    debug!(
        workflow_id = %workflow_id,
        execution_id = %execution_id,
        "Handling CancelChildWorkflow command"
    );

    let attrs = command
        .attributes
        .as_ref()
        .and_then(|a| match a {
            orcher_proto::orcher::v1::command::Attributes::CancelChildWorkflow(attrs) => {
                Some(attrs)
            }
            _ => None,
        })
        .ok_or_else(|| OrcherError::invalid_input("Missing CancelChildWorkflow attributes"))?;

    // The child is identified by the id its parent gave it; the SDKs do not
    // know the child's run id, and the run changes when the child is retried.
    let cancelled = child_workflow_manager
        .request_cancel_child(execution_id, &attrs.workflow_id)
        .await?;

    info!(
        workflow_id = %workflow_id,
        execution_id = %execution_id,
        child_workflow_id = %attrs.workflow_id,
        cancelled,
        "Child workflow cancellation processed"
    );

    Ok(())
}

/// Handle QueryChildWorkflow command
///
/// Queries the current state of a child workflow execution.
/// Returns the child's status and result if completed.
pub async fn handle_query_child_workflow(
    child_workflow_manager: &Arc<ChildWorkflowManager>,
    workflow_id: &str,
    execution_id: &str,
    command: &Command,
) -> OrcherResult<()> {
    debug!(
        workflow_id = %workflow_id,
        execution_id = %execution_id,
        "Handling QueryChildWorkflow command"
    );

    let attrs = command
        .attributes
        .as_ref()
        .and_then(|a| match a {
            orcher_proto::orcher::v1::command::Attributes::QueryChildWorkflow(attrs) => Some(attrs),
            _ => None,
        })
        .ok_or_else(|| OrcherError::invalid_input("Missing QueryChildWorkflow attributes"))?;

    // Look up child workflow state
    let child = child_workflow_manager
        .get_child(&attrs.workflow_id, &attrs.run_id)
        .await?;

    match child {
        Some(child_record) => {
            info!(
                workflow_id = %workflow_id,
                execution_id = %execution_id,
                child_workflow_id = %attrs.workflow_id,
                child_state = ?child_record.state,
                query_type = %attrs.query_type,
                "Child workflow query completed"
            );
        }
        None => {
            info!(
                workflow_id = %workflow_id,
                execution_id = %execution_id,
                child_workflow_id = %attrs.workflow_id,
                query_type = %attrs.query_type,
                "Child workflow not found for query"
            );
        }
    }

    Ok(())
}

/// Where a child belongs: the namespace it asked for, or its parent's.
///
/// No SDK fills this field in, so the old fallback — a namespace literally
/// named "default" — sent every child of every tenant to the same place. A poll
/// claims by namespace, so no worker in the parent's namespace could see the
/// child: it sat runnable and unclaimed for good, while the parent waited on a
/// result that could not arrive. Nothing failed; the work simply never ran.
///
/// The contract suites did not catch it because they run in `default`, where
/// inheriting and defaulting produce the same string.
fn child_namespace<'a>(requested: &'a str, parent: &'a str) -> &'a str {
    if requested.is_empty() {
        parent
    } else {
        requested
    }
}

#[cfg(test)]
mod tests {
    use super::child_namespace;

    /// The case that stranded every child of a real tenant.
    #[test]
    fn a_child_without_a_namespace_belongs_to_its_parent() {
        assert_eq!(child_namespace("", "payments-staging"), "payments-staging");
    }

    /// A caller that names a namespace means it — cross-namespace children stay
    /// possible, which is the only reason the field exists.
    #[test]
    fn a_named_namespace_is_honoured() {
        assert_eq!(
            child_namespace("reporting", "payments-staging"),
            "reporting"
        );
    }
}
