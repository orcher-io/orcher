//! Timer execution service.

use crate::engine::dispatch::PollingManager;
use crate::engine::queues::TimerTask;
use crate::workflows::JournalService;
use orcher_common::{error::OrcherError, OrcherResult};
use orcher_storage::repositories::domains::workflow_execution::WorkflowExecutionRepository;
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Service responsible for executing timer tasks.
///
/// When a timer fires, this service:
/// 1. Appends a `TimerFired` journal event so the workflow can replay past the sleep step
/// 2. Releases the workflow claim so a poller can pick it up
/// 3. Notifies waiting pollers via the `PollingManager`
pub struct TimerExecutionService {
    workflow_execution_repository: Arc<WorkflowExecutionRepository>,
    journal_service: Arc<JournalService>,
    polling_manager: Arc<PollingManager>,
}

impl TimerExecutionService {
    /// Create a new timer execution service.
    pub fn new(
        workflow_execution_repository: Arc<WorkflowExecutionRepository>,
        journal_service: Arc<JournalService>,
        polling_manager: Arc<PollingManager>,
    ) -> Self {
        Self {
            workflow_execution_repository,
            journal_service,
            polling_manager,
        }
    }

    /// Fire a timer and resume the waiting workflow.
    ///
    /// Appends a `TimerFired` journal event, releases the workflow claim, and
    /// notifies pollers so the workflow continues from after the sleep step.
    pub async fn fire_timer(&self, task: TimerTask) -> OrcherResult<()> {
        debug!(
            task_id = %task.task_id,
            workflow_id = %task.workflow_id,
            fire_at = ?task.fire_at,
            "Firing timer"
        );

        // Extract the timer name recorded when the timer was scheduled
        let timer_name = task
            .payload
            .get("timer_name")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();

        // Fetch workflow execution to get execution_id, namespace, task_queue
        let execution = self
            .workflow_execution_repository
            .find_by_id(task.workflow_id)
            .await?
            .ok_or_else(|| {
                OrcherError::not_found("WorkflowExecution", task.workflow_id.to_string())
            })?;

        // Skip a run that has ended, or failed and is waiting for a retry: there
        // is nothing for the timer to resume. The list here used to be its own,
        // and it missed timed-out, reset and restarted runs, so a timer outliving
        // one of those wrote TimerFired into a finished journal.
        if execution.status == "failed" || execution.has_ended() {
            debug!(
                task_id = %task.task_id,
                workflow_id = %task.workflow_id,
                status = %execution.status,
                "Workflow already terminal, skipping timer fire"
            );
            return Ok(());
        }

        let workflow_id_str = task.workflow_id.to_string();

        // Append TimerFired journal event so replay skips past the sleep step
        self.journal_service
            .append_timer_fired(
                &workflow_id_str,
                &execution.execution_id,
                &timer_name,
                0, // started_event_id is not tracked; replay matches by timer_name
            )
            .await?;

        // Release the workflow claim so a poller can pick it up
        if let Err(e) = self
            .workflow_execution_repository
            .release_claim_to_resume(&execution.execution_id)
            .await
        {
            warn!(
                task_id = %task.task_id,
                workflow_id = %task.workflow_id,
                error = %e,
                "Failed to release workflow claim after timer fire (workflow may still be claimed)"
            );
        }

        // Notify waiting pollers that this task queue has work
        self.polling_manager
            .notify_work_available(&execution.task_queue, &execution.namespace)
            .await;

        info!(
            task_id = %task.task_id,
            workflow_id = %task.workflow_id,
            timer_name = %timer_name,
            execution_id = %execution.execution_id,
            "Timer fired — workflow requeued for continuation"
        );

        Ok(())
    }

    /// Execute a timer task (entry point from `TimerProcessor`).
    pub async fn execute_task(&self, task: TimerTask) -> OrcherResult<()> {
        debug!(
            task_id = %task.task_id,
            task_type = ?task.task_type,
            workflow_id = %task.workflow_id,
            "Processing timer task"
        );

        self.fire_timer(task).await
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_timer_execution_service_is_send_sync() {
        fn _is_send<T: Send>() {}
        fn _is_sync<T: Sync>() {}
        _is_send::<super::TimerExecutionService>();
        _is_sync::<super::TimerExecutionService>();
    }
}
