//! Ending the queued tasks a workflow's cancellation interrupted.
//!
//! A task the workflow scheduled before it was asked to cancel, and that no
//! worker had started yet, is not started afterwards: the start refuses it.
//! It is ended here instead, as cancelled before it started, and the workflow
//! is told the way it is told of a terminal failure — the cancelled step its
//! replay reads as a failure of type `Cancelled`, and a TaskFailed entry — so
//! a workflow waiting on it is not left waiting. It is never retried.
//!
//! Three paths end such tasks, all through [`cancel_queued_tasks`]: the
//! cancellation request itself, a poll that was refused one, and the timeout
//! watcher's sweep, which takes whatever the other two missed.

use std::sync::Arc;

use orcher_common::error::{OrcherError, OrcherResult};
use orcher_common::types::time::Timestamptz;
use orcher_storage::repositories::domains::journal::JournalRepository;
use orcher_storage::repositories::domains::task_execution::TaskExecutionRepository;
use orcher_storage::repositories::ExecutionLogRepository;
use orcher_storage::schema::types::execution::{TaskExecution, CANCELLED_FAILURE_TYPE};
use sqlx::PgPool;
use tracing::info;
use uuid::Uuid;

use super::task_failure;

/// What the workflow is told of a task ended before it started.
const MESSAGE: &str = "Task cancelled before it started: its workflow was cancelled";

/// How many of a workflow's queued tasks are read at a time.
const BATCH: i64 = 100;

/// End the queued tasks of the workflow with row id `workflow_id` that its
/// cancellation stops, telling the workflow of each. Returns how many were
/// ended here. The caller wakes the workflow when any were.
pub async fn cancel_queued_tasks(
    tasks: &TaskExecutionRepository,
    execution_log: &dyn ExecutionLogRepository,
    workflow_id: Uuid,
) -> OrcherResult<usize> {
    let pool = tasks.pool_for_workflow(workflow_id);
    let mut ended = 0;
    loop {
        let queued = TaskExecutionRepository::scheduled_stopped_by_cancellation_on(
            pool,
            Some(workflow_id),
            BATCH,
        )
        .await?;
        let mut ended_now = 0;
        for task in &queued {
            if cancel_task(pool, execution_log, task).await? {
                ended_now += 1;
            }
        }
        ended += ended_now;
        // A batch in which nothing could be ended is left to the sweep rather
        // than read again.
        if ended_now == 0 || (queued.len() as i64) < BATCH {
            return Ok(ended);
        }
    }
}

/// End `task`, a scheduled task its workflow's cancellation stops, as
/// cancelled before it started. `workflow_pool` is the pool of the
/// workflow's shard. Returns whether this call ended it; `false` when it had
/// already left `scheduled`.
///
/// Recorded as a terminal failure is, and in the same order: the cancelled
/// step first, an upsert that never replaces a completion, then one transaction
/// that ends the task and appends TaskFailed. An error leaves the task
/// scheduled, where the start still refuses it and the next sweep finds it.
pub(crate) async fn cancel_task(
    workflow_pool: &PgPool,
    execution_log: &dyn ExecutionLogRepository,
    task: &TaskExecution,
) -> OrcherResult<bool> {
    let now = Timestamptz::now();
    let details = task_failure::cancelled_details();
    let mut cancelled = task.clone();
    cancelled.error_type = Some(CANCELLED_FAILURE_TYPE.to_string());
    cancelled.error_message = Some(MESSAGE.to_string());

    let step = task_failure::cancelled_step(&cancelled, MESSAGE, now);
    execution_log
        .store_failure(&step)
        .await
        .map_err(|e| OrcherError::internal(format!("Failed to record the cancelled step: {e}")))?;

    let mut tx = workflow_pool
        .begin()
        .await
        .map_err(|e| OrcherError::internal(format!("Failed to begin: {e}")))?;
    let ended = TaskExecutionRepository::cancel_before_start_on(
        &mut tx,
        task.id,
        MESSAGE,
        CANCELLED_FAILURE_TYPE,
        &details,
    )
    .await?;
    if ended.is_none() {
        return Ok(false);
    }
    JournalRepository::append_next_on(
        &mut tx,
        task_failure::task_failed_entry(&cancelled, MESSAGE, Some(&details), now),
    )
    .await
    .map_err(|e| OrcherError::internal(format!("Failed to journal the task failure: {e}")))?;
    crate::workflows::journal::SettledStep::from(&step)
        .append_on(
            &mut tx,
            &task.workflow_id.to_string(),
            &task.execution_id.to_string(),
        )
        .await?;
    tx.commit()
        .await
        .map_err(|e| OrcherError::internal(format!("Failed to record the cancelled task: {e}")))?;
    info!(
        task_id = %task.task_id,
        attempt = task.attempt_number,
        workflow_id = %task.workflow_id,
        "Task cancelled before it started: its workflow's cancellation interrupted it"
    );
    Ok(true)
}

/// What ends a workflow's queued interrupted tasks when its cancellation is
/// requested, for a service that does not otherwise hold the repositories.
#[derive(Clone)]
pub struct QueuedTaskCancellation {
    tasks: Arc<TaskExecutionRepository>,
    execution_log: Arc<dyn ExecutionLogRepository>,
}

impl QueuedTaskCancellation {
    /// Over the engine's task and execution-log repositories.
    pub fn new(
        tasks: Arc<TaskExecutionRepository>,
        execution_log: Arc<dyn ExecutionLogRepository>,
    ) -> Self {
        Self {
            tasks,
            execution_log,
        }
    }

    /// See [`cancel_queued_tasks`].
    pub async fn cancel_queued_tasks(&self, workflow_id: Uuid) -> OrcherResult<usize> {
        cancel_queued_tasks(&self.tasks, self.execution_log.as_ref(), workflow_id).await
    }
}
