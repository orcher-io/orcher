//! Task command handlers.

use orcher_common::types::time::Timestamptz;
use orcher_common::{error::OrcherError, OrcherResult};
use orcher_proto::orcher::v1::{command, Command, StepType};
use orcher_storage::repositories::domains::task_execution::TaskExecutionRepository;
use orcher_storage::repositories::domains::workflow_execution::logs::{
    WorkflowExecutionLog, WorkflowExecutionLogsRepository,
};
use orcher_storage::repositories::{
    ExecutionLogEntry, ExecutionLogRepository, ExecutionLogStatus, ExecutionStepType,
};
use orcher_storage::schema::types::TaskExecution;
use std::sync::Arc;
use tracing::{debug, error, info, instrument, warn};
use uuid::Uuid;

use crate::engine::dispatch::PollingManager;
use crate::engine::WorkflowCoordinator;
use crate::workflows::JournalService;

/// Convert a proto `Duration` to whole seconds for a task-timeout column,
/// rounding a positive sub-second value UP to 1s. These columns are whole
/// seconds, so a sub-second timeout (e.g. 500ms) read as `.seconds` would coerce
/// to 0 — silently meaning "no timeout". Rounding up preserves the user's intent
/// that a timeout exists; an exactly-zero (or negative) Duration stays 0.
fn duration_to_timeout_secs(d: &orcher_proto::prost_types::Duration) -> i32 {
    let total_nanos = d.seconds as i128 * 1_000_000_000 + d.nanos as i128;
    if total_nanos <= 0 {
        0
    } else {
        // Ceil to whole seconds so a positive sub-second timeout becomes 1s, not 0.
        ((total_nanos + 999_999_999) / 1_000_000_000) as i32
    }
}

/// Handle ScheduleTask command
///
/// Routes to task queue to schedule task execution.
pub async fn handle_schedule_task(
    journal_service: &Arc<JournalService>,
    task_execution_repository: &Arc<TaskExecutionRepository>,
    coordinator: &Option<Arc<WorkflowCoordinator>>,
    polling_manager: &Option<Arc<PollingManager>>,
    logs_repository: &Arc<WorkflowExecutionLogsRepository>,
    workflow_id: &str,
    execution_id: &str,
    namespace: &str,
    command: &Command,
) -> OrcherResult<()> {
    debug!(
        workflow_id = %workflow_id,
        execution_id = %execution_id,
        namespace = %namespace,
        "SCHEDULE_TASK: Entering handle_schedule_task"
    );

    // Extract command attributes
    let attrs = command
        .attributes
        .as_ref()
        .and_then(|a| match a {
            orcher_proto::orcher::v1::command::Attributes::ScheduleTask(attrs) => Some(attrs),
            _ => None,
        })
        .ok_or_else(|| OrcherError::invalid_input("Missing ScheduleTask attributes"))?;

    // Parse workflow_id and execution_id as UUIDs
    let workflow_id_uuid = uuid::Uuid::parse_str(workflow_id)
        .map_err(|e| OrcherError::invalid_input(format!("Invalid workflow_id UUID: {}", e)))?;
    let execution_id_uuid = uuid::Uuid::parse_str(execution_id)
        .map_err(|e| OrcherError::invalid_input(format!("Invalid execution_id UUID: {}", e)))?;

    // Convert proto Duration to seconds
    let schedule_to_start_timeout_secs = attrs
        .schedule_to_start_timeout
        .as_ref()
        .map(duration_to_timeout_secs);
    let start_to_close_timeout_secs = attrs
        .start_to_close_timeout
        .as_ref()
        .map(duration_to_timeout_secs);
    let heartbeat_timeout_secs = attrs
        .heartbeat_timeout
        .as_ref()
        .map(duration_to_timeout_secs);

    // The task's retry policy as its row stores it; see task_retry_policy.
    let retry_policy_json = attrs
        .retry_policy
        .as_ref()
        .map(task_retry_policy)
        .transpose()?;

    // Idempotency guard: a ScheduleTask command can be re-emitted on replay if the workflow
    // is re-dispatched (e.g. by an incoming event/signal) while this task is still in flight —
    // replay has no injected result for a task that hasn't finished, so the SDK re-schedules it.
    // Without this guard `create_scheduled` would INSERT a duplicate attempt-1 row alongside the
    // live one (and, for a permanently-failing task, feed a re-dispatch loop). If a non-terminal
    // execution already exists for (workflow_id, execution_id, task_id), nothing is scheduled
    // again — but one that never reached the outbox is dispatched now (see `already_in_flight`).
    // Terminal rows (completed/failed/canceled/timed_out) are handled by replay injection, not here.
    if already_in_flight(
        task_execution_repository,
        coordinator,
        workflow_id_uuid,
        execution_id_uuid,
        &attrs.task_id,
    )
    .await?
    {
        return Ok(());
    }

    // Create the scheduled task execution record and its outbox dispatch,
    // unless this command has been applied before. The check above only sees a
    // first row still in flight; one that has finished is caught here, by the
    // row's uniqueness (V049), so a repeated command cannot run a finished
    // task a second time.
    let Some(task_execution) = record_and_dispatch(
        task_execution_repository,
        coordinator,
        workflow_id_uuid,
        execution_id_uuid,
        namespace,
        attrs,
        retry_policy_json,
        schedule_to_start_timeout_secs,
        start_to_close_timeout_secs,
        heartbeat_timeout_secs,
    )
    .await?
    else {
        info!(
            workflow_id = %workflow_id,
            execution_id = %execution_id,
            task_id = %attrs.task_id,
            "SCHEDULE_TASK: task already scheduled by this workflow — not scheduling it again"
        );
        return Ok(());
    };

    debug!(
        workflow_id = %workflow_id,
        execution_id = %execution_id,
        task_id = %attrs.task_id,
        task_execution_id = %task_execution.id,
        "SCHEDULE_TASK: Task execution record and its dispatch committed"
    );

    // Append TaskScheduled journal entry
    journal_service
        .append_task_scheduled(
            workflow_id,
            execution_id,
            &attrs.task_id,
            &attrs.task_type,
            serde_json::json!({
                "task_queue": attrs.task_queue,
                "input": format!("{} bytes", attrs.input.len()),
            }),
        )
        .await?;

    debug!(
        workflow_id = %workflow_id,
        execution_id = %execution_id,
        task_id = %attrs.task_id,
        task_type = %attrs.task_type,
        "SCHEDULE_TASK: Task scheduled successfully (journal entry appended)"
    );

    // Write user-facing log for task scheduled
    let mut log = WorkflowExecutionLog::info(
        execution_id.to_string(),
        format!("Task '{}' scheduled", attrs.task_type),
    )
    .with_step(attrs.task_id.clone())
    .with_source("CommandProcessor".to_string())
    .with_metadata(serde_json::json!({
        "task_type": attrs.task_type,
        "task_queue": attrs.task_queue,
    }));

    if let Ok(task_uuid) = uuid::Uuid::parse_str(&attrs.task_id) {
        log = log.with_task_id(task_uuid);
    }

    if let Err(e) = logs_repository.add_log(&log).await {
        warn!(
            execution_id = %execution_id,
            task_id = %attrs.task_id,
            error = %e,
            "Failed to write task scheduled log"
        );
    }

    // Notify waiting services that new task work is available
    if let Some(ref pm) = polling_manager {
        pm.notify_work_available(&attrs.task_queue, namespace).await;

        debug!(
            task_id = %attrs.task_id,
            task_queue = %attrs.task_queue,
            namespace = %namespace,
            "Notified waiting services of scheduled task"
        );
    }

    Ok(())
}

/// Handle ScheduleTask command with result for eager dispatch
///
/// This is the enhanced version that returns the TaskExecution for eager task return.
pub async fn handle_schedule_task_with_result(
    journal_service: &Arc<JournalService>,
    task_execution_repository: &Arc<TaskExecutionRepository>,
    coordinator: &Option<Arc<WorkflowCoordinator>>,
    logs_repository: &Arc<WorkflowExecutionLogsRepository>,
    workflow_id: &str,
    execution_id: &str,
    namespace: &str,
    command: &Command,
) -> OrcherResult<Option<TaskExecution>> {
    debug!(
        workflow_id = %workflow_id,
        execution_id = %execution_id,
        namespace = %namespace,
        "SCHEDULE_TASK_WITH_RESULT: Entering (eager task return path)"
    );

    // Extract command attributes
    let attrs = command
        .attributes
        .as_ref()
        .and_then(|a| match a {
            orcher_proto::orcher::v1::command::Attributes::ScheduleTask(attrs) => Some(attrs),
            _ => None,
        })
        .ok_or_else(|| OrcherError::invalid_input("Missing ScheduleTask attributes"))?;

    // Parse workflow_id and execution_id as UUIDs
    let workflow_id_uuid = uuid::Uuid::parse_str(workflow_id)
        .map_err(|e| OrcherError::invalid_input(format!("Invalid workflow_id UUID: {}", e)))?;
    let execution_id_uuid = uuid::Uuid::parse_str(execution_id)
        .map_err(|e| OrcherError::invalid_input(format!("Invalid execution_id UUID: {}", e)))?;

    // Convert proto Duration to seconds
    let schedule_to_start_timeout_secs = attrs
        .schedule_to_start_timeout
        .as_ref()
        .map(duration_to_timeout_secs);
    let start_to_close_timeout_secs = attrs
        .start_to_close_timeout
        .as_ref()
        .map(duration_to_timeout_secs);
    let heartbeat_timeout_secs = attrs
        .heartbeat_timeout
        .as_ref()
        .map(duration_to_timeout_secs);

    // The task's retry policy as its row stores it; see task_retry_policy.
    let retry_policy_json = attrs
        .retry_policy
        .as_ref()
        .map(task_retry_policy)
        .transpose()?;

    // Idempotency guard: a ScheduleTask command can be re-emitted on replay if the workflow
    // is re-dispatched (e.g. by an incoming event/signal) while this task is still in flight —
    // replay has no injected result for a task that hasn't finished, so the SDK re-schedules it.
    // Without this guard `create_scheduled` would INSERT a duplicate attempt-1 row alongside the
    // live one (and, for a permanently-failing task, feed a re-dispatch loop). If a non-terminal
    // execution already exists for (workflow_id, execution_id, task_id), nothing is scheduled
    // again — but one that never reached the outbox is dispatched now (see `already_in_flight`).
    // Terminal rows (completed/failed/canceled/timed_out) are handled by replay injection, not here.
    if already_in_flight(
        task_execution_repository,
        coordinator,
        workflow_id_uuid,
        execution_id_uuid,
        &attrs.task_id,
    )
    .await?
    {
        return Ok(Default::default());
    }

    // Create the scheduled task execution record and its outbox dispatch —
    // the fallback should the eager hand-over not reach a worker — unless this
    // command has been applied before. The check above only sees a first row
    // still in flight; one that has finished is caught here, by the row's
    // uniqueness (V049), so a repeated command cannot run a finished task a
    // second time.
    let Some(task_execution) = record_and_dispatch(
        task_execution_repository,
        coordinator,
        workflow_id_uuid,
        execution_id_uuid,
        namespace,
        attrs,
        retry_policy_json,
        schedule_to_start_timeout_secs,
        start_to_close_timeout_secs,
        heartbeat_timeout_secs,
    )
    .await?
    else {
        info!(
            workflow_id = %workflow_id,
            execution_id = %execution_id,
            task_id = %attrs.task_id,
            "SCHEDULE_TASK_WITH_RESULT: task already scheduled by this workflow — not scheduling it again"
        );
        return Ok(None);
    };

    debug!(
        workflow_id = %workflow_id,
        execution_id = %execution_id,
        task_id = %attrs.task_id,
        task_execution_id = %task_execution.id,
        task_queue = %attrs.task_queue,
        "SCHEDULE_TASK_WITH_RESULT: Task execution record and its dispatch committed"
    );

    // Append TaskScheduled journal entry
    journal_service
        .append_task_scheduled(
            workflow_id,
            execution_id,
            &attrs.task_id,
            &attrs.task_type,
            serde_json::json!({
                "task_queue": attrs.task_queue,
                "input": format!("{} bytes", attrs.input.len()),
            }),
        )
        .await?;

    // Write user-facing log
    let mut log = WorkflowExecutionLog::info(
        execution_id.to_string(),
        format!("Task '{}' scheduled (eager)", attrs.task_type),
    )
    .with_step(attrs.task_id.clone())
    .with_source("CommandProcessor".to_string())
    .with_metadata(serde_json::json!({
        "task_type": attrs.task_type,
        "task_queue": attrs.task_queue,
        "eager_dispatch": true,
    }));

    if let Ok(task_uuid) = uuid::Uuid::parse_str(&attrs.task_id) {
        log = log.with_task_id(task_uuid);
    }

    if let Err(e) = logs_repository.add_log(&log).await {
        warn!(
            execution_id = %execution_id,
            task_id = %attrs.task_id,
            error = %e,
            "Failed to write task scheduled log"
        );
    }

    debug!(
        workflow_id = %workflow_id,
        execution_id = %execution_id,
        task_id = %attrs.task_id,
        task_execution_id = %task_execution.id,
        "SCHEDULE_TASK_WITH_RESULT: Returning task for eager dispatch"
    );

    Ok(Some(task_execution))
}

/// Whether the workflow already has this task in flight, in which case a
/// ScheduleTask schedules nothing more.
///
/// A first attempt in flight that was never handed to the outbox is dispatched
/// here first. Such a row is what an engine left when it stopped between
/// writing a task and writing its dispatch, before the two were written in one
/// transaction: it is scheduled, nothing will ever deliver it, and the
/// workflow's replays — each re-emitting this ScheduleTask — used to find it in
/// flight and do nothing, so the workflow waited on it for good. Dispatching
/// it is claimed on the row and keyed in the outbox, so a row whose dispatch
/// exists, or that another engine is dispatching now, is not dispatched again.
async fn already_in_flight(
    task_execution_repository: &Arc<TaskExecutionRepository>,
    coordinator: &Option<Arc<WorkflowCoordinator>>,
    workflow_id: Uuid,
    execution_id: Uuid,
    task_id: &str,
) -> OrcherResult<bool> {
    let Ok(Some(existing)) = task_execution_repository
        .find_by_idempotency_key(
            orcher_common::types::identifiers::WorkflowId::from_uuid(workflow_id),
            orcher_common::types::identifiers::ExecutionId::from_uuid(execution_id),
            task_id,
        )
        .await
    else {
        return Ok(false);
    };
    let in_flight = matches!(
        existing.status.as_str(),
        "scheduled" | "started" | "running" | "pending"
    );
    if !in_flight {
        return Ok(false);
    }
    if existing.status == "scheduled" {
        if let Some(coord) = coordinator {
            // A failure here is not the completion's: the task is in flight
            // either way, and a row still undispatched is found by the sweep.
            // Failing the completion for it would only have the worker send
            // it again.
            match coord.dispatch_if_never_dispatched(&existing).await {
                Ok(true) => {
                    warn!(
                        workflow_id = %workflow_id,
                        execution_id = %execution_id,
                        task_id = %task_id,
                        task_execution_id = %existing.id,
                        "SCHEDULE_TASK: task was scheduled but never dispatched — dispatched it now"
                    );
                    return Ok(true);
                }
                Ok(false) => {}
                Err(e) => warn!(
                    workflow_id = %workflow_id,
                    execution_id = %execution_id,
                    task_id = %task_id,
                    task_execution_id = %existing.id,
                    error = %e,
                    "SCHEDULE_TASK: could not check whether the task in flight was ever \
                     dispatched; leaving it to the undispatched-task sweep"
                ),
            }
        }
    }
    debug!(
        workflow_id = %workflow_id,
        execution_id = %execution_id,
        task_id = %task_id,
        existing_status = %existing.status,
        existing_attempt = existing.attempt_number,
        "SCHEDULE_TASK: task already in flight — skipping duplicate schedule (idempotent replay)"
    );
    Ok(true)
}

/// Record a first attempt and its outbox dispatch in one transaction, or
/// nothing if this workflow has scheduled the task before (`None`).
///
/// Written apart, an engine that stopped between the two left a task
/// scheduled that nothing would deliver. Without a coordinator there is no
/// outbox to write to, and the row is recorded alone, as it always was.
#[allow(clippy::too_many_arguments)]
async fn record_and_dispatch(
    task_execution_repository: &Arc<TaskExecutionRepository>,
    coordinator: &Option<Arc<WorkflowCoordinator>>,
    workflow_id: Uuid,
    execution_id: Uuid,
    namespace: &str,
    attrs: &orcher_proto::orcher::v1::ScheduleTaskCommandAttributes,
    retry_policy_json: Option<serde_json::Value>,
    schedule_to_start_timeout_secs: Option<i32>,
    start_to_close_timeout_secs: Option<i32>,
    heartbeat_timeout_secs: Option<i32>,
) -> OrcherResult<Option<TaskExecution>> {
    let record_failed = |e: OrcherError| {
        error!(
            error = %e,
            workflow_id = %workflow_id,
            task_id = %attrs.task_id,
            "Failed to create task execution record"
        );
        OrcherError::internal(format!("Failed to create task execution record: {}", e))
    };
    let wf = orcher_common::types::identifiers::WorkflowId::from_uuid(workflow_id);
    let exec = orcher_common::types::identifiers::ExecutionId::from_uuid(execution_id);

    let Some(coord) = coordinator else {
        warn!(
            workflow_id = %workflow_id,
            execution_id = %execution_id,
            task_id = %attrs.task_id,
            "SCHEDULE_TASK: WorkflowCoordinator not available - task not queued"
        );
        return task_execution_repository
            .create_scheduled_once(
                attrs.task_id.clone(),
                attrs.task_type.clone(),
                wf,
                exec,
                attrs.task_queue.clone(),
                namespace.to_string(),
                1, // Initial attempt
                Some(attrs.input.clone()),
                retry_policy_json,
                schedule_to_start_timeout_secs,
                start_to_close_timeout_secs,
                heartbeat_timeout_secs,
                None, // headers
                None, // metadata
            )
            .await
            .map_err(record_failed);
    };

    // Nothing else may take a connection while this transaction holds one:
    // under load every caller holding one and waiting for a second is how the
    // pool ran dry. The announcement goes after the commit for that reason.
    let mut tx = coord.begin_for(workflow_id).await?;
    let Some(task_execution) = task_execution_repository
        .create_scheduled_once_in_tx(
            &mut tx,
            attrs.task_id.clone(),
            attrs.task_type.clone(),
            wf,
            exec,
            attrs.task_queue.clone(),
            namespace.to_string(),
            1, // Initial attempt
            Some(attrs.input.clone()),
            retry_policy_json,
            schedule_to_start_timeout_secs,
            start_to_close_timeout_secs,
            heartbeat_timeout_secs,
            None, // headers
            None, // metadata
        )
        .await
        .map_err(record_failed)?
    else {
        return Ok(None);
    };
    let dispatch = coord
        .dispatch_first_attempt_in_tx(&mut tx, &task_execution)
        .await?;
    tx.commit()
        .await
        .map_err(|e| OrcherError::internal(format!("Failed to commit scheduled task: {}", e)))?;
    if let Some(dispatch) = dispatch {
        coord.announce(dispatch).await;
    }
    Ok(Some(task_execution))
}

/// Handle RequestCancelTask command
///
/// Requests cancellation of a running task by recording a TaskCancelRequested
/// journal entry. The task execution runtime checks for cancellation via heartbeat.
pub async fn handle_request_cancel_task(
    journal_service: &Arc<JournalService>,
    _task_execution_repository: &Arc<TaskExecutionRepository>,
    workflow_id: &str,
    execution_id: &str,
    command: &Command,
) -> OrcherResult<()> {
    debug!(
        workflow_id = %workflow_id,
        execution_id = %execution_id,
        "Handling RequestCancelTask command"
    );

    let attrs = command
        .attributes
        .as_ref()
        .and_then(|a| match a {
            orcher_proto::orcher::v1::command::Attributes::RequestCancelTask(attrs) => Some(attrs),
            _ => None,
        })
        .ok_or_else(|| OrcherError::invalid_input("Missing RequestCancelTask attributes"))?;

    // Record the cancellation request in the journal
    journal_service
        .append_entry(
            workflow_id,
            execution_id,
            "TaskCancelRequested",
            serde_json::json!({
                "scheduled_event_id": attrs.scheduled_event_id,
            }),
        )
        .await?;

    info!(
        workflow_id = %workflow_id,
        execution_id = %execution_id,
        scheduled_event_id = attrs.scheduled_event_id,
        "Task cancellation requested"
    );

    Ok(())
}

/// Handle RecordStepResult command (Closure result persistence)
///
/// Stores step execution results (closures, tasks, etc.) to execution_log table
/// for durable replay and observability.
#[instrument(skip(journal_service, execution_log_repository, logs_repository, command), fields(
    workflow_id = %workflow_id,
    execution_id = %execution_id
))]
pub async fn handle_record_step_result(
    journal_service: &Arc<JournalService>,
    execution_log_repository: &Arc<dyn ExecutionLogRepository>,
    logs_repository: &Arc<WorkflowExecutionLogsRepository>,
    workflow_id: &str,
    execution_id: &str,
    command: &Command,
) -> OrcherResult<()> {
    debug!(
        workflow_id = %workflow_id,
        execution_id = %execution_id,
        "Processing RecordStepResult command"
    );

    // 1. Extract command attributes
    let Some(ref attributes) = command.attributes else {
        return Err(OrcherError::invalid_input(
            "Missing RecordStepResult command attributes",
        ));
    };

    let record_step_attrs = match attributes {
        command::Attributes::RecordStepResult(attrs) => attrs,
        _ => {
            return Err(OrcherError::invalid_input(
                "Invalid command attributes type for RecordStepResult",
            ))
        }
    };

    // 2. Parse workflow_id as UUID
    let workflow_id_uuid = Uuid::parse_str(workflow_id)
        .map_err(|e| OrcherError::invalid_input(format!("Invalid workflow_id UUID: {}", e)))?;

    // 3. Deserialize result (if present)
    let result_json: Option<serde_json::Value> = if !record_step_attrs.result.is_empty() {
        Some(
            serde_json::from_slice(&record_step_attrs.result).map_err(|e| {
                OrcherError::invalid_input(format!("Failed to deserialize step result: {}", e))
            })?,
        )
    } else {
        None
    };

    // 4. Map proto StepType to storage ExecutionStepType
    let step_type = match StepType::try_from(record_step_attrs.step_type) {
        Ok(StepType::Task) => ExecutionStepType::Task,
        Ok(StepType::Closure) => ExecutionStepType::Closure,
        Ok(StepType::ChildWorkflow) => ExecutionStepType::ChildWorkflow,
        Ok(StepType::SideEffect) => ExecutionStepType::SideEffect,
        _ => ExecutionStepType::Closure, // Default to closure for unknown types
    };

    // 5. Determine status (completed or failed)
    let (status, error_message, error_details) =
        if let Some(ref failure) = record_step_attrs.failure {
            let mut error_json = serde_json::Map::new();
            error_json.insert(
                "message".to_string(),
                serde_json::Value::String(failure.message.clone()),
            );
            error_json.insert(
                "stack_trace".to_string(),
                serde_json::Value::String(failure.stack_trace.clone()),
            );
            if let Some(ref cause) = failure.cause {
                error_json.insert(
                    "cause".to_string(),
                    serde_json::Value::String(cause.message.clone()),
                );
            }

            (
                ExecutionLogStatus::Failed,
                Some(failure.message.clone()),
                Some(serde_json::Value::Object(error_json)),
            )
        } else {
            (ExecutionLogStatus::Completed, None, None)
        };

    // 6. Create execution log entry
    let now = Timestamptz::now();
    let entry = ExecutionLogEntry {
        workflow_id: workflow_id_uuid,
        step_name: record_step_attrs.step_name.clone(),
        step_type,
        execution_attempt: record_step_attrs.execution_attempt,
        status,
        input: None,
        result: result_json.clone(),
        error_message,
        error_details,
        created_at: now,
        started_at: Some(now),
        completed_at: Some(now),
        duration_ms: None,
        retry_count: 0,
        last_error: None,
    };

    // 7. Persist to execution_log table
    execution_log_repository
        .store_entry(&entry)
        .await
        .map_err(|e| {
            OrcherError::internal(format!("Failed to store execution log entry: {}", e))
        })?;

    info!(
        workflow_id = %workflow_id,
        execution_id = %execution_id,
        step_name = %record_step_attrs.step_name,
        step_type = ?step_type,
        status = ?status,
        "Step result stored in execution_log"
    );

    // 7.5. Write user-facing log to workflow_execution_logs
    let log_message = if status == ExecutionLogStatus::Completed {
        format!(
            "Task '{}' completed successfully",
            record_step_attrs.step_name
        )
    } else {
        format!("Task '{}' failed", record_step_attrs.step_name)
    };

    let log = if status == ExecutionLogStatus::Completed {
        WorkflowExecutionLog::info(execution_id.to_string(), log_message.clone())
    } else {
        WorkflowExecutionLog::error(execution_id.to_string(), log_message.clone())
    }
    .with_step(record_step_attrs.step_name.clone())
    .with_source("CommandProcessor".to_string())
    .with_metadata(serde_json::json!({
        "step_type": format!("{:?}", step_type),
        "status": format!("{:?}", status),
    }));

    if let Err(e) = logs_repository.add_log(&log).await {
        warn!(
            execution_id = %execution_id,
            step_name = %record_step_attrs.step_name,
            error = %e,
            "Failed to write step result log"
        );
    }

    // 8. The step's StepCompleted entry, which its workflow reads. Written
    // once: a repeated command leaves the first.
    journal_service
        .append_settled_step(
            workflow_id,
            execution_id,
            &crate::workflows::journal::SettledStep::from(&entry),
        )
        .await
        .map_err(|e| {
            OrcherError::internal(format!(
                "Failed to append StepCompleted journal entry: {}",
                e
            ))
        })?;

    Ok(())
}

/// A ScheduleTask command's retry policy in the form a task's row stores it,
/// the one the retry evaluator reads (`retries::retry_policy::RetryPolicy`).
///
/// `maximum_attempts` counts attempts, the first included, as a workflow's
/// does: `N` allows `N` attempts, `1` none after the first, and `0` is
/// unlimited, as the proto documents it and the evaluator reads a stored 0.
/// It used to store 0 as 3, so a task asking for unlimited attempts was
/// given three. A negative count means nothing and is refused, which fails
/// the workflow with the reason rather than retrying the task some number of
/// times nobody asked for. A command without a policy is not given one here.
///
/// Keys MUST match the `RetryPolicy` struct the retry evaluator deserializes:
/// max_attempts / initial_interval_seconds / max_interval_seconds. A previous
/// mismatch made the policy unparsable, so every failed task was sent straight
/// to the DLQ on attempt 1. Unset intervals and coefficient fall back to
/// defaults, so a partially-populated policy still yields a valid one.
pub(crate) fn task_retry_policy(
    rp: &orcher_proto::orcher::v1::RetryPolicy,
) -> OrcherResult<serde_json::Value> {
    if rp.maximum_attempts < 0 {
        return Err(OrcherError::invalid_input(format!(
            "a task's retry_policy.maximum_attempts must not be negative (0 is unlimited), got {}",
            rp.maximum_attempts
        )));
    }
    let backoff_coefficient = if rp.backoff_coefficient > 0.0 {
        rp.backoff_coefficient
    } else {
        2.0
    };
    // Read the FULL Duration (seconds + nanos) as fractional seconds so
    // sub-second retry intervals are preserved.
    let duration_secs = |d: &orcher_proto::prost_types::Duration| -> f64 {
        d.seconds as f64 + (d.nanos as f64) / 1_000_000_000.0
    };
    Ok(serde_json::json!({
        "max_attempts": rp.maximum_attempts,
        "initial_interval_seconds": rp.initial_interval.as_ref().map(duration_secs).unwrap_or(1.0),
        "max_interval_seconds": rp.maximum_interval.as_ref().map(duration_secs).unwrap_or(3600.0),
        "backoff_coefficient": backoff_coefficient,
        "non_retryable_error_types": rp.non_retryable_error_types,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use orcher_proto::prost_types::Duration;

    /// The policy a worker is told is the one the workflow scheduled: stored
    /// by `task_retry_policy` and read back for the poll response. The read
    /// used other key names and integer intervals, so every worker was told
    /// maximum_attempts 0 (unlimited) and no intervals.
    #[test]
    fn a_task_retry_policy_reaches_its_worker_as_scheduled() {
        for maximum_attempts in [0, 1, 4] {
            let scheduled = orcher_proto::orcher::v1::RetryPolicy {
                initial_interval: Some(dur(0, 250_000_000)),
                backoff_coefficient: 1.5,
                maximum_interval: Some(dur(30, 500_000_000)),
                maximum_attempts,
                non_retryable_error_types: vec!["Declined".to_string()],
            };
            let stored = task_retry_policy(&scheduled).unwrap();
            let told = crate::retries::retry_policy::stored_to_proto(&stored);
            assert_eq!(told, scheduled);
        }
    }

    fn dur(seconds: i64, nanos: i32) -> Duration {
        Duration { seconds, nanos }
    }

    fn policy(maximum_attempts: i32) -> orcher_proto::orcher::v1::RetryPolicy {
        orcher_proto::orcher::v1::RetryPolicy {
            maximum_attempts,
            ..Default::default()
        }
    }

    fn stored(maximum_attempts: i32) -> crate::retries::retry_policy::RetryPolicy {
        serde_json::from_value(task_retry_policy(&policy(maximum_attempts)).unwrap())
            .expect("the evaluator reads the stored policy")
    }

    /// 0 is unlimited, N is N attempts counting the first, as a workflow's
    /// maximum_attempts is read: 0 used to be stored as 3.
    #[test]
    fn a_tasks_maximum_attempts_counts_attempts_and_0_is_unlimited() {
        let unlimited = stored(0);
        assert!(unlimited.has_attempts_remaining(1_000_000));

        let once = stored(1);
        assert!(!once.has_attempts_remaining(1), "1 attempt: no retry");

        let three = stored(3);
        assert!(three.has_attempts_remaining(2));
        assert!(
            !three.has_attempts_remaining(3),
            "3 attempts, the first included"
        );
    }

    #[test]
    fn a_negative_maximum_attempts_is_refused() {
        assert!(matches!(
            task_retry_policy(&policy(-1)),
            Err(OrcherError::InvalidInput { .. })
        ));
    }

    #[test]
    fn timeout_secs_preserves_whole_seconds() {
        assert_eq!(duration_to_timeout_secs(&dur(5, 0)), 5);
        assert_eq!(duration_to_timeout_secs(&dur(30, 0)), 30);
    }

    #[test]
    fn timeout_secs_ceils_positive_sub_second_to_one() {
        // The B10 fix: a positive sub-second timeout must not coerce to 0
        // ("no timeout") — it rounds up to 1s.
        assert_eq!(duration_to_timeout_secs(&dur(0, 500_000_000)), 1); // 500ms
        assert_eq!(duration_to_timeout_secs(&dur(0, 1)), 1); // 1ns
    }

    #[test]
    fn timeout_secs_ceils_fractional_seconds() {
        assert_eq!(duration_to_timeout_secs(&dur(1, 500_000_000)), 2); // 1.5s -> 2s
        assert_eq!(duration_to_timeout_secs(&dur(2, 1)), 3); // 2.000000001s -> 3s
    }

    #[test]
    fn timeout_secs_zero_and_negative_stay_zero() {
        assert_eq!(duration_to_timeout_secs(&dur(0, 0)), 0);
        assert_eq!(duration_to_timeout_secs(&dur(-5, 0)), 0);
    }
}
