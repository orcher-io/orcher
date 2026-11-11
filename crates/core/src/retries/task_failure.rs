//! What a task's failure records, built the same way by every path that ends
//! a task: the worker's own failure report and the timeout watcher.
//!
//! The two used to build these separately and had drifted: one took the
//! task's input from a journal lookup that never matched, so its failed steps
//! carried no input, and the two disagreed on `duration_ms` and `failed_at`.

use orcher_common::types::time::Timestamptz;
use orcher_storage::repositories::{ExecutionLogEntry, ExecutionLogStatus, ExecutionStepType};
use orcher_storage::schema::types::execution::{TaskExecution, CANCELLED_FAILURE_TYPE};
use orcher_storage::schema::types::journal::JournalEntry;

/// How long the attempt ran, from when it started to `now`.
fn duration_ms(task: &TaskExecution, now: Timestamptz) -> Option<i64> {
    task.started_at.map(|start| elapsed_ms(start, now))
}

/// Milliseconds from `start` to `now`, never negative.
///
/// Both are read from the wall clock, which can be stepped back between the
/// two readings (a clock resynchronized under load). A negative duration
/// broke the step row's `duration_ms >= 0` check, and the completion that
/// wrote it failed after it had been recorded, before its workflow was woken.
pub(crate) fn elapsed_ms(start: Timestamptz, now: Timestamptz) -> i64 {
    (now - start).as_millis().max(0) as i64
}

/// The input a task's step records: the input the task was scheduled with,
/// which its row holds. Every path that ends a task (completion, failure,
/// cancellation) records it from here.
pub(crate) fn step_input(task: &TaskExecution) -> Option<serde_json::Value> {
    task.input
        .as_deref()
        .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(bytes).ok())
}

/// The details a failed step records, always naming the failure's type: the
/// type the failure carried, or else the task's own error type (a timeout's
/// `HeartbeatTimeout` or `TimeoutError`), so the workflow's replay can hand it
/// on.
fn typed_details(
    task: &TaskExecution,
    details: Option<&serde_json::Value>,
) -> Option<serde_json::Value> {
    let named = details
        .and_then(|d| d.get("failure_type"))
        .and_then(|t| t.as_str())
        .is_some_and(|t| !t.is_empty());
    match (named, task.error_type.as_deref()) {
        (false, Some(error_type)) => {
            let mut typed = match details {
                Some(serde_json::Value::Object(fields)) => fields.clone(),
                _ => serde_json::Map::new(),
            };
            typed.insert("failure_type".to_string(), error_type.into());
            Some(serde_json::Value::Object(typed))
        }
        _ => details.cloned(),
    }
}

/// The failed step the workflow's replay reads to observe the failure.
pub(crate) fn failed_step(
    task: &TaskExecution,
    message: &str,
    details: Option<&serde_json::Value>,
    now: Timestamptz,
) -> ExecutionLogEntry {
    let input = step_input(task);
    ExecutionLogEntry {
        workflow_id: task.workflow_id.as_uuid(),
        step_name: task.task_id.clone(),
        step_type: ExecutionStepType::Task,
        execution_attempt: task.attempt_number,
        status: ExecutionLogStatus::Failed,
        input,
        result: None,
        error_message: Some(message.to_string()),
        error_details: typed_details(task, details),
        created_at: task.started_at.unwrap_or(now),
        started_at: task.started_at,
        completed_at: Some(now),
        duration_ms: duration_ms(task, now),
        retry_count: (task.attempt_number - 1).max(0),
        last_error: Some(message.to_string()),
    }
}

/// The details a cancelled step records: the one failure type every
/// cancellation replays as, and never retried.
pub(crate) fn cancelled_details() -> serde_json::Value {
    serde_json::json!({
        "failure_type": CANCELLED_FAILURE_TYPE,
        "non_retryable": true,
    })
}

/// The cancelled step the workflow's replay reads to observe that `task`
/// ended cancelled, for `message`, which says how. It replays as a failure
/// of type [`CANCELLED_FAILURE_TYPE`], whichever path ended the task.
pub(crate) fn cancelled_step(
    task: &TaskExecution,
    message: &str,
    now: Timestamptz,
) -> ExecutionLogEntry {
    ExecutionLogEntry {
        status: ExecutionLogStatus::Cancelled,
        ..failed_step(task, message, Some(&cancelled_details()), now)
    }
}

/// The TaskFailed journal entry for one failed attempt.
pub(crate) fn task_failed_entry(
    task: &TaskExecution,
    message: &str,
    details: Option<&serde_json::Value>,
    now: Timestamptz,
) -> JournalEntry {
    JournalEntry::new(
        task.workflow_id.to_string(),
        task.execution_id.to_string(),
        0,
        "TaskFailed".to_string(),
        serde_json::json!({
            "task_id": task.task_id,
            "error_message": message,
            "error_details": details,
            "execution_attempt": task.attempt_number,
            "duration_ms": duration_ms(task, now),
            "failed_at": now.to_rfc3339(),
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use orcher_common::types::identifiers::{ExecutionId, WorkflowId};

    fn task() -> TaskExecution {
        TaskExecution::new_scheduled(
            "charge_0".into(),
            "charge".into(),
            WorkflowId::new_v4(),
            ExecutionId::new_v4(),
            "orders".into(),
            "default".into(),
            Some(br#"{"amount":7}"#.to_vec()),
            None,
            None,
            None,
            None,
        )
    }

    /// A clock stepped back between a task's start and its end gives a
    /// duration of zero, never a negative one.
    #[test]
    fn a_clock_stepped_back_gives_no_negative_duration() {
        let now = Timestamptz::now();
        let later = now + jiff::SignedDuration::from_millis(5);
        assert_eq!(elapsed_ms(later, now), 0);
        assert_eq!(elapsed_ms(now, later), 5);
    }

    /// A timed-out task's step names the timeout as its failure type, so
    /// the workflow can tell a timeout from other failures.
    #[test]
    fn a_timed_out_step_names_its_timeout() {
        let mut timed_out = task();
        timed_out.mark_timed_out("heartbeat".into());
        let step = failed_step(&timed_out, "Task timed out", None, Timestamptz::now());
        assert_eq!(
            step.error_details.unwrap()["failure_type"],
            "HeartbeatTimeout"
        );
        assert_eq!(step.input, Some(serde_json::json!({"amount": 7})));
    }

    /// A failure that carried its own type keeps it, whatever the task's.
    #[test]
    fn a_failure_keeps_the_type_it_carried() {
        let mut failed = task();
        failed.error_type = Some("TimeoutError".into());
        let details = serde_json::json!({"failure_type": "CardDeclined", "non_retryable": true});
        let step = failed_step(&failed, "declined", Some(&details), Timestamptz::now());
        assert_eq!(step.error_details, Some(details));
    }
}
