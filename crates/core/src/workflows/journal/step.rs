//! A settled step as its workflow reads it: the `StepCompleted` entry.
//!
//! A step — a task, an inline closure, a side effect — settles once, with a
//! result or a failure, and the workflow learns of it from a `StepCompleted`
//! entry. A run created by this engine has that entry in its journal, written
//! with the rest of the step's outcome; a run an earlier engine created has it
//! rebuilt from its execution log on each activation (see
//! `WorkflowExecution::step_results_journaled`). Both are built from one
//! [`SettledStep`], so the two kinds of run read their steps alike.

use orcher_common::types::time::Timestamptz;
use orcher_proto::orcher::v1::{Failure, StepCompletedEventAttributes, StepType};
use orcher_storage::repositories::domains::journal::JournalRepository;
use orcher_storage::repositories::{ExecutionLogEntry, ExecutionLogStatus, ExecutionStepType};
use orcher_storage::schema::types::execution::CANCELLED_FAILURE_TYPE;
use orcher_storage::schema::types::journal::JournalEntry;
use serde::{Deserialize, Deserializer, Serialize};

/// The journal entry type of a settled step.
pub const STEP_COMPLETED: &str = "StepCompleted";

/// A step that has settled, as its `StepCompleted` entry records it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SettledStep {
    /// The step's id: a task's id, or the id an inline step was given.
    pub step_name: String,
    /// What kind of step it is.
    pub step_type: ExecutionStepType,
    /// `Completed`, `Failed` or `Cancelled`.
    pub status: ExecutionLogStatus,
    /// The attempt that settled it, counted from 1.
    pub execution_attempt: i32,
    /// The step's result. A JSON `null` is a result — a step returning
    /// nothing — and is kept apart from no result at all.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present"
    )]
    pub result: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// What failed, for a failed or cancelled step.
    pub error_message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// The failure's type, retryability and stack trace, as recorded.
    pub error_details: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// When it settled.
    pub completed_at: Option<Timestamptz>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// How long the attempt that settled it ran.
    pub duration_ms: Option<i64>,
}

/// A field that is there, `null` included.
fn present<'de, D: Deserializer<'de>>(d: D) -> Result<Option<serde_json::Value>, D::Error> {
    serde_json::Value::deserialize(d).map(Some)
}

/// A task's result as its step records it: the JSON the task returned, a
/// JSON `null` for no bytes, and the bytes as a string when they are not
/// JSON.
pub fn result_value(bytes: &[u8]) -> serde_json::Value {
    if bytes.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(bytes).unwrap_or_else(|_| {
            serde_json::Value::String(String::from_utf8_lossy(bytes).to_string())
        })
    }
}

impl From<&ExecutionLogEntry> for SettledStep {
    fn from(step: &ExecutionLogEntry) -> Self {
        SettledStep {
            step_name: step.step_name.clone(),
            step_type: step.step_type,
            status: step.status,
            execution_attempt: step.execution_attempt,
            result: step.result.clone(),
            error_message: step.error_message.clone(),
            error_details: step.error_details.clone(),
            completed_at: step.completed_at,
            duration_ms: step.duration_ms,
        }
    }
}

impl SettledStep {
    /// Whether the step has settled in a way its workflow can read: a result,
    /// a failure that says what failed, or a cancellation. Internal entries
    /// of the execution log (`event:…`) are not steps.
    pub fn is_readable(&self) -> bool {
        if self.step_name.starts_with("event:") {
            return false;
        }
        match self.status {
            ExecutionLogStatus::Completed => self.result.is_some(),
            ExecutionLogStatus::Failed => self.result.is_some() || self.error_message.is_some(),
            ExecutionLogStatus::Cancelled => true,
            ExecutionLogStatus::Pending | ExecutionLogStatus::Running => false,
        }
    }

    /// The step's `StepCompleted` entry, for the journal of execution
    /// `execution_id` of the run whose journal key the step's log row holds.
    pub fn journal_entry(&self, workflow_key: &str, execution_id: &str) -> JournalEntry {
        JournalEntry::new(
            workflow_key.to_string(),
            execution_id.to_string(),
            0,
            STEP_COMPLETED.to_string(),
            serde_json::to_value(self).expect("a settled step serializes"),
        )
    }

    /// Append the step's `StepCompleted` entry inside the caller's
    /// transaction `conn`, unless the execution already holds one for this
    /// step: a step settles once, and the first outcome recorded stands.
    /// Returns whether it was appended.
    pub async fn append_on(
        &self,
        conn: &mut sqlx::PgConnection,
        workflow_key: &str,
        execution_id: &str,
    ) -> orcher_common::OrcherResult<bool> {
        Ok(JournalRepository::append_next_once_on(
            conn,
            self.journal_entry(workflow_key, execution_id),
            &[STEP_COMPLETED],
            "step_name",
            &self.step_name,
        )
        .await?
        .is_some())
    }

    /// The entry's attributes as a worker reads them.
    pub fn attributes(&self) -> StepCompletedEventAttributes {
        let step_type = match self.step_type {
            ExecutionStepType::Task => StepType::Task,
            ExecutionStepType::Closure => StepType::Closure,
            ExecutionStepType::ChildWorkflow => StepType::ChildWorkflow,
            ExecutionStepType::SideEffect => StepType::SideEffect,
        };
        let completed_at = self
            .completed_at
            .map(|at| orcher_proto::prost_types::Timestamp {
                seconds: at.timestamp(),
                nanos: at.timestamp_subsec_nanos() as i32,
            });
        StepCompletedEventAttributes {
            step_name: self.step_name.clone(),
            step_type: step_type as i32,
            // The bytes the workflow decodes: the JSON the step's log row
            // holds, written out again.
            result: self
                .result
                .as_ref()
                .map(|result| serde_json::to_vec(result).unwrap_or_default())
                .unwrap_or_default(),
            failure: self.failure(),
            execution_attempt: self.execution_attempt,
            completed_at,
            duration_ms: self.duration_ms.unwrap_or(0),
        }
    }

    /// The failure a failed step replays to its workflow: the message, and
    /// the type and retryability the step recorded (a task's own failure
    /// type, or a timeout's), so workflow code can tell one failure from
    /// another. A step that recorded no type replays as the generic
    /// `ExecutionError`.
    ///
    /// A cancelled step replays as a failure of type
    /// [`CANCELLED_FAILURE_TYPE`], never retryable, whatever path ended its
    /// task; the message says how. One recorded before cancelled steps named
    /// their type replays the same way.
    pub fn failure(&self) -> Option<Failure> {
        let detail = |name: &str| self.error_details.as_ref().and_then(|d| d.get(name));
        let (message, failure_type, non_retryable) = match self.status {
            ExecutionLogStatus::Failed => (
                self.error_message.clone()?,
                detail("failure_type")
                    .and_then(|v| v.as_str())
                    .filter(|t| !t.is_empty())
                    .unwrap_or("ExecutionError")
                    .to_string(),
                detail("non_retryable")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false),
            ),
            ExecutionLogStatus::Cancelled => (
                self.error_message
                    .clone()
                    .unwrap_or_else(|| "Task was cancelled".to_string()),
                CANCELLED_FAILURE_TYPE.to_string(),
                true,
            ),
            _ => return None,
        };
        Some(Failure {
            non_retryable,
            message,
            source: "StepExecution".to_string(),
            stack_trace: detail("stack_trace")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            cause: None,
            failure_type,
            details: vec![],
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn step(status: ExecutionLogStatus) -> SettledStep {
        SettledStep {
            step_name: "charge_0".into(),
            step_type: ExecutionStepType::Task,
            status,
            execution_attempt: 2,
            result: None,
            error_message: None,
            error_details: None,
            completed_at: Some(Timestamptz::now()),
            duration_ms: Some(12),
        }
    }

    /// What the journal stores reads back as the same step, a `null` result
    /// still a result.
    #[test]
    fn a_settled_step_reads_back_from_its_entry() {
        for result in [
            Some(serde_json::Value::Null),
            Some(serde_json::json!({"n": 1.1, "s": "x"})),
            None,
        ] {
            let mut completed = step(ExecutionLogStatus::Completed);
            completed.result = result;
            let entry = completed.journal_entry("key", "run");
            assert_eq!(entry.event_type, STEP_COMPLETED);
            let back: SettledStep = serde_json::from_value(entry.payload).unwrap();
            assert_eq!(back, completed);
        }
    }

    /// A step returning nothing carries `null`, as the rebuilt entry did; no
    /// result at all carries no bytes.
    #[test]
    fn a_null_result_is_sent_as_null() {
        let mut completed = step(ExecutionLogStatus::Completed);
        completed.result = Some(serde_json::Value::Null);
        assert_eq!(completed.attributes().result, b"null");
        assert!(completed.is_readable());
        assert!(!step(ExecutionLogStatus::Completed).is_readable());
    }

    #[test]
    fn a_failed_step_carries_its_failure() {
        let mut failed = step(ExecutionLogStatus::Failed);
        failed.error_message = Some("card declined".into());
        failed.error_details = Some(serde_json::json!({
            "failure_type": "Declined",
            "non_retryable": true,
            "stack_trace": "at charge",
        }));
        let failure = failed.attributes().failure.unwrap();
        assert_eq!(failure.message, "card declined");
        assert_eq!(failure.failure_type, "Declined");
        assert!(failure.non_retryable);
        assert_eq!(failure.stack_trace, "at charge");

        let cancelled = step(ExecutionLogStatus::Cancelled);
        let failure = cancelled.failure().unwrap();
        assert_eq!(failure.failure_type, CANCELLED_FAILURE_TYPE);
        assert!(failure.non_retryable);
    }

    #[test]
    fn internal_entries_are_not_steps() {
        let mut internal = step(ExecutionLogStatus::Cancelled);
        internal.step_name = "event:task_scheduled:x".into();
        assert!(!internal.is_readable());
    }

    #[test]
    fn a_result_that_is_not_json_is_kept_as_text() {
        assert_eq!(result_value(b""), serde_json::Value::Null);
        assert_eq!(result_value(b"[1,2]"), serde_json::json!([1, 2]));
        assert_eq!(result_value(b"not json"), serde_json::json!("not json"));
    }
}
