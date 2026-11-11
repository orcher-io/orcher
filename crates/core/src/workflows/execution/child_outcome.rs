//! Telling a parent how its child ended.
//!
//! A child is tracked by one row in `child_workflow_executions`, written when
//! its parent starts it. The row names the run that is currently executing the
//! child and follows it from run to run — a retry, a restart-fresh or a reset
//! moves it to the successor — so it is the one authority on who a run's parent
//! is. A run's own `parent_execution_id` cannot be: a successor points there at
//! the run it replaced, and treating that as a parent is how a retried
//! workflow used to "notify" its own failed first attempt.
//!
//! The outcome is delivered once, when the child's run has ended for good,
//! and only then: a failed attempt that is going to be retried is not an
//! outcome, and telling the parent about it failed the parent over a child
//! that then went on to succeed.

use std::sync::Arc;

use orcher_common::OrcherResult;
use orcher_storage::repositories::domains::child_workflow::ChildWorkflowRepository;
use orcher_storage::repositories::domains::journal::JournalRepository;
use orcher_storage::repositories::domains::workflow_execution::WorkflowExecutionRepository;
use orcher_storage::schema::types::child_workflow::{ChildWorkflowExecution, ChildWorkflowState};
use orcher_storage::schema::types::execution::WorkflowExecution;
use orcher_storage::schema::types::journal::JournalEntry;
use serde_json::Value;
use tracing::{debug, info, warn};

use crate::engine::dispatch::PollingManager;

/// The journal events a child's outcome is recorded as.
///
/// Only these two, whatever the ending: the SDKs other than sdk-core react to
/// nothing else, so a child that was cancelled, timed out or terminated is
/// reported as failed, with a failure type saying which. A parent told in any
/// other vocabulary would wait on the child forever.
const OUTCOME_EVENT_TYPES: [&str; 2] = [
    "ChildWorkflowExecutionCompleted",
    "ChildWorkflowExecutionFailed",
];

/// How a child workflow ended, as its parent is told.
#[derive(Debug, Clone, PartialEq)]
pub enum ChildOutcome {
    /// It completed.
    Completed {
        /// What it returned; null for nothing.
        result: Value,
    },
    /// It failed, and its retry policy will not run it again.
    Failed {
        /// Its error message.
        error: String,
    },
    /// It was cancelled — through the API, by its parent, or by itself.
    Canceled {
        /// Why, as the parent is told.
        reason: String,
    },
    /// It ran past its execution timeout.
    TimedOut {
        /// Why, as the parent is told.
        reason: String,
    },
    /// It was terminated.
    Terminated {
        /// Why, as the parent is told.
        reason: String,
    },
}

impl ChildOutcome {
    /// How `run` ended, or `None` while it may still produce a different
    /// outcome — including a failure that is going to be retried.
    pub fn of_run(run: &WorkflowExecution) -> Option<Self> {
        if !run_is_final(run) {
            return None;
        }
        let reason = |fallback: &str| {
            run.error
                .clone()
                .filter(|e| !e.is_empty())
                .unwrap_or_else(|| fallback.to_string())
        };
        Some(match run.status.as_str() {
            // A child that returned nothing completed with null; that is its
            // result, not a missing one.
            "completed" => ChildOutcome::Completed {
                result: run.result.clone().unwrap_or(Value::Null),
            },
            "failed" => ChildOutcome::Failed {
                error: reason("Child workflow failed"),
            },
            "cancelled" => ChildOutcome::Canceled {
                reason: reason("Child workflow was cancelled"),
            },
            "timed_out" => ChildOutcome::TimedOut {
                reason: reason("Child workflow timed out"),
            },
            "terminated" => ChildOutcome::Terminated {
                reason: reason("Child workflow was terminated"),
            },
            _ => return None,
        })
    }

    /// The journal event and payload the parent receives.
    fn journal_event(
        &self,
        tracking: &ChildWorkflowExecution,
        child_execution_id: &str,
    ) -> (&'static str, Value) {
        let mut payload = serde_json::json!({
            "child_workflow_id": tracking.child_workflow_id,
            "child_execution_id": child_execution_id,
            "initiated_event_id": tracking.parent_initiated_event_id,
            "started_event_id": tracking.parent_started_event_id.unwrap_or(0),
        });
        let (event_type, reason, failure_type) = match self {
            ChildOutcome::Completed { result } => {
                payload["result"] = result.clone();
                return ("ChildWorkflowExecutionCompleted", payload);
            }
            ChildOutcome::Failed { error } => ("ChildWorkflowExecutionFailed", error, None),
            ChildOutcome::Canceled { reason } => (
                "ChildWorkflowExecutionFailed",
                reason,
                Some("ChildWorkflowCanceled"),
            ),
            ChildOutcome::TimedOut { reason } => (
                "ChildWorkflowExecutionFailed",
                reason,
                Some("ChildWorkflowTimedOut"),
            ),
            ChildOutcome::Terminated { reason } => (
                "ChildWorkflowExecutionFailed",
                reason,
                Some("ChildWorkflowTerminated"),
            ),
        };
        payload["failure_reason"] = Value::String(reason.clone());
        if let Some(failure_type) = failure_type {
            payload["failure_type"] = Value::String(failure_type.to_string());
        }
        (event_type, payload)
    }
}

/// Whether `run` has ended for good: nothing will run it again, so its
/// outcome is the child's outcome.
///
/// A failed run is final only once its retry policy has given up on it: its
/// retries are used up, or the failure path settled it (`retries_settled_at`)
/// on deciding not to retry. A timed-out run is final unless a retry is
/// scheduled for it: the timeout watcher decides as it times the run out, so
/// a run timeout the policy retries carries `next_retry_at` and the workflow
/// goes on in the retry, while an execution timeout is never retried. A run
/// replaced by a successor (`restarted_fresh`, `reset`) is not final either:
/// the child goes on in the successor.
///
/// The one definition of "finished" for a run: a child's parent and a client
/// waiting on a result use it alike. The older spellings the status column
/// has held are accepted with the current ones.
pub fn run_is_final(run: &WorkflowExecution) -> bool {
    let retries_left = run.retry_count < run.max_retries && run.retries_settled_at.is_none();
    match run.status.to_lowercase().as_str() {
        "completed" | "success" | "cancelled" | "canceled" | "terminated" => true,
        "timed_out" | "timeout" => !(retries_left && run.next_retry_at.is_some()),
        "failed" | "error" => !retries_left,
        _ => false,
    }
}

/// Whether `run` was replaced by a successor: a restart-fresh, a retry or a
/// reset ended it and started another run in its place (same workflow id,
/// `parent_execution_id` pointing back). What was asked of the run — its
/// result, a parent's wait on it — goes on in the successor.
pub fn run_was_replaced(run: &WorkflowExecution) -> bool {
    matches!(run.status.as_str(), "restarted_fresh" | "reset")
}

/// Whether a parent's run is over, so there is nothing to wake.
///
/// Broader than [`run_is_final`]: a failed parent waiting on a retry will
/// resume as a fresh run, not this one, and a replaced one never resumes.
pub(crate) fn run_is_over(run: &WorkflowExecution) -> bool {
    matches!(
        run.status.as_str(),
        "completed"
            | "failed"
            | "cancelled"
            | "terminated"
            | "timed_out"
            | "restarted_fresh"
            | "reset"
    )
}

/// What an attempt to deliver a run's outcome did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Delivery {
    /// No tracking row names this run: it is nobody's child.
    NotAChild,
    /// The run has not ended for good; there is nothing to tell yet.
    NotFinal,
    /// The tracking row is already settled; the parent was told before.
    AlreadyDelivered,
    /// The outcome is in the parent's journal and the row is settled.
    Delivered,
}

/// Appends a finished child's outcome to its parent's journal, once, and
/// wakes the parent.
///
/// Shared by the completion paths, which deliver as a run finishes, and the
/// reconciliation sweep, which delivers what they could not — outcomes they
/// failed to record, and endings that happen outside them: a cancellation, a
/// timeout, a termination. Every step is idempotent, so either may repeat
/// what the other did.
pub struct ChildOutcomeDelivery {
    executions: Arc<WorkflowExecutionRepository>,
    journal: Arc<JournalRepository>,
    children: Arc<ChildWorkflowRepository>,
    polling_manager: Arc<PollingManager>,
}

impl ChildOutcomeDelivery {
    /// Create the delivery component.
    pub fn new(
        executions: Arc<WorkflowExecutionRepository>,
        journal: Arc<JournalRepository>,
        children: Arc<ChildWorkflowRepository>,
        polling_manager: Arc<PollingManager>,
    ) -> Self {
        Self {
            executions,
            journal,
            children,
            polling_manager,
        }
    }

    /// The tracking repository this component reads and settles.
    pub fn children(&self) -> &Arc<ChildWorkflowRepository> {
        &self.children
    }

    /// Deliver `run`'s outcome if it is a child that has ended for good.
    pub async fn deliver_for_run(&self, run: &WorkflowExecution) -> OrcherResult<Delivery> {
        let tracking = match self
            .children
            .get_by_child_execution_id(&run.execution_id)
            .await?
        {
            Some(tracking) => tracking,
            None => match self.legacy_row_for(run).await? {
                Some(tracking) => tracking,
                None => return Ok(Delivery::NotAChild),
            },
        };
        self.deliver(&tracking, run).await
    }

    /// The tracking row for `run` when the row does not name it: a row written
    /// with an id no execution has — by an engine from before rows named their
    /// run, still running during a rolling deploy, or on a shard the migration
    /// that corrected the old rows could not reach.
    ///
    /// Only a child's first run can be found this way: its parent is the run
    /// its `parent_execution_id` names, and it carries the business id its
    /// parent gave it. A successor carries its predecessor's `workflow_id` and
    /// points at the predecessor, so it never matches as a child of that run;
    /// the reconciliation sweep follows those chains instead. A row that names
    /// a run which does exist belongs to that run and is left alone. A row
    /// found this way is moved onto `run`.
    async fn legacy_row_for(
        &self,
        run: &WorkflowExecution,
    ) -> OrcherResult<Option<ChildWorkflowExecution>> {
        let (Some(parent_execution_id), Some(workflow_id)) = (
            run.parent_execution_id.as_deref(),
            run.workflow_id.as_deref(),
        ) else {
            return Ok(None);
        };
        let Some(parent) = self
            .executions
            .find_by_execution_id(parent_execution_id)
            .await?
        else {
            return Ok(None);
        };
        if parent.workflow_id.as_deref() == Some(workflow_id) {
            // Same logical workflow: `run` is its predecessor's successor.
            return Ok(None);
        }
        let Some(mut tracking) = self
            .children
            .get_by_parent_and_child_id(parent_execution_id, workflow_id)
            .await?
        else {
            return Ok(None);
        };
        if tracking.child_execution_id != run.execution_id
            && self
                .executions
                .find_by_execution_id(&tracking.child_execution_id)
                .await?
                .is_some()
        {
            return Ok(None);
        }
        self.children
            .repoint_child_execution(&tracking.child_execution_id, &run.execution_id)
            .await?;
        info!(
            parent_execution_id = %parent_execution_id,
            child_execution_id = %run.execution_id,
            "Child record named no run; moved it onto the child's run"
        );
        tracking.child_execution_id = run.execution_id.clone();
        Ok(Some(tracking))
    }

    /// Deliver the outcome of `run`, the run `tracking` currently names.
    ///
    /// The steps run in the order that makes a repeat safe and keeps the
    /// parent from being woken twice. The outcome is appended at most once
    /// (deduplicated on the run's id inside the journal lock); the row is then
    /// settled conditionally, which exactly one caller wins; and only that
    /// caller wakes the parent. A second waker would release a claim the
    /// parent may by then hold for a worker running it, and hand the same
    /// activation to another worker.
    ///
    /// An attempt that appends and stops before settling leaves the row open,
    /// and the next attempt — here or in the sweep — settles it and wakes the
    /// parent. One that settles and stops before waking leaves the parent
    /// parked with its outcome in its journal; its claim times out and the
    /// reclaimer hands it out, as for any worker that disappears.
    pub async fn deliver(
        &self,
        tracking: &ChildWorkflowExecution,
        run: &WorkflowExecution,
    ) -> OrcherResult<Delivery> {
        if tracking.state.is_terminal() {
            return Ok(Delivery::AlreadyDelivered);
        }
        let Some(outcome) = ChildOutcome::of_run(run) else {
            return Ok(Delivery::NotFinal);
        };

        let parent = self
            .executions
            .find_by_execution_id(&tracking.parent_execution_id)
            .await?;
        let Some(parent) = parent else {
            // Removed by retention: there is no one to tell. Settling the row
            // stops the sweep from asking again on every pass.
            warn!(
                parent_execution_id = %tracking.parent_execution_id,
                child_execution_id = %run.execution_id,
                "The finished child's parent no longer exists; nothing to tell"
            );
            return Ok(if self.settle(tracking, &outcome).await? {
                Delivery::Delivered
            } else {
                Delivery::AlreadyDelivered
            });
        };

        let (event_type, payload) = outcome.journal_event(tracking, &run.execution_id);
        let mut entry = JournalEntry::new(
            parent.id.as_uuid().to_string(),
            parent.execution_id.clone(),
            0,
            event_type.to_string(),
            payload,
        );
        // Owned by the parent's organization, like everything in its journal.
        entry.organization_id = parent.organization_id;

        let recorded = self
            .journal
            .append_next_once(
                entry,
                &OUTCOME_EVENT_TYPES,
                "child_execution_id",
                &run.execution_id,
            )
            .await?;
        if recorded.is_some() {
            info!(
                parent_execution_id = %parent.execution_id,
                child_execution_id = %run.execution_id,
                event_type = %event_type,
                "Child workflow outcome recorded in parent journal"
            );
        }

        if !self.settle(tracking, &outcome).await? {
            debug!(
                parent_execution_id = %parent.execution_id,
                child_execution_id = %run.execution_id,
                "Child workflow outcome already delivered"
            );
            return Ok(Delivery::AlreadyDelivered);
        }

        // A parent that is over has nothing to resume: waking it would hand a
        // finished run back to a worker. Its journal still gets the outcome,
        // which is harmless and keeps the record complete.
        if !run_is_over(&parent) {
            self.wake(&parent).await;
        }
        Ok(Delivery::Delivered)
    }

    /// Close an open row whose child can never report: its run does not exist
    /// and its parent is over or gone, so nothing will create the run and no
    /// one is waiting for it. Nothing is delivered. Returns whether this call
    /// closed it.
    pub async fn abandon_row(
        &self,
        tracking: &ChildWorkflowExecution,
        reason: &str,
    ) -> OrcherResult<bool> {
        self.children
            .settle_open(
                tracking.id,
                ChildWorkflowState::Failed,
                None,
                Some(reason.to_string()),
            )
            .await
    }

    /// Close the tracking row with how the child ended, if it is still open.
    /// Returns whether this call closed it.
    async fn settle(
        &self,
        tracking: &ChildWorkflowExecution,
        outcome: &ChildOutcome,
    ) -> OrcherResult<bool> {
        let (state, result, reason) = match outcome {
            ChildOutcome::Completed { result } => {
                (ChildWorkflowState::Completed, Some(result.clone()), None)
            }
            ChildOutcome::Failed { error } => (ChildWorkflowState::Failed, None, Some(error)),
            ChildOutcome::Canceled { reason } => (ChildWorkflowState::Canceled, None, Some(reason)),
            ChildOutcome::TimedOut { reason } => (ChildWorkflowState::TimedOut, None, Some(reason)),
            ChildOutcome::Terminated { reason } => {
                (ChildWorkflowState::Terminated, None, Some(reason))
            }
        };
        self.children
            .settle_open(tracking.id, state, result, reason.cloned())
            .await
    }

    /// Hand the parent back to a worker so it replays and sees the outcome.
    ///
    /// The parent parked on its child holding its claim; releasing it and
    /// queueing a continuation is the same wake-up a task completion gives.
    /// A failure to release is not fatal — the claim times out and the
    /// reclaimer hands the parent out anyway.
    ///
    /// A parent whose activation is still out with a worker — it started
    /// several children, and this outcome arrived before that activation had
    /// completed — is not handed out a second time alongside it: the wake-up
    /// is left for that completion to take.
    async fn wake(&self, parent: &WorkflowExecution) {
        use orcher_storage::repositories::domains::workflow_execution::Resume;
        match self
            .executions
            .release_claim_to_resume(&parent.execution_id)
            .await
        {
            Ok(Resume::Deferred) => return,
            Ok(_) => {}
            Err(e) => warn!(
                execution_id = %parent.execution_id,
                error = %e,
                "Failed to release the parent's claim; it resumes when the claim times out"
            ),
        }
        let preferred_worker = self
            .polling_manager
            .journal_holder(&parent.execution_id)
            .await;
        self.polling_manager.push_continuation(
            &parent.task_queue,
            &parent.namespace,
            &parent.execution_id,
            preferred_worker,
        );
        self.polling_manager
            .notify_work_available(&parent.task_queue, &parent.namespace)
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orcher_common::types::time::Timestamptz;
    use orcher_storage::schema::types::child_workflow::OrphanPolicy;

    fn run(status: &str, retry_count: i32, max_retries: i32) -> WorkflowExecution {
        let mut run = WorkflowExecution::builder()
            .execution_id("run-1".to_string())
            .workflow_name("child".to_string())
            .workflow_version("1.0".to_string())
            .namespace("default".to_string())
            .build();
        run.status = status.to_string();
        run.retry_count = retry_count;
        run.max_retries = max_retries;
        run
    }

    fn tracking() -> ChildWorkflowExecution {
        ChildWorkflowExecution::new(
            "parent".to_string(),
            "parent-run".to_string(),
            7,
            "child_0".to_string(),
            "run-1".to_string(),
            "child".to_string(),
            "default".to_string(),
            Value::Null,
            OrphanPolicy::Abandon,
        )
    }

    /// A failure the policy will retry is not the child's outcome.
    /// Every spelling the status column has held is read as the state it
    /// names: a timed-out run with no retry scheduled is final at once, a
    /// failure once its retries are used up.
    #[test]
    fn every_spelling_of_a_final_status_is_final() {
        for status in [
            "completed",
            "success",
            "cancelled",
            "canceled",
            "terminated",
            "timed_out",
            "timeout",
        ] {
            assert!(run_is_final(&run(status, 0, 3)), "{status}");
        }
        assert!(!run_is_final(&run("error", 0, 3)));
        assert!(run_is_final(&run("error", 3, 3)));
        assert!(!run_is_final(&run("running", 0, 0)));
    }

    #[test]
    fn a_failure_with_retries_left_is_not_final() {
        assert!(!run_is_final(&run("failed", 0, 2)));
        assert_eq!(ChildOutcome::of_run(&run("failed", 0, 2)), None);
        assert!(run_is_final(&run("failed", 2, 2)));
    }

    /// A run timeout with a retry scheduled is not the outcome: the workflow
    /// goes on in the retry. One with no retry scheduled — an execution
    /// timeout, or a run timeout the policy settled — is.
    #[test]
    fn a_timeout_with_a_retry_scheduled_is_not_final() {
        let mut retried = run("timed_out", 0, 2);
        retried.next_retry_at = Some(Timestamptz::now());
        assert!(!run_is_final(&retried));
        assert_eq!(ChildOutcome::of_run(&retried), None);

        let mut settled = retried.clone();
        settled.retries_settled_at = Some(Timestamptz::now());
        assert!(run_is_final(&settled));

        let mut used_up = run("timed_out", 2, 2);
        used_up.next_retry_at = Some(Timestamptz::now());
        assert!(run_is_final(&used_up));

        assert!(run_is_final(&run("timed_out", 0, 2)));
    }

    #[test]
    fn a_replaced_run_is_not_final() {
        assert!(!run_is_final(&run("restarted_fresh", 0, 0)));
        assert!(!run_is_final(&run("reset", 0, 0)));
        assert!(!run_is_final(&run("running", 0, 0)));
    }

    /// Only Completed and Failed reach SDKs that know nothing else; the
    /// failure type says which ending it was.
    #[test]
    fn endings_other_than_completion_reach_the_parent_as_failures() {
        let tracking = tracking();
        for (status, failure_type) in [
            ("cancelled", "ChildWorkflowCanceled"),
            ("timed_out", "ChildWorkflowTimedOut"),
            ("terminated", "ChildWorkflowTerminated"),
        ] {
            let outcome = ChildOutcome::of_run(&run(status, 0, 0)).expect("final");
            let (event_type, payload) = outcome.journal_event(&tracking, "run-1");
            assert_eq!(event_type, "ChildWorkflowExecutionFailed", "{status}");
            assert_eq!(payload["failure_type"], failure_type, "{status}");
            assert_eq!(payload["child_workflow_id"], "child_0");
            assert_eq!(payload["initiated_event_id"], 7);
            assert!(payload["failure_reason"]
                .as_str()
                .is_some_and(|r| !r.is_empty()));
        }
    }

    #[test]
    fn a_completion_carries_its_result_and_null_is_a_result() {
        let mut completed = run("completed", 0, 0);
        let (event_type, payload) = ChildOutcome::of_run(&completed)
            .unwrap()
            .journal_event(&tracking(), "run-1");
        assert_eq!(event_type, "ChildWorkflowExecutionCompleted");
        assert!(payload["result"].is_null());

        completed.result = Some(serde_json::json!({ "n": 1 }));
        let (_, payload) = ChildOutcome::of_run(&completed)
            .unwrap()
            .journal_event(&tracking(), "run-1");
        assert_eq!(payload["result"]["n"], 1);
    }
}
