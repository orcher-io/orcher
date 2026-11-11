//! The sweep that settles every child the completion paths did not.
//!
//! A child's outcome is delivered inline when its run completes or fails
//! through a worker. Everything else ends a child without passing through
//! there — a cancellation through the API or a command, a timeout, a
//! termination — and an inline delivery can fail part-way. Nothing used to
//! catch any of those: the parent stayed parked on a child that had already
//! ended. This sweep walks the children whose outcome has not been delivered
//! yet and finishes each one, idempotently, so it is safe on every replica at
//! once and safe against the inline path doing the same work.
//!
//! It also applies the parent close policy. A child started with TERMINATE or
//! REQUEST_CANCEL is ended once the run that started it is over; nothing did
//! that before either, so those children ran on with no one waiting for them.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use orcher_common::OrcherResult;
use orcher_storage::repositories::domains::workflow_execution::WorkflowExecutionRepository;
use orcher_storage::schema::types::child_workflow::{ChildWorkflowExecution, OrphanPolicy};
use orcher_storage::schema::types::execution::WorkflowExecution;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use super::child_outcome::{run_is_final, run_was_replaced, ChildOutcomeDelivery, Delivery};

/// How far the sweep follows a chain of successor runs in one pass. Real
/// chains are as long as a child's retries and continuations; the bound only
/// stops a malformed chain from holding the sweep.
const MAX_SUCCESSOR_HOPS: usize = 1000;

/// Configuration for the child reconciliation sweep.
#[derive(Debug, Clone)]
pub struct ChildReconcilerConfig {
    /// Seconds between passes.
    pub poll_interval_secs: u64,
    /// Tracking rows examined per pass. A pass takes the next batch after the
    /// previous one and wraps around, so every open row is reached however
    /// many there are.
    pub batch_size: i64,
}

impl Default for ChildReconcilerConfig {
    fn default() -> Self {
        Self {
            poll_interval_secs: 5,
            batch_size: 500,
        }
    }
}

/// What one pass did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SweepReport {
    /// Open tracking rows examined.
    pub examined: usize,
    /// Rows moved to a successor run.
    pub repointed: usize,
    /// Outcomes delivered to a parent.
    pub delivered: usize,
    /// Children cancelled or terminated because their parent's run is over.
    pub closed_by_parent: usize,
    /// Rows closed because their run does not exist and no parent waits.
    pub orphans_closed: usize,
}

impl SweepReport {
    fn acted(&self) -> bool {
        self.repointed + self.delivered + self.closed_by_parent + self.orphans_closed > 0
    }
}

/// Background sweep over children whose outcome has not reached their parent.
pub struct ChildReconciler {
    delivery: Arc<ChildOutcomeDelivery>,
    executions: Arc<WorkflowExecutionRepository>,
    /// The path every cancellation takes, for a child whose parent's close
    /// policy is to cancel it. Without it the child is cancelled at once.
    cancellation: Option<Arc<super::WorkflowExecutionService>>,
    config: ChildReconcilerConfig,
    /// Id of the last tracking row examined; the next pass starts after it.
    cursor: AtomicI64,
    stop_signal: tokio::sync::watch::Sender<bool>,
    /// Rows already reported as stuck on a replaced run with no successor.
    warned_stalled: std::sync::Mutex<std::collections::HashSet<i64>>,
}

impl ChildReconciler {
    /// Create the sweep. `executions` should be the shard-routed repository
    /// the rest of the engine uses, so runs on every shard are found.
    pub fn new(
        delivery: Arc<ChildOutcomeDelivery>,
        executions: Arc<WorkflowExecutionRepository>,
        config: ChildReconcilerConfig,
    ) -> Self {
        Self {
            delivery,
            executions,
            cancellation: None,
            config,
            cursor: AtomicI64::new(0),
            stop_signal: tokio::sync::watch::channel(false).0,
            warned_stalled: std::sync::Mutex::new(std::collections::HashSet::new()),
        }
    }

    /// Cancel orphans through `service`, the path every cancellation takes, so
    /// a child whose code has run is asked and may clean up.
    pub fn with_cancellation(mut self, service: Arc<super::WorkflowExecutionService>) -> Self {
        self.cancellation = Some(service);
        self
    }

    /// Run a pass every `poll_interval_secs` until [`stop`](Self::stop).
    pub fn start(self: Arc<Self>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let interval = Duration::from_secs(self.config.poll_interval_secs.max(1));
            info!(
                poll_interval_secs = self.config.poll_interval_secs,
                batch_size = self.config.batch_size,
                "Child reconciler started"
            );
            let mut stop = self.stop_signal.subscribe();
            while !*stop.borrow() {
                // A pass is not interrupted part-way: every step is safe to
                // stop between, but there is no reason to leave one half done.
                match self.sweep().await {
                    Ok(report) if report.acted() => info!(?report, "Child reconciler pass"),
                    Ok(report) => debug!(?report, "Child reconciler pass"),
                    Err(e) => warn!(error = %e, "Child reconciler pass failed; retrying next pass"),
                }
                tokio::select! {
                    _ = tokio::time::sleep(interval) => {}
                    _ = stop.wait_for(|stopped| *stopped) => {}
                }
            }
            info!("Child reconciler stopped");
        })
    }

    /// Stop after the pass in progress, if any, without waiting out the
    /// interval.
    pub fn stop(&self) {
        self.stop_signal.send_replace(true);
    }

    /// One pass over the next batch of open tracking rows.
    pub async fn sweep(&self) -> OrcherResult<SweepReport> {
        let after = self.cursor.load(Ordering::SeqCst);
        let rows = self
            .delivery
            .children()
            .list_open_after(after, self.config.batch_size)
            .await?;
        let next = match rows.last() {
            Some(last) if rows.len() as i64 >= self.config.batch_size => last.id,
            _ => 0,
        };
        self.cursor.store(next, Ordering::SeqCst);

        let mut report = SweepReport {
            examined: rows.len(),
            ..SweepReport::default()
        };
        if rows.is_empty() {
            return Ok(report);
        }

        // Each row's child run and its parent's run, loaded together.
        let mut ids: Vec<String> = rows
            .iter()
            .flat_map(|r| [r.child_execution_id.clone(), r.parent_execution_id.clone()])
            .collect();
        ids.sort();
        ids.dedup();
        let runs: HashMap<String, WorkflowExecution> = self
            .executions
            .find_by_execution_ids(&ids)
            .await?
            .into_iter()
            .map(|run| (run.execution_id.clone(), run))
            .collect();

        for tracking in rows {
            if let Err(e) = self.reconcile(tracking.clone(), &runs, &mut report).await {
                warn!(
                    tracking_id = tracking.id,
                    parent_execution_id = %tracking.parent_execution_id,
                    child_execution_id = %tracking.child_execution_id,
                    error = %e,
                    "Could not reconcile child; retrying next pass"
                );
            }
        }
        Ok(report)
    }

    async fn reconcile(
        &self,
        mut tracking: ChildWorkflowExecution,
        runs: &HashMap<String, WorkflowExecution>,
        report: &mut SweepReport,
    ) -> OrcherResult<()> {
        let mut run = match runs.get(&tracking.child_execution_id).cloned() {
            Some(run) => run,
            None => match self.find_unnamed_run(&tracking, runs).await? {
                Some(run) => run,
                None => return self.close_if_orphaned(&tracking, runs, report).await,
            },
        };

        // A replaced run: the child goes on in its successor. Moving the row
        // along is normally done when the successor is created; this covers a
        // move that failed there, and rows that predate it.
        let mut hops = 0;
        while run_was_replaced(&run) {
            let Some(workflow_id) = run.workflow_id.clone() else {
                return Ok(());
            };
            let Some(successor) = self
                .executions
                .find_latest_successor(&run.execution_id, &workflow_id)
                .await?
            else {
                // Marked replaced, successor not created yet. Normally that is
                // a moment; if it lasts, whatever created the replacement
                // stopped half-way — a retry is finished by the retry
                // scheduler's stranded-retry pass, a restart-fresh or reset
                // needs a person. Said once per row rather than every pass.
                if self
                    .warned_stalled
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .insert(tracking.id)
                {
                    warn!(
                        tracking_id = tracking.id,
                        child_execution_id = %run.execution_id,
                        status = %run.status,
                        "Child's run was replaced but no successor exists yet"
                    );
                }
                return Ok(());
            };
            self.delivery
                .children()
                .repoint_child_execution(&run.execution_id, &successor.execution_id)
                .await?;
            report.repointed += 1;
            tracking.child_execution_id = successor.execution_id.clone();
            run = successor;
            hops += 1;
            if hops >= MAX_SUCCESSOR_HOPS {
                warn!(
                    tracking_id = tracking.id,
                    "Child's successor chain is implausibly long; stopping here"
                );
                return Ok(());
            }
        }

        if run_is_final(&run) {
            if self.delivery.deliver(&tracking, &run).await? == Delivery::Delivered {
                report.delivered += 1;
            }
            return Ok(());
        }

        // Still live (or a failure the retry scheduler has yet to act on).
        // What happens to it now depends on its parent.
        let Some(parent) = runs.get(&tracking.parent_execution_id) else {
            return Ok(());
        };
        if !parent_attempt_over(parent) {
            return Ok(());
        }
        let closed = match tracking.orphan_policy {
            OrphanPolicy::Terminate => self.executions.terminate(&run.execution_id).await?,
            // Asked, the child cleans up and ends itself, and its ending is
            // delivered like any other; only one cancelled now is closed here.
            // Asking again on a later sweep does not wake it again.
            OrphanPolicy::Cancel => match &self.cancellation {
                Some(service) => {
                    service
                        .cancel_execution(&run.execution_id, "Its parent ended", None)
                        .await?
                        == super::CancelOutcome::Cancelled
                }
                None => self.executions.cancel(&run.execution_id).await?,
            },
            // WAIT is treated as ABANDON: no proto value maps to it and nothing
            // issues it, and "the parent may not close" is not something a
            // parent that has already closed can be held to.
            OrphanPolicy::Abandon | OrphanPolicy::Wait => false,
        };
        if !closed {
            return Ok(());
        }
        report.closed_by_parent += 1;
        info!(
            parent_execution_id = %tracking.parent_execution_id,
            child_execution_id = %run.execution_id,
            policy = %tracking.orphan_policy.as_str(),
            "Parent is over; applied its close policy to the child"
        );

        // Deliver the ending now rather than a pass later. The parent is over,
        // so this only completes its journal and settles the row.
        if let Some(ended) = self
            .executions
            .find_by_execution_id(&run.execution_id)
            .await?
        {
            if self.delivery.deliver(&tracking, &ended).await? == Delivery::Delivered {
                report.delivered += 1;
            }
        }
        Ok(())
    }
}

impl ChildReconciler {
    /// The run a row names no id of: the row was written with an id no
    /// execution has, by an engine from before rows named their run (still
    /// running during a rolling deploy) or on a shard the migration that
    /// corrected old rows could not reach. The child's first run is the one
    /// its parent started under the row's business id; the caller follows it
    /// to the latest run. The row is moved onto what is found.
    ///
    /// Not every run pointing at the parent with that business id is the
    /// child. A parent that was itself retried or continued as new has a
    /// successor pointing at it under its own workflow id; if the parent
    /// gave its child that same id, the successor matches too, and adopting
    /// it would apply the child's close policy to the parent's own next run —
    /// a TERMINATE child would terminate it. So when the ids coincide nothing
    /// is adopted, and neither is a run another row already names. A row left
    /// without a run is closed once its parent is over.
    async fn find_unnamed_run(
        &self,
        tracking: &ChildWorkflowExecution,
        runs: &HashMap<String, WorkflowExecution>,
    ) -> OrcherResult<Option<WorkflowExecution>> {
        let Some(parent) = runs.get(&tracking.parent_execution_id) else {
            return Ok(None);
        };
        if parent.workflow_id.as_deref() == Some(tracking.child_workflow_id.as_str()) {
            return Ok(None);
        }
        let Some(run) = self
            .executions
            .find_child(&tracking.parent_execution_id, &tracking.child_workflow_id)
            .await?
        else {
            return Ok(None);
        };
        if let Some(other) = self
            .delivery
            .children()
            .get_by_child_execution_id(&run.execution_id)
            .await?
        {
            if other.id != tracking.id {
                return Ok(None);
            }
        }
        self.delivery
            .children()
            .repoint_child_execution(&tracking.child_execution_id, &run.execution_id)
            .await?;
        info!(
            tracking_id = tracking.id,
            child_execution_id = %run.execution_id,
            "Child record named no run; moved it onto the child's run"
        );
        Ok(Some(run))
    }

    /// A row with no run at all. While its parent may still replay, replaying
    /// the parent's start creates the run under the row's id, so the row is
    /// left for that. Once the parent is over — or gone — nothing will create
    /// the run and nobody is waiting for it; left open, the sweep would read
    /// it on every pass forever, so it is closed without delivering anything.
    async fn close_if_orphaned(
        &self,
        tracking: &ChildWorkflowExecution,
        runs: &HashMap<String, WorkflowExecution>,
        report: &mut SweepReport,
    ) -> OrcherResult<()> {
        let parent_over = match runs.get(&tracking.parent_execution_id) {
            Some(parent) => parent_attempt_over(parent),
            None => true,
        };
        if parent_over
            && self
                .delivery
                .abandon_row(
                    tracking,
                    "The child's run was never created or no longer exists",
                )
                .await?
        {
            report.orphans_closed += 1;
            info!(
                tracking_id = tracking.id,
                parent_execution_id = %tracking.parent_execution_id,
                "Closed a child record with no run and no parent waiting on it"
            );
        }
        Ok(())
    }
}

/// Whether the parent run that started a child is over: ended for good, or
/// replaced by a successor. A successor starts its own children afresh, so a
/// child of the replaced run is as orphaned as a child of a finished one.
fn parent_attempt_over(parent: &WorkflowExecution) -> bool {
    run_is_final(parent) || run_was_replaced(parent)
}
