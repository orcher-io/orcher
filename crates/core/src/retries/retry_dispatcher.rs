//! Background dispatcher that delivers ready task retries to workers.
//!
//! When a task fails and is scheduled for retry, the retry scheduler INSERTTs a new
//! `task_executions` row (status `scheduled`, `attempt_number > 1`,
//! `scheduled_at = now + backoff`). Nothing else delivers that row to a worker — the
//! immediate outbox dispatch that used to live in `fail_task_execution` produced a
//! payload the task poller could never match, and `find_ready_retries` had no caller.
//!
//! This loop is the sole task-retry delivery path. Each tick it finds retries whose
//! backoff has elapsed (`scheduled_at <= now`) and dispatches each through the SAME
//! `WorkflowCoordinator::dispatch_task` path the initial attempt uses, so there is a
//! single, drift-free delivery mechanism. Dispatch is idempotent: the outbox dedups on
//! an attempt-scoped idempotency key, so re-sweeping a row that a worker has not yet
//! claimed — and running multiple orchestrator replicas — are both safe.
//!
//! Every tenth tick it also delivers first attempts that were scheduled and never
//! dispatched: rows an engine left behind when it stopped between writing a task and
//! writing its dispatch, before the two went into one transaction. Nothing else would
//! deliver them — the workflow's next replay repairs its own row, but a workflow parked
//! on that task is never replayed for it.

use std::sync::Arc;
use std::time::Duration;

use orcher_common::error::OrcherResult;
use orcher_storage::schema::types::coordination::OutboxTaskType;
use tracing::{debug, error, info, warn};

use super::retry_scheduler::TaskRetryScheduler;
use crate::engine::WorkflowCoordinator;

/// Default interval between retry-dispatch sweeps.
pub const DEFAULT_SWEEP_INTERVAL: Duration = Duration::from_secs(1);

/// How many retry sweeps go by between two sweeps for undispatched first
/// attempts. Those rows are rare and none of them is urgent to the second;
/// at the default interval this looks every ten seconds.
const UNDISPATCHED_SWEEP_EVERY: u64 = 10;

/// How old an undispatched first attempt must be before the sweep dispatches
/// it. A row younger than this may belong to an engine that is still writing
/// its dispatch; see `find_undispatched_first_attempts`.
pub const UNDISPATCHED_GRACE: Duration = Duration::from_secs(60);

/// Polls for task retries whose backoff has elapsed and dispatches them to workers.
pub struct TaskRetryDispatcher {
    scheduler: TaskRetryScheduler,
    coordinator: Arc<WorkflowCoordinator>,
    interval: Duration,
}

impl TaskRetryDispatcher {
    /// Create a dispatcher with the default sweep interval.
    pub fn new(scheduler: TaskRetryScheduler, coordinator: Arc<WorkflowCoordinator>) -> Self {
        Self {
            scheduler,
            coordinator,
            interval: DEFAULT_SWEEP_INTERVAL,
        }
    }

    /// Override the sweep interval.
    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    /// Spawn the background sweep loop, returning its join handle.
    pub fn start(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(self.interval);
            ticker.tick().await; // consume the immediate first tick
            let mut ticks: u64 = 0;
            loop {
                ticker.tick().await;
                if let Err(e) = self.dispatch_ready().await {
                    warn!(error = %e, "Task retry sweep failed");
                }
                if ticks % UNDISPATCHED_SWEEP_EVERY == 0 {
                    if let Err(e) = self.dispatch_undispatched().await {
                        warn!(error = %e, "Undispatched task sweep failed");
                    }
                }
                ticks = ticks.wrapping_add(1);
            }
        })
    }

    /// Dispatch every task retry whose backoff has elapsed.
    async fn dispatch_ready(&self) -> OrcherResult<()> {
        let ready = self.scheduler.find_ready_retries().await?;
        if ready.is_empty() {
            return Ok(());
        }
        debug!(count = ready.len(), "Dispatching ready task retries");

        for t in ready {
            // The same payload the initial attempt is dispatched with, so the task
            // poller can both match (task_queue/namespace) and rebuild (task_type/input/
            // timeouts/retry_policy) the task. `attempt_number` makes the outbox idempotency
            // key distinct from the original dispatch and from other attempts.
            let payload = WorkflowCoordinator::task_dispatch_payload(&t);

            match self
                .coordinator
                .dispatch_task(
                    t.workflow_id.as_uuid(),
                    OutboxTaskType::DispatchTask,
                    payload,
                )
                .await
            {
                Ok(_) => {
                    // Record the hand-over so the next sweep can move past this
                    // row and see the rest of the backlog. Without it the same
                    // hundred rows are re-read every tick and anything newer
                    // never gets dispatched at all.
                    if let Err(e) = self.scheduler.mark_dispatched(&t.id).await {
                        warn!(
                            task_id = %t.task_id,
                            error = %e,
                            "Dispatched the retry but could not record it; the next \
                             sweep will consider this row again"
                        );
                    }
                    info!(
                        task_id = %t.task_id,
                        attempt = t.attempt_number,
                        "Dispatched task retry to worker"
                    )
                }
                Err(e) => error!(
                    task_id = %t.task_id,
                    attempt = t.attempt_number,
                    error = %e,
                    "Failed to dispatch task retry"
                ),
            }
        }
        Ok(())
    }

    /// Dispatch every first attempt that was scheduled and never dispatched.
    ///
    /// Each is claimed on its row before its dispatch is written, so a row
    /// that another node's sweep, or a replay of its workflow, is dispatching
    /// at the same moment is dispatched by one of them.
    async fn dispatch_undispatched(&self) -> OrcherResult<()> {
        let found = self
            .scheduler
            .find_undispatched_first_attempts(
                UNDISPATCHED_GRACE,
                self.coordinator.undispatched_repair_cutoff(),
            )
            .await?;
        for t in found {
            match self.coordinator.dispatch_if_never_dispatched(&t).await {
                Ok(true) => warn!(
                    workflow_id = %t.workflow_id,
                    execution_id = %t.execution_id,
                    task_id = %t.task_id,
                    scheduled_at = %t.scheduled_at,
                    "Dispatched a task that was scheduled and never dispatched"
                ),
                Ok(false) => debug!(
                    task_id = %t.task_id,
                    "Undispatched task already had its dispatch, or another engine made it first"
                ),
                Err(e) => error!(
                    task_id = %t.task_id,
                    error = %e,
                    "Failed to dispatch an undispatched task"
                ),
            }
        }
        Ok(())
    }
}
