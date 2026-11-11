//! Handing out a delayed start's run once it is due.
//!
//! A start with a `start_delay` writes its run at once — its id taken, its
//! workflow id held, its dispatch and first journal entry written in the same
//! transaction as any other start — but marks it as awaiting its start
//! (`awaiting_start`, due at `start_after`, V069). No claim hands out a run
//! that is: the predicate is on every claim statement, and the claim indexes
//! leave such runs out, so a queue holding many of them costs a poll nothing.
//!
//! [`DelayedStarts`] is what lets them go. Each pass releases the runs that
//! are due, earliest first, and wakes the workflow polls waiting on their
//! queues; it then sleeps until the next one is due, as the database knows
//! it, but no longer than [`MAX_SLEEP`], and is woken early by a delayed
//! start made on this engine, which may be due sooner than what it was
//! sleeping towards. A run is so handed out within a few milliseconds of
//! being due when this engine started it or knew of it, and within
//! [`MAX_SLEEP`] when another engine started it since this one last looked.
//!
//! The run's own dispatch is written to fire when the run is due as well:
//! when it does, a worker already waiting is handed the run directly, as it
//! is for any start. Whichever of the two comes first, the release is what
//! makes the run claimable, and a claim is what hands it out, so it is
//! handed out once.
//!
//! # Alongside the other engines
//!
//! Every engine runs this. A release takes each run once — rows another
//! engine is releasing at the same moment are skipped, not waited for — and
//! with clustering on, each engine releases only the runs on its own shards,
//! whose polls it serves and can wake. A run whose engine is gone is released
//! by the engine that takes its shard over.
//!
//! # What a delay is not
//!
//! The run's clocks start when it is due: its run timeout is measured from
//! `start_after`, and a start's execution deadline is fixed from it, so a
//! delay is spent from neither. A delayed run is open from the moment it is
//! written — it holds its workflow id, and can be cancelled or terminated,
//! which ends it without its ever being handed out.

use std::sync::Arc;
use std::time::Duration;

use orcher_common::types::time::Timestamptz;
use orcher_common::OrcherResult;
use orcher_storage::repositories::domains::workflow_execution::{
    DueStart, WorkflowExecutionRepository,
};
use tokio::sync::Notify;
use tracing::{debug, warn};

use super::PollingManager;

/// The longest a pass sleeps: how late a run another engine started can be
/// handed out, at worst, after it is due.
pub const MAX_SLEEP: Duration = Duration::from_secs(1);

/// How many due runs one release takes, per database.
pub const RELEASE_BATCH: i64 = 500;

/// Releases delayed starts as they come due; see the module documentation.
pub struct DelayedStarts {
    executions: Arc<WorkflowExecutionRepository>,
    polling: Arc<PollingManager>,
    wake: Arc<Notify>,
}

impl DelayedStarts {
    /// Release the delayed runs of `executions` as they come due, waking
    /// their queues through `polling`. `wake` is notified by the start of a
    /// delayed run on this engine (`WorkflowExecutionService::delayed_start_wake`).
    pub fn new(
        executions: Arc<WorkflowExecutionRepository>,
        polling: Arc<PollingManager>,
        wake: Arc<Notify>,
    ) -> Self {
        Self {
            executions,
            polling,
            wake,
        }
    }

    /// Release every delayed run that is due, and wake the queues they are on.
    /// Returns the runs released.
    pub async fn release_due(&self) -> OrcherResult<Vec<DueStart>> {
        let mut released = Vec::new();
        loop {
            let batch = self.executions.release_due_starts(RELEASE_BATCH).await?;
            let full = batch.len() as i64 >= RELEASE_BATCH;
            released.extend(batch);
            if !full {
                break;
            }
        }
        let mut woken: Vec<(&str, &str)> = released
            .iter()
            .map(|due| (due.task_queue.as_str(), due.namespace.as_str()))
            .collect();
        woken.sort_unstable();
        woken.dedup();
        for (task_queue, namespace) in woken {
            self.polling
                .notify_work_available(task_queue, namespace)
                .await;
        }
        if !released.is_empty() {
            debug!(
                count = released.len(),
                "Released delayed starts that came due"
            );
        }
        Ok(released)
    }

    /// How long to sleep before the next pass: until the next delayed run is
    /// due, at most [`MAX_SLEEP`].
    async fn sleep_for(&self) -> Duration {
        match self.executions.next_start_due().await {
            Ok(Some(due)) => {
                let wait_ms = due.timestamp_millis() - Timestamptz::now().timestamp_millis();
                Duration::from_millis(wait_ms.max(0) as u64).min(MAX_SLEEP)
            }
            Ok(None) => MAX_SLEEP,
            Err(e) => {
                warn!(error = %e, "Could not read when the next delayed start is due");
                MAX_SLEEP
            }
        }
    }

    /// Release delayed starts as they come due until `stop` completes.
    pub async fn run(self: Arc<Self>, stop: impl std::future::Future<Output = ()>) {
        tokio::pin!(stop);
        loop {
            if let Err(e) = self.release_due().await {
                warn!(error = %e, "Releasing due delayed starts failed; trying again");
            }
            let sleep = self.sleep_for().await;
            tokio::select! {
                _ = tokio::time::sleep(sleep) => {}
                _ = self.wake.notified() => {}
                _ = &mut stop => break,
            }
        }
    }
}
