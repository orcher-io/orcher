//! Releasing the claims of a worker process that died.
//!
//! A worker process that dies while it holds an activation — killed while
//! reporting a task's result, say, just after the engine handed it the
//! workflow's next step — never answers it. The workflow used to stay claimed
//! by the dead process until the claim timed out, five minutes in production,
//! and only then could another worker carry it on. The task attempts it was
//! running waited out their heartbeat timeouts the same way.
//!
//! Every engine counts the polls that are open on it, by the process each
//! names ([`WorkerLiveness`]), and every [`refresh`](LivenessSettings::refresh)
//! [`record`](LapsedWorkerClaims::record)s the processes it has seen, in the
//! primary database where every engine sees them. A process nobody has seen
//! for [`missed_refreshes`](LivenessSettings::missed_refreshes) refreshes has
//! lapsed, and [`sweep`](LapsedWorkerClaims::sweep), run by every engine,
//! releases what it held:
//!
//! - each workflow claim recorded against it whose activation is still out is
//!   released and voided, on whichever shard it lives, and offered to this
//!   engine's pollers; see
//!   `WorkflowExecutionRepository::release_claims_of_lapsed_workers` for what
//!   is released and why a completion from the process, if it was alive after
//!   all, is then acknowledged without being applied;
//! - each task attempt it was running is timed out as a heartbeat timeout
//!   would time it out, but only once the attempt's own heartbeats have
//!   stopped too; see `TimeoutWatcher::time_out_tasks_of_lapsed_workers`.
//!
//! A process is known within the organization and namespace its polls were
//! made for, and only claims in those are released on its account, so a
//! process of another tenant that sends the same instance id neither keeps it
//! alive nor has its claims released.
//!
//! # Alongside the other recoveries
//!
//! Each of them clears a claim with a statement that only acts on a claim
//! still held as it found it, so whichever comes first — this, the claim
//! timeout's reclaimer, the release of a dead engine's claims — takes it and
//! the others find nothing; nothing is handed out twice, because handing out
//! is a claim, and a claim is taken by one poll. A parked workflow, its
//! activation completed, is never released here, so the sweep for lost
//! wake-ups, which only looks at parked workflows, never meets this one. An
//! attempt handed out eagerly, with a workflow completion, records no process
//! (the completion names none) and is left to its eager claim's TTL and its
//! timeouts. A task attempt is timed out through the same update the timeout
//! watcher uses, which acts only on an attempt still running, so the two
//! cannot both time it out.
//!
//! # When an engine may judge
//!
//! An engine that has just started has not seen anyone yet, and after an
//! outage of every engine, every process's last sighting is old. So an engine
//! sweeps only once it has itself recorded what it sees, without a failure,
//! for a whole lapse: by then every live process has polled some engine, and
//! been recorded.
//!
//! Every engine must record, or the others take the workers polling it for
//! dead. An engine configured not to release still records
//! ([`releasing`](LapsedWorkerClaims::releasing)), and every engine of a
//! deployment should run with the same refresh and lapse.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use orcher_common::OrcherResult;
use orcher_storage::repositories::domains::worker::WorkerInstanceRepository;
use orcher_storage::repositories::domains::workflow_execution::WorkflowExecutionRepository;
use tracing::{debug, info, warn};

use super::PollingManager;
use crate::retries::timeout_watcher::TimeoutWatcher;
use crate::worker::WorkerLiveness;

/// How often each engine records the processes it sees, by default.
pub const DEFAULT_REFRESH: Duration = Duration::from_secs(5);

/// How many refreshes a process may go unseen before it has lapsed, by
/// default: fifteen seconds, at the default refresh.
pub const DEFAULT_MISSED_REFRESHES: u32 = 3;

/// How worker processes are judged gone.
#[derive(Debug, Clone, Copy)]
pub struct LivenessSettings {
    /// How often each engine records the processes it has seen polling.
    pub refresh: Duration,
    /// How many refreshes a process may go unseen before what it holds is
    /// released. At least two, so that one late write is never a death.
    pub missed_refreshes: u32,
    /// The claim timeout. A process last seen longer ago than this (plus the
    /// lapse) held nothing that has not timed out already.
    pub claim_timeout_secs: i64,
}

impl LivenessSettings {
    /// The defaults, under `claim_timeout_secs`.
    pub fn new(claim_timeout_secs: i64) -> Self {
        Self {
            refresh: DEFAULT_REFRESH,
            missed_refreshes: DEFAULT_MISSED_REFRESHES,
            claim_timeout_secs,
        }
    }

    fn refresh_secs(&self) -> i64 {
        self.refresh.as_secs().max(1) as i64
    }

    /// How long a process goes unseen before it has lapsed.
    pub fn lapse_secs(&self) -> i64 {
        self.refresh_secs() * i64::from(self.missed_refreshes.max(2))
    }
}

/// What one sweep released.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LapsedSwept {
    /// Processes found lapsed.
    pub workers: usize,
    /// Workflow claims released, each queued as a continuation here.
    pub workflows: usize,
    /// Task attempts timed out.
    pub tasks: usize,
}

#[derive(Debug)]
struct Recording {
    /// When the last successful record began; what it reported was seen
    /// since the one before.
    since: Instant,
    /// Since when every record has succeeded, if the last one did.
    watching_since: Option<Instant>,
}

/// Records which worker processes this engine sees and releases what the
/// ones nobody sees any more held.
pub struct LapsedWorkerClaims {
    liveness: Arc<WorkerLiveness>,
    instances: WorkerInstanceRepository,
    executions: Arc<WorkflowExecutionRepository>,
    polling_manager: Arc<PollingManager>,
    tasks: Option<TimeoutWatcher>,
    settings: LivenessSettings,
    releasing: bool,
    recording: Mutex<Recording>,
}

impl LapsedWorkerClaims {
    /// Record what `liveness` sees in `instances` — the primary database —
    /// and release on `executions`, across every shard it reaches, what the
    /// processes nobody sees held, offering it to `polling_manager`'s pollers.
    pub fn new(
        liveness: Arc<WorkerLiveness>,
        instances: WorkerInstanceRepository,
        executions: Arc<WorkflowExecutionRepository>,
        polling_manager: Arc<PollingManager>,
        settings: LivenessSettings,
    ) -> Self {
        Self {
            liveness,
            instances,
            executions,
            polling_manager,
            tasks: None,
            settings,
            releasing: true,
            recording: Mutex::new(Recording {
                since: Instant::now(),
                watching_since: None,
            }),
        }
    }

    /// Also time out the task attempts the lapsed processes were running,
    /// through `watcher`.
    pub fn with_tasks(mut self, watcher: TimeoutWatcher) -> Self {
        self.tasks = Some(watcher);
        self
    }

    /// Whether this engine releases what lapsed processes held, or only
    /// records the processes it sees for the engines that do. On by default.
    pub fn releasing(mut self, releasing: bool) -> Self {
        self.releasing = releasing;
        self
    }

    /// The settings this runs under.
    pub fn settings(&self) -> LivenessSettings {
        self.settings
    }

    /// Write the processes seen polling this engine since the last record.
    /// Returns how many.
    pub async fn record(&self) -> OrcherResult<usize> {
        let began = Instant::now();
        let since = self.lock().since;
        let seen = self.liveness.take_seen(since);
        match self.instances.record_seen(&seen).await {
            Ok(()) => {
                let mut recording = self.lock();
                recording.since = began;
                recording.watching_since.get_or_insert(began);
                Ok(seen.len())
            }
            Err(e) => {
                // What was taken is still in `liveness` (a process is
                // forgotten only once it has gone unseen since `since`), and
                // `since` stays, so the next record writes it. Until then
                // other engines may not know this one's processes are alive,
                // and neither does this one: it starts watching afresh.
                self.lock().watching_since = None;
                Err(e)
            }
        }
    }

    /// Release what lapsed processes held, if this engine has watched long
    /// enough to judge. Safe to run on every engine at once and on every
    /// tick: each claim is released once, by one engine.
    pub async fn sweep(&self) -> OrcherResult<LapsedSwept> {
        if !self.releasing {
            return Ok(LapsedSwept::default());
        }
        let lapse_secs = self.settings.lapse_secs();
        let watched_long_enough = self
            .lock()
            .watching_since
            .is_some_and(|since| since.elapsed() >= Duration::from_secs(lapse_secs as u64));
        if !watched_long_enough {
            return Ok(LapsedSwept::default());
        }
        let lookback_secs = self.settings.claim_timeout_secs + lapse_secs;
        if let Err(e) = self.instances.forget_older_than(lookback_secs).await {
            debug!(error = %e, "Could not forget old worker processes; trying again next sweep");
        }
        let lapsed = self.instances.lapsed(lapse_secs, lookback_secs).await?;
        if lapsed.is_empty() {
            return Ok(LapsedSwept::default());
        }

        // Every claim the process made was made by a poll that was recorded,
        // by the next record at the latest; a refresh is the margin for that.
        let released = self
            .executions
            .release_claims_of_lapsed_workers(&lapsed, self.settings.refresh_secs())
            .await?;
        for claim in &released {
            // No preferred worker: the one that ran it last is gone. Pollers
            // here are woken for it; pollers elsewhere find it by their scan.
            self.polling_manager.push_continuation(
                &claim.task_queue,
                &claim.namespace,
                &claim.execution_id,
                None,
            );
            self.polling_manager
                .notify_work_available(&claim.task_queue, &claim.namespace)
                .await;
        }
        if !released.is_empty() {
            info!(
                released = released.len(),
                "Handed out again the workflows a dead worker process left claimed"
            );
        }

        let tasks = match &self.tasks {
            Some(watcher) => watcher
                .time_out_tasks_of_lapsed_workers(&lapsed, lapse_secs)
                .await
                .unwrap_or_else(|e| {
                    warn!(error = %e, "Could not time out the tasks of dead worker processes");
                    0
                }),
            None => 0,
        };

        Ok(LapsedSwept {
            workers: lapsed.len(),
            workflows: released.len(),
            tasks,
        })
    }

    /// Record, then sweep: one tick of [`run`](Self::run).
    pub async fn tick(&self) -> LapsedSwept {
        if let Err(e) = self.record().await {
            warn!(error = %e, "Could not record the worker processes seen polling");
            return LapsedSwept::default();
        }
        match self.sweep().await {
            Ok(swept) => swept,
            Err(e) => {
                warn!(error = %e, "Could not release the claims of dead worker processes");
                LapsedSwept::default()
            }
        }
    }

    /// Tick every refresh until `stop` resolves.
    pub async fn run(self: Arc<Self>, stop: impl std::future::Future<Output = ()>) {
        let mut every = tokio::time::interval(self.settings.refresh);
        every.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tokio::pin!(stop);
        loop {
            tokio::select! {
                _ = every.tick() => {
                    // Bounded, so a database slow enough to stall it delays
                    // the next tick rather than piling ticks up behind it.
                    if tokio::time::timeout(self.settings.refresh * 2, self.tick()).await.is_err() {
                        warn!("Worker liveness tick took longer than two refreshes; abandoned");
                    }
                }
                _ = &mut stop => break,
            }
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Recording> {
        self.recording
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}
