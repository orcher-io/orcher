//! Workflow task polling from database.

use orcher_common::OrcherResult;
use orcher_dispatch::polling::prefetch::PrefetchKey;
use orcher_storage::repositories::domains::workflow_execution::HandOut;
use orcher_storage::repositories::domains::{
    journal::JournalRepository, task_execution::TaskExecutionRepository,
    workflow_execution::WorkflowExecutionRepository,
};
use std::sync::Arc;

use tokio::sync::Notify;
use tokio::time::{Duration, Instant};
use tracing::{debug, error, info, trace, warn};

use super::converter::Converter;
use super::waiters::WaitersRegistry;
use orcher_common::types::dispatch::polling::{
    PollExecutionRequest, PollExecutionResponse, QueueStats,
};
use orcher_dispatch::polling::claimer::Claimer;
use orcher_dispatch::polling::notifier::Notifier;
use orcher_dispatch::polling::reclaimer::Reclaimer;
use orcher_dispatch::routing::{WorkerAffinityConfig, WorkerAffinityManager};
use orcher_storage::repositories::domains::matching::MatchingStore;

// Redis queue integration for fast workflow matching

// Redis Streams and sync-match support
use crate::engine::state::high_perf_cache::HighPerfCache;

/// Polling manager for orchestrating workflow execution polling
///
/// Coordinates long-polling, claim operations, notifications, and crash recovery
/// to provide efficient work distribution to services.
///
/// Supports both Redis-based matching (fast path) and PostgreSQL-based polling (fallback).
pub struct PollingManager {
    /// Claimer for atomic claim operations
    claimer: Claimer,

    /// Notifier for waking waiting services
    notifier: Notifier,

    /// Converter for DB → response type conversion
    converter: Converter,

    /// For each run, the worker process its last activation's journal went
    /// to and the last entry it carried, so a next activation for the same
    /// process can carry only the entries after it. Bounded: a run left out
    /// is sent its whole journal, which is always correct.
    history_sent: moka::future::Cache<String, (String, i64)>,

    /// How long a continuation is held for the worker process that keeps its
    /// run's journal before another may take it. The claim enforces the same
    /// hold (`preferred_until`); this keeps the queue from offering an entry
    /// another process's poll could not claim yet.
    journal_hold: Duration,

    /// Execution repository for DB access
    execution_repo: Arc<WorkflowExecutionRepository>,

    /// Claim timeout in seconds
    claim_timeout_secs: i64,

    /// Shutdown signal for reclaimer
    shutdown: Arc<Notify>,

    /// Redis Streams client for consumer groups
    #[allow(dead_code)]

    /// Consumer group manager for pending message recovery (Redis Streams)

    /// Waiters registry for sync-match
    waiters_registry: Option<Arc<WaitersRegistry>>,

    /// Workflow state cache for fast-path optimization
    state_cache: Option<Arc<HighPerfCache>>,

    /// Work prefetch buffer for batch claiming (optional)
    /// When enabled, batch-claims multiple executions and distributes
    /// them to subsequent poll requests, reducing DB contention.
    prefetch_buffer: Option<Arc<orcher_dispatch::polling::prefetch::WorkPrefetchBuffer>>,

    /// Task execution repository for eager-claim TTL reclamation in the reclaimer.
    task_execution_repo: Option<Arc<TaskExecutionRepository>>,

    /// Direct dispatch queue for workflow continuations (sync-match).
    /// When a task completes and the workflow is re-queued, the execution_id
    /// is pushed here instead of going through Redis. The poller picks it up
    /// directly, skipping Redis round-trip and the expensive claim scan query.
    ///
    /// Shared with the reclaimer, whose wake-up sweep queues continuations too.
    continuation_queue: Arc<ContinuationQueue>,

    /// The matching partitions, whose expired leases the reclaimer returns.
    /// Set once the matching layer is wired, which is after the reclaimer
    /// starts. Unset ⇒ there are none.
    matching_store: Arc<std::sync::OnceLock<Arc<dyn MatchingStore>>>,

    /// Worker affinity manager for routing consecutive workflow tasks to the same worker.
    /// Improves cache locality on HighPerfCache and reduces state reconstruction overhead.
    affinity_manager: Option<Arc<WorkerAffinityManager>>,

    /// The workers that have said they are shutting down, whose polls are
    /// answered with nothing. Unset ⇒ no poll is refused.
    worker_shutdowns: std::sync::OnceLock<Arc<crate::worker::WorkerShutdowns>>,

    /// How many claim queries the long-poll loop has run, for telling whether
    /// something woke it for nothing.
    claim_scans: std::sync::atomic::AtomicU64,

    /// How long a waiting poll goes between looks when nothing wakes it.
    /// A wake is what answers a poll promptly; this only bounds how late a
    /// poll finds work whose wake it missed, at a scan per waiting poll per
    /// interval.
    retry_interval: Duration,
}

/// A workflow poll, as far as worker shutdowns are concerned.
fn shutdown_poll(request: &PollExecutionRequest) -> crate::worker::shutdowns::Poll<'_> {
    crate::worker::shutdowns::Poll {
        organization_id: request.organization_id,
        namespace: &request.namespace,
        identity: &request.identity,
        worker_instance_id: request.worker_instance_id.as_deref().unwrap_or_default(),
        task_queue: &request.task_queue,
    }
}

/// Queued workflow continuations, keyed by `"task_queue:namespace"`.
type ContinuationQueue = dashmap::DashMap<String, std::collections::VecDeque<ContinuationEntry>>;

/// Queue a continuation for `execution_id`.
fn queue_continuation(
    queue: &ContinuationQueue,
    task_queue: &str,
    namespace: &str,
    execution_id: &str,
    preferred_worker: Option<String>,
) {
    queue
        .entry(format!("{}:{}", task_queue, namespace))
        .or_default()
        .push_back(ContinuationEntry {
            execution_id: execution_id.to_string(),
            preferred_worker,
            queued_at: Instant::now(),
            owner: None,
            marked_at: None,
            skips: 0,
        });
}

/// Entry in the continuation queue, optionally held for a worker process.
#[derive(Debug, Clone)]
struct ContinuationEntry {
    execution_id: String,
    /// The worker process that keeps the run's journal
    /// ([`PollingManager::journal_holder`]): its polls take the entry first,
    /// and others only once the hold has passed.
    preferred_worker: Option<String>,
    /// When it was queued, which the hold counts from.
    queued_at: Instant,
    /// The organization the execution belongs to, once a poll of another one
    /// has popped it and found out; `None` until then. Polls that may not
    /// claim it pass it over, so it waits for one of its own organization's.
    owner: Option<Option<uuid::Uuid>>,
    /// When the owner was found out.
    marked_at: Option<Instant>,
    /// How many polls have passed over it since.
    skips: u32,
}

/// How many times polls may pass over a continuation marked for another
/// organization, and for how long, before it is dropped. Its owner may never
/// poll on this replica, and a marked entry would otherwise stay for good; the
/// continuation only spares a scan, and the owner's poll finds the workflow by
/// the scan anyway. Every pass of a waiting poll counts, five a second for an
/// idle one, so the count is generous and the age is what usually ends it.
pub const MARKED_CONTINUATION_SKIPS: u32 = 50;
const MARKED_CONTINUATION_AGE: Duration = Duration::from_secs(30);

/// A waiting poll's periodic look, for work whose wake it missed. Matches the
/// task polls' interval, and keeps an idle poll at five scans a second.
const DEFAULT_RETRY_INTERVAL: Duration = Duration::from_millis(200);

impl ContinuationEntry {
    /// Whether this entry has waited as long as a marked one may.
    fn is_stale(&self) -> bool {
        self.marked_at.is_some_and(|at| {
            self.skips >= MARKED_CONTINUATION_SKIPS || at.elapsed() >= MARKED_CONTINUATION_AGE
        })
    }

    /// Whether a poll made for `organization_id` may take this entry.
    fn for_poll(&self, organization_id: Option<uuid::Uuid>) -> bool {
        match (self.owner, organization_id) {
            (Some(owner), Some(poll)) => owner == Some(poll),
            _ => true,
        }
    }
}

impl PollingManager {
    /// Create a new polling manager
    ///
    /// # Arguments
    ///
    /// * `execution_repo` - Repository for execution persistence
    /// * `journal_repo` - Repository for journal entries
    /// * `redis_queue` - Optional Redis queue for fast workflow matching
    /// * `claim_timeout_secs` - How long claims are valid (e.g., 300 = 5 minutes)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // With Redis matching
    /// let manager = PollingManager::new(
    ///     execution_repo,
    ///     journal_repo,
    ///     Some(redis_queue),
    ///     300
    /// );
    ///
    /// // Without Redis (fallback to PostgreSQL)
    /// let manager = PollingManager::new(execution_repo, journal_repo, 300);
    /// ```
    pub fn new(
        execution_repo: Arc<WorkflowExecutionRepository>,
        journal_repo: Arc<JournalRepository>,
        claim_timeout_secs: i64,
    ) -> Self {
        info!(
            claim_timeout_secs = claim_timeout_secs,
            "Creating polling manager"
        );

        let notifier = Notifier::new();
        let claimer = Claimer::new(Arc::clone(&execution_repo), claim_timeout_secs);
        let converter = Converter::new(journal_repo);

        Self {
            claimer,
            notifier,
            converter,
            history_sent: history_sent_record(),
            journal_hold:
                orcher_storage::repositories::domains::workflow_execution::DEFAULT_JOURNAL_HOLD,
            execution_repo,
            claim_timeout_secs,
            shutdown: Arc::new(Notify::new()),
            waiters_registry: None,
            state_cache: None,
            prefetch_buffer: None,
            task_execution_repo: None,
            continuation_queue: Arc::new(dashmap::DashMap::new()),
            matching_store: Arc::new(std::sync::OnceLock::new()),
            affinity_manager: None,
            worker_shutdowns: std::sync::OnceLock::new(),
            claim_scans: std::sync::atomic::AtomicU64::new(0),
            retry_interval: DEFAULT_RETRY_INTERVAL,
        }
    }

    /// Answer the polls of workers that have said they are shutting down with
    /// nothing. A second call is ignored.
    pub fn set_worker_shutdowns(&self, shutdowns: Arc<crate::worker::WorkerShutdowns>) {
        let _ = self.worker_shutdowns.set(shutdowns);
    }

    /// Whether the worker sending `request` has said it is shutting down.
    pub fn is_shut_down(&self, request: &PollExecutionRequest) -> bool {
        self.worker_shutdowns
            .get()
            .is_some_and(|shutdowns| shutdowns.is_shut_down(&shutdown_poll(request)))
    }

    /// How many claim queries the long-poll loop has run so far.
    pub fn claim_scans(&self) -> u64 {
        self.claim_scans.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// The matching partitions tasks are delivered from, so that the
    /// reclaimer returns the rows whose lease expired before their task was
    /// taken. A second call is ignored.
    pub fn set_matching_store(&self, store: Arc<dyn MatchingStore>) {
        let _ = self.matching_store.set(store);
    }

    /// Attach a task execution repository for eager-claim TTL reclamation.
    pub fn with_task_repo(mut self, repo: Arc<TaskExecutionRepository>) -> Self {
        self.task_execution_repo = Some(repo);
        self
    }

    /// Wait at most `timeout` for a claim query before the poll fails with a
    /// timeout. Without this, it waits 2 seconds.
    pub fn with_claim_query_timeout(mut self, timeout: Duration) -> Self {
        self.claimer = Claimer::with_query_timeout(
            Arc::clone(&self.execution_repo),
            self.claim_timeout_secs,
            u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX),
        );
        self
    }

    /// Create with Redis Streams support
    ///
    /// Enables consumer group-based polling with automatic load balancing.
    /// Create with sync-match support
    ///
    /// Enables direct worker dispatch with sub-millisecond latency.
    pub fn with_sync_match(
        execution_repo: Arc<WorkflowExecutionRepository>,
        journal_repo: Arc<JournalRepository>,
        state_cache: Arc<HighPerfCache>,
        routing_config: orcher_common::types::dispatch::routing::RoutingConfig,
    ) -> Self {
        info!(
            claim_timeout_secs = routing_config.claim_timeout_secs,
            max_waiters_per_queue = routing_config.max_waiters_per_queue,
            "Creating polling manager with sync-match"
        );

        let notifier = Notifier::new();
        let claimer = Claimer::new(
            Arc::clone(&execution_repo),
            routing_config.claim_timeout_secs,
        );
        // Use cache-enabled converter for incremental journal loading
        let converter = Converter::with_cache(journal_repo, state_cache.clone());
        let waiters_registry = Arc::new(WaitersRegistry::new(routing_config.max_waiters_per_queue));

        let affinity_manager = if routing_config.affinity.enabled {
            let config = WorkerAffinityConfig {
                enabled: true,
                default_lease: Duration::from_secs(
                    routing_config.affinity.default_lease_secs as u64,
                ),
                max_lease: Duration::from_secs(routing_config.affinity.max_lease_secs as u64),
                cleanup_interval: Duration::from_secs(30),
            };
            info!(
                default_lease_secs = routing_config.affinity.default_lease_secs,
                max_lease_secs = routing_config.affinity.max_lease_secs,
                "Worker affinity enabled"
            );
            Some(Arc::new(WorkerAffinityManager::new(config)))
        } else {
            None
        };

        Self {
            claimer,
            notifier,
            converter,
            history_sent: history_sent_record(),
            journal_hold:
                orcher_storage::repositories::domains::workflow_execution::DEFAULT_JOURNAL_HOLD,
            execution_repo,
            claim_timeout_secs: routing_config.claim_timeout_secs,
            shutdown: Arc::new(Notify::new()),
            waiters_registry: Some(waiters_registry),
            state_cache: Some(state_cache),
            prefetch_buffer: None,
            task_execution_repo: None,
            continuation_queue: Arc::new(dashmap::DashMap::new()),
            matching_store: Arc::new(std::sync::OnceLock::new()),
            affinity_manager,
            worker_shutdowns: std::sync::OnceLock::new(),
            claim_scans: std::sync::atomic::AtomicU64::new(0),
            retry_interval: DEFAULT_RETRY_INTERVAL,
        }
    }

    /// Enable prefetch buffer for batch claiming
    ///
    /// When enabled, the polling manager batch-claims multiple executions
    /// and distributes them to subsequent poll requests, dramatically
    /// reducing database contention.
    ///
    /// # Arguments
    ///
    /// * `config` - Prefetch configuration (batch size, TTL, etc.)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let manager = PollingManager::new(...)
    ///     .with_prefetch(PrefetchConfig::default());
    /// ```
    pub fn with_prefetch(
        mut self,
        config: orcher_dispatch::polling::prefetch::PrefetchConfig,
    ) -> Self {
        info!(
            batch_size = config.batch_size,
            "Enabling prefetch buffer for batch claiming"
        );
        self.prefetch_buffer = Some(Arc::new(
            orcher_dispatch::polling::prefetch::WorkPrefetchBuffer::with_config(config),
        ));
        self
    }

    /// Sets how long a waiting poll goes between looks when nothing wakes
    /// it (default 200ms, matching the task polls').
    pub fn with_retry_interval(mut self, interval: Duration) -> Self {
        self.retry_interval = interval;
        self
    }

    /// Enable prefetch buffer with default configuration
    pub fn with_default_prefetch(self) -> Self {
        self.with_prefetch(orcher_dispatch::polling::prefetch::PrefetchConfig::default())
    }

    /// Get prefetch buffer statistics (if enabled)
    pub fn prefetch_stats(&self) -> Option<orcher_dispatch::polling::prefetch::PrefetchStats> {
        self.prefetch_buffer.as_ref().map(|b| b.stats())
    }

    /// Start the polling manager
    ///
    /// Starts the background reclaimer task that handles crash recovery.
    /// This should be called once at startup.
    ///
    /// # Returns
    ///
    /// A handle to the reclaimer task
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let manager = PollingManager::new(...);
    /// let reclaimer_handle = manager.start().await;
    ///
    /// // Manager is now ready to handle polls
    /// ```
    pub async fn start(&self) -> tokio::task::JoinHandle<OrcherResult<()>> {
        info!("Starting polling manager");

        // Start reclaimer background task
        let mut reclaimer = Reclaimer::new(Arc::clone(&self.execution_repo), self.notifier.clone());
        if let Some(ref task_repo) = self.task_execution_repo {
            reclaimer = reclaimer.with_task_repo(Arc::clone(task_repo));
        }
        if let Some(ref affinity) = self.affinity_manager {
            reclaimer = reclaimer.with_affinity(Arc::clone(affinity));
        }
        let queue = Arc::clone(&self.continuation_queue);
        reclaimer = reclaimer
            .with_wake_sweep(
                Arc::new(
                    move |task_queue: &str, namespace: &str, execution_id: &str| {
                        // No preferred worker: the one that ran the last
                        // activation may be gone, as the wake-up was.
                        queue_continuation(&queue, task_queue, namespace, execution_id, None)
                    },
                ),
                self.claim_timeout_secs,
            )
            .with_matching(Arc::clone(&self.matching_store));

        let handle = tokio::spawn(async move { reclaimer.start().await });

        info!("Polling manager started (reclaimer running)");
        handle
    }

    /// Get a shutdown handle
    ///
    /// Calling `notify_one()` on this handle will signal the manager to stop.
    pub fn shutdown_handle(&self) -> Arc<Notify> {
        Arc::clone(&self.shutdown)
    }

    /// Poll for workflow execution work (long-poll with notifications)
    ///
    /// Attempts to claim work from the specified task queue. If no work is
    /// immediately available, blocks up to `request.poll_timeout` waiting for
    /// work to become available via notifications.
    ///
    /// # Arguments
    ///
    /// * `request` - Poll request with task_queue, namespace, identity, timeout
    ///
    /// # Returns
    ///
    /// - `Ok(Some(response))` - Work was claimed and returned
    /// - `Ok(None)` - No work available (poll timeout)
    /// - `Err(_)` - Database or conversion error
    ///
    /// # ORCHER Terminology
    ///
    /// - "Service" polls for work (not "Worker")
    /// - "Execution" is claimed (not "Activation")
    /// - Long-poll blocks efficiently (no DB hammering)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let request = PollExecutionRequest::new(
    ///     "orders-queue",
    ///     "production",
    ///     "service-1",
    ///     Duration::from_secs(60),
    /// );
    ///
    /// match manager.poll_execution(request).await? {
    ///     Some(response) => {
    ///         // Execute workflow with response.journal
    ///         process_execution(response).await?;
    ///         manager.release_claim(&response.execution_id).await?;
    ///     }
    ///     None => {
    ///         // No work available, poll again
    ///     }
    /// }
    /// ```
    pub async fn poll_execution(
        &self,
        request: PollExecutionRequest,
    ) -> OrcherResult<Option<PollExecutionResponse>> {
        self.poll_execution_until(request, std::future::pending())
            .await
    }

    /// [`poll_execution`](Self::poll_execution), giving up waiting once `gone`
    /// completes — the poll it is for has ended. Only waiting is given up:
    /// a claim under way is finished and its activation returned, for the
    /// caller to deliver or [`hand_back`](Self::hand_back).
    pub async fn poll_execution_until(
        &self,
        request: PollExecutionRequest,
        gone: impl std::future::Future<Output = ()>,
    ) -> OrcherResult<Option<PollExecutionResponse>> {
        debug!(
            task_queue = %request.task_queue,
            namespace = %request.namespace,
            identity = %request.identity,
            poll_timeout_secs = request.poll_timeout.as_secs(),
            "poll_execution: starting"
        );

        let poll_start = std::time::Instant::now();

        // Fallback: PostgreSQL-based polling (slow path)
        let result = self.poll_with_postgres(&request, gone).await;

        debug!(
            task_queue = %request.task_queue,
            elapsed_ms = poll_start.elapsed().as_millis(),
            has_result = result.as_ref().map(|r| r.is_some()).unwrap_or(false),
            "poll_execution: complete"
        );

        result
    }

    /// Poll for work using PostgreSQL-based polling (fallback/slow path)
    ///
    /// This is the fallback mechanism when Redis is unavailable.
    /// Uses PostgreSQL SELECT FOR UPDATE SKIP LOCKED with in-memory notifications.
    ///
    /// ## Prefetch Buffer Optimization
    ///
    /// When prefetch buffer is enabled, this method:
    /// 1. First checks the prefetch buffer for cached executions (instant return)
    /// 2. If buffer empty, batch-claims multiple executions
    /// 3. Stores extras in buffer for subsequent polls
    /// 4. Falls back to single-claim if batch claim fails
    async fn poll_with_postgres(
        &self,
        request: &PollExecutionRequest,
        gone: impl std::future::Future<Output = ()>,
    ) -> OrcherResult<Option<PollExecutionResponse>> {
        tokio::pin!(gone);
        if self.is_shut_down(request) {
            return Ok(None);
        }
        let prefetch_enabled = self.prefetch_buffer.is_some();

        debug!(
            task_queue = %request.task_queue,
            timeout_secs = request.poll_timeout.as_secs(),
            prefetch_enabled = prefetch_enabled,
            "poll_with_postgres: starting"
        );

        // Try prefetch buffer first (fast path)
        if let Some(ref prefetch) = self.prefetch_buffer {
            // Check if we have a cached execution
            if let Some(execution) = self.take_prefetched(prefetch, request).await? {
                debug!(
                    execution_id = %execution.execution_id,
                    workflow_name = %execution.workflow_name,
                    task_queue = %request.task_queue,
                    "poll_with_postgres: prefetch hit"
                );

                // Convert to poll response
                let response = self.deliver(&execution, request).await?;
                return Ok(Some(response));
            }

            // Buffer empty - try batch claim with thundering herd prevention
            let key = prefetch_key(request);
            if prefetch.needs_refill(&key) {
                // Try to acquire the refill lock - only one worker will win
                if prefetch.try_start_refill(&key) {
                    // Released on drop, not by a call after the await: a poll
                    // is cancelled when its worker hangs up, and a cancelled
                    // future runs nothing after the await it was parked on.
                    // The flag then stayed set for good, and this queue never
                    // refilled its prefetch buffer again.
                    let refill = RefillGuard {
                        prefetch,
                        key: &key,
                    };
                    let batch_size = prefetch.batch_size();

                    debug!(
                        task_queue = %request.task_queue,
                        batch_size = batch_size,
                        "poll_with_postgres: prefetch miss, batch claiming"
                    );

                    // Batch claim with timeout
                    let claim_timeout = Duration::from_millis(5000); // Longer for batch
                    let batch_result = tokio::time::timeout(
                        claim_timeout,
                        self.claimer.claim_batch(
                            &request.task_queue,
                            &request.namespace,
                            &request.identity,
                            Some(batch_size),
                            request.version_id.as_deref(),
                            request.organization_id,
                            &request.step_ids,
                        ),
                    )
                    .await;

                    drop(refill);

                    match batch_result {
                        Ok(Ok(executions)) if !executions.is_empty() => {
                            // Add to buffer and get first execution
                            if let Some(execution) =
                                prefetch.add_batch(&key, &request.identity, executions)
                            {
                                debug!(
                                    execution_id = %execution.execution_id,
                                    workflow_name = %execution.workflow_name,
                                    "poll_with_postgres: batch claimed, returning first"
                                );

                                // Convert to poll response
                                let response = self.deliver(&execution, request).await?;
                                return Ok(Some(response));
                            }
                        }
                        Ok(Ok(_)) => {
                            trace!(
                                task_queue = %request.task_queue,
                                "poll_with_postgres: batch claim returned empty"
                            );
                        }
                        Ok(Err(e)) => {
                            warn!(
                                task_queue = %request.task_queue,
                                error = %e,
                                "poll_with_postgres: batch claim failed, falling back to single claim"
                            );
                        }
                        Err(_) => {
                            warn!(
                                task_queue = %request.task_queue,
                                "poll_with_postgres: batch claim timed out, falling back to single claim"
                            );
                        }
                    }
                } else {
                    // Another worker is already refilling - wait briefly then check buffer again
                    trace!(
                        task_queue = %request.task_queue,
                        "poll_with_postgres: lost refill race, waiting"
                    );

                    // Brief wait for the winning worker to complete batch claim
                    tokio::time::sleep(Duration::from_millis(50)).await;

                    // Check buffer again - it might have been filled by the other worker
                    if let Some(execution) = self.take_prefetched(prefetch, request).await? {
                        debug!(
                            execution_id = %execution.execution_id,
                            "poll_with_postgres: prefetch hit after wait"
                        );
                        let response = self.deliver(&execution, request).await?;
                        return Ok(Some(response));
                    }

                    trace!(
                        task_queue = %request.task_queue,
                        "poll_with_postgres: buffer still empty after wait, single claim"
                    );
                }
            }
        }

        // Fall through to original single-claim logic (or if prefetch is disabled)

        // Get or create notifier for this queue
        let notify = self
            .notifier
            .get_or_create(&request.task_queue, &request.namespace)
            .await;

        let deadline = Instant::now() + request.poll_timeout;
        let mut loop_count = 0u32;
        // Ends the wait below when a shutdown covering this poll is added.
        let shutdown = crate::worker::shutdowns::ShutdownWatch::new(
            self.worker_shutdowns.get().map(|shutdowns| &**shutdowns),
            shutdown_poll(request),
        );

        loop {
            loop_count += 1;

            // Listen before looking. Work is signalled with notify_waiters,
            // which reaches only polls already listening: a run made while
            // this pass claims, after its claim found nothing and before it
            // started to wait, would be missed, and the poll would find it
            // only at its periodic look. Enabled here, this pass hears it.
            let notified = notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            // A worker that has said it is shutting down is answered now, not
            // at the end of its poll timeout, and before it claims anything.
            if shutdown.is_shut_down() {
                debug!(
                    task_queue = %request.task_queue,
                    namespace = %request.namespace,
                    identity = %request.identity,
                    "poll_with_postgres: worker is shutting down, answering with no work"
                );
                return Ok(None);
            }

            // The first pass always looks, so a poll that may not wait at
            // all still takes work that is ready.
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() && loop_count > 1 {
                debug!(
                    task_queue = %request.task_queue,
                    loop_count = loop_count,
                    "poll_with_postgres: deadline reached"
                );
                return Ok(None);
            }

            // Fast path: check continuation queue first (workflow re-dispatch after task completion)
            // This avoids the expensive claim scan query by providing a targeted execution_id
            if let Some(execution_id) = self.try_pop_continuation_for(
                &request.task_queue,
                &request.namespace,
                request.worker_instance_id.as_deref().unwrap_or_default(),
                request.organization_id,
            ) {
                debug!(
                    execution_id = %execution_id,
                    task_queue = %request.task_queue,
                    "⚡ poll_with_postgres: continuation hit, targeted claim"
                );

                // Targeted claim by execution_id (indexed, fast)
                let claim_result = self
                    .claimer
                    .claim_by_execution_id(
                        &execution_id,
                        &request.identity,
                        request.version_id.as_deref(),
                        // A continuation queue is shared by every organization
                        // with a namespace of this name; one that is not the
                        // poll's is put back below for its own organization.
                        request.organization_id,
                        claimant_instance(request),
                        &request.step_ids,
                    )
                    .await;

                match claim_result {
                    Ok(Some(execution)) => {
                        debug!(
                            execution_id = %execution.execution_id,
                            workflow_name = %execution.workflow_name,
                            "⚡ poll_with_postgres: continuation claimed (skipped scan query)"
                        );
                        self.renew_affinity(&execution.execution_id, &request.identity);
                        let response = self.deliver(&execution, request).await?;
                        return Ok(Some(response));
                    }
                    Ok(None) => {
                        debug!(
                            execution_id = %execution_id,
                            "poll_with_postgres: continuation claim failed (already claimed), falling through"
                        );
                        // Refused because it belongs to another organization,
                        // it is still waiting: put it back for a poll of its
                        // own, which would otherwise find it only by a scan.
                        // Asked only of a poll made for an organization, the
                        // only kind the claim refuses for that.
                        if request.organization_id.is_some() {
                            match self.claimer.unclaimed_owner(&execution_id).await {
                                Ok(Some(owner)) if owner != request.organization_id => {
                                    self.return_continuation(
                                        &request.task_queue,
                                        &request.namespace,
                                        execution_id,
                                        owner,
                                    );
                                }
                                Ok(_) => {}
                                Err(e) => warn!(
                                    error = %e,
                                    "poll_with_postgres: could not tell whose continuation this is; \
                                     left to the scan"
                                ),
                            }
                        }
                        // Fall through to normal claim
                    }
                    Err(e) => {
                        warn!(
                            execution_id = %execution_id,
                            error = %e,
                            "poll_with_postgres: continuation claim error, falling through"
                        );
                    }
                }
            }

            // Check prefetch buffer again in the loop
            if let Some(ref prefetch) = self.prefetch_buffer {
                if let Some(execution) = self.take_prefetched(prefetch, request).await? {
                    debug!(
                        execution_id = %execution.execution_id,
                        "poll_with_postgres: prefetch hit in loop"
                    );
                    let response = self.deliver(&execution, request).await?;
                    return Ok(Some(response));
                }
            }

            let claim_timeout = Duration::from_millis(2000);
            let claim_start = std::time::Instant::now();
            self.claim_scans
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let claim_result = tokio::time::timeout(
                claim_timeout,
                self.claimer.claim_execution(
                    &request.task_queue,
                    &request.namespace,
                    &request.identity,
                    request.version_id.as_deref(),
                    request.organization_id,
                    claimant_instance(request),
                    &request.step_ids,
                ),
            )
            .await;

            match claim_result {
                Ok(Ok(Some(execution))) => {
                    debug!(
                        execution_id = %execution.execution_id,
                        workflow_name = %execution.workflow_name,
                        claim_ms = claim_start.elapsed().as_millis(),
                        "poll_with_postgres: claimed execution"
                    );
                    self.record_affinity(&execution.execution_id, &request.identity);
                    let response = self.deliver(&execution, request).await?;
                    return Ok(Some(response));
                }
                Ok(Ok(None)) => {
                    // No work available — fall through to wait
                }
                Ok(Err(e)) => {
                    error!(
                        task_queue = %request.task_queue,
                        error = %e,
                        "poll_with_postgres: claim failed"
                    );
                    return Err(e);
                }
                Err(_elapsed) => {
                    warn!(
                        task_queue = %request.task_queue,
                        timeout_ms = 2000,
                        "poll_with_postgres: claim query timed out"
                    );
                }
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Ok(None);
            }

            // Wait for a wake, or look again after the retry interval.
            let wait = std::cmp::min(remaining, self.retry_interval);
            tokio::select! {
                _ = &mut notified => {
                    trace!(
                        task_queue = %request.task_queue,
                        "poll_with_postgres: notification received, retrying"
                    );
                }
                _ = tokio::time::sleep(wait) => {
                    trace!(
                        task_queue = %request.task_queue,
                        "poll_with_postgres: periodic retry"
                    );
                }
                _ = shutdown.wait() => {}
                // Nobody is waiting for this poll any more. Between claims, so
                // it holds none.
                _ = &mut gone => return Ok(None),
            }
        }
    }

    /// The activation of `execution`, claimed for the poll `request`, as
    /// the poll returns it. Should building it fail, the claim is handed back
    /// rather than left with a poll that will never deliver it.
    ///
    /// It carries only the journal entries after those this run's last
    /// activation carried when that went to the same worker process, which
    /// keeps them (`history_cached`), and the run's step results are entries
    /// of its journal; otherwise the whole journal. A worker that does not
    /// hold them after all hands the activation back, and is sent the whole
    /// journal next time.
    ///
    /// What was sent is recorded as the activation is built, not once the
    /// worker has it. An activation that never reaches the worker (the poll
    /// ended, or it was too large to send) leaves a record ahead of what the
    /// worker holds, so its next activation is partial, misses and costs one
    /// `history_missing` round trip. Each node records only what it sent: one
    /// that sent less than another since can send entries the worker already
    /// holds, which it takes as missing in the same way. Either costs a round
    /// trip; neither can join a worker's entries to a wrong journal, since it
    /// joins only to exactly entries 1 to N.
    async fn deliver(
        &self,
        execution: &orcher_storage::schema::types::execution::WorkflowExecution,
        request: &PollExecutionRequest,
    ) -> OrcherResult<PollExecutionResponse> {
        let instance = request
            .worker_instance_id
            .as_deref()
            .filter(|_| request.history_cached && execution.step_results_journaled);
        let after = match instance {
            Some(instance) => match self.history_sent.get(&execution.execution_id).await {
                Some((sent_to, last)) if sent_to == instance => last,
                _ => 0,
            },
            None => 0,
        };
        match self
            .converter
            .to_poll_response_after(execution, after)
            .await
        {
            Ok(response) => {
                match instance {
                    Some(instance) => {
                        let last = response
                            .journal
                            .iter()
                            .map(|entry| entry.event_id)
                            .max()
                            .unwrap_or(after)
                            .max(after);
                        self.history_sent
                            .insert(execution.execution_id.clone(), (instance.to_string(), last))
                            .await;
                    }
                    None => self.history_sent.invalidate(&execution.execution_id).await,
                }
                Ok(response)
            }
            Err(e) => {
                self.hand_back(
                    &execution.execution_id,
                    execution.activation_seq,
                    &request.identity,
                )
                .await;
                Err(e)
            }
        }
    }

    /// Forget what was sent of `execution_id`'s journal: its worker did not
    /// hold it, and its next activation carries the whole journal.
    pub async fn forget_history_sent(&self, execution_id: &str) {
        self.history_sent.invalidate(execution_id).await;
    }

    /// Hand back activation `activation` of `execution_id`, claimed for the
    /// poll `identity` and never delivered to it: the poll ended first, or
    /// the activation could not be built. Its claim is released and its
    /// queue woken, so the next poll takes it now rather than once the claim
    /// times out. A claim no longer that one is left alone.
    pub async fn hand_back(&self, execution_id: &str, activation: i64, identity: &str) {
        match self
            .execution_repo
            .release_undelivered(execution_id, activation, identity)
            .await
        {
            Ok(Some((task_queue, namespace))) => {
                info!(
                    execution_id = %execution_id,
                    activation,
                    identity = %identity,
                    "Activation not delivered to its poll; handed back for the next one"
                );
                self.notify_work_available(&task_queue, &namespace).await;
            }
            Ok(None) => {}
            Err(e) => warn!(
                execution_id = %execution_id,
                error = %e,
                "Could not hand back an undelivered activation; it is released when its \
                 claim times out"
            ),
        }
    }

    /// Release a claim on an execution
    ///
    /// Should be called when:
    /// - Execution completes successfully
    /// - Execution fails
    /// - Service stops processing the execution
    ///
    /// # Arguments
    ///
    /// * `execution_id` - ID of the execution to release
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // After completing work
    /// manager.release_claim(&execution_id).await?;
    /// ```
    pub async fn release_claim(&self, execution_id: &str) -> OrcherResult<()> {
        debug!(execution_id = %execution_id, "Releasing claim");
        self.claimer.release_claim(execution_id).await
    }

    /// Take an execution from the prefetch buffer for the poller `identity`.
    ///
    /// A batch is claimed under the identity of whichever poller refilled the
    /// buffer, and each execution in it is then handed to whichever poller
    /// asks next — a different one, often on a different worker. The claim is
    /// moved to the poller it is actually handed to, so it names the worker
    /// that has the activation: a completion without a token is attributed to
    /// its activation by exactly that. One whose claim lapsed in the buffer
    /// and was taken by another claim is skipped, not handed out as well.
    async fn take_prefetched(
        &self,
        prefetch: &orcher_dispatch::polling::prefetch::WorkPrefetchBuffer,
        request: &PollExecutionRequest,
    ) -> OrcherResult<Option<orcher_storage::schema::types::execution::WorkflowExecution>> {
        let identity = request.identity.as_str();
        let key = prefetch_key(request);
        while let Some(execution) = prefetch.try_get(&key) {
            // Still claimed while it is handed out. Should that fail, or the
            // poll be dropped before it finishes, it goes back to the buffer
            // for the next poll: dropped, it would stay claimed by nobody
            // until the claim timed out.
            let held = Unhanded {
                prefetch,
                key: &key,
                execution: Some(execution),
            };
            let execution = held.execution.as_ref().expect("held until handed out");
            let handed = self
                .claimer
                .hand_to(
                    &execution.execution_id,
                    execution.activation_seq,
                    identity,
                    request.organization_id,
                    claimant_instance(request),
                )
                .await?;
            match handed {
                HandOut::Handed => {
                    let mut execution = held.taken();
                    execution.claimed_by = Some(identity.to_string());
                    return Ok(Some(execution));
                }
                // A completion of it is being applied. Still claimed for the
                // buffer: `held` puts it back for the next poll, which finds
                // the completion done. Not offered again to this poll, which
                // would only find it busy again.
                HandOut::Busy => return Ok(None),
                // Its claim lapsed in the buffer and was taken back: not
                // ours to hand out.
                HandOut::Gone => drop(held.taken()),
            }
        }
        Ok(None)
    }

    /// Release the claims sitting in the prefetch buffer, as the engine shuts
    /// down. Nobody else knows they are there: left alone they wait until the
    /// other engines see this one stop heartbeating, or until they time out.
    /// Returns how many were released.
    pub async fn release_prefetched(&self) -> usize {
        let Some(prefetch) = &self.prefetch_buffer else {
            return 0;
        };
        let mut released = 0;
        for execution in prefetch.drain_all() {
            let Some(claimed_by) = execution.claimed_by.as_deref() else {
                continue;
            };
            match self
                .execution_repo
                .release_unhanded(
                    &execution.execution_id,
                    execution.activation_seq,
                    claimed_by,
                )
                .await
            {
                Ok(true) => released += 1,
                Ok(false) => {}
                Err(e) => warn!(
                    execution_id = %execution.execution_id,
                    error = %e,
                    "Could not release a prefetched claim on shutdown; it is released by the \
                     other engines or times out"
                ),
            }
        }
        released
    }

    /// Release a parked execution's claim to wake it — or, when an activation
    /// of it is out with a worker, leave the claim for that activation's
    /// completion to release. See
    /// `WorkflowExecutionRepository::release_claim_to_resume`.
    pub async fn release_claim_to_resume(
        &self,
        execution_id: &str,
    ) -> OrcherResult<orcher_storage::repositories::domains::workflow_execution::Resume> {
        self.claimer.release_claim_to_resume(execution_id).await
    }

    /// Notify that work is available on a task queue
    ///
    /// Called when:
    /// - New workflow executions are created
    /// - Commands schedule new work
    /// - Any operation that makes work available
    ///
    /// Wakes ONE waiting service on the specified queue.
    ///
    /// # Arguments
    ///
    /// * `task_queue` - Name of the task queue
    /// * `namespace` - Namespace for isolation
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // After creating a workflow execution
    /// manager.notify_work_available("orders-queue", "production").await;
    /// ```
    pub async fn notify_work_available(&self, task_queue: &str, namespace: &str) {
        debug!(
            task_queue = %task_queue,
            namespace = %namespace,
            "Notifying work available"
        );

        self.notifier
            .notify_work_available(task_queue, namespace)
            .await;
    }

    /// Push a workflow continuation for direct dispatch.
    ///
    /// Called by `requeue_workflow_for_continuation_inner` after releasing the claim.
    /// The execution_id is pushed to an in-memory queue that pollers check first,
    /// skipping Redis round-trip and the expensive claim scan query.
    ///
    /// When `preferred_worker` is `Some`, the entry carries an affinity hint so
    /// `try_pop_continuation` can prefer the worker that last processed this workflow.
    pub fn push_continuation(
        &self,
        task_queue: &str,
        namespace: &str,
        execution_id: &str,
        preferred_worker: Option<String>,
    ) {
        queue_continuation(
            &self.continuation_queue,
            task_queue,
            namespace,
            execution_id,
            preferred_worker,
        );
    }

    /// Put back a continuation a poll of another organization popped, marked
    /// with the organization it belongs to so polls that may not claim it
    /// pass it over. At the front, since it was the next one due.
    fn return_continuation(
        &self,
        task_queue: &str,
        namespace: &str,
        execution_id: String,
        owner: Option<uuid::Uuid>,
    ) {
        let key = format!("{}:{}", task_queue, namespace);
        self.continuation_queue
            .entry(key)
            .or_insert_with(std::collections::VecDeque::new)
            .push_front(ContinuationEntry {
                execution_id,
                preferred_worker: None,
                queued_at: Instant::now(),
                owner: Some(owner),
                marked_at: Some(Instant::now()),
                skips: 0,
            });
    }

    /// Try to pop a workflow continuation for the given queue.
    ///
    /// Returns an execution_id that was pushed by a task completion handler.
    /// The poller can then do a targeted claim by execution_id instead of
    /// the expensive claim scan query.
    ///
    /// An entry held for a worker process, the one that keeps its run's
    /// journal, goes to that process's polls first; to another only once the
    /// hold has passed, as the claim allows. `worker_instance` is the
    /// polling process; empty, it is held nothing.
    pub fn try_pop_continuation(
        &self,
        task_queue: &str,
        namespace: &str,
        worker_instance: &str,
    ) -> Option<String> {
        self.try_pop_continuation_for(task_queue, namespace, worker_instance, None)
    }

    /// [`try_pop_continuation`](Self::try_pop_continuation) for a poll made for
    /// `organization_id`, which passes over an entry known to belong to
    /// another organization.
    pub fn try_pop_continuation_for(
        &self,
        task_queue: &str,
        namespace: &str,
        worker_instance: &str,
        organization_id: Option<uuid::Uuid>,
    ) -> Option<String> {
        let key = format!("{}:{}", task_queue, namespace);
        let mut queue = self.continuation_queue.get_mut(&key)?;

        // Count this poll against each marked entry it may not take, and drop
        // those that have waited as long as they may.
        for entry in queue.iter_mut() {
            if !entry.for_poll(organization_id) {
                entry.skips += 1;
            }
        }
        queue.retain(|entry| !entry.is_stale());

        // First an entry held for this poll's worker process, which keeps
        // the run's journal; a poll naming no process is held nothing.
        let held_here = |entry: &ContinuationEntry| {
            !worker_instance.is_empty()
                && entry.preferred_worker.as_deref() == Some(worker_instance)
        };
        // Then one held for nobody, or whose hold has passed: the claim lets
        // any process take those, and the holder may be busy or gone.
        let free = |entry: &ContinuationEntry| {
            entry.preferred_worker.is_none() || entry.queued_at.elapsed() >= self.journal_hold
        };
        let position = queue
            .iter()
            .position(|entry| entry.for_poll(organization_id) && held_here(entry))
            .or_else(|| {
                queue
                    .iter()
                    .position(|entry| entry.for_poll(organization_id) && free(entry))
            })?;
        let entry = queue.remove(position)?;
        trace!(
            execution_id = %entry.execution_id,
            worker_instance = %worker_instance,
            held_here = entry.preferred_worker.as_deref() == Some(worker_instance),
            "Continuation taken"
        );
        Some(entry.execution_id)
    }

    /// Get statistics about a task queue
    ///
    /// Returns current queue depth and execution counts.
    /// Useful for monitoring, observability, and autoscaling.
    ///
    /// # Arguments
    ///
    /// * `task_queue` - Name of the task queue
    /// * `namespace` - Namespace for isolation
    ///
    /// # Returns
    ///
    /// Queue statistics including available, claimed, and running counts
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let stats = manager.get_queue_stats("orders-queue", "production").await?;
    /// println!("Available: {}", stats.available_count);
    /// println!("Total: {}", stats.total_count());
    /// ```
    pub async fn get_queue_stats(
        &self,
        task_queue: &str,
        namespace: &str,
    ) -> OrcherResult<QueueStats> {
        debug!(
            task_queue = %task_queue,
            namespace = %namespace,
            "Getting queue statistics"
        );

        let (available_count, claimed_count, running_count) = self
            .execution_repo
            .get_queue_counts(task_queue, namespace)
            .await?;

        let mut stats = QueueStats::new(task_queue.to_string(), namespace.to_string());
        stats.available_count = available_count;
        stats.claimed_count = claimed_count;
        stats.running_count = running_count;

        debug!(
            task_queue = %task_queue,
            available = available_count,
            claimed = claimed_count,
            running = running_count,
            "Retrieved queue statistics"
        );

        Ok(stats)
    }

    /// Get the claim timeout in seconds
    pub fn claim_timeout_secs(&self) -> i64 {
        self.claim_timeout_secs
    }

    /// Check if sync-match is enabled
    pub fn is_sync_match_enabled(&self) -> bool {
        self.waiters_registry.is_some()
    }

    /// Peek at waiting worker without removing from queue (sync-match)
    pub fn peek_waiting_worker(&self, namespace: &str, task_queue: &str) -> Option<String> {
        self.waiters_registry
            .as_ref()
            .and_then(|registry| registry.peek_waiter(namespace, task_queue))
    }

    /// Get waiters registry for sync-match (used by polling logic)
    pub fn waiters_registry(&self) -> Option<Arc<WaitersRegistry>> {
        self.waiters_registry.clone()
    }

    /// Get state cache for fast-path optimization
    pub fn state_cache(&self) -> Option<Arc<HighPerfCache>> {
        self.state_cache.clone()
    }

    /// Get worker affinity manager
    pub fn affinity_manager(&self) -> Option<&Arc<WorkerAffinityManager>> {
        self.affinity_manager.as_ref()
    }

    /// Record a new affinity lease when a worker claims a workflow for the first time
    /// or claims it via a non-continuation path (Redis, scan query).
    fn record_affinity(&self, workflow_id: &str, worker_identity: &str) {
        if let Some(ref affinity) = self.affinity_manager {
            if let Ok(wf_uuid) = uuid::Uuid::parse_str(workflow_id) {
                affinity.set_preferred_worker(wf_uuid, worker_identity.to_string(), None);
            }
        }
    }

    /// Renew an existing affinity lease when the same worker reclaims via continuation.
    /// Unlike `record_affinity`, this preserves the original `created_at` and increments
    /// `task_count`, giving a more accurate picture of lease age.
    fn renew_affinity(&self, workflow_id: &str, worker_identity: &str) {
        if let Some(ref affinity) = self.affinity_manager {
            if let Ok(wf_uuid) = uuid::Uuid::parse_str(workflow_id) {
                affinity.renew_lease(&wf_uuid, worker_identity);
            }
        }
    }

    /// Clear an affinity lease when a workflow completes or fails.
    pub fn clear_affinity(&self, workflow_id: &str) {
        if let Some(ref affinity) = self.affinity_manager {
            if let Ok(wf_uuid) = uuid::Uuid::parse_str(workflow_id) {
                affinity.clear_lease(&wf_uuid);
            }
        }
    }

    /// The worker process this engine last sent `execution_id`'s journal to,
    /// which keeps it: a continuation of the run is held for that process
    /// first ([`try_pop_continuation_for`](Self::try_pop_continuation_for)).
    /// `None` when this engine sent it to none, or the process said it keeps
    /// nothing, or did not hold it after all.
    pub async fn journal_holder(&self, execution_id: &str) -> Option<String> {
        self.history_sent
            .get(execution_id)
            .await
            .map(|(instance, _)| instance)
    }

    /// How long a run released to resume is held for the worker process that
    /// keeps its journal. Set alongside the execution repository's
    /// (`set_journal_holder_hold`), which the claim enforces.
    pub fn with_journal_holder_hold(mut self, hold: Duration) -> Self {
        self.journal_hold = hold;
        self
    }

    /// Poll for multiple workflow executions in a single batch (batch claiming optimization)
    ///
    /// Claims up to `batch_size` workflow executions in a single database query,
    /// dramatically reducing contention when multiple workers are polling for work.
    ///
    /// ## Performance Benefits
    ///
    /// With single-claim polling, N workers require N database round-trips to claim
    /// N workflows. With batch polling, one coordinator can claim N workflows in a
    /// single round-trip and distribute them to waiting workers.
    ///
    /// Benchmark results (8 workers, 200 workflows):
    /// - Single-claim: ~4.5 wf/s, poor scaling (0.41x efficiency)
    /// - Batch-claim: ~20+ wf/s expected, near-linear scaling
    ///
    /// ## Use Cases
    ///
    /// 1. **Coordinator pattern**: Central service claims batch and distributes
    /// 2. **Prefetch pattern**: Worker claims batch for local processing queue
    /// 3. **Load balancer**: Claims batch and routes based on affinity/capacity
    ///
    /// # Arguments
    ///
    /// * `request` - Poll request with task_queue, namespace, identity, timeout
    /// * `batch_size` - Maximum number of executions to claim (recommended: 5-20)
    ///
    /// # Returns
    ///
    /// - `Ok(vec)` - Vector of poll responses (may be empty if no work available)
    /// - `Err(_)` - Database, conversion, or timeout error
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let request = PollExecutionRequest::new(
    ///     "orders-queue",
    ///     "production",
    ///     "coordinator-1",
    ///     Duration::from_secs(60),
    /// );
    ///
    /// // Claim up to 10 workflows
    /// let responses = manager.poll_batch(request, 10).await?;
    ///
    /// println!("Claimed {} workflows for distribution", responses.len());
    /// for response in responses {
    ///     // Distribute to waiting workers or process locally
    ///     distribute_to_worker(response).await?;
    /// }
    /// ```
    pub async fn poll_batch(
        &self,
        request: PollExecutionRequest,
        batch_size: i32,
    ) -> OrcherResult<Vec<PollExecutionResponse>> {
        let poll_start = std::time::Instant::now();

        debug!(
            task_queue = %request.task_queue,
            namespace = %request.namespace,
            identity = %request.identity,
            batch_size = batch_size,
            "🔷 POLL_BATCH ENTRY - Starting batch poll for workflow executions"
        );

        // Use batch claiming from the claimer
        let executions = self
            .claimer
            .claim_batch(
                &request.task_queue,
                &request.namespace,
                &request.identity,
                Some(batch_size),
                request.version_id.as_deref(),
                request.organization_id,
                &request.step_ids,
            )
            .await?;

        if executions.is_empty() {
            debug!(
                task_queue = %request.task_queue,
                namespace = %request.namespace,
                elapsed_ms = poll_start.elapsed().as_millis(),
                "🔷 POLL_BATCH: No executions available"
            );
            return Ok(vec![]);
        }

        // Convert each execution to a poll response
        let mut responses = Vec::with_capacity(executions.len());
        for execution in executions {
            match self.converter.to_poll_response(&execution).await {
                Ok(response) => {
                    debug!(
                        execution_id = %response.execution_id,
                        journal_entries = response.journal.len(),
                        "🔷 POLL_BATCH: Converted execution to response"
                    );
                    responses.push(response);
                }
                Err(e) => {
                    // Log error but continue with other executions
                    error!(
                        execution_id = %execution.execution_id,
                        error = %e,
                        "🔷 POLL_BATCH: Failed to convert execution to response, releasing claim"
                    );
                    // Release the claim since we can't process it
                    if let Err(release_err) = self.release_claim(&execution.execution_id).await {
                        error!(
                            execution_id = %execution.execution_id,
                            error = %release_err,
                            "🔷 POLL_BATCH: Failed to release claim after conversion error"
                        );
                    }
                }
            }
        }

        debug!(
            task_queue = %request.task_queue,
            namespace = %request.namespace,
            requested = batch_size,
            claimed = responses.len(),
            elapsed_ms = poll_start.elapsed().as_millis(),
            "🔷 POLL_BATCH EXIT - Returning {} poll responses",
            responses.len()
        );

        Ok(responses)
    }

    /// Get the claimer for direct access (used by batch polling coordinator)
    pub fn claimer(&self) -> &Claimer {
        &self.claimer
    }

    /// Get the converter for direct access (used by batch polling coordinator)
    pub fn converter(&self) -> &Converter {
        &self.converter
    }
}

/// Clears a queue's prefetch refill flag when dropped, including when the
/// poll that set it is cancelled.
struct RefillGuard<'a> {
    prefetch: &'a orcher_dispatch::polling::prefetch::WorkPrefetchBuffer,
    key: &'a PrefetchKey,
}

impl Drop for RefillGuard<'_> {
    fn drop(&mut self) {
        self.prefetch.finish_refill(self.key);
    }
}

/// The prefetch buffer a poll is served from: the one whose batches were
/// claimed as this poll's own claim would be (see [`PrefetchKey`]).
fn prefetch_key(request: &PollExecutionRequest) -> PrefetchKey {
    PrefetchKey::new(
        &request.task_queue,
        &request.namespace,
        request.organization_id,
    )
    .with_version(request.version_id.as_deref())
    .with_step_ids(&request.step_ids)
}

/// A prefetched execution taken from its buffer and not yet handed out. Put
/// back, still claimed, unless [`taken`](Self::taken) first.
struct Unhanded<'a> {
    prefetch: &'a orcher_dispatch::polling::prefetch::WorkPrefetchBuffer,
    key: &'a PrefetchKey,
    execution: Option<orcher_storage::schema::types::execution::WorkflowExecution>,
}

impl Unhanded<'_> {
    /// The execution, no longer to be put back.
    fn taken(mut self) -> orcher_storage::schema::types::execution::WorkflowExecution {
        self.execution.take().expect("held until handed out")
    }
}

impl Drop for Unhanded<'_> {
    fn drop(&mut self) {
        if let Some(execution) = self.execution.take() {
            self.prefetch.put_back(self.key, execution);
        }
    }
}

/// The record of what was sent of each run's journal, for this many runs at
/// most; one not looked at for an hour is dropped too.
fn history_sent_record() -> moka::future::Cache<String, (String, i64)> {
    moka::future::Cache::builder()
        .max_capacity(100_000)
        .time_to_idle(Duration::from_secs(3600))
        .build()
}

/// The worker process a claim made for `request` is recorded against: the one
/// the poll names, if it names one that is tracked (`crate::worker::liveness`).
fn claimant_instance(request: &PollExecutionRequest) -> Option<&str> {
    request
        .worker_instance_id
        .as_deref()
        .and_then(crate::worker::liveness::tracked_instance)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A prefetched execution a poll took and did not hand out — the hand-out
    /// failed, or the poll was dropped awaiting it — goes back to the buffer,
    /// still claimed, rather than staying claimed by nobody.
    #[test]
    fn an_execution_not_handed_out_goes_back_to_its_buffer() {
        use orcher_dispatch::polling::prefetch::WorkPrefetchBuffer;
        use orcher_storage::schema::types::execution::WorkflowExecution;
        let run = |id: &str| {
            WorkflowExecution::builder()
                .execution_id(id.to_string())
                .workflow_name("w".to_string())
                .workflow_version("1".to_string())
                .namespace("ns".to_string())
                .parameters(serde_json::json!({}))
                .build()
        };
        let prefetch = WorkPrefetchBuffer::new();
        let key = PrefetchKey::new("queue", "ns", None);
        prefetch.add_batch(&key, "poller-1", vec![run("a"), run("b")]);

        let held = Unhanded {
            prefetch: &prefetch,
            key: &key,
            execution: prefetch.try_get(&key),
        };
        drop(held);
        let held = Unhanded {
            prefetch: &prefetch,
            key: &key,
            execution: prefetch.try_get(&key),
        };
        assert_eq!(held.taken().execution_id, "b");
        assert!(prefetch.try_get(&key).is_none());
    }

    /// The claim query timeout a deployment configures is the one its claims
    /// wait for, not the claimer's own default.
    #[tokio::test]
    async fn claims_wait_for_the_configured_query_timeout() {
        let pool = sqlx::PgPool::connect_lazy("postgres://nobody@127.0.0.1:1/none").unwrap();
        let manager = PollingManager::new(
            Arc::new(WorkflowExecutionRepository::new(pool.clone())),
            Arc::new(JournalRepository::new(pool)),
            300,
        );
        assert_eq!(
            manager.claimer.claim_query_timeout(),
            Duration::from_secs(2)
        );

        let manager = manager.with_claim_query_timeout(Duration::from_millis(7500));
        assert_eq!(
            manager.claimer.claim_query_timeout(),
            Duration::from_millis(7500)
        );
    }

    /// A poll cancelled while it holds the refill flag must not keep it: the
    /// queue would never batch-claim again.
    #[tokio::test]
    async fn a_cancelled_refill_releases_the_flag() {
        let prefetch =
            std::sync::Arc::new(orcher_dispatch::polling::prefetch::WorkPrefetchBuffer::new());
        assert!(prefetch.try_start_refill(&PrefetchKey::new("q", "ns", None)));
        assert!(
            !prefetch.try_start_refill(&PrefetchKey::new("q", "ns", None)),
            "the flag is held"
        );
        prefetch.finish_refill(&PrefetchKey::new("q", "ns", None));

        let held = prefetch.clone();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let poll = tokio::spawn(async move {
            assert!(held.try_start_refill(&PrefetchKey::new("q", "ns", None)));
            let key = PrefetchKey::new("q", "ns", None);
            let _refill = RefillGuard {
                prefetch: &held,
                key: &key,
            };
            let _ = entered_tx.send(());
            // The batch claim, still in flight when the worker hangs up.
            std::future::pending::<()>().await;
        });
        entered_rx.await.unwrap();
        poll.abort();
        let _ = poll.await;

        assert!(
            prefetch.try_start_refill(&PrefetchKey::new("q", "ns", None)),
            "the cancelled poll left the refill flag set"
        );
    }
    use orcher_storage::schema::types::{execution::WorkflowExecution, journal::JournalEntry};
    use sqlx::PgPool;
    use std::time::Duration as StdDuration;
    use tokio::time::timeout;
    use uuid::Uuid;

    async fn create_test_pool() -> PgPool {
        let database_url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://orcher:orcher@localhost/orcher_test".to_string());

        PgPool::connect(&database_url)
            .await
            .expect("Failed to connect to test database")
    }

    #[tokio::test]
    #[ignore] // Requires database
    async fn test_poll_execution_immediate_work() {
        let pool = create_test_pool().await;
        let execution_repo = Arc::new(WorkflowExecutionRepository::new(pool.clone()));
        let journal_repo = Arc::new(JournalRepository::new(pool.clone()));
        let manager = PollingManager::new(execution_repo.clone(), journal_repo.clone(), 300);

        // Create an execution
        let exec_id = format!("test_exec_{}", Uuid::new_v4());
        let workflow_id = "test_workflow".to_string();

        let mut execution = WorkflowExecution::builder()
            .execution_id(exec_id.clone())
            .workflow_name(workflow_id.clone())
            .workflow_version("v1.0".to_string())
            .namespace("test".to_string())
            .parameters(serde_json::json!({}))
            .build();

        execution.task_queue = "test-queue".to_string();
        execution.status = "pending".to_string();

        execution_repo.create(&execution).await.unwrap();

        // Create journal entry
        let entry = JournalEntry::new(
            workflow_id.clone(),
            exec_id.clone(),
            1,
            "WorkflowExecutionStarted".to_string(),
            serde_json::json!({"workflow_id": workflow_id}),
        );
        journal_repo.append(entry).await.unwrap();

        // Poll (should return immediately)
        let request =
            PollExecutionRequest::new("test-queue", "test", "service-1", StdDuration::from_secs(1));

        let result = manager.poll_execution(request).await.unwrap();

        assert!(result.is_some());
        let response = result.unwrap();
        assert_eq!(response.execution_id, exec_id);
        assert_eq!(response.task_queue, "test-queue");
        assert_eq!(response.journal.len(), 1);
    }

    #[tokio::test]
    #[ignore] // Requires database
    async fn test_poll_execution_timeout() {
        let pool = create_test_pool().await;
        let execution_repo = Arc::new(WorkflowExecutionRepository::new(pool.clone()));
        let journal_repo = Arc::new(JournalRepository::new(pool.clone()));
        let manager = PollingManager::new(execution_repo, journal_repo, 300);

        // Poll empty queue (should timeout)
        let request = PollExecutionRequest::new(
            "empty-queue",
            "test",
            "service-1",
            StdDuration::from_secs(1),
        );

        let start = Instant::now();
        let result = manager.poll_execution(request).await.unwrap();
        let elapsed = start.elapsed();

        assert!(result.is_none());
        assert!(elapsed >= StdDuration::from_secs(1));
        assert!(elapsed < StdDuration::from_secs(2)); // Should timeout promptly
    }

    #[tokio::test]
    #[ignore] // Requires database
    async fn test_poll_execution_with_notification() {
        let pool = create_test_pool().await;
        let execution_repo = Arc::new(WorkflowExecutionRepository::new(pool.clone()));
        let journal_repo = Arc::new(JournalRepository::new(pool.clone()));
        let manager = Arc::new(PollingManager::new(
            execution_repo.clone(),
            journal_repo.clone(),
            300,
        ));

        // Start polling in background (no work initially)
        let manager_clone = Arc::clone(&manager);
        let poll_task = tokio::spawn(async move {
            let request = PollExecutionRequest::new(
                "test-queue",
                "test",
                "service-1",
                StdDuration::from_secs(10),
            );

            manager_clone.poll_execution(request).await
        });

        // Give poller time to start waiting
        tokio::time::sleep(StdDuration::from_millis(100)).await;

        // Create work and notify
        let exec_id = format!("test_exec_{}", Uuid::new_v4());
        let workflow_id = "test_workflow".to_string();

        let mut execution = WorkflowExecution::builder()
            .execution_id(exec_id.clone())
            .workflow_name(workflow_id.clone())
            .workflow_version("v1.0".to_string())
            .namespace("test".to_string())
            .parameters(serde_json::json!({}))
            .build();

        execution.task_queue = "test-queue".to_string();
        execution.status = "pending".to_string();

        execution_repo.create(&execution).await.unwrap();

        // Create journal entry
        let entry = JournalEntry::new(
            workflow_id.clone(),
            exec_id.clone(),
            1,
            "WorkflowExecutionStarted".to_string(),
            serde_json::json!({"workflow_id": workflow_id}),
        );
        journal_repo.append(entry).await.unwrap();

        // Notify
        manager.notify_work_available("test-queue", "test").await;

        // Poller should wake up and claim work
        let result = timeout(StdDuration::from_secs(2), poll_task)
            .await
            .expect("Poll task timed out")
            .expect("Poll task failed")
            .expect("Poll returned error");

        assert!(result.is_some());
        let response = result.unwrap();
        assert_eq!(response.execution_id, exec_id);
    }

    #[tokio::test]
    #[ignore] // Requires database
    async fn test_release_claim() {
        let pool = create_test_pool().await;
        let execution_repo = Arc::new(WorkflowExecutionRepository::new(pool.clone()));
        let journal_repo = Arc::new(JournalRepository::new(pool.clone()));
        let manager = PollingManager::new(execution_repo.clone(), journal_repo, 300);

        // Create and claim execution
        let exec_id = format!("test_exec_{}", Uuid::new_v4());
        let workflow_id = "test_workflow".to_string();

        let mut execution = WorkflowExecution::builder()
            .execution_id(exec_id.clone())
            .workflow_name(workflow_id.clone())
            .workflow_version("v1.0".to_string())
            .namespace("test".to_string())
            .parameters(serde_json::json!({}))
            .build();

        execution.task_queue = "test-queue".to_string();
        execution.status = "pending".to_string();

        execution_repo.create(&execution).await.unwrap();

        // Claim via repository
        let claimed = execution_repo
            .claim_execution_for_poll(
                "test-queue",
                "test",
                "service-1",
                300,
                None,
                None,
                None,
                &[],
            )
            .await
            .unwrap()
            .unwrap();

        // Release via manager
        manager.release_claim(&claimed.execution_id).await.unwrap();

        // Verify claim was released
        let execution = execution_repo
            .find_by_execution_id(&claimed.execution_id)
            .await
            .unwrap()
            .unwrap();

        assert!(execution.claimed_by.is_none());
    }

    #[tokio::test]
    #[ignore] // Requires database
    async fn test_get_queue_stats() {
        let pool = create_test_pool().await;
        let execution_repo = Arc::new(WorkflowExecutionRepository::new(pool.clone()));
        let journal_repo = Arc::new(JournalRepository::new(pool.clone()));
        let manager = PollingManager::new(execution_repo.clone(), journal_repo, 300);

        // Create 3 executions
        for _i in 0..3 {
            let mut execution = WorkflowExecution::builder()
                .execution_id(format!("test_exec_{}", Uuid::new_v4()))
                .workflow_name("test_workflow".to_string())
                .workflow_version("v1.0".to_string())
                .namespace("test".to_string())
                .parameters(serde_json::json!({}))
                .build();

            execution.task_queue = "test-queue".to_string();
            execution.status = "pending".to_string();

            execution_repo.create(&execution).await.unwrap();
        }

        // Get stats
        let stats = manager.get_queue_stats("test-queue", "test").await.unwrap();

        assert_eq!(stats.available_count, 3);
        assert_eq!(stats.task_queue, "test-queue");
        assert_eq!(stats.namespace, "test");
    }
}
