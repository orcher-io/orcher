//! Async waiters for workflow task completion.

use crate::engine::state::types::WorkflowExecutionState;
use dashmap::DashMap;
use orcher_common::types::dispatch::polling::PollExecutionResponse;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{oneshot, Notify};
use tracing::{debug, trace, warn};
use uuid::Uuid;

// ============================================================================
// Types
// ============================================================================

/// Key for identifying a task queue
#[derive(Clone, Hash, Eq, PartialEq, Debug)]
pub struct TaskQueueKey {
    /// Namespace for multi-tenancy isolation
    pub namespace: String,
    /// Task queue name
    pub task_queue: String,
}

impl TaskQueueKey {
    /// Create a new task queue key
    pub fn new(namespace: impl Into<String>, task_queue: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            task_queue: task_queue.into(),
        }
    }

    /// Create a key from string references (avoids allocation when looking up)
    #[inline]
    pub fn from_refs(namespace: &str, task_queue: &str) -> Self {
        Self {
            namespace: namespace.to_string(),
            task_queue: task_queue.to_string(),
        }
    }
}

impl std::fmt::Display for TaskQueueKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.namespace, self.task_queue)
    }
}

/// Work dispatched to a waiting worker via sync-match
#[derive(Debug)]
pub struct RoutedTask {
    /// The poll response to send to the worker
    pub response: PollExecutionResponse,

    /// When routing started (for latency metrics)
    pub routed_at: Instant,

    /// Cached workflow state (sync-match optimization)
    /// When present, the worker can skip loading state from the database.
    pub cached_state: Option<WorkflowExecutionState>,
}

impl RoutedTask {
    /// Create a new routed task
    pub fn new(response: PollExecutionResponse) -> Self {
        Self {
            response,
            routed_at: Instant::now(),
            cached_state: None,
        }
    }

    /// Create a new routed task with cached state
    pub fn with_cached_state(
        response: PollExecutionResponse,
        cached_state: WorkflowExecutionState,
    ) -> Self {
        Self {
            response,
            routed_at: Instant::now(),
            cached_state: Some(cached_state),
        }
    }

    /// Get the elapsed time since routing started
    pub fn elapsed(&self) -> Duration {
        self.routed_at.elapsed()
    }
}

/// A wake notification sent to waiters when work is available
///
/// This is a lightweight signal that tells waiters to check Redis Streams
/// for work, avoiding the need to build full responses at dispatch time.
#[derive(Debug, Clone)]
pub struct WakeNotification {
    /// The execution ID that has work available
    pub execution_id: String,
    /// The stream entry ID for quick lookup
    pub stream_entry_id: String,
    /// When the notification was sent
    pub sent_at: Instant,
}

impl WakeNotification {
    /// Create a new wake notification
    pub fn new(execution_id: impl Into<String>, stream_entry_id: impl Into<String>) -> Self {
        Self {
            execution_id: execution_id.into(),
            stream_entry_id: stream_entry_id.into(),
            sent_at: Instant::now(),
        }
    }
}

/// A worker registered as waiting for work
///
/// Contains a oneshot sender that allows direct dispatch of work
/// without going through Redis/PostgreSQL.
pub struct Waiter {
    /// Unique identifier for this wait operation
    pub wait_id: Uuid,

    /// Worker identity (for logging/debugging and affinity routing)
    pub worker_id: String,

    /// Channel to send work when available
    pub sender: oneshot::Sender<RoutedTask>,

    /// When this waiter was registered
    pub registered_at: Instant,

    /// When this waiter times out (should be unregistered)
    pub timeout_at: Instant,

    /// Wake notification channel (alternative to full dispatch)
    /// When set, the waiter can be woken up to poll Redis immediately
    pub wake_notify: Option<Arc<Notify>>,
}

impl Waiter {
    /// Check if this waiter has expired
    #[inline]
    pub fn is_expired(&self) -> bool {
        Instant::now() >= self.timeout_at
    }

    /// Check if this waiter's channel is still open
    #[inline]
    pub fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }

    /// Get the remaining time before timeout
    pub fn remaining_time(&self) -> Duration {
        self.timeout_at.saturating_duration_since(Instant::now())
    }
}

impl std::fmt::Debug for Waiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Waiter")
            .field("wait_id", &self.wait_id)
            .field("worker_id", &self.worker_id)
            .field("registered_at", &self.registered_at)
            .field("timeout_at", &self.timeout_at)
            .field("is_expired", &self.is_expired())
            .field("is_closed", &self.is_closed())
            .finish()
    }
}

// ============================================================================
// Metrics
// ============================================================================

/// Metrics for the waiters registry
#[derive(Debug, Default)]
pub struct WaitersMetrics {
    /// Total waiters registered
    pub registered: AtomicU64,
    /// Waiters that received work (successful sync-match)
    pub served: AtomicU64,
    /// Waiters that expired without receiving work
    pub expired: AtomicU64,
    /// Waiters rejected due to queue full
    pub rejected: AtomicU64,
    /// Failed dispatch attempts (channel closed)
    pub dispatch_failures: AtomicU64,
    /// Waiters explicitly unregistered (timeout or cancel)
    pub unregistered: AtomicU64,
}

impl WaitersMetrics {
    /// Create new metrics
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a successful registration
    #[inline]
    pub fn record_registered(&self) {
        self.registered.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a successful dispatch (sync-match)
    #[inline]
    pub fn record_served(&self) {
        self.served.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an expired waiter
    #[inline]
    pub fn record_expired(&self) {
        self.expired.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a rejected registration (queue full)
    #[inline]
    pub fn record_rejected(&self) {
        self.rejected.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a dispatch failure (channel closed)
    #[inline]
    pub fn record_dispatch_failure(&self) {
        self.dispatch_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an explicit unregistration
    #[inline]
    pub fn record_unregistered(&self) {
        self.unregistered.fetch_add(1, Ordering::Relaxed);
    }

    /// Get the sync-match success rate
    pub fn sync_match_rate(&self) -> f64 {
        let served = self.served.load(Ordering::Relaxed);
        let registered = self.registered.load(Ordering::Relaxed);
        if registered == 0 {
            0.0
        } else {
            served as f64 / registered as f64
        }
    }
}

// ============================================================================
// WaitersRegistry
// ============================================================================

/// Registry of workers waiting for work per task queue
///
/// Thread-safe, sharded by task queue key using `DashMap` for high concurrency.
/// Waiters are stored in FIFO order within each queue for fair dispatch.
///
/// ## Memory Limits
///
/// Each queue has a maximum number of waiters to prevent memory exhaustion.
/// When a queue is full, new registrations are rejected and the worker
/// falls back to standard Redis polling.
///
/// ## Thread Safety
///
/// - Uses `DashMap` for per-key sharded locking
/// - No global locks
/// - Locks released before any async operations
///
/// ## Wake Notification Pattern
///
/// In addition to full sync-match dispatch, the registry supports a lightweight
/// "wake" notification pattern:
///
/// 1. Dispatch publishes to Redis Streams first (durability)
/// 2. Dispatch calls `wake_waiters()` with just the execution_id
/// 3. All waiters on that queue are notified to check Redis immediately
/// 4. This achieves low latency without building full responses at dispatch time
///
/// This avoids claim conflicts because:
/// - Dispatch doesn't claim or build responses
/// - Workers claim when they actually process (after Redis poll)
pub struct WaitersRegistry {
    /// Map of task queue -> FIFO queue of waiters
    waiters: DashMap<TaskQueueKey, VecDeque<Waiter>>,

    /// Map of task queue -> shared notify for wake signals
    wake_notifiers: DashMap<TaskQueueKey, Arc<Notify>>,

    /// Maximum waiters per queue (prevents memory exhaustion)
    max_waiters_per_queue: usize,

    /// Metrics
    metrics: WaitersMetrics,

    /// Wake notifications counter
    wake_count: AtomicU64,
}

impl WaitersRegistry {
    /// Create a new waiters registry
    ///
    /// # Arguments
    ///
    /// * `max_waiters_per_queue` - Maximum number of waiters allowed per queue.
    ///   Recommended: 10,000 (~2MB per queue at ~200 bytes per waiter)
    pub fn new(max_waiters_per_queue: usize) -> Self {
        debug!(
            max_waiters_per_queue,
            "Creating WaitersRegistry for sync-match"
        );

        Self {
            waiters: DashMap::new(),
            wake_notifiers: DashMap::new(),
            max_waiters_per_queue,
            metrics: WaitersMetrics::new(),
            wake_count: AtomicU64::new(0),
        }
    }

    /// Register a worker as waiting for work
    ///
    /// Returns `Some((wait_id, receiver))` on success, or `None` if the queue is full.
    /// The worker should wait on the receiver for work, with a timeout.
    ///
    /// # Arguments
    ///
    /// * `namespace` - Namespace for isolation
    /// * `task_queue` - Task queue name
    /// * `worker_id` - Worker identity (for logging and affinity)
    /// * `timeout` - How long the worker will wait
    ///
    /// # Returns
    ///
    /// * `Some((wait_id, receiver))` - Successfully registered; wait on receiver
    /// * `None` - Queue is full; fall back to Redis polling
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let (wait_id, receiver) = registry.register(
    ///     "production",
    ///     "orders-queue",
    ///     "worker-1",
    ///     Duration::from_secs(60),
    /// )?;
    ///
    /// tokio::select! {
    ///     result = receiver => {
    ///         // Received work directly!
    ///     }
    ///     _ = tokio::time::sleep(timeout) => {
    ///         registry.unregister("production", "orders-queue", wait_id);
    ///     }
    /// }
    /// ```
    pub fn register(
        &self,
        namespace: &str,
        task_queue: &str,
        worker_id: &str,
        timeout: Duration,
    ) -> Option<(Uuid, oneshot::Receiver<RoutedTask>)> {
        let key = TaskQueueKey::from_refs(namespace, task_queue);
        let now = Instant::now();
        let wait_id = Uuid::new_v4();

        // Create oneshot channel
        let (sender, receiver) = oneshot::channel();

        // Get or create the shared wake notifier for this queue
        let wake_notify = self
            .wake_notifiers
            .entry(key.clone())
            .or_insert_with(|| Arc::new(Notify::new()))
            .clone();

        // Get or create queue, then check limit and push
        let mut queue = self
            .waiters
            .entry(key.clone())
            .or_insert_with(VecDeque::new);

        // Check queue limit
        if queue.len() >= self.max_waiters_per_queue {
            self.metrics.record_rejected();
            warn!(
                namespace = %namespace,
                task_queue = %task_queue,
                worker_id = %worker_id,
                queue_size = queue.len(),
                max = self.max_waiters_per_queue,
                "Waiter registration rejected: queue full"
            );
            return None;
        }

        // Create waiter with wake notify
        let waiter = Waiter {
            wait_id,
            worker_id: worker_id.to_string(),
            sender,
            registered_at: now,
            timeout_at: now + timeout,
            wake_notify: Some(wake_notify),
        };

        // Push to back (FIFO ordering)
        queue.push_back(waiter);

        self.metrics.record_registered();

        trace!(
            namespace = %namespace,
            task_queue = %task_queue,
            worker_id = %worker_id,
            wait_id = %wait_id,
            timeout_secs = timeout.as_secs(),
            queue_size = queue.len(),
            "Waiter registered"
        );

        Some((wait_id, receiver))
    }

    /// Register a worker with wake notification support
    ///
    /// This variant returns both a oneshot receiver for sync-match dispatch
    /// AND a reference to the shared Notify for wake signals.
    ///
    /// # Arguments
    ///
    /// * `namespace` - Namespace for isolation
    /// * `task_queue` - Task queue name
    /// * `worker_id` - Worker identity
    /// * `timeout` - How long the worker will wait
    ///
    /// # Returns
    ///
    /// * `Some((wait_id, receiver, wake_notify))` - Successfully registered
    /// * `None` - Queue is full
    pub fn register_with_wake(
        &self,
        namespace: &str,
        task_queue: &str,
        worker_id: &str,
        timeout: Duration,
    ) -> Option<(Uuid, oneshot::Receiver<RoutedTask>, Arc<Notify>)> {
        let key = TaskQueueKey::from_refs(namespace, task_queue);
        let now = Instant::now();
        let wait_id = Uuid::new_v4();

        // Create oneshot channel
        let (sender, receiver) = oneshot::channel();

        // Get or create the shared wake notifier for this queue
        let wake_notify = self
            .wake_notifiers
            .entry(key.clone())
            .or_insert_with(|| Arc::new(Notify::new()))
            .clone();

        // Get or create queue, then check limit and push
        let mut queue = self
            .waiters
            .entry(key.clone())
            .or_insert_with(VecDeque::new);

        // Check queue limit
        if queue.len() >= self.max_waiters_per_queue {
            self.metrics.record_rejected();
            warn!(
                namespace = %namespace,
                task_queue = %task_queue,
                worker_id = %worker_id,
                queue_size = queue.len(),
                max = self.max_waiters_per_queue,
                "Waiter registration rejected: queue full"
            );
            return None;
        }

        // Create waiter with wake notify
        let waiter = Waiter {
            wait_id,
            worker_id: worker_id.to_string(),
            sender,
            registered_at: now,
            timeout_at: now + timeout,
            wake_notify: Some(wake_notify.clone()),
        };

        // Push to back (FIFO ordering)
        queue.push_back(waiter);

        self.metrics.record_registered();

        trace!(
            namespace = %namespace,
            task_queue = %task_queue,
            worker_id = %worker_id,
            wait_id = %wait_id,
            timeout_secs = timeout.as_secs(),
            queue_size = queue.len(),
            "Waiter registered with wake support"
        );

        Some((wait_id, receiver, wake_notify))
    }

    /// Wake all waiters on a task queue to check for work
    ///
    /// This is a lightweight notification that tells all waiting workers
    /// to immediately check Redis Streams for work. Unlike `try_dispatch`,
    /// this doesn't require building a full response or claiming executions.
    ///
    /// ## Use Case
    ///
    /// After publishing to Redis Streams, call this to wake waiters:
    ///
    /// ```rust,ignore
    /// // 1. Publish to Redis Streams (durability)
    /// let stream_id = streams_client.publish(...).await?;
    ///
    /// // 2. Wake waiters to check immediately
    /// registry.wake_waiters("production", "orders-queue");
    ///
    /// // Waiters will wake up and poll Redis, finding the new work
    /// ```
    ///
    /// ## Benefits
    ///
    /// - No claim conflicts (workers claim when they process)
    /// - No need to build responses at dispatch time
    /// - Very low latency (~microseconds)
    /// - Works with existing Redis Streams flow
    ///
    /// # Arguments
    ///
    /// * `namespace` - Namespace for isolation
    /// * `task_queue` - Task queue name
    ///
    /// # Returns
    ///
    /// Number of waiters that were notified (approximate)
    pub fn wake_waiters(&self, namespace: &str, task_queue: &str) -> usize {
        let key = TaskQueueKey::from_refs(namespace, task_queue);

        // Get the wake notifier for this queue
        if let Some(notify) = self.wake_notifiers.get(&key) {
            // Get approximate waiter count before notifying
            let waiter_count = self.waiters.get(&key).map(|q| q.len()).unwrap_or(0);

            if waiter_count > 0 {
                // Notify only ONE waiter to avoid race conditions
                // Multiple waiters trying to claim the same work causes conflicts
                notify.notify_one();
                self.wake_count.fetch_add(1, Ordering::Relaxed);

                debug!(
                    namespace = %namespace,
                    task_queue = %task_queue,
                    waiter_count,
                    "Woke one waiter to check Redis"
                );

                return 1;
            }
        }

        0
    }

    /// Get the wake notifier for a task queue
    ///
    /// This can be used by workers to wait for wake signals.
    pub fn get_wake_notifier(&self, namespace: &str, task_queue: &str) -> Option<Arc<Notify>> {
        let key = TaskQueueKey::from_refs(namespace, task_queue);
        self.wake_notifiers.get(&key).map(|n| n.clone())
    }

    /// Get total wake notification count
    pub fn wake_count(&self) -> u64 {
        self.wake_count.load(Ordering::Relaxed)
    }

    /// Try to dispatch work to a waiting worker (FIFO)
    ///
    /// Pops waiters from the front of the queue, skipping expired or closed ones,
    /// until work is successfully dispatched or no waiters remain.
    ///
    /// # Arguments
    ///
    /// * `namespace` - Namespace for isolation
    /// * `task_queue` - Task queue name
    /// * `work` - The work to dispatch
    ///
    /// # Returns
    ///
    /// * `Ok(worker_id)` - Work dispatched to worker
    /// * `Err(work)` - No waiter available; work returned for Redis queueing
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let work = RoutedTask::new(response);
    /// match registry.try_dispatch("production", "orders-queue", work) {
    ///     Ok(worker_id) => {
    ///         // Sync-matched! XACK the Redis entry.
    ///     }
    ///     Err(work) => {
    ///         // No waiter, leave in Redis for polling.
    ///     }
    /// }
    /// ```
    #[allow(clippy::result_large_err)]
    pub fn try_dispatch(
        &self,
        namespace: &str,
        task_queue: &str,
        work: RoutedTask,
    ) -> Result<String, RoutedTask> {
        let key = TaskQueueKey::from_refs(namespace, task_queue);

        // Try to get the queue
        let mut queue_entry = match self.waiters.get_mut(&key) {
            Some(entry) => entry,
            None => {
                trace!(
                    namespace = %namespace,
                    task_queue = %task_queue,
                    "No waiters queue exists"
                );
                return Err(work);
            }
        };

        let queue = queue_entry.value_mut();

        // Pop waiters from front until we find a valid one
        loop {
            let waiter = match queue.pop_front() {
                Some(w) => w,
                None => {
                    trace!(
                        namespace = %namespace,
                        task_queue = %task_queue,
                        "No waiters in queue"
                    );
                    return Err(work);
                }
            };

            // Skip expired waiters
            if waiter.is_expired() {
                self.metrics.record_expired();
                trace!(
                    wait_id = %waiter.wait_id,
                    worker_id = %waiter.worker_id,
                    "Skipping expired waiter"
                );
                continue;
            }

            // Skip closed channels (worker disconnected)
            if waiter.is_closed() {
                self.metrics.record_dispatch_failure();
                trace!(
                    wait_id = %waiter.wait_id,
                    worker_id = %waiter.worker_id,
                    "Skipping closed waiter channel"
                );
                continue;
            }

            // Try to send work
            let worker_id = waiter.worker_id.clone();
            match waiter.sender.send(work) {
                Ok(()) => {
                    self.metrics.record_served();
                    debug!(
                        namespace = %namespace,
                        task_queue = %task_queue,
                        worker_id = %worker_id,
                        wait_id = %waiter.wait_id,
                        queue_remaining = queue.len(),
                        "Sync-match dispatch successful"
                    );
                    return Ok(worker_id);
                }
                Err(returned_work) => {
                    // Channel closed between check and send (race)
                    self.metrics.record_dispatch_failure();
                    warn!(
                        wait_id = %waiter.wait_id,
                        worker_id = %worker_id,
                        "Dispatch failed: channel closed during send"
                    );
                    // Continue with next waiter, using the returned work
                    return self.try_dispatch_inner(queue, returned_work);
                }
            }
        }
    }

    /// Internal dispatch helper after initial pop (avoids re-acquiring lock)
    #[allow(clippy::result_large_err)]
    fn try_dispatch_inner(
        &self,
        queue: &mut VecDeque<Waiter>,
        work: RoutedTask,
    ) -> Result<String, RoutedTask> {
        loop {
            let waiter = match queue.pop_front() {
                Some(w) => w,
                None => return Err(work),
            };

            if waiter.is_expired() {
                self.metrics.record_expired();
                continue;
            }

            if waiter.is_closed() {
                self.metrics.record_dispatch_failure();
                continue;
            }

            let worker_id = waiter.worker_id.clone();
            match waiter.sender.send(work) {
                Ok(()) => {
                    self.metrics.record_served();
                    return Ok(worker_id);
                }
                Err(returned_work) => {
                    self.metrics.record_dispatch_failure();
                    return self.try_dispatch_inner(queue, returned_work);
                }
            }
        }
    }

    /// Check if any waiter is available for a task queue (without removing)
    ///
    /// This is used for the fast-path optimization to decide whether to
    /// build a full response at dispatch time.
    ///
    /// # Arguments
    ///
    /// * `namespace` - Namespace for isolation
    /// * `task_queue` - Task queue name
    ///
    /// # Returns
    ///
    /// * `Some(worker_id)` - A waiter is available with this worker ID
    /// * `None` - No valid waiters available
    pub fn peek_waiter(&self, namespace: &str, task_queue: &str) -> Option<String> {
        let key = TaskQueueKey::from_refs(namespace, task_queue);

        let queue_entry = self.waiters.get(&key)?;
        let queue = queue_entry.value();

        // Find first valid waiter (not expired, not closed)
        for waiter in queue.iter() {
            if !waiter.is_expired() && !waiter.is_closed() {
                return Some(waiter.worker_id.clone());
            }
        }

        None
    }

    /// Check if any waiter exists for a task queue (quick check)
    ///
    /// This is a faster version of `peek_waiter` that just checks existence,
    /// without validating expiration or channel status.
    ///
    /// # Arguments
    ///
    /// * `namespace` - Namespace for isolation
    /// * `task_queue` - Task queue name
    ///
    /// # Returns
    ///
    /// * `true` - At least one waiter exists (may be expired)
    /// * `false` - No waiters in queue
    #[inline]
    pub fn has_waiters(&self, namespace: &str, task_queue: &str) -> bool {
        let key = TaskQueueKey::from_refs(namespace, task_queue);
        self.waiters
            .get(&key)
            .map(|q| !q.is_empty())
            .unwrap_or(false)
    }

    /// Try to dispatch work with cached state (fast-path optimization)
    ///
    /// Similar to `try_dispatch`, but the RoutedTask includes cached state
    /// so the worker can skip loading from the database.
    ///
    /// # Arguments
    ///
    /// * `namespace` - Namespace for isolation
    /// * `task_queue` - Task queue name
    /// * `response` - The poll response to send
    /// * `cached_state` - Pre-loaded workflow state
    ///
    /// # Returns
    ///
    /// * `Ok(worker_id)` - Work dispatched to worker with cached state
    /// * `Err(response)` - No waiter available; response returned
    #[allow(clippy::result_large_err)]
    pub fn try_dispatch_with_cached_state(
        &self,
        namespace: &str,
        task_queue: &str,
        response: PollExecutionResponse,
        cached_state: WorkflowExecutionState,
    ) -> Result<String, PollExecutionResponse> {
        let work = RoutedTask::with_cached_state(response, cached_state);

        match self.try_dispatch(namespace, task_queue, work) {
            Ok(worker_id) => Ok(worker_id),
            Err(returned_work) => Err(returned_work.response),
        }
    }

    /// Try to dispatch to a specific preferred worker (for affinity routing)
    ///
    /// Searches the queue for a waiter with the specified worker_id.
    /// If found and valid, dispatches work to that worker.
    /// Does NOT fall back to other waiters - use `try_dispatch` for that.
    ///
    /// # Arguments
    ///
    /// * `namespace` - Namespace for isolation
    /// * `task_queue` - Task queue name
    /// * `worker_id` - Preferred worker identity
    /// * `work` - The work to dispatch
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Work dispatched to preferred worker
    /// * `Err(work)` - Preferred worker not waiting; work returned
    #[allow(clippy::result_large_err)]
    pub fn try_dispatch_to_worker(
        &self,
        namespace: &str,
        task_queue: &str,
        worker_id: &str,
        work: RoutedTask,
    ) -> Result<(), RoutedTask> {
        let key = TaskQueueKey::from_refs(namespace, task_queue);

        let mut queue_entry = match self.waiters.get_mut(&key) {
            Some(entry) => entry,
            None => return Err(work),
        };

        let queue = queue_entry.value_mut();

        // Find the waiter with matching worker_id
        let position = queue
            .iter()
            .position(|w| w.worker_id == worker_id && !w.is_expired() && !w.is_closed());

        let position = match position {
            Some(pos) => pos,
            None => {
                trace!(
                    namespace = %namespace,
                    task_queue = %task_queue,
                    worker_id = %worker_id,
                    "Preferred worker not found in waiters queue"
                );
                return Err(work);
            }
        };

        // Remove the waiter from the queue
        let waiter = queue.remove(position).expect("Position was just validated");

        // Try to send
        match waiter.sender.send(work) {
            Ok(()) => {
                self.metrics.record_served();
                debug!(
                    namespace = %namespace,
                    task_queue = %task_queue,
                    worker_id = %worker_id,
                    wait_id = %waiter.wait_id,
                    "Affinity dispatch to preferred worker successful"
                );
                Ok(())
            }
            Err(returned_work) => {
                self.metrics.record_dispatch_failure();
                Err(returned_work)
            }
        }
    }

    // ========================================================================
    // Zero-copy dispatch: the response is shared, not cloned
    // ========================================================================

    /// Try to dispatch work using zero-copy Arc-based response
    ///
    /// This is the optimized dispatch path that avoids cloning the response
    /// when retrying with multiple waiters. Only one Arc clone (8 bytes) is
    /// needed per dispatch attempt instead of cloning the entire response.
    ///
    /// # Performance
    ///
    /// - First dispatch: wraps response in Arc (one allocation)
    /// - Retry with next waiter: Arc::clone() (8-byte copy)
    /// - vs previous: full response clone per retry (~KB of data)
    ///
    /// # Arguments
    ///
    /// * `namespace` - Namespace for isolation
    /// * `task_queue` - Task queue name
    /// * `response` - Shared poll response to dispatch
    ///
    /// # Returns
    ///
    /// * `Ok(worker_id)` - Work dispatched successfully
    /// * `Err(response)` - No waiter available; shared response returned
    #[allow(clippy::result_large_err)]
    pub fn try_dispatch_zero_copy(
        &self,
        namespace: &str,
        task_queue: &str,
        response: orcher_common::types::dispatch::polling::SharedPollResponse,
    ) -> Result<String, orcher_common::types::dispatch::polling::SharedPollResponse> {
        let key = TaskQueueKey::from_refs(namespace, task_queue);

        let mut queue_entry = match self.waiters.get_mut(&key) {
            Some(entry) => entry,
            None => {
                trace!(
                    namespace = %namespace,
                    task_queue = %task_queue,
                    "No waiters queue exists (zero-copy)"
                );
                return Err(response);
            }
        };

        let queue = queue_entry.value_mut();
        self.try_dispatch_zero_copy_inner(queue, response, namespace, task_queue)
    }

    /// Internal zero-copy dispatch helper
    #[allow(clippy::result_large_err)]
    fn try_dispatch_zero_copy_inner(
        &self,
        queue: &mut VecDeque<Waiter>,
        response: orcher_common::types::dispatch::polling::SharedPollResponse,
        namespace: &str,
        task_queue: &str,
    ) -> Result<String, orcher_common::types::dispatch::polling::SharedPollResponse> {
        loop {
            let waiter = match queue.pop_front() {
                Some(w) => w,
                None => {
                    trace!(
                        namespace = %namespace,
                        task_queue = %task_queue,
                        "No waiters in queue (zero-copy)"
                    );
                    return Err(response);
                }
            };

            // Skip expired waiters
            if waiter.is_expired() {
                self.metrics.record_expired();
                continue;
            }

            // Skip closed channels
            if waiter.is_closed() {
                self.metrics.record_dispatch_failure();
                continue;
            }

            // Convert SharedPollResponse to RoutedTask (zero-copy: just Arc clone)
            let worker_id = waiter.worker_id.clone();
            let work = RoutedTask::new(response.to_owned());

            match waiter.sender.send(work) {
                Ok(()) => {
                    self.metrics.record_served();
                    debug!(
                        namespace = %namespace,
                        task_queue = %task_queue,
                        worker_id = %worker_id,
                        "Zero-copy sync-match dispatch successful"
                    );
                    return Ok(worker_id);
                }
                Err(returned_work) => {
                    // Channel closed during send - try next waiter
                    self.metrics.record_dispatch_failure();
                    // Recover the SharedPollResponse from the returned RoutedTask
                    let recovered =
                        orcher_common::types::dispatch::polling::SharedPollResponse::new(
                            returned_work.response,
                        );
                    return self
                        .try_dispatch_zero_copy_inner(queue, recovered, namespace, task_queue);
                }
            }
        }
    }

    /// Unregister a waiter (on timeout or cancel)
    ///
    /// Call this when a worker's poll times out or is cancelled to remove
    /// the waiter from the queue and prevent memory leaks.
    ///
    /// # Arguments
    ///
    /// * `namespace` - Namespace for isolation
    /// * `task_queue` - Task queue name
    /// * `wait_id` - The wait ID returned from `register()`
    pub fn unregister(&self, namespace: &str, task_queue: &str, wait_id: Uuid) {
        let key = TaskQueueKey::from_refs(namespace, task_queue);

        if let Some(mut queue) = self.waiters.get_mut(&key) {
            let original_len = queue.len();
            queue.retain(|w| w.wait_id != wait_id);
            if queue.len() < original_len {
                self.metrics.record_unregistered();
                trace!(
                    namespace = %namespace,
                    task_queue = %task_queue,
                    wait_id = %wait_id,
                    "Waiter unregistered"
                );
            }
        }
    }

    /// Get waiter count for a specific queue
    pub fn waiter_count(&self, namespace: &str, task_queue: &str) -> usize {
        let key = TaskQueueKey::from_refs(namespace, task_queue);
        self.waiters.get(&key).map(|q| q.len()).unwrap_or(0)
    }

    /// Get total waiter count across all queues
    pub fn total_waiter_count(&self) -> usize {
        self.waiters.iter().map(|entry| entry.value().len()).sum()
    }

    /// Get the number of queues with waiters
    pub fn queue_count(&self) -> usize {
        self.waiters.len()
    }

    /// Clean up expired waiters across all queues
    ///
    /// Call this periodically (e.g., every 30 seconds) to remove expired
    /// waiters and free memory. Returns the number of expired waiters removed.
    pub fn cleanup_expired(&self) -> usize {
        let mut total_expired = 0;

        // Iterate through all queues
        for mut entry in self.waiters.iter_mut() {
            let queue = entry.value_mut();
            let before = queue.len();

            // Remove expired and closed waiters
            queue.retain(|w| !w.is_expired() && !w.is_closed());

            let removed = before - queue.len();
            total_expired += removed;

            // Record metrics for each expired
            for _ in 0..removed {
                self.metrics.record_expired();
            }
        }

        // Remove empty queues to free memory
        self.waiters.retain(|_, queue| !queue.is_empty());

        if total_expired > 0 {
            debug!(
                expired_count = total_expired,
                remaining_queues = self.waiters.len(),
                "Cleaned up expired waiters"
            );
        }

        total_expired
    }

    /// Get a reference to the metrics
    pub fn metrics(&self) -> &WaitersMetrics {
        &self.metrics
    }

    /// Get the maximum waiters per queue limit
    pub fn max_waiters_per_queue(&self) -> usize {
        self.max_waiters_per_queue
    }
}

impl std::fmt::Debug for WaitersRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WaitersRegistry")
            .field("queue_count", &self.waiters.len())
            .field("total_waiters", &self.total_waiter_count())
            .field("max_waiters_per_queue", &self.max_waiters_per_queue)
            .finish()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use jiff::SignedDuration;
    use orcher_common::types::time::Timestamptz;
    use std::time::Duration;

    fn create_test_response(execution_id: &str) -> PollExecutionResponse {
        PollExecutionResponse {
            workflow_id: "wf-123".to_string(),
            execution_id: execution_id.to_string(),
            workflow_type: "TestWorkflow".to_string(),
            task_queue: "test-queue".to_string(),
            namespace: "test".to_string(),
            input: serde_json::json!({}),
            journal: vec![],
            started_event_id: 1,
            previous_started_event_id: None,
            attempt: 1,
            started_at: Timestamptz::now(),
            claimed_at: Timestamptz::now(),
            claim_timeout_at: Timestamptz::now() + SignedDuration::from_secs(300),
            claimed_by: "test-worker".to_string(),
            stream_entry_id: None,
            parent_execution_id: None,
            activation_seq: 1,
            step_id_scheme: None,
            history_lineage: String::new(),
            step_results_journaled: true,
            history_after_entry_id: 0,
        }
    }

    #[tokio::test]
    async fn test_register_and_dispatch() {
        let registry = WaitersRegistry::new(100);

        // Register a waiter
        let (wait_id, receiver) = registry
            .register("test", "queue", "worker-1", Duration::from_secs(60))
            .expect("Should register");

        assert!(!wait_id.is_nil());
        assert_eq!(registry.waiter_count("test", "queue"), 1);

        // Dispatch work
        let work = RoutedTask::new(create_test_response("exec-1"));
        let result = registry.try_dispatch("test", "queue", work);

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "worker-1");
        assert_eq!(registry.waiter_count("test", "queue"), 0);

        // Receiver should have the work
        let received = receiver.await.expect("Should receive work");
        assert_eq!(received.response.execution_id, "exec-1");
    }

    #[tokio::test]
    async fn test_fifo_ordering() {
        let registry = WaitersRegistry::new(100);

        // Register 3 waiters
        let (_id1, _rx1) = registry
            .register("test", "queue", "worker-1", Duration::from_secs(60))
            .unwrap();
        let (_id2, _rx2) = registry
            .register("test", "queue", "worker-2", Duration::from_secs(60))
            .unwrap();
        let (_id3, _rx3) = registry
            .register("test", "queue", "worker-3", Duration::from_secs(60))
            .unwrap();

        assert_eq!(registry.waiter_count("test", "queue"), 3);

        // First dispatch should go to worker-1 (FIFO)
        let work1 = RoutedTask::new(create_test_response("exec-1"));
        assert_eq!(
            registry.try_dispatch("test", "queue", work1).unwrap(),
            "worker-1"
        );

        // Second dispatch should go to worker-2
        let work2 = RoutedTask::new(create_test_response("exec-2"));
        assert_eq!(
            registry.try_dispatch("test", "queue", work2).unwrap(),
            "worker-2"
        );

        // Third dispatch should go to worker-3
        let work3 = RoutedTask::new(create_test_response("exec-3"));
        assert_eq!(
            registry.try_dispatch("test", "queue", work3).unwrap(),
            "worker-3"
        );

        // Queue should be empty
        assert_eq!(registry.waiter_count("test", "queue"), 0);
    }

    #[tokio::test]
    async fn test_expired_waiters_skipped() {
        let registry = WaitersRegistry::new(100);

        // Register a waiter with very short timeout
        let (_id1, _rx1) = registry
            .register("test", "queue", "worker-1", Duration::from_millis(1))
            .unwrap();

        // Wait for it to expire
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Register another waiter with longer timeout
        let (_id2, rx2) = registry
            .register("test", "queue", "worker-2", Duration::from_secs(60))
            .unwrap();

        // Dispatch should skip expired waiter-1 and go to waiter-2
        let work = RoutedTask::new(create_test_response("exec-1"));
        let result = registry.try_dispatch("test", "queue", work);

        assert_eq!(result.unwrap(), "worker-2");

        // waiter-2 should receive the work
        let received = rx2.await.expect("Should receive work");
        assert_eq!(received.response.execution_id, "exec-1");

        // Metrics should show 1 expired
        assert_eq!(registry.metrics().expired.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_max_waiters_limit() {
        let registry = WaitersRegistry::new(2); // Very small limit

        // Register up to limit
        let _r1 = registry
            .register("test", "queue", "worker-1", Duration::from_secs(60))
            .unwrap();
        let _r2 = registry
            .register("test", "queue", "worker-2", Duration::from_secs(60))
            .unwrap();

        // Third registration should be rejected
        let result = registry.register("test", "queue", "worker-3", Duration::from_secs(60));
        assert!(result.is_none());

        // Metrics should show 1 rejected
        assert_eq!(registry.metrics().rejected.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_dispatch_to_specific_worker() {
        let registry = WaitersRegistry::new(100);

        // Register 3 waiters
        let (_id1, _rx1) = registry
            .register("test", "queue", "worker-1", Duration::from_secs(60))
            .unwrap();
        let (_id2, rx2) = registry
            .register("test", "queue", "worker-2", Duration::from_secs(60))
            .unwrap();
        let (_id3, _rx3) = registry
            .register("test", "queue", "worker-3", Duration::from_secs(60))
            .unwrap();

        // Dispatch to worker-2 specifically (skipping worker-1)
        let work = RoutedTask::new(create_test_response("exec-affinity"));
        let result = registry.try_dispatch_to_worker("test", "queue", "worker-2", work);

        assert!(result.is_ok());

        // worker-2 should receive the work
        let received = rx2.await.expect("Should receive work");
        assert_eq!(received.response.execution_id, "exec-affinity");

        // Queue should have worker-1 and worker-3 remaining
        assert_eq!(registry.waiter_count("test", "queue"), 2);
    }

    #[tokio::test]
    async fn test_dispatch_to_specific_worker_not_found() {
        let registry = WaitersRegistry::new(100);

        // Register only worker-1
        let (_id1, _rx1) = registry
            .register("test", "queue", "worker-1", Duration::from_secs(60))
            .unwrap();

        // Try to dispatch to worker-2 (not registered)
        let work = RoutedTask::new(create_test_response("exec-1"));
        let result = registry.try_dispatch_to_worker("test", "queue", "worker-2", work);

        // Should fail and return work
        assert!(result.is_err());

        // Queue should still have worker-1
        assert_eq!(registry.waiter_count("test", "queue"), 1);
    }

    #[tokio::test]
    async fn test_no_waiters_returns_work() {
        let registry = WaitersRegistry::new(100);

        // Try to dispatch with no waiters
        let work = RoutedTask::new(create_test_response("exec-1"));
        let result = registry.try_dispatch("test", "queue", work);

        // Should return the work
        assert!(result.is_err());
        let returned = result.unwrap_err();
        assert_eq!(returned.response.execution_id, "exec-1");
    }

    #[tokio::test]
    async fn test_unregister() {
        let registry = WaitersRegistry::new(100);

        // Register a waiter
        let (wait_id, _rx) = registry
            .register("test", "queue", "worker-1", Duration::from_secs(60))
            .unwrap();

        assert_eq!(registry.waiter_count("test", "queue"), 1);

        // Unregister
        registry.unregister("test", "queue", wait_id);

        assert_eq!(registry.waiter_count("test", "queue"), 0);
        assert_eq!(registry.metrics().unregistered.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_cleanup_expired() {
        let registry = WaitersRegistry::new(100);

        // Register waiters with short timeout
        let (_id1, _rx1) = registry
            .register("test", "queue1", "worker-1", Duration::from_millis(1))
            .unwrap();
        let (_id2, _rx2) = registry
            .register("test", "queue2", "worker-2", Duration::from_millis(1))
            .unwrap();

        // Wait for expiration
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Cleanup
        let expired_count = registry.cleanup_expired();

        assert_eq!(expired_count, 2);
        assert_eq!(registry.total_waiter_count(), 0);
        assert_eq!(registry.queue_count(), 0); // Empty queues removed
    }

    #[test]
    fn test_task_queue_key() {
        let key1 = TaskQueueKey::new("prod", "orders");
        let key2 = TaskQueueKey::from_refs("prod", "orders");
        let key3 = TaskQueueKey::new("prod", "payments");

        assert_eq!(key1, key2);
        assert_ne!(key1, key3);
        assert_eq!(format!("{}", key1), "prod:orders");
    }

    #[tokio::test]
    async fn test_closed_channel_skipped() {
        let registry = WaitersRegistry::new(100);

        // Register a waiter
        let (_id1, rx1) = registry
            .register("test", "queue", "worker-1", Duration::from_secs(60))
            .unwrap();

        // Register another waiter
        let (_id2, rx2) = registry
            .register("test", "queue", "worker-2", Duration::from_secs(60))
            .unwrap();

        // Drop the first receiver (simulates worker disconnect)
        drop(rx1);

        // Dispatch should skip closed worker-1 and go to worker-2
        let work = RoutedTask::new(create_test_response("exec-1"));
        let result = registry.try_dispatch("test", "queue", work);

        assert_eq!(result.unwrap(), "worker-2");

        // worker-2 should receive the work
        let received = rx2.await.expect("Should receive work");
        assert_eq!(received.response.execution_id, "exec-1");
    }

    #[tokio::test]
    async fn test_metrics() {
        let registry = WaitersRegistry::new(100);

        // Register
        let (_id, _rx) = registry
            .register("test", "queue", "worker-1", Duration::from_secs(60))
            .unwrap();

        assert_eq!(registry.metrics().registered.load(Ordering::Relaxed), 1);

        // Dispatch
        let work = RoutedTask::new(create_test_response("exec-1"));
        let _ = registry.try_dispatch("test", "queue", work);

        assert_eq!(registry.metrics().served.load(Ordering::Relaxed), 1);

        // Check sync-match rate
        assert_eq!(registry.metrics().sync_match_rate(), 1.0);
    }
}
