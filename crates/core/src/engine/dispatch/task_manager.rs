//! Task lifecycle management (create, complete, fail).

use orcher_common::{error::OrcherError, OrcherResult};
use orcher_storage::schema::types::coordination::OutboxTask;
use serde_json::Value;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::time::{Duration, Instant};
use tracing::{debug, info, warn};

use crate::engine::dispatch::waiters::{TaskQueueKey, WaitersRegistry};
use crate::engine::queues::outbox::OutboxQueue;
use dashmap::DashMap;
use orcher_dispatch::polling::notifier::Notifier;

/// Task delivered via sync-match (bypasses outbox claim on poll side)
#[derive(Debug)]
pub struct SyncMatchedTask {
    /// The full outbox task payload (contains all fields needed for response),
    /// the attempt it delivers among them as `attempt_number`
    pub payload: Value,
    /// Outbox task ID (to mark as completed after delivery)
    pub outbox_task_id: uuid::Uuid,
    /// When the outbox task was created (for scheduled_time field)
    pub created_at: orcher_common::types::time::Timestamptz,
}

/// Result of polling for a task — either claimed from outbox or sync-matched
#[allow(clippy::large_enum_variant)]
pub enum PollTaskResult {
    /// Task claimed from outbox via DB (existing path)
    Claimed(OutboxTask),
    /// Task delivered directly via sync-match channel (fast path)
    SyncMatched(SyncMatchedTask),
}

/// Request for polling task execution work
#[derive(Debug, Clone)]
pub struct PollTaskRequest {
    /// Task queue to poll from
    pub task_queue: String,

    /// Namespace for isolation
    pub namespace: String,

    /// Identity of the polling service
    pub identity: String,

    /// How long to wait for work (long-poll timeout)
    pub poll_timeout: Duration,

    /// Optional organization ID for multi-tenant task filtering
    pub organization_id: Option<uuid::Uuid>,

    /// The process the poll came from, if the worker said; see
    /// [`crate::worker::shutdowns`].
    pub worker_instance_id: Option<String>,
}

impl PollTaskRequest {
    /// Create a new poll task request
    pub fn new(
        task_queue: impl Into<String>,
        namespace: impl Into<String>,
        identity: impl Into<String>,
        poll_timeout: Duration,
    ) -> Self {
        Self {
            task_queue: task_queue.into(),
            namespace: namespace.into(),
            identity: identity.into(),
            poll_timeout,
            organization_id: None,
            worker_instance_id: None,
        }
    }

    /// Scope this poll request to a specific organization.
    pub fn with_organization_id(mut self, org_id: Option<uuid::Uuid>) -> Self {
        self.organization_id = org_id;
        self
    }

    /// The process the poll came from; empty means it did not say.
    pub fn with_worker_instance_id(mut self, worker_instance_id: Option<String>) -> Self {
        self.worker_instance_id = worker_instance_id.filter(|id| !id.is_empty());
        self
    }

    /// This poll, as far as worker shutdowns are concerned.
    fn as_shutdown_poll(&self) -> crate::worker::shutdowns::Poll<'_> {
        crate::worker::shutdowns::Poll {
            organization_id: self.organization_id,
            namespace: &self.namespace,
            identity: &self.identity,
            worker_instance_id: self.worker_instance_id.as_deref().unwrap_or_default(),
            task_queue: &self.task_queue,
        }
    }
}

/// Task polling manager for orchestrating task execution polling
///
/// Coordinates long-polling, claim operations, and notifications
/// to provide efficient work distribution to services with minimal CPU usage.
/// A waiter for task sync-match: oneshot sender + expiry tracking
struct TaskWaiter {
    sender: oneshot::Sender<SyncMatchedTask>,
    timeout_at: Instant,
}

impl TaskWaiter {
    fn is_expired(&self) -> bool {
        Instant::now() >= self.timeout_at
    }
}

/// Task polling manager for distributing task work to service workers.
pub struct TaskPollingManager {
    /// Outbox queue for claiming tasks
    outbox: Arc<OutboxQueue>,

    /// Notifier for waking waiting services (fallback)
    notifier: Arc<Notifier>,

    /// Waiters registry for workflow sync-match (kept for wake_waiters)
    waiters_registry: Arc<WaitersRegistry>,

    /// Task-specific sync-match waiters: task_queue:namespace → FIFO queue of waiters
    /// Dispatchers try to send SyncMatchedTask directly through these channels
    task_waiters: DashMap<TaskQueueKey, VecDeque<TaskWaiter>>,

    /// Short retry interval when notification-based polling is active
    retry_interval: Duration,

    /// Matching store (partitioned task delivery). When set, a poll claims from the
    /// `(namespace, task_queue)` matching partition instead of scanning the primary
    /// outbox — the claim side of the matching layer. Unset ⇒ legacy outbox claim,
    /// unchanged. Injected once at startup, gated by a flag.
    matching_store: std::sync::OnceLock<
        Arc<dyn orcher_storage::repositories::domains::matching::MatchingStore>,
    >,

    /// The workers that have said they are shutting down, whose polls are
    /// answered with nothing. Unset ⇒ no poll is refused.
    worker_shutdowns: std::sync::OnceLock<Arc<crate::worker::WorkerShutdowns>>,
}

impl TaskPollingManager {
    /// Create a new task polling manager
    ///
    /// # Arguments
    ///
    /// * `outbox` - Outbox queue for task claiming
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let manager = TaskPollingManager::new(outbox_queue);
    /// ```
    pub fn new(outbox: Arc<OutboxQueue>) -> Self {
        info!("Creating task polling manager with sync-match support");

        Self {
            outbox,
            notifier: Arc::new(Notifier::new()),
            waiters_registry: Arc::new(WaitersRegistry::new(10_000)),
            task_waiters: DashMap::new(),
            retry_interval: Duration::from_millis(200),
            matching_store: std::sync::OnceLock::new(),
            worker_shutdowns: std::sync::OnceLock::new(),
        }
    }

    /// Answer the polls of workers that have said they are shutting down with
    /// nothing. A second call is ignored.
    pub fn set_worker_shutdowns(&self, shutdowns: Arc<crate::worker::WorkerShutdowns>) {
        let _ = self.worker_shutdowns.set(shutdowns);
    }

    /// Whether the worker sending `request` has said it is shutting down.
    fn shut_down(&self, request: &PollTaskRequest) -> bool {
        self.worker_shutdowns
            .get()
            .is_some_and(|shutdowns| shutdowns.is_shut_down(&request.as_shutdown_poll()))
    }

    /// Claim a task for `request`, unless its worker is shutting down.
    ///
    /// Asked before the claim, and again after it: a shutdown recorded while
    /// the claim was running is answered as if it had come first, and the task
    /// goes back to the outbox for another worker. From the matching layer,
    /// which has no way to give a claim back, the task is handed over, and its
    /// lease is what brings it back if the worker does not run it.
    async fn claim_unless_shut_down(
        &self,
        request: &PollTaskRequest,
    ) -> OrcherResult<Option<OutboxTask>> {
        if self.shut_down(request) {
            return Ok(None);
        }
        let claimed = self.try_claim_from_outbox(request).await?;
        match claimed {
            Some(task) if self.shut_down(request) && self.matching_store.get().is_none() => {
                if let Err(e) = self.outbox.release_claimed_task(task.task_id).await {
                    warn!(
                        task_id = %task.task_id,
                        error = %e,
                        "Could not give back a task claimed as its worker shut down"
                    );
                }
                Ok(None)
            }
            claimed => Ok(claimed),
        }
    }

    /// Enable the matching layer: claim tasks from the `(namespace, task_queue)`
    /// matching partition instead of the primary outbox. Idempotent; a second call is
    /// ignored. Wired once at startup behind a flag.
    pub fn set_matching_store(
        &self,
        store: Arc<dyn orcher_storage::repositories::domains::matching::MatchingStore>,
    ) {
        let _ = self.matching_store.set(store);
    }

    /// Remove a delivered task's row from its matching partition, when tasks
    /// are delivered from the matching layer; otherwise nothing.
    ///
    /// Called once the task a poll claimed is taken — marked started for the
    /// worker it goes to — or found already taken by another dispatch. Either
    /// way the row has done its work, and a row left behind stays claimed for
    /// good: nothing else ever removed one, so the table grew by a row for
    /// every task delivered. A failure is only logged; the row's lease expires
    /// and the next poll to claim it finds the task taken and drops it then.
    pub async fn settle_matching(&self, task_id: uuid::Uuid) {
        let Some(store) = self.matching_store.get() else {
            return;
        };
        if let Err(e) = store.ack(task_id).await {
            warn!(task_id = %task_id, error = %e, "Could not remove a delivered task's matching row");
        }
    }

    /// Get a reference to the notifier for external notification
    ///
    /// This allows other components (like CommandProcessor) to notify
    /// the manager when new tasks are available.
    pub fn notifier(&self) -> Arc<Notifier> {
        Arc::clone(&self.notifier)
    }

    /// Get a reference to the waiters registry
    pub fn waiters_registry(&self) -> &Arc<WaitersRegistry> {
        &self.waiters_registry
    }

    /// Try to deliver a task directly to a waiting poller (sync-match).
    ///
    /// If a poller is registered and waiting, sends the task through its oneshot
    /// channel — the poller gets the task without any DB reads.
    /// If no poller is waiting, falls back to wake notification so the poller
    /// claims from the outbox.
    ///
    /// Returns true if sync-match succeeded (task delivered directly).
    pub fn try_sync_match_task(
        &self,
        task_queue: &str,
        namespace: &str,
        task: SyncMatchedTask,
    ) -> bool {
        let key = TaskQueueKey::from_refs(namespace, task_queue);

        if let Some(mut queue) = self.task_waiters.get_mut(&key) {
            // Pop waiters from front until we find a valid one
            while let Some(waiter) = queue.pop_front() {
                if waiter.is_expired() || waiter.sender.is_closed() {
                    continue;
                }
                match waiter.sender.send(task) {
                    Ok(()) => {
                        debug!(
                            task_queue = %task_queue,
                            namespace = %namespace,
                            "Sync-match: task delivered directly to poller (0 DB reads)"
                        );
                        return true;
                    }
                    Err(_returned_task) => {
                        // Channel closed between check and send — try next waiter
                        // But we lost the task data. Fall through to wake path.
                        warn!("Sync-match channel closed during send, falling back to wake");
                        break;
                    }
                }
            }
        }

        // No sync-match — fall back to wake notification
        let woken = self.waiters_registry.wake_waiters(namespace, task_queue);
        if woken == 0 {
            let notifier = self.notifier.clone();
            let tq = task_queue.to_string();
            let ns = namespace.to_string();
            tokio::spawn(async move {
                notifier.notify_work_available(&tq, &ns).await;
            });
        }
        false
    }

    /// Poll for task execution work (long-poll with notifications)
    ///
    /// Attempts to claim a task from the outbox queue. If no task is
    /// immediately available, blocks up to `request.poll_timeout` waiting for
    /// work to become available via notifications.
    ///
    /// This implementation prevents CPU spinning by:
    /// 1. Using notifications instead of tight polling loops
    /// 2. Only querying the database when notified or on initial attempt
    /// 3. Sleeping between retries to avoid hammering the database
    ///
    /// # Arguments
    ///
    /// * `request` - Poll request with task_queue, namespace, identity, timeout
    ///
    /// # Returns
    ///
    /// - `Ok(Some(task))` - Task was claimed and returned
    /// - `Ok(None)` - No task available (poll timeout)
    /// - `Err(_)` - Database error
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let request = PollTaskRequest::new(
    ///     "etl-jobs",
    ///     "data-pipeline",
    ///     "worker-1",
    ///     Duration::from_secs(60),
    /// );
    ///
    /// match manager.poll_task(request).await? {
    ///     Some(task) => {
    ///         // Execute task
    ///         process_task(task).await?;
    ///     }
    ///     None => {
    ///         // No work available, poll again
    ///     }
    /// }
    /// ```
    pub async fn poll_task(
        &self,
        request: PollTaskRequest,
    ) -> OrcherResult<Option<PollTaskResult>> {
        debug!(
            task_queue = %request.task_queue,
            namespace = %request.namespace,
            identity = %request.identity,
            poll_timeout_secs = request.poll_timeout.as_secs(),
            "TaskPollingManager::poll_task ENTRY"
        );

        let start = Instant::now();
        let deadline = start + request.poll_timeout;

        // First attempt: try claiming from outbox (handles tasks dispatched before we registered)
        if let Some(task) = self.claim_unless_shut_down(&request).await? {
            return Ok(Some(PollTaskResult::Claimed(task)));
        }

        // Ends the waits below when a shutdown covering this poll is added.
        let shutdown = crate::worker::shutdowns::ShutdownWatch::new(
            self.worker_shutdowns.get().map(|s| &**s),
            request.as_shutdown_poll(),
        );

        // Main loop: register for sync-match and fall back to outbox claim
        loop {
            // A worker that has said it is shutting down is answered now, not
            // at the end of its poll timeout.
            if shutdown.is_shut_down() {
                debug!(
                    task_queue = %request.task_queue,
                    namespace = %request.namespace,
                    identity = %request.identity,
                    "TaskPollingManager: worker is shutting down, answering with no task"
                );
                return Ok(None);
            }

            // Check if timeout reached
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                debug!(
                    task_queue = %request.task_queue,
                    namespace = %request.namespace,
                    elapsed_ms = start.elapsed().as_millis(),
                    "TaskPollingManager: Poll timeout reached"
                );
                return Ok(None);
            }

            let wait_duration = remaining.min(self.retry_interval);

            // Register with WaitersRegistry for targeted wake (notify_one)
            if let Some((_wait_id, _receiver, wake_notify)) =
                self.waiters_registry.register_with_wake(
                    &request.namespace,
                    &request.task_queue,
                    &request.identity,
                    wait_duration,
                )
            {
                // Wait for wake signal or timeout, then claim from outbox
                tokio::select! {
                    _ = wake_notify.notified() => {}
                    _ = tokio::time::sleep(wait_duration) => {}
                    _ = shutdown.wait() => continue,
                }
            } else {
                // Registry full — fall back to generic notifier
                let notify = self
                    .notifier
                    .get_or_create(&request.task_queue, &request.namespace)
                    .await;
                tokio::select! {
                    _ = tokio::time::timeout(wait_duration, notify.notified()) => {}
                    _ = shutdown.wait() => continue,
                }
            }
            if let Some(task) = self.claim_unless_shut_down(&request).await? {
                return Ok(Some(PollTaskResult::Claimed(task)));
            }

            tokio::task::yield_now().await;
        }
    }

    /// Try to claim a matching task from the outbox
    /// Claim the next ready task from this poll's `(namespace, task_queue)` matching
    /// partition, adapting it to an [`OutboxTask`] for the existing response path. The
    /// matching `task_id` is preserved so completion acks the right partition row.
    async fn claim_from_matching(
        &self,
        store: &dyn orcher_storage::repositories::domains::matching::MatchingStore,
        request: &PollTaskRequest,
    ) -> OrcherResult<Option<OutboxTask>> {
        use orcher_storage::schema::types::coordination::OutboxTaskType;

        // Lease long enough for the worker to build a response and start the task; the
        // matching reclaimer returns an expired lease to `ready`.
        let lease = Duration::from_secs(300);
        let claimed = store
            .claim(
                &request.namespace,
                &request.task_queue,
                &request.identity,
                lease,
                request.organization_id,
            )
            .await?;

        Ok(claimed.map(|mt| {
            let mut task = OutboxTask::new(
                mt.workflow_id,
                OutboxTaskType::DispatchTask,
                mt.payload,
                None,
            );
            task.task_id = mt.task_id;
            task
        }))
    }

    async fn try_claim_from_outbox(
        &self,
        request: &PollTaskRequest,
    ) -> OrcherResult<Option<OutboxTask>> {
        // Matching layer (claim): when wired, claim from the (namespace, task_queue)
        // partition — already SQL-filtered to this queue, so no primary-outbox scan and
        // no in-code namespace/task_queue matching.
        if let Some(store) = self.matching_store.get() {
            return self.claim_from_matching(store.as_ref(), request).await;
        }

        // One task, of this poll's own queue: the claim is filtered to it in
        // the statement, so there is nothing to claim and give back. Claiming
        // across queues and releasing what did not match held other queues'
        // rows away from their own pollers meanwhile, and let a workflow's
        // dispatch on a queue nobody serves stand in front of its others.
        let tasks = self
            .outbox
            .claim_dispatch_tasks(
                request.organization_id,
                1,
                &request.task_queue,
                &request.namespace,
            )
            .await
            .map_err(|e| OrcherError::Internal {
                message: format!("Failed to claim tasks from outbox: {}", e),
                source: Some(e.to_string()),
            })?;

        Ok(tasks.into_iter().next())
    }

    /// Settle the outbox row of a dispatch a poll claimed but could not
    /// deliver because its task is no longer waiting: started by another
    /// dispatch, or ended before it started. The row has nothing left to
    /// deliver; left `processing`, it was put back to `pending` by the
    /// stuck-task sweep and claimed again every few minutes for good. From the
    /// matching layer, the partition row is removed instead.
    pub async fn settle_undeliverable(&self, task_id: uuid::Uuid) {
        if self.matching_store.get().is_some() {
            return self.settle_matching(task_id).await;
        }
        if let Err(e) = self.outbox.settle_claimed(task_id).await {
            warn!(task_id = %task_id, error = %e, "Could not settle an undeliverable task's outbox row");
        }
    }

    /// Notify waiting pollers that new task work is available
    ///
    /// This should be called by:
    /// - CommandProcessor when tasks are dispatched to outbox
    /// - Retry logic when tasks are scheduled for retry
    /// - Any component that adds work to the outbox
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // After dispatching tasks to outbox
    /// outbox.enqueue_tasks(tasks).await?;
    /// task_manager.notify_work_available("etl-jobs", "data-pipeline").await;
    /// ```
    pub async fn notify_work_available(&self, task_queue: &str, namespace: &str) {
        debug!(
            task_queue = %task_queue,
            namespace = %namespace,
            "Notifying waiting services that task work is available"
        );
        // Wake a parked poller directly so it claims the new task immediately,
        // rather than idling the full `retry_interval` before its next scan.
        // `wake_waiters` notifies exactly one waiter (avoiding claim races), and
        // delivery still goes through the atomic outbox scan-claim, so
        // exactly-once execution is unchanged — this only removes wake latency.
        self.waiters_registry.wake_waiters(namespace, task_queue);
        // Also poke the generic notifier, which covers pollers that fell back to
        // it when the waiter registry was full.
        self.notifier
            .notify_work_available(task_queue, namespace)
            .await;
    }

    /// Get statistics about task polling (for debugging/monitoring)
    ///
    /// Returns information about the task polling manager.
    pub fn get_stats(&self) -> TaskPollingStats {
        TaskPollingStats {
            // Note: Notifier doesn't expose waiter count
            // This is intentional to keep the API simple
        }
    }
}

/// Statistics about task polling manager
#[derive(Debug, Clone)]
pub struct TaskPollingStats {
    // Currently empty - can be extended with metrics in the future
    // Examples: notification_count, poll_count, timeout_count, etc.
}

#[cfg(test)]
mod tests {
    use super::*;
    use orcher_storage::repositories::domains::coordination::OutboxRepository;
    use sqlx::PgPool;

    async fn create_test_pool() -> PgPool {
        // Use test database
        let database_url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://localhost/orcher_test".to_string());
        PgPool::connect(&database_url)
            .await
            .expect("Failed to connect to test database")
    }

    #[tokio::test]
    #[ignore] // Requires database
    async fn test_poll_task_immediate_work() {
        let pool = create_test_pool().await;
        let repo = Arc::new(OutboxRepository::new(Arc::new(pool)));
        let outbox = Arc::new(OutboxQueue::new(repo));
        let manager = TaskPollingManager::new(outbox);

        // TODO: Create test task in outbox
        // For now, test structure is in place

        let request = PollTaskRequest::new(
            "test-queue",
            "default",
            "test-worker",
            Duration::from_secs(1),
        );

        // If task exists, should return immediately
        let result = manager.poll_task(request).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    #[ignore] // Requires database with outbox_queue schema (organization_id column)
    async fn test_poll_task_timeout() {
        // Create manager with empty outbox (in-memory for this test)
        let pool = create_test_pool().await;
        let repo = Arc::new(OutboxRepository::new(Arc::new(pool)));
        let outbox = Arc::new(OutboxQueue::new(repo));
        let manager = TaskPollingManager::new(outbox);

        let request = PollTaskRequest::new(
            "empty-queue",
            "default",
            "test-worker",
            Duration::from_millis(500), // Short timeout for test
        );

        let start = Instant::now();
        let result = manager.poll_task(request).await.unwrap();
        let elapsed = start.elapsed();

        // Should timeout and return None
        assert!(result.is_none());
        // Should wait approximately the timeout duration
        assert!(elapsed >= Duration::from_millis(400));
        assert!(elapsed <= Duration::from_millis(700));
    }

    #[tokio::test]
    #[ignore] // Requires database with outbox_queue schema (organization_id column)
    async fn test_poll_task_with_notification() {
        let pool = create_test_pool().await;
        let repo = Arc::new(OutboxRepository::new(Arc::new(pool)));
        let outbox = Arc::new(OutboxQueue::new(repo));
        let manager = Arc::new(TaskPollingManager::new(outbox));

        // Spawn a task that polls (will wait for notification)
        let manager_clone = Arc::clone(&manager);
        let poll_handle = tokio::spawn(async move {
            let request = PollTaskRequest::new(
                "test-queue",
                "default",
                "test-worker",
                Duration::from_secs(10),
            );
            manager_clone.poll_task(request).await
        });

        // Give poller time to start waiting
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Send notification
        manager.notify_work_available("test-queue", "default").await;

        // Poller should wake up and check for work
        // (In this test, it will still return None since no task was added,
        // but the notification mechanism is tested)
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Cancel the poll (since we didn't add actual work)
        poll_handle.abort();
    }

    #[test]
    fn test_poll_request_creation() {
        let request = PollTaskRequest::new(
            "orders-queue",
            "production",
            "worker-123",
            Duration::from_secs(60),
        );

        assert_eq!(request.task_queue, "orders-queue");
        assert_eq!(request.namespace, "production");
        assert_eq!(request.identity, "worker-123");
        assert_eq!(request.poll_timeout, Duration::from_secs(60));
    }
}
