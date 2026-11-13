//! Actor operation routing and dispatch.
//!
//! Implements fair readers–writer scheduling per actor key:
//!
//! - **Exclusive** operations (the default) run strictly one at a time per
//!   actor key — single-writer semantics.
//! - **Shared** operations may run concurrently with other shared operations
//!   on the same key, up to [`MAX_SHARED_IN_FLIGHT`].
//! - An exclusive operation acts as a **barrier**: when one arrives, admission
//!   to the current shared batch closes, the batch drains, the exclusive
//!   operation runs alone, and only then does the queue continue. Shared
//!   operations that arrive while an exclusive is waiting queue *behind* it,
//!   so a continuous stream of reads can never starve a write.
//! - Operations on *different* keys are always independent.
//!
//! The effective mode of an operation is resolved from worker handler
//! registrations (`ServiceRegistry::resolve_operation_mode`) — callers cannot
//! request a mode.

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use orcher_common::OrcherResult;
use orcher_proto::orcher::v1::{ActorOperation, ExecutionStatus, OperationMode};
use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, Mutex, Notify};
use tokio::task::JoinSet;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// How long the dispatcher waits for a polled operation to be completed by a
/// worker before failing it and unblocking the key. Without this backstop, a
/// worker that polls an operation and then dies would wedge its actor key
/// forever.
const DEFAULT_COMPLETION_TIMEOUT: Duration = Duration::from_secs(300);

/// How long a per-key dispatcher lingers idle before shutting down and
/// removing its queue.
const IDLE_QUEUE_TTL: Duration = Duration::from_secs(60);

/// Maximum shared operations in flight concurrently for a single actor key.
/// Acts as backpressure so one hot key cannot flood the work queue.
const MAX_SHARED_IN_FLIGHT: usize = 32;

/// Attempts to enqueue an operation before giving up. An attempt can fail
/// only when it races a dispatcher shutting down its queue, in which case the
/// stale queue is dropped and the send is retried against a fresh one.
const ENQUEUE_ATTEMPTS: usize = 3;

/// Pending operation in the per-key queue
#[derive(Debug)]
struct PendingOperation {
    /// Unique operation ID
    operation_id: String,

    /// Actor type name
    actor_name: String,

    /// Actor instance key
    key: String,

    /// Operation name
    operation: String,

    /// Serialized payload
    payload: Vec<u8>,

    /// Operation mode
    mode: OperationMode,

    /// Execution ID for tracing
    execution_id: String,

    /// Request metadata
    metadata: HashMap<String, String>,

    /// The organization that invoked the operation, if the caller declared one
    organization_id: Option<Uuid>,

    /// Channel to send result back to client
    response_tx: oneshot::Sender<OrcherResult<Vec<u8>>>,
}

/// Operation ready for worker polling
#[derive(Debug, Clone)]
struct ReadyOperation {
    /// Unique operation ID
    operation_id: String,

    /// Actor type name
    actor_name: String,

    /// Actor instance key
    key: String,

    /// Operation name
    operation: String,

    /// Serialized payload
    payload: Vec<u8>,

    /// Operation mode
    mode: OperationMode,

    /// Execution ID for tracing
    execution_id: String,

    /// Request metadata
    metadata: HashMap<String, String>,

    /// The organization that invoked the operation, if the caller declared one
    organization_id: Option<Uuid>,
}

/// Completed operation result
pub struct OperationResult {
    /// Serialized result
    pub result: Vec<u8>,

    /// Execution status
    pub status: ExecutionStatus,

    /// Error message (if failed)
    pub error_message: String,

    /// Error code (if failed)
    pub error_code: String,

    /// Execution duration
    pub duration_ms: u64,
}

/// Bookkeeping for an operation that has been dispatched to the work queue
/// and not yet completed.
///
/// `done_tx` is dropped (or fired) exactly when this entry leaves
/// `pending_results`, so the dispatcher can treat *any* resolution of the
/// paired receiver as "this operation finished".
#[derive(Debug)]
struct PendingEntry {
    /// Channel that answers the waiting client.
    response_tx: oneshot::Sender<OrcherResult<Vec<u8>>>,

    /// Completion signal for the per-key dispatcher.
    done_tx: oneshot::Sender<()>,

    /// The organization that invoked the operation. Only a caller from it may
    /// answer the operation.
    organization_id: Option<Uuid>,
}

/// Per-key queue handle
#[derive(Debug, Clone)]
struct KeyQueue {
    /// Channel for sending operations to the key dispatcher
    tx: mpsc::UnboundedSender<PendingOperation>,

    /// Number of operations currently queued or in flight for this key
    in_flight: Arc<Mutex<usize>>,

    /// Queue generation id, distinguishing successive queues for the same key
    /// so a dispatcher shutting down never removes its successor's entry.
    queue_id: u64,
}

/// Actor router: fair readers–writer scheduling per actor key, with a
/// polling dispatch model.
#[derive(Clone)]
pub struct ActorRouter {
    /// Per-key queues
    key_queues: Arc<DashMap<String, KeyQueue>>,

    /// Global work queue for worker polling
    work_queue: Arc<Mutex<Vec<ReadyOperation>>>,

    /// Notify for new work availability
    work_notify: Arc<Notify>,

    /// Dispatched-but-not-completed operations (operation_id -> entry)
    pending_results: Arc<DashMap<String, PendingEntry>>,

    /// Count of shared operations currently in flight per actor key.
    /// Non-zero means the key is in a shared (read-only) window and state
    /// writes are rejected — see [`ActorRouter::state_writes_blocked`].
    shared_in_flight: Arc<DashMap<String, usize>>,

    /// Monotonic source for queue generation ids.
    queue_seq: Arc<AtomicU64>,

    /// How long to wait for a dispatched operation's completion.
    completion_timeout: Duration,
}

impl ActorRouter {
    /// Create a new actor router
    pub fn new() -> Self {
        Self {
            key_queues: Arc::new(DashMap::new()),
            work_queue: Arc::new(Mutex::new(Vec::new())),
            work_notify: Arc::new(Notify::new()),
            pending_results: Arc::new(DashMap::new()),
            shared_in_flight: Arc::new(DashMap::new()),
            queue_seq: Arc::new(AtomicU64::new(0)),
            completion_timeout: DEFAULT_COMPLETION_TIMEOUT,
        }
    }

    /// Override the completion timeout (primarily for tests).
    pub fn with_completion_timeout(mut self, timeout: Duration) -> Self {
        self.completion_timeout = timeout;
        self
    }

    /// The queue key for an actor instance of one organization.
    ///
    /// Two organizations may use the same actor name and key; they are
    /// different instances. Sharing a queue made one wait on the other: an
    /// exclusive operation for an organization with no worker online, which
    /// only its own workers may now take, held the key until it timed out.
    fn build_tenant_actor_key(
        organization_id: Option<Uuid>,
        actor_name: &str,
        key: &str,
    ) -> String {
        match organization_id {
            Some(organization_id) => format!("{}/{}:{}", organization_id, actor_name, key),
            None => format!("{}:{}", actor_name, key),
        }
    }

    /// Invoke an operation (called by client via InvokeOperation RPC)
    ///
    /// Queues the operation under fair readers–writer scheduling for its key.
    /// The operation will be picked up by a worker via polling.
    pub async fn invoke_operation(
        &self,
        actor_name: String,
        key: String,
        operation: String,
        payload: Vec<u8>,
        mode: OperationMode,
        execution_id: String,
        metadata: HashMap<String, String>,
    ) -> OrcherResult<(String, oneshot::Receiver<OrcherResult<Vec<u8>>>)> {
        self.invoke_operation_for(
            None,
            actor_name,
            key,
            operation,
            payload,
            mode,
            execution_id,
            metadata,
        )
        .await
    }

    /// [`invoke_operation`](Self::invoke_operation) on behalf of an
    /// organization. The operation is then dispatched only to that
    /// organization's workers, and only they may complete it.
    #[allow(clippy::too_many_arguments)]
    pub async fn invoke_operation_for(
        &self,
        organization_id: Option<Uuid>,
        actor_name: String,
        key: String,
        operation: String,
        payload: Vec<u8>,
        mode: OperationMode,
        execution_id: String,
        metadata: HashMap<String, String>,
    ) -> OrcherResult<(String, oneshot::Receiver<OrcherResult<Vec<u8>>>)> {
        debug!(
            "Invoking operation: actor={}, key={}, operation={}, mode={:?}, execution_id={}",
            actor_name, key, operation, mode, execution_id
        );

        let operation_id = Uuid::new_v4().to_string();
        let (response_tx, response_rx) = oneshot::channel();

        let pending_op = PendingOperation {
            operation_id: operation_id.clone(),
            actor_name,
            key,
            operation,
            payload,
            mode,
            execution_id,
            metadata,
            organization_id,
            response_tx,
        };

        self.enqueue(pending_op).await?;

        Ok((operation_id, response_rx))
    }

    /// Enqueue an operation onto its key's queue, retrying if the send races
    /// a dispatcher shutting the queue down.
    async fn enqueue(&self, mut op: PendingOperation) -> OrcherResult<()> {
        let actor_key = Self::build_tenant_actor_key(op.organization_id, &op.actor_name, &op.key);

        for attempt in 0..ENQUEUE_ATTEMPTS {
            let queue = self.get_or_create_queue(&actor_key);

            // Increment before send so an idle-check never observes a queued
            // operation with a zero in-flight count.
            {
                let mut in_flight = queue.in_flight.lock().await;
                *in_flight += 1;
            }

            match queue.tx.send(op) {
                Ok(()) => {
                    debug!(
                        "Operation queued: actor_key={}, queue_id={}, attempt={}",
                        actor_key, queue.queue_id, attempt
                    );
                    return Ok(());
                }
                Err(mpsc::error::SendError(returned)) => {
                    // The dispatcher for this queue generation is shutting
                    // down. Undo our accounting, drop the stale entry (guarded
                    // by generation id), and retry against a fresh queue.
                    op = returned;
                    {
                        let mut in_flight = queue.in_flight.lock().await;
                        *in_flight = in_flight.saturating_sub(1);
                    }
                    self.key_queues
                        .remove_if(&actor_key, |_, q| q.queue_id == queue.queue_id);
                    debug!(
                        "Queue closed during enqueue, retrying: actor_key={}, queue_id={}",
                        actor_key, queue.queue_id
                    );
                }
            }
        }

        error!(
            "Failed to enqueue operation after {} attempts: actor_key={}",
            ENQUEUE_ATTEMPTS, actor_key
        );
        Err(orcher_common::error::OrcherError::internal(format!(
            "Failed to queue operation for actor key {}",
            actor_key
        )))
    }

    /// Poll for operations to execute (called by worker via PollActorOperation RPC)
    ///
    /// This is a long-poll that blocks until work is available or timeout.
    ///
    /// `can_handle(actor_name, operation)` filters the work queue to operations
    /// this worker actually registered a handler for — dispatch is otherwise a
    /// single global queue, so without the filter a worker would be handed an
    /// operation for an actor it does not implement and reject it. Operations
    /// no polling worker can handle stay queued (and are failed by the
    /// dispatcher's completion timeout if no capable worker ever appears)
    /// rather than being misrouted. Matching preserves FIFO order.
    pub async fn poll_operations(
        &self,
        service_id: &str,
        max_operations: usize,
        timeout: Duration,
        can_handle: impl Fn(&str, &str) -> bool,
    ) -> OrcherResult<Vec<ActorOperation>> {
        self.poll_operations_for(None, service_id, max_operations, timeout, can_handle)
            .await
    }

    /// [`poll_operations`](Self::poll_operations) for a worker of one
    /// organization, which is handed only that organization's operations.
    /// The queue is shared by every tenant, so without this a worker would be
    /// given another organization's payload and the invoker's answer would
    /// then be refused to it at completion.
    pub async fn poll_operations_for(
        &self,
        organization_id: Option<Uuid>,
        service_id: &str,
        max_operations: usize,
        timeout: Duration,
        can_handle: impl Fn(&str, &str) -> bool,
    ) -> OrcherResult<Vec<ActorOperation>> {
        debug!(
            "Worker polling for operations: service_id={}, max_operations={}, timeout={:?}",
            service_id, max_operations, timeout
        );

        let deadline = tokio::time::Instant::now() + timeout;

        loop {
            // Check for available work this worker is capable of executing.
            {
                let mut queue = self.work_queue.lock().await;
                if !queue.is_empty() {
                    // Take up to `max_operations` operations this worker can
                    // handle, preserving queue order; leave the rest for a
                    // capable worker to poll.
                    let mut operations: Vec<ReadyOperation> = Vec::new();
                    let mut i = 0;
                    while i < queue.len() && operations.len() < max_operations {
                        if crate::grpc::tenant::may_act_on(
                            queue[i].organization_id,
                            organization_id,
                        ) && can_handle(&queue[i].actor_name, &queue[i].operation)
                        {
                            operations.push(queue.remove(i));
                        } else {
                            i += 1;
                        }
                    }

                    if !operations.is_empty() {
                        debug!(
                            "Dispatching operations to worker: service_id={}, count={}",
                            service_id,
                            operations.len()
                        );

                        let actor_operations: Vec<ActorOperation> = operations
                            .into_iter()
                            .map(|op| ActorOperation {
                                operation_id: op.operation_id,
                                actor_name: op.actor_name,
                                key: op.key,
                                operation: op.operation,
                                payload: op.payload,
                                mode: op.mode as i32,
                                execution_id: op.execution_id,
                                metadata: op.metadata,
                            })
                            .collect();

                        return Ok(actor_operations);
                    }
                    // Work exists but none of it is for this worker: fall
                    // through and long-poll for work we can handle.
                }
            }

            // Check timeout
            if tokio::time::Instant::now() >= deadline {
                debug!("Poll timeout: service_id={}", service_id);
                return Ok(Vec::new());
            }

            // Wait for notification or timeout
            tokio::select! {
                _ = self.work_notify.notified() => {
                    continue;
                }
                _ = tokio::time::sleep_until(deadline) => {
                    debug!("Poll timeout: service_id={}", service_id);
                    return Ok(Vec::new());
                }
            }
        }
    }

    /// Complete an operation (called by worker via CompleteActorOperation RPC)
    ///
    /// Delivers the result back to the waiting client and wakes the key's
    /// dispatcher.
    pub async fn complete_operation(
        &self,
        operation_id: String,
        result: OperationResult,
    ) -> OrcherResult<()> {
        self.complete_operation_as(None, operation_id, result).await
    }

    /// [`complete_operation`](Self::complete_operation) by a caller of the
    /// given organization. An operation another organization invoked is
    /// answered exactly as an unknown one is, and stays pending for its own
    /// organization's worker: an operation id is all a caller needs to name
    /// it, and naming it must not be enough to answer it.
    pub async fn complete_operation_as(
        &self,
        caller: Option<Uuid>,
        operation_id: String,
        result: OperationResult,
    ) -> OrcherResult<()> {
        debug!(
            "Completing operation: operation_id={}, status={:?}",
            operation_id, result.status
        );

        let entry = match self.pending_results.remove_if(&operation_id, |_, entry| {
            crate::grpc::tenant::may_act_on(entry.organization_id, caller)
        }) {
            Some((_, entry)) => entry,
            None => {
                warn!(
                    "No pending result channel for operation: operation_id={}",
                    operation_id
                );
                return Err(orcher_common::error::OrcherError::not_found(
                    "Operation",
                    &operation_id,
                ));
            }
        };

        let send_result = if result.status == ExecutionStatus::Success {
            Ok(result.result)
        } else {
            Err(orcher_common::error::OrcherError::internal(format!(
                "Operation failed: {} ({})",
                result.error_message, result.error_code
            )))
        };

        if entry.response_tx.send(send_result).is_err() {
            warn!(
                "Failed to send operation result, client may have disconnected: operation_id={}",
                operation_id
            );
        }

        // Wake the key dispatcher (dropping the sender would also do this;
        // sending is explicit).
        let _ = entry.done_tx.send(());

        info!(
            "Operation completed: operation_id={}, duration_ms={}",
            operation_id, result.duration_ms
        );

        Ok(())
    }

    /// Whether state writes are currently rejected for this actor instance.
    ///
    /// True while shared (read-only) operations are in flight for the key: a
    /// write racing concurrent shared operations would break their isolation,
    /// so `SetState`/`DeleteState` are refused during a shared window.
    ///
    /// Enforcement is per-key rather than per-operation because the state RPCs
    /// carry no operation identity today; adding `operation_id` to the state
    /// protos would allow airtight attribution (tracked follow-up).
    pub fn state_writes_blocked(&self, actor_name: &str, key: &str) -> bool {
        self.state_writes_blocked_for(None, actor_name, key)
    }

    /// [`state_writes_blocked`](Self::state_writes_blocked) for an actor
    /// instance of one organization, whose shared window is its own.
    pub fn state_writes_blocked_for(
        &self,
        organization_id: Option<Uuid>,
        actor_name: &str,
        key: &str,
    ) -> bool {
        let actor_key = Self::build_tenant_actor_key(organization_id, actor_name, key);
        self.shared_in_flight
            .get(&actor_key)
            .map(|count| *count > 0)
            .unwrap_or(false)
    }

    /// Get or create the queue for an actor key.
    ///
    /// Uses the map's entry API so two concurrent creators cannot both spawn
    /// a dispatcher for the same key.
    ///
    /// Deliberately a sync fn: the dispatcher it spawns calls back into
    /// `enqueue` (which calls this) when forwarding operations at shutdown,
    /// and keeping this function non-async breaks the recursive-async type
    /// cycle that would otherwise prevent the compiler from proving the
    /// futures `Send`.
    fn get_or_create_queue(&self, actor_key: &str) -> KeyQueue {
        if let Some(queue) = self.key_queues.get(actor_key) {
            return queue.value().clone();
        }

        let (tx, rx) = mpsc::unbounded_channel();
        let in_flight = Arc::new(Mutex::new(0usize));
        let queue_id = self.queue_seq.fetch_add(1, Ordering::Relaxed);

        let queue = KeyQueue {
            tx,
            in_flight: in_flight.clone(),
            queue_id,
        };

        match self.key_queues.entry(actor_key.to_string()) {
            Entry::Occupied(existing) => {
                // Lost the creation race; use the winner. Our rx drops here,
                // so no dispatcher is spawned for the losing queue.
                existing.get().clone()
            }
            Entry::Vacant(slot) => {
                slot.insert(queue.clone());
                info!(
                    "Created actor key queue: actor_key={}, queue_id={}",
                    actor_key, queue_id
                );

                let router = self.clone();
                let key = actor_key.to_string();
                tokio::spawn(async move {
                    router
                        .run_key_dispatcher(key, queue_id, rx, in_flight)
                        .await;
                });

                queue
            }
        }
    }

    /// Per-key dispatcher implementing the fair readers–writer schedule.
    async fn run_key_dispatcher(
        self,
        actor_key: String,
        queue_id: u64,
        mut rx: mpsc::UnboundedReceiver<PendingOperation>,
        in_flight: Arc<Mutex<usize>>,
    ) {
        debug!(
            "Started key dispatcher: actor_key={}, queue_id={}",
            actor_key, queue_id
        );

        // An exclusive operation received while a shared batch was draining.
        let mut carry: Option<PendingOperation> = None;

        loop {
            let op = match carry.take() {
                Some(op) => op,
                None => match tokio::time::timeout(IDLE_QUEUE_TTL, rx.recv()).await {
                    Ok(Some(op)) => op,
                    // All senders dropped (our map entry was replaced).
                    Ok(None) => break,
                    Err(_) => {
                        // Idle. Shut down only if truly quiescent: enqueue()
                        // increments in_flight before sending, so a buffered
                        // operation always shows a non-zero count here.
                        if *in_flight.lock().await == 0 {
                            break;
                        }
                        continue;
                    }
                },
            };

            if op.mode == OperationMode::Shared {
                self.run_shared_batch(&actor_key, op, &mut rx, &mut carry, &in_flight)
                    .await;
            } else {
                // Exclusive: dispatch alone and wait for completion before
                // touching the next operation (single-writer-per-key).
                let completion = self.dispatch(&actor_key, op).await;
                completion.await;
                let mut count = in_flight.lock().await;
                *count = count.saturating_sub(1);
            }
        }

        // Shutdown: unpublish this queue generation first, then stop accepting
        // sends, then forward anything that raced in to the key's current
        // queue. The old dispatcher never *dispatches* after unpublishing, so
        // there is never more than one dispatcher scheduling a key.
        self.key_queues
            .remove_if(&actor_key, |_, q| q.queue_id == queue_id);
        rx.close();
        while let Some(op) = rx.recv().await {
            {
                let mut count = in_flight.lock().await;
                *count = count.saturating_sub(1);
            }
            debug!(
                "Forwarding operation that raced queue shutdown: actor_key={}, operation_id={}",
                actor_key, op.operation_id
            );
            if let Err(e) = self.enqueue(op).await {
                // The response channel inside `op` is dropped with it, which
                // surfaces a channel-closed error to the waiting client.
                error!(
                    "Failed to forward operation after queue shutdown: actor_key={}, error={}",
                    actor_key, e
                );
            }
        }

        debug!(
            "Key dispatcher stopped: actor_key={}, queue_id={}",
            actor_key, queue_id
        );
    }

    /// Run a batch of shared operations concurrently.
    ///
    /// The batch starts with `first` and admits further shared operations as
    /// they arrive, up to [`MAX_SHARED_IN_FLIGHT`]. Admission closes when an
    /// exclusive operation arrives (it becomes `carry`, forming a barrier) or
    /// the channel closes; the batch then drains before returning.
    async fn run_shared_batch(
        &self,
        actor_key: &str,
        first: PendingOperation,
        rx: &mut mpsc::UnboundedReceiver<PendingOperation>,
        carry: &mut Option<PendingOperation>,
        in_flight: &Arc<Mutex<usize>>,
    ) {
        let mut completions: JoinSet<()> = JoinSet::new();
        completions.spawn(self.dispatch(actor_key, first).await);
        let mut admitting = true;

        while !completions.is_empty() {
            if admitting && completions.len() < MAX_SHARED_IN_FLIGHT {
                tokio::select! {
                    next = rx.recv() => match next {
                        Some(op) if op.mode == OperationMode::Shared => {
                            completions.spawn(self.dispatch(actor_key, op).await);
                        }
                        Some(op) => {
                            // Exclusive arrived: close admission. It runs after
                            // the batch drains; shared operations arriving from
                            // now on queue behind it (no writer starvation).
                            *carry = Some(op);
                            admitting = false;
                        }
                        None => {
                            admitting = false;
                        }
                    },
                    Some(_) = completions.join_next() => {
                        let mut count = in_flight.lock().await;
                        *count = count.saturating_sub(1);
                    }
                }
            } else if completions.join_next().await.is_some() {
                let mut count = in_flight.lock().await;
                *count = count.saturating_sub(1);
            }
        }
    }

    /// Dispatch an operation to the work queue and return a future that
    /// resolves when the operation completes — or times out, in which case
    /// the client is failed and the key unblocked.
    async fn dispatch(
        &self,
        actor_key: &str,
        op: PendingOperation,
    ) -> impl Future<Output = ()> + Send + 'static {
        let PendingOperation {
            operation_id,
            actor_name,
            key,
            operation,
            payload,
            mode,
            execution_id,
            metadata,
            organization_id,
            response_tx,
        } = op;

        let (done_tx, done_rx) = oneshot::channel();
        let is_shared = mode == OperationMode::Shared;

        self.pending_results.insert(
            operation_id.clone(),
            PendingEntry {
                response_tx,
                done_tx,
                organization_id,
            },
        );

        if is_shared {
            *self
                .shared_in_flight
                .entry(actor_key.to_string())
                .or_insert(0) += 1;
        }

        {
            let mut queue = self.work_queue.lock().await;
            queue.push(ReadyOperation {
                operation_id: operation_id.clone(),
                actor_name,
                key,
                operation,
                payload,
                mode,
                execution_id,
                metadata,
                organization_id,
            });
        }
        self.work_notify.notify_one();

        debug!(
            "Operation dispatched to work queue: operation_id={}, actor_key={}, mode={:?}",
            operation_id, actor_key, mode
        );

        let pending_results = self.pending_results.clone();
        let shared_in_flight = self.shared_in_flight.clone();
        let completion_timeout = self.completion_timeout;
        let actor_key = actor_key.to_string();

        async move {
            // done_tx is fired or dropped exactly when the pending entry
            // leaves the map, so any resolution here means "finished".
            if tokio::time::timeout(completion_timeout, done_rx)
                .await
                .is_err()
            {
                // The worker never completed the operation (e.g. it died
                // after polling). Fail the client and unblock the key.
                if let Some((_, entry)) = pending_results.remove(&operation_id) {
                    warn!(
                        "Operation timed out awaiting completion: operation_id={}, actor_key={}, timeout={:?}",
                        operation_id, actor_key, completion_timeout
                    );
                    let _ =
                        entry
                            .response_tx
                            .send(Err(orcher_common::error::OrcherError::timeout(
                                format!("actor operation {}", operation_id),
                                completion_timeout,
                            )));
                }
            }

            if is_shared {
                if let Some(mut count) = shared_in_flight.get_mut(&actor_key) {
                    *count = count.saturating_sub(1);
                }
                shared_in_flight.remove_if(&actor_key, |_, count| *count == 0);
            }
        }
    }

    /// Get current work queue size (for monitoring)
    pub async fn work_queue_size(&self) -> usize {
        let queue = self.work_queue.lock().await;
        queue.len()
    }

    /// Get number of active key queues (for monitoring)
    pub fn active_key_queues(&self) -> usize {
        self.key_queues.len()
    }

    /// Get number of pending results (for monitoring)
    pub fn pending_results_count(&self) -> usize {
        self.pending_results.len()
    }
}

impl Default for ActorRouter {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for ActorRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActorRouter")
            .field("key_queues_count", &self.key_queues.len())
            .field("pending_results_count", &self.pending_results.len())
            .finish()
    }
}

/// Helper to create operation result
impl OperationResult {
    /// Create a success result
    pub fn success(result: Vec<u8>, duration_ms: u64) -> Self {
        Self {
            result,
            status: ExecutionStatus::Success,
            error_message: String::new(),
            error_code: String::new(),
            duration_ms,
        }
    }

    /// Create an error result
    pub fn error(error_message: String, error_code: String, duration_ms: u64) -> Self {
        Self {
            result: Vec::new(),
            status: ExecutionStatus::Error,
            error_message,
            error_code,
            duration_ms,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &str = "user-123";
    const ACTOR: &str = "TestActor";

    async fn invoke(
        router: &ActorRouter,
        operation: &str,
        mode: OperationMode,
    ) -> (String, oneshot::Receiver<OrcherResult<Vec<u8>>>) {
        router
            .invoke_operation(
                ACTOR.to_string(),
                KEY.to_string(),
                operation.to_string(),
                b"payload".to_vec(),
                mode,
                Uuid::new_v4().to_string(),
                HashMap::new(),
            )
            .await
            .expect("invoke failed")
    }

    /// Poll until `n` operations have been collected (or panic after ~3s).
    async fn poll_n(router: &ActorRouter, n: usize) -> Vec<ActorOperation> {
        let mut collected = Vec::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while collected.len() < n {
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for {} operations (got {})",
                n,
                collected.len()
            );
            let ops = router
                .poll_operations("test-worker", 16, Duration::from_millis(200), |_, _| true)
                .await
                .unwrap();
            collected.extend(ops);
        }
        collected
    }

    /// A short poll asserting NO work is available.
    async fn assert_no_work(router: &ActorRouter) {
        let ops = router
            .poll_operations("test-worker", 16, Duration::from_millis(150), |_, _| true)
            .await
            .unwrap();
        assert!(
            ops.is_empty(),
            "expected no dispatchable work, got {:?}",
            ops.len()
        );
    }

    async fn complete_ok(router: &ActorRouter, operation_id: &str) {
        router
            .complete_operation(
                operation_id.to_string(),
                OperationResult::success(b"ok".to_vec(), 1),
            )
            .await
            .expect("complete failed");
    }

    #[tokio::test]
    async fn exclusive_operations_serialize_per_key() {
        let router = ActorRouter::new();
        let (_id1, _rx1) = invoke(&router, "op1", OperationMode::Exclusive).await;
        let (_id2, _rx2) = invoke(&router, "op2", OperationMode::Exclusive).await;

        // Only the first exclusive op is dispatchable.
        let first = poll_n(&router, 1).await;
        assert_no_work(&router).await;

        // Completing it releases the second.
        complete_ok(&router, &first[0].operation_id).await;
        let second = poll_n(&router, 1).await;
        assert_ne!(first[0].operation_id, second[0].operation_id);
    }

    #[tokio::test]
    async fn shared_operations_dispatch_concurrently() {
        let router = ActorRouter::new();
        for i in 0..3 {
            invoke(&router, &format!("read{}", i), OperationMode::Shared).await;
        }

        // All three shared ops are dispatchable before ANY completion.
        let ops = poll_n(&router, 3).await;
        assert_eq!(ops.len(), 3);
        for op in &ops {
            assert_eq!(op.mode, OperationMode::Shared as i32);
        }
    }

    #[tokio::test]
    async fn exclusive_is_a_barrier_for_shared_batches() {
        let router = ActorRouter::new();
        invoke(&router, "read1", OperationMode::Shared).await;
        invoke(&router, "read2", OperationMode::Shared).await;
        // Let the batch open before the barrier arrives.
        let shared = poll_n(&router, 2).await;

        invoke(&router, "write", OperationMode::Exclusive).await;
        invoke(&router, "read3", OperationMode::Shared).await;

        // The exclusive op is NOT dispatchable while the batch is in flight,
        // and read3 queues behind the exclusive (no starvation).
        assert_no_work(&router).await;

        complete_ok(&router, &shared[0].operation_id).await;
        assert_no_work(&router).await; // batch not fully drained yet
        complete_ok(&router, &shared[1].operation_id).await;

        // Barrier releases: the exclusive runs ALONE.
        let excl = poll_n(&router, 1).await;
        assert_eq!(excl[0].operation, "write");
        assert_no_work(&router).await;

        // After the exclusive completes, the trailing shared op runs.
        complete_ok(&router, &excl[0].operation_id).await;
        let trailing = poll_n(&router, 1).await;
        assert_eq!(trailing[0].operation, "read3");
    }

    #[tokio::test]
    async fn results_round_trip_to_the_client() {
        let router = ActorRouter::new();
        let (_id, rx) = invoke(&router, "op", OperationMode::Exclusive).await;
        let ops = poll_n(&router, 1).await;

        router
            .complete_operation(
                ops[0].operation_id.clone(),
                OperationResult::success(b"hello".to_vec(), 7),
            )
            .await
            .unwrap();

        let result = rx.await.expect("channel closed").expect("operation failed");
        assert_eq!(result, b"hello");
    }

    #[tokio::test]
    async fn failures_round_trip_to_the_client() {
        let router = ActorRouter::new();
        let (_id, rx) = invoke(&router, "op", OperationMode::Exclusive).await;
        let ops = poll_n(&router, 1).await;

        router
            .complete_operation(
                ops[0].operation_id.clone(),
                OperationResult::error("boom".to_string(), "ERR".to_string(), 7),
            )
            .await
            .unwrap();

        let result = rx.await.expect("channel closed");
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn duplicate_completion_is_rejected() {
        let router = ActorRouter::new();
        invoke(&router, "op", OperationMode::Exclusive).await;
        let ops = poll_n(&router, 1).await;

        complete_ok(&router, &ops[0].operation_id).await;
        let second = router
            .complete_operation(
                ops[0].operation_id.clone(),
                OperationResult::success(Vec::new(), 1),
            )
            .await;
        assert!(second.is_err());
    }

    #[tokio::test]
    async fn shared_window_blocks_state_writes() {
        let router = ActorRouter::new();
        assert!(!router.state_writes_blocked(ACTOR, KEY));

        invoke(&router, "read", OperationMode::Shared).await;
        let ops = poll_n(&router, 1).await;
        assert!(router.state_writes_blocked(ACTOR, KEY));

        complete_ok(&router, &ops[0].operation_id).await;
        // The window closes asynchronously after completion.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while router.state_writes_blocked(ACTOR, KEY) {
            assert!(
                tokio::time::Instant::now() < deadline,
                "shared window never closed"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn exclusive_operations_do_not_block_state_writes() {
        let router = ActorRouter::new();
        invoke(&router, "write", OperationMode::Exclusive).await;
        poll_n(&router, 1).await;
        assert!(!router.state_writes_blocked(ACTOR, KEY));
    }

    #[tokio::test]
    async fn completion_timeout_fails_client_and_unblocks_key() {
        let router = ActorRouter::new().with_completion_timeout(Duration::from_millis(200));

        let (_id1, rx1) = invoke(&router, "op1", OperationMode::Exclusive).await;
        let (_id2, _rx2) = invoke(&router, "op2", OperationMode::Exclusive).await;

        // op1 is polled by a worker that then "dies" (never completes).
        poll_n(&router, 1).await;

        // The client gets a timeout error...
        let result = tokio::time::timeout(Duration::from_secs(3), rx1)
            .await
            .expect("client never received a result")
            .expect("channel closed");
        assert!(result.is_err(), "expected a timeout error");

        // ...and the key is unblocked: op2 becomes dispatchable.
        let ops = poll_n(&router, 1).await;
        assert_eq!(ops[0].operation, "op2");
    }

    #[tokio::test]
    async fn different_keys_are_independent() {
        let router = ActorRouter::new();
        router
            .invoke_operation(
                ACTOR.to_string(),
                "key-a".to_string(),
                "op".to_string(),
                Vec::new(),
                OperationMode::Exclusive,
                Uuid::new_v4().to_string(),
                HashMap::new(),
            )
            .await
            .unwrap();
        router
            .invoke_operation(
                ACTOR.to_string(),
                "key-b".to_string(),
                "op".to_string(),
                Vec::new(),
                OperationMode::Exclusive,
                Uuid::new_v4().to_string(),
                HashMap::new(),
            )
            .await
            .unwrap();

        // Both exclusive ops dispatch concurrently — they are on different keys.
        let ops = poll_n(&router, 2).await;
        assert_eq!(ops.len(), 2);
    }

    #[tokio::test]
    async fn poll_only_returns_operations_the_worker_can_handle() {
        let router = ActorRouter::new();

        // Two operations on two different actors, on independent keys so both
        // are dispatchable at once.
        router
            .invoke_operation(
                "CartActor".to_string(),
                "cart-1".to_string(),
                "add_item".to_string(),
                Vec::new(),
                OperationMode::Exclusive,
                Uuid::new_v4().to_string(),
                HashMap::new(),
            )
            .await
            .unwrap();
        router
            .invoke_operation(
                "OrderActor".to_string(),
                "order-1".to_string(),
                "place".to_string(),
                Vec::new(),
                OperationMode::Exclusive,
                Uuid::new_v4().to_string(),
                HashMap::new(),
            )
            .await
            .unwrap();

        // A worker that only registered CartActor must receive ONLY the cart
        // operation, even though the order operation is queued and ready.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        let mut cart_ops = Vec::new();
        while cart_ops.is_empty() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "cart op never dispatched"
            );
            cart_ops = router
                .poll_operations("cart-worker", 16, Duration::from_millis(200), |actor, _| {
                    actor == "CartActor"
                })
                .await
                .unwrap();
        }
        assert_eq!(cart_ops.len(), 1);
        assert_eq!(cart_ops[0].actor_name, "CartActor");

        // The order operation was NOT misrouted to the cart worker — it remains
        // queued until a capable worker polls for it.
        let order_ops = router
            .poll_operations(
                "order-worker",
                16,
                Duration::from_millis(200),
                |actor, _| actor == "OrderActor",
            )
            .await
            .unwrap();
        assert_eq!(order_ops.len(), 1);
        assert_eq!(order_ops[0].actor_name, "OrderActor");
    }

    #[test]
    fn test_actor_key_building() {
        let key = ActorRouter::build_tenant_actor_key(None, "ShoppingCart", "user-123");
        assert_eq!(key, "ShoppingCart:user-123");
    }

    #[test]
    fn test_operation_result_success() {
        let result = OperationResult::success(vec![1, 2, 3], 100);
        assert_eq!(result.status, ExecutionStatus::Success);
        assert_eq!(result.result, vec![1, 2, 3]);
        assert_eq!(result.duration_ms, 100);
    }

    #[test]
    fn test_operation_result_error() {
        let result = OperationResult::error(
            "Something went wrong".to_string(),
            "ERR_TEST".to_string(),
            50,
        );
        assert_eq!(result.status, ExecutionStatus::Error);
        assert_eq!(result.error_message, "Something went wrong");
        assert_eq!(result.error_code, "ERR_TEST");
        assert_eq!(result.duration_ms, 50);
    }
}
