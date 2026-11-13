//! Task dispatch service.

use crate::engine::dispatch::PollingManager;
use crate::engine::queues::OutboxTask;
use crate::engine::state::high_perf_cache::HighPerfCache;
use orcher_common::error::{OrcherError, OrcherResult};
use orcher_storage::repositories::domains::journal::JournalRepository;
use orcher_storage::repositories::domains::workflow_execution::WorkflowExecutionRepository;
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Outcome of a dispatch operation
#[derive(Debug, Clone)]
pub enum DispatchOutcome {
    /// Fast-path: Worker received complete response immediately at dispatch time
    FastPathMatched {
        /// ID of the worker that received the work
        worker_id: String,
        /// Redis stream entry ID for tracking
        stream_entry_id: String,
    },
    /// Sync-match: Worker received work directly without polling Redis
    SyncMatched {
        /// ID of the worker that received the work
        worker_id: String,
    },
    /// Affinity routing: Work sent to preferred worker
    AffinityRouted {
        /// ID of the worker that received the work
        worker_id: String,
    },
    /// Normal path: Work queued to Redis/PostgreSQL
    Queued {
        /// Redis stream entry ID if queued to streams
        stream_entry_id: Option<String>,
    },
}

/// Service responsible for dispatching tasks to workers
///
/// This service encapsulates the logic for:
/// - Fast-path dispatch with cached state (sub-ms latency)
/// - Sync-match dispatch to waiting workers
/// - Legacy: Redis Lists or PostgreSQL-only polling
pub struct TaskDispatchService {
    /// Polling manager for PostgreSQL-based notifications
    polling_manager: Arc<PollingManager>,
    /// Workflow execution repository (fast-path)
    workflow_execution_repo: Option<Arc<WorkflowExecutionRepository>>,
    /// Journal repository (fast-path)
    journal_repo: Option<Arc<JournalRepository>>,
    /// Workflow state cache (fast-path)
    #[allow(dead_code)]
    state_cache: Option<Arc<HighPerfCache>>,
    /// Matching store (partitioned task delivery). When set, a `DispatchTask` is
    /// transferred into its `(namespace, task_queue)` matching partition instead of the
    /// legacy task-dispatch path — the emitter side of the matching layer. Unset ⇒
    /// legacy path, unchanged. Injected once at startup, gated by a flag.
    matching_store: std::sync::OnceLock<
        Arc<dyn orcher_storage::repositories::domains::matching::MatchingStore>,
    >,
    /// Task polling manager. Its `task_waiters` are what the activity task pollers
    /// register in — a SEPARATE registry from `polling_manager`'s. Set so a dispatch
    /// wakes those pollers directly; without it they only re-check on their ~200ms
    /// retry interval, which is the delivery-latency floor. Unset ⇒ unchanged (the
    /// pollers still claim on their retry).
    task_polling_manager: std::sync::OnceLock<Arc<crate::engine::dispatch::TaskPollingManager>>,
}

impl TaskDispatchService {
    /// Create a new task dispatch service (legacy mode)
    ///
    /// # Arguments
    /// * `polling_manager` - Manager for PostgreSQL polling and notifications
    /// * `redis_queue` - Optional Redis queue for fast task delivery
    pub fn new(polling_manager: Arc<PollingManager>) -> Self {
        Self {
            polling_manager,
            workflow_execution_repo: None,
            journal_repo: None,
            state_cache: None,
            matching_store: std::sync::OnceLock::new(),
            task_polling_manager: std::sync::OnceLock::new(),
        }
    }

    /// Create with sync-match support
    ///
    /// # Arguments
    /// * `polling_manager` - Manager with WaitersRegistry for sync-match
    /// * `state_cache` - High-performance cache for fast-path optimization
    pub fn with_sync_match(
        polling_manager: Arc<PollingManager>,
        state_cache: Arc<HighPerfCache>,
    ) -> Self {
        Self {
            polling_manager,
            workflow_execution_repo: None,
            journal_repo: None,
            state_cache: Some(state_cache),
            matching_store: std::sync::OnceLock::new(),
            task_polling_manager: std::sync::OnceLock::new(),
        }
    }

    /// Enable fast-path optimization with repository access
    ///
    /// This allows building complete poll responses at dispatch time
    pub fn with_repositories(
        mut self,
        workflow_execution_repo: Arc<WorkflowExecutionRepository>,
        journal_repo: Arc<JournalRepository>,
    ) -> Self {
        self.workflow_execution_repo = Some(workflow_execution_repo);
        self.journal_repo = Some(journal_repo);
        self
    }

    /// Enable the matching layer: transfer `DispatchTask` outbox rows into their
    /// `(namespace, task_queue)` matching partition instead of the legacy dispatch.
    /// Idempotent; a second call is ignored. Wired once at startup behind a flag.
    pub fn set_matching_store(
        &self,
        store: Arc<dyn orcher_storage::repositories::domains::matching::MatchingStore>,
    ) {
        let _ = self.matching_store.set(store);
    }

    /// Wire the [`TaskPollingManager`](crate::engine::dispatch::TaskPollingManager) so a
    /// dispatch wakes the activity task pollers directly. They register their waiters in
    /// this manager's registry, which is separate from `polling_manager`'s — so without
    /// this, a dispatch's notify never reaches them and they only pick up work on their
    /// ~200ms retry. Idempotent; a second call is ignored.
    pub fn set_task_polling_manager(
        &self,
        manager: Arc<crate::engine::dispatch::TaskPollingManager>,
    ) {
        let _ = self.task_polling_manager.set(manager);
    }

    /// Dispatch with fast-path optimization
    ///
    /// Flow:
    /// 1. Durability is the outbox row, already committed with the workflow state
    /// 2. Check if worker is waiting (peek_waiter)
    /// 3. If yes, notify the waiter immediately
    /// 4. Worker will load state when it processes the notification
    ///
    /// Note: Full response building happens on the worker side to avoid
    /// incomplete PollExecutionResponse construction. This still achieves
    /// sub-millisecond notification latency.
    pub async fn dispatch_with_fast_path(
        &self,
        namespace: &str,
        task_queue: &str,
        execution_id: &str,
    ) -> OrcherResult<DispatchOutcome> {
        // For now, delegate to sync-match which handles notification properly
        // Future optimization: pre-build response from cache if available
        self.dispatch_with_sync_match(namespace, task_queue, execution_id)
            .await
    }

    /// Dispatch with sync-match (simplified notification)
    ///
    /// Flow:
    /// 1. Durability is the outbox row, already committed with the workflow state
    /// 2. Notify polling manager (which wakes waiting workers)
    /// 3. Return outcome based on whether workers are waiting
    pub async fn dispatch_with_sync_match(
        &self,
        namespace: &str,
        task_queue: &str,
        execution_id: &str,
    ) -> OrcherResult<DispatchOutcome> {
        // Durability is the outbox row, written in the same PostgreSQL
        // transaction as the workflow state that produced this task. A Redis
        // Streams XADD used to run first, but it could never have been the
        // durable record: it cannot commit atomically with that transaction, and
        // nothing ever read the stream back. The match and the wake below are
        // in-process, so removing the write leaves delivery unchanged.

        // Step 2: Check if sync-match is enabled and workers are waiting
        let has_waiting_worker = if self.polling_manager.is_sync_match_enabled() {
            self.polling_manager
                .peek_waiting_worker(namespace, task_queue)
                .is_some()
        } else {
            false
        };

        // Step 3: Notify polling manager (wakes waiting workers or queues for poll)
        self.polling_manager
            .notify_work_available(task_queue, namespace)
            .await;

        // Step 4: Return appropriate outcome
        if has_waiting_worker {
            debug!(
                execution_id = %execution_id,
                "Sync-match notification sent to waiting worker"
            );

            Ok(DispatchOutcome::SyncMatched {
                worker_id: "notified".to_string(), // Actual worker_id determined by PollingManager
            })
        } else {
            info!(
                execution_id = %execution_id,
                "Queued for polling (no waiting workers)"
            );

            Ok(DispatchOutcome::Queued {
                // No stream entry: the durable record is the outbox row. Other
                // call sites already pass None/empty here; the field is vestigial
                // and goes with the rest of the Streams plumbing.
                stream_entry_id: None,
            })
        }
    }

    /// Dispatch a workflow step execution
    ///
    /// This handles `DispatchStep` tasks by:
    /// 1. Pushing to Redis matching queue (if available)
    /// 2. Notifying PostgreSQL pollers
    ///
    /// # Arguments
    /// * `task` - The outbox task containing workflow dispatch information
    pub async fn dispatch_workflow_step(&self, task: &OutboxTask) -> OrcherResult<()> {
        debug!(
            task_id = %task.task_id,
            workflow_id = %task.workflow_id,
            "DISPATCH_STEP: Entering dispatch_workflow_step"
        );

        let payload = &task.payload;

        let task_queue = payload
            .get("task_queue")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                orcher_common::error::OrcherError::internal(
                    "DispatchStep task missing task_queue in payload",
                )
            })?;

        let namespace = payload
            .get("namespace")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                orcher_common::error::OrcherError::internal(
                    "DispatchStep task missing namespace in payload",
                )
            })?;

        let execution_id = payload
            .get("execution_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                orcher_common::error::OrcherError::internal(
                    "DispatchStep task missing execution_id in payload",
                )
            })?;

        // Notify PostgreSQL pollers (fallback mechanism)
        self.polling_manager
            .notify_work_available(task_queue, namespace)
            .await;

        debug!(
            task_queue = %task_queue,
            namespace = %namespace,
            execution_id = %execution_id,
            "DISPATCH_STEP: Workflow step dispatched successfully"
        );

        Ok(())
    }

    /// Transfer a `DispatchTask` outbox row into its `(namespace, task_queue)` matching
    /// partition — the emitter side of the matching layer. The partition key comes from
    /// the payload; enqueue is idempotent on `task_id`. Then wake any worker parked on
    /// the partition so it claims promptly (correctness doesn't depend on the wake — the
    /// poll loop also drains on its interval).
    async fn transfer_to_matching(
        &self,
        store: &dyn orcher_storage::repositories::domains::matching::MatchingStore,
        task: &OutboxTask,
    ) -> OrcherResult<()> {
        use orcher_storage::repositories::domains::matching::MatchTask;

        let namespace = task
            .payload
            .get("namespace")
            .and_then(|v| v.as_str())
            .ok_or_else(|| OrcherError::internal("DispatchTask missing namespace in payload"))?
            .to_string();
        let task_queue = task
            .payload
            .get("task_queue")
            .and_then(|v| v.as_str())
            .ok_or_else(|| OrcherError::internal("DispatchTask missing task_queue in payload"))?
            .to_string();

        store
            .enqueue(&MatchTask {
                task_id: task.task_id,
                namespace: namespace.clone(),
                task_queue: task_queue.clone(),
                workflow_id: task.workflow_id,
                payload: task.payload.clone(),
                organization_id: task.organization_id,
            })
            .await?;

        // Wake the ACTIVITY task pollers. They register their waiters in the
        // `TaskPollingManager` registry — a different one from `polling_manager` — so the
        // `polling_manager` notify below never reaches them, and they'd otherwise only
        // claim this task on their ~200ms retry (the delivery-latency floor). Enqueue is
        // committed above, so the row is visible when a woken poller claims. When the
        // manager isn't wired, behavior is unchanged (pollers wait for their retry).
        if let Some(tpm) = self.task_polling_manager.get() {
            tpm.notify_work_available(&task_queue, &namespace).await;
        }

        self.polling_manager
            .notify_work_available(&task_queue, &namespace)
            .await;
        Ok(())
    }

    /// Hand an outbox task to the pollers serving its queue.
    pub async fn dispatch_activity_task(&self, task: &OutboxTask) -> OrcherResult<()> {
        debug!(
            task_id = %task.task_id,
            workflow_id = %task.workflow_id,
            "DISPATCH_ACTIVITY: Entering dispatch_activity_task"
        );

        let payload = &task.payload;

        let task_queue = payload
            .get("task_queue")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                orcher_common::error::OrcherError::internal(
                    "DispatchTask missing task_queue in payload",
                )
            })?;

        let namespace = payload
            .get("namespace")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                orcher_common::error::OrcherError::internal(
                    "DispatchTask missing namespace in payload",
                )
            })?;

        let task_id = payload
            .get("task_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                orcher_common::error::OrcherError::internal(
                    "DispatchTask missing task_id in payload",
                )
            })?;

        // Notify PostgreSQL pollers (fallback mechanism)
        self.polling_manager
            .notify_work_available(task_queue, namespace)
            .await;

        debug!(
            task_queue = %task_queue,
            namespace = %namespace,
            task_id = %task_id,
            "DISPATCH_ACTIVITY: Activity task dispatched successfully"
        );

        Ok(())
    }

    /// Execute an outbox task
    ///
    /// This is the main entry point called by the OutboxProcessor.
    /// It routes tasks to the appropriate handler based on task type.
    ///
    /// # Arguments
    /// * `task` - The outbox task to execute
    pub async fn execute_task(&self, task: OutboxTask) -> OrcherResult<()> {
        debug!(
            task_id = %task.task_id,
            task_type = ?task.task_type,
            workflow_id = %task.workflow_id,
            "TASK_DISPATCH: Entering execute_task"
        );

        // Route to appropriate handler based on task type
        let task_type_str = format!("{:?}", task.task_type);

        debug!(
            task_id = %task.task_id,
            task_type = %task_type_str,
            workflow_id = %task.workflow_id,
            "TASK_DISPATCH: Routing task based on type"
        );

        match task_type_str.as_str() {
            "DispatchStep" => {
                // Sync-match: hand the step straight to a worker that is already
                // waiting, instead of leaving it for the next poll. Gated on the
                // capability itself — the polling manager having a waiters registry
                // — rather than on a Redis client being present, which is what it
                // used to key off even though the handoff was always in-process.
                if self.polling_manager.is_sync_match_enabled() {
                    // Extract execution details from payload
                    let payload = &task.payload;
                    let task_queue = payload
                        .get("task_queue")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| {
                            OrcherError::internal("DispatchStep task missing task_queue in payload")
                        })?;
                    let namespace = payload
                        .get("namespace")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| {
                            OrcherError::internal("DispatchStep task missing namespace in payload")
                        })?;
                    let execution_id = payload
                        .get("execution_id")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| {
                            OrcherError::internal(
                                "DispatchStep task missing execution_id in payload",
                            )
                        })?;

                    debug!(
                        task_id = %task.task_id,
                        workflow_id = %task.workflow_id,
                        execution_id = %execution_id,
                        "TASK_DISPATCH: dispatching via sync-match"
                    );

                    match self
                        .dispatch_with_sync_match(namespace, task_queue, execution_id)
                        .await
                    {
                        Ok(outcome) => {
                            debug!(
                                task_id = %task.task_id,
                                outcome = ?outcome,
                                "TASK_DISPATCH: sync-match dispatch completed"
                            );
                            Ok(())
                        }
                        Err(e) => {
                            warn!(
                                task_id = %task.task_id,
                                error = %e,
                                "TASK_DISPATCH: sync-match dispatch failed, falling back to legacy"
                            );
                            // Fall back to legacy dispatch on error
                            self.dispatch_workflow_step(&task).await
                        }
                    }
                } else {
                    // Legacy: Use Redis Lists or PostgreSQL-only
                    debug!(
                        task_id = %task.task_id,
                        workflow_id = %task.workflow_id,
                        "TASK_DISPATCH: Routing to dispatch_workflow_step (legacy)"
                    );
                    self.dispatch_workflow_step(&task).await
                }
            }
            "DispatchTask" => {
                // Matching layer (emitter): when a matching store is wired, transfer the
                // task into its (namespace, task_queue) partition and let the worker poll
                // claim it from there. The per-shard outbox processor runs this executor,
                // so it is the transfer emitter for its shard. Off ⇒ legacy dispatch.
                if let Some(store) = self.matching_store.get() {
                    return self.transfer_to_matching(store.as_ref(), &task).await;
                }
                debug!(
                    task_id = %task.task_id,
                    workflow_id = %task.workflow_id,
                    "TASK_DISPATCH: Routing to dispatch_activity_task"
                );
                self.dispatch_activity_task(&task).await
            }
            _ => {
                warn!(
                    task_id = %task.task_id,
                    task_type = ?task.task_type,
                    "TASK_DISPATCH: Outbox task type not handled by TaskDispatchService, marking as complete"
                );
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_dispatch_service_construction() {
        // Ensure service can be constructed (compile-time validation)
        fn _assert_send_sync() {
            fn _is_send<T: Send>() {}
            fn _is_sync<T: Sync>() {}
            _is_send::<TaskDispatchService>();
            _is_sync::<TaskDispatchService>();
        }
    }

    #[test]
    fn test_dispatch_outcome_variants() {
        let outcome = DispatchOutcome::FastPathMatched {
            worker_id: "worker-1".to_string(),
            stream_entry_id: "1234-0".to_string(),
        };
        assert!(matches!(outcome, DispatchOutcome::FastPathMatched { .. }));

        let outcome = DispatchOutcome::SyncMatched {
            worker_id: "worker-2".to_string(),
        };
        assert!(matches!(outcome, DispatchOutcome::SyncMatched { .. }));

        let outcome = DispatchOutcome::Queued {
            stream_entry_id: Some("5678-0".to_string()),
        };
        assert!(matches!(outcome, DispatchOutcome::Queued { .. }));
    }
}
