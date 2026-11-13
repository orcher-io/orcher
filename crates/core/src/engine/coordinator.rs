//! Workflow task coordinator — claims, executes, and completes workflow tasks.

use crate::engine::queues::{
    OutboxQueue, OutboxTask, OutboxTaskType, TimerQueue, TimerTask, TimerTaskType,
};
use crate::engine::state::{
    CacheConfig, CacheInvalidator, CacheStats, EventType, InvalidatorConfig, LockConfig,
    StateReconstructor, WorkflowExecutionState, WorkflowStateCache, WorkflowStateLock,
};
use jiff::SignedDuration;
use orcher_common::error::{OrcherError, OrcherResult};
use orcher_common::types::time::Timestamptz;
use orcher_storage::repositories::domains::task_execution::TaskExecutionRepository;
use orcher_storage::schema::types::TaskExecution;
use serde_json::Value;
use sqlx::PgPool;
use std::sync::Arc;
use tracing::{debug, info, warn};
use uuid::Uuid;

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for the WorkflowCoordinator
#[derive(Debug, Clone)]
pub struct CoordinatorConfig {
    /// Cache configuration
    pub cache: CacheConfig,

    /// Lock configuration
    pub lock: LockConfig,

    /// Invalidation configuration
    pub invalidation: InvalidatorConfig,

    /// Coordinator instance ID (for distributed coordination)
    pub instance_id: String,

    /// Enable automatic crash recovery on startup
    pub enable_recovery: bool,

    /// Maximum number of workflows to recover in parallel
    pub recovery_parallelism: usize,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            cache: CacheConfig::default(),
            lock: LockConfig::default(),
            invalidation: InvalidatorConfig::default(),
            instance_id: format!("coordinator-{}", Uuid::new_v4()),
            enable_recovery: true,
            recovery_parallelism: 10,
        }
    }
}

// ============================================================================
// Coordinator Metrics
// ============================================================================

/// Metrics for the coordinator
#[derive(Debug, Clone)]
pub struct CoordinatorMetrics {
    /// Number of workflows started
    pub workflows_started: u64,

    /// Number of workflows completed
    pub workflows_completed: u64,

    /// Number of workflows failed
    pub workflows_failed: u64,

    /// Number of steps completed
    pub steps_completed: u64,

    /// Number of timers scheduled
    pub timers_scheduled: u64,

    /// Number of tasks dispatched
    pub tasks_dispatched: u64,

    /// Number of cache hits
    pub cache_hits: u64,

    /// Number of cache misses
    pub cache_misses: u64,

    /// Number of state reconstructions
    pub state_reconstructions: u64,

    /// Number of lock acquisitions
    pub lock_acquisitions: u64,

    /// Number of invalidations sent
    pub invalidations_sent: u64,
}

impl Default for CoordinatorMetrics {
    fn default() -> Self {
        Self {
            workflows_started: 0,
            workflows_completed: 0,
            workflows_failed: 0,
            steps_completed: 0,
            timers_scheduled: 0,
            tasks_dispatched: 0,
            cache_hits: 0,
            cache_misses: 0,
            state_reconstructions: 0,
            lock_acquisitions: 0,
            invalidations_sent: 0,
        }
    }
}

// ============================================================================
// Workflow Coordinator
// ============================================================================

/// A task dispatch written inside a caller's transaction, to be announced
/// with [`WorkflowCoordinator::announce`] once that transaction commits.
#[derive(Debug)]
pub struct TaskDispatch {
    workflow_id: Uuid,
    outbox_id: Uuid,
    payload: Value,
}

/// Central workflow coordination component
///
/// Provides high-level workflow lifecycle operations with:
/// - Transactional state updates
/// - Distributed locking
/// - Cache consistency
/// - Durable task dispatch
/// - Event sourcing
pub struct WorkflowCoordinator {
    /// Database connection pool
    pool: Arc<PgPool>,

    /// Configuration
    config: CoordinatorConfig,

    /// Workflow state cache
    cache: Arc<WorkflowStateCache>,

    /// Cache invalidation
    invalidator: Arc<CacheInvalidator>,

    /// State reconstruction
    reconstructor: Arc<StateReconstructor>,

    /// Outbox queue
    outbox: Arc<OutboxQueue>,

    /// Timer queue
    timers: Arc<TimerQueue>,

    /// Coordinator metrics
    metrics: Arc<tokio::sync::RwLock<CoordinatorMetrics>>,

    /// Task polling manager for sending notifications when tasks are dispatched
    /// Wrapped in RwLock for interior mutability (can be set after coordinator creation)
    task_polling_manager:
        Arc<tokio::sync::RwLock<Option<Arc<crate::engine::dispatch::TaskPollingManager>>>>,

    /// Workflow retry scheduler for automatic retry scheduling
    workflow_retry_scheduler: Option<Arc<crate::workflows::WorkflowRetryScheduler>>,

    /// Direct dispatch service for sync-match fast path on task dispatch.
    /// Bypasses the outbox processor poll delay by dispatching directly to waiting workers.
    task_dispatch_service:
        std::sync::OnceLock<Arc<crate::services::task_dispatch::TaskDispatchService>>,

    /// Signals the timer processor that a timer was just scheduled, so it can
    /// stop waiting out its idle sleep. Absent → the processor still picks the
    /// timer up on its next poll, just later.
    timer_wake: std::sync::OnceLock<Arc<tokio::sync::Notify>>,

    /// Shard-ownership fencing (set once at startup when clustering is enabled).
    /// When present, per-shard state transitions are committed only if this node
    /// still holds the shard's generation. Absent (single-node / clustering off) →
    /// writes proceed exactly as before.
    shard_ownership: std::sync::OnceLock<Arc<orcher_cluster::ShardOwnership>>,

    /// Shard → database routing (set once at startup for partitioned persistence).
    /// When present, a workflow's write transactions open on the pool that holds
    /// its shard's data. Absent → every write uses the single `pool` (byte-identical
    /// to before). A `SinglePoolRouter` is also a no-op — behavior only changes with
    /// a multi-database router.
    shard_router: std::sync::OnceLock<Arc<dyn orcher_cluster::ShardRouter>>,

    /// How many days data retention keeps a completed outbox row (set once at
    /// startup from the retention config). Bounds how old a first attempt may
    /// be for this coordinator to dispatch it as never dispatched; see
    /// [`undispatched_repair_window`]. Absent → the retention default.
    outbox_retention_days: std::sync::OnceLock<i32>,
}

/// How far back a first attempt may have been scheduled for an engine to still
/// dispatch it as never dispatched, given how many days completed outbox rows
/// are kept.
///
/// A first attempt written before rows were marked on dispatch has only its
/// outbox row to show it was dispatched. Once retention deletes that row, the
/// task looks exactly like one never dispatched, and dispatching it would run
/// it twice. Retention deletes a row only once it completed more than the
/// retention period ago, and it completed after its task was scheduled, so a
/// task scheduled more recently than that still has its outbox row. The
/// window stops short of the period by a day, or by half the period when it is
/// under two days — room for clocks that disagree and a retention job that
/// runs late never hurts, since that only deletes later.
pub fn undispatched_repair_window(outbox_retention_days: i32) -> SignedDuration {
    let retention = SignedDuration::from_hours(i64::from(outbox_retention_days.max(1)) * 24);
    let margin = std::cmp::min(SignedDuration::from_hours(24), retention / 2);
    retention - margin
}

impl WorkflowCoordinator {
    /// Create a new workflow coordinator
    pub async fn new(pool: Arc<PgPool>, config: CoordinatorConfig) -> OrcherResult<Self> {
        info!(
            instance_id = %config.instance_id,
            "Initializing WorkflowCoordinator"
        );

        // Initialize components
        let cache = Arc::new(WorkflowStateCache::new(config.cache.clone()));
        let invalidator = Arc::new(CacheInvalidator::new(
            pool.clone(),
            config.invalidation.clone(),
            cache.clone(),
        ));
        let reconstructor = Arc::new(StateReconstructor::new(pool.clone()));
        // Create repositories for coordination queues
        use orcher_storage::repositories::domains::coordination::{
            OutboxRepository, TimerRepository,
        };
        let outbox_repo = Arc::new(OutboxRepository::new(pool.clone()));
        let timer_repo = Arc::new(TimerRepository::new(pool.clone()));

        let outbox = Arc::new(OutboxQueue::new(outbox_repo));
        let timers = Arc::new(TimerQueue::new(timer_repo));

        // Start cache invalidation listener before moving invalidator
        invalidator.clone().start_listener().await?;

        let coordinator = Self {
            pool,
            config: config.clone(),
            cache,
            invalidator,
            reconstructor,
            outbox,
            timers,
            metrics: Arc::new(tokio::sync::RwLock::new(CoordinatorMetrics::default())),
            task_polling_manager: Arc::new(tokio::sync::RwLock::new(None)),
            workflow_retry_scheduler: None,
            task_dispatch_service: std::sync::OnceLock::new(),
            timer_wake: std::sync::OnceLock::new(),
            shard_ownership: std::sync::OnceLock::new(),
            shard_router: std::sync::OnceLock::new(),
            outbox_retention_days: std::sync::OnceLock::new(),
        };

        // Perform crash recovery if enabled
        if config.enable_recovery {
            coordinator.recover_inflight_workflows().await?;
        }

        info!(
            instance_id = %config.instance_id,
            "WorkflowCoordinator initialized successfully"
        );

        Ok(coordinator)
    }

    /// Set the workflow retry scheduler
    ///
    /// This enables automatic retry scheduling for failed workflows.
    pub fn with_workflow_retry_scheduler(
        mut self,
        scheduler: Arc<crate::workflows::WorkflowRetryScheduler>,
    ) -> Self {
        self.workflow_retry_scheduler = Some(scheduler);
        self
    }

    // ========================================================================
    // Workflow Lifecycle Operations
    // ========================================================================

    /// Start a new workflow execution
    ///
    /// Creates a new workflow execution record and initializes its state.
    /// This operation is atomic and durable.
    pub async fn start_workflow(
        &self,
        workflow_name: String,
        namespace: String,
        input: Option<Value>,
    ) -> OrcherResult<Uuid> {
        let workflow_id = Uuid::new_v4();

        info!(
            workflow_id = %workflow_id,
            workflow_name = %workflow_name,
            namespace = %namespace,
            "Starting workflow"
        );

        // Create initial state
        let state = WorkflowExecutionState::new(
            workflow_id,
            workflow_name.clone(),
            namespace.clone(),
            input.clone(),
        );

        // Begin transaction
        let mut tx =
            self.pool_for(workflow_id)
                .begin()
                .await
                .map_err(|e| OrcherError::Internal {
                    message: format!("Failed to begin transaction: {}", e),
                    source: None,
                })?;

        // Insert workflow execution record
        sqlx::query(
            r#"
            INSERT INTO workflow_executions (
                id, execution_id, workflow_name, namespace, status,
                parameters, submitted_at, started_at, created_at, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            "#,
        )
        .bind(workflow_id)
        .bind(workflow_id.to_string())
        .bind(&workflow_name)
        .bind(&namespace)
        .bind("running")
        .bind(&input)
        .bind(Timestamptz::now())
        .bind(Timestamptz::now())
        .bind(Timestamptz::now())
        .bind(Timestamptz::now())
        .execute(&mut *tx)
        .await
        .map_err(|e| OrcherError::Internal {
            message: format!("Failed to insert workflow execution: {}", e),
            source: None,
        })?;

        // Note: WorkflowStarted event is recorded in workflow_execution_journal
        // We do NOT write orchestration events to execution_log

        // Commit transaction
        tx.commit().await.map_err(|e| OrcherError::Internal {
            message: format!("Failed to commit transaction: {}", e),
            source: None,
        })?;

        // Cache the initial state
        self.cache.insert(state).await?;

        // Update metrics
        let mut metrics = self.metrics.write().await;
        metrics.workflows_started += 1;
        drop(metrics);

        info!(
            workflow_id = %workflow_id,
            "Workflow started successfully"
        );

        // Write user-facing log
        let _ = self
            .write_execution_log(
                &workflow_id.to_string(),
                "info",
                format!("Workflow '{}' started", workflow_name),
                None,
                None,
                Some(serde_json::json!({
                    "workflow_name": workflow_name,
                    "namespace": namespace,
                })),
            )
            .await;

        Ok(workflow_id)
    }

    /// Get workflow state
    ///
    /// First checks the cache, then falls back to reconstruction if needed.
    pub async fn get_state(&self, workflow_id: Uuid) -> OrcherResult<WorkflowExecutionState> {
        debug!(workflow_id = %workflow_id, "Getting workflow state");

        // Try cache first
        if let Some(state) = self.cache.get(&workflow_id).await? {
            debug!(workflow_id = %workflow_id, "Cache hit");
            let mut metrics = self.metrics.write().await;
            metrics.cache_hits += 1;
            drop(metrics);
            return Ok(state);
        }

        // Cache miss - reconstruct from event log
        debug!(workflow_id = %workflow_id, "Cache miss - reconstructing");
        let mut metrics = self.metrics.write().await;
        metrics.cache_misses += 1;
        metrics.state_reconstructions += 1;
        drop(metrics);

        let state = self
            .reconstructor
            .reconstruct(workflow_id, self.pool_for(workflow_id))
            .await?;

        // Put in cache for next time
        self.cache.insert(state.clone()).await?;

        Ok(state)
    }

    /// Open a transaction for a per-workflow state transition, fenced by shard
    /// ownership when clustering is enabled.
    ///
    /// With a `ShardOwnership` set, this acquires (lazily) the generation for the
    /// workflow's shard and opens a guarded transaction that commits only if this
    /// node still holds it — rejecting a superseded owner's write. Returns an error
    /// (do not process this workflow) if a live other node owns the shard. Without
    /// ownership (single-node / clustering off) it is a plain `begin()`, so the
    /// non-clustered path is byte-identical to before.
    async fn begin_guarded(
        &self,
        workflow_id: Uuid,
    ) -> OrcherResult<sqlx::Transaction<'_, sqlx::Postgres>> {
        let pool = self.pool_for(workflow_id);
        let Some(ownership) = self.shard_ownership.get().cloned() else {
            return pool.begin().await.map_err(|e| OrcherError::Internal {
                message: format!("Failed to begin transaction: {}", e),
                source: None,
            });
        };

        match ownership
            .acquire_for(&workflow_id.to_string())
            .await
            .map_err(|e| OrcherError::internal(format!("shard acquire failed: {e}")))?
        {
            Some((shard, generation)) => {
                orcher_storage::repositories::domains::cluster::begin_shard_guarded_tx(
                    pool,
                    shard.0 as i32,
                    generation as i64,
                )
                .await
            }
            None => Err(OrcherError::internal(format!(
                "{} for workflow {workflow_id} (a live node owns its shard)",
                orcher_storage::sharding::OWNERSHIP_LOST
            ))),
        }
    }

    /// Update workflow state atomically
    ///
    /// Reads the state, applies `update_fn`, writes the row and refreshes the
    /// cache, with every other update of the same workflow held off until it
    /// is done.
    ///
    /// All of it happens on one connection. The workflow's lock used to be a
    /// session advisory lock on a connection of its own, held while the update
    /// took a second connection for its transaction — and, on a cache miss, a
    /// third to rebuild the state. Under load every caller held its first and
    /// waited on the pool for the next, and with enough of them at once none
    /// was ever given one: with a pool of ten, most of three hundred concurrent
    /// completions timed out, and with a pool of four the engine deadlocked.
    /// The lock is now transaction-scoped, taken inside the update's own
    /// transaction on the same key the session lock used, and released by its
    /// commit.
    pub async fn update_state<F>(
        &self,
        workflow_id: Uuid,
        update_fn: F,
    ) -> OrcherResult<WorkflowExecutionState>
    where
        F: FnOnce(&mut WorkflowExecutionState) -> OrcherResult<()>,
    {
        debug!(workflow_id = %workflow_id, "Updating workflow state");

        // Begin transaction — fenced by shard ownership when clustering is enabled.
        // With fencing on, this node must still hold the workflow's shard generation
        // or the write is rejected (a superseded owner cannot corrupt state). With
        // fencing off (single-node / clustering disabled) this is a plain begin().
        let mut tx = self.begin_guarded(workflow_id).await?;

        // Bounded as the session lock's acquire was, so a stuck holder surfaces
        // as an error rather than a wait with no end.
        sqlx::query("SELECT set_config('lock_timeout', $1, true)")
            .bind(format!("{}s", self.config.lock.acquire_timeout_secs.max(1)))
            .execute(&mut *tx)
            .await
            .map_err(|e| OrcherError::internal(format!("Failed to bound the lock wait: {e}")))?;
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(WorkflowStateLock::workflow_id_to_lock_id(&workflow_id))
            .execute(&mut *tx)
            .await
            .map_err(|e| OrcherError::Internal {
                message: format!("Failed to acquire lock for workflow {workflow_id}: {e}"),
                source: None,
            })?;

        let mut metrics = self.metrics.write().await;
        metrics.lock_acquisitions += 1;
        drop(metrics);

        debug!(workflow_id = %workflow_id, "Lock acquired");

        // Get current state: from the cache, or rebuilt on this transaction's
        // own connection.
        let mut state = match self.cache.get(&workflow_id).await? {
            Some(state) => {
                self.metrics.write().await.cache_hits += 1;
                state
            }
            None => {
                let mut metrics = self.metrics.write().await;
                metrics.cache_misses += 1;
                metrics.state_reconstructions += 1;
                drop(metrics);
                self.reconstructor
                    .reconstruct_on(workflow_id, &mut tx)
                    .await?
            }
        };

        // Apply update
        update_fn(&mut state)?;

        // Increment version
        state.version += 1;
        state.last_updated = Timestamptz::now();

        // Update workflow execution record
        sqlx::query(
            r#"
            UPDATE workflow_executions
            SET
                status = $2,
                current_step = $3,
                completed_steps = $4,
                updated_at = $5
            WHERE id = $1
            "#,
        )
        .bind(workflow_id)
        .bind(state.status.to_string())
        .bind(&state.current_step)
        .bind(state.completed_steps.len() as i32)
        .bind(state.last_updated)
        .execute(&mut *tx)
        .await
        .map_err(|e| OrcherError::Internal {
            message: format!("Failed to update workflow execution: {}", e),
            source: None,
        })?;

        // Cached before the commit releases the lock, so the next update of
        // this workflow reads what this one wrote — but distrusted until the
        // commit is seen to succeed. Distrusted, the entry reads as a miss and
        // is rebuilt from the database. Marked before the commit, the mark is
        // in place whatever happens during it: a failed commit, or this future
        // dropped mid-commit, which runs no code after the await. A reader
        // arriving between the commit and the mark's removal only rebuilds a
        // state it could have had from the cache.
        self.cache.insert(state.clone()).await?;
        self.cache.distrust(workflow_id);
        if let Err(e) = tx.commit().await {
            return Err(OrcherError::Internal {
                message: format!("Failed to commit transaction: {}", e),
                source: None,
            });
        }
        self.cache.trust(workflow_id);

        // Broadcast invalidation to other coordinators
        self.invalidator.invalidate_workflow(workflow_id).await?;

        let mut metrics = self.metrics.write().await;
        metrics.invalidations_sent += 1;
        drop(metrics);

        debug!(
            workflow_id = %workflow_id,
            version = state.version,
            "State updated successfully"
        );

        Ok(state)
    }

    /// Complete a workflow step
    ///
    /// Records step completion and optionally schedules follow-up tasks.
    pub async fn complete_step(
        &self,
        workflow_id: Uuid,
        step_name: String,
        _result: Value,
    ) -> OrcherResult<()> {
        info!(
            workflow_id = %workflow_id,
            step_name = %step_name,
            "Completing workflow step"
        );

        self.update_state(workflow_id, |state| {
            state.completed_steps.insert(step_name.clone());
            state.pending_steps.remove(&step_name);
            state.current_step = None;
            Ok(())
        })
        .await?;

        // Record event
        let tx = self
            .pool_for(workflow_id)
            .begin()
            .await
            .map_err(|e| OrcherError::Internal {
                message: format!("Failed to begin transaction: {}", e),
                source: None,
            })?;

        // Note: StepCompleted event is recorded in workflow_execution_journal
        // We do NOT write orchestration events to execution_log

        tx.commit().await.map_err(|e| OrcherError::Internal {
            message: format!("Failed to commit transaction: {}", e),
            source: None,
        })?;

        let mut metrics = self.metrics.write().await;
        metrics.steps_completed += 1;
        drop(metrics);

        info!(
            workflow_id = %workflow_id,
            step_name = %step_name,
            "Step completed successfully"
        );

        // Write user-facing log
        let _ = self
            .write_execution_log(
                &workflow_id.to_string(),
                "info",
                format!("Step '{}' completed", step_name),
                Some(step_name.clone()),
                None,
                None,
            )
            .await;

        Ok(())
    }

    /// Complete a workflow (success)
    pub async fn complete_workflow(&self, workflow_id: Uuid, result: Value) -> OrcherResult<()> {
        info!(workflow_id = %workflow_id, "Completing workflow");

        self.update_state(workflow_id, |state| {
            state.complete(Some(result.clone()));
            Ok(())
        })
        .await?;

        // Note: WorkflowCompleted event is recorded in workflow_execution_journal
        // We do NOT write orchestration events to execution_log

        let mut metrics = self.metrics.write().await;
        metrics.workflows_completed += 1;
        drop(metrics);

        info!(workflow_id = %workflow_id, "Workflow completed successfully");

        // Write user-facing log
        let _ = self
            .write_execution_log(
                &workflow_id.to_string(),
                "info",
                "Workflow completed successfully".to_string(),
                None,
                None,
                Some(serde_json::json!({
                    "result": result,
                })),
            )
            .await;

        Ok(())
    }

    /// Fail a workflow
    pub async fn fail_workflow(&self, workflow_id: Uuid, error: String) -> OrcherResult<()> {
        warn!(workflow_id = %workflow_id, error = %error, "Failing workflow");

        self.update_state(workflow_id, |state| {
            state.fail(error.clone());
            Ok(())
        })
        .await?;

        // Note: WorkflowFailed event is recorded in workflow_execution_journal
        // We do NOT write orchestration events to execution_log

        let mut metrics = self.metrics.write().await;
        metrics.workflows_failed += 1;
        drop(metrics);

        // Write user-facing log
        let _ = self
            .write_execution_log(
                &workflow_id.to_string(),
                "error",
                format!("Workflow failed: {}", error),
                None,
                None,
                Some(serde_json::json!({
                    "error": error,
                })),
            )
            .await;

        // Trigger workflow retry evaluation if scheduler is configured
        if let Some(_scheduler) = &self.workflow_retry_scheduler {
            debug!(
                workflow_id = %workflow_id,
                "Evaluating workflow for automatic retry"
            );

            // Load workflow execution record
            // Note: We need to load the full execution record from the database
            // This will be implemented when the workflow_execution_repository is added to coordinator
            // For now, retry evaluation happens in ExecutionService::fail_workflow_execution
        }

        Ok(())
    }

    // ========================================================================
    // Timer Operations
    // ========================================================================

    /// Schedule a timer for a workflow
    ///
    /// Timers are durable and will fire even if the coordinator crashes.
    pub async fn schedule_timer(
        &self,
        workflow_id: Uuid,
        timer_name: String,
        duration: SignedDuration,
    ) -> OrcherResult<Uuid> {
        self.schedule_timer_inner(workflow_id, timer_name, duration, false)
            .await
            .map(|scheduled| scheduled.expect("an unconditional schedule always creates a timer"))
    }

    /// Schedule a timer unless this workflow execution already has one with
    /// this business name. Returns `None` when it did.
    ///
    /// A workflow replays while its timer is pending and emits the same
    /// StartTimer each time; without this every replay scheduled another copy.
    /// The check and the insert share one transaction.
    pub async fn schedule_timer_once(
        &self,
        workflow_id: Uuid,
        timer_name: String,
        duration: SignedDuration,
    ) -> OrcherResult<Option<Uuid>> {
        self.schedule_timer_inner(workflow_id, timer_name, duration, true)
            .await
    }

    async fn schedule_timer_inner(
        &self,
        workflow_id: Uuid,
        timer_name: String,
        duration: SignedDuration,
        only_if_new: bool,
    ) -> OrcherResult<Option<Uuid>> {
        let fire_at = Timestamptz::now() + duration;

        info!(
            workflow_id = %workflow_id,
            timer_name = %timer_name,
            fire_at = %fire_at,
            "Scheduling timer"
        );

        let timer_id = Uuid::new_v4();

        // Begin transaction
        let mut tx =
            self.pool_for(workflow_id)
                .begin()
                .await
                .map_err(|e| OrcherError::Internal {
                    message: format!("Failed to begin transaction: {}", e),
                    source: None,
                })?;

        if only_if_new
            && self
                .timers
                .fire_timer_exists_in_tx(&mut tx, workflow_id, &timer_name)
                .await?
        {
            debug!(
                workflow_id = %workflow_id,
                timer_name = %timer_name,
                "Timer already scheduled for this execution; not scheduling it again"
            );
            return Ok(None);
        }

        // Create timer task
        let timer_task = TimerTask::new(
            workflow_id,
            TimerTaskType::FireTimer,
            serde_json::json!({
                "timer_name": timer_name,
            }),
            fire_at,
        );

        self.timers.create_task(&mut tx, timer_task).await?;

        // Note: TimerScheduled event is recorded in workflow_execution_journal
        // We do NOT write orchestration events to execution_log

        tx.commit().await.map_err(|e| OrcherError::Internal {
            message: format!("Failed to commit transaction: {}", e),
            source: None,
        })?;

        // After the commit, not inside the transaction: the state update takes
        // a connection of its own, and waiting for one while holding this one
        // is what exhausted the pool under load. The timer row is the durable
        // part; the state's list of timers is bookkeeping, rebuilt from the
        // journal on a cache miss, and read by nothing that decides anything.
        //
        // So a failure here is not the caller's failure. Returned as one, it
        // stopped the StartTimer handler before it journaled TimerStarted, and
        // every replay after that found the timer already scheduled and
        // skipped the journal too: the workflow's history lost the timer.
        if let Err(e) = self
            .update_state(workflow_id, |state| {
                state.add_timer(timer_name.clone(), fire_at);
                Ok(())
            })
            .await
        {
            warn!(
                workflow_id = %workflow_id,
                timer_name = %timer_name,
                error = %e,
                "Timer scheduled; recording it in the cached state failed (bookkeeping only)"
            );
        }

        let mut metrics = self.metrics.write().await;
        metrics.timers_scheduled += 1;
        drop(metrics);

        // Tell the processor to look again. Signalled after commit so it cannot
        // read ahead of the row, and `notify_one` rather than `notify_waiters`
        // so a signal raised while the processor is mid-cycle is not lost.
        if let Some(wake) = self.timer_wake.get() {
            wake.notify_one();
        }

        info!(
            workflow_id = %workflow_id,
            timer_id = %timer_id,
            "Timer scheduled successfully"
        );

        Ok(Some(timer_id))
    }

    /// Handle timer fire event
    ///
    /// Called by the TimerProcessor when a timer fires.
    pub async fn handle_timer_fire(
        &self,
        workflow_id: Uuid,
        timer_name: String,
    ) -> OrcherResult<()> {
        info!(
            workflow_id = %workflow_id,
            timer_name = %timer_name,
            "Handling timer fire"
        );

        self.update_state(workflow_id, |state| {
            state.remove_timer(&timer_name);
            Ok(())
        })
        .await?;

        // Record event
        let tx = self
            .pool_for(workflow_id)
            .begin()
            .await
            .map_err(|e| OrcherError::Internal {
                message: format!("Failed to begin transaction: {}", e),
                source: None,
            })?;

        // Note: TimerFired event is recorded in workflow_execution_journal
        // We do NOT write orchestration events to execution_log

        tx.commit().await.map_err(|e| OrcherError::Internal {
            message: format!("Failed to commit transaction: {}", e),
            source: None,
        })?;

        Ok(())
    }

    // ========================================================================
    // Task Operations
    // ========================================================================

    /// Dispatch a task via the outbox, in a transaction of its own.
    ///
    /// For a task row that is already committed and needs another dispatch —
    /// a retry, whose attempt number keys a dispatch of its own. A first
    /// attempt is dispatched with
    /// [`dispatch_first_attempt_in_tx`](Self::dispatch_first_attempt_in_tx),
    /// in the transaction that writes its row.
    pub async fn dispatch_task(
        &self,
        workflow_id: Uuid,
        task_type: OutboxTaskType,
        payload: Value,
    ) -> OrcherResult<Uuid> {
        debug!(
            workflow_id = %workflow_id,
            task_type = ?task_type,
            "COORDINATOR: Entering dispatch_task"
        );

        let mut tx = self.begin_for(workflow_id).await?;
        let task = self.outbox_task(workflow_id, task_type, &payload);
        let task_id = task.task_id;
        self.outbox.create_task(&mut tx, task).await?;

        debug!(
            workflow_id = %workflow_id,
            task_id = %task_id,
            task_type = ?task_type,
            "COORDINATOR: Outbox task created successfully"
        );

        // Note: TaskScheduled event is already recorded in workflow_execution_journal
        // via the outbox.create_task() call above. We do NOT dual-write to execution_log.
        // execution_log is only for step results (task outputs and closure results),
        // NOT for orchestration events.

        tx.commit().await.map_err(|e| OrcherError::Internal {
            message: format!("Failed to commit transaction: {}", e),
            source: None,
        })?;

        self.announce_dispatch(workflow_id, task_type, task_id, &payload)
            .await;
        Ok(task_id)
    }

    /// Dispatch a first attempt that is scheduled but was never dispatched.
    ///
    /// For a row an engine left behind when it stopped between writing it and
    /// dispatching it, before the two were written together. Called when the
    /// workflow re-emits the ScheduleTask, and by the sweep for rows no replay
    /// comes back to. Returns whether this call put a new dispatch in the
    /// outbox.
    ///
    /// Safe to call on any row, from any number of engines at once. The row is
    /// claimed with a conditional update, so one caller dispatches it and the
    /// rest find it taken; and the outbox row carries the attempt's
    /// idempotency key, so a dispatch that is already there — made by an older
    /// engine that never marked the row — is not made again.
    pub async fn dispatch_if_never_dispatched(&self, task: &TaskExecution) -> OrcherResult<bool> {
        let workflow_id = task.workflow_id.as_uuid();
        let mut tx = self.begin_for(workflow_id).await?;
        let dispatched = self.dispatch_first_attempt_in_tx(&mut tx, task).await?;
        tx.commit().await.map_err(|e| OrcherError::Internal {
            message: format!("Failed to commit transaction: {}", e),
            source: None,
        })?;
        match dispatched {
            Some(dispatch) => {
                self.announce(dispatch).await;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Claim a first attempt's dispatch and put it in the outbox, inside `tx`.
    ///
    /// The task row and its outbox dispatch used to be written one after the
    /// other, each committing on its own. An engine that stopped between the
    /// two left a task scheduled that nothing would ever deliver — the outbox
    /// row is the only way a task reaches a worker — and the ScheduleTask the
    /// workflow re-emitted on replay found the row in flight and did nothing
    /// more. Written in the transaction that writes the row, a scheduled first
    /// attempt always has its dispatch, and `last_dispatched_at` says so.
    ///
    /// `None` when the row was already claimed or its dispatch already exists.
    /// Otherwise the dispatch is to be passed to [`announce`](Self::announce)
    /// once `tx` has committed, not before: announcing takes connections of
    /// its own, and a worker woken early would find nothing to claim.
    pub async fn dispatch_first_attempt_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'static, sqlx::Postgres>,
        task: &TaskExecution,
    ) -> OrcherResult<Option<TaskDispatch>> {
        let scheduled_since = self.undispatched_repair_cutoff();
        if !TaskExecutionRepository::claim_first_dispatch(tx, task.id, scheduled_since).await? {
            return Ok(None);
        }
        let workflow_id = task.workflow_id.as_uuid();
        let payload = Self::task_dispatch_payload(task);
        let outbox_task = self.outbox_task(workflow_id, OutboxTaskType::DispatchTask, &payload);
        Ok(self
            .outbox
            .create_task_if_new(tx, outbox_task)
            .await?
            .map(|outbox_id| TaskDispatch {
                workflow_id,
                outbox_id,
                payload,
            }))
    }

    /// Announce a dispatch from
    /// [`dispatch_first_attempt_in_tx`](Self::dispatch_first_attempt_in_tx)
    /// whose transaction has committed.
    pub async fn announce(&self, dispatch: TaskDispatch) {
        self.announce_dispatch(
            dispatch.workflow_id,
            OutboxTaskType::DispatchTask,
            dispatch.outbox_id,
            &dispatch.payload,
        )
        .await;
    }

    /// The outbox payload that delivers `task` to a worker: what the task
    /// poller matches on (task_queue, namespace) and rebuilds the task from.
    /// `attempt_number` keys the dispatch, so each attempt is dispatched once.
    pub fn task_dispatch_payload(task: &TaskExecution) -> Value {
        serde_json::json!({
            "workflow_id": task.workflow_id.as_uuid().to_string(),
            "execution_id": task.execution_id.as_uuid().to_string(),
            "task_id": task.task_id,
            "task_type": task.task_type,
            "task_queue": task.task_queue,
            "namespace": task.namespace,
            "input": task.input.clone().unwrap_or_default(),
            "schedule_to_start_timeout_secs": task.schedule_to_start_timeout_secs,
            "start_to_close_timeout_secs": task.start_to_close_timeout_secs,
            "heartbeat_timeout_secs": task.heartbeat_timeout_secs,
            "retry_policy": task.retry_policy,
            "task_execution_id": task.id.as_uuid().to_string(),
            "attempt_number": task.attempt_number,
            // Carried so the dispatch is stamped with its organization, which
            // the claims that hand it to a worker are scoped by.
            "organization_id": task.organization_id,
        })
    }

    /// A transaction on the database that owns `workflow_id`'s shard.
    pub async fn begin_for(
        &self,
        workflow_id: Uuid,
    ) -> OrcherResult<sqlx::Transaction<'static, sqlx::Postgres>> {
        self.pool_for(workflow_id)
            .begin()
            .await
            .map_err(|e| OrcherError::Internal {
                message: format!("Failed to begin transaction: {}", e),
                source: None,
            })
    }

    /// The idempotency key of one attempt's dispatch in the outbox:
    /// `workflow_id:execution_id:task_type:task_id:attempt`. The one place it
    /// is built. Outbox rows written by earlier engines carry it, and V050's
    /// backfill matches first attempts to their dispatch by it, so its format
    /// cannot change.
    pub fn task_dispatch_key(
        workflow_id: Uuid,
        execution_id: &str,
        task_type: OutboxTaskType,
        task_id: &str,
        attempt_number: i64,
    ) -> String {
        format!("{workflow_id}:{execution_id}:{task_type}:{task_id}:{attempt_number}")
    }

    /// The outbox row for one dispatch, keyed so the same attempt of the same
    /// task is never dispatched twice.
    fn outbox_task(
        &self,
        workflow_id: Uuid,
        task_type: OutboxTaskType,
        payload: &Value,
    ) -> OutboxTask {
        // Extract stable identifiers from payload for idempotency key
        // Use workflow_id + execution_id + task_id (from workflow) to ensure
        // the same task for the same execution cannot be dispatched twice
        let execution_id = payload
            .get("execution_id")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        let workflow_task_id = payload
            .get("task_id")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        // Attempt number distinguishes a retry dispatch from the original (and from other
        // attempts). Absent on the initial dispatch → treated as attempt 1. Without this a
        // retry would reuse the initial dispatch's idempotency key and be dropped by the
        // outbox's ON CONFLICT (idempotency_key) DO NOTHING.
        let attempt_number = payload
            .get("attempt_number")
            .and_then(|v| v.as_i64())
            .unwrap_or(1);

        let mut task = OutboxTask::new(workflow_id, task_type, payload.clone(), None);
        task.task_id = Uuid::new_v4();
        // The organization the work belongs to. A worker acting for an
        // organization claims only its own organization's dispatches, from the
        // outbox and from the matching partitions alike; one left unstamped
        // would reach only workers acting for none.
        task.organization_id = payload
            .get("organization_id")
            .and_then(|v| v.as_str())
            .and_then(|v| Uuid::parse_str(v).ok());
        // Use stable identifiers for idempotency: workflow_id + execution_id + task_id + attempt.
        // This prevents duplicate dispatch of the same attempt while still allowing each retry
        // attempt to be dispatched exactly once.
        task.idempotency_key = Some(Self::task_dispatch_key(
            workflow_id,
            execution_id,
            task_type,
            workflow_task_id,
            attempt_number,
        ));
        // Stamp the shard from the same router that just chose this row's pool, so a
        // row can never claim a shard it was not routed to.
        task.shard_id = self.shard_for(workflow_id);
        task
    }

    /// What follows a committed dispatch: the user-facing log line, the
    /// metric, and a wake-up for a poller parked on the task's queue.
    async fn announce_dispatch(
        &self,
        workflow_id: Uuid,
        task_type: OutboxTaskType,
        task_id: Uuid,
        payload: &Value,
    ) {
        // Write user-facing log. After the commit: it goes through the pool,
        // and taking a second connection while the transaction held the first
        // is how concurrent dispatches exhausted it — each holding one, all
        // waiting for another. The log line is best-effort either way.
        let _ = self
            .write_execution_log(
                &workflow_id.to_string(),
                "info",
                format!("Task '{}' scheduled", task_type),
                Some(task_id.to_string()),
                Some(task_id),
                Some(serde_json::json!({
                    "task_type": task_type,
                    "task_id": task_id.to_string(),
                })),
            )
            .await;

        let mut metrics = self.metrics.write().await;
        metrics.tasks_dispatched += 1;
        drop(metrics);

        debug!(
            workflow_id = %workflow_id,
            task_id = %task_id,
            task_type = ?task_type,
            "COORDINATOR: Task dispatched successfully"
        );

        // Wake one waiting task poller (notify_one, not notify_all)
        // Sync-match (direct task delivery) is disabled — it caused task loss
        // under high concurrency. Wake-match is safe: poller wakes and claims
        // from the outbox, which has durability guarantees.
        let task_manager_guard = self.task_polling_manager.read().await;
        if let Some(ref task_manager) = *task_manager_guard {
            if let (Some(queue), Some(ns)) = (
                payload.get("task_queue").and_then(|v| v.as_str()),
                payload.get("namespace").and_then(|v| v.as_str()),
            ) {
                let woken = task_manager.waiters_registry().wake_waiters(ns, queue);
                if woken == 0 {
                    // No waiters — fall back to broadcast notifier
                    task_manager
                        .notifier()
                        .notify_work_available(queue, ns)
                        .await;
                }
                debug!(
                    task_queue = queue,
                    namespace = ns,
                    task_id = %task_id,
                    "COORDINATOR: Woke task poller for outbox claim"
                );
            } else {
                warn!(
                    task_id = %task_id,
                    "Failed to extract task_queue/namespace from payload for notification"
                );
            }
        } else {
            warn!(
                task_id = %task_id,
                "TaskPollingManager not configured - no notification sent"
            );
        }
    }

    // ========================================================================
    // Recovery Operations
    // ========================================================================

    /// Recover in-flight workflows after a crash
    ///
    /// This is called on coordinator startup to resume any workflows that
    /// were running when the previous coordinator crashed.
    async fn recover_inflight_workflows(&self) -> OrcherResult<()> {
        info!(
            instance_id = %self.config.instance_id,
            "Starting crash recovery"
        );

        // Find all running workflows across every shard database (scatter-gather;
        // a single pool with no router). A workflow is recovered on its own shard's
        // pool via reconstruct(), so unioning the ids here is sufficient.
        let mut running_workflows: Vec<(Uuid,)> = Vec::new();
        for pool in self.all_pools() {
            let mut rows: Vec<(Uuid,)> = sqlx::query_as(
                r#"
                SELECT id
                FROM workflow_executions
                WHERE status IN ('running', 'pending')
                ORDER BY started_at ASC
                "#,
            )
            .fetch_all(pool)
            .await
            .map_err(|e| OrcherError::Internal {
                message: format!("Failed to query running workflows: {}", e),
                source: None,
            })?;
            running_workflows.append(&mut rows);
        }

        info!(
            count = running_workflows.len(),
            "Found in-flight workflows to recover"
        );

        // Recover workflows in parallel (with limit)
        let mut tasks = vec![];
        for (workflow_id,) in running_workflows {
            let coordinator = self.clone();
            let task = tokio::spawn(async move {
                match coordinator.recover_workflow(workflow_id).await {
                    Ok(_) => {
                        debug!(workflow_id = %workflow_id, "Workflow recovered successfully");
                    }
                    Err(e) => {
                        warn!(
                            workflow_id = %workflow_id,
                            error = %e,
                            "Failed to recover workflow"
                        );
                    }
                }
            });
            tasks.push(task);

            // Limit parallelism
            if tasks.len() >= self.config.recovery_parallelism {
                for task in tasks.drain(..) {
                    let _ = task.await;
                }
            }
        }

        // Wait for remaining tasks
        for task in tasks {
            let _ = task.await;
        }

        info!("Crash recovery completed");

        Ok(())
    }

    /// Recover a single workflow (with retry for transient errors)
    async fn recover_workflow(&self, workflow_id: Uuid) -> OrcherResult<()> {
        debug!(workflow_id = %workflow_id, "Recovering workflow");

        let max_retries = 3;
        let mut last_error = None;

        for attempt in 0..max_retries {
            if attempt > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(500 * attempt as u64)).await;
            }

            match self
                .reconstructor
                .reconstruct(workflow_id, self.pool_for(workflow_id))
                .await
            {
                Ok(state) => {
                    self.cache.insert(state).await?;
                    return Ok(());
                }
                Err(e) => {
                    let is_transient = e.to_string().contains("pool timed out")
                        || e.to_string().contains("connection reset");
                    if is_transient && attempt + 1 < max_retries {
                        debug!(
                            workflow_id = %workflow_id,
                            attempt = attempt + 1,
                            error = %e,
                            "Transient error during recovery, retrying"
                        );
                        last_error = Some(e);
                        continue;
                    }
                    return Err(e);
                }
            }
        }

        Err(last_error.unwrap_or_else(|| OrcherError::Internal {
            message: "Recovery failed after retries".to_string(),
            source: None,
        }))
    }

    // ========================================================================
    // Helper Methods
    // ========================================================================

    /// DEPRECATED: This method is no longer used.
    ///
    /// Orchestration events (WorkflowStarted, TaskScheduled, etc.) are now written
    /// ONLY to workflow_execution_journal, not to execution_log.
    ///
    /// execution_log is now used exclusively for step result caching:
    /// - Task results (from ctx.execute_task)
    /// - Closure results (from ctx.execute)
    ///
    /// This prevents dual-write complexity and aligns with event sourcing best practices.
    #[deprecated(note = "Orchestration events should only go to workflow_execution_journal")]
    #[allow(dead_code)]
    async fn record_event(
        &self,
        _tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        _workflow_id: Uuid,
        _event_type: EventType,
        _payload: Option<Value>,
    ) -> OrcherResult<()> {
        // No-op: orchestration events are now written directly to workflow_execution_journal
        // by the respective coordinator methods (via journal.append or outbox operations)
        Ok(())
    }

    /// Write a user-facing log entry to workflow_execution_logs
    ///
    /// This provides observability for end users streaming workflow progress.
    async fn write_execution_log(
        &self,
        workflow_id: &str,
        level: &str,
        message: String,
        step: Option<String>,
        task_id: Option<Uuid>,
        metadata: Option<Value>,
    ) -> OrcherResult<()> {
        let source = "WorkflowCoordinator";

        // The coordinator identifies a workflow by its internal primary key
        // (`workflow_executions.id`), but `workflow_execution_logs.execution_id`
        // has a foreign key to the external `workflow_executions.execution_id`.
        // Resolve the external id from the internal one so the insert satisfies
        // the FK (previously the internal id was written as the execution_id,
        // which failed the constraint and silently dropped every log line).
        let result = sqlx::query(
            r#"
            INSERT INTO workflow_execution_logs
                (execution_id, level, message, step, task_id, source, metadata)
            SELECT execution_id, $2, $3, $4, $5, $6, $7
            FROM workflow_executions
            WHERE id = $1::uuid
            "#,
        )
        .bind(workflow_id)
        .bind(level)
        .bind(&message)
        .bind(step)
        .bind(task_id)
        .bind(source)
        .bind(metadata.unwrap_or(serde_json::json!({})))
        // NOTE: execution_log routing deferred — write_execution_log takes a &str
        // workflow_id (must confirm it matches the Uuid string pool_for hashes) and
        // execution_log co-location is a separate design question. Stays on `pool`.
        .execute(&*self.pool)
        .await
        .map_err(|e| {
            warn!(
                workflow_id = %workflow_id,
                error = %e,
                "Failed to write execution log"
            );
            OrcherError::internal(format!("Failed to write execution log: {}", e))
        })?;

        if result.rows_affected() == 0 {
            debug!(
                workflow_id = %workflow_id,
                "Execution log skipped: no matching workflow_executions row"
            );
        }

        Ok(())
    }

    /// Get coordinator metrics
    pub async fn get_metrics(&self) -> CoordinatorMetrics {
        self.metrics.read().await.clone()
    }

    /// Get cache statistics
    pub async fn get_cache_stats(&self) -> CacheStats {
        self.cache.get_stats().await
    }

    /// Get reference to the outbox queue
    ///
    /// Allows external components to claim tasks from the outbox for processing.
    pub fn outbox(&self) -> &Arc<OutboxQueue> {
        &self.outbox
    }

    /// Set the task polling manager for notification-based task polling
    ///
    /// This enables the coordinator to send notifications when tasks are dispatched,
    /// allowing workers to receive tasks immediately without database polling.
    ///
    /// This method can be called on Arc<Self> thanks to interior mutability via RwLock.
    ///
    /// # Arguments
    ///
    /// * `task_polling_manager` - The task polling manager instance
    pub async fn set_task_polling_manager(
        &self,
        task_polling_manager: Arc<crate::engine::dispatch::TaskPollingManager>,
    ) {
        info!("WorkflowCoordinator: Setting task_polling_manager for notification-based dispatch");
        let mut guard = self.task_polling_manager.write().await;
        *guard = Some(task_polling_manager);
    }

    /// Set the task dispatch service for sync-match fast path on task dispatch.
    pub fn set_task_dispatch_service(
        &self,
        service: Arc<crate::services::task_dispatch::TaskDispatchService>,
    ) {
        let _ = self.task_dispatch_service.set(service);
    }

    /// Let the coordinator wake the timer processor when it schedules a timer
    /// (called once at startup). Until set, timers wait for the next poll.
    pub fn set_timer_wake(&self, wake: Arc<tokio::sync::Notify>) {
        let _ = self.timer_wake.set(wake);
    }

    /// Enable shard-ownership fencing on the write path (called once at startup by
    /// the orchestrator when clustering is enabled). Until set, writes are unguarded.
    pub fn set_shard_ownership(&self, ownership: Arc<orcher_cluster::ShardOwnership>) {
        let _ = self.shard_ownership.set(ownership);
    }

    /// Enable shard→database routing (called once at startup for partitioned
    /// persistence). Until set — or with a single-pool router — writes use the
    /// coordinator's own `pool`.
    pub fn set_shard_router(&self, router: Arc<dyn orcher_cluster::ShardRouter>) {
        let _ = self.shard_router.set(router);
    }

    /// Tell the coordinator how many days completed outbox rows are kept — the
    /// configured `retention_days.outbox_queue` (called once at startup).
    pub fn set_outbox_retention_days(&self, days: i32) {
        let _ = self.outbox_retention_days.set(days);
    }

    /// See [`undispatched_repair_window`]: for the configured outbox
    /// retention, or its default when none was set.
    pub fn undispatched_repair_window(&self) -> SignedDuration {
        undispatched_repair_window(
            self.outbox_retention_days
                .get()
                .copied()
                .unwrap_or_else(|| crate::jobs::RetentionDays::default().outbox_queue),
        )
    }

    /// The earliest `scheduled_at` of a first attempt this coordinator will
    /// dispatch as never dispatched: now, less the repair window.
    pub fn undispatched_repair_cutoff(&self) -> Timestamptz {
        let now = Timestamptz::now();
        now.checked_sub(self.undispatched_repair_window())
            .unwrap_or(now)
    }

    /// The database pool a `workflow_id`'s data lives on — reads and writes alike,
    /// since a workflow's whole state is co-located in its shard. Its shard's pool
    /// when a router is set, else the coordinator's single pool.
    fn pool_for(&self, workflow_id: Uuid) -> &PgPool {
        match self.shard_router.get() {
            Some(router) => router.pool_for_workflow(&workflow_id.to_string()),
            None => &self.pool,
        }
    }

    /// The virtual shard `workflow_id` belongs to, from the same router that picks
    /// its pool — so a persisted `shard_id` and the pool the row landed in can
    /// never disagree. `None` when no router is injected; rows written then carry
    /// no shard and are simply not claimable by shard-filtered queries.
    fn shard_for(&self, workflow_id: Uuid) -> Option<i32> {
        self.shard_router.get().map(|router| {
            orcher_cluster::ShardId::of(&workflow_id.to_string(), router.vshards()).0 as i32
        })
    }

    /// Every database that holds workflow data — for cross-shard scans (recovery,
    /// list/search). With a router this is all its pools; otherwise the single pool.
    fn all_pools(&self) -> Vec<&PgPool> {
        match self.shard_router.get() {
            Some(router) => router.all_pools(),
            None => vec![&self.pool],
        }
    }

    /// Get reference to the timer queue
    ///
    /// Allows external components to access timer queue functionality.
    pub fn timers(&self) -> &Arc<TimerQueue> {
        &self.timers
    }
}

// Clone implementation for spawning recovery tasks
impl Clone for WorkflowCoordinator {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            config: self.config.clone(),
            cache: self.cache.clone(),
            invalidator: self.invalidator.clone(),
            reconstructor: self.reconstructor.clone(),
            outbox: self.outbox.clone(),
            timers: self.timers.clone(),
            metrics: self.metrics.clone(),
            task_polling_manager: self.task_polling_manager.clone(),
            workflow_retry_scheduler: self.workflow_retry_scheduler.clone(),
            task_dispatch_service: {
                let lock = std::sync::OnceLock::new();
                if let Some(svc) = self.task_dispatch_service.get() {
                    let _ = lock.set(svc.clone());
                }
                lock
            },
            timer_wake: {
                let lock = std::sync::OnceLock::new();
                if let Some(wake) = self.timer_wake.get() {
                    let _ = lock.set(wake.clone());
                }
                lock
            },
            shard_ownership: {
                let lock = std::sync::OnceLock::new();
                if let Some(own) = self.shard_ownership.get() {
                    let _ = lock.set(own.clone());
                }
                lock
            },
            shard_router: {
                let lock = std::sync::OnceLock::new();
                if let Some(r) = self.shard_router.get() {
                    let _ = lock.set(r.clone());
                }
                lock
            },
            outbox_retention_days: {
                let lock = std::sync::OnceLock::new();
                if let Some(days) = self.outbox_retention_days.get() {
                    let _ = lock.set(*days);
                }
                lock
            },
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// The key 0.4.1 gave a first attempt's dispatch, written out as it built
    /// it — its payload carried no attempt number, which read as 1. V050's
    /// backfill and the outbox's dedup both depend on the key never changing.
    #[test]
    fn a_dispatch_key_is_the_one_earlier_engines_wrote() {
        let workflow_id = Uuid::parse_str("0e7a6d98-3eeb-4e38-88cc-bbf062efbb9a").unwrap();
        let execution_id = "554f2aeb-5bef-482d-90d5-b8ee87bfd428";
        assert_eq!(
            WorkflowCoordinator::task_dispatch_key(
                workflow_id,
                execution_id,
                OutboxTaskType::DispatchTask,
                "task_0",
                1,
            ),
            format!(
                "{}:{}:dispatch_task:{}:{}",
                workflow_id, execution_id, "task_0", 1
            )
        );
    }

    /// The window stays inside the retention period by a day, or by half the
    /// period when that is shorter.
    #[test]
    fn the_repair_window_stays_inside_outbox_retention() {
        assert_eq!(
            undispatched_repair_window(7),
            SignedDuration::from_hours(6 * 24)
        );
        assert_eq!(
            undispatched_repair_window(2),
            SignedDuration::from_hours(24)
        );
        assert_eq!(
            undispatched_repair_window(1),
            SignedDuration::from_hours(12)
        );
        assert_eq!(
            undispatched_repair_window(0),
            SignedDuration::from_hours(12)
        );
    }

    #[test]
    fn test_coordinator_config_default() {
        let config = CoordinatorConfig::default();
        assert!(config.enable_recovery);
        assert_eq!(config.recovery_parallelism, 10);
    }

    #[test]
    fn test_coordinator_metrics_default() {
        let metrics = CoordinatorMetrics::default();
        assert_eq!(metrics.workflows_started, 0);
        assert_eq!(metrics.workflows_completed, 0);
        assert_eq!(metrics.cache_hits, 0);
    }

    // Integration tests require a PostgreSQL instance
    #[tokio::test]
    #[ignore = "requires PostgreSQL"]
    async fn test_coordinator_lifecycle() {
        // TODO: Implement integration test
    }
}
