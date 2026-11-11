//! Engine system lifecycle and background task management.

use crate::engine::coordinator::{CoordinatorConfig, WorkflowCoordinator};
use crate::engine::processors::{
    OutboxProcessor, OutboxProcessorConfig, ScheduleProcessor, ScheduleProcessorConfig,
    TimerProcessor, TimerProcessorConfig,
};
use orcher_common::error::OrcherResult;
use orcher_common::types::time::Timestamptz;
use sqlx::PgPool;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for the orchestration system
#[derive(Debug, Clone)]
pub struct OrchestrationConfig {
    /// Coordinator configuration
    pub coordinator: CoordinatorConfig,

    /// Outbox processor configuration
    pub outbox_processor: OutboxProcessorConfig,

    /// Timer processor configuration
    pub timer_processor: TimerProcessorConfig,

    /// Schedule processor configuration
    pub schedule_processor: ScheduleProcessorConfig,

    /// Enable background processors
    pub enable_processors: bool,

    /// Graceful shutdown timeout in seconds
    pub shutdown_timeout_secs: u64,
}

impl Default for OrchestrationConfig {
    fn default() -> Self {
        Self {
            coordinator: CoordinatorConfig::default(),
            outbox_processor: OutboxProcessorConfig::default(),
            timer_processor: TimerProcessorConfig::default(),
            schedule_processor: ScheduleProcessorConfig::default(),
            enable_processors: false, // Changed to false - processors need custom executors
            shutdown_timeout_secs: 30,
        }
    }
}

// ============================================================================
// System State
// ============================================================================

/// State of a background processor
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessorState {
    /// Processor is stopped
    Stopped,
    /// Processor is starting
    Starting,
    /// Processor is running
    Running,
    /// Processor is shutting down
    ShuttingDown,
    /// Processor has failed
    Failed,
}

/// System health status
#[derive(Debug, Clone)]
pub struct SystemHealth {
    /// Coordinator is healthy
    pub coordinator_healthy: bool,

    /// Outbox processor state
    pub outbox_state: ProcessorState,

    /// Timer processor state
    pub timer_state: ProcessorState,

    /// Schedule processor state
    pub schedule_state: ProcessorState,

    /// Overall system is healthy
    pub healthy: bool,

    /// Last health check timestamp
    pub checked_at: Timestamptz,
}

impl SystemHealth {
    /// Check if the system is fully operational
    pub fn is_operational(&self) -> bool {
        self.healthy
            && self.coordinator_healthy
            && self.outbox_state == ProcessorState::Running
            && self.timer_state == ProcessorState::Running
    }
}

// ============================================================================
// Orchestration System
// ============================================================================

/// Complete orchestration system with coordinator and background processors
pub struct OrchestrationSystem {
    /// Database connection pool
    pool: Arc<PgPool>,

    /// Configuration
    config: OrchestrationConfig,

    /// Workflow coordinator
    coordinator: Arc<WorkflowCoordinator>,

    /// Outbox processor
    outbox_processor: Arc<OutboxProcessor>,

    /// Timer processor
    timer_processor: Arc<TimerProcessor>,

    /// Schedule processor
    schedule_processor: Arc<ScheduleProcessor>,

    /// Outbox processor task handle
    outbox_handle: Arc<RwLock<Option<JoinHandle<()>>>>,

    /// Timer processor task handle
    timer_handle: Arc<RwLock<Option<JoinHandle<()>>>>,

    /// Schedule processor task handle
    schedule_handle: Arc<RwLock<Option<JoinHandle<()>>>>,

    /// Per-shard-database outbox/timer processors (partitioned persistence). Empty
    /// for a single-database deployment; one each per additional shard database, so
    /// tasks written to a shard's transactional outbox are claimed from that shard.
    /// The primary pool's processors above cover shard-0 (the coordination database).
    shard_outbox_processors: Vec<Arc<OutboxProcessor>>,
    shard_timer_processors: Vec<Arc<TimerProcessor>>,
    /// Handles for the per-shard processor tasks.
    shard_handles: Arc<RwLock<Vec<JoinHandle<()>>>>,

    /// Shutdown signal
    shutdown_tx: Arc<RwLock<Option<tokio::sync::broadcast::Sender<()>>>>,
}

impl OrchestrationSystem {
    /// Create a new orchestration system
    ///
    /// This initializes the coordinator and background processors but does not
    /// start them. Call `start()` to begin processing.
    pub async fn new(pool: Arc<PgPool>, config: OrchestrationConfig) -> OrcherResult<Self> {
        Self::new_partitioned(pool, Vec::new(), config).await
    }

    /// Create an orchestration system over a primary (coordination) database plus
    /// zero or more shard databases (partitioned persistence). Each shard database
    /// gets its own outbox + timer processor so tasks written to its transactional
    /// outbox are claimed locally; the primary's processors cover shard-0. Passing an
    /// empty `shard_pools` is identical to a single-database deployment.
    pub async fn new_partitioned(
        pool: Arc<PgPool>,
        shard_pools: Vec<Arc<PgPool>>,
        config: OrchestrationConfig,
    ) -> OrcherResult<Self> {
        info!(
            instance_id = %config.coordinator.instance_id,
            shard_databases = shard_pools.len(),
            "Initializing OrchestrationSystem"
        );

        // Initialize coordinator
        let coordinator =
            Arc::new(WorkflowCoordinator::new(pool.clone(), config.coordinator.clone()).await?);

        // Initialize processors (but don't start them yet - they need custom executors)
        use orcher_storage::repositories::domains::coordination::{
            CoordinationRepository, OutboxRepository, TimerRepository,
        };

        // Create coordination repository for DLQ operations
        let coordination_repo = Arc::new(CoordinationRepository::new(pool.clone()));

        let outbox_repo = Arc::new(OutboxRepository::new(pool.clone()));
        let outbox_queue = crate::engine::queues::OutboxQueue::new(outbox_repo);
        let outbox_processor = Arc::new(OutboxProcessor::new(
            outbox_queue,
            coordination_repo.clone(),
            config.outbox_processor.clone(),
        ));

        let timer_repo = Arc::new(TimerRepository::new(pool.clone()));
        let timer_queue = crate::engine::queues::TimerQueue::new(timer_repo);
        let timer_processor = Arc::new(TimerProcessor::new(
            timer_queue,
            coordination_repo.clone(),
            config.timer_processor.clone(),
        ));

        // Let a scheduled timer wake the processor immediately instead of
        // waiting out its idle sleep.
        coordinator.set_timer_wake(timer_processor.wake_handle());

        let schedule_repo = Arc::new(
            orcher_storage::repositories::domains::schedule::ScheduleRepository::new(
                (*pool).clone(),
            ),
        );
        let schedule_queue = crate::engine::queues::ScheduleQueue::new(schedule_repo);
        // Schedules are not sharded (cron-like, global) — a single schedule
        // processor on the primary/coordination database.
        let schedule_processor = Arc::new(ScheduleProcessor::new(
            schedule_queue,
            config.schedule_processor.clone(),
        ));

        // Per-shard outbox + timer processors: one each per additional shard
        // database, with its own DLQ/coordination repo co-located on that database.
        let mut shard_outbox_processors = Vec::with_capacity(shard_pools.len());
        let mut shard_timer_processors = Vec::with_capacity(shard_pools.len());
        for sp in &shard_pools {
            let shard_coord = Arc::new(CoordinationRepository::new(sp.clone()));
            let obx = crate::engine::queues::OutboxQueue::new(Arc::new(OutboxRepository::new(
                sp.clone(),
            )));
            shard_outbox_processors.push(Arc::new(OutboxProcessor::new(
                obx,
                shard_coord.clone(),
                config.outbox_processor.clone(),
            )));
            let tmr =
                crate::engine::queues::TimerQueue::new(Arc::new(TimerRepository::new(sp.clone())));
            shard_timer_processors.push(Arc::new(TimerProcessor::new(
                tmr,
                shard_coord,
                config.timer_processor.clone(),
            )));
        }

        // Create shutdown channel
        let (shutdown_tx, _) = tokio::sync::broadcast::channel(1);

        let system = Self {
            pool,
            config: config.clone(),
            coordinator,
            outbox_processor,
            timer_processor,
            schedule_processor,
            outbox_handle: Arc::new(RwLock::new(None)),
            timer_handle: Arc::new(RwLock::new(None)),
            schedule_handle: Arc::new(RwLock::new(None)),
            shard_outbox_processors,
            shard_timer_processors,
            shard_handles: Arc::new(RwLock::new(Vec::new())),
            shutdown_tx: Arc::new(RwLock::new(Some(shutdown_tx))),
        };

        // Start background processors if enabled
        if config.enable_processors {
            system.start_processors().await?;
        }

        info!(
            instance_id = %config.coordinator.instance_id,
            processors_enabled = config.enable_processors,
            "OrchestrationSystem initialized successfully"
        );

        Ok(system)
    }

    /// Get a reference to the workflow coordinator
    ///
    /// Use this to interact with workflows (start, complete, etc.)
    pub fn coordinator(&self) -> &WorkflowCoordinator {
        &self.coordinator
    }

    /// Get an Arc clone of the workflow coordinator
    ///
    /// Use this when you need to share the coordinator across threads or store it
    pub fn coordinator_arc(&self) -> Arc<WorkflowCoordinator> {
        self.coordinator.clone()
    }

    /// Get coordinator metrics
    pub async fn coordinator_metrics(&self) -> crate::engine::coordinator::CoordinatorMetrics {
        self.coordinator.get_metrics().await
    }

    /// Get a reference to the outbox processor
    ///
    /// Use this to start the processor with a custom task executor
    pub fn outbox_processor(&self) -> &OutboxProcessor {
        &self.outbox_processor
    }

    /// Get an Arc clone of the outbox processor
    ///
    /// Use this when you need to share the processor across threads or start it
    pub fn outbox_processor_arc(&self) -> Arc<OutboxProcessor> {
        self.outbox_processor.clone()
    }

    /// Get a reference to the timer processor
    ///
    /// Use this to start the processor with a custom timer handler
    pub fn timer_processor(&self) -> &TimerProcessor {
        &self.timer_processor
    }

    /// Get an Arc clone of the timer processor
    ///
    /// Use this when you need to share the processor across threads or start it
    pub fn timer_processor_arc(&self) -> Arc<TimerProcessor> {
        self.timer_processor.clone()
    }

    /// Get outbox processor metrics
    pub async fn outbox_metrics(&self) -> crate::engine::processors::ProcessorMetrics {
        self.outbox_processor.get_metrics().await
    }

    /// Get timer processor metrics
    pub async fn timer_metrics(&self) -> crate::engine::processors::TimerMetrics {
        self.timer_processor.get_metrics().await
    }

    /// Get a reference to the schedule processor
    pub fn schedule_processor(&self) -> &ScheduleProcessor {
        &self.schedule_processor
    }

    /// Get an Arc clone of the schedule processor
    pub fn schedule_processor_arc(&self) -> Arc<ScheduleProcessor> {
        self.schedule_processor.clone()
    }

    /// Get schedule processor metrics
    pub async fn schedule_metrics(&self) -> crate::engine::processors::ScheduleMetrics {
        self.schedule_processor.get_metrics().await
    }

    /// Start background processors
    async fn start_processors(&self) -> OrcherResult<()> {
        info!("Starting background processors with default executors");

        // Create default no-op executor for outbox tasks
        // This logs a warning when tasks need execution - allowing the system to be operational
        // while indicating that proper task executors should be wired up for production use
        let outbox_executor = |task: crate::engine::queues::OutboxTask| {
            Box::pin(async move {
                warn!(
                    task_id = %task.task_id,
                    task_type = ?task.task_type,
                    "OutboxTask received but no custom executor configured - task will be re-queued"
                );
                // Return error so task gets re-queued and can be picked up by a proper executor
                Err(orcher_common::error::OrcherError::Configuration {
                    key: "outbox_task_executor".to_string(),
                    message: "No custom task executor configured".to_string(),
                })
            })
                as std::pin::Pin<Box<dyn std::future::Future<Output = OrcherResult<()>> + Send>>
        };

        // Create default no-op executor for timer tasks
        let timer_executor = |task: crate::engine::queues::TimerTask| {
            Box::pin(async move {
                warn!(
                    task_id = %task.task_id,
                    workflow_id = %task.workflow_id,
                    "TimerTask received but no custom executor configured - timer will be re-queued"
                );
                // Return error so timer gets re-queued
                Err(orcher_common::error::OrcherError::Configuration {
                    key: "timer_task_executor".to_string(),
                    message: "No custom timer executor configured".to_string(),
                })
            })
                as std::pin::Pin<Box<dyn std::future::Future<Output = OrcherResult<()>> + Send>>
        };

        // Start outbox processor
        let outbox_handle = {
            let processor = self.outbox_processor.clone();
            tokio::spawn(async move {
                if let Err(e) = processor.start(outbox_executor).await {
                    error!("Outbox processor failed to start: {}", e);
                }
            })
        };
        *self.outbox_handle.write().await = Some(outbox_handle);
        info!("Outbox processor started with default executor");

        // Start timer processor
        let timer_handle = {
            let processor = self.timer_processor.clone();
            tokio::spawn(async move {
                if let Err(e) = processor.start(timer_executor).await {
                    error!("Timer processor failed to start: {}", e);
                }
            })
        };
        *self.timer_handle.write().await = Some(timer_handle);
        info!("Timer processor started with default executor");

        info!("Background processors started successfully");
        info!("Using default no-op executors - configure custom executors for production use");

        Ok(())
    }

    /// Get system health status
    pub async fn health(&self) -> SystemHealth {
        let outbox_state = if self.outbox_handle.read().await.is_some() {
            ProcessorState::Running
        } else {
            ProcessorState::Stopped
        };

        let timer_state = if self.timer_handle.read().await.is_some() {
            ProcessorState::Running
        } else {
            ProcessorState::Stopped
        };

        let schedule_state = if self.schedule_handle.read().await.is_some() {
            ProcessorState::Running
        } else {
            ProcessorState::Stopped
        };

        let coordinator_healthy = sqlx::query("SELECT 1").execute(&*self.pool).await.is_ok();
        let healthy = coordinator_healthy
            && (outbox_state == ProcessorState::Running || outbox_state == ProcessorState::Stopped)
            && (timer_state == ProcessorState::Running || timer_state == ProcessorState::Stopped)
            && (schedule_state == ProcessorState::Running
                || schedule_state == ProcessorState::Stopped);

        SystemHealth {
            coordinator_healthy,
            outbox_state,
            timer_state,
            schedule_state,
            healthy,
            checked_at: Timestamptz::now(),
        }
    }

    /// Check if the system is operational
    pub async fn is_operational(&self) -> bool {
        self.health().await.is_operational()
    }

    /// Start the outbox processor with a custom executor and track the handle
    ///
    /// This method starts the outbox processor with a custom task executor function
    /// and properly registers the task handle so that health checks report the processor
    /// as running.
    ///
    /// # Arguments
    ///
    /// * `executor` - A function that executes outbox tasks
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use orcher_core::coordination::queues::OutboxTask;
    ///
    /// let executor = |task: OutboxTask| {
    ///     Box::pin(async move {
    ///         // Execute the task...
    ///         Ok(())
    ///     })
    /// };
    ///
    /// system.start_outbox_processor_with_executor(executor).await?;
    /// ```
    pub async fn start_outbox_processor_with_executor<F, Fut>(
        &self,
        executor: F,
    ) -> OrcherResult<()>
    where
        F: Fn(crate::engine::queues::OutboxTask) -> Fut + Send + Sync + Clone + 'static,
        Fut: std::future::Future<Output = OrcherResult<()>> + Send + 'static,
    {
        info!("Starting OutboxProcessor with custom executor");

        let processor = self.outbox_processor.clone();

        // Wrap the executor to match the expected signature
        let primary_executor = executor.clone();
        let wrapped_executor = move |task: crate::engine::queues::OutboxTask| {
            let fut = primary_executor(task);
            Box::pin(fut)
                as std::pin::Pin<Box<dyn std::future::Future<Output = OrcherResult<()>> + Send>>
        };

        let outbox_handle = {
            tokio::spawn(async move {
                if let Err(e) = processor.start(wrapped_executor).await {
                    error!("Outbox processor failed: {}", e);
                }
            })
        };

        *self.outbox_handle.write().await = Some(outbox_handle);

        // Start one processor per shard database. Each claims from its own outbox
        // (co-located with that shard's workflow state) using a clone of the executor.
        for (idx, shard_processor) in self.shard_outbox_processors.iter().cloned().enumerate() {
            let shard_executor = executor.clone();
            let wrapped = move |task: crate::engine::queues::OutboxTask| {
                let fut = shard_executor(task);
                Box::pin(fut)
                    as std::pin::Pin<Box<dyn std::future::Future<Output = OrcherResult<()>> + Send>>
            };
            let handle = tokio::spawn(async move {
                if let Err(e) = shard_processor.start(wrapped).await {
                    error!("Shard outbox processor {} failed: {}", idx, e);
                }
            });
            self.shard_handles.write().await.push(handle);
        }

        info!("OutboxProcessor started and handle registered for health tracking");

        Ok(())
    }

    /// Start the timer processor with a custom executor and track the handle
    ///
    /// This method starts the timer processor with a custom timer executor function
    /// and properly registers the task handle so that health checks report the processor
    /// as running.
    ///
    /// # Arguments
    ///
    /// * `executor` - A function that executes timer tasks
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use orcher_core::coordination::queues::TimerTask;
    ///
    /// let executor = |task: TimerTask| {
    ///     Box::pin(async move {
    ///         // Execute the timer task...
    ///         Ok(())
    ///     })
    /// };
    ///
    /// system.start_timer_processor_with_executor(executor).await?;
    /// ```
    pub async fn start_timer_processor_with_executor<F, Fut>(&self, executor: F) -> OrcherResult<()>
    where
        F: Fn(crate::engine::queues::TimerTask) -> Fut + Send + Sync + Clone + 'static,
        Fut: std::future::Future<Output = OrcherResult<()>> + Send + 'static,
    {
        info!("Starting TimerProcessor with custom executor");

        let processor = self.timer_processor.clone();

        // Wrap the executor to match the expected signature
        let primary_executor = executor.clone();
        let wrapped_executor = move |task: crate::engine::queues::TimerTask| {
            let fut = primary_executor(task);
            Box::pin(fut)
                as std::pin::Pin<Box<dyn std::future::Future<Output = OrcherResult<()>> + Send>>
        };

        let timer_handle = {
            tokio::spawn(async move {
                if let Err(e) = processor.start(wrapped_executor).await {
                    error!("Timer processor failed: {}", e);
                }
            })
        };

        *self.timer_handle.write().await = Some(timer_handle);

        // Start one processor per shard database. Each claims from its own timer queue
        // (co-located with that shard's workflow state) using a clone of the executor.
        for (idx, shard_processor) in self.shard_timer_processors.iter().cloned().enumerate() {
            let shard_executor = executor.clone();
            let wrapped = move |task: crate::engine::queues::TimerTask| {
                let fut = shard_executor(task);
                Box::pin(fut)
                    as std::pin::Pin<Box<dyn std::future::Future<Output = OrcherResult<()>> + Send>>
            };
            let handle = tokio::spawn(async move {
                if let Err(e) = shard_processor.start(wrapped).await {
                    error!("Shard timer processor {} failed: {}", idx, e);
                }
            });
            self.shard_handles.write().await.push(handle);
        }

        info!("TimerProcessor started and handle registered for health tracking");

        Ok(())
    }

    /// Start the schedule processor with a custom executor and track the handle
    pub async fn start_schedule_processor_with_executor<F>(&self, executor: F) -> OrcherResult<()>
    where
        F: Fn(
                orcher_storage::schema::types::Schedule,
            ) -> std::pin::Pin<
                Box<dyn std::future::Future<Output = OrcherResult<uuid::Uuid>> + Send>,
            > + Send
            + Sync
            + 'static,
    {
        info!("Starting ScheduleProcessor with custom executor");

        let processor = self.schedule_processor.clone();

        let schedule_handle = tokio::spawn(async move {
            if let Err(e) = processor.start(executor).await {
                error!("Schedule processor failed: {}", e);
            }
        });

        *self.schedule_handle.write().await = Some(schedule_handle);
        info!("ScheduleProcessor started and handle registered for health tracking");

        Ok(())
    }

    /// Mark the outbox processor as running (for externally-started processors)
    ///
    /// Use this when you start the processor manually via `outbox_processor_arc().start()`
    /// and want the health check to report it as running.
    pub async fn set_outbox_running(&self, handle: JoinHandle<()>) {
        *self.outbox_handle.write().await = Some(handle);
        info!("OutboxProcessor handle registered for health tracking");
    }

    /// Mark the timer processor as running (for externally-started processors)
    ///
    /// Use this when you start the processor manually via `timer_processor_arc().start()`
    /// and want the health check to report it as running.
    pub async fn set_timer_running(&self, handle: JoinHandle<()>) {
        *self.timer_handle.write().await = Some(handle);
        info!("TimerProcessor handle registered for health tracking");
    }

    /// Mark the schedule processor as running (for externally-started processors)
    pub async fn set_schedule_running(&self, handle: JoinHandle<()>) {
        *self.schedule_handle.write().await = Some(handle);
        info!("ScheduleProcessor handle registered for health tracking");
    }

    /// Gracefully shutdown the system
    ///
    /// This will:
    /// 1. Stop accepting new work
    /// 2. Wait for in-flight operations to complete (up to timeout)
    /// 3. Stop background processors
    /// 4. Close database connections
    pub async fn shutdown(self) -> OrcherResult<()> {
        info!("Initiating graceful shutdown");

        // Send shutdown signal to processors
        if let Some(tx) = self.shutdown_tx.write().await.take() {
            let _ = tx.send(());
        }

        // Wait for processors to finish with timeout
        let timeout = tokio::time::Duration::from_secs(self.config.shutdown_timeout_secs);

        // Wait for outbox processor
        if let Some(handle) = self.outbox_handle.write().await.take() {
            match tokio::time::timeout(timeout, handle).await {
                Ok(Ok(())) => {
                    info!("Outbox processor stopped gracefully");
                }
                Ok(Err(e)) => {
                    warn!(error = ?e, "Outbox processor task panicked");
                }
                Err(_) => {
                    warn!("Outbox processor shutdown timed out");
                }
            }
        }

        // Wait for timer processor
        if let Some(handle) = self.timer_handle.write().await.take() {
            match tokio::time::timeout(timeout, handle).await {
                Ok(Ok(())) => {
                    info!("Timer processor stopped gracefully");
                }
                Ok(Err(e)) => {
                    warn!(error = ?e, "Timer processor task panicked");
                }
                Err(_) => {
                    warn!("Timer processor shutdown timed out");
                }
            }
        }

        // Wait for schedule processor
        if let Some(handle) = self.schedule_handle.write().await.take() {
            self.schedule_processor.shutdown().await;
            match tokio::time::timeout(timeout, handle).await {
                Ok(Ok(())) => {
                    info!("Schedule processor stopped gracefully");
                }
                Ok(Err(e)) => {
                    warn!(error = ?e, "Schedule processor task panicked");
                }
                Err(_) => {
                    warn!("Schedule processor shutdown timed out");
                }
            }
        }

        // Signal the per-shard processing loops to stop, then wait for their spawn handles
        for p in &self.shard_outbox_processors {
            p.shutdown().await;
        }
        for p in &self.shard_timer_processors {
            p.shutdown().await;
        }
        let shard_handles: Vec<_> = self.shard_handles.write().await.drain(..).collect();
        for (idx, handle) in shard_handles.into_iter().enumerate() {
            match tokio::time::timeout(timeout, handle).await {
                Ok(Ok(())) => {
                    info!("Shard processor {} stopped gracefully", idx);
                }
                Ok(Err(e)) => {
                    warn!(error = ?e, "Shard processor {} task panicked", idx);
                }
                Err(_) => {
                    warn!("Shard processor {} shutdown timed out", idx);
                }
            }
        }

        // Close database pool
        self.pool.close().await;

        info!("OrchestrationSystem shutdown complete");

        Ok(())
    }

    /// Get a summary of the system state
    pub async fn summary(&self) -> SystemSummary {
        let health = self.health().await;
        let coordinator_metrics = self.coordinator_metrics().await;
        let outbox_metrics = self.outbox_metrics().await;
        let timer_metrics = self.timer_metrics().await;
        let schedule_metrics = self.schedule_metrics().await;

        SystemSummary {
            instance_id: self.config.coordinator.instance_id.clone(),
            healthy: health.healthy,
            operational: health.is_operational(),
            workflows_started: coordinator_metrics.workflows_started,
            workflows_completed: coordinator_metrics.workflows_completed,
            workflows_failed: coordinator_metrics.workflows_failed,
            tasks_dispatched: coordinator_metrics.tasks_dispatched,
            timers_scheduled: coordinator_metrics.timers_scheduled,
            cache_hit_rate: if coordinator_metrics.cache_hits + coordinator_metrics.cache_misses > 0
            {
                coordinator_metrics.cache_hits as f64
                    / (coordinator_metrics.cache_hits + coordinator_metrics.cache_misses) as f64
            } else {
                0.0
            },
            outbox_processed: outbox_metrics.tasks_succeeded,
            outbox_failed: outbox_metrics.tasks_failed,
            timers_fired: timer_metrics.timers_fired,
            timers_failed: timer_metrics.timers_failed,
            schedules_fired: schedule_metrics.schedules_fired,
            schedules_skipped: schedule_metrics.schedules_skipped,
            schedules_failed: schedule_metrics.schedules_failed,
        }
    }
}

// ============================================================================
// System Summary
// ============================================================================

/// Summary of system metrics and state
#[derive(Debug, Clone)]
pub struct SystemSummary {
    /// System instance ID
    pub instance_id: String,

    /// System is healthy
    pub healthy: bool,

    /// System is operational
    pub operational: bool,

    /// Total workflows started
    pub workflows_started: u64,

    /// Total workflows completed
    pub workflows_completed: u64,

    /// Total workflows failed
    pub workflows_failed: u64,

    /// Total tasks dispatched
    pub tasks_dispatched: u64,

    /// Total timers scheduled
    pub timers_scheduled: u64,

    /// Cache hit rate (0.0 to 1.0)
    pub cache_hit_rate: f64,

    /// Outbox tasks processed
    pub outbox_processed: u64,

    /// Outbox tasks failed
    pub outbox_failed: u64,

    /// Timers fired
    pub timers_fired: u64,

    /// Timers failed
    pub timers_failed: u64,

    /// Schedules fired
    pub schedules_fired: u64,

    /// Schedules skipped (overlap policy)
    pub schedules_skipped: u64,

    /// Schedules failed
    pub schedules_failed: u64,
}

impl std::fmt::Display for SystemSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "OrchestrationSystem Summary")?;
        writeln!(f, "  Instance ID: {}", self.instance_id)?;
        writeln!(
            f,
            "  Status: {}",
            if self.operational {
                "Operational"
            } else {
                "Not Operational"
            }
        )?;
        writeln!(f, "  Healthy: {}", if self.healthy { "Yes" } else { "No" })?;
        writeln!(f)?;
        writeln!(f, "Workflows:")?;
        writeln!(f, "  Started: {}", self.workflows_started)?;
        writeln!(f, "  Completed: {}", self.workflows_completed)?;
        writeln!(f, "  Failed: {}", self.workflows_failed)?;
        writeln!(f)?;
        writeln!(f, "Tasks & Timers:")?;
        writeln!(f, "  Tasks dispatched: {}", self.tasks_dispatched)?;
        writeln!(f, "  Timers scheduled: {}", self.timers_scheduled)?;
        writeln!(f, "  Timers fired: {}", self.timers_fired)?;
        writeln!(f)?;
        writeln!(f, "Schedules:")?;
        writeln!(f, "  Fired: {}", self.schedules_fired)?;
        writeln!(f, "  Skipped: {}", self.schedules_skipped)?;
        writeln!(f, "  Failed: {}", self.schedules_failed)?;
        writeln!(f)?;
        writeln!(f, "Performance:")?;
        writeln!(f, "  Cache hit rate: {:.1}%", self.cache_hit_rate * 100.0)?;
        writeln!(f, "  Outbox processed: {}", self.outbox_processed)?;
        writeln!(f, "  Outbox failed: {}", self.outbox_failed)?;
        writeln!(f, "  Timers failed: {}", self.timers_failed)?;

        Ok(())
    }
}

// ============================================================================
// Builder Pattern (Optional)
// ============================================================================

/// Builder for OrchestrationSystem
pub struct OrchestrationSystemBuilder {
    pool: Arc<PgPool>,
    config: OrchestrationConfig,
}

impl OrchestrationSystemBuilder {
    /// Create a new builder
    pub fn new(pool: Arc<PgPool>) -> Self {
        Self {
            pool,
            config: OrchestrationConfig::default(),
        }
    }

    /// Set the coordinator configuration
    pub fn coordinator_config(mut self, config: CoordinatorConfig) -> Self {
        self.config.coordinator = config;
        self
    }

    /// Set the outbox processor configuration
    pub fn outbox_processor_config(mut self, config: OutboxProcessorConfig) -> Self {
        self.config.outbox_processor = config;
        self
    }

    /// Set the timer processor configuration
    pub fn timer_processor_config(mut self, config: TimerProcessorConfig) -> Self {
        self.config.timer_processor = config;
        self
    }

    /// Set the schedule processor configuration
    pub fn schedule_processor_config(mut self, config: ScheduleProcessorConfig) -> Self {
        self.config.schedule_processor = config;
        self
    }

    /// Enable or disable background processors
    pub fn enable_processors(mut self, enable: bool) -> Self {
        self.config.enable_processors = enable;
        self
    }

    /// Set the shutdown timeout
    pub fn shutdown_timeout_secs(mut self, seconds: u64) -> Self {
        self.config.shutdown_timeout_secs = seconds;
        self
    }

    /// Build the orchestration system
    pub async fn build(self) -> OrcherResult<OrchestrationSystem> {
        OrchestrationSystem::new(self.pool, self.config).await
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_orchestration_config_default() {
        let config = OrchestrationConfig::default();
        // Processors are disabled by default — they need custom executors
        assert!(!config.enable_processors);
        assert_eq!(config.shutdown_timeout_secs, 30);
    }

    #[test]
    fn test_processor_state() {
        assert_eq!(ProcessorState::Running, ProcessorState::Running);
        assert_ne!(ProcessorState::Running, ProcessorState::Stopped);
    }

    #[tokio::test]
    #[ignore = "requires PostgreSQL"]
    async fn test_system_initialization() {
        // TODO: Implement with test database
    }

    #[tokio::test]
    #[ignore = "requires PostgreSQL"]
    async fn test_system_health_check() {
        // TODO: Implement with test database
    }
}
