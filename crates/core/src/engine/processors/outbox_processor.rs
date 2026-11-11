//! Outbox queue processor for reliable task dispatch.

use crate::engine::queues::{OutboxQueue, OutboxTask};
use orcher_common::error::OrcherResult;
use orcher_storage::repositories::domains::coordination::CoordinationRepository;
use orcher_storage::traits::domain::CoordinationRepositoryTrait;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Notify};
use tokio::time::{interval, sleep};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// Outbox processor configuration
#[derive(Debug, Clone)]
pub struct OutboxProcessorConfig {
    /// Number of tasks to claim per batch
    pub batch_size: usize,

    /// Interval between processing polls
    pub polling_interval: Duration,

    /// Interval between cleanup runs
    pub cleanup_interval: Duration,

    /// Threshold for stuck task detection (seconds)
    pub stuck_threshold_secs: i64,

    /// Maximum concurrent workflows being processed
    pub max_concurrent_workflows: usize,

    /// Partition ID for this processor (optional)
    /// When set, only processes tasks for this partition
    pub partition_id: Option<u32>,

    /// Total number of partitions (for partition-aware claiming)
    pub total_partitions: u32,

    /// Also claim `dispatch_task` rows (the matching layer's transfer emitter). Off by
    /// default: `dispatch_task` is left for the poller (`TaskPollingManager`). On when
    /// the matching layer is enabled, so this processor transfers those rows into their
    /// matching partition via the executor.
    pub claim_dispatch_task: bool,
}

impl Default for OutboxProcessorConfig {
    fn default() -> Self {
        Self {
            batch_size: 100,
            polling_interval: Duration::from_millis(100), // 100ms for low-latency dispatch
            cleanup_interval: Duration::from_secs(60),
            stuck_threshold_secs: 300, // 5 minutes
            max_concurrent_workflows: 1000,
            partition_id: None,
            total_partitions: 1,
            claim_dispatch_task: false,
        }
    }
}

impl OutboxProcessorConfig {
    /// Create a partitioned configuration
    pub fn for_partition(partition_id: u32, total_partitions: u32) -> Self {
        Self {
            partition_id: Some(partition_id),
            total_partitions,
            ..Default::default()
        }
    }

    /// Set batch size
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set polling interval
    pub fn with_polling_interval(mut self, interval: Duration) -> Self {
        self.polling_interval = interval;
        self
    }
}

/// Metrics for the outbox processor
#[derive(Debug, Clone, Default)]
pub struct ProcessorMetrics {
    /// Total tasks processed successfully
    pub tasks_succeeded: u64,

    /// Total tasks failed
    pub tasks_failed: u64,

    /// Total tasks moved to DLQ
    pub tasks_dlq: u64,

    /// Total stuck tasks recovered
    pub stuck_recovered: u64,

    /// Current workflows being processed
    pub active_workflows: usize,
}

/// Outbox processor for background task execution
pub struct OutboxProcessor {
    /// Outbox queue
    outbox: OutboxQueue,

    /// Coordination repository (for DLQ operations)
    coordination_repo: Arc<CoordinationRepository>,

    /// Configuration
    config: OutboxProcessorConfig,

    /// Per-workflow locks to ensure ordering
    workflow_locks: Arc<Mutex<HashMap<Uuid, Arc<Mutex<()>>>>>,

    /// Currently processing workflows
    active_workflows: Arc<Mutex<HashSet<Uuid>>>,

    /// Processor metrics
    metrics: Arc<Mutex<ProcessorMetrics>>,

    /// Shutdown signal
    shutdown: Arc<Mutex<bool>>,

    /// Immediate wake-up signal. Called when new tasks are inserted into the outbox,
    /// so the processor doesn't have to wait for the next poll interval.
    wake: Arc<Notify>,
}

impl OutboxProcessor {
    /// Create a new outbox processor
    pub fn new(
        outbox: OutboxQueue,
        coordination_repo: Arc<CoordinationRepository>,
        config: OutboxProcessorConfig,
    ) -> Self {
        // Share the wake signal from the outbox queue so that inserts
        // can wake the processor immediately without poll-interval delay.
        let wake = outbox.wake_signal();
        Self {
            outbox,
            coordination_repo,
            config,
            workflow_locks: Arc::new(Mutex::new(HashMap::new())),
            active_workflows: Arc::new(Mutex::new(HashSet::new())),
            metrics: Arc::new(Mutex::new(ProcessorMetrics::default())),
            shutdown: Arc::new(Mutex::new(false)),
            wake,
        }
    }

    /// Create with default configuration
    pub fn with_defaults(
        outbox: OutboxQueue,
        coordination_repo: Arc<CoordinationRepository>,
    ) -> Self {
        Self::new(outbox, coordination_repo, OutboxProcessorConfig::default())
    }

    /// Start the processor (spawns background tasks)
    pub async fn start<F>(self: Arc<Self>, executor: F) -> OrcherResult<()>
    where
        F: Fn(
                OutboxTask,
            )
                -> std::pin::Pin<Box<dyn std::future::Future<Output = OrcherResult<()>> + Send>>
            + Send
            + Sync
            + 'static,
    {
        let executor = Arc::new(executor);

        info!("Starting outbox processor");

        // Spawn processing loop
        let processor = self.clone();
        let exec = executor.clone();
        tokio::spawn(async move {
            processor.run_processing_loop(exec).await;
        });

        // Spawn cleanup loop
        let processor = self.clone();
        tokio::spawn(async move {
            processor.run_cleanup_loop().await;
        });

        Ok(())
    }

    /// Signal shutdown
    pub async fn shutdown(&self) {
        info!("Shutting down outbox processor");
        *self.shutdown.lock().await = true;
        self.wake.notify_waiters();
    }

    /// Wake the processor immediately to process newly inserted outbox tasks.
    /// Call this after inserting into the outbox queue to eliminate poll-interval latency.
    pub fn notify_new_tasks(&self) {
        self.wake.notify_one();
    }

    /// Get current metrics
    pub async fn get_metrics(&self) -> ProcessorMetrics {
        self.metrics.lock().await.clone()
    }

    /// Main processing loop
    async fn run_processing_loop<F>(self: Arc<Self>, executor: Arc<F>)
    where
        F: Fn(
                OutboxTask,
            )
                -> std::pin::Pin<Box<dyn std::future::Future<Output = OrcherResult<()>> + Send>>
            + Send
            + Sync
            + 'static,
    {
        let mut poll_interval = interval(self.config.polling_interval);

        loop {
            // Wait for either the poll interval OR an immediate wake-up signal.
            // This eliminates the 0-100ms latency when new tasks are inserted —
            // the caller notifies us via wake.notify_waiters().
            //
            // After a wake-up, we yield briefly (1ms) to let additional tasks accumulate
            // before claiming, so we still benefit from batch processing.
            tokio::select! {
                _ = poll_interval.tick() => {}
                _ = self.wake.notified() => {
                    // Brief yield to coalesce rapid successive inserts into a single batch
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    poll_interval.reset();
                }
            }

            if *self.shutdown.lock().await {
                info!("Processing loop received shutdown signal");
                break;
            }

            // Check if we're at max capacity
            let active_count = self.active_workflows.lock().await.len();
            if active_count >= self.config.max_concurrent_workflows {
                debug!(
                    active_workflows = active_count,
                    max_concurrent = self.config.max_concurrent_workflows,
                    "At max capacity, waiting"
                );
                continue;
            }

            // Claim tasks from the queue. By default `dispatch_task` is excluded — the
            // poller (TaskPollingManager) claims those, so the two never compete. With
            // the matching layer on, this processor instead claims `dispatch_task` too
            // and its executor transfers each into its matching partition (the poller
            // then claims from the partition, so there is still no competition).
            // TODO: Pass organization_id from context when multi-tenant mode is enabled
            let excluded: &[&str] = if self.config.claim_dispatch_task {
                &[]
            } else {
                &["dispatch_task"]
            };
            let tasks = match self
                .outbox
                .claim_tasks_excluding_types(None, self.config.batch_size, excluded)
                .await
            {
                Ok(tasks) => tasks,
                Err(e) => {
                    error!("Failed to claim tasks: {}", e);
                    sleep(Duration::from_secs(1)).await;
                    continue;
                }
            };

            if tasks.is_empty() {
                continue;
            }

            // Filter tasks by partition if partitioning is enabled
            let tasks = self.filter_by_partition(tasks);

            if tasks.is_empty() {
                continue;
            }

            // Group tasks by workflow, each workflow's oldest first. A claim
            // takes the oldest ready rows whatever their workflow, so one
            // workflow can have several here; the statement returns them in
            // no particular order. Handling them one after another is not an
            // ordering their types need (see
            // `OutboxRepository::claim_tasks_excluding_types`), only the
            // order they were written in.
            let mut workflow_tasks: HashMap<Uuid, Vec<OutboxTask>> = HashMap::new();
            for task in tasks {
                workflow_tasks
                    .entry(task.workflow_id)
                    .or_default()
                    .push(task);
            }
            for tasks in workflow_tasks.values_mut() {
                tasks.sort_by_key(|t| t.created_at);
            }

            // Process each workflow's tasks
            for (workflow_id, tasks) in workflow_tasks {
                let processor = self.clone();
                let exec = executor.clone();

                tokio::spawn(async move {
                    processor
                        .process_workflow_tasks(workflow_id, tasks, exec)
                        .await;
                });
            }
        }
    }

    /// Process all tasks for a specific workflow (maintains ordering)
    async fn process_workflow_tasks<F>(
        self: Arc<Self>,
        workflow_id: Uuid,
        tasks: Vec<OutboxTask>,
        executor: Arc<F>,
    ) where
        F: Fn(
                OutboxTask,
            )
                -> std::pin::Pin<Box<dyn std::future::Future<Output = OrcherResult<()>> + Send>>
            + Send
            + Sync
            + 'static,
    {
        // Get or create workflow lock
        let workflow_lock = {
            let mut locks = self.workflow_locks.lock().await;
            locks
                .entry(workflow_id)
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };

        // Mark workflow as active
        self.active_workflows.lock().await.insert(workflow_id);

        // Acquire workflow lock to ensure ordering
        let _guard = workflow_lock.lock().await;

        debug!(
            workflow_id = %workflow_id,
            task_count = tasks.len(),
            "Processing workflow tasks"
        );

        // Process tasks in order
        for task in tasks {
            if *self.shutdown.lock().await {
                break;
            }

            if let Err(e) = self.execute_task(&task, executor.clone()).await {
                error!(
                    task_id = %task.task_id,
                    workflow_id = %workflow_id,
                    error = %e,
                    "Task execution failed"
                );
            }
        }

        // Remove from active workflows
        self.active_workflows.lock().await.remove(&workflow_id);

        // Clean up lock if no more tasks pending
        if let Ok(pending_count) = self
            .outbox
            .get_pending_count_for_workflow(workflow_id)
            .await
        {
            if pending_count == 0 {
                self.workflow_locks.lock().await.remove(&workflow_id);
            }
        }
    }

    /// Execute a single task
    async fn execute_task<F>(&self, task: &OutboxTask, executor: Arc<F>) -> OrcherResult<()>
    where
        F: Fn(
                OutboxTask,
            )
                -> std::pin::Pin<Box<dyn std::future::Future<Output = OrcherResult<()>> + Send>>
            + Send
            + Sync
            + 'static,
    {
        debug!(
            task_id = %task.task_id,
            task_type = %task.task_type,
            attempt = task.attempt_count,
            "Executing outbox task"
        );

        // Execute the task using the provided executor
        let result = executor(task.clone()).await;

        match result {
            Ok(()) => {
                // Mark as completed
                if let Err(e) = self.outbox.mark_completed(task.task_id).await {
                    error!(
                        task_id = %task.task_id,
                        error = %e,
                        "Failed to mark task as completed"
                    );
                    return Err(e);
                }

                // Update metrics
                let mut metrics = self.metrics.lock().await;
                metrics.tasks_succeeded += 1;

                debug!(task_id = %task.task_id, "Task completed successfully");
                Ok(())
            }
            Err(e) => {
                // Mark as failed (handles retry logic)
                let error_msg = e.to_string();
                let error_details = serde_json::json!({
                    "error": error_msg,
                    "attempt": task.attempt_count,
                });

                match self
                    .outbox
                    .mark_failed(task.task_id, error_msg.clone(), Some(error_details.clone()))
                    .await
                {
                    Ok(should_move_to_dlq) => {
                        if should_move_to_dlq {
                            // Move to DLQ after exhausting retries using coordination repository
                            let dlq_error_details = serde_json::json!({
                                "error": error_msg,
                                "error_details": error_details,
                                "attempt_count": task.attempt_count,
                                "max_retries": task.max_retries,
                            });

                            match self
                                .coordination_repo
                                .move_outbox_task_to_dlq(task, dlq_error_details)
                                .await
                            {
                                Ok(dlq_task) => {
                                    info!(
                                        task_id = %task.task_id,
                                        dlq_id = %dlq_task.dlq_id,
                                        "Moved task to DLQ after exhausting retries"
                                    );
                                    let mut metrics = self.metrics.lock().await;
                                    metrics.tasks_dlq += 1;
                                }
                                Err(dlq_err) => {
                                    error!(
                                        task_id = %task.task_id,
                                        error = %dlq_err,
                                        "Failed to move task to DLQ"
                                    );
                                }
                            }
                        }

                        let mut metrics = self.metrics.lock().await;
                        metrics.tasks_failed += 1;

                        Err(e)
                    }
                    Err(mark_err) => {
                        error!(
                            task_id = %task.task_id,
                            error = %mark_err,
                            "Failed to mark task as failed"
                        );
                        Err(mark_err)
                    }
                }
            }
        }
    }

    /// Cleanup loop for stuck tasks
    async fn run_cleanup_loop(self: Arc<Self>) {
        let mut cleanup_interval = interval(self.config.cleanup_interval);

        loop {
            cleanup_interval.tick().await;

            if *self.shutdown.lock().await {
                info!("Cleanup loop received shutdown signal");
                break;
            }

            match self
                .outbox
                .cleanup_stuck_tasks(self.config.stuck_threshold_secs)
                .await
            {
                Ok(recovered_count) => {
                    if recovered_count > 0 {
                        let mut metrics = self.metrics.lock().await;
                        metrics.stuck_recovered += recovered_count as u64;

                        warn!(recovered = recovered_count, "Recovered stuck tasks");
                    }
                }
                Err(e) => {
                    error!("Failed to cleanup stuck tasks: {}", e);
                }
            }
        }
    }

    /// Filter tasks by partition based on workflow_id hash
    ///
    /// Used when partition_id is set to only process tasks belonging to this partition
    fn filter_by_partition(&self, tasks: Vec<OutboxTask>) -> Vec<OutboxTask> {
        match self.config.partition_id {
            Some(partition_id) => tasks
                .into_iter()
                .filter(|task| {
                    let hash = Self::partition_hash(&task.workflow_id);
                    hash % self.config.total_partitions == partition_id
                })
                .collect(),
            None => tasks, // No partitioning, return all
        }
    }

    /// Calculate partition hash from workflow_id
    #[inline]
    fn partition_hash(workflow_id: &Uuid) -> u32 {
        // Use simple XOR-based hash of UUID bytes
        let bytes = workflow_id.as_bytes();
        let mut hash: u32 = 0;
        for chunk in bytes.chunks(4) {
            let mut val: u32 = 0;
            for (i, &b) in chunk.iter().enumerate() {
                val |= (b as u32) << (i * 8);
            }
            hash ^= val;
        }
        hash
    }

    /// Get the partition ID for a given workflow
    pub fn get_partition_for_workflow(workflow_id: &Uuid, total_partitions: u32) -> u32 {
        Self::partition_hash(workflow_id) % total_partitions
    }
}

// ============================================================================
// Partitioned Outbox Processor
// ============================================================================

/// Configuration for partitioned outbox processing
#[derive(Debug, Clone)]
pub struct PartitionedOutboxConfig {
    /// Number of partitions to create
    pub num_partitions: u32,

    /// Base configuration for each partition processor
    pub base_config: OutboxProcessorConfig,
}

impl Default for PartitionedOutboxConfig {
    fn default() -> Self {
        Self {
            num_partitions: 4,
            base_config: OutboxProcessorConfig::default(),
        }
    }
}

impl PartitionedOutboxConfig {
    /// Create a new partitioned config
    pub fn new(num_partitions: u32) -> Self {
        Self {
            num_partitions,
            ..Default::default()
        }
    }

    /// Set the base batch size (will be divided by partitions)
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.base_config.batch_size = batch_size;
        self
    }

    /// Set the polling interval
    pub fn with_polling_interval(mut self, interval: Duration) -> Self {
        self.base_config.polling_interval = interval;
        self
    }
}

/// Aggregated metrics across all partitions
#[derive(Debug, Clone, Default)]
pub struct PartitionedProcessorMetrics {
    /// Metrics per partition
    pub partition_metrics: Vec<ProcessorMetrics>,
    /// Total tasks succeeded across all partitions
    pub total_succeeded: u64,
    /// Total tasks failed across all partitions
    pub total_failed: u64,
    /// Total active workflows across all partitions
    pub total_active_workflows: usize,
}

/// Partitioned outbox processor that runs multiple partition processors
///
/// This allows parallel processing of outbox tasks while maintaining
/// per-workflow ordering within each partition. Workflows are assigned
/// to partitions based on a hash of their workflow_id.
///
/// ## Benefits
///
/// - **Horizontal scalability**: Run N processors in parallel
/// - **Reduced contention**: Each processor only competes for its partition
/// - **Maintained ordering**: Workflows always go to the same partition
/// - **Load distribution**: Hash-based distribution spreads load evenly
///
/// ## Usage
///
/// ```rust,ignore
/// let config = PartitionedOutboxConfig::new(4); // 4 partitions
/// let processor = PartitionedOutboxProcessor::new(
///     outbox_queue,
///     coordination_repo,
///     config,
/// );
///
/// processor.start_all(executor).await?;
/// ```
pub struct PartitionedOutboxProcessor {
    /// Individual partition processors
    partitions: Vec<Arc<OutboxProcessor>>,

    /// Configuration
    config: PartitionedOutboxConfig,
}

impl PartitionedOutboxProcessor {
    /// Create a new partitioned processor
    pub fn new(
        outbox: OutboxQueue,
        coordination_repo: Arc<CoordinationRepository>,
        config: PartitionedOutboxConfig,
    ) -> Self {
        let mut partitions = Vec::with_capacity(config.num_partitions as usize);

        for partition_id in 0..config.num_partitions {
            let partition_config = OutboxProcessorConfig {
                partition_id: Some(partition_id),
                total_partitions: config.num_partitions,
                batch_size: config.base_config.batch_size / config.num_partitions as usize,
                polling_interval: config.base_config.polling_interval,
                cleanup_interval: config.base_config.cleanup_interval,
                stuck_threshold_secs: config.base_config.stuck_threshold_secs,
                max_concurrent_workflows: config.base_config.max_concurrent_workflows
                    / config.num_partitions as usize,
                claim_dispatch_task: config.base_config.claim_dispatch_task,
            };

            let processor = Arc::new(OutboxProcessor::new(
                outbox.clone(),
                coordination_repo.clone(),
                partition_config,
            ));

            partitions.push(processor);
        }

        info!(
            num_partitions = config.num_partitions,
            batch_per_partition = config.base_config.batch_size / config.num_partitions as usize,
            "Created partitioned outbox processor"
        );

        Self { partitions, config }
    }

    /// Start all partition processors
    pub async fn start_all<F>(&self, executor: F) -> OrcherResult<()>
    where
        F: Fn(
                OutboxTask,
            )
                -> std::pin::Pin<Box<dyn std::future::Future<Output = OrcherResult<()>> + Send>>
            + Send
            + Sync
            + Clone
            + 'static,
    {
        info!(
            num_partitions = self.config.num_partitions,
            "Starting all partition processors"
        );

        for (partition_id, processor) in self.partitions.iter().enumerate() {
            let exec = executor.clone();
            processor.clone().start(exec).await?;

            debug!(partition_id = partition_id, "Started partition processor");
        }

        Ok(())
    }

    /// Shutdown all partition processors
    pub async fn shutdown_all(&self) {
        info!("Shutting down all partition processors");

        for processor in &self.partitions {
            processor.shutdown().await;
        }
    }

    /// Get aggregated metrics from all partitions
    pub async fn get_metrics(&self) -> PartitionedProcessorMetrics {
        let mut result = PartitionedProcessorMetrics::default();

        for processor in &self.partitions {
            let metrics = processor.get_metrics().await;
            result.total_succeeded += metrics.tasks_succeeded;
            result.total_failed += metrics.tasks_failed;
            result.total_active_workflows += metrics.active_workflows;
            result.partition_metrics.push(metrics);
        }

        result
    }

    /// Get the number of partitions
    pub fn num_partitions(&self) -> u32 {
        self.config.num_partitions
    }

    /// Get a specific partition processor
    pub fn get_partition(&self, partition_id: u32) -> Option<&Arc<OutboxProcessor>> {
        self.partitions.get(partition_id as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::PgPool;
    use std::sync::atomic::{AtomicUsize, Ordering};

    async fn create_test_pool() -> PgPool {
        todo!("Test pool creation")
    }

    #[tokio::test]
    #[ignore] // Requires database
    async fn test_processor_creation() {
        let pool = Arc::new(create_test_pool().await);
        let outbox_repo = Arc::new(
            orcher_storage::repositories::domains::coordination::OutboxRepository::new(
                pool.clone(),
            ),
        );
        let outbox = OutboxQueue::new(outbox_repo);
        let coordination_repo = Arc::new(CoordinationRepository::new(pool));
        let processor = OutboxProcessor::with_defaults(outbox, coordination_repo);

        let metrics = processor.get_metrics().await;
        assert_eq!(metrics.tasks_succeeded, 0);
        assert_eq!(metrics.tasks_failed, 0);
    }

    #[tokio::test]
    #[ignore] // Requires database
    async fn test_task_execution() {
        let pool = Arc::new(create_test_pool().await);
        let outbox_repo = Arc::new(
            orcher_storage::repositories::domains::coordination::OutboxRepository::new(
                pool.clone(),
            ),
        );
        let outbox = OutboxQueue::new(outbox_repo);
        let coordination_repo = Arc::new(CoordinationRepository::new(pool));
        let processor = Arc::new(OutboxProcessor::with_defaults(outbox, coordination_repo));

        let execution_count = Arc::new(AtomicUsize::new(0));
        let exec_count = execution_count.clone();

        // Simple executor that counts executions
        let executor = move |_task: OutboxTask| {
            let count = exec_count.clone();
            Box::pin(async move {
                count.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
                as std::pin::Pin<Box<dyn std::future::Future<Output = OrcherResult<()>> + Send>>
        };

        let processor_clone = Arc::clone(&processor);
        processor_clone.start(executor).await.unwrap();

        // Give it time to process
        sleep(Duration::from_millis(100)).await;

        processor.shutdown().await;
    }
}
