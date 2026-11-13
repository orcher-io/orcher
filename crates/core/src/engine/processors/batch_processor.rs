//! Background batch operation processor.
//!
//! Claims pending batch operations, iterates through matching workflows,
//! and executes the requested operation (terminate, cancel, send_event, reset)
//! at a configurable rate. Resumable via cursor-based pagination.

use orcher_common::error::OrcherResult;
use orcher_storage::repositories::domains::workflow_execution::{
    ListExecutionsFilters, WorkflowExecutionRepository,
};
use orcher_storage::schema::types::WorkflowExecution;
use sqlx::PgPool;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for the batch operation processor.
#[derive(Debug, Clone)]
pub struct BatchProcessorConfig {
    /// How often to poll for pending batch operations.
    pub poll_interval: Duration,
    /// Number of workflows to fetch per page.
    pub page_size: i64,
    /// Batch operations in 'processing' longer than this are considered stuck.
    pub stuck_threshold_secs: i64,
    /// How often to run the cleanup loop.
    pub cleanup_interval: Duration,
    /// Instance identifier for distributed claiming.
    pub instance_id: String,
}

impl Default for BatchProcessorConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(5),
            page_size: 100,
            stuck_threshold_secs: 600,
            cleanup_interval: Duration::from_secs(60),
            instance_id: format!(
                "batch-{}",
                Uuid::new_v4()
                    .to_string()
                    .split('-')
                    .next()
                    .unwrap_or("unknown")
            ),
        }
    }
}

// ============================================================================
// Metrics
// ============================================================================

/// Metrics tracked by the batch processor.
#[derive(Debug, Clone, Default)]
pub struct BatchMetrics {
    /// Number of batch jobs completed successfully.
    pub jobs_completed: u64,
    /// Number of batch jobs that failed.
    pub jobs_failed: u64,
    /// Total individual workflow operations executed.
    pub total_operations: u64,
    /// Total individual operation errors.
    pub total_errors: u64,
}

// ============================================================================
// Processor
// ============================================================================

/// Background processor for batch operations on workflows.
pub struct BatchProcessor {
    pool: PgPool,
    workflow_repo: Arc<WorkflowExecutionRepository>,
    /// Cancels as CancelWorkflow does: a run whose code has run is asked, and
    /// can clean up; one whose code never ran is cancelled at once.
    executions: Arc<crate::workflows::WorkflowExecutionService>,
    config: BatchProcessorConfig,
    metrics: Arc<Mutex<BatchMetrics>>,
    shutdown: Arc<Mutex<bool>>,
    /// How a `send_event` operation delivers to each run; without it such an
    /// operation fails every run.
    events: Option<crate::workflows::event_delivery::EventDelivery>,
}

impl BatchProcessor {
    /// Create a new batch processor.
    pub fn new(
        pool: PgPool,
        workflow_repo: Arc<WorkflowExecutionRepository>,
        executions: Arc<crate::workflows::WorkflowExecutionService>,
        config: BatchProcessorConfig,
    ) -> Self {
        Self {
            pool,
            workflow_repo,
            executions,
            config,
            metrics: Arc::new(Mutex::new(BatchMetrics::default())),
            shutdown: Arc::new(Mutex::new(false)),
            events: None,
        }
    }

    /// Deliver the events of `send_event` operations through `events`, as a
    /// single `SendEvent` is delivered.
    pub fn with_event_delivery(
        mut self,
        events: crate::workflows::event_delivery::EventDelivery,
    ) -> Self {
        self.events = Some(events);
        self
    }

    /// Signal the processor to shut down gracefully.
    pub async fn shutdown(&self) {
        *self.shutdown.lock().await = true;
    }

    /// Get current processor metrics.
    pub async fn get_metrics(&self) -> BatchMetrics {
        self.metrics.lock().await.clone()
    }

    /// Start the batch processor main loop.
    pub async fn start(self: Arc<Self>) -> OrcherResult<()> {
        info!(
            instance_id = %self.config.instance_id,
            poll_interval_ms = self.config.poll_interval.as_millis() as u64,
            "Starting batch processor"
        );

        // Spawn cleanup loop
        let cleanup_self = self.clone();
        tokio::spawn(async move {
            cleanup_self.run_cleanup_loop().await;
        });

        // Main loop
        self.run_processing_loop().await;

        info!("Batch processor stopped");
        Ok(())
    }

    async fn run_processing_loop(self: Arc<Self>) {
        loop {
            if *self.shutdown.lock().await {
                break;
            }

            // Try to claim a pending batch operation
            match self.claim_pending_job().await {
                Ok(Some(job_id)) => {
                    info!(job_id = %job_id, "Claimed batch operation");
                    if let Err(e) = self.process_batch_operation(job_id).await {
                        error!(job_id = %job_id, error = %e, "Batch operation failed");
                        let _ = self.mark_failed(job_id, &e.to_string()).await;
                        let mut m = self.metrics.lock().await;
                        m.jobs_failed += 1;
                    } else {
                        let mut m = self.metrics.lock().await;
                        m.jobs_completed += 1;
                    }
                    // Hot-loop: check for more immediately
                    continue;
                }
                Ok(None) => {}
                Err(e) => {
                    error!(error = %e, "Failed to claim batch operation");
                }
            }

            // Sleep before next poll
            sleep(self.config.poll_interval).await;
        }
    }

    async fn claim_pending_job(&self) -> OrcherResult<Option<Uuid>> {
        let row: Option<(Uuid,)> = sqlx::query_as(
            r#"
            UPDATE batch_operations
            SET status = 'processing',
                claimed_at = NOW(),
                claimed_by = $1,
                started_at = COALESCE(started_at, NOW())
            WHERE id = (
                SELECT id FROM batch_operations
                WHERE status = 'pending'
                ORDER BY created_at ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
            RETURNING id
            "#,
        )
        .bind(&self.config.instance_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| orcher_common::error::OrcherError::Internal {
            message: format!("Failed to claim batch operation: {}", e),
            source: None,
        })?;

        Ok(row.map(|(id,)| id))
    }

    async fn process_batch_operation(&self, job_id: Uuid) -> OrcherResult<()> {
        // Load the batch operation
        let job = self.load_job(job_id).await?;

        let op_type = job.operation_type.clone();
        let rate_limit = job.rate_limit_per_second as u64;
        let delay = 1000u64
            .checked_div(rate_limit)
            .map_or(Duration::from_millis(20), Duration::from_millis);

        // Count matching workflows
        let total = self.count_matching_workflows(&job).await?;
        self.update_total(job_id, total).await?;

        info!(
            job_id = %job_id,
            operation = %op_type,
            total = total,
            rate_limit = rate_limit,
            "Processing batch operation"
        );

        // Iterate through matching workflows in pages
        let mut offset = 0i64;
        let mut processed = 0i64;
        let mut success = 0i64;
        let mut failure = 0i64;

        loop {
            // Check if job was terminated
            if self.is_terminated(job_id).await? {
                info!(job_id = %job_id, "Batch operation terminated by user");
                return Ok(());
            }

            if *self.shutdown.lock().await {
                return Ok(());
            }

            let workflows = self.fetch_matching_page(&job, offset).await?;
            if workflows.is_empty() {
                break;
            }

            for execution in &workflows {
                // Check termination between each operation
                if self.is_terminated(job_id).await? {
                    self.update_progress(job_id, processed, success, failure)
                        .await?;
                    return Ok(());
                }

                match self.execute_operation(&job, execution).await {
                    Ok(()) => {
                        success += 1;
                        let mut m = self.metrics.lock().await;
                        m.total_operations += 1;
                    }
                    Err(e) => {
                        failure += 1;
                        let mut m = self.metrics.lock().await;
                        m.total_errors += 1;
                        debug!(execution_id = %execution.execution_id, error = %e, "Operation failed on workflow");
                    }
                }
                processed += 1;

                // Rate limiting
                sleep(delay).await;
            }

            // Update progress after each page
            self.update_progress(job_id, processed, success, failure)
                .await?;
            offset += self.config.page_size;
        }

        // Mark completed
        self.update_progress(job_id, processed, success, failure)
            .await?;
        self.mark_completed(job_id).await?;

        info!(
            job_id = %job_id,
            processed = processed,
            success = success,
            failure = failure,
            "Batch operation completed"
        );

        Ok(())
    }

    async fn execute_operation(
        &self,
        job: &BatchJob,
        execution: &WorkflowExecution,
    ) -> OrcherResult<()> {
        let execution_id = execution.execution_id.as_str();
        match job.operation_type.as_str() {
            // Each as its single-workflow call does, so a batch is exactly
            // that many single operations. A run that has already ended is not
            // an error for a batch: it is simply no longer one to act on.
            "cancel" => self
                .executions
                .cancel_execution(
                    execution_id,
                    &format!("Cancelled by batch operation {}", job.id),
                    None,
                )
                .await
                .map(|_| ()),
            "terminate" => self.workflow_repo.terminate(execution_id).await.map(|_| ()),
            "send_event" => {
                let events = self.events.as_ref().ok_or_else(|| {
                    orcher_common::error::OrcherError::internal(
                        "event delivery is not configured for batch operations",
                    )
                })?;
                let name = job
                    .event_name
                    .as_deref()
                    .filter(|n| !n.is_empty())
                    .ok_or_else(|| {
                        orcher_common::error::OrcherError::validation(
                            "event_name",
                            "a send_event batch operation names its event",
                        )
                    })?;
                let payload = match &job.event_payload {
                    Some(value) => serde_json::to_vec(value).map_err(|e| {
                        orcher_common::error::OrcherError::internal(format!("event payload: {e}"))
                    })?,
                    None => Vec::new(),
                };
                // The operation's id is the request id: a job resumed after a
                // restart delivers nothing twice to a run it already reached.
                events
                    .deliver(
                        execution,
                        name,
                        &payload,
                        &format!("batch-operation:{}", job.id),
                    )
                    .await
                    .map(|_| ())
            }
            "reset" => Err(orcher_common::error::OrcherError::validation(
                "operation_type",
                "reset is not supported as a batch operation",
            )),
            _ => Err(orcher_common::error::OrcherError::Internal {
                message: format!("Unknown batch operation type: {}", job.operation_type),
                source: None,
            }),
        }
    }

    // ── SQL helpers ──

    async fn load_job(&self, id: Uuid) -> OrcherResult<BatchJob> {
        let row: BatchJob = sqlx::query_as(
            "SELECT id, organization_id, operation_type, namespace, filter_status, \
             filter_workflow_type, rate_limit_per_second, event_name, event_payload \
             FROM batch_operations WHERE id = $1",
        )
        .bind(id)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| orcher_common::error::OrcherError::Internal {
            message: format!("Failed to load batch job: {}", e),
            source: None,
        })?;
        Ok(row)
    }

    async fn count_matching_workflows(&self, job: &BatchJob) -> OrcherResult<i64> {
        let filters = self.build_filters(job, 0);
        self.workflow_repo.count(filters).await
    }

    async fn fetch_matching_page(
        &self,
        job: &BatchJob,
        offset: i64,
    ) -> OrcherResult<Vec<WorkflowExecution>> {
        let filters = self.build_filters(job, offset);
        self.workflow_repo.list(filters).await
    }

    fn build_filters(&self, job: &BatchJob, offset: i64) -> ListExecutionsFilters {
        ListExecutionsFilters {
            workflow_name: job.filter_workflow_type.clone(),
            workflow_version: None,
            namespace: Some(job.namespace.clone()),
            task_queue: None,
            status: job.filter_status.clone(),
            user_id: None,
            // The owner recorded at creation. NULL means the operation was
            // created without tenancy, which leaves the match unscoped — the
            // correct behaviour for a single-tenant deployment, and the reason
            // V040 terminates operations that were already in flight when the
            // column was added rather than running them unowned.
            organization_id: job.organization_id,
            parent_execution_id: None,
            labels: None,
            submitted_after: None,
            limit: self.config.page_size,
            offset,
            cursor_submitted_at: None,
            cursor_execution_id: None,
        }
    }

    async fn update_total(&self, id: Uuid, total: i64) -> OrcherResult<()> {
        sqlx::query("UPDATE batch_operations SET total_count = $2 WHERE id = $1")
            .bind(id)
            .bind(total)
            .execute(&self.pool)
            .await
            .map_err(|e| orcher_common::error::OrcherError::Internal {
                message: format!("Failed to update total: {}", e),
                source: None,
            })?;
        Ok(())
    }

    async fn update_progress(
        &self,
        id: Uuid,
        processed: i64,
        success: i64,
        failure: i64,
    ) -> OrcherResult<()> {
        sqlx::query(
            "UPDATE batch_operations SET processed_count = $2, success_count = $3, failure_count = $4 WHERE id = $1",
        )
        .bind(id)
        .bind(processed)
        .bind(success)
        .bind(failure)
        .execute(&self.pool)
        .await
        .map_err(|e| orcher_common::error::OrcherError::Internal {
            message: format!("Failed to update progress: {}", e),
            source: None,
        })?;
        Ok(())
    }

    async fn mark_completed(&self, id: Uuid) -> OrcherResult<()> {
        sqlx::query(
            "UPDATE batch_operations SET status = 'completed', completed_at = NOW() WHERE id = $1",
        )
        .bind(id)
        .execute(&self.pool)
        .await
        .map_err(|e| orcher_common::error::OrcherError::Internal {
            message: format!("Failed to mark completed: {}", e),
            source: None,
        })?;
        Ok(())
    }

    async fn mark_failed(&self, id: Uuid, error: &str) -> OrcherResult<()> {
        sqlx::query("UPDATE batch_operations SET status = 'failed', error = $2, completed_at = NOW() WHERE id = $1")
            .bind(id)
            .bind(error)
            .execute(&self.pool)
            .await
            .map_err(|e| orcher_common::error::OrcherError::Internal {
                message: format!("Failed to mark failed: {}", e),
                source: None,
            })?;
        Ok(())
    }

    async fn is_terminated(&self, id: Uuid) -> OrcherResult<bool> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT status FROM batch_operations WHERE id = $1")
                .bind(id)
                .fetch_optional(&self.pool)
                .await
                .map_err(|e| orcher_common::error::OrcherError::Internal {
                    message: format!("Failed to check status: {}", e),
                    source: None,
                })?;
        Ok(row.map(|(s,)| s == "terminated").unwrap_or(true))
    }

    async fn run_cleanup_loop(self: Arc<Self>) {
        loop {
            if *self.shutdown.lock().await {
                break;
            }
            sleep(self.config.cleanup_interval).await;

            match sqlx::query(
                &format!(
                    "UPDATE batch_operations SET status = 'pending', claimed_at = NULL, claimed_by = NULL \
                     WHERE status = 'processing' AND claimed_at < NOW() - INTERVAL '{} seconds'",
                    self.config.stuck_threshold_secs
                ),
            )
            .execute(&self.pool)
            .await
            {
                Ok(r) => {
                    if r.rows_affected() > 0 {
                        warn!(count = r.rows_affected(), "Recovered stuck batch operations");
                    }
                }
                Err(e) => {
                    error!(error = %e, "Failed to cleanup stuck batch operations");
                }
            }
        }
    }
}

// Minimal struct for loading job info
#[derive(sqlx::FromRow)]
#[allow(dead_code)]
struct BatchJob {
    id: Uuid,
    /// Organisation the operation acts within, recorded when it was created.
    ///
    /// This is what bounds the workflows the job may touch. Without it the
    /// match was by namespace *name* alone, and namespace names are unique per
    /// organisation rather than per deployment — so a cancel against 'default'
    /// matched every organisation that also had a namespace called 'default'.
    organization_id: Option<Uuid>,
    operation_type: String,
    namespace: String,
    filter_status: Option<String>,
    filter_workflow_type: Option<String>,
    rate_limit_per_second: i32,
    /// The event a `send_event` operation delivers.
    event_name: Option<String>,
    event_payload: Option<serde_json::Value>,
}
