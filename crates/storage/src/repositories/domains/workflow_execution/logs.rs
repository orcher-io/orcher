//! # Workflow Execution Logs Repository
//!
//! Repository for managing workflow execution logs with support for real-time streaming.
//! Optimized for Server-Sent Events (SSE) polling patterns.
//!
//! ## Features
//!
//! - Add log entries during workflow execution
//! - Retrieve logs with pagination
//! - Poll for new logs since a specific ID (SSE support)
//! - Filter logs by level (error, warn, info, etc.)
//! - Efficient querying with optimized indexes
//!
//! ## Example
//!
//! ```rust,ignore
//! use orcher_storage::repositories::domains::workflow_execution::WorkflowExecutionLogsRepository;
//!
//! let repo = WorkflowExecutionLogsRepository::new(pool);
//!
//! // Add a log entry
//! repo.add_log(&WorkflowExecutionLog {
//!     execution_id: "exec-123".to_string(),
//!     level: "info".to_string(),
//!     message: "Workflow started".to_string(),
//!     ..Default::default()
//! }).await?;
//!
//! // Get latest logs
//! let logs = repo.get_logs("exec-123", 100).await?;
//!
//! // Poll for new logs (SSE)
//! let new_logs = repo.get_logs_after("exec-123", last_log_id, 50).await?;
//! ```

use orcher_common::types::time::Timestamptz;
use orcher_common::{error::OrcherError, OrcherResult};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, QueryBuilder};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};
use uuid::Uuid;

/// Workflow execution log entry
///
/// Maps directly to the workflow_execution_logs table (V003 schema)
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct WorkflowExecutionLog {
    /// Log entry ID (auto-incrementing)
    pub id: i64,

    /// Reference to workflow_executions.execution_id
    pub execution_id: String,

    /// Log timestamp
    pub timestamp: Timestamptz,

    /// Log level: trace, debug, info, warn, error, fatal
    pub level: String,

    /// Log message content
    pub message: String,

    /// Current workflow step/phase
    pub step: Option<String>,

    /// Associated task ID
    pub task_id: Option<Uuid>,

    /// Worker ID that generated the log
    pub worker_id: Option<String>,

    /// Additional structured metadata
    pub metadata: serde_json::Value,

    /// Log source identifier
    pub source: Option<String>,

    /// Record creation timestamp
    pub created_at: Timestamptz,
}

impl Default for WorkflowExecutionLog {
    fn default() -> Self {
        Self {
            id: 0,
            execution_id: String::new(),
            timestamp: Timestamptz::now(),
            level: "info".to_string(),
            message: String::new(),
            step: None,
            task_id: None,
            worker_id: None,
            metadata: serde_json::json!({}),
            source: None,
            created_at: Timestamptz::now(),
        }
    }
}

impl WorkflowExecutionLog {
    /// Create a new log entry
    pub fn new(execution_id: String, level: String, message: String) -> Self {
        let now = Timestamptz::now();
        Self {
            id: 0, // Will be set by database
            execution_id,
            timestamp: now,
            level,
            message,
            step: None,
            task_id: None,
            worker_id: None,
            metadata: serde_json::json!({}),
            source: None,
            created_at: now,
        }
    }

    /// Create an info log
    pub fn info(execution_id: String, message: String) -> Self {
        Self::new(execution_id, "info".to_string(), message)
    }

    /// Create a warn log
    pub fn warn(execution_id: String, message: String) -> Self {
        Self::new(execution_id, "warn".to_string(), message)
    }

    /// Create an error log
    pub fn error(execution_id: String, message: String) -> Self {
        Self::new(execution_id, "error".to_string(), message)
    }

    /// Create a debug log
    pub fn debug(execution_id: String, message: String) -> Self {
        Self::new(execution_id, "debug".to_string(), message)
    }

    /// Set the step
    pub fn with_step(mut self, step: String) -> Self {
        self.step = Some(step);
        self
    }

    /// Set the task ID
    pub fn with_task_id(mut self, task_id: Uuid) -> Self {
        self.task_id = Some(task_id);
        self
    }

    /// Set the worker ID
    pub fn with_worker_id(mut self, worker_id: String) -> Self {
        self.worker_id = Some(worker_id);
        self
    }

    /// Set the metadata
    pub fn with_metadata(mut self, metadata: serde_json::Value) -> Self {
        self.metadata = metadata;
        self
    }

    /// Set the source
    pub fn with_source(mut self, source: String) -> Self {
        self.source = Some(source);
        self
    }

    /// Check if this is an error log
    pub fn is_error(&self) -> bool {
        matches!(self.level.as_str(), "error" | "fatal")
    }

    /// Check if this is a warning log
    pub fn is_warning(&self) -> bool {
        self.level == "warn"
    }
}

/// Workflow execution logs repository
///
/// Handles persistence and querying of workflow execution logs.
/// Optimized for real-time streaming via Server-Sent Events (SSE).
pub struct WorkflowExecutionLogsRepository {
    pool: PgPool,
}

impl WorkflowExecutionLogsRepository {
    /// Create a new workflow execution logs repository
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Add a log entry
    ///
    /// # Arguments
    ///
    /// * `log` - Log entry to add
    ///
    /// # Returns
    ///
    /// Returns the log entry with ID populated
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let log = WorkflowExecutionLog::info(
    ///     "exec-123".to_string(),
    ///     "Workflow started".to_string(),
    /// );
    /// let created = repo.add_log(&log).await?;
    /// println!("Log ID: {}", created.id);
    /// ```
    pub async fn add_log(&self, log: &WorkflowExecutionLog) -> OrcherResult<WorkflowExecutionLog> {
        // Fast path: if a process-wide async writer is initialized, hand the log
        // off to it and return immediately. These logs are best-effort observability
        // (every call site already treats a failure as non-fatal), so keeping them
        // off the synchronous transition path removes ~7 INSERT statements per workflow from
        // the hot path. The returned row echoes the input (id unassigned) — no
        // caller reads the id. Falls through to the synchronous insert when no
        // writer is installed (tests, non-orchestrator processes).
        if let Some(writer) = ASYNC_LOG_WRITER.get() {
            writer.enqueue(log.clone());
            return Ok(log.clone());
        }

        debug!(
            execution_id = %log.execution_id,
            level = %log.level,
            "Adding workflow execution log"
        );

        let row = sqlx::query_as::<_, WorkflowExecutionLog>(
            concat!(r#"
            INSERT INTO workflow_execution_logs (
                execution_id, timestamp, level, message, step, task_id,
                worker_id, metadata, source
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            RETURNING "#, crate::repositories::columns::workflow_execution_logs!(), r#"
            "#),
        )
        .bind(&log.execution_id)
        .bind(log.timestamp)
        .bind(&log.level)
        .bind(&log.message)
        .bind(&log.step)
        .bind(log.task_id)
        .bind(&log.worker_id)
        .bind(&log.metadata)
        .bind(&log.source)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| {
            warn!(
                error = %e,
                execution_id = %log.execution_id,
                level = %log.level,
                message = %log.message,
                error_code = ?e.as_database_error().map(|de| de.code()),
                error_constraint = ?e.as_database_error().and_then(|de| de.constraint()),
                "Failed to add workflow execution log - check foreign key constraint on execution_id"
            );
            OrcherError::internal(format!(
                "Failed to add workflow execution log for execution_id '{}': {} (constraint: {:?})",
                log.execution_id,
                e,
                e.as_database_error().and_then(|de| de.constraint())
            ))
        })?;

        Ok(row)
    }

    /// Add multiple log entries in a single transaction
    ///
    /// More efficient than calling add_log multiple times.
    ///
    /// # Arguments
    ///
    /// * `logs` - Log entries to add
    ///
    /// # Returns
    ///
    /// Returns the number of logs inserted
    pub async fn add_logs_batch(&self, logs: &[WorkflowExecutionLog]) -> OrcherResult<u64> {
        if logs.is_empty() {
            return Ok(0);
        }

        debug!(
            execution_id = %logs[0].execution_id,
            count = logs.len(),
            "Adding batch of workflow execution logs"
        );

        // One multi-row INSERT (a single statement + commit) instead of a row per
        // log — this is what makes the async writer's coalescing pay off.
        let mut builder = QueryBuilder::new(
            "INSERT INTO workflow_execution_logs \
             (execution_id, timestamp, level, message, step, task_id, worker_id, metadata, source) ",
        );
        builder.push_values(logs, |mut b, log| {
            b.push_bind(&log.execution_id)
                .push_bind(log.timestamp)
                .push_bind(&log.level)
                .push_bind(&log.message)
                .push_bind(&log.step)
                .push_bind(log.task_id)
                .push_bind(&log.worker_id)
                .push_bind(&log.metadata)
                .push_bind(&log.source);
        });

        let result = builder
            .build()
            .execute(&self.pool)
            .await
            .map_err(|e| OrcherError::internal(format!("Failed to insert log batch: {}", e)))?;

        let inserted = result.rows_affected();
        debug!(count = inserted, "Batch logs inserted successfully");

        Ok(inserted)
    }

    /// Get logs for an execution
    ///
    /// Returns the latest `limit` lines in the order they were written, oldest
    /// first, as every read here does.
    ///
    /// The order is the id's, which the database assigns as each line is
    /// written, and which `after_id` pages and follows by. Not the timestamp's:
    /// a line's timestamp is the clock of whatever wrote it, and lines reach
    /// the table from more than one process and through a batching writer, so
    /// timestamps do not follow write order. Ordered by them, a log read newest
    /// first and a follow that resumes after the last id it was given could
    /// skip a line or read one twice.
    ///
    /// # Arguments
    ///
    /// * `execution_id` - Execution identifier
    /// * `limit` - Maximum number of logs to return
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let logs = repo.get_logs("exec-123", 100).await?;
    /// for log in logs {
    ///     println!("[{}] {}", log.level, log.message);
    /// }
    /// ```
    pub async fn get_logs(
        &self,
        execution_id: &str,
        limit: i64,
    ) -> OrcherResult<Vec<WorkflowExecutionLog>> {
        debug!(execution_id = %execution_id, limit, "Getting workflow execution logs");

        let logs = sqlx::query_as::<_, WorkflowExecutionLog>(concat!(
            r#"
            SELECT * FROM (
                SELECT "#,
            crate::repositories::columns::workflow_execution_logs!(),
            r#" FROM workflow_execution_logs
                WHERE execution_id = $1
                ORDER BY id DESC
                LIMIT $2
            ) latest
            ORDER BY id
            "#
        ))
        .bind(execution_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            OrcherError::internal(format!("Failed to get workflow execution logs: {}", e))
        })?;

        Ok(logs)
    }

    /// Get logs after a specific log ID
    ///
    /// This is optimized for SSE polling. Returns logs in ascending order (oldest first)
    /// so clients can process them in chronological order.
    ///
    /// # Arguments
    ///
    /// * `execution_id` - Execution identifier
    /// * `after_id` - Log ID to start after (exclusive)
    /// * `limit` - Maximum number of logs to return
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // SSE polling loop
    /// let mut last_id = 0;
    /// loop {
    ///     let new_logs = repo.get_logs_after("exec-123", last_id, 50).await?;
    ///     for log in new_logs {
    ///         // Send to SSE client
    ///         last_id = log.id;
    ///     }
    ///     tokio::time::sleep(Duration::from_millis(500)).await;
    /// }
    /// ```
    pub async fn get_logs_after(
        &self,
        execution_id: &str,
        after_id: i64,
        limit: i64,
    ) -> OrcherResult<Vec<WorkflowExecutionLog>> {
        debug!(
            execution_id = %execution_id,
            after_id,
            limit,
            "Getting workflow execution logs after ID"
        );

        let logs = sqlx::query_as::<_, WorkflowExecutionLog>(concat!(
            r#"
            SELECT "#,
            crate::repositories::columns::workflow_execution_logs!(),
            r#" FROM workflow_execution_logs
            WHERE execution_id = $1 AND id > $2
            ORDER BY id ASC
            LIMIT $3
            "#
        ))
        .bind(execution_id)
        .bind(after_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            OrcherError::internal(format!(
                "Failed to get workflow execution logs after ID: {}",
                e
            ))
        })?;

        Ok(logs)
    }

    /// Get logs by level
    ///
    /// The latest `limit` such lines, in the order they were written; see
    /// [`get_logs`](Self::get_logs).
    ///
    /// # Arguments
    ///
    /// * `execution_id` - Execution identifier
    /// * `level` - Log level to filter by (error, warn, info, debug, trace)
    /// * `limit` - Maximum number of logs to return
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // Get all error logs
    /// let errors = repo.get_logs_by_level("exec-123", "error", 100).await?;
    /// ```
    pub async fn get_logs_by_level(
        &self,
        execution_id: &str,
        level: &str,
        limit: i64,
    ) -> OrcherResult<Vec<WorkflowExecutionLog>> {
        debug!(
            execution_id = %execution_id,
            level = %level,
            limit,
            "Getting workflow execution logs by level"
        );

        let logs = sqlx::query_as::<_, WorkflowExecutionLog>(concat!(
            r#"
            SELECT * FROM (
                SELECT "#,
            crate::repositories::columns::workflow_execution_logs!(),
            r#" FROM workflow_execution_logs
                WHERE execution_id = $1 AND level = $2
                ORDER BY id DESC
                LIMIT $3
            ) latest
            ORDER BY id
            "#
        ))
        .bind(execution_id)
        .bind(level)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            OrcherError::internal(format!(
                "Failed to get workflow execution logs by level: {}",
                e
            ))
        })?;

        Ok(logs)
    }

    /// Get logs by step
    ///
    /// The latest `limit` such lines, in the order they were written; see
    /// [`get_logs`](Self::get_logs).
    ///
    /// # Arguments
    ///
    /// * `execution_id` - Execution identifier
    /// * `step` - Step name to filter by
    /// * `limit` - Maximum number of logs to return
    pub async fn get_logs_by_step(
        &self,
        execution_id: &str,
        step: &str,
        limit: i64,
    ) -> OrcherResult<Vec<WorkflowExecutionLog>> {
        debug!(
            execution_id = %execution_id,
            step = %step,
            limit,
            "Getting workflow execution logs by step"
        );

        let logs = sqlx::query_as::<_, WorkflowExecutionLog>(concat!(
            r#"
            SELECT * FROM (
                SELECT "#,
            crate::repositories::columns::workflow_execution_logs!(),
            r#" FROM workflow_execution_logs
                WHERE execution_id = $1 AND step = $2
                ORDER BY id DESC
                LIMIT $3
            ) latest
            ORDER BY id
            "#
        ))
        .bind(execution_id)
        .bind(step)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            OrcherError::internal(format!(
                "Failed to get workflow execution logs by step: {}",
                e
            ))
        })?;

        Ok(logs)
    }

    /// Get error and fatal logs
    ///
    /// Convenience method to get only error-level logs.
    ///
    /// The latest `limit` such lines, in the order they were written; see
    /// [`get_logs`](Self::get_logs).
    ///
    /// # Arguments
    ///
    /// * `execution_id` - Execution identifier
    /// * `limit` - Maximum number of logs to return
    pub async fn get_error_logs(
        &self,
        execution_id: &str,
        limit: i64,
    ) -> OrcherResult<Vec<WorkflowExecutionLog>> {
        debug!(execution_id = %execution_id, limit, "Getting error logs");

        let logs = sqlx::query_as::<_, WorkflowExecutionLog>(concat!(
            r#"
            SELECT * FROM (
                SELECT "#,
            crate::repositories::columns::workflow_execution_logs!(),
            r#" FROM workflow_execution_logs
                WHERE execution_id = $1 AND level IN ('error', 'fatal')
                ORDER BY id DESC
                LIMIT $2
            ) latest
            ORDER BY id
            "#
        ))
        .bind(execution_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| OrcherError::internal(format!("Failed to get error logs: {}", e)))?;

        Ok(logs)
    }

    /// Count logs for an execution
    ///
    /// # Arguments
    ///
    /// * `execution_id` - Execution identifier
    ///
    /// # Returns
    ///
    /// Returns the total number of log entries for the execution
    pub async fn count_logs(&self, execution_id: &str) -> OrcherResult<i64> {
        debug!(execution_id = %execution_id, "Counting workflow execution logs");

        let count = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM workflow_execution_logs WHERE execution_id = $1",
        )
        .bind(execution_id)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| {
            OrcherError::internal(format!("Failed to count workflow execution logs: {}", e))
        })?;

        Ok(count)
    }

    /// Count logs by level
    ///
    /// # Arguments
    ///
    /// * `execution_id` - Execution identifier
    /// * `level` - Log level to count
    pub async fn count_logs_by_level(&self, execution_id: &str, level: &str) -> OrcherResult<i64> {
        debug!(
            execution_id = %execution_id,
            level = %level,
            "Counting workflow execution logs by level"
        );

        let count = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM workflow_execution_logs WHERE execution_id = $1 AND level = $2",
        )
        .bind(execution_id)
        .bind(level)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| {
            OrcherError::internal(format!(
                "Failed to count workflow execution logs by level: {}",
                e
            ))
        })?;

        Ok(count)
    }

    /// Get the latest log ID for an execution
    ///
    /// Useful for clients to start SSE streaming from the latest position.
    ///
    /// # Arguments
    ///
    /// * `execution_id` - Execution identifier
    ///
    /// # Returns
    ///
    /// Returns the latest log ID, or None if no logs exist
    pub async fn get_latest_log_id(&self, execution_id: &str) -> OrcherResult<Option<i64>> {
        debug!(execution_id = %execution_id, "Getting latest log ID");

        let id = sqlx::query_scalar::<_, Option<i64>>(
            "SELECT MAX(id) FROM workflow_execution_logs WHERE execution_id = $1",
        )
        .bind(execution_id)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| OrcherError::internal(format!("Failed to get latest log ID: {}", e)))?;

        Ok(id)
    }

    /// Delete logs for an execution
    ///
    /// This is called automatically via CASCADE when the execution is deleted,
    /// but can also be called manually for cleanup.
    ///
    /// # Arguments
    ///
    /// * `execution_id` - Execution identifier
    ///
    /// # Returns
    ///
    /// Returns the number of logs deleted
    pub async fn delete_logs(&self, execution_id: &str) -> OrcherResult<u64> {
        info!(execution_id = %execution_id, "Deleting workflow execution logs");

        let result = sqlx::query("DELETE FROM workflow_execution_logs WHERE execution_id = $1")
            .bind(execution_id)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                OrcherError::internal(format!("Failed to delete workflow execution logs: {}", e))
            })?;

        let deleted = result.rows_affected();
        info!(execution_id = %execution_id, deleted, "Logs deleted successfully");

        Ok(deleted)
    }

    /// Delete old logs
    ///
    /// Useful for log retention policies.
    ///
    /// # Arguments
    ///
    /// * `older_than` - Delete logs older than this timestamp
    ///
    /// # Returns
    ///
    /// Returns the number of logs deleted
    pub async fn delete_logs_older_than(&self, older_than: Timestamptz) -> OrcherResult<u64> {
        info!(older_than = %older_than, "Deleting old workflow execution logs");

        let result = sqlx::query("DELETE FROM workflow_execution_logs WHERE created_at < $1")
            .bind(older_than)
            .execute(&self.pool)
            .await
            .map_err(|e| OrcherError::internal(format!("Failed to delete old logs: {}", e)))?;

        let deleted = result.rows_affected();
        info!(deleted, "Old logs deleted successfully");

        Ok(deleted)
    }
}

/// Process-wide async log writer. Set once at orchestrator startup via
/// [`init_async_log_writer`]; when present, [`WorkflowExecutionLogsRepository::add_log`]
/// enqueues into it instead of writing synchronously.
static ASYNC_LOG_WRITER: OnceLock<AsyncLogWriter> = OnceLock::new();

/// Tuning for the async workflow-log writer.
#[derive(Debug, Clone)]
pub struct LogWriterConfig {
    /// Bounded queue capacity. When full, new logs are dropped (best-effort
    /// observability) and counted rather than blocking the hot path.
    pub queue_capacity: usize,
    /// Flush when this many logs have accumulated.
    pub max_batch: usize,
    /// Flush at least this often even if `max_batch` isn't reached.
    pub flush_interval: Duration,
}

impl Default for LogWriterConfig {
    fn default() -> Self {
        Self {
            queue_capacity: 8192,
            max_batch: 512,
            flush_interval: Duration::from_millis(250),
        }
    }
}

/// Handle to the background log-flushing task: a bounded channel plus a
/// dropped-log counter.
struct AsyncLogWriter {
    tx: mpsc::Sender<WorkflowExecutionLog>,
    dropped: AtomicU64,
}

impl AsyncLogWriter {
    /// Non-blocking enqueue. Drops (and counts) the log if the queue is full —
    /// these are best-effort observability records, never durable state.
    fn enqueue(&self, log: WorkflowExecutionLog) {
        if self.tx.try_send(log).is_err() {
            let n = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
            // Log sparsely so a saturated queue doesn't itself become a log flood.
            if n.is_power_of_two() {
                warn!(
                    dropped_total = n,
                    "workflow-execution-log queue full; dropping logs (observability only)"
                );
            }
        }
    }
}

/// Initialize the process-wide async workflow-log writer. Idempotent: only the
/// first call installs a writer (subsequent calls are ignored). Spawns a
/// background task that coalesces enqueued logs into multi-row inserts.
///
/// Call once at startup on a process that owns the hot path (the orchestrator).
/// Processes that never call this keep the synchronous `add_log` behavior.
pub fn init_async_log_writer(pool: PgPool, config: LogWriterConfig) {
    let _ = ASYNC_LOG_WRITER.get_or_init(|| {
        let (tx, rx) = mpsc::channel(config.queue_capacity);
        let repo = WorkflowExecutionLogsRepository::new(pool);
        tokio::spawn(run_log_flusher(repo, rx, config.clone()));
        info!(
            queue_capacity = config.queue_capacity,
            max_batch = config.max_batch,
            flush_interval_ms = config.flush_interval.as_millis() as u64,
            "Async workflow-execution-log writer initialized"
        );
        AsyncLogWriter {
            tx,
            dropped: AtomicU64::new(0),
        }
    });
}

/// Background loop: accumulate logs and flush on size threshold, on a timer, or
/// when the channel closes (shutdown). A failed flush is logged and the batch is
/// dropped — logs are best-effort and must never stall the writer.
async fn run_log_flusher(
    repo: WorkflowExecutionLogsRepository,
    mut rx: mpsc::Receiver<WorkflowExecutionLog>,
    config: LogWriterConfig,
) {
    let mut buf: Vec<WorkflowExecutionLog> = Vec::with_capacity(config.max_batch);
    let mut ticker = tokio::time::interval(config.flush_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            maybe = rx.recv() => {
                match maybe {
                    Some(log) => {
                        buf.push(log);
                        if buf.len() >= config.max_batch {
                            flush_log_buffer(&repo, &mut buf).await;
                        }
                    }
                    None => {
                        // Senders dropped (shutdown) — drain and exit.
                        flush_log_buffer(&repo, &mut buf).await;
                        break;
                    }
                }
            }
            _ = ticker.tick() => {
                flush_log_buffer(&repo, &mut buf).await;
            }
        }
    }
}

async fn flush_log_buffer(
    repo: &WorkflowExecutionLogsRepository,
    buf: &mut Vec<WorkflowExecutionLog>,
) {
    if buf.is_empty() {
        return;
    }
    if let Err(e) = repo.add_logs_batch(buf).await {
        warn!(
            count = buf.len(),
            error = %e,
            "Failed to flush workflow-execution-log batch (dropping)"
        );
    }
    buf.clear();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_creation() {
        let log = WorkflowExecutionLog::info("exec-123".to_string(), "Test message".to_string());

        assert_eq!(log.execution_id, "exec-123");
        assert_eq!(log.level, "info");
        assert_eq!(log.message, "Test message");
        assert!(log.step.is_none());
    }

    #[test]
    fn test_log_builder() {
        let log = WorkflowExecutionLog::error("exec-456".to_string(), "Error occurred".to_string())
            .with_step("step1".to_string())
            .with_source("worker-1".to_string())
            .with_metadata(serde_json::json!({"error_code": 500}));

        assert_eq!(log.execution_id, "exec-456");
        assert_eq!(log.level, "error");
        assert_eq!(log.step, Some("step1".to_string()));
        assert_eq!(log.source, Some("worker-1".to_string()));
        assert!(log.is_error());
        assert!(!log.is_warning());
    }

    #[test]
    fn test_log_level_checks() {
        let error_log = WorkflowExecutionLog::error("exec-1".to_string(), "Error".to_string());
        assert!(error_log.is_error());
        assert!(!error_log.is_warning());

        let warn_log = WorkflowExecutionLog::warn("exec-1".to_string(), "Warning".to_string());
        assert!(!warn_log.is_error());
        assert!(warn_log.is_warning());

        let info_log = WorkflowExecutionLog::info("exec-1".to_string(), "Info".to_string());
        assert!(!info_log.is_error());
        assert!(!info_log.is_warning());
    }

    #[test]
    fn test_log_default() {
        let log = WorkflowExecutionLog::default();
        assert_eq!(log.level, "info");
        assert!(log.execution_id.is_empty());
        assert!(log.message.is_empty());
    }
}
