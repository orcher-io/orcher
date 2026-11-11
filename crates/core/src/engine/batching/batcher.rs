//! Batch accumulator and flush logic.

use super::types::{
    BatchWriteResult, BatchedWrite, JournalAppendOp, OutboxCompleteOp, OutboxInsertOp,
    StateUpdateOp, WriteOperation,
};
use orcher_common::error::{OrcherError, OrcherResult};
use orcher_common::types::time::Timestamptz;
use sqlx::PgPool;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{interval, Instant};
use tracing::{debug, error, info};

/// Configuration for the write batcher
#[derive(Debug, Clone)]
pub struct WriteBatcherConfig {
    /// Maximum number of writes to batch before flushing
    pub max_batch_size: usize,

    /// Maximum time to wait before flushing (even if batch is not full)
    pub max_wait_time: Duration,

    /// Channel buffer size for incoming writes
    pub channel_buffer_size: usize,

    /// Number of parallel flush workers
    pub flush_workers: usize,

    /// Enable adaptive batching based on load
    pub adaptive_batching: bool,
}

impl Default for WriteBatcherConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 100,
            max_wait_time: Duration::from_millis(10),
            channel_buffer_size: 10_000,
            flush_workers: 2,
            adaptive_batching: true,
        }
    }
}

impl WriteBatcherConfig {
    /// Create a high-throughput configuration
    pub fn high_throughput() -> Self {
        Self {
            max_batch_size: 500,
            max_wait_time: Duration::from_millis(20),
            channel_buffer_size: 50_000,
            flush_workers: 4,
            adaptive_batching: true,
        }
    }

    /// Create a low-latency configuration
    pub fn low_latency() -> Self {
        Self {
            max_batch_size: 20,
            max_wait_time: Duration::from_millis(2),
            channel_buffer_size: 5_000,
            flush_workers: 2,
            adaptive_batching: false,
        }
    }
}

/// Write batcher for high-performance database writes
///
/// Collects individual write operations and executes them in batches,
/// reducing database round-trips and improving throughput.
pub struct WriteBatcher {
    /// Database connection pool
    pool: PgPool,

    /// Configuration
    config: WriteBatcherConfig,

    /// Channel sender for incoming writes
    tx: mpsc::Sender<BatchedWrite>,

    /// Channel receiver (moved to background task on start)
    rx: Option<mpsc::Receiver<BatchedWrite>>,

    /// Shutdown flag
    shutdown: Arc<AtomicBool>,

    /// Metrics: total writes queued
    writes_queued: Arc<AtomicU64>,

    /// Metrics: total writes flushed
    writes_flushed: Arc<AtomicU64>,

    /// Metrics: total batches flushed
    batches_flushed: Arc<AtomicU64>,
}

impl WriteBatcher {
    /// Create a new write batcher
    pub fn new(pool: PgPool, config: WriteBatcherConfig) -> Self {
        let (tx, rx) = mpsc::channel(config.channel_buffer_size);

        Self {
            pool,
            config,
            tx,
            rx: Some(rx),
            shutdown: Arc::new(AtomicBool::new(false)),
            writes_queued: Arc::new(AtomicU64::new(0)),
            writes_flushed: Arc::new(AtomicU64::new(0)),
            batches_flushed: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Start the background flush task
    ///
    /// Returns a handle that can be used to shutdown the batcher.
    pub fn start(&mut self) -> WriteBatcherHandle {
        let rx = self.rx.take().expect("Batcher already started");
        let pool = self.pool.clone();
        let config = self.config.clone();
        let shutdown = self.shutdown.clone();
        let writes_flushed = self.writes_flushed.clone();
        let batches_flushed = self.batches_flushed.clone();

        let handle = tokio::spawn(async move {
            Self::flush_loop(rx, pool, config, shutdown, writes_flushed, batches_flushed).await;
        });

        info!(
            max_batch_size = self.config.max_batch_size,
            max_wait_ms = self.config.max_wait_time.as_millis(),
            "Write batcher started"
        );

        WriteBatcherHandle {
            shutdown: self.shutdown.clone(),
            handle: Some(handle),
        }
    }

    /// Queue a journal append operation
    pub async fn queue_journal_append(&self, op: JournalAppendOp) -> OrcherResult<()> {
        let (write, rx) = BatchedWrite::new(WriteOperation::JournalAppend(op));
        self.queue_write(write, rx).await
    }

    /// Queue a state update operation
    pub async fn queue_state_update(&self, op: StateUpdateOp) -> OrcherResult<()> {
        let (write, rx) = BatchedWrite::new(WriteOperation::StateUpdate(op));
        self.queue_write(write, rx).await
    }

    /// Queue an outbox insert operation
    pub async fn queue_outbox_insert(&self, op: OutboxInsertOp) -> OrcherResult<()> {
        let (write, rx) = BatchedWrite::new(WriteOperation::OutboxInsert(op));
        self.queue_write(write, rx).await
    }

    /// Queue an outbox complete operation
    pub async fn queue_outbox_complete(&self, op: OutboxCompleteOp) -> OrcherResult<()> {
        let (write, rx) = BatchedWrite::new(WriteOperation::OutboxComplete(op));
        self.queue_write(write, rx).await
    }

    /// Queue a write and wait for completion
    async fn queue_write(
        &self,
        write: BatchedWrite,
        rx: tokio::sync::oneshot::Receiver<BatchWriteResult>,
    ) -> OrcherResult<()> {
        self.writes_queued.fetch_add(1, Ordering::Relaxed);

        self.tx.send(write).await.map_err(|_| {
            OrcherError::internal("Write batcher channel closed - batcher may have shut down")
        })?;

        // Wait for the write to complete
        match rx.await {
            Ok(BatchWriteResult::Success) => Ok(()),
            Ok(BatchWriteResult::Error(msg)) => Err(OrcherError::internal(msg)),
            Err(_) => Err(OrcherError::internal(
                "Write batcher dropped without completing write",
            )),
        }
    }

    /// Queue a write without waiting for completion (fire-and-forget)
    pub async fn queue_fire_and_forget(&self, operation: WriteOperation) -> OrcherResult<()> {
        let (write, _rx) = BatchedWrite::new(operation);
        self.writes_queued.fetch_add(1, Ordering::Relaxed);

        self.tx.send(write).await.map_err(|_| {
            OrcherError::internal("Write batcher channel closed - batcher may have shut down")
        })?;

        Ok(())
    }

    /// Get the number of writes queued
    pub fn writes_queued(&self) -> u64 {
        self.writes_queued.load(Ordering::Relaxed)
    }

    /// Get the number of writes flushed
    pub fn writes_flushed(&self) -> u64 {
        self.writes_flushed.load(Ordering::Relaxed)
    }

    /// Get the number of batches flushed
    pub fn batches_flushed(&self) -> u64 {
        self.batches_flushed.load(Ordering::Relaxed)
    }

    /// Background flush loop
    async fn flush_loop(
        mut rx: mpsc::Receiver<BatchedWrite>,
        pool: PgPool,
        config: WriteBatcherConfig,
        shutdown: Arc<AtomicBool>,
        writes_flushed: Arc<AtomicU64>,
        batches_flushed: Arc<AtomicU64>,
    ) {
        let mut batch: Vec<BatchedWrite> = Vec::with_capacity(config.max_batch_size);
        let mut flush_interval = interval(config.max_wait_time);
        flush_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                // Receive writes from the channel
                Some(write) = rx.recv() => {
                    batch.push(write);

                    // Flush if batch is full
                    if batch.len() >= config.max_batch_size {
                        Self::flush_batch(&pool, &mut batch, &writes_flushed, &batches_flushed).await;
                    }
                }

                // Periodic flush for partial batches
                _ = flush_interval.tick() => {
                    if !batch.is_empty() {
                        Self::flush_batch(&pool, &mut batch, &writes_flushed, &batches_flushed).await;
                    }
                }

                // Check for shutdown
                _ = tokio::time::sleep(Duration::from_millis(100)), if shutdown.load(Ordering::Relaxed) => {
                    // Drain remaining writes
                    while let Ok(write) = rx.try_recv() {
                        batch.push(write);
                    }
                    if !batch.is_empty() {
                        Self::flush_batch(&pool, &mut batch, &writes_flushed, &batches_flushed).await;
                    }
                    info!("Write batcher shutdown complete");
                    break;
                }
            }
        }
    }

    /// Flush a batch of writes to the database
    async fn flush_batch(
        pool: &PgPool,
        batch: &mut Vec<BatchedWrite>,
        writes_flushed: &AtomicU64,
        batches_flushed: &AtomicU64,
    ) {
        if batch.is_empty() {
            return;
        }

        let batch_size = batch.len();
        let flush_start = Instant::now();

        // Group writes by operation type
        let mut journal_writes: Vec<BatchedWrite> = Vec::new();
        let mut state_writes: Vec<BatchedWrite> = Vec::new();
        let mut outbox_insert_writes: Vec<BatchedWrite> = Vec::new();
        let mut outbox_complete_writes: Vec<BatchedWrite> = Vec::new();

        for write in batch.drain(..) {
            match &write.operation {
                WriteOperation::JournalAppend(_) => journal_writes.push(write),
                WriteOperation::StateUpdate(_) => state_writes.push(write),
                WriteOperation::OutboxInsert(_) => outbox_insert_writes.push(write),
                WriteOperation::OutboxComplete(_) => outbox_complete_writes.push(write),
            }
        }

        // Execute each type of batch
        let mut errors = 0u64;

        if !journal_writes.is_empty() {
            if let Err(e) = Self::execute_journal_batch(pool, &mut journal_writes).await {
                error!(error = %e, count = journal_writes.len(), "Failed to flush journal batch");
                errors += journal_writes.len() as u64;
                for write in journal_writes {
                    write.complete(BatchWriteResult::Error(e.to_string()));
                }
            }
        }

        if !state_writes.is_empty() {
            if let Err(e) = Self::execute_state_batch(pool, &mut state_writes).await {
                error!(error = %e, count = state_writes.len(), "Failed to flush state batch");
                errors += state_writes.len() as u64;
                for write in state_writes {
                    write.complete(BatchWriteResult::Error(e.to_string()));
                }
            }
        }

        if !outbox_insert_writes.is_empty() {
            if let Err(e) = Self::execute_outbox_insert_batch(pool, &mut outbox_insert_writes).await
            {
                error!(error = %e, count = outbox_insert_writes.len(), "Failed to flush outbox insert batch");
                errors += outbox_insert_writes.len() as u64;
                for write in outbox_insert_writes {
                    write.complete(BatchWriteResult::Error(e.to_string()));
                }
            }
        }

        if !outbox_complete_writes.is_empty() {
            if let Err(e) =
                Self::execute_outbox_complete_batch(pool, &mut outbox_complete_writes).await
            {
                error!(error = %e, count = outbox_complete_writes.len(), "Failed to flush outbox complete batch");
                errors += outbox_complete_writes.len() as u64;
                for write in outbox_complete_writes {
                    write.complete(BatchWriteResult::Error(e.to_string()));
                }
            }
        }

        let flush_duration = flush_start.elapsed();
        writes_flushed.fetch_add(batch_size as u64 - errors, Ordering::Relaxed);
        batches_flushed.fetch_add(1, Ordering::Relaxed);

        debug!(
            batch_size = batch_size,
            errors = errors,
            duration_ms = flush_duration.as_millis(),
            "Flushed write batch"
        );
    }

    /// Execute a batch of journal appends
    async fn execute_journal_batch(
        pool: &PgPool,
        writes: &mut Vec<BatchedWrite>,
    ) -> OrcherResult<()> {
        if writes.is_empty() {
            return Ok(());
        }

        // Build bulk insert query
        let mut query = String::from(
            "INSERT INTO workflow_execution_journal (workflow_id, execution_id, event_id, event_type, payload, timestamp) VALUES ",
        );

        let mut values: Vec<String> = Vec::with_capacity(writes.len());
        let mut params: Vec<(String, String, i64, String, serde_json::Value, Timestamptz)> =
            Vec::with_capacity(writes.len());

        for (i, write) in writes.iter().enumerate() {
            if let WriteOperation::JournalAppend(op) = &write.operation {
                let offset = i * 6;
                values.push(format!(
                    "(${}, ${}, ${}, ${}, ${}, ${})",
                    offset + 1,
                    offset + 2,
                    offset + 3,
                    offset + 4,
                    offset + 5,
                    offset + 6
                ));
                params.push((
                    op.workflow_id.clone(),
                    op.execution_id.clone(),
                    op.event_id,
                    op.event_type.clone(),
                    op.payload.clone(),
                    op.timestamp,
                ));
            }
        }

        query.push_str(&values.join(", "));
        query.push_str(" ON CONFLICT (workflow_id, execution_id, event_id) DO NOTHING");

        // Build and execute the query
        let mut sqlx_query = sqlx::query(&query);
        for (workflow_id, execution_id, event_id, event_type, payload, timestamp) in params {
            sqlx_query = sqlx_query
                .bind(workflow_id)
                .bind(execution_id)
                .bind(event_id)
                .bind(event_type)
                .bind(payload)
                .bind(timestamp);
        }

        sqlx_query.execute(pool).await.map_err(|e| {
            OrcherError::internal(format!("Failed to execute journal batch: {}", e))
        })?;

        // Notify all writers of success
        for write in writes.drain(..) {
            write.complete(BatchWriteResult::Success);
        }

        Ok(())
    }

    /// Execute a batch of state updates
    async fn execute_state_batch(
        pool: &PgPool,
        writes: &mut Vec<BatchedWrite>,
    ) -> OrcherResult<()> {
        // State updates are more complex - execute individually for now
        // Could be optimized with CASE statements later
        for write in writes.drain(..) {
            if let WriteOperation::StateUpdate(op) = &write.operation {
                let result = Self::execute_single_state_update(pool, op).await;
                match result {
                    Ok(_) => write.complete(BatchWriteResult::Success),
                    Err(e) => write.complete(BatchWriteResult::Error(e.to_string())),
                }
            }
        }
        Ok(())
    }

    /// Execute a single state update
    async fn execute_single_state_update(pool: &PgPool, op: &StateUpdateOp) -> OrcherResult<()> {
        let mut query = String::from("UPDATE workflow_executions SET ");
        let mut updates: Vec<String> = Vec::new();
        let mut param_idx = 1;

        if op.status.is_some() {
            updates.push(format!("status = ${}", param_idx));
            param_idx += 1;
        }
        if op.current_step.is_some() {
            updates.push(format!("current_step = ${}", param_idx));
            param_idx += 1;
        }
        if op.result.is_some() {
            updates.push(format!("result = ${}", param_idx));
            param_idx += 1;
        }
        if op.error.is_some() {
            updates.push(format!("error = ${}", param_idx));
            param_idx += 1;
        }
        if op.completed_at.is_some() {
            updates.push(format!("completed_at = ${}", param_idx));
            param_idx += 1;
        }

        if updates.is_empty() {
            return Ok(()); // Nothing to update
        }

        query.push_str(&updates.join(", "));
        query.push_str(&format!(" WHERE execution_id = ${}", param_idx));

        let mut sqlx_query = sqlx::query(&query);

        if let Some(ref status) = op.status {
            sqlx_query = sqlx_query.bind(status);
        }
        if let Some(ref current_step) = op.current_step {
            sqlx_query = sqlx_query.bind(current_step);
        }
        if let Some(ref result) = op.result {
            sqlx_query = sqlx_query.bind(result);
        }
        if let Some(ref error) = op.error {
            sqlx_query = sqlx_query.bind(error);
        }
        if let Some(completed_at) = op.completed_at {
            sqlx_query = sqlx_query.bind(completed_at);
        }

        sqlx_query = sqlx_query.bind(&op.execution_id);

        sqlx_query
            .execute(pool)
            .await
            .map_err(|e| OrcherError::internal(format!("Failed to execute state update: {}", e)))?;

        Ok(())
    }

    /// Execute a batch of outbox inserts
    async fn execute_outbox_insert_batch(
        pool: &PgPool,
        writes: &mut Vec<BatchedWrite>,
    ) -> OrcherResult<()> {
        if writes.is_empty() {
            return Ok(());
        }

        // Build bulk insert
        let mut query = String::from(
            "INSERT INTO outbox_tasks (id, workflow_id, execution_id, task_type, payload, scheduled_at, status, created_at) VALUES ",
        );

        let mut values: Vec<String> = Vec::with_capacity(writes.len());

        for (i, write) in writes.iter().enumerate() {
            if let WriteOperation::OutboxInsert(_) = &write.operation {
                let offset = i * 8;
                values.push(format!(
                    "(${}, ${}, ${}, ${}, ${}, ${}, 'pending', ${})",
                    offset + 1,
                    offset + 2,
                    offset + 3,
                    offset + 4,
                    offset + 5,
                    offset + 6,
                    offset + 7
                ));
            }
        }

        query.push_str(&values.join(", "));

        let mut sqlx_query = sqlx::query(&query);
        let now = Timestamptz::now();

        for write in writes.iter() {
            if let WriteOperation::OutboxInsert(op) = &write.operation {
                sqlx_query = sqlx_query
                    .bind(op.task_id)
                    .bind(&op.workflow_id)
                    .bind(&op.execution_id)
                    .bind(&op.task_type)
                    .bind(&op.payload)
                    .bind(op.scheduled_at.unwrap_or(now))
                    .bind(now);
            }
        }

        sqlx_query.execute(pool).await.map_err(|e| {
            OrcherError::internal(format!("Failed to execute outbox insert batch: {}", e))
        })?;

        // Notify all writers of success
        for write in writes.drain(..) {
            write.complete(BatchWriteResult::Success);
        }

        Ok(())
    }

    /// Execute a batch of outbox completions
    async fn execute_outbox_complete_batch(
        pool: &PgPool,
        writes: &mut Vec<BatchedWrite>,
    ) -> OrcherResult<()> {
        if writes.is_empty() {
            return Ok(());
        }

        // Separate successes and failures
        let mut success_ids: Vec<uuid::Uuid> = Vec::new();
        let mut failure_writes: Vec<BatchedWrite> = Vec::new();

        for write in writes.drain(..) {
            if let WriteOperation::OutboxComplete(op) = &write.operation {
                if op.success {
                    success_ids.push(op.task_id);
                    // We'll notify after the batch update
                    write.complete(BatchWriteResult::Success);
                } else {
                    failure_writes.push(write);
                }
            }
        }

        // Batch update for successes
        if !success_ids.is_empty() {
            let placeholders: Vec<String> =
                (1..=success_ids.len()).map(|i| format!("${}", i)).collect();
            let query = format!(
                "UPDATE outbox_tasks SET status = 'completed', completed_at = NOW() WHERE id IN ({})",
                placeholders.join(", ")
            );

            let mut sqlx_query = sqlx::query(&query);
            for id in &success_ids {
                sqlx_query = sqlx_query.bind(id);
            }

            sqlx_query.execute(pool).await.map_err(|e| {
                OrcherError::internal(format!("Failed to complete outbox tasks: {}", e))
            })?;
        }

        // Handle failures individually (they have error messages)
        for write in failure_writes {
            if let WriteOperation::OutboxComplete(op) = &write.operation {
                let result = sqlx::query(
                    "UPDATE outbox_tasks SET status = 'failed', error = $1, completed_at = NOW() WHERE id = $2",
                )
                .bind(op.error.as_deref().unwrap_or("Unknown error"))
                .bind(op.task_id)
                .execute(pool)
                .await;

                match result {
                    Ok(_) => write.complete(BatchWriteResult::Success),
                    Err(e) => write.complete(BatchWriteResult::Error(e.to_string())),
                }
            }
        }

        Ok(())
    }
}

/// Handle for managing the write batcher lifecycle
pub struct WriteBatcherHandle {
    shutdown: Arc<AtomicBool>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl WriteBatcherHandle {
    /// Signal the batcher to shutdown gracefully
    pub async fn shutdown(mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        if let Some(handle) = self.handle.take() {
            let _ = handle.await;
        }
    }

    /// Check if the batcher is still running
    pub fn is_running(&self) -> bool {
        self.handle
            .as_ref()
            .map(|h| !h.is_finished())
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_defaults() {
        let config = WriteBatcherConfig::default();
        assert_eq!(config.max_batch_size, 100);
        assert_eq!(config.max_wait_time, Duration::from_millis(10));
    }

    #[test]
    fn test_config_high_throughput() {
        let config = WriteBatcherConfig::high_throughput();
        assert_eq!(config.max_batch_size, 500);
        assert_eq!(config.flush_workers, 4);
    }

    #[test]
    fn test_config_low_latency() {
        let config = WriteBatcherConfig::low_latency();
        assert_eq!(config.max_batch_size, 20);
        assert!(!config.adaptive_batching);
    }
}
