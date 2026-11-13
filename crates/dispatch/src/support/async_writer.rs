//! Write-behind buffer that batches journal entries before flushing to Postgres.

use orcher_common::error::{OrcherError, OrcherResult};
use orcher_monitoring::metrics::{
    record_async_writer_dropped, record_async_writer_enqueue, record_async_writer_error,
    record_async_writer_flush, update_async_writer_buffer_depth,
};
use orcher_storage::schema::types::JournalEntry;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{debug, error, info, trace, warn};

#[derive(Clone, Debug)]
pub struct AsyncWriterConfig {
    /// Channel capacity - senders block when full.
    pub buffer_size: usize,

    /// Triggers a flush when this many ops are buffered.
    pub batch_size: usize,

    pub flush_interval: Duration,

    /// When false, all writes go synchronous.
    pub enabled: bool,

    pub max_retries: u32,
    pub retry_backoff: Duration,
}

impl Default for AsyncWriterConfig {
    fn default() -> Self {
        Self {
            buffer_size: 10_000,
            batch_size: 100,
            flush_interval: Duration::from_millis(100),
            enabled: true,
            max_retries: 3,
            retry_backoff: Duration::from_millis(50),
        }
    }
}

impl AsyncWriterConfig {
    pub fn high_throughput() -> Self {
        Self {
            buffer_size: 50_000,
            batch_size: 500,
            flush_interval: Duration::from_millis(50),
            enabled: true,
            max_retries: 3,
            retry_backoff: Duration::from_millis(25),
        }
    }

    pub fn low_latency() -> Self {
        Self {
            buffer_size: 1_000,
            batch_size: 10,
            flush_interval: Duration::from_millis(10),
            enabled: true,
            max_retries: 2,
            retry_backoff: Duration::from_millis(10),
        }
    }

    pub fn disabled() -> Self {
        Self {
            enabled: false,
            ..Default::default()
        }
    }
}

#[derive(Debug)]
enum WriteOperation {
    JournalEntries {
        workflow_id: String,
        execution_id: String,
        entries: Vec<JournalEntry>,
    },
    Flush {
        done: tokio::sync::oneshot::Sender<()>,
    },
    Shutdown,
}

#[derive(Debug, Default)]
pub struct AsyncWriterMetrics {
    pub enqueued: AtomicU64,
    pub flushed: AtomicU64,
    pub flush_count: AtomicU64,
    pub write_errors: AtomicU64,
    pub dropped: AtomicU64,
    pub buffer_depth: AtomicU64,
}

impl AsyncWriterMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_enqueue(&self, count: u64) {
        self.enqueued.fetch_add(count, Ordering::Relaxed);
        self.buffer_depth.fetch_add(count, Ordering::Relaxed);
        record_async_writer_enqueue(count);
        trace!(count = count, "Async writer: entries enqueued");
    }

    pub fn record_flush(&self, entry_count: u64, duration: Duration) {
        self.flushed.fetch_add(entry_count, Ordering::Relaxed);
        self.flush_count.fetch_add(1, Ordering::Relaxed);
        self.buffer_depth.fetch_sub(entry_count, Ordering::Relaxed);
        record_async_writer_flush(entry_count, duration);
        trace!(
            entry_count = entry_count,
            duration_ms = duration.as_millis(),
            "Async writer: flush completed"
        );
    }

    pub fn record_error(&self) {
        self.write_errors.fetch_add(1, Ordering::Relaxed);
        record_async_writer_error();
        trace!("Async writer: write error recorded");
    }

    pub fn record_dropped(&self, count: u64) {
        self.dropped.fetch_add(count, Ordering::Relaxed);
        record_async_writer_dropped(count);
        warn!(count = count, "Async writer: entries dropped");
    }

    pub fn update_buffer_gauge(&self) {
        let depth = self.buffer_depth.load(Ordering::Relaxed);
        update_async_writer_buffer_depth(depth);
        trace!(depth = depth, "Async writer: buffer depth updated");
    }

    pub fn snapshot(&self) -> AsyncWriterStats {
        AsyncWriterStats {
            enqueued: self.enqueued.load(Ordering::Relaxed),
            flushed: self.flushed.load(Ordering::Relaxed),
            flush_count: self.flush_count.load(Ordering::Relaxed),
            write_errors: self.write_errors.load(Ordering::Relaxed),
            dropped: self.dropped.load(Ordering::Relaxed),
            buffer_depth: self.buffer_depth.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct AsyncWriterStats {
    pub enqueued: u64,
    pub flushed: u64,
    pub flush_count: u64,
    pub write_errors: u64,
    pub dropped: u64,
    pub buffer_depth: u64,
}

#[async_trait::async_trait]
pub trait JournalWriter: Send + Sync {
    async fn append_batch(&self, entries: &[JournalEntry]) -> OrcherResult<usize>;
}

#[async_trait::async_trait]
impl JournalWriter for orcher_storage::repositories::domains::JournalRepository {
    async fn append_batch(&self, entries: &[JournalEntry]) -> OrcherResult<usize> {
        // Convert orcher_storage JournalEntry to the storage crate's type
        // Since both use the same underlying type (re-exported), this is direct
        self.append_batch(entries).await
    }
}

/// Batches journal writes and flushes them in the background.
pub struct AsyncWriter {
    sender: mpsc::Sender<WriteOperation>,
    metrics: Arc<AsyncWriterMetrics>,
    config: AsyncWriterConfig,
    is_shutdown: Arc<AtomicBool>,
    task_handle: Option<tokio::task::JoinHandle<()>>,
}

impl AsyncWriter {
    pub fn new<J: JournalWriter + 'static>(
        journal_writer: Arc<J>,
        config: AsyncWriterConfig,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(config.buffer_size);
        let metrics = Arc::new(AsyncWriterMetrics::new());
        let is_shutdown = Arc::new(AtomicBool::new(false));

        // Start background writer task
        let task_handle = if config.enabled {
            let writer_loop =
                WriterLoop::new(receiver, journal_writer, config.clone(), metrics.clone());
            Some(tokio::spawn(writer_loop.run()))
        } else {
            // Drop receiver immediately if disabled
            drop(receiver);
            None
        };

        Self {
            sender,
            metrics,
            config,
            is_shutdown,
            task_handle,
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.config.enabled && !self.is_shutdown.load(Ordering::Relaxed)
    }

    /// Queues entries and returns immediately - errors if the buffer is full or writer is shut down.
    pub async fn write_journal(
        &self,
        workflow_id: String,
        execution_id: String,
        entries: Vec<JournalEntry>,
    ) -> OrcherResult<()> {
        if !self.is_enabled() {
            return Err(OrcherError::internal(
                "AsyncWriter is disabled or shut down",
            ));
        }

        if entries.is_empty() {
            return Ok(());
        }

        let entry_count = entries.len() as u64;

        let op = WriteOperation::JournalEntries {
            workflow_id,
            execution_id,
            entries,
        };

        match self.sender.try_send(op) {
            Ok(()) => {
                self.metrics.record_enqueue(entry_count);
                trace!(
                    count = entry_count,
                    "Queued journal entries for async write"
                );
                Ok(())
            }
            Err(mpsc::error::TrySendError::Full(op)) => {
                // Buffer is full - try with timeout
                warn!("AsyncWriter buffer full, waiting for space");

                match tokio::time::timeout(Duration::from_millis(100), self.sender.send(op)).await {
                    Ok(Ok(())) => {
                        self.metrics.record_enqueue(entry_count);
                        Ok(())
                    }
                    Ok(Err(_)) => {
                        self.metrics.record_dropped(entry_count);
                        Err(OrcherError::internal("AsyncWriter channel closed"))
                    }
                    Err(_) => {
                        self.metrics.record_dropped(entry_count);
                        Err(OrcherError::internal(
                            "AsyncWriter buffer full, entries dropped",
                        ))
                    }
                }
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.metrics.record_dropped(entry_count);
                Err(OrcherError::internal("AsyncWriter has been shut down"))
            }
        }
    }

    /// Blocks until all buffered writes are flushed.
    pub async fn flush(&self) -> OrcherResult<()> {
        if !self.is_enabled() {
            return Ok(());
        }

        let (done_tx, done_rx) = tokio::sync::oneshot::channel();

        self.sender
            .send(WriteOperation::Flush { done: done_tx })
            .await
            .map_err(|_| OrcherError::internal("AsyncWriter channel closed"))?;

        done_rx
            .await
            .map_err(|_| OrcherError::internal("Flush completion signal lost"))?;

        Ok(())
    }

    /// Flushes pending writes and stops the background task.
    pub async fn shutdown(&mut self) {
        if self.is_shutdown.swap(true, Ordering::SeqCst) {
            // Already shut down
            return;
        }

        info!("Shutting down AsyncWriter");

        // Send shutdown signal
        if self.sender.send(WriteOperation::Shutdown).await.is_err() {
            warn!("AsyncWriter channel already closed");
        }

        // Wait for background task to complete
        if let Some(handle) = self.task_handle.take() {
            match tokio::time::timeout(Duration::from_secs(5), handle).await {
                Ok(Ok(())) => info!("AsyncWriter shutdown complete"),
                Ok(Err(e)) => error!("AsyncWriter task panicked: {}", e),
                Err(_) => warn!("AsyncWriter shutdown timed out"),
            }
        }
    }

    pub fn metrics(&self) -> &Arc<AsyncWriterMetrics> {
        &self.metrics
    }

    pub fn stats(&self) -> AsyncWriterStats {
        self.metrics.snapshot()
    }

    pub fn config(&self) -> &AsyncWriterConfig {
        &self.config
    }
}

impl Drop for AsyncWriter {
    fn drop(&mut self) {
        if !self.is_shutdown.load(Ordering::Relaxed) {
            warn!("AsyncWriter dropped without explicit shutdown - buffered writes may be lost");
        }
    }
}

struct WriterLoop<J: JournalWriter> {
    receiver: mpsc::Receiver<WriteOperation>,
    journal_writer: Arc<J>,
    config: AsyncWriterConfig,
    metrics: Arc<AsyncWriterMetrics>,
    buffer: Vec<WriteOperation>,
    last_flush: Instant,
}

impl<J: JournalWriter + 'static> WriterLoop<J> {
    fn new(
        receiver: mpsc::Receiver<WriteOperation>,
        journal_writer: Arc<J>,
        config: AsyncWriterConfig,
        metrics: Arc<AsyncWriterMetrics>,
    ) -> Self {
        Self {
            receiver,
            journal_writer,
            config,
            metrics,
            buffer: Vec::with_capacity(128),
            last_flush: Instant::now(),
        }
    }

    async fn run(mut self) {
        info!("AsyncWriter background task started");

        loop {
            // Calculate timeout until next flush
            let time_since_flush = self.last_flush.elapsed();
            let timeout = if time_since_flush >= self.config.flush_interval {
                Duration::ZERO
            } else {
                self.config.flush_interval - time_since_flush
            };

            // Try to receive with timeout
            let recv_result = if timeout.is_zero() {
                // Time to flush, just try_recv
                match self.receiver.try_recv() {
                    Ok(op) => Some(op),
                    Err(mpsc::error::TryRecvError::Empty) => None,
                    Err(mpsc::error::TryRecvError::Disconnected) => {
                        // Channel closed, flush remaining and exit
                        self.flush_all().await;
                        break;
                    }
                }
            } else {
                match tokio::time::timeout(timeout, self.receiver.recv()).await {
                    Ok(Some(op)) => Some(op),
                    Ok(None) => {
                        // Channel closed
                        self.flush_all().await;
                        break;
                    }
                    Err(_) => None, // Timeout - time to flush
                }
            };

            // Process received operation
            if let Some(op) = recv_result {
                match op {
                    WriteOperation::Shutdown => {
                        info!("AsyncWriter received shutdown signal");
                        self.flush_all().await;
                        break;
                    }
                    WriteOperation::Flush { done } => {
                        self.flush_all().await;
                        let _ = done.send(());
                    }
                    write_op => {
                        self.buffer.push(write_op);

                        // Flush if buffer is full
                        if self.buffer.len() >= self.config.batch_size {
                            self.flush_all().await;
                        }
                    }
                }
            } else if !self.buffer.is_empty() {
                // Timeout - flush partial buffer
                self.flush_all().await;
            }

            // Update metrics gauge
            self.metrics.update_buffer_gauge();
        }

        info!("AsyncWriter background task stopped");
    }

    async fn flush_all(&mut self) {
        if self.buffer.is_empty() {
            self.last_flush = Instant::now();
            return;
        }

        let start = Instant::now();
        let ops: Vec<_> = std::mem::take(&mut self.buffer);

        // Group journal entries by workflow for efficient batch writes
        let mut journal_batches: HashMap<(String, String), Vec<JournalEntry>> = HashMap::new();

        for op in ops {
            match op {
                WriteOperation::JournalEntries {
                    workflow_id,
                    execution_id,
                    entries,
                } => {
                    journal_batches
                        .entry((workflow_id, execution_id))
                        .or_default()
                        .extend(entries);
                }
                WriteOperation::Flush { done } => {
                    // Should not happen in buffer, but handle gracefully
                    let _ = done.send(());
                }
                WriteOperation::Shutdown => {
                    // Should not happen in buffer
                }
            }
        }

        // Write batched journal entries
        let mut total_entries = 0u64;
        for ((workflow_id, execution_id), entries) in journal_batches {
            let entry_count = entries.len();
            total_entries += entry_count as u64;

            if let Err(e) = self.write_batch_with_retry(&entries).await {
                error!(
                    workflow_id = %workflow_id,
                    execution_id = %execution_id,
                    entry_count = entry_count,
                    error = %e,
                    "Failed to batch write journal entries after retries"
                );
                self.metrics.record_error();
            }
        }

        let duration = start.elapsed();
        if total_entries > 0 {
            self.metrics.record_flush(total_entries, duration);
            debug!(
                entry_count = total_entries,
                duration_ms = duration.as_millis(),
                "Flushed journal entries"
            );
        }

        self.last_flush = Instant::now();
    }

    async fn write_batch_with_retry(&self, entries: &[JournalEntry]) -> OrcherResult<usize> {
        let mut last_error = None;

        for attempt in 0..=self.config.max_retries {
            match self.journal_writer.append_batch(entries).await {
                Ok(count) => return Ok(count),
                Err(e) => {
                    warn!(
                        attempt = attempt + 1,
                        max_retries = self.config.max_retries,
                        error = %e,
                        "Batch write failed, retrying"
                    );
                    last_error = Some(e);

                    if attempt < self.config.max_retries {
                        tokio::time::sleep(self.config.retry_backoff).await;
                    }
                }
            }
        }

        Err(last_error.unwrap_or_else(|| OrcherError::internal("Unknown write error")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Mutex;
    use tokio::time::sleep;

    /// Mock journal writer for testing
    struct MockJournalWriter {
        writes: Mutex<Vec<Vec<JournalEntry>>>,
        should_fail: AtomicBool,
        fail_count: AtomicU64,
    }

    impl MockJournalWriter {
        fn new() -> Self {
            Self {
                writes: Mutex::new(Vec::new()),
                should_fail: AtomicBool::new(false),
                fail_count: AtomicU64::new(0),
            }
        }

        fn set_should_fail(&self, should_fail: bool) {
            self.should_fail.store(should_fail, Ordering::Relaxed);
        }

        fn get_writes(&self) -> Vec<Vec<JournalEntry>> {
            self.writes.lock().unwrap().clone()
        }

        fn total_entries_written(&self) -> usize {
            self.writes.lock().unwrap().iter().map(|v| v.len()).sum()
        }
    }

    #[async_trait::async_trait]
    impl JournalWriter for MockJournalWriter {
        async fn append_batch(&self, entries: &[JournalEntry]) -> OrcherResult<usize> {
            if self.should_fail.load(Ordering::Relaxed) {
                self.fail_count.fetch_add(1, Ordering::Relaxed);
                return Err(OrcherError::internal("Mock failure"));
            }
            let mut writes = self.writes.lock().unwrap();
            writes.push(entries.to_vec());
            Ok(entries.len())
        }
    }

    fn create_test_entry(workflow_id: &str, event_id: i64) -> JournalEntry {
        JournalEntry::new(
            workflow_id.to_string(),
            "exec-1".to_string(),
            event_id,
            "TestEvent".to_string(),
            serde_json::json!({"test": true}),
        )
    }

    #[tokio::test]
    async fn test_async_writer_basic() {
        let mock_writer = Arc::new(MockJournalWriter::new());
        let config = AsyncWriterConfig {
            batch_size: 5,
            flush_interval: Duration::from_millis(50),
            ..Default::default()
        };

        let mut writer = AsyncWriter::new(mock_writer.clone(), config);

        // Write some entries
        let entries = vec![create_test_entry("wf-1", 1), create_test_entry("wf-1", 2)];
        writer
            .write_journal("wf-1".to_string(), "exec-1".to_string(), entries)
            .await
            .unwrap();

        // Force flush
        writer.flush().await.unwrap();

        // Verify writes
        assert_eq!(mock_writer.total_entries_written(), 2);

        writer.shutdown().await;
    }

    #[tokio::test]
    async fn test_async_writer_batch_flush() {
        let mock_writer = Arc::new(MockJournalWriter::new());
        let config = AsyncWriterConfig {
            batch_size: 3,
            flush_interval: Duration::from_secs(10), // Long interval so batch triggers
            ..Default::default()
        };

        let mut writer = AsyncWriter::new(mock_writer.clone(), config);

        // Write 3 entries (batch_size) - should auto-flush
        for i in 1..=3 {
            let entries = vec![create_test_entry("wf-1", i)];
            writer
                .write_journal("wf-1".to_string(), "exec-1".to_string(), entries)
                .await
                .unwrap();
        }

        // Give background task time to flush
        sleep(Duration::from_millis(50)).await;

        // Should have flushed
        assert_eq!(mock_writer.total_entries_written(), 3);

        writer.shutdown().await;
    }

    #[tokio::test]
    async fn test_async_writer_time_flush() {
        let mock_writer = Arc::new(MockJournalWriter::new());
        let config = AsyncWriterConfig {
            batch_size: 100, // Large batch so time triggers first
            flush_interval: Duration::from_millis(50),
            ..Default::default()
        };

        let mut writer = AsyncWriter::new(mock_writer.clone(), config);

        // Write 1 entry
        let entries = vec![create_test_entry("wf-1", 1)];
        writer
            .write_journal("wf-1".to_string(), "exec-1".to_string(), entries)
            .await
            .unwrap();

        // Wait for time-based flush
        sleep(Duration::from_millis(100)).await;

        // Should have flushed
        assert_eq!(mock_writer.total_entries_written(), 1);

        writer.shutdown().await;
    }

    #[tokio::test]
    async fn test_async_writer_shutdown_flushes() {
        let mock_writer = Arc::new(MockJournalWriter::new());
        let config = AsyncWriterConfig {
            batch_size: 100,
            flush_interval: Duration::from_secs(10),
            ..Default::default()
        };

        let mut writer = AsyncWriter::new(mock_writer.clone(), config);

        // Write entries
        for i in 1..=5 {
            let entries = vec![create_test_entry("wf-1", i)];
            writer
                .write_journal("wf-1".to_string(), "exec-1".to_string(), entries)
                .await
                .unwrap();
        }

        // Shutdown should flush
        writer.shutdown().await;

        // Verify all entries written
        assert_eq!(mock_writer.total_entries_written(), 5);
    }

    #[tokio::test]
    async fn test_async_writer_retry_on_failure() {
        let mock_writer = Arc::new(MockJournalWriter::new());
        mock_writer.set_should_fail(true);

        let config = AsyncWriterConfig {
            batch_size: 1,
            flush_interval: Duration::from_millis(10),
            max_retries: 2,
            retry_backoff: Duration::from_millis(10),
            ..Default::default()
        };

        let mut writer = AsyncWriter::new(mock_writer.clone(), config);

        // Write entry
        let entries = vec![create_test_entry("wf-1", 1)];
        writer
            .write_journal("wf-1".to_string(), "exec-1".to_string(), entries)
            .await
            .unwrap();

        // Wait for retries to complete (1 attempt + 2 retries * 10ms backoff = ~30ms)
        // Poll until expected count or timeout
        let deadline = Instant::now() + Duration::from_secs(2);
        while mock_writer.fail_count.load(Ordering::Relaxed) < 3 {
            if Instant::now() > deadline {
                panic!(
                    "Timed out waiting for retries. fail_count = {}",
                    mock_writer.fail_count.load(Ordering::Relaxed)
                );
            }
            sleep(Duration::from_millis(10)).await;
        }

        // Should have attempted 3 times (1 + 2 retries)
        assert_eq!(mock_writer.fail_count.load(Ordering::Relaxed), 3);

        writer.shutdown().await;
    }

    #[tokio::test]
    async fn test_async_writer_disabled() {
        let mock_writer = Arc::new(MockJournalWriter::new());
        let config = AsyncWriterConfig::disabled();

        let writer = AsyncWriter::new(mock_writer.clone(), config);

        assert!(!writer.is_enabled());

        // Write should fail
        let entries = vec![create_test_entry("wf-1", 1)];
        let result = writer
            .write_journal("wf-1".to_string(), "exec-1".to_string(), entries)
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_async_writer_metrics() {
        let mock_writer = Arc::new(MockJournalWriter::new());
        let config = AsyncWriterConfig {
            batch_size: 10,
            flush_interval: Duration::from_millis(50),
            ..Default::default()
        };

        let mut writer = AsyncWriter::new(mock_writer.clone(), config);

        // Write entries
        let entries = vec![
            create_test_entry("wf-1", 1),
            create_test_entry("wf-1", 2),
            create_test_entry("wf-1", 3),
        ];
        writer
            .write_journal("wf-1".to_string(), "exec-1".to_string(), entries)
            .await
            .unwrap();

        // Flush
        writer.flush().await.unwrap();

        let stats = writer.stats();
        assert_eq!(stats.enqueued, 3);
        assert_eq!(stats.flushed, 3);
        assert_eq!(stats.flush_count, 1);
        assert_eq!(stats.write_errors, 0);

        writer.shutdown().await;
    }

    #[tokio::test]
    async fn test_async_writer_grouping() {
        let mock_writer = Arc::new(MockJournalWriter::new());
        let config = AsyncWriterConfig {
            batch_size: 10,
            flush_interval: Duration::from_secs(10),
            ..Default::default()
        };

        let mut writer = AsyncWriter::new(mock_writer.clone(), config);

        // Write entries for different workflows
        writer
            .write_journal(
                "wf-1".to_string(),
                "exec-1".to_string(),
                vec![create_test_entry("wf-1", 1)],
            )
            .await
            .unwrap();

        writer
            .write_journal(
                "wf-2".to_string(),
                "exec-2".to_string(),
                vec![create_test_entry("wf-2", 1)],
            )
            .await
            .unwrap();

        writer
            .write_journal(
                "wf-1".to_string(),
                "exec-1".to_string(),
                vec![create_test_entry("wf-1", 2)],
            )
            .await
            .unwrap();

        // Flush
        writer.flush().await.unwrap();

        // Entries should be grouped by workflow
        let writes = mock_writer.get_writes();
        assert_eq!(writes.len(), 2); // Two batches (one per workflow)

        let total_entries: usize = writes.iter().map(|v| v.len()).sum();
        assert_eq!(total_entries, 3);

        writer.shutdown().await;
    }
}
