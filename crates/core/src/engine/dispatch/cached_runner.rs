//! Cached workflow runner for replay optimization.

use super::waiters::RoutedTask;
use crate::engine::state::high_perf_cache::HighPerfCache;
use crate::engine::state::types::WorkflowExecutionState;
use orcher_common::error::{OrcherError, OrcherResult};
use orcher_common::types::time::Timestamptz;
use orcher_dispatch::support::async_writer::AsyncWriter;
use orcher_storage::repositories::domains::workflow_execution::WorkflowExecutionRepository;
use orcher_storage::repositories::domains::JournalRepository;
use orcher_storage::schema::types::journal::JournalEntry;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, trace, warn};
use uuid::Uuid;

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for the cached workflow runner
#[derive(Clone, Debug)]
pub struct RunnerConfig {
    /// Enable async writes for workflow continuations
    pub async_writes_enabled: bool,

    /// Force sync writes for all operations (debugging/testing)
    pub force_sync_writes: bool,

    /// Maximum time to wait for state load on cache miss
    pub state_load_timeout: Duration,

    /// Enable metrics collection
    pub metrics_enabled: bool,
}

impl Default for RunnerConfig {
    fn default() -> Self {
        Self {
            async_writes_enabled: true,
            force_sync_writes: false,
            state_load_timeout: Duration::from_secs(5),
            metrics_enabled: true,
        }
    }
}

impl RunnerConfig {
    /// Create a config with all sync writes (for testing/debugging)
    pub fn sync_only() -> Self {
        Self {
            async_writes_enabled: false,
            force_sync_writes: true,
            ..Default::default()
        }
    }

    /// Create a config optimized for low latency
    pub fn low_latency() -> Self {
        Self {
            async_writes_enabled: true,
            force_sync_writes: false,
            state_load_timeout: Duration::from_secs(2),
            metrics_enabled: true,
        }
    }
}

// ============================================================================
// Task Result
// ============================================================================

/// Result of executing a workflow task
#[derive(Debug, Clone)]
pub enum TaskExecutionResult {
    /// Workflow completed successfully
    Completed {
        /// Final result value
        result: serde_json::Value,
        /// Journal entries to persist
        journal_entries: Vec<JournalEntry>,
    },

    /// Workflow is continuing (waiting for timer, activity, etc.)
    Continued {
        /// Commands to execute (schedule activity, start timer, etc.)
        commands: Vec<serde_json::Value>,
        /// Journal entries to persist
        journal_entries: Vec<JournalEntry>,
    },

    /// Workflow failed
    Failed {
        /// Error message
        error: String,
        /// Error details
        details: Option<serde_json::Value>,
        /// Journal entries to persist
        journal_entries: Vec<JournalEntry>,
    },

    /// Workflow was cancelled
    Cancelled {
        /// Reason for cancellation
        reason: Option<String>,
        /// Journal entries to persist
        journal_entries: Vec<JournalEntry>,
    },
}

impl TaskExecutionResult {
    /// Check if this result requires synchronous writes
    pub fn requires_sync_write(&self) -> bool {
        matches!(
            self,
            TaskExecutionResult::Completed { .. }
                | TaskExecutionResult::Failed { .. }
                | TaskExecutionResult::Cancelled { .. }
        )
    }

    /// Get the journal entries from this result
    pub fn journal_entries(&self) -> &[JournalEntry] {
        match self {
            TaskExecutionResult::Completed {
                journal_entries, ..
            } => journal_entries,
            TaskExecutionResult::Continued {
                journal_entries, ..
            } => journal_entries,
            TaskExecutionResult::Failed {
                journal_entries, ..
            } => journal_entries,
            TaskExecutionResult::Cancelled {
                journal_entries, ..
            } => journal_entries,
        }
    }
}

// ============================================================================
// Metrics
// ============================================================================

/// Metrics for the cached workflow runner
#[derive(Debug, Default)]
pub struct RunnerMetrics {
    /// Cache hits (state was already cached)
    pub cache_hits: AtomicU64,
    /// Cache misses (had to load from DB)
    pub cache_misses: AtomicU64,
    /// Async writes performed
    pub async_writes: AtomicU64,
    /// Sync writes performed
    pub sync_writes: AtomicU64,
    /// Write errors
    pub write_errors: AtomicU64,
    /// Total tasks executed
    pub tasks_executed: AtomicU64,
    /// Tasks completed
    pub tasks_completed: AtomicU64,
    /// Tasks failed
    pub tasks_failed: AtomicU64,
}

impl RunnerMetrics {
    /// Create new metrics instance
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a cache hit
    pub fn record_cache_hit(&self) {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a cache miss
    pub fn record_cache_miss(&self) {
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an async write
    pub fn record_async_write(&self) {
        self.async_writes.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a sync write
    pub fn record_sync_write(&self) {
        self.sync_writes.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a write error
    pub fn record_write_error(&self) {
        self.write_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Record task execution
    pub fn record_task_executed(&self) {
        self.tasks_executed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record task completed
    pub fn record_task_completed(&self) {
        self.tasks_completed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record task failed
    pub fn record_task_failed(&self) {
        self.tasks_failed.fetch_add(1, Ordering::Relaxed);
    }

    /// Calculate cache hit rate
    pub fn cache_hit_rate(&self) -> f64 {
        let hits = self.cache_hits.load(Ordering::Relaxed);
        let misses = self.cache_misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }

    /// Calculate async write rate
    pub fn async_write_rate(&self) -> f64 {
        let async_writes = self.async_writes.load(Ordering::Relaxed);
        let sync_writes = self.sync_writes.load(Ordering::Relaxed);
        let total = async_writes + sync_writes;
        if total == 0 {
            0.0
        } else {
            async_writes as f64 / total as f64
        }
    }

    /// Get snapshot of current metrics
    pub fn snapshot(&self) -> RunnerMetricsSnapshot {
        RunnerMetricsSnapshot {
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.cache_misses.load(Ordering::Relaxed),
            async_writes: self.async_writes.load(Ordering::Relaxed),
            sync_writes: self.sync_writes.load(Ordering::Relaxed),
            write_errors: self.write_errors.load(Ordering::Relaxed),
            tasks_executed: self.tasks_executed.load(Ordering::Relaxed),
            tasks_completed: self.tasks_completed.load(Ordering::Relaxed),
            tasks_failed: self.tasks_failed.load(Ordering::Relaxed),
            cache_hit_rate: self.cache_hit_rate(),
            async_write_rate: self.async_write_rate(),
        }
    }
}

/// Snapshot of runner metrics
#[derive(Debug, Clone, Default)]
pub struct RunnerMetricsSnapshot {
    /// Cache hits
    pub cache_hits: u64,
    /// Cache misses
    pub cache_misses: u64,
    /// Async writes
    pub async_writes: u64,
    /// Sync writes
    pub sync_writes: u64,
    /// Write errors
    pub write_errors: u64,
    /// Tasks executed
    pub tasks_executed: u64,
    /// Tasks completed
    pub tasks_completed: u64,
    /// Tasks failed
    pub tasks_failed: u64,
    /// Cache hit rate (0.0 - 1.0)
    pub cache_hit_rate: f64,
    /// Async write rate (0.0 - 1.0)
    pub async_write_rate: f64,
}

// ============================================================================
// Cached Workflow Runner
// ============================================================================

/// Optimized workflow task executor with cached state loading
///
/// Provides an optimized execution path by:
/// - Using cached workflow state when available
/// - Loading and caching state on cache miss
/// - Using async writes for non-critical operations
/// - Performing sync writes only when required
pub struct CachedWorkflowRunner {
    /// High-performance state cache
    state_cache: Arc<HighPerfCache>,

    /// Async writer for non-blocking journal writes
    async_writer: Arc<AsyncWriter>,

    /// Journal repository for synchronous operations
    journal_repo: Arc<JournalRepository>,

    /// Execution repository for state loading on cache miss
    execution_repo: Arc<WorkflowExecutionRepository>,

    /// Configuration
    config: RunnerConfig,

    /// Metrics
    metrics: Arc<RunnerMetrics>,
}

impl CachedWorkflowRunner {
    /// Create a new cached workflow runner
    ///
    /// # Arguments
    ///
    /// * `state_cache` - High-performance cache for workflow state
    /// * `async_writer` - Async writer for non-blocking journal writes
    /// * `journal_repo` - Journal repository for synchronous writes
    /// * `execution_repo` - Repository for loading state on cache miss
    /// * `config` - Runner configuration
    pub fn new(
        state_cache: Arc<HighPerfCache>,
        async_writer: Arc<AsyncWriter>,
        journal_repo: Arc<JournalRepository>,
        execution_repo: Arc<WorkflowExecutionRepository>,
        config: RunnerConfig,
    ) -> Self {
        info!(
            async_writes_enabled = config.async_writes_enabled,
            force_sync_writes = config.force_sync_writes,
            "Creating CachedWorkflowRunner"
        );

        Self {
            state_cache,
            async_writer,
            journal_repo,
            execution_repo,
            config,
            metrics: Arc::new(RunnerMetrics::new()),
        }
    }

    /// Get the runner metrics
    pub fn metrics(&self) -> &Arc<RunnerMetrics> {
        &self.metrics
    }

    /// Get the configuration
    pub fn config(&self) -> &RunnerConfig {
        &self.config
    }

    /// Get workflow state, preferring cached state
    ///
    /// Returns cached state if available, otherwise loads from DB and caches.
    ///
    /// # Arguments
    ///
    /// * `cached_state` - Optional cached state (from sync-match dispatch)
    /// * `workflow_id` - Workflow identifier
    /// * `execution_id` - Execution identifier
    pub async fn get_or_load_state(
        &self,
        cached_state: Option<WorkflowExecutionState>,
        workflow_id: &str,
        execution_id: &str,
    ) -> OrcherResult<WorkflowExecutionState> {
        // Use cached state if provided (sync-match path)
        if let Some(state) = cached_state {
            self.metrics.record_cache_hit();
            trace!(
                workflow_id = %workflow_id,
                execution_id = %execution_id,
                "Using cached state from sync-match"
            );
            return Ok(state);
        }

        // Check the state cache
        if let Some(state) = self.state_cache.get_workflow_state(execution_id).await {
            self.metrics.record_cache_hit();
            debug!(
                workflow_id = %workflow_id,
                execution_id = %execution_id,
                "State found in cache"
            );
            return Ok(state);
        }

        // Cache miss - load from database
        self.metrics.record_cache_miss();
        debug!(
            workflow_id = %workflow_id,
            execution_id = %execution_id,
            "Cache miss - loading state from database"
        );

        let start = Instant::now();

        // Load execution from repository
        let execution = tokio::time::timeout(
            self.config.state_load_timeout,
            self.execution_repo.find_by_execution_id(execution_id),
        )
        .await
        .map_err(|_| {
            OrcherError::internal(format!(
                "Timeout loading workflow state for {}",
                execution_id
            ))
        })?
        .map_err(|e| OrcherError::internal(format!("Failed to load workflow state: {}", e)))?
        .ok_or_else(|| OrcherError::not_found("WorkflowExecution", execution_id))?;

        // Parse workflow_id as UUID, falling back to new UUID if invalid
        let workflow_uuid = execution
            .workflow_id
            .as_ref()
            .and_then(|id| Uuid::parse_str(id).ok())
            .unwrap_or_else(Uuid::new_v4);

        // Parse status string to WorkflowStatus
        let status = match execution.status.as_str() {
            "running" => crate::engine::state::types::WorkflowStatus::Running,
            "completed" => crate::engine::state::types::WorkflowStatus::Completed,
            "failed" => crate::engine::state::types::WorkflowStatus::Failed,
            "cancelled" => crate::engine::state::types::WorkflowStatus::Canceled,
            "pending" => crate::engine::state::types::WorkflowStatus::Running,
            "paused" => crate::engine::state::types::WorkflowStatus::Running,
            _ => crate::engine::state::types::WorkflowStatus::Running,
        };

        // Convert to state
        let state = WorkflowExecutionState {
            workflow_id: workflow_uuid,
            workflow_name: execution.workflow_name.clone(),
            namespace: execution.namespace.clone(),
            status,
            started_at: execution.created_at,
            completed_at: execution.completed_at,
            current_step: execution.current_step.clone(),
            completed_steps: HashSet::new(),
            pending_steps: HashSet::new(),
            pending_timers: HashMap::new(),
            pending_event_gates: HashMap::new(),
            pending_tasks: HashMap::new(),
            last_updated: Timestamptz::now(),
            version: 1,
            input: Some(execution.parameters.clone()),
            result: execution.result.clone(),
            last_error: execution.error.clone(),
            last_journal_event_id: None,
            cached_at: Some(Timestamptz::now()),
        };

        // Cache the loaded state
        self.state_cache
            .set_workflow_state(workflow_id, state.clone())
            .await;

        let duration = start.elapsed();
        debug!(
            workflow_id = %workflow_id,
            execution_id = %execution_id,
            duration_ms = duration.as_millis(),
            "Loaded and cached workflow state"
        );

        Ok(state)
    }

    /// Write journal entries, choosing async or sync based on result type
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - Workflow identifier
    /// * `execution_id` - Execution identifier
    /// * `result` - Task execution result
    pub async fn write_journal_entries(
        &self,
        workflow_id: &str,
        execution_id: &str,
        result: &TaskExecutionResult,
    ) -> OrcherResult<()> {
        let entries = result.journal_entries();
        if entries.is_empty() {
            return Ok(());
        }

        // Determine write strategy
        let use_sync_write = self.config.force_sync_writes
            || result.requires_sync_write()
            || !self.config.async_writes_enabled;

        if use_sync_write {
            self.sync_write_journal(workflow_id, execution_id, entries)
                .await
        } else {
            self.async_write_journal(workflow_id, execution_id, entries)
        }
    }

    /// Perform synchronous journal write
    async fn sync_write_journal(
        &self,
        workflow_id: &str,
        execution_id: &str,
        entries: &[JournalEntry],
    ) -> OrcherResult<()> {
        let start = Instant::now();

        match self.journal_repo.append_batch(entries).await {
            Ok(count) => {
                self.metrics.record_sync_write();
                debug!(
                    workflow_id = %workflow_id,
                    execution_id = %execution_id,
                    entry_count = count,
                    duration_ms = start.elapsed().as_millis(),
                    "Sync write completed"
                );
                Ok(())
            }
            Err(e) => {
                self.metrics.record_write_error();
                error!(
                    workflow_id = %workflow_id,
                    execution_id = %execution_id,
                    error = %e,
                    "Sync write failed"
                );
                Err(OrcherError::internal(format!("Sync write failed: {}", e)))
            }
        }
    }

    /// Perform asynchronous journal write
    fn async_write_journal(
        &self,
        workflow_id: &str,
        execution_id: &str,
        entries: &[JournalEntry],
    ) -> OrcherResult<()> {
        let async_writer = self.async_writer.clone();
        let wf_id = workflow_id.to_string();
        let exec_id = execution_id.to_string();
        let entries_vec = entries.to_vec();
        let metrics = self.metrics.clone();

        // Spawn async write task
        tokio::spawn(async move {
            match async_writer
                .write_journal(wf_id.clone(), exec_id.clone(), entries_vec)
                .await
            {
                Ok(()) => {
                    metrics.record_async_write();
                    trace!(
                        workflow_id = %wf_id,
                        execution_id = %exec_id,
                        "Async write queued"
                    );
                }
                Err(e) => {
                    metrics.record_write_error();
                    warn!(
                        workflow_id = %wf_id,
                        execution_id = %exec_id,
                        error = %e,
                        "Async write failed to queue"
                    );
                }
            }
        });

        Ok(())
    }

    /// Update the state cache with new journal entries
    ///
    /// Call this after task execution to keep the cache up-to-date.
    pub async fn update_cache_with_journal(&self, execution_id: &str, entries: &[JournalEntry]) {
        for entry in entries {
            self.state_cache
                .append_to_journal(execution_id, entry.clone())
                .await;
        }
    }

    /// Invalidate cached state for an execution
    ///
    /// Call this when the execution completes or fails.
    pub async fn invalidate_cache(&self, execution_id: &str) {
        self.state_cache
            .invalidate_workflow_state(execution_id)
            .await;
        self.state_cache.invalidate_journal(execution_id).await;
        debug!(execution_id = %execution_id, "Cache invalidated");
    }

    /// Record task execution completion in metrics
    pub fn record_task_result(&self, result: &TaskExecutionResult) {
        self.metrics.record_task_executed();

        match result {
            TaskExecutionResult::Completed { .. } => {
                self.metrics.record_task_completed();
            }
            TaskExecutionResult::Failed { .. } | TaskExecutionResult::Cancelled { .. } => {
                self.metrics.record_task_failed();
            }
            TaskExecutionResult::Continued { .. } => {
                // Not a terminal state
            }
        }
    }
}

// ============================================================================
// Convenience Functions
// ============================================================================

/// Check if a routed task has cached state
pub fn has_cached_state(task: &RoutedTask) -> bool {
    task.cached_state.is_some()
}

/// Get the workflow ID from a routed task
pub fn get_workflow_id(task: &RoutedTask) -> &str {
    &task.response.workflow_id
}

/// Get the execution ID from a routed task
pub fn get_execution_id(task: &RoutedTask) -> &str {
    &task.response.execution_id
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_runner_config_defaults() {
        let config = RunnerConfig::default();
        assert!(config.async_writes_enabled);
        assert!(!config.force_sync_writes);
        assert!(config.metrics_enabled);
    }

    #[test]
    fn test_runner_config_sync_only() {
        let config = RunnerConfig::sync_only();
        assert!(!config.async_writes_enabled);
        assert!(config.force_sync_writes);
    }

    #[test]
    fn test_task_result_requires_sync_write() {
        let completed = TaskExecutionResult::Completed {
            result: serde_json::json!({}),
            journal_entries: vec![],
        };
        assert!(completed.requires_sync_write());

        let continued = TaskExecutionResult::Continued {
            commands: vec![],
            journal_entries: vec![],
        };
        assert!(!continued.requires_sync_write());

        let failed = TaskExecutionResult::Failed {
            error: "error".to_string(),
            details: None,
            journal_entries: vec![],
        };
        assert!(failed.requires_sync_write());

        let cancelled = TaskExecutionResult::Cancelled {
            reason: None,
            journal_entries: vec![],
        };
        assert!(cancelled.requires_sync_write());
    }

    #[test]
    fn test_metrics_cache_hit_rate() {
        let metrics = RunnerMetrics::new();

        // No operations - 0% hit rate
        assert_eq!(metrics.cache_hit_rate(), 0.0);

        // 3 hits, 1 miss = 75% hit rate
        metrics.record_cache_hit();
        metrics.record_cache_hit();
        metrics.record_cache_hit();
        metrics.record_cache_miss();

        assert!((metrics.cache_hit_rate() - 0.75).abs() < 0.001);
    }

    #[test]
    fn test_metrics_async_write_rate() {
        let metrics = RunnerMetrics::new();

        // No operations - 0% async rate
        assert_eq!(metrics.async_write_rate(), 0.0);

        // 4 async, 1 sync = 80% async rate
        metrics.record_async_write();
        metrics.record_async_write();
        metrics.record_async_write();
        metrics.record_async_write();
        metrics.record_sync_write();

        assert!((metrics.async_write_rate() - 0.80).abs() < 0.001);
    }

    #[test]
    fn test_metrics_snapshot() {
        let metrics = RunnerMetrics::new();
        metrics.record_cache_hit();
        metrics.record_cache_miss();
        metrics.record_async_write();
        metrics.record_sync_write();
        metrics.record_task_executed();
        metrics.record_task_completed();

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.cache_hits, 1);
        assert_eq!(snapshot.cache_misses, 1);
        assert_eq!(snapshot.async_writes, 1);
        assert_eq!(snapshot.sync_writes, 1);
        assert_eq!(snapshot.tasks_executed, 1);
        assert_eq!(snapshot.tasks_completed, 1);
        assert_eq!(snapshot.cache_hit_rate, 0.5);
        assert_eq!(snapshot.async_write_rate, 0.5);
    }
}
