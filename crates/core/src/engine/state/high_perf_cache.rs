//! Moka-based high-performance state cache.

use crate::engine::state::types::WorkflowExecutionState;
use moka::future::Cache;
use orcher_common::error::OrcherResult;
use orcher_common::types::time::Timestamptz;
use orcher_monitoring::metrics::storage::{
    record_journal_cache_hit, record_journal_cache_miss, record_journal_incremental_load,
};
use orcher_storage::schema::types::journal::JournalEntry;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info};

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for the high-performance cache
#[derive(Debug, Clone)]
pub struct HighPerfCacheConfig {
    /// Maximum number of workflow states to cache
    pub max_workflow_states: u64,

    /// TTL for workflow state entries (seconds)
    pub workflow_state_ttl_secs: u64,

    /// Maximum number of journal entry lists to cache
    pub max_journal_entries: u64,

    /// TTL for journal entry cache (seconds)
    pub journal_cache_ttl_secs: u64,

    /// Maximum number of claim info entries to cache
    pub max_claims: u64,

    /// TTL for claim cache (seconds)
    pub claim_cache_ttl_secs: u64,

    /// Enable metrics collection
    pub enable_metrics: bool,
}

impl Default for HighPerfCacheConfig {
    fn default() -> Self {
        Self {
            max_workflow_states: 10_000,  // 10K workflows
            workflow_state_ttl_secs: 300, // 5 minutes
            max_journal_entries: 5_000,   // 5K journal lists
            journal_cache_ttl_secs: 60,   // 1 minute
            max_claims: 50_000,           // 50K claims
            claim_cache_ttl_secs: 120,    // 2 minutes (should be less than claim_timeout)
            enable_metrics: true,
        }
    }
}

// ============================================================================
// Claim Information
// ============================================================================

/// Cached claim information for an execution
#[derive(Debug, Clone)]
pub struct ClaimInfo {
    /// Identity of the service holding the claim
    pub claimed_by: String,

    /// When the claim was acquired
    pub claimed_at: Timestamptz,

    /// When the claim expires
    pub claim_timeout_at: Timestamptz,
}

impl ClaimInfo {
    /// Create new claim info
    pub fn new(claimed_by: String, claimed_at: Timestamptz, claim_timeout_at: Timestamptz) -> Self {
        Self {
            claimed_by,
            claimed_at,
            claim_timeout_at,
        }
    }

    /// Check if the claim is expired
    pub fn is_expired(&self) -> bool {
        Timestamptz::now() > self.claim_timeout_at
    }

    /// Check if claimed by specific identity
    pub fn is_claimed_by(&self, identity: &str) -> bool {
        self.claimed_by == identity
    }
}

// ============================================================================
// Cache Metrics
// ============================================================================

/// Metrics for cache operations
#[derive(Debug, Default)]
pub struct CacheMetrics {
    /// Workflow state cache hits
    pub workflow_hits: AtomicU64,
    /// Workflow state cache misses
    pub workflow_misses: AtomicU64,
    /// Journal cache hits
    pub journal_hits: AtomicU64,
    /// Journal cache misses
    pub journal_misses: AtomicU64,
    /// Claim cache hits
    pub claim_hits: AtomicU64,
    /// Claim cache misses
    pub claim_misses: AtomicU64,
    /// Total invalidations
    pub invalidations: AtomicU64,
}

impl CacheMetrics {
    /// Record a workflow cache hit
    pub fn record_workflow_hit(&self) {
        self.workflow_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a workflow cache miss
    pub fn record_workflow_miss(&self) {
        self.workflow_misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a journal cache hit
    pub fn record_journal_hit(&self) {
        self.journal_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a journal cache miss
    pub fn record_journal_miss(&self) {
        self.journal_misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a claim cache hit
    pub fn record_claim_hit(&self) {
        self.claim_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a claim cache miss
    pub fn record_claim_miss(&self) {
        self.claim_misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an invalidation
    pub fn record_invalidation(&self) {
        self.invalidations.fetch_add(1, Ordering::Relaxed);
    }

    /// Get workflow cache hit rate
    pub fn workflow_hit_rate(&self) -> f64 {
        let hits = self.workflow_hits.load(Ordering::Relaxed);
        let misses = self.workflow_misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }

    /// Get journal cache hit rate
    pub fn journal_hit_rate(&self) -> f64 {
        let hits = self.journal_hits.load(Ordering::Relaxed);
        let misses = self.journal_misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }

    /// Get claim cache hit rate
    pub fn claim_hit_rate(&self) -> f64 {
        let hits = self.claim_hits.load(Ordering::Relaxed);
        let misses = self.claim_misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }
}

/// Snapshot of cache statistics
#[derive(Debug, Clone)]
pub struct CacheStatsSnapshot {
    /// Number of workflow state entries in cache
    pub workflow_entries: u64,
    /// Workflow cache hit rate (0.0 - 1.0)
    pub workflow_hit_rate: f64,
    /// Total workflow cache hits
    pub workflow_hits: u64,
    /// Total workflow cache misses
    pub workflow_misses: u64,

    /// Number of journal entries in cache
    pub journal_entries: u64,
    /// Journal cache hit rate (0.0 - 1.0)
    pub journal_hit_rate: f64,
    /// Total journal cache hits
    pub journal_hits: u64,
    /// Total journal cache misses
    pub journal_misses: u64,

    /// Number of claim entries in cache
    pub claim_entries: u64,
    /// Claim cache hit rate (0.0 - 1.0)
    pub claim_hit_rate: f64,
    /// Total claim cache hits
    pub claim_hits: u64,
    /// Total claim cache misses
    pub claim_misses: u64,

    /// Total cache invalidations across all caches
    pub total_invalidations: u64,
}

// ============================================================================
// High-Performance Cache
// ============================================================================

/// High-performance async cache for workflow data
///
/// Uses moka for lock-free concurrent access with automatic TTL expiration.
/// Designed for high-throughput workflow orchestration workloads.
///
/// # Example
///
/// ```rust,ignore
/// use orcher_core::coordination::state::high_perf_cache::{HighPerfCache, HighPerfCacheConfig};
///
/// let cache = HighPerfCache::new(HighPerfCacheConfig::default());
///
/// // Cache workflow state
/// cache.set_workflow_state("exec-123", state).await;
///
/// // Get with loader (read-through pattern)
/// let state = cache.get_or_load_workflow_state("exec-123", || async {
///     repo.find_by_execution_id("exec-123").await
/// }).await?;
/// ```
pub struct HighPerfCache {
    /// Workflow state cache (key: execution_id)
    workflow_states: Cache<String, WorkflowExecutionState>,

    /// Journal entries cache (key: execution_id, value: Vec<JournalEntry>)
    journal_cache: Cache<String, Vec<JournalEntry>>,

    /// Claim information cache (key: execution_id)
    claim_cache: Cache<String, ClaimInfo>,

    /// Configuration
    config: HighPerfCacheConfig,

    /// Metrics
    metrics: Arc<CacheMetrics>,
}

impl HighPerfCache {
    /// Create a new high-performance cache
    pub fn new(config: HighPerfCacheConfig) -> Self {
        info!(
            max_workflow_states = config.max_workflow_states,
            workflow_ttl_secs = config.workflow_state_ttl_secs,
            max_journal_entries = config.max_journal_entries,
            journal_ttl_secs = config.journal_cache_ttl_secs,
            max_claims = config.max_claims,
            claim_ttl_secs = config.claim_cache_ttl_secs,
            "Initializing high-performance workflow cache"
        );

        let workflow_states = Cache::builder()
            .max_capacity(config.max_workflow_states)
            .time_to_live(Duration::from_secs(config.workflow_state_ttl_secs))
            .time_to_idle(Duration::from_secs(config.workflow_state_ttl_secs / 2))
            .build();

        let journal_cache = Cache::builder()
            .max_capacity(config.max_journal_entries)
            .time_to_live(Duration::from_secs(config.journal_cache_ttl_secs))
            .time_to_idle(Duration::from_secs(config.journal_cache_ttl_secs / 2))
            .build();

        let claim_cache = Cache::builder()
            .max_capacity(config.max_claims)
            .time_to_live(Duration::from_secs(config.claim_cache_ttl_secs))
            .time_to_idle(Duration::from_secs(config.claim_cache_ttl_secs / 2))
            .build();

        Self {
            workflow_states,
            journal_cache,
            claim_cache,
            config,
            metrics: Arc::new(CacheMetrics::default()),
        }
    }

    /// Create with default configuration
    pub fn with_defaults() -> Self {
        Self::new(HighPerfCacheConfig::default())
    }

    // ========================================================================
    // Workflow State Cache Operations
    // ========================================================================

    /// Get a workflow state from cache
    pub async fn get_workflow_state(&self, execution_id: &str) -> Option<WorkflowExecutionState> {
        let result = self.workflow_states.get(execution_id).await;
        if result.is_some() {
            self.metrics.record_workflow_hit();
            debug!(execution_id = %execution_id, "Workflow state cache hit");
        } else {
            self.metrics.record_workflow_miss();
            debug!(execution_id = %execution_id, "Workflow state cache miss");
        }
        result
    }

    /// Set a workflow state in cache
    pub async fn set_workflow_state(&self, execution_id: &str, state: WorkflowExecutionState) {
        self.workflow_states
            .insert(execution_id.to_string(), state)
            .await;
        debug!(execution_id = %execution_id, "Workflow state cached");
    }

    /// Get or load workflow state (read-through pattern)
    ///
    /// If the state is not in cache, loads it using the provided loader function.
    /// The loaded value is automatically cached.
    pub async fn get_or_load_workflow_state<F, Fut>(
        &self,
        execution_id: &str,
        loader: F,
    ) -> OrcherResult<Option<WorkflowExecutionState>>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = OrcherResult<Option<WorkflowExecutionState>>>,
    {
        // Check cache first
        if let Some(state) = self.get_workflow_state(execution_id).await {
            return Ok(Some(state));
        }

        // Load from source
        let state = loader().await?;

        // Cache if found
        if let Some(ref s) = state {
            self.set_workflow_state(execution_id, s.clone()).await;
        }

        Ok(state)
    }

    /// Invalidate a workflow state
    pub async fn invalidate_workflow_state(&self, execution_id: &str) {
        self.workflow_states.invalidate(execution_id).await;
        self.metrics.record_invalidation();
        debug!(execution_id = %execution_id, "Workflow state invalidated");
    }

    // ========================================================================
    // Journal Cache Operations
    // ========================================================================

    /// Get journal entries from cache
    pub async fn get_journal(&self, execution_id: &str) -> Option<Vec<JournalEntry>> {
        let result = self.journal_cache.get(execution_id).await;
        if result.is_some() {
            self.metrics.record_journal_hit();
            debug!(execution_id = %execution_id, "Journal cache hit");
        } else {
            self.metrics.record_journal_miss();
            debug!(execution_id = %execution_id, "Journal cache miss");
        }
        result
    }

    /// Set journal entries in cache
    pub async fn set_journal(&self, execution_id: &str, entries: Vec<JournalEntry>) {
        let count = entries.len();
        self.journal_cache
            .insert(execution_id.to_string(), entries)
            .await;
        debug!(execution_id = %execution_id, entry_count = count, "Journal entries cached");
    }

    /// Get or load journal entries (read-through pattern)
    pub async fn get_or_load_journal<F, Fut>(
        &self,
        execution_id: &str,
        loader: F,
    ) -> OrcherResult<Vec<JournalEntry>>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = OrcherResult<Vec<JournalEntry>>>,
    {
        // Check cache first
        if let Some(entries) = self.get_journal(execution_id).await {
            return Ok(entries);
        }

        // Load from source
        let entries = loader().await?;

        // Cache the result
        if !entries.is_empty() {
            self.set_journal(execution_id, entries.clone()).await;
        }

        Ok(entries)
    }

    /// Append to cached journal (optimistic update)
    ///
    /// If the journal is in cache, appends the new entry.
    /// If not in cache, does nothing (will be loaded fresh next time).
    pub async fn append_to_journal(&self, execution_id: &str, entry: JournalEntry) {
        if let Some(mut entries) = self.journal_cache.get(execution_id).await {
            entries.push(entry);
            self.journal_cache
                .insert(execution_id.to_string(), entries)
                .await;
            debug!(execution_id = %execution_id, "Appended entry to cached journal");
        }
    }

    /// Append multiple entries to cached journal (for incremental loading)
    ///
    /// If the journal is in cache, appends the new entries.
    /// If not in cache, caches the new entries as the initial set.
    ///
    /// # Arguments
    ///
    /// * `execution_id` - The execution identifier
    /// * `new_entries` - New journal entries to append (should be ordered by event_id)
    ///
    /// # Returns
    ///
    /// The total number of entries now in cache
    pub async fn append_entries_to_journal(
        &self,
        execution_id: &str,
        new_entries: Vec<JournalEntry>,
    ) -> usize {
        if new_entries.is_empty() {
            // Nothing to append, return current count
            return self
                .journal_cache
                .get(execution_id)
                .await
                .map(|e| e.len())
                .unwrap_or(0);
        }

        let total_count = if let Some(mut entries) = self.journal_cache.get(execution_id).await {
            entries.extend(new_entries);
            let count = entries.len();
            self.journal_cache
                .insert(execution_id.to_string(), entries)
                .await;
            count
        } else {
            // No existing cache, start with new entries
            let count = new_entries.len();
            self.journal_cache
                .insert(execution_id.to_string(), new_entries)
                .await;
            count
        };

        debug!(
            execution_id = %execution_id,
            total_entries = total_count,
            "Appended entries to cached journal"
        );

        total_count
    }

    /// Get or incrementally load journal entries
    ///
    /// This is an optimized version that:
    /// 1. Checks if we have cached entries
    /// 2. If cached, loads only new entries after the last cached event_id
    /// 3. Appends new entries to cache
    ///
    /// # Arguments
    ///
    /// * `execution_id` - The execution identifier
    /// * `full_loader` - Function to load full journal (called if cache miss)
    /// * `incremental_loader` - Function to load entries after a given event_id
    ///
    /// # Returns
    ///
    /// All journal entries (cached + newly loaded)
    pub async fn get_or_load_journal_incremental<F1, Fut1, F2, Fut2>(
        &self,
        execution_id: &str,
        full_loader: F1,
        incremental_loader: F2,
    ) -> OrcherResult<Vec<JournalEntry>>
    where
        F1: FnOnce() -> Fut1,
        Fut1: std::future::Future<Output = OrcherResult<Vec<JournalEntry>>>,
        F2: FnOnce(i64) -> Fut2,
        Fut2: std::future::Future<Output = OrcherResult<Vec<JournalEntry>>>,
    {
        // Check cache first
        if let Some(cached_entries) = self.journal_cache.get(execution_id).await {
            self.metrics.record_journal_hit();

            // Find the max event_id in cached entries
            let max_cached_event_id = cached_entries.iter().map(|e| e.event_id).max().unwrap_or(0);

            // Load only new entries
            let new_entries = incremental_loader(max_cached_event_id).await?;

            if new_entries.is_empty() {
                // No new entries, return cached - full cache hit!
                record_journal_cache_hit();
                debug!(
                    execution_id = %execution_id,
                    cached_count = cached_entries.len(),
                    "Returning fully cached journal (no new entries)"
                );
                return Ok(cached_entries);
            }

            // Append new entries to cache and return all - incremental load
            record_journal_incremental_load(new_entries.len());
            let mut all_entries = cached_entries;
            all_entries.extend(new_entries.clone());

            // Update cache
            self.journal_cache
                .insert(execution_id.to_string(), all_entries.clone())
                .await;

            info!(
                execution_id = %execution_id,
                total_entries = all_entries.len(),
                new_entries = new_entries.len(),
                "Incrementally loaded and cached journal"
            );

            return Ok(all_entries);
        }

        // Cache miss - load full journal
        self.metrics.record_journal_miss();
        record_journal_cache_miss();
        let entries = full_loader().await?;

        // Cache the result
        if !entries.is_empty() {
            self.set_journal(execution_id, entries.clone()).await;
        }

        Ok(entries)
    }

    /// Get the last (max) event_id from cached journal
    ///
    /// Returns None if journal is not in cache or is empty
    pub async fn get_cached_max_event_id(&self, execution_id: &str) -> Option<i64> {
        self.journal_cache
            .get(execution_id)
            .await
            .and_then(|entries| entries.iter().map(|e| e.event_id).max())
    }

    /// Invalidate journal cache
    pub async fn invalidate_journal(&self, execution_id: &str) {
        self.journal_cache.invalidate(execution_id).await;
        self.metrics.record_invalidation();
        debug!(execution_id = %execution_id, "Journal cache invalidated");
    }

    // ========================================================================
    // Claim Cache Operations
    // ========================================================================

    /// Get claim info from cache
    pub async fn get_claim(&self, execution_id: &str) -> Option<ClaimInfo> {
        let result = self.claim_cache.get(execution_id).await;
        if result.is_some() {
            self.metrics.record_claim_hit();
        } else {
            self.metrics.record_claim_miss();
        }
        result
    }

    /// Set claim info in cache
    pub async fn set_claim(&self, execution_id: &str, claim: ClaimInfo) {
        self.claim_cache
            .insert(execution_id.to_string(), claim)
            .await;
        debug!(execution_id = %execution_id, "Claim info cached");
    }

    /// Check if execution is claimed (using cache)
    ///
    /// Returns None if not in cache (caller should check database).
    /// Returns Some(true) if claimed and not expired.
    /// Returns Some(false) if claim is expired.
    pub async fn is_claimed(&self, execution_id: &str) -> Option<bool> {
        self.get_claim(execution_id).await.map(|c| !c.is_expired())
    }

    /// Check if execution is claimed by specific identity
    pub async fn is_claimed_by(&self, execution_id: &str, identity: &str) -> Option<bool> {
        self.get_claim(execution_id)
            .await
            .map(|c| c.is_claimed_by(identity) && !c.is_expired())
    }

    /// Invalidate claim cache
    pub async fn invalidate_claim(&self, execution_id: &str) {
        self.claim_cache.invalidate(execution_id).await;
        self.metrics.record_invalidation();
        debug!(execution_id = %execution_id, "Claim cache invalidated");
    }

    // ========================================================================
    // Bulk Operations
    // ========================================================================

    /// Invalidate all caches for an execution
    pub async fn invalidate_all(&self, execution_id: &str) {
        self.invalidate_workflow_state(execution_id).await;
        self.invalidate_journal(execution_id).await;
        self.invalidate_claim(execution_id).await;
        debug!(execution_id = %execution_id, "All caches invalidated");
    }

    /// Clear all caches
    pub async fn clear_all(&self) {
        self.workflow_states.invalidate_all();
        self.journal_cache.invalidate_all();
        self.claim_cache.invalidate_all();
        info!("All caches cleared");
    }

    // ========================================================================
    // Metrics and Stats
    // ========================================================================

    /// Get cache statistics snapshot
    pub async fn get_stats(&self) -> CacheStatsSnapshot {
        CacheStatsSnapshot {
            workflow_entries: self.workflow_states.entry_count(),
            workflow_hit_rate: self.metrics.workflow_hit_rate(),
            workflow_hits: self.metrics.workflow_hits.load(Ordering::Relaxed),
            workflow_misses: self.metrics.workflow_misses.load(Ordering::Relaxed),

            journal_entries: self.journal_cache.entry_count(),
            journal_hit_rate: self.metrics.journal_hit_rate(),
            journal_hits: self.metrics.journal_hits.load(Ordering::Relaxed),
            journal_misses: self.metrics.journal_misses.load(Ordering::Relaxed),

            claim_entries: self.claim_cache.entry_count(),
            claim_hit_rate: self.metrics.claim_hit_rate(),
            claim_hits: self.metrics.claim_hits.load(Ordering::Relaxed),
            claim_misses: self.metrics.claim_misses.load(Ordering::Relaxed),

            total_invalidations: self.metrics.invalidations.load(Ordering::Relaxed),
        }
    }

    /// Get reference to metrics (for external monitoring)
    pub fn metrics(&self) -> Arc<CacheMetrics> {
        Arc::clone(&self.metrics)
    }

    /// Get configuration
    pub fn config(&self) -> &HighPerfCacheConfig {
        &self.config
    }
}

// ============================================================================
// JournalCache Trait Implementation
// ============================================================================

#[async_trait::async_trait]
impl orcher_dispatch::traits::JournalCache for HighPerfCache {
    async fn get_workflow_state(&self, execution_id: &str) -> Option<WorkflowExecutionState> {
        self.get_workflow_state(execution_id).await
    }

    async fn set_workflow_state(&self, execution_id: &str, state: WorkflowExecutionState) {
        self.set_workflow_state(execution_id, state).await
    }

    async fn invalidate_workflow_state(&self, execution_id: &str) {
        self.invalidate_workflow_state(execution_id).await
    }

    async fn get_or_load_journal_incremental(
        &self,
        execution_id: &str,
        full_loader: orcher_dispatch::traits::JournalLoader,
        incremental_loader: orcher_dispatch::traits::IncrementalJournalLoader,
    ) -> OrcherResult<Vec<JournalEntry>> {
        self.get_or_load_journal_incremental(execution_id, full_loader, incremental_loader)
            .await
    }

    async fn get_journal(&self, execution_id: &str) -> Option<Vec<JournalEntry>> {
        self.get_journal(execution_id).await
    }

    async fn set_journal(&self, execution_id: &str, entries: Vec<JournalEntry>) {
        self.set_journal(execution_id, entries).await
    }

    async fn invalidate_journal(&self, execution_id: &str) {
        self.invalidate_journal(execution_id).await
    }

    async fn invalidate_all(&self, execution_id: &str) {
        self.invalidate_all(execution_id).await
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use jiff::SignedDuration;
    use uuid::Uuid;

    fn create_test_state(execution_id: &str) -> WorkflowExecutionState {
        // Use the correct constructor signature
        WorkflowExecutionState::new(
            Uuid::new_v4(),
            format!("workflow-{}", execution_id),
            "default".to_string(),
            None,
        )
    }

    fn create_test_journal_entry(execution_id: &str, event_id: i64) -> JournalEntry {
        JournalEntry {
            id: 0,
            workflow_id: "workflow-1".to_string(),
            execution_id: execution_id.to_string(),
            event_id,
            event_type: "TestEvent".to_string(),
            timestamp: Timestamptz::now(),
            payload: serde_json::json!({}),
            version: 1,
            task_id: 0,
            created_at: Timestamptz::now(),
            shard_id: None,
            organization_id: None,
        }
    }

    #[tokio::test]
    async fn test_workflow_state_cache() {
        let cache = HighPerfCache::with_defaults();
        let state = create_test_state("exec-1");

        // Should be empty initially
        assert!(cache.get_workflow_state("exec-1").await.is_none());

        // Insert and retrieve
        cache.set_workflow_state("exec-1", state.clone()).await;
        let retrieved = cache.get_workflow_state("exec-1").await;
        assert!(retrieved.is_some());
        assert!(retrieved.unwrap().workflow_name.contains("exec-1"));

        // Invalidate
        cache.invalidate_workflow_state("exec-1").await;
        assert!(cache.get_workflow_state("exec-1").await.is_none());
    }

    #[tokio::test]
    async fn test_journal_cache() {
        let cache = HighPerfCache::with_defaults();

        // Should be empty initially
        assert!(cache.get_journal("exec-1").await.is_none());

        // Insert and retrieve
        let entries = vec![
            create_test_journal_entry("exec-1", 1),
            create_test_journal_entry("exec-1", 2),
        ];
        cache.set_journal("exec-1", entries.clone()).await;

        let retrieved = cache.get_journal("exec-1").await;
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().len(), 2);

        // Append to journal
        let new_entry = create_test_journal_entry("exec-1", 3);
        cache.append_to_journal("exec-1", new_entry).await;

        let updated = cache.get_journal("exec-1").await;
        assert!(updated.is_some());
        assert_eq!(updated.unwrap().len(), 3);
    }

    #[tokio::test]
    async fn test_claim_cache() {
        let cache = HighPerfCache::with_defaults();

        // Should be empty initially
        assert!(cache.get_claim("exec-1").await.is_none());

        // Insert claim
        let claim = ClaimInfo::new(
            "worker-1".to_string(),
            Timestamptz::now(),
            Timestamptz::now() + SignedDuration::from_secs(300),
        );
        cache.set_claim("exec-1", claim).await;

        // Check claim
        assert!(cache.is_claimed("exec-1").await == Some(true));
        assert!(cache.is_claimed_by("exec-1", "worker-1").await == Some(true));
        assert!(cache.is_claimed_by("exec-1", "worker-2").await == Some(false));
    }

    #[tokio::test]
    async fn test_cache_metrics() {
        let cache = HighPerfCache::with_defaults();

        // Generate some hits and misses
        cache.get_workflow_state("miss-1").await;
        cache.get_workflow_state("miss-2").await;

        let state = create_test_state("hit-1");
        cache.set_workflow_state("hit-1", state).await;
        cache.get_workflow_state("hit-1").await;
        cache.get_workflow_state("hit-1").await;
        cache.get_workflow_state("hit-1").await;

        let stats = cache.get_stats().await;
        assert_eq!(stats.workflow_hits, 3);
        assert_eq!(stats.workflow_misses, 2);
        assert!(stats.workflow_hit_rate > 0.5);
    }

    #[tokio::test]
    async fn test_read_through_pattern() {
        let cache = HighPerfCache::with_defaults();

        // First call should load from "database"
        let loaded = cache
            .get_or_load_workflow_state("exec-1", || async {
                Ok(Some(create_test_state("exec-1")))
            })
            .await
            .unwrap();

        assert!(loaded.is_some());

        // Second call should come from cache (loader not called)
        let mut loader_called = false;
        let cached = cache
            .get_or_load_workflow_state("exec-1", || async {
                loader_called = true;
                Ok(Some(create_test_state("exec-1")))
            })
            .await
            .unwrap();

        assert!(cached.is_some());
        assert!(!loader_called, "Loader should not be called when cache hit");

        // Verify stats show one miss and one hit
        let stats = cache.get_stats().await;
        assert_eq!(stats.workflow_hits, 1);
        assert_eq!(stats.workflow_misses, 1);
    }

    #[tokio::test]
    async fn test_invalidate_all() {
        let cache = HighPerfCache::with_defaults();

        // Populate caches
        cache
            .set_workflow_state("exec-1", create_test_state("exec-1"))
            .await;
        cache
            .set_journal("exec-1", vec![create_test_journal_entry("exec-1", 1)])
            .await;
        cache
            .set_claim(
                "exec-1",
                ClaimInfo::new(
                    "worker-1".to_string(),
                    Timestamptz::now(),
                    Timestamptz::now() + SignedDuration::from_secs(300),
                ),
            )
            .await;

        // Verify populated
        assert!(cache.get_workflow_state("exec-1").await.is_some());
        assert!(cache.get_journal("exec-1").await.is_some());
        assert!(cache.get_claim("exec-1").await.is_some());

        // Invalidate all
        cache.invalidate_all("exec-1").await;

        // Verify all invalidated
        assert!(cache.get_workflow_state("exec-1").await.is_none());
        assert!(cache.get_journal("exec-1").await.is_none());
        assert!(cache.get_claim("exec-1").await.is_none());
    }
}
