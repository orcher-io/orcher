//! In-memory workflow state cache.

use crate::engine::state::types::WorkflowExecutionState;
use orcher_common::error::OrcherResult;
use orcher_common::types::time::Timestamptz;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};
use uuid::Uuid;

// ============================================================================
// Cache Configuration
// ============================================================================

/// Configuration for the workflow state cache
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Maximum number of entries in the cache
    pub max_entries: usize,

    /// Maximum memory size in bytes (soft limit)
    pub max_memory_bytes: usize,

    /// Enable metrics tracking
    pub enable_metrics: bool,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_entries: 10_000,           // 10K workflows
            max_memory_bytes: 100_000_000, // 100MB
            enable_metrics: true,
        }
    }
}

// ============================================================================
// Cache Entry
// ============================================================================

/// A cached entry with metadata
#[derive(Debug, Clone)]
struct CachedEntry {
    /// The workflow state
    state: WorkflowExecutionState,

    /// When this entry was cached
    #[allow(dead_code)]
    cached_at: Timestamptz,

    /// Number of times this entry has been accessed
    access_count: u64,

    /// Last access timestamp (for LRU)
    last_accessed: Timestamptz,

    /// Estimated memory size
    memory_size: usize,
}

impl CachedEntry {
    fn new(state: WorkflowExecutionState) -> Self {
        let memory_size = state.estimate_memory_size();
        let now = Timestamptz::now();

        Self {
            state,
            cached_at: now,
            access_count: 0,
            last_accessed: now,
            memory_size,
        }
    }

    fn access(&mut self) -> WorkflowExecutionState {
        self.access_count += 1;
        self.last_accessed = Timestamptz::now();
        self.state.clone()
    }
}

// ============================================================================
// Cache Metrics
// ============================================================================

/// Cache statistics
#[derive(Debug, Clone)]
pub struct CacheStats {
    /// Current number of entries
    pub entries: usize,

    /// Maximum entries allowed
    pub max_entries: usize,

    /// Current memory usage in bytes
    pub memory_bytes: usize,

    /// Maximum memory allowed
    pub max_memory_bytes: usize,

    /// Cache hit rate (0.0 to 1.0)
    pub hit_rate: f64,

    /// Total cache hits
    pub hits: u64,

    /// Total cache misses
    pub misses: u64,

    /// Total evictions
    pub evictions: u64,

    /// Total invalidations
    pub invalidations: u64,
}

#[derive(Debug, Default)]
struct CacheMetrics {
    hits: u64,
    misses: u64,
    evictions: u64,
    invalidations: u64,
}

impl CacheMetrics {
    fn record_hit(&mut self) {
        self.hits += 1;
    }

    fn record_miss(&mut self) {
        self.misses += 1;
    }

    fn record_eviction(&mut self) {
        self.evictions += 1;
    }

    fn record_invalidation(&mut self) {
        self.invalidations += 1;
    }

    fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }
}

// ============================================================================
// Workflow State Cache
// ============================================================================

/// Production-grade workflow state cache with LRU eviction
pub struct WorkflowStateCache {
    /// Cache storage
    cache: Arc<RwLock<HashMap<Uuid, CachedEntry>>>,

    /// Configuration
    config: CacheConfig,

    /// Current memory usage
    current_memory: Arc<RwLock<usize>>,

    /// Metrics
    metrics: Arc<RwLock<CacheMetrics>>,

    /// Workflows whose cached state may not have been committed, marked by
    /// [`WorkflowStateCache::distrust`]. A plain mutex so the mark is made
    /// without an await; the next read of the workflow discards its entry
    /// instead of returning it.
    distrusted: Arc<std::sync::Mutex<std::collections::HashSet<Uuid>>>,
}

impl WorkflowStateCache {
    /// Create a new workflow state cache
    pub fn new(config: CacheConfig) -> Self {
        info!(
            max_entries = config.max_entries,
            max_memory_mb = config.max_memory_bytes / 1_000_000,
            "Initializing workflow state cache"
        );

        Self {
            cache: Arc::new(RwLock::new(HashMap::new())),
            config,
            current_memory: Arc::new(RwLock::new(0)),
            metrics: Arc::new(RwLock::new(CacheMetrics::default())),
            distrusted: Arc::new(std::sync::Mutex::new(std::collections::HashSet::new())),
        }
    }

    /// Create with default configuration
    pub fn with_defaults() -> Self {
        Self::new(CacheConfig::default())
    }

    /// Get a workflow state from the cache
    pub async fn get(&self, workflow_id: &Uuid) -> OrcherResult<Option<WorkflowExecutionState>> {
        let mut cache = self.cache.write().await;

        // Marked as possibly uncommitted: dropped, and read as a miss, so the
        // caller rebuilds the state from what the database actually holds.
        let distrusted = self
            .distrusted
            .lock()
            .map(|mut marked| marked.remove(workflow_id))
            .unwrap_or(false);
        if distrusted {
            if let Some(entry) = cache.remove(workflow_id) {
                let mut current_memory = self.current_memory.write().await;
                *current_memory = current_memory.saturating_sub(entry.memory_size);
            }
            return Ok(None);
        }

        match cache.get_mut(workflow_id) {
            Some(entry) => {
                if self.config.enable_metrics {
                    self.metrics.write().await.record_hit();
                }

                debug!(
                    workflow_id = %workflow_id,
                    access_count = entry.access_count + 1,
                    "Cache hit"
                );

                Ok(Some(entry.access()))
            }
            None => {
                if self.config.enable_metrics {
                    self.metrics.write().await.record_miss();
                }

                debug!(workflow_id = %workflow_id, "Cache miss");
                Ok(None)
            }
        }
    }

    /// Insert or update a workflow state in the cache
    pub async fn insert(&self, state: WorkflowExecutionState) -> OrcherResult<()> {
        let workflow_id = state.workflow_id;
        let entry = CachedEntry::new(state);
        let entry_size = entry.memory_size;

        // Evict if necessary to make room
        self.evict_to_fit(entry_size).await?;

        let mut cache = self.cache.write().await;
        let mut current_memory = self.current_memory.write().await;

        // A fresh state supersedes a mark on the one before it.
        if let Ok(mut marked) = self.distrusted.lock() {
            marked.remove(&workflow_id);
        }

        // Remove old entry if exists
        if let Some(old_entry) = cache.remove(&workflow_id) {
            *current_memory = current_memory.saturating_sub(old_entry.memory_size);
        }

        // Insert new entry
        cache.insert(workflow_id, entry);
        *current_memory += entry_size;

        debug!(
            workflow_id = %workflow_id,
            entries = cache.len(),
            memory_mb = *current_memory / 1_000_000,
            "Cached workflow state"
        );

        Ok(())
    }

    /// Stop trusting a workflow's cached state, synchronously: until
    /// [`trust`](Self::trust), its next read is a miss and drops the entry.
    /// Synchronous so it can be set before an await whose future may be
    /// dropped, where an async invalidation could never run.
    pub fn distrust(&self, workflow_id: Uuid) {
        if let Ok(mut marked) = self.distrusted.lock() {
            marked.insert(workflow_id);
        }
    }

    /// Trust a workflow's cached state again, once what it was cached ahead
    /// of has committed. Its entry was left in place, so a read that has not
    /// happened since finds it; one that has already discarded it.
    pub fn trust(&self, workflow_id: Uuid) {
        if let Ok(mut marked) = self.distrusted.lock() {
            marked.remove(&workflow_id);
        }
    }

    /// Invalidate a workflow state (remove from cache)
    pub async fn invalidate(&self, workflow_id: &Uuid) -> OrcherResult<bool> {
        let mut cache = self.cache.write().await;
        let mut current_memory = self.current_memory.write().await;

        if let Some(entry) = cache.remove(workflow_id) {
            *current_memory = current_memory.saturating_sub(entry.memory_size);

            if self.config.enable_metrics {
                self.metrics.write().await.record_invalidation();
            }

            debug!(
                workflow_id = %workflow_id,
                "Invalidated workflow state"
            );

            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Evict entries to fit a new entry of the given size
    async fn evict_to_fit(&self, new_entry_size: usize) -> OrcherResult<()> {
        let cache = self.cache.read().await;
        let current_memory = *self.current_memory.read().await;

        // Check if we need to evict
        let needs_eviction = cache.len() >= self.config.max_entries
            || (current_memory + new_entry_size) > self.config.max_memory_bytes;

        if !needs_eviction {
            return Ok(());
        }

        drop(cache); // Release read lock

        // Collect entries to evict (LRU)
        let mut cache = self.cache.write().await;
        let mut entries: Vec<_> = cache
            .iter()
            .map(|(id, entry)| (*id, entry.last_accessed, entry.memory_size))
            .collect();

        // Sort by last accessed (oldest first)
        entries.sort_by_key(|(_, last_accessed, _)| *last_accessed);

        // Evict until we have enough space
        let mut memory_to_free =
            (current_memory + new_entry_size).saturating_sub(self.config.max_memory_bytes);
        // Need to free at least 1 slot for the new entry
        let mut entries_to_evict = (cache.len() + 1).saturating_sub(self.config.max_entries);

        let mut evicted_count = 0;
        let mut freed_memory = 0;

        for (workflow_id, _, _size) in entries.iter() {
            if memory_to_free == 0 && entries_to_evict == 0 {
                break;
            }

            if let Some(entry) = cache.remove(workflow_id) {
                freed_memory += entry.memory_size;
                evicted_count += 1;

                memory_to_free = memory_to_free.saturating_sub(entry.memory_size);
                entries_to_evict = entries_to_evict.saturating_sub(1);

                if self.config.enable_metrics {
                    self.metrics.write().await.record_eviction();
                }
            }
        }

        if evicted_count > 0 {
            let mut current_memory = self.current_memory.write().await;
            *current_memory = current_memory.saturating_sub(freed_memory);

            warn!(
                evicted = evicted_count,
                freed_mb = freed_memory / 1_000_000,
                remaining_entries = cache.len(),
                "Evicted entries from cache"
            );
        }

        Ok(())
    }

    /// Get cache statistics
    pub async fn get_stats(&self) -> CacheStats {
        let cache = self.cache.read().await;
        let current_memory = *self.current_memory.read().await;
        let metrics = self.metrics.read().await;

        CacheStats {
            entries: cache.len(),
            max_entries: self.config.max_entries,
            memory_bytes: current_memory,
            max_memory_bytes: self.config.max_memory_bytes,
            hit_rate: metrics.hit_rate(),
            hits: metrics.hits,
            misses: metrics.misses,
            evictions: metrics.evictions,
            invalidations: metrics.invalidations,
        }
    }

    /// Clear the entire cache
    pub async fn clear(&self) -> OrcherResult<()> {
        let mut cache = self.cache.write().await;
        let mut current_memory = self.current_memory.write().await;

        let count = cache.len();
        cache.clear();
        *current_memory = 0;

        info!(cleared = count, "Cleared workflow state cache");

        Ok(())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_state(workflow_id: Uuid, workflow_name: &str) -> WorkflowExecutionState {
        WorkflowExecutionState::new(
            workflow_id,
            workflow_name.to_string(),
            "test".to_string(),
            None,
        )
    }

    /// A state cached ahead of a commit that was never seen to succeed is not
    /// served: the next read misses and drops it. Trusted again once the
    /// commit is seen, it is served as before.
    #[tokio::test]
    async fn a_distrusted_state_is_not_served() {
        let cache = WorkflowStateCache::with_defaults();
        let uncommitted = Uuid::new_v4();
        let committed = Uuid::new_v4();
        for id in [uncommitted, committed] {
            cache.insert(create_test_state(id, "wf")).await.unwrap();
            cache.distrust(id);
        }
        cache.trust(committed);

        assert!(cache.get(&uncommitted).await.unwrap().is_none());
        assert!(
            cache.get(&uncommitted).await.unwrap().is_none(),
            "and it stays gone"
        );
        assert!(cache.get(&committed).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn test_cache_creation() {
        let cache = WorkflowStateCache::with_defaults();
        let stats = cache.get_stats().await;

        assert_eq!(stats.entries, 0);
        assert_eq!(stats.hits, 0);
        assert_eq!(stats.misses, 0);
    }

    #[tokio::test]
    async fn test_cache_insert_and_get() {
        let cache = WorkflowStateCache::with_defaults();
        let workflow_id = Uuid::new_v4();
        let state = create_test_state(workflow_id, "test_workflow");

        // Insert
        cache.insert(state.clone()).await.unwrap();

        // Get (should hit)
        let retrieved = cache.get(&workflow_id).await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().workflow_id, workflow_id);

        // Check stats
        let stats = cache.get_stats().await;
        assert_eq!(stats.entries, 1);
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 0);
    }

    #[tokio::test]
    async fn test_cache_miss() {
        let cache = WorkflowStateCache::with_defaults();
        let workflow_id = Uuid::new_v4();

        // Get non-existent workflow
        let retrieved = cache.get(&workflow_id).await.unwrap();
        assert!(retrieved.is_none());

        // Check stats
        let stats = cache.get_stats().await;
        assert_eq!(stats.misses, 1);
    }

    #[tokio::test]
    async fn test_cache_invalidation() {
        let cache = WorkflowStateCache::with_defaults();
        let workflow_id = Uuid::new_v4();
        let state = create_test_state(workflow_id, "test_workflow");

        // Insert
        cache.insert(state).await.unwrap();
        assert_eq!(cache.get_stats().await.entries, 1);

        // Invalidate
        let invalidated = cache.invalidate(&workflow_id).await.unwrap();
        assert!(invalidated);
        assert_eq!(cache.get_stats().await.entries, 0);

        // Try to invalidate again (should return false)
        let invalidated_again = cache.invalidate(&workflow_id).await.unwrap();
        assert!(!invalidated_again);
    }

    #[tokio::test]
    async fn test_cache_lru_eviction() {
        let config = CacheConfig {
            max_entries: 2,
            max_memory_bytes: 100_000_000,
            enable_metrics: true,
        };
        let cache = WorkflowStateCache::new(config);

        // Insert 3 workflows (exceeds max_entries)
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        let id3 = Uuid::new_v4();

        cache
            .insert(create_test_state(id1, "workflow1"))
            .await
            .unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        cache
            .insert(create_test_state(id2, "workflow2"))
            .await
            .unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Access id1 to make it more recent
        cache.get(&id1).await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Insert id3 (should evict id2, the LRU)
        cache
            .insert(create_test_state(id3, "workflow3"))
            .await
            .unwrap();

        // Check that id1 and id3 are present, id2 is evicted
        assert!(cache.get(&id1).await.unwrap().is_some());
        assert!(cache.get(&id2).await.unwrap().is_none());
        assert!(cache.get(&id3).await.unwrap().is_some());

        let stats = cache.get_stats().await;
        assert_eq!(stats.entries, 2);
        assert!(stats.evictions > 0);
    }

    #[tokio::test]
    async fn test_cache_memory_limit() {
        let config = CacheConfig {
            max_entries: 1000,
            max_memory_bytes: 1000, // Very small limit
            enable_metrics: true,
        };
        let cache = WorkflowStateCache::new(config.clone());

        // Insert multiple workflows
        for i in 0..5 {
            let id = Uuid::new_v4();
            cache
                .insert(create_test_state(id, &format!("workflow{}", i)))
                .await
                .unwrap();
        }

        let stats = cache.get_stats().await;
        // Should have evicted some entries due to memory limit
        assert!(stats.entries < 5);
        assert!(stats.evictions > 0);
        assert!(stats.memory_bytes <= config.max_memory_bytes);
    }

    #[tokio::test]
    async fn test_cache_clear() {
        let cache = WorkflowStateCache::with_defaults();

        // Insert multiple workflows
        for i in 0..5 {
            let id = Uuid::new_v4();
            cache
                .insert(create_test_state(id, &format!("workflow{}", i)))
                .await
                .unwrap();
        }

        assert_eq!(cache.get_stats().await.entries, 5);

        // Clear cache
        cache.clear().await.unwrap();

        let stats = cache.get_stats().await;
        assert_eq!(stats.entries, 0);
        assert_eq!(stats.memory_bytes, 0);
    }

    #[tokio::test]
    async fn test_cache_hit_rate_calculation() {
        let cache = WorkflowStateCache::with_defaults();
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();

        cache
            .insert(create_test_state(id1, "workflow1"))
            .await
            .unwrap();

        // 3 hits, 2 misses
        cache.get(&id1).await.unwrap(); // hit
        cache.get(&id1).await.unwrap(); // hit
        cache.get(&id1).await.unwrap(); // hit
        cache.get(&id2).await.unwrap(); // miss
        cache.get(&id2).await.unwrap(); // miss

        let stats = cache.get_stats().await;
        assert_eq!(stats.hits, 3);
        assert_eq!(stats.misses, 2);
        assert!((stats.hit_rate - 0.6).abs() < 0.01); // 3/5 = 0.6
    }

    #[tokio::test]
    async fn test_cache_update_existing() {
        let cache = WorkflowStateCache::with_defaults();
        let workflow_id = Uuid::new_v4();

        // Insert initial state
        let mut state1 = create_test_state(workflow_id, "workflow1");
        cache.insert(state1.clone()).await.unwrap();

        // Update state (add a timer)
        state1.add_timer("timer-1".to_string(), Timestamptz::now());
        cache.insert(state1.clone()).await.unwrap();

        // Should still have only 1 entry
        let stats = cache.get_stats().await;
        assert_eq!(stats.entries, 1);

        // Get and verify the updated state
        let retrieved = cache.get(&workflow_id).await.unwrap().unwrap();
        assert_eq!(retrieved.pending_timers.len(), 1);
        assert_eq!(retrieved.version, 2); // Version should be incremented
    }
}
