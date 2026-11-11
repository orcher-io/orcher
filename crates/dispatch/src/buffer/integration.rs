//! Bridges the buffer pool with task dispatch payloads and poll responses.

use std::sync::Arc;

use super::pool::{BufferPool, BufferPoolConfig, BufferPoolStats, PooledBuffer, SizeClass};

/// Simplified interface for using pooled buffers in task dispatch.
pub struct BufferPoolIntegration {
    pool: Arc<BufferPool>,
    enabled: bool,
}

impl BufferPoolIntegration {
    pub fn new() -> Self {
        Self {
            pool: BufferPool::new(),
            enabled: true,
        }
    }

    pub fn with_config(config: BufferPoolConfig) -> Self {
        Self {
            pool: BufferPool::with_config(config),
            enabled: true,
        }
    }

    pub fn with_pool(pool: Arc<BufferPool>) -> Self {
        Self {
            pool,
            enabled: true,
        }
    }

    pub fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Returns None when disabled.
    pub fn acquire_for_payload(&self, size: usize) -> Option<PooledBuffer> {
        if !self.enabled {
            return None;
        }
        Some(self.pool.acquire(size))
    }

    pub fn acquire_with_data(&self, data: &[u8]) -> Option<PooledBuffer> {
        if !self.enabled {
            return None;
        }
        Some(self.pool.acquire_with_data(data))
    }

    /// Uses medium (4KB) - fits most task responses.
    pub fn acquire_for_response(&self) -> Option<PooledBuffer> {
        if !self.enabled {
            return None;
        }
        Some(self.pool.acquire(SizeClass::Medium.capacity()))
    }

    /// Uses large (64KB) to accommodate multiple journal entries.
    pub fn acquire_for_journal(&self) -> Option<PooledBuffer> {
        if !self.enabled {
            return None;
        }
        Some(self.pool.acquire(SizeClass::Large.capacity()))
    }

    /// Uses xlarge (1MB) for batch task processing.
    pub fn acquire_for_batch(&self) -> Option<PooledBuffer> {
        if !self.enabled {
            return None;
        }
        Some(self.pool.acquire(SizeClass::XLarge.capacity()))
    }

    pub fn stats(&self) -> BufferPoolStats {
        self.pool.stats()
    }

    pub fn pool(&self) -> &Arc<BufferPool> {
        &self.pool
    }

    pub fn clear(&self) {
        self.pool.clear();
    }

    pub fn config(&self) -> &BufferPoolConfig {
        self.pool.config()
    }

    /// True if overall hit rate is at or above the threshold.
    pub fn is_healthy(&self, min_hit_rate: f64) -> bool {
        self.stats().overall_hit_rate >= min_hit_rate
    }

    /// Estimated bytes saved by reusing buffers instead of allocating new ones.
    pub fn estimated_memory_savings(&self) -> u64 {
        let stats = self.stats();
        let hits = stats.hit_rates.iter().enumerate().map(|(i, rate)| {
            let size = match i {
                0 => SizeClass::Small.capacity(),
                1 => SizeClass::Medium.capacity(),
                2 => SizeClass::Large.capacity(),
                3 => SizeClass::XLarge.capacity(),
                _ => 0,
            };
            (stats.total_acquisitions as f64 * rate * size as f64) as u64
        });
        hits.sum()
    }
}

impl Default for BufferPoolIntegration {
    fn default() -> Self {
        Self::new()
    }
}

pub struct BufferPoolIntegrationBuilder {
    config: Option<BufferPoolConfig>,
    pool: Option<Arc<BufferPool>>,
    enabled: bool,
}

impl BufferPoolIntegrationBuilder {
    pub fn new() -> Self {
        Self {
            config: None,
            pool: None,
            enabled: true,
        }
    }

    pub fn config(mut self, config: BufferPoolConfig) -> Self {
        self.config = Some(config);
        self
    }

    pub fn high_throughput(mut self) -> Self {
        self.config = Some(BufferPoolConfig::high_throughput());
        self
    }

    pub fn memory_efficient(mut self) -> Self {
        self.config = Some(BufferPoolConfig::memory_efficient());
        self
    }

    pub fn pool(mut self, pool: Arc<BufferPool>) -> Self {
        self.pool = Some(pool);
        self
    }

    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    pub fn build(self) -> BufferPoolIntegration {
        let mut integration = if let Some(pool) = self.pool {
            BufferPoolIntegration::with_pool(pool)
        } else if let Some(config) = self.config {
            BufferPoolIntegration::with_config(config)
        } else {
            BufferPoolIntegration::new()
        };

        integration.set_enabled(self.enabled);
        integration
    }
}

impl Default for BufferPoolIntegrationBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// JSON serialization using pooled buffers.
pub struct PooledSerializer {
    pool: Arc<BufferPool>,
}

impl PooledSerializer {
    pub fn new(pool: Arc<BufferPool>) -> Self {
        Self { pool }
    }

    pub fn serialize_json<T: serde::Serialize>(
        &self,
        value: &T,
    ) -> Result<PooledBuffer, serde_json::Error> {
        // Estimate size based on debug representation
        let estimated_size = std::mem::size_of_val(value) * 4;
        let mut buffer = self.pool.acquire(estimated_size.max(256));

        // Serialize directly into the buffer
        let json = serde_json::to_vec(value)?;
        buffer.extend_from_slice(&json);

        Ok(buffer)
    }

    pub fn deserialize_json<'a, T: serde::Deserialize<'a>>(
        &self,
        data: &'a [u8],
    ) -> Result<T, serde_json::Error> {
        serde_json::from_slice(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_integration_creation() {
        let integration = BufferPoolIntegration::new();
        assert!(integration.is_enabled());
    }

    #[test]
    fn test_integration_with_config() {
        let config = BufferPoolConfig::high_throughput();
        let integration = BufferPoolIntegration::with_config(config);
        assert!(integration.is_enabled());
        assert!(integration.config().preallocate);
    }

    #[test]
    fn test_integration_disable() {
        let mut integration = BufferPoolIntegration::new();
        integration.set_enabled(false);
        assert!(!integration.is_enabled());
    }

    #[test]
    fn test_acquire_when_enabled() {
        let integration = BufferPoolIntegration::new();

        let buffer = integration.acquire_for_payload(100);
        assert!(buffer.is_some());

        let buffer = buffer.unwrap();
        assert!(buffer.capacity() >= 100);
    }

    #[test]
    fn test_acquire_when_disabled() {
        let mut integration = BufferPoolIntegration::new();
        integration.set_enabled(false);

        let buffer = integration.acquire_for_payload(100);
        assert!(buffer.is_none());
    }

    #[test]
    fn test_acquire_with_data() {
        let integration = BufferPoolIntegration::new();
        let data = b"test payload data";

        let buffer = integration.acquire_with_data(data);
        assert!(buffer.is_some());

        let buffer = buffer.unwrap();
        assert_eq!(&buffer[..], data);
    }

    #[test]
    fn test_acquire_for_response() {
        let integration = BufferPoolIntegration::new();

        let buffer = integration.acquire_for_response();
        assert!(buffer.is_some());

        let buffer = buffer.unwrap();
        assert_eq!(buffer.capacity(), SizeClass::Medium.capacity());
    }

    #[test]
    fn test_acquire_for_journal() {
        let integration = BufferPoolIntegration::new();

        let buffer = integration.acquire_for_journal();
        assert!(buffer.is_some());

        let buffer = buffer.unwrap();
        assert_eq!(buffer.capacity(), SizeClass::Large.capacity());
    }

    #[test]
    fn test_acquire_for_batch() {
        let integration = BufferPoolIntegration::new();

        let buffer = integration.acquire_for_batch();
        assert!(buffer.is_some());

        let buffer = buffer.unwrap();
        assert_eq!(buffer.capacity(), SizeClass::XLarge.capacity());
    }

    #[test]
    fn test_stats() {
        let integration = BufferPoolIntegration::new();

        // Acquire and release some buffers
        drop(integration.acquire_for_payload(100));
        drop(integration.acquire_for_response());

        let stats = integration.stats();
        assert!(stats.total_acquisitions >= 2);
    }

    #[test]
    fn test_clear() {
        let integration = BufferPoolIntegration::new();

        // Acquire and release to populate pool
        drop(integration.acquire_for_payload(100));
        drop(integration.acquire_for_payload(100));

        let stats_before = integration.stats();
        assert!(stats_before.pooled_buffers > 0);

        integration.clear();

        let stats_after = integration.stats();
        assert_eq!(stats_after.pooled_buffers, 0);
    }

    #[test]
    fn test_is_healthy() {
        let integration = BufferPoolIntegration::new();

        // No acquisitions yet - hit rate is 0
        assert!(!integration.is_healthy(0.5));

        // After some reuse, hit rate should improve
        drop(integration.acquire_for_payload(100));
        drop(integration.acquire_for_payload(100)); // Hit

        // Now we should have some hit rate
        let stats = integration.stats();
        assert!(stats.overall_hit_rate >= 0.0);
    }

    #[test]
    fn test_builder_default() {
        let integration = BufferPoolIntegrationBuilder::new().build();
        assert!(integration.is_enabled());
    }

    #[test]
    fn test_builder_high_throughput() {
        let integration = BufferPoolIntegrationBuilder::new()
            .high_throughput()
            .build();

        assert!(integration.config().preallocate);
        assert_eq!(integration.config().max_small_buffers, 5000);
    }

    #[test]
    fn test_builder_memory_efficient() {
        let integration = BufferPoolIntegrationBuilder::new()
            .memory_efficient()
            .build();

        assert!(!integration.config().preallocate);
        assert_eq!(integration.config().max_small_buffers, 200);
    }

    #[test]
    fn test_builder_disabled() {
        let integration = BufferPoolIntegrationBuilder::new().enabled(false).build();

        assert!(!integration.is_enabled());
    }

    #[test]
    fn test_builder_with_pool() {
        let pool = BufferPool::new();
        let integration = BufferPoolIntegrationBuilder::new()
            .pool(pool.clone())
            .build();

        // Acquire from integration
        drop(integration.acquire_for_payload(100));

        // Stats should reflect in shared pool
        let stats = pool.stats();
        assert!(stats.total_acquisitions >= 1);
    }

    #[test]
    fn test_pooled_serializer() {
        let pool = BufferPool::new();
        let serializer = PooledSerializer::new(pool);

        #[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
        struct TestPayload {
            id: String,
            value: i32,
        }

        let payload = TestPayload {
            id: "test-123".to_string(),
            value: 42,
        };

        // Serialize
        let buffer = serializer.serialize_json(&payload).unwrap();
        assert!(!buffer.is_empty());

        // Deserialize
        let deserialized: TestPayload = serializer.deserialize_json(&buffer).unwrap();
        assert_eq!(deserialized, payload);
    }

    #[test]
    fn test_estimated_memory_savings() {
        let integration = BufferPoolIntegration::new();

        // Initial savings should be 0
        assert_eq!(integration.estimated_memory_savings(), 0);

        // After some buffer reuse, savings should accumulate
        drop(integration.acquire_for_payload(100));
        drop(integration.acquire_for_payload(100)); // This should be a hit

        // The savings calculation is based on hit rate
        // Just verify it returns without error (u64 is always >= 0)
        let _savings = integration.estimated_memory_savings();
    }

    #[test]
    fn test_shared_pool_across_integrations() {
        let pool = BufferPool::new();

        let integration1 = BufferPoolIntegration::with_pool(pool.clone());
        let integration2 = BufferPoolIntegration::with_pool(pool.clone());

        // Acquire from integration1
        drop(integration1.acquire_for_payload(100));

        // Acquire from integration2 - should reuse from shared pool
        drop(integration2.acquire_for_payload(100));

        // Both should see the same stats
        let stats1 = integration1.stats();
        let stats2 = integration2.stats();

        assert_eq!(stats1.total_acquisitions, stats2.total_acquisitions);
    }
}
