//! Thread-safe buffer pool with size classes for memory reuse in task dispatch.

use std::collections::VecDeque;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

const SMALL_SIZE: usize = 256;
const MEDIUM_SIZE: usize = 4096;
const LARGE_SIZE: usize = 65536;
const XLARGE_SIZE: usize = 1048576;

#[derive(Debug, Clone)]
pub struct BufferPoolConfig {
    pub max_small_buffers: usize,
    pub max_medium_buffers: usize,
    pub max_large_buffers: usize,
    pub max_xlarge_buffers: usize,
    pub preallocate: bool,
    /// Slight overhead when enabled.
    pub detailed_metrics: bool,
}

impl Default for BufferPoolConfig {
    fn default() -> Self {
        Self {
            max_small_buffers: 1000,
            max_medium_buffers: 500,
            max_large_buffers: 100,
            max_xlarge_buffers: 10,
            preallocate: false,
            detailed_metrics: true,
        }
    }
}

impl BufferPoolConfig {
    pub fn high_throughput() -> Self {
        Self {
            max_small_buffers: 5000,
            max_medium_buffers: 2000,
            max_large_buffers: 500,
            max_xlarge_buffers: 50,
            preallocate: true,
            detailed_metrics: false,
        }
    }

    pub fn memory_efficient() -> Self {
        Self {
            max_small_buffers: 200,
            max_medium_buffers: 100,
            max_large_buffers: 20,
            max_xlarge_buffers: 2,
            preallocate: false,
            detailed_metrics: true,
        }
    }

    pub fn max_memory_bytes(&self) -> usize {
        self.max_small_buffers * SMALL_SIZE
            + self.max_medium_buffers * MEDIUM_SIZE
            + self.max_large_buffers * LARGE_SIZE
            + self.max_xlarge_buffers * XLARGE_SIZE
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SizeClass {
    Small,
    Medium,
    Large,
    XLarge,
    /// Not pooled - allocated and freed normally.
    Oversized,
}

impl SizeClass {
    pub fn for_size(size: usize) -> Self {
        if size <= SMALL_SIZE {
            SizeClass::Small
        } else if size <= MEDIUM_SIZE {
            SizeClass::Medium
        } else if size <= LARGE_SIZE {
            SizeClass::Large
        } else if size <= XLARGE_SIZE {
            SizeClass::XLarge
        } else {
            SizeClass::Oversized
        }
    }

    pub fn capacity(&self) -> usize {
        match self {
            SizeClass::Small => SMALL_SIZE,
            SizeClass::Medium => MEDIUM_SIZE,
            SizeClass::Large => LARGE_SIZE,
            SizeClass::XLarge => XLARGE_SIZE,
            SizeClass::Oversized => 0, // No fixed capacity
        }
    }
}

struct SizeClassPool {
    buffers: Mutex<VecDeque<Vec<u8>>>,
    max_buffers: usize,
    capacity: usize,
    current_size: AtomicUsize,
    acquisitions: AtomicU64,
    releases: AtomicU64,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl SizeClassPool {
    fn new(max_buffers: usize, capacity: usize, preallocate: bool) -> Self {
        let mut buffers = VecDeque::with_capacity(max_buffers);

        if preallocate {
            for _ in 0..max_buffers {
                buffers.push_back(Vec::with_capacity(capacity));
            }
        }

        let current_size = if preallocate { max_buffers } else { 0 };

        Self {
            buffers: Mutex::new(buffers),
            max_buffers,
            capacity,
            current_size: AtomicUsize::new(current_size),
            acquisitions: AtomicU64::new(0),
            releases: AtomicU64::new(0),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    fn acquire(&self) -> Vec<u8> {
        self.acquisitions.fetch_add(1, Ordering::Relaxed);

        // Try to get a buffer from the pool
        let mut buffers = self.buffers.lock().unwrap();
        if let Some(mut buffer) = buffers.pop_front() {
            self.current_size.fetch_sub(1, Ordering::Relaxed);
            self.hits.fetch_add(1, Ordering::Relaxed);
            buffer.clear();
            return buffer;
        }
        drop(buffers);

        // No buffer available, allocate new
        self.misses.fetch_add(1, Ordering::Relaxed);
        Vec::with_capacity(self.capacity)
    }

    fn release(&self, mut buffer: Vec<u8>) {
        self.releases.fetch_add(1, Ordering::Relaxed);

        // Only keep buffers that match our capacity
        if buffer.capacity() != self.capacity {
            return; // Drop buffer
        }

        let mut buffers = self.buffers.lock().unwrap();
        if buffers.len() < self.max_buffers {
            buffer.clear();
            buffers.push_back(buffer);
            self.current_size.fetch_add(1, Ordering::Relaxed);
        }
        // Otherwise, buffer is dropped
    }

    fn size(&self) -> usize {
        self.current_size.load(Ordering::Relaxed)
    }

    fn hit_rate(&self) -> f64 {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }
}

#[derive(Debug, Clone)]
pub struct BufferPoolStats {
    pub pooled_buffers: usize,
    /// Indexed by size class: [small, medium, large, xlarge].
    pub buffers_per_class: [usize; 4],
    pub total_acquisitions: u64,
    pub total_releases: u64,
    /// Indexed by size class: [small, medium, large, xlarge].
    pub hit_rates: [f64; 4],
    pub overall_hit_rate: f64,
    pub memory_usage: usize,
}

/// Returns to the pool when dropped unless `take()` is called.
pub struct PooledBuffer {
    buffer: Option<Vec<u8>>,
    size_class: SizeClass,
    pool: Arc<BufferPool>,
}

impl PooledBuffer {
    fn new(buffer: Vec<u8>, size_class: SizeClass, pool: Arc<BufferPool>) -> Self {
        Self {
            buffer: Some(buffer),
            size_class,
            pool,
        }
    }

    pub fn size_class(&self) -> SizeClass {
        self.size_class
    }

    pub fn capacity(&self) -> usize {
        self.buffer.as_ref().map(|b| b.capacity()).unwrap_or(0)
    }

    /// Takes ownership of the inner vec - it won't be returned to the pool.
    pub fn take(mut self) -> Vec<u8> {
        self.buffer.take().unwrap_or_default()
    }

    pub fn is_pooled(&self) -> bool {
        self.size_class != SizeClass::Oversized
    }
}

impl Deref for PooledBuffer {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        self.buffer.as_ref().unwrap()
    }
}

impl DerefMut for PooledBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.buffer.as_mut().unwrap()
    }
}

impl Drop for PooledBuffer {
    fn drop(&mut self) {
        if let Some(buffer) = self.buffer.take() {
            if self.size_class != SizeClass::Oversized {
                self.pool.release_internal(buffer, self.size_class);
            }
            // Oversized buffers are just dropped
        }
    }
}

/// Thread-safe buffer pool with size classes.
pub struct BufferPool {
    small: SizeClassPool,
    medium: SizeClassPool,
    large: SizeClassPool,
    xlarge: SizeClassPool,
    config: BufferPoolConfig,
}

impl BufferPool {
    pub fn new() -> Arc<Self> {
        Self::with_config(BufferPoolConfig::default())
    }

    pub fn with_config(config: BufferPoolConfig) -> Arc<Self> {
        Arc::new(Self {
            small: SizeClassPool::new(config.max_small_buffers, SMALL_SIZE, config.preallocate),
            medium: SizeClassPool::new(config.max_medium_buffers, MEDIUM_SIZE, config.preallocate),
            large: SizeClassPool::new(config.max_large_buffers, LARGE_SIZE, config.preallocate),
            xlarge: SizeClassPool::new(config.max_xlarge_buffers, XLARGE_SIZE, config.preallocate),
            config,
        })
    }

    pub fn acquire(self: &Arc<Self>, size: usize) -> PooledBuffer {
        let size_class = SizeClass::for_size(size);

        let buffer = match size_class {
            SizeClass::Small => self.small.acquire(),
            SizeClass::Medium => self.medium.acquire(),
            SizeClass::Large => self.large.acquire(),
            SizeClass::XLarge => self.xlarge.acquire(),
            SizeClass::Oversized => Vec::with_capacity(size),
        };

        PooledBuffer::new(buffer, size_class, Arc::clone(self))
    }

    pub fn acquire_with_data(self: &Arc<Self>, data: &[u8]) -> PooledBuffer {
        let mut buffer = self.acquire(data.len());
        buffer.extend_from_slice(data);
        buffer
    }

    fn release_internal(&self, buffer: Vec<u8>, size_class: SizeClass) {
        match size_class {
            SizeClass::Small => self.small.release(buffer),
            SizeClass::Medium => self.medium.release(buffer),
            SizeClass::Large => self.large.release(buffer),
            SizeClass::XLarge => self.xlarge.release(buffer),
            SizeClass::Oversized => {} // Just drop
        }
    }

    pub fn stats(&self) -> BufferPoolStats {
        let small_size = self.small.size();
        let medium_size = self.medium.size();
        let large_size = self.large.size();
        let xlarge_size = self.xlarge.size();

        let total_acquisitions = self.small.acquisitions.load(Ordering::Relaxed)
            + self.medium.acquisitions.load(Ordering::Relaxed)
            + self.large.acquisitions.load(Ordering::Relaxed)
            + self.xlarge.acquisitions.load(Ordering::Relaxed);

        let total_releases = self.small.releases.load(Ordering::Relaxed)
            + self.medium.releases.load(Ordering::Relaxed)
            + self.large.releases.load(Ordering::Relaxed)
            + self.xlarge.releases.load(Ordering::Relaxed);

        let total_hits = self.small.hits.load(Ordering::Relaxed)
            + self.medium.hits.load(Ordering::Relaxed)
            + self.large.hits.load(Ordering::Relaxed)
            + self.xlarge.hits.load(Ordering::Relaxed);

        let total_misses = self.small.misses.load(Ordering::Relaxed)
            + self.medium.misses.load(Ordering::Relaxed)
            + self.large.misses.load(Ordering::Relaxed)
            + self.xlarge.misses.load(Ordering::Relaxed);

        let overall_hit_rate = if total_hits + total_misses == 0 {
            0.0
        } else {
            total_hits as f64 / (total_hits + total_misses) as f64
        };

        let memory_usage = small_size * SMALL_SIZE
            + medium_size * MEDIUM_SIZE
            + large_size * LARGE_SIZE
            + xlarge_size * XLARGE_SIZE;

        BufferPoolStats {
            pooled_buffers: small_size + medium_size + large_size + xlarge_size,
            buffers_per_class: [small_size, medium_size, large_size, xlarge_size],
            total_acquisitions,
            total_releases,
            hit_rates: [
                self.small.hit_rate(),
                self.medium.hit_rate(),
                self.large.hit_rate(),
                self.xlarge.hit_rate(),
            ],
            overall_hit_rate,
            memory_usage,
        }
    }

    pub fn config(&self) -> &BufferPoolConfig {
        &self.config
    }

    pub fn clear(&self) {
        self.small.buffers.lock().unwrap().clear();
        self.medium.buffers.lock().unwrap().clear();
        self.large.buffers.lock().unwrap().clear();
        self.xlarge.buffers.lock().unwrap().clear();

        self.small.current_size.store(0, Ordering::Relaxed);
        self.medium.current_size.store(0, Ordering::Relaxed);
        self.large.current_size.store(0, Ordering::Relaxed);
        self.xlarge.current_size.store(0, Ordering::Relaxed);
    }
}

impl Default for BufferPool {
    fn default() -> Self {
        // Note: This creates a non-Arc version for use in tests
        Self {
            small: SizeClassPool::new(
                BufferPoolConfig::default().max_small_buffers,
                SMALL_SIZE,
                false,
            ),
            medium: SizeClassPool::new(
                BufferPoolConfig::default().max_medium_buffers,
                MEDIUM_SIZE,
                false,
            ),
            large: SizeClassPool::new(
                BufferPoolConfig::default().max_large_buffers,
                LARGE_SIZE,
                false,
            ),
            xlarge: SizeClassPool::new(
                BufferPoolConfig::default().max_xlarge_buffers,
                XLARGE_SIZE,
                false,
            ),
            config: BufferPoolConfig::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_size_class_for_size() {
        assert_eq!(SizeClass::for_size(0), SizeClass::Small);
        assert_eq!(SizeClass::for_size(100), SizeClass::Small);
        assert_eq!(SizeClass::for_size(256), SizeClass::Small);
        assert_eq!(SizeClass::for_size(257), SizeClass::Medium);
        assert_eq!(SizeClass::for_size(4096), SizeClass::Medium);
        assert_eq!(SizeClass::for_size(4097), SizeClass::Large);
        assert_eq!(SizeClass::for_size(65536), SizeClass::Large);
        assert_eq!(SizeClass::for_size(65537), SizeClass::XLarge);
        assert_eq!(SizeClass::for_size(1048576), SizeClass::XLarge);
        assert_eq!(SizeClass::for_size(1048577), SizeClass::Oversized);
    }

    #[test]
    fn test_size_class_capacity() {
        assert_eq!(SizeClass::Small.capacity(), 256);
        assert_eq!(SizeClass::Medium.capacity(), 4096);
        assert_eq!(SizeClass::Large.capacity(), 65536);
        assert_eq!(SizeClass::XLarge.capacity(), 1048576);
        assert_eq!(SizeClass::Oversized.capacity(), 0);
    }

    #[test]
    fn test_buffer_pool_creation() {
        let pool = BufferPool::new();
        let stats = pool.stats();
        assert_eq!(stats.pooled_buffers, 0);
    }

    #[test]
    fn test_buffer_pool_with_preallocation() {
        let config = BufferPoolConfig {
            max_small_buffers: 10,
            max_medium_buffers: 5,
            max_large_buffers: 2,
            max_xlarge_buffers: 1,
            preallocate: true,
            detailed_metrics: true,
        };
        let pool = BufferPool::with_config(config);
        let stats = pool.stats();
        assert_eq!(stats.pooled_buffers, 18); // 10 + 5 + 2 + 1
    }

    #[test]
    fn test_acquire_small_buffer() {
        let pool = BufferPool::new();
        let buffer = pool.acquire(100);

        assert_eq!(buffer.size_class(), SizeClass::Small);
        assert_eq!(buffer.capacity(), 256);
        assert!(buffer.is_pooled());
    }

    #[test]
    fn test_acquire_medium_buffer() {
        let pool = BufferPool::new();
        let buffer = pool.acquire(1000);

        assert_eq!(buffer.size_class(), SizeClass::Medium);
        assert_eq!(buffer.capacity(), 4096);
    }

    #[test]
    fn test_acquire_large_buffer() {
        let pool = BufferPool::new();
        let buffer = pool.acquire(10000);

        assert_eq!(buffer.size_class(), SizeClass::Large);
        assert_eq!(buffer.capacity(), 65536);
    }

    #[test]
    fn test_acquire_xlarge_buffer() {
        let pool = BufferPool::new();
        let buffer = pool.acquire(100000);

        assert_eq!(buffer.size_class(), SizeClass::XLarge);
        assert_eq!(buffer.capacity(), 1048576);
    }

    #[test]
    fn test_acquire_oversized_buffer() {
        let pool = BufferPool::new();
        let buffer = pool.acquire(2000000);

        assert_eq!(buffer.size_class(), SizeClass::Oversized);
        assert!(!buffer.is_pooled());
    }

    #[test]
    fn test_buffer_reuse() {
        let pool = BufferPool::new();

        // Acquire and release a buffer
        {
            let mut buffer = pool.acquire(100);
            buffer.extend_from_slice(b"test data");
        }

        // Acquire again - should reuse the released buffer
        let buffer = pool.acquire(100);
        assert_eq!(buffer.len(), 0); // Should be cleared
        assert_eq!(buffer.capacity(), 256);

        let stats = pool.stats();
        assert_eq!(stats.total_acquisitions, 2);
        assert!(stats.overall_hit_rate > 0.0);
    }

    #[test]
    fn test_acquire_with_data() {
        let pool = BufferPool::new();
        let data = b"hello world";
        let buffer = pool.acquire_with_data(data);

        assert_eq!(&buffer[..], data);
        assert_eq!(buffer.size_class(), SizeClass::Small);
    }

    #[test]
    fn test_buffer_take() {
        let pool = BufferPool::new();
        let mut buffer = pool.acquire(100);
        buffer.extend_from_slice(b"data");

        let vec = buffer.take();
        assert_eq!(&vec[..], b"data");

        // Pool should not get the buffer back
        let stats = pool.stats();
        assert_eq!(stats.total_releases, 0);
    }

    #[test]
    fn test_pool_stats() {
        let pool = BufferPool::new();

        // Acquire some buffers
        let _b1 = pool.acquire(100);
        let _b2 = pool.acquire(1000);
        let _b3 = pool.acquire(10000);

        let stats = pool.stats();
        assert_eq!(stats.total_acquisitions, 3);
    }

    #[test]
    fn test_config_presets() {
        let high = BufferPoolConfig::high_throughput();
        assert_eq!(high.max_small_buffers, 5000);
        assert!(high.preallocate);

        let efficient = BufferPoolConfig::memory_efficient();
        assert_eq!(efficient.max_small_buffers, 200);
        assert!(!efficient.preallocate);
    }

    #[test]
    fn test_max_memory_calculation() {
        let config = BufferPoolConfig::default();
        let expected = 1000 * 256 + 500 * 4096 + 100 * 65536 + 10 * 1048576;
        assert_eq!(config.max_memory_bytes(), expected);
    }

    #[test]
    fn test_pool_clear() {
        let pool = BufferPool::new();

        // Acquire and release buffers to populate pool
        drop(pool.acquire(100));
        drop(pool.acquire(1000));

        let stats_before = pool.stats();
        assert!(stats_before.pooled_buffers > 0);

        pool.clear();

        let stats_after = pool.stats();
        assert_eq!(stats_after.pooled_buffers, 0);
    }

    #[test]
    fn test_deref_and_deref_mut() {
        let pool = BufferPool::new();
        let mut buffer = pool.acquire(100);

        // Test DerefMut
        buffer.push(1);
        buffer.push(2);
        buffer.push(3);

        // Test Deref
        assert_eq!(buffer.len(), 3);
        assert_eq!(&buffer[..], &[1, 2, 3]);
    }

    #[test]
    fn test_hit_rate() {
        let pool = BufferPool::new();

        // First acquisition - miss
        drop(pool.acquire(100));

        // Second acquisition - hit (buffer was returned)
        drop(pool.acquire(100));

        let stats = pool.stats();
        // One miss, one hit = 50% hit rate
        assert!((stats.hit_rates[0] - 0.5).abs() < 0.01);
    }
}
