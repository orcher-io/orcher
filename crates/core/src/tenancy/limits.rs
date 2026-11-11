//! Rate limiting and resource quotas.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Resource limits for a tenant
#[derive(Debug, Clone)]
pub struct ResourceLimits {
    /// Maximum concurrent workflows
    pub max_workflows: Option<u64>,

    /// Maximum concurrent tasks
    pub max_tasks: Option<u64>,

    /// Maximum memory in bytes
    pub max_memory_bytes: Option<u64>,

    /// Maximum payload size in bytes
    pub max_payload_bytes: Option<u64>,

    /// Maximum requests per second
    pub max_rate_per_second: Option<u64>,

    /// Maximum burst size
    pub max_burst: Option<u64>,
}

impl ResourceLimits {
    /// Create unlimited resource limits
    pub fn unlimited() -> Self {
        Self {
            max_workflows: None,
            max_tasks: None,
            max_memory_bytes: None,
            max_payload_bytes: None,
            max_rate_per_second: None,
            max_burst: None,
        }
    }

    /// Create resource limits with specified values
    #[allow(clippy::new_ret_no_self)]
    pub fn new() -> ResourceLimitsBuilder {
        ResourceLimitsBuilder::new()
    }

    /// Check if a workflow limit exists
    pub fn has_workflow_limit(&self) -> bool {
        self.max_workflows.is_some()
    }

    /// Check if a rate limit exists
    pub fn has_rate_limit(&self) -> bool {
        self.max_rate_per_second.is_some()
    }
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self::unlimited()
    }
}

/// Builder for ResourceLimits
pub struct ResourceLimitsBuilder {
    limits: ResourceLimits,
}

impl ResourceLimitsBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            limits: ResourceLimits::unlimited(),
        }
    }

    /// Set maximum workflows
    pub fn max_workflows(mut self, limit: u64) -> Self {
        self.limits.max_workflows = Some(limit);
        self
    }

    /// Set maximum tasks
    pub fn max_tasks(mut self, limit: u64) -> Self {
        self.limits.max_tasks = Some(limit);
        self
    }

    /// Set maximum memory
    pub fn max_memory(mut self, bytes: u64) -> Self {
        self.limits.max_memory_bytes = Some(bytes);
        self
    }

    /// Set rate limit
    pub fn rate_limit(mut self, per_second: u64, burst: u64) -> Self {
        self.limits.max_rate_per_second = Some(per_second);
        self.limits.max_burst = Some(burst);
        self
    }

    /// Build the resource limits
    pub fn build(self) -> ResourceLimits {
        self.limits
    }
}

impl Default for ResourceLimitsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Resource quota tracking
#[derive(Debug)]
pub struct ResourceQuota {
    /// Limit for this resource
    limit: Option<u64>,

    /// Current usage
    current: AtomicU64,

    /// High water mark
    high_water_mark: AtomicU64,

    /// Total allocations
    total_allocations: AtomicU64,

    /// Total releases
    total_releases: AtomicU64,

    /// Rejected requests (over quota)
    rejections: AtomicU64,
}

impl ResourceQuota {
    /// Create a new resource quota with a limit
    pub fn new(limit: Option<u64>) -> Self {
        Self {
            limit,
            current: AtomicU64::new(0),
            high_water_mark: AtomicU64::new(0),
            total_allocations: AtomicU64::new(0),
            total_releases: AtomicU64::new(0),
            rejections: AtomicU64::new(0),
        }
    }

    /// Create an unlimited quota
    pub fn unlimited() -> Self {
        Self::new(None)
    }

    /// Try to acquire resources
    pub fn try_acquire(&self, amount: u64) -> bool {
        if let Some(limit) = self.limit {
            loop {
                let current = self.current.load(Ordering::Relaxed);
                if current + amount > limit {
                    self.rejections.fetch_add(1, Ordering::Relaxed);
                    return false;
                }

                if self
                    .current
                    .compare_exchange(
                        current,
                        current + amount,
                        Ordering::SeqCst,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    self.total_allocations.fetch_add(1, Ordering::Relaxed);

                    // Update high water mark
                    let new_current = current + amount;
                    loop {
                        let hwm = self.high_water_mark.load(Ordering::Relaxed);
                        if new_current <= hwm {
                            break;
                        }
                        if self
                            .high_water_mark
                            .compare_exchange(hwm, new_current, Ordering::SeqCst, Ordering::Relaxed)
                            .is_ok()
                        {
                            break;
                        }
                    }

                    return true;
                }
            }
        } else {
            // No limit, always allow
            self.current.fetch_add(amount, Ordering::Relaxed);
            self.total_allocations.fetch_add(1, Ordering::Relaxed);
            true
        }
    }

    /// Release resources
    pub fn release(&self, amount: u64) {
        self.current.fetch_sub(amount, Ordering::Relaxed);
        self.total_releases.fetch_add(1, Ordering::Relaxed);
    }

    /// Get current usage
    pub fn current(&self) -> u64 {
        self.current.load(Ordering::Relaxed)
    }

    /// Get the limit
    pub fn limit(&self) -> Option<u64> {
        self.limit
    }

    /// Get available capacity
    pub fn available(&self) -> Option<u64> {
        self.limit.map(|l| l.saturating_sub(self.current()))
    }

    /// Get utilization as a fraction (0.0 - 1.0)
    pub fn utilization(&self) -> Option<f64> {
        self.limit.map(|l| {
            if l == 0 {
                0.0
            } else {
                self.current() as f64 / l as f64
            }
        })
    }

    /// Get high water mark
    pub fn high_water_mark(&self) -> u64 {
        self.high_water_mark.load(Ordering::Relaxed)
    }

    /// Get rejection count
    pub fn rejections(&self) -> u64 {
        self.rejections.load(Ordering::Relaxed)
    }

    /// Reset usage (for testing)
    pub fn reset(&self) {
        self.current.store(0, Ordering::SeqCst);
    }
}

/// Token bucket rate limiter
#[derive(Debug)]
pub struct TokenBucket {
    /// Maximum tokens in the bucket
    capacity: u64,

    /// Current number of tokens
    tokens: AtomicU64,

    /// Token refill rate (tokens per second)
    refill_rate: u64,

    /// Last refill time (as nanos since an epoch)
    last_refill: AtomicU64,

    /// Total requests
    total_requests: AtomicU64,

    /// Rejected requests
    rejected_requests: AtomicU64,
}

impl TokenBucket {
    /// Create a new token bucket
    pub fn new(capacity: u64, refill_rate: u64) -> Self {
        Self {
            capacity,
            tokens: AtomicU64::new(capacity),
            refill_rate,
            last_refill: AtomicU64::new(Self::now_nanos()),
            total_requests: AtomicU64::new(0),
            rejected_requests: AtomicU64::new(0),
        }
    }

    /// Try to acquire a token
    pub fn try_acquire(&self) -> bool {
        self.try_acquire_n(1)
    }

    /// Try to acquire N tokens
    pub fn try_acquire_n(&self, n: u64) -> bool {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
        self.refill();

        loop {
            let current = self.tokens.load(Ordering::Relaxed);
            if current < n {
                self.rejected_requests.fetch_add(1, Ordering::Relaxed);
                return false;
            }

            if self
                .tokens
                .compare_exchange(current, current - n, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok()
            {
                return true;
            }
        }
    }

    /// Refill tokens based on elapsed time
    fn refill(&self) {
        let now = Self::now_nanos();
        let last = self.last_refill.load(Ordering::Relaxed);
        let elapsed_nanos = now.saturating_sub(last);

        // Calculate tokens to add
        let tokens_to_add =
            (elapsed_nanos as u128 * self.refill_rate as u128 / 1_000_000_000) as u64;

        if tokens_to_add > 0
            && self
                .last_refill
                .compare_exchange(last, now, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok()
        {
            loop {
                let current = self.tokens.load(Ordering::Relaxed);
                let new_tokens = (current + tokens_to_add).min(self.capacity);
                if current == new_tokens {
                    break;
                }
                if self
                    .tokens
                    .compare_exchange(current, new_tokens, Ordering::SeqCst, Ordering::Relaxed)
                    .is_ok()
                {
                    break;
                }
            }
        }
    }

    /// Get current token count
    pub fn tokens(&self) -> u64 {
        self.refill();
        self.tokens.load(Ordering::Relaxed)
    }

    /// Get capacity
    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    /// Get refill rate
    pub fn refill_rate(&self) -> u64 {
        self.refill_rate
    }

    /// Get rejection count
    pub fn rejections(&self) -> u64 {
        self.rejected_requests.load(Ordering::Relaxed)
    }

    /// Get total request count
    pub fn total_requests(&self) -> u64 {
        self.total_requests.load(Ordering::Relaxed)
    }

    /// Current time in nanoseconds
    fn now_nanos() -> u64 {
        // Use a monotonic reference point
        static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
        let start = START.get_or_init(Instant::now);
        start.elapsed().as_nanos() as u64
    }
}

/// Rate limiter with multiple strategies
#[derive(Debug)]
pub struct RateLimiter {
    /// Token bucket for rate limiting
    bucket: TokenBucket,

    /// Window duration for sliding window (if used)
    #[allow(dead_code)]
    window: Duration,
}

impl RateLimiter {
    /// Create a new rate limiter
    pub fn new(rate_per_second: u64, burst: u64) -> Self {
        Self {
            bucket: TokenBucket::new(burst, rate_per_second),
            window: Duration::from_secs(1),
        }
    }

    /// Try to acquire permission for a request
    pub fn try_acquire(&self) -> bool {
        self.bucket.try_acquire()
    }

    /// Try to acquire N permissions
    pub fn try_acquire_n(&self, n: u64) -> bool {
        self.bucket.try_acquire_n(n)
    }

    /// Get current available tokens
    pub fn available(&self) -> u64 {
        self.bucket.tokens()
    }

    /// Get the rate limit
    pub fn rate(&self) -> u64 {
        self.bucket.refill_rate()
    }

    /// Get the burst limit
    pub fn burst(&self) -> u64 {
        self.bucket.capacity()
    }

    /// Get rejection statistics
    pub fn rejections(&self) -> u64 {
        self.bucket.rejections()
    }

    /// Get total requests
    pub fn total_requests(&self) -> u64 {
        self.bucket.total_requests()
    }

    /// Get acceptance rate
    pub fn acceptance_rate(&self) -> f64 {
        let total = self.total_requests();
        let rejected = self.rejections();
        if total == 0 {
            1.0
        } else {
            (total - rejected) as f64 / total as f64
        }
    }
}

// ================================================================================================
// TESTS
// ================================================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resource_limits_unlimited() {
        let limits = ResourceLimits::unlimited();
        assert!(!limits.has_workflow_limit());
        assert!(!limits.has_rate_limit());
    }

    #[test]
    fn test_resource_limits_builder() {
        let limits = ResourceLimits::new()
            .max_workflows(1000)
            .max_tasks(10000)
            .max_memory(1024 * 1024)
            .rate_limit(100, 200)
            .build();

        assert_eq!(limits.max_workflows, Some(1000));
        assert_eq!(limits.max_tasks, Some(10000));
        assert_eq!(limits.max_rate_per_second, Some(100));
        assert_eq!(limits.max_burst, Some(200));
        assert!(limits.has_workflow_limit());
        assert!(limits.has_rate_limit());
    }

    #[test]
    fn test_resource_quota_basic() {
        let quota = ResourceQuota::new(Some(100));

        assert!(quota.try_acquire(50));
        assert_eq!(quota.current(), 50);
        assert_eq!(quota.available(), Some(50));

        assert!(quota.try_acquire(50));
        assert_eq!(quota.current(), 100);
        assert_eq!(quota.available(), Some(0));

        // Should fail - over quota
        assert!(!quota.try_acquire(1));
        assert_eq!(quota.rejections(), 1);

        // Release and try again
        quota.release(50);
        assert_eq!(quota.current(), 50);
        assert!(quota.try_acquire(25));
    }

    #[test]
    fn test_resource_quota_unlimited() {
        let quota = ResourceQuota::unlimited();

        // Should always succeed
        assert!(quota.try_acquire(1_000_000));
        assert!(quota.try_acquire(1_000_000));
        assert_eq!(quota.current(), 2_000_000);
        assert!(quota.available().is_none());
    }

    #[test]
    fn test_resource_quota_utilization() {
        let quota = ResourceQuota::new(Some(100));

        assert!(quota.try_acquire(25));
        assert!((quota.utilization().unwrap() - 0.25).abs() < 0.01);

        assert!(quota.try_acquire(25));
        assert!((quota.utilization().unwrap() - 0.50).abs() < 0.01);
    }

    #[test]
    fn test_resource_quota_high_water_mark() {
        let quota = ResourceQuota::new(Some(100));

        quota.try_acquire(30);
        quota.try_acquire(40);
        assert_eq!(quota.high_water_mark(), 70);

        quota.release(50);
        assert_eq!(quota.current(), 20);
        assert_eq!(quota.high_water_mark(), 70); // HWM doesn't decrease
    }

    #[test]
    fn test_token_bucket_basic() {
        let bucket = TokenBucket::new(10, 100);

        // Should have initial capacity
        assert_eq!(bucket.capacity(), 10);

        // Acquire tokens
        assert!(bucket.try_acquire());
        assert!(bucket.try_acquire_n(5));

        // Check remaining
        assert!(bucket.tokens() <= 4);
    }

    #[test]
    fn test_token_bucket_exhaustion() {
        let bucket = TokenBucket::new(5, 0); // No refill

        // Use all tokens
        for _ in 0..5 {
            assert!(bucket.try_acquire());
        }

        // Should fail
        assert!(!bucket.try_acquire());
        assert_eq!(bucket.rejections(), 1);
    }

    #[test]
    fn test_rate_limiter() {
        let limiter = RateLimiter::new(100, 10);

        assert_eq!(limiter.rate(), 100);
        assert_eq!(limiter.burst(), 10);

        // Initial burst should succeed
        for _ in 0..10 {
            assert!(limiter.try_acquire());
        }

        // Should be rate limited now
        assert!(!limiter.try_acquire());
    }

    #[test]
    fn test_rate_limiter_acceptance_rate() {
        let limiter = RateLimiter::new(100, 5);

        // Use all burst
        for _ in 0..5 {
            limiter.try_acquire();
        }

        // These will fail
        for _ in 0..5 {
            limiter.try_acquire();
        }

        // 5 accepted, 5 rejected = 50% acceptance
        assert!((limiter.acceptance_rate() - 0.5).abs() < 0.01);
    }
}
