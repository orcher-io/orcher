//! Sync-match dispatch metrics.

use metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram};
use std::time::{Duration, Instant};
use tracing::debug;

pub mod names {
    pub const SYNC_MATCH_TOTAL: &str = "orcher_sync_match_total";
    pub const SYNC_MATCH_SUCCESS_TOTAL: &str = "orcher_sync_match_success_total";
    pub const SYNC_MATCH_MISS_TOTAL: &str = "orcher_sync_match_miss_total";
    pub const AFFINITY_ROUTE_TOTAL: &str = "orcher_affinity_route_total";
    pub const AFFINITY_ROUTE_SUCCESS_TOTAL: &str = "orcher_affinity_route_success_total";
    pub const AFFINITY_ROUTE_MISS_TOTAL: &str = "orcher_affinity_route_miss_total";
    pub const DISPATCH_LATENCY_SECONDS: &str = "orcher_dispatch_latency_seconds";
    pub const WAITER_WAIT_DURATION_SECONDS: &str = "orcher_waiter_wait_duration_seconds";
    pub const WAITER_REGISTERED_TOTAL: &str = "orcher_waiter_registered_total";
    pub const WAITER_SERVED_TOTAL: &str = "orcher_waiter_served_total";
    pub const WAITER_EXPIRED_TOTAL: &str = "orcher_waiter_expired_total";
    pub const WAITER_REJECTED_TOTAL: &str = "orcher_waiter_rejected_total";
    pub const WAITER_DISPATCH_FAILURE_TOTAL: &str = "orcher_waiter_dispatch_failure_total";
    pub const WAITER_QUEUE_DEPTH: &str = "orcher_waiter_queue_depth";
    pub const STATE_CACHE_HIT_TOTAL: &str = "orcher_state_cache_hit_total";
    pub const STATE_CACHE_MISS_TOTAL: &str = "orcher_state_cache_miss_total";
    pub const STATE_CACHE_EVICTION_TOTAL: &str = "orcher_state_cache_eviction_total";
    pub const STATE_CACHE_SIZE: &str = "orcher_state_cache_size";
    pub const AFFINITY_LEASE_COUNT: &str = "orcher_affinity_lease_count";
    pub const AFFINITY_LEASE_EXPIRED_TOTAL: &str = "orcher_affinity_lease_expired_total";
    pub const REDIS_FALLBACK_TOTAL: &str = "orcher_sync_match_redis_fallback_total";
    pub const FAST_PATH_SUCCESS_TOTAL: &str = "orcher_fast_path_success_total";
    /// Which path actually delivered a task to a polling worker.
    /// `path` label ∈ {sync_match, claimed, legacy_claimed}.
    pub const POLL_SERVED_TOTAL: &str = "orcher_poll_served_total";
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncMatchOutcome {
    /// Response built at dispatch time with cached state.
    FastPath,
    Matched,
    AffinityMatched,
    Queued,
    Error,
}

impl SyncMatchOutcome {
    pub fn as_str(&self) -> &'static str {
        match self {
            SyncMatchOutcome::FastPath => "fast_path",
            SyncMatchOutcome::Matched => "matched",
            SyncMatchOutcome::AffinityMatched => "affinity_matched",
            SyncMatchOutcome::Queued => "queued",
            SyncMatchOutcome::Error => "error",
        }
    }

    pub fn is_sync_matched(&self) -> bool {
        matches!(
            self,
            SyncMatchOutcome::FastPath
                | SyncMatchOutcome::Matched
                | SyncMatchOutcome::AffinityMatched
        )
    }

    pub fn is_fast_path(&self) -> bool {
        matches!(self, SyncMatchOutcome::FastPath)
    }
}

// ============================================================================
// Metric Registration
// ============================================================================

/// Register metric descriptions. Call once at startup.
pub fn describe_sync_match_metrics() {
    describe_counter!(
        names::SYNC_MATCH_TOTAL,
        "Total number of sync-match dispatch attempts"
    );
    describe_counter!(
        names::SYNC_MATCH_SUCCESS_TOTAL,
        "Number of successful sync-match dispatches (work sent directly to waiter)"
    );
    describe_counter!(
        names::SYNC_MATCH_MISS_TOTAL,
        "Number of sync-match misses (no waiter available)"
    );

    describe_counter!(
        names::AFFINITY_ROUTE_TOTAL,
        "Total number of affinity routing attempts"
    );
    describe_counter!(
        names::AFFINITY_ROUTE_SUCCESS_TOTAL,
        "Number of successful affinity routes to preferred worker"
    );
    describe_counter!(
        names::AFFINITY_ROUTE_MISS_TOTAL,
        "Number of affinity routing misses (preferred worker not available)"
    );

    describe_histogram!(
        names::DISPATCH_LATENCY_SECONDS,
        "Dispatch latency from XADD to worker notification in seconds"
    );
    describe_histogram!(
        names::WAITER_WAIT_DURATION_SECONDS,
        "Time workers spend waiting for work in seconds"
    );

    describe_counter!(
        names::WAITER_REGISTERED_TOTAL,
        "Total number of workers registered as waiters"
    );
    describe_counter!(
        names::WAITER_SERVED_TOTAL,
        "Number of waiters that successfully received work"
    );
    describe_counter!(
        names::WAITER_EXPIRED_TOTAL,
        "Number of waiters that expired without receiving work"
    );
    describe_counter!(
        names::WAITER_REJECTED_TOTAL,
        "Number of waiters rejected due to queue full"
    );
    describe_counter!(
        names::WAITER_DISPATCH_FAILURE_TOTAL,
        "Number of waiter dispatch failures (channel closed)"
    );

    describe_gauge!(
        names::WAITER_QUEUE_DEPTH,
        "Current number of workers waiting for work per queue"
    );

    describe_counter!(
        names::STATE_CACHE_HIT_TOTAL,
        "Number of workflow state cache hits"
    );
    describe_counter!(
        names::STATE_CACHE_MISS_TOTAL,
        "Number of workflow state cache misses"
    );
    describe_counter!(
        names::STATE_CACHE_EVICTION_TOTAL,
        "Number of workflow state cache evictions"
    );
    describe_gauge!(
        names::STATE_CACHE_SIZE,
        "Current number of entries in the workflow state cache"
    );

    describe_gauge!(
        names::AFFINITY_LEASE_COUNT,
        "Current number of active worker affinity leases"
    );
    describe_counter!(
        names::AFFINITY_LEASE_EXPIRED_TOTAL,
        "Number of affinity leases that expired"
    );

    describe_counter!(
        names::REDIS_FALLBACK_TOTAL,
        "Number of times sync-match fell back to Redis queuing"
    );

    describe_counter!(
        names::FAST_PATH_SUCCESS_TOTAL,
        "Number of fast-path dispatches with cached state (no DB read at poll time)"
    );
    describe_counter!(
        names::POLL_SERVED_TOTAL,
        "Tasks delivered to polling workers, labelled by delivery path (sync_match|claimed|legacy_claimed)"
    );
}

// ============================================================================
// Sync-Match Dispatch Metrics
// ============================================================================

pub fn record_sync_match_attempt(task_queue: &str, namespace: &str) {
    counter!(
        names::SYNC_MATCH_TOTAL,
        "task_queue" => task_queue.to_string(),
        "namespace" => namespace.to_string()
    )
    .increment(1);
}

pub fn record_sync_match_success(task_queue: &str, namespace: &str, latency: Duration) {
    counter!(
        names::SYNC_MATCH_SUCCESS_TOTAL,
        "task_queue" => task_queue.to_string(),
        "namespace" => namespace.to_string()
    )
    .increment(1);

    histogram!(
        names::DISPATCH_LATENCY_SECONDS,
        "task_queue" => task_queue.to_string(),
        "namespace" => namespace.to_string(),
        "outcome" => "matched"
    )
    .record(latency.as_secs_f64());

    debug!(
        task_queue = %task_queue,
        namespace = %namespace,
        latency_us = latency.as_micros(),
        "Sync-match success"
    );
}

pub fn record_sync_match_miss(task_queue: &str, namespace: &str) {
    counter!(
        names::SYNC_MATCH_MISS_TOTAL,
        "task_queue" => task_queue.to_string(),
        "namespace" => namespace.to_string()
    )
    .increment(1);

    counter!(
        names::REDIS_FALLBACK_TOTAL,
        "task_queue" => task_queue.to_string(),
        "namespace" => namespace.to_string()
    )
    .increment(1);
}

/// Record which path delivered a task to a polling worker.
///
/// `path` is one of `sync_match` (delivered directly to a parked waiter),
/// `claimed` (served by the PostgreSQL poll/claim path), or `legacy_claimed`
/// (served by the legacy coordinator poll). The ratio makes it observable
/// whether the sync-match fast path is actually engaging.
pub fn record_poll_served(path: &str, task_queue: &str, namespace: &str) {
    counter!(
        names::POLL_SERVED_TOTAL,
        "path" => path.to_string(),
        "task_queue" => task_queue.to_string(),
        "namespace" => namespace.to_string()
    )
    .increment(1);
}

pub fn record_dispatch_latency(
    task_queue: &str,
    namespace: &str,
    latency: Duration,
    outcome: SyncMatchOutcome,
) {
    histogram!(
        names::DISPATCH_LATENCY_SECONDS,
        "task_queue" => task_queue.to_string(),
        "namespace" => namespace.to_string(),
        "outcome" => outcome.as_str().to_string()
    )
    .record(latency.as_secs_f64());
}

// ============================================================================
// Fast-Path Metrics
// ============================================================================

/// Also counts as a sync-match success.
pub fn record_fast_path_success(task_queue: &str, namespace: &str, latency: Duration) {
    counter!(
        names::FAST_PATH_SUCCESS_TOTAL,
        "task_queue" => task_queue.to_string(),
        "namespace" => namespace.to_string()
    )
    .increment(1);

    // Also count as sync-match success
    counter!(
        names::SYNC_MATCH_SUCCESS_TOTAL,
        "task_queue" => task_queue.to_string(),
        "namespace" => namespace.to_string()
    )
    .increment(1);

    histogram!(
        names::DISPATCH_LATENCY_SECONDS,
        "task_queue" => task_queue.to_string(),
        "namespace" => namespace.to_string(),
        "outcome" => "fast_path"
    )
    .record(latency.as_secs_f64());

    debug!(
        task_queue = %task_queue,
        namespace = %namespace,
        latency_us = latency.as_micros(),
        "Fast-path dispatch success (cached state)"
    );
}

// ============================================================================
// Affinity Routing Metrics
// ============================================================================

pub fn record_affinity_attempt(task_queue: &str, namespace: &str) {
    counter!(
        names::AFFINITY_ROUTE_TOTAL,
        "task_queue" => task_queue.to_string(),
        "namespace" => namespace.to_string()
    )
    .increment(1);
}

pub fn record_affinity_success(task_queue: &str, namespace: &str, latency: Duration) {
    counter!(
        names::AFFINITY_ROUTE_SUCCESS_TOTAL,
        "task_queue" => task_queue.to_string(),
        "namespace" => namespace.to_string()
    )
    .increment(1);

    histogram!(
        names::DISPATCH_LATENCY_SECONDS,
        "task_queue" => task_queue.to_string(),
        "namespace" => namespace.to_string(),
        "outcome" => "affinity_matched"
    )
    .record(latency.as_secs_f64());

    debug!(
        task_queue = %task_queue,
        namespace = %namespace,
        latency_us = latency.as_micros(),
        "Affinity route success"
    );
}

pub fn record_affinity_miss(task_queue: &str, namespace: &str) {
    counter!(
        names::AFFINITY_ROUTE_MISS_TOTAL,
        "task_queue" => task_queue.to_string(),
        "namespace" => namespace.to_string()
    )
    .increment(1);
}

// ============================================================================
// Waiter Lifecycle Metrics
// ============================================================================

pub fn track_waiter_registered(task_queue: &str, namespace: &str) {
    counter!(
        names::WAITER_REGISTERED_TOTAL,
        "task_queue" => task_queue.to_string(),
        "namespace" => namespace.to_string()
    )
    .increment(1);
}

pub fn track_waiter_served(task_queue: &str, namespace: &str, wait_duration: Duration) {
    counter!(
        names::WAITER_SERVED_TOTAL,
        "task_queue" => task_queue.to_string(),
        "namespace" => namespace.to_string()
    )
    .increment(1);

    histogram!(
        names::WAITER_WAIT_DURATION_SECONDS,
        "task_queue" => task_queue.to_string(),
        "namespace" => namespace.to_string(),
        "outcome" => "served"
    )
    .record(wait_duration.as_secs_f64());
}

pub fn track_waiter_expired(task_queue: &str, namespace: &str, wait_duration: Duration) {
    counter!(
        names::WAITER_EXPIRED_TOTAL,
        "task_queue" => task_queue.to_string(),
        "namespace" => namespace.to_string()
    )
    .increment(1);

    histogram!(
        names::WAITER_WAIT_DURATION_SECONDS,
        "task_queue" => task_queue.to_string(),
        "namespace" => namespace.to_string(),
        "outcome" => "expired"
    )
    .record(wait_duration.as_secs_f64());
}

pub fn track_waiter_rejected(task_queue: &str, namespace: &str) {
    counter!(
        names::WAITER_REJECTED_TOTAL,
        "task_queue" => task_queue.to_string(),
        "namespace" => namespace.to_string()
    )
    .increment(1);
}

pub fn track_waiter_dispatch_failure(task_queue: &str, namespace: &str) {
    counter!(
        names::WAITER_DISPATCH_FAILURE_TOTAL,
        "task_queue" => task_queue.to_string(),
        "namespace" => namespace.to_string()
    )
    .increment(1);
}

pub fn update_waiter_queue_depth(task_queue: &str, namespace: &str, depth: usize) {
    gauge!(
        names::WAITER_QUEUE_DEPTH,
        "task_queue" => task_queue.to_string(),
        "namespace" => namespace.to_string()
    )
    .set(depth as f64);
}

pub fn increment_waiter_queue_depth(task_queue: &str, namespace: &str) {
    // Note: metrics crate doesn't have increment on gauge with labels,
    // so we use a different approach - track via counter
    counter!(
        names::WAITER_REGISTERED_TOTAL,
        "task_queue" => task_queue.to_string(),
        "namespace" => namespace.to_string()
    )
    .increment(1);
}

pub fn decrement_waiter_queue_depth(task_queue: &str, namespace: &str) {
    // Note: handled by explicit update_waiter_queue_depth calls
    let _ = (task_queue, namespace);
}

// ============================================================================
// State Cache Metrics
// ============================================================================

pub fn record_state_cache_hit(task_queue: &str, namespace: &str) {
    counter!(
        names::STATE_CACHE_HIT_TOTAL,
        "task_queue" => task_queue.to_string(),
        "namespace" => namespace.to_string()
    )
    .increment(1);
}

pub fn record_state_cache_miss(task_queue: &str, namespace: &str) {
    counter!(
        names::STATE_CACHE_MISS_TOTAL,
        "task_queue" => task_queue.to_string(),
        "namespace" => namespace.to_string()
    )
    .increment(1);
}

pub fn record_state_cache_eviction() {
    counter!(names::STATE_CACHE_EVICTION_TOTAL).increment(1);
}

pub fn update_state_cache_size(size: usize) {
    gauge!(names::STATE_CACHE_SIZE).set(size as f64);
}

// ============================================================================
// Affinity Lease Metrics
// ============================================================================

pub fn update_affinity_lease_count(count: usize) {
    gauge!(names::AFFINITY_LEASE_COUNT).set(count as f64);
}

pub fn record_affinity_lease_expired(task_queue: &str, namespace: &str) {
    counter!(
        names::AFFINITY_LEASE_EXPIRED_TOTAL,
        "task_queue" => task_queue.to_string(),
        "namespace" => namespace.to_string()
    )
    .increment(1);
}

// ============================================================================
// RAII Guards
// ============================================================================

/// RAII guard that records sync-match dispatch duration and outcome on drop.
pub struct SyncMatchGuard {
    task_queue: String,
    namespace: String,
    start: Instant,
    outcome: Option<SyncMatchOutcome>,
}

impl SyncMatchGuard {
    pub fn new(task_queue: &str, namespace: &str) -> Self {
        record_sync_match_attempt(task_queue, namespace);
        Self {
            task_queue: task_queue.to_string(),
            namespace: namespace.to_string(),
            start: Instant::now(),
            outcome: None,
        }
    }

    pub fn set_outcome(&mut self, outcome: SyncMatchOutcome) {
        self.outcome = Some(outcome);
    }

    pub fn matched(&mut self) {
        self.outcome = Some(SyncMatchOutcome::Matched);
    }

    pub fn affinity_matched(&mut self) {
        self.outcome = Some(SyncMatchOutcome::AffinityMatched);
    }

    pub fn queued(&mut self) {
        self.outcome = Some(SyncMatchOutcome::Queued);
    }

    pub fn error(&mut self) {
        self.outcome = Some(SyncMatchOutcome::Error);
    }

    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }
}

impl Drop for SyncMatchGuard {
    fn drop(&mut self) {
        let elapsed = self.start.elapsed();
        let outcome = self.outcome.unwrap_or(SyncMatchOutcome::Error);

        record_dispatch_latency(&self.task_queue, &self.namespace, elapsed, outcome);

        match outcome {
            SyncMatchOutcome::FastPath => {
                record_sync_match_success(&self.task_queue, &self.namespace, elapsed);
            }
            SyncMatchOutcome::Matched => {
                record_sync_match_success(&self.task_queue, &self.namespace, elapsed);
            }
            SyncMatchOutcome::AffinityMatched => {
                record_affinity_success(&self.task_queue, &self.namespace, elapsed);
            }
            SyncMatchOutcome::Queued => {
                record_sync_match_miss(&self.task_queue, &self.namespace);
            }
            SyncMatchOutcome::Error => {
                // Error case - latency already recorded above
            }
        }
    }
}

/// RAII guard that records waiter lifetime on drop.
pub struct WaiterGuard {
    task_queue: String,
    namespace: String,
    start: Instant,
    served: bool,
}

impl WaiterGuard {
    pub fn new(task_queue: &str, namespace: &str) -> Self {
        track_waiter_registered(task_queue, namespace);
        Self {
            task_queue: task_queue.to_string(),
            namespace: namespace.to_string(),
            start: Instant::now(),
            served: false,
        }
    }

    pub fn served(&mut self) {
        self.served = true;
    }

    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }
}

impl Drop for WaiterGuard {
    fn drop(&mut self) {
        let wait_duration = self.start.elapsed();

        if self.served {
            track_waiter_served(&self.task_queue, &self.namespace, wait_duration);
        } else {
            track_waiter_expired(&self.task_queue, &self.namespace, wait_duration);
        }
    }
}

// ============================================================================
// Aggregate Statistics
// ============================================================================

#[derive(Debug, Clone, Default)]
pub struct SyncMatchStats {
    pub total_attempts: u64,
    pub successes: u64,
    pub misses: u64,
    pub affinity_attempts: u64,
    pub affinity_successes: u64,
    pub waiter_queue_depth: u64,
    pub waiters_registered: u64,
    pub waiters_served: u64,
    pub waiters_expired: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub affinity_leases: u64,
}

impl SyncMatchStats {
    /// 0.0 to 1.0
    pub fn sync_match_rate(&self) -> f64 {
        if self.total_attempts == 0 {
            0.0
        } else {
            self.successes as f64 / self.total_attempts as f64
        }
    }

    /// 0.0 to 1.0
    pub fn affinity_hit_rate(&self) -> f64 {
        if self.affinity_attempts == 0 {
            0.0
        } else {
            self.affinity_successes as f64 / self.affinity_attempts as f64
        }
    }

    /// 0.0 to 1.0
    pub fn cache_hit_rate(&self) -> f64 {
        let total = self.cache_hits + self.cache_misses;
        if total == 0 {
            0.0
        } else {
            self.cache_hits as f64 / total as f64
        }
    }

    /// 0.0 to 1.0
    pub fn waiter_service_rate(&self) -> f64 {
        if self.waiters_registered == 0 {
            0.0
        } else {
            self.waiters_served as f64 / self.waiters_registered as f64
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sync_match_outcome_as_str() {
        assert_eq!(SyncMatchOutcome::Matched.as_str(), "matched");
        assert_eq!(
            SyncMatchOutcome::AffinityMatched.as_str(),
            "affinity_matched"
        );
        assert_eq!(SyncMatchOutcome::Queued.as_str(), "queued");
        assert_eq!(SyncMatchOutcome::Error.as_str(), "error");
    }

    #[test]
    fn test_sync_match_outcome_is_sync_matched() {
        assert!(SyncMatchOutcome::Matched.is_sync_matched());
        assert!(SyncMatchOutcome::AffinityMatched.is_sync_matched());
        assert!(!SyncMatchOutcome::Queued.is_sync_matched());
        assert!(!SyncMatchOutcome::Error.is_sync_matched());
    }

    #[test]
    fn test_describe_sync_match_metrics() {
        // Just verify it doesn't panic
        describe_sync_match_metrics();
    }

    #[test]
    fn test_record_sync_match_attempt() {
        // Just verify it doesn't panic
        record_sync_match_attempt("test-queue", "test-namespace");
    }

    #[test]
    fn test_record_sync_match_success() {
        // Just verify it doesn't panic
        record_sync_match_success("test-queue", "test-namespace", Duration::from_micros(500));
    }

    #[test]
    fn test_record_sync_match_miss() {
        // Just verify it doesn't panic
        record_sync_match_miss("test-queue", "test-namespace");
    }

    #[test]
    fn test_waiter_lifecycle_metrics() {
        // Just verify they don't panic
        track_waiter_registered("test-queue", "test-namespace");
        track_waiter_served("test-queue", "test-namespace", Duration::from_secs(1));
        track_waiter_expired("test-queue", "test-namespace", Duration::from_secs(30));
        track_waiter_rejected("test-queue", "test-namespace");
        track_waiter_dispatch_failure("test-queue", "test-namespace");
    }

    #[test]
    fn test_waiter_queue_depth_metrics() {
        update_waiter_queue_depth("test-queue", "test-namespace", 5);
        increment_waiter_queue_depth("test-queue", "test-namespace");
        decrement_waiter_queue_depth("test-queue", "test-namespace");
    }

    #[test]
    fn test_state_cache_metrics() {
        record_state_cache_hit("test-queue", "test-namespace");
        record_state_cache_miss("test-queue", "test-namespace");
        record_state_cache_eviction();
        update_state_cache_size(100);
    }

    #[test]
    fn test_affinity_metrics() {
        record_affinity_attempt("test-queue", "test-namespace");
        record_affinity_success("test-queue", "test-namespace", Duration::from_micros(100));
        record_affinity_miss("test-queue", "test-namespace");
        update_affinity_lease_count(10);
        record_affinity_lease_expired("test-queue", "test-namespace");
    }

    #[test]
    fn test_sync_match_guard() {
        {
            let mut guard = SyncMatchGuard::new("test-queue", "test-namespace");
            guard.matched();
            // Guard drops here and records metrics
        }

        {
            let mut guard = SyncMatchGuard::new("test-queue", "test-namespace");
            guard.queued();
        }

        {
            let mut guard = SyncMatchGuard::new("test-queue", "test-namespace");
            guard.affinity_matched();
        }
    }

    #[test]
    fn test_waiter_guard() {
        {
            let mut guard = WaiterGuard::new("test-queue", "test-namespace");
            guard.served();
            // Guard drops here and records metrics
        }

        {
            let _guard = WaiterGuard::new("test-queue", "test-namespace");
            // Guard drops without being served (expired)
        }
    }

    #[test]
    fn test_sync_match_stats() {
        let stats = SyncMatchStats {
            total_attempts: 100,
            successes: 80,
            misses: 20,
            affinity_attempts: 50,
            affinity_successes: 40,
            waiter_queue_depth: 5,
            waiters_registered: 100,
            waiters_served: 90,
            waiters_expired: 10,
            cache_hits: 150,
            cache_misses: 50,
            affinity_leases: 25,
        };

        assert!((stats.sync_match_rate() - 0.8).abs() < 0.001);
        assert!((stats.affinity_hit_rate() - 0.8).abs() < 0.001);
        assert!((stats.cache_hit_rate() - 0.75).abs() < 0.001);
        assert!((stats.waiter_service_rate() - 0.9).abs() < 0.001);
    }

    #[test]
    fn test_sync_match_stats_zero_division() {
        let stats = SyncMatchStats::default();

        assert_eq!(stats.sync_match_rate(), 0.0);
        assert_eq!(stats.affinity_hit_rate(), 0.0);
        assert_eq!(stats.cache_hit_rate(), 0.0);
        assert_eq!(stats.waiter_service_rate(), 0.0);
    }
}
