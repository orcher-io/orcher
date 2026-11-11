//! Coordinator Metrics
//!
//! Provides metrics tracking for coordinator operations using the `metrics` crate.

use metrics::{counter, describe_counter, describe_histogram, histogram};
use std::time::{Duration, Instant};

/// Metric name constants for coordinator metrics
pub mod coordinator_names {
    pub const STATE_UPDATES: &str = "orcher_coordinator_state_updates_total";
    pub const CACHE_HITS: &str = "orcher_coordinator_cache_hits_total";
    pub const CACHE_MISSES: &str = "orcher_coordinator_cache_misses_total";
    pub const LOCK_ACQUISITIONS: &str = "orcher_coordinator_lock_acquisitions_total";
    pub const LOCK_CONTENTIONS: &str = "orcher_coordinator_lock_contentions_total";
    pub const INVALIDATIONS_SENT: &str = "orcher_coordinator_invalidations_total";
    pub const STATE_RECONSTRUCTIONS: &str = "orcher_coordinator_state_reconstructions_total";
    pub const TIMERS_SCHEDULED: &str = "orcher_coordinator_timers_scheduled_total";
    pub const TIMERS_FIRED: &str = "orcher_coordinator_timers_fired_total";
    pub const OUTBOX_CREATED: &str = "orcher_coordinator_outbox_created_total";
    pub const OUTBOX_PROCESSED: &str = "orcher_coordinator_outbox_processed_total";
    pub const OPERATION_DURATION: &str = "orcher_coordinator_operation_duration_seconds";
}

/// Describe all coordinator metrics (call at startup)
pub fn describe_coordinator_metrics() {
    // State management metrics
    describe_counter!(
        coordinator_names::STATE_UPDATES,
        "Total state updates in coordinator"
    );
    describe_counter!(
        coordinator_names::STATE_RECONSTRUCTIONS,
        "State reconstructions in coordinator"
    );
    describe_counter!(
        coordinator_names::INVALIDATIONS_SENT,
        "Invalidations sent by coordinator"
    );

    // Cache metrics
    describe_counter!(coordinator_names::CACHE_HITS, "Cache hits in coordinator");
    describe_counter!(
        coordinator_names::CACHE_MISSES,
        "Cache misses in coordinator"
    );

    // Lock metrics
    describe_counter!(
        coordinator_names::LOCK_ACQUISITIONS,
        "Lock acquisitions in coordinator"
    );
    describe_counter!(
        coordinator_names::LOCK_CONTENTIONS,
        "Lock contentions (waited for lock) in coordinator"
    );

    // Timer metrics
    describe_counter!(
        coordinator_names::TIMERS_SCHEDULED,
        "Timers scheduled by coordinator"
    );
    describe_counter!(
        coordinator_names::TIMERS_FIRED,
        "Timers fired by coordinator"
    );

    // Outbox metrics
    describe_counter!(coordinator_names::OUTBOX_CREATED, "Outbox tasks created");
    describe_counter!(
        coordinator_names::OUTBOX_PROCESSED,
        "Outbox tasks processed"
    );

    // Operation duration histogram
    describe_histogram!(
        coordinator_names::OPERATION_DURATION,
        "Duration of coordinator operations in seconds"
    );
}

// ============================================================================
// State Management Recording Functions
// ============================================================================

/// Record a state update
pub fn record_coordinator_state_update(namespace: &str) {
    counter!(coordinator_names::STATE_UPDATES, "namespace" => namespace.to_string()).increment(1);
}

/// Record a state reconstruction
pub fn record_coordinator_state_reconstruction(namespace: &str) {
    counter!(coordinator_names::STATE_RECONSTRUCTIONS, "namespace" => namespace.to_string())
        .increment(1);
}

/// Record an invalidation sent
pub fn record_coordinator_invalidation_sent(namespace: &str) {
    counter!(coordinator_names::INVALIDATIONS_SENT, "namespace" => namespace.to_string())
        .increment(1);
}

// ============================================================================
// Cache Recording Functions
// ============================================================================

/// Record a cache hit
pub fn record_coordinator_cache_hit(namespace: &str) {
    counter!(coordinator_names::CACHE_HITS, "namespace" => namespace.to_string()).increment(1);
}

/// Record a cache miss
pub fn record_coordinator_cache_miss(namespace: &str) {
    counter!(coordinator_names::CACHE_MISSES, "namespace" => namespace.to_string()).increment(1);
}

// ============================================================================
// Lock Recording Functions
// ============================================================================

/// Record a lock acquisition
pub fn record_coordinator_lock_acquired(namespace: &str, waited: bool) {
    counter!(coordinator_names::LOCK_ACQUISITIONS, "namespace" => namespace.to_string())
        .increment(1);
    if waited {
        counter!(coordinator_names::LOCK_CONTENTIONS, "namespace" => namespace.to_string())
            .increment(1);
    }
}

// ============================================================================
// Timer Recording Functions
// ============================================================================

/// Record a timer scheduled
pub fn record_coordinator_timer_scheduled(namespace: &str) {
    counter!(coordinator_names::TIMERS_SCHEDULED, "namespace" => namespace.to_string())
        .increment(1);
}

/// Record a timer fired
pub fn record_coordinator_timer_fired(namespace: &str) {
    counter!(coordinator_names::TIMERS_FIRED, "namespace" => namespace.to_string()).increment(1);
}

// ============================================================================
// Outbox Recording Functions
// ============================================================================

/// Record an outbox task created
pub fn record_coordinator_outbox_created(namespace: &str) {
    counter!(coordinator_names::OUTBOX_CREATED, "namespace" => namespace.to_string()).increment(1);
}

/// Record an outbox task processed
pub fn record_coordinator_outbox_processed(namespace: &str) {
    counter!(coordinator_names::OUTBOX_PROCESSED, "namespace" => namespace.to_string())
        .increment(1);
}

// ============================================================================
// Operation Duration Recording
// ============================================================================

/// Record the duration of a coordinator operation
pub fn record_coordinator_operation_duration(operation: &str, namespace: &str, duration: Duration) {
    histogram!(
        coordinator_names::OPERATION_DURATION,
        "operation" => operation.to_string(),
        "namespace" => namespace.to_string()
    )
    .record(duration.as_secs_f64());
}

// ============================================================================
// RAII Guard for Operation Timing
// ============================================================================

/// RAII guard for automatic operation timing
pub struct CoordinatorOperationGuard {
    operation: String,
    namespace: String,
    start: Instant,
}

impl CoordinatorOperationGuard {
    /// Create a new operation guard
    pub fn new(operation: impl Into<String>, namespace: impl Into<String>) -> Self {
        Self {
            operation: operation.into(),
            namespace: namespace.into(),
            start: Instant::now(),
        }
    }

    /// Get elapsed time so far
    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }
}

impl Drop for CoordinatorOperationGuard {
    fn drop(&mut self) {
        record_coordinator_operation_duration(
            &self.operation,
            &self.namespace,
            self.start.elapsed(),
        );
    }
}

/// Start tracking a coordinator operation (returns a guard that records on drop)
pub fn track_coordinator_operation_domain(
    operation: impl Into<String>,
    namespace: impl Into<String>,
) -> CoordinatorOperationGuard {
    CoordinatorOperationGuard::new(operation, namespace)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_describe_metrics_does_not_panic() {
        describe_coordinator_metrics();
    }

    #[test]
    fn test_record_state_operations() {
        record_coordinator_state_update("test-ns");
        record_coordinator_state_reconstruction("test-ns");
        record_coordinator_invalidation_sent("test-ns");
    }

    #[test]
    fn test_record_cache_operations() {
        record_coordinator_cache_hit("test-ns");
        record_coordinator_cache_miss("test-ns");
    }

    #[test]
    fn test_record_lock_operations() {
        record_coordinator_lock_acquired("test-ns", false);
        record_coordinator_lock_acquired("test-ns", true);
    }

    #[test]
    fn test_record_timer_operations() {
        record_coordinator_timer_scheduled("test-ns");
        record_coordinator_timer_fired("test-ns");
    }

    #[test]
    fn test_record_outbox_operations() {
        record_coordinator_outbox_created("test-ns");
        record_coordinator_outbox_processed("test-ns");
    }

    #[test]
    fn test_record_operation_duration() {
        record_coordinator_operation_duration("test-op", "test-ns", Duration::from_millis(100));
    }

    #[test]
    fn test_operation_guard() {
        {
            let _guard = track_coordinator_operation_domain("test-operation", "test-namespace");
            // Operation runs here
            std::thread::sleep(Duration::from_millis(1));
        }
        // Guard drops and records duration
    }

    #[test]
    fn test_operation_guard_elapsed() {
        let guard = track_coordinator_operation_domain("test", "ns");
        std::thread::sleep(Duration::from_millis(10));
        assert!(guard.elapsed() >= Duration::from_millis(10));
    }
}
