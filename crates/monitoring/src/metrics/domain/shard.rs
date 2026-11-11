//! Per-Shard Metrics
//!
//! Provides metrics tracking for each shard in the system using the `metrics` crate.

use metrics::{counter, describe_counter, describe_gauge, gauge};

/// Metric name constants for shard metrics
pub mod shard_names {
    pub const WORKFLOWS_TOTAL: &str = "orcher_shard_workflows_total";
    pub const WORKFLOWS_ACTIVE: &str = "orcher_shard_workflows_active";
    pub const WORKFLOWS_COMPLETED: &str = "orcher_shard_workflows_completed_total";
    pub const WORKFLOWS_FAILED: &str = "orcher_shard_workflows_failed_total";
    pub const TASKS_DISPATCHED: &str = "orcher_shard_tasks_dispatched_total";
    pub const TASKS_COMPLETED: &str = "orcher_shard_tasks_completed_total";
    pub const TASKS_FAILED: &str = "orcher_shard_tasks_failed_total";
    pub const TIMERS_SCHEDULED: &str = "orcher_shard_timers_scheduled_total";
    pub const TIMERS_FIRED: &str = "orcher_shard_timers_fired_total";
    pub const CACHE_HITS: &str = "orcher_shard_cache_hits_total";
    pub const CACHE_MISSES: &str = "orcher_shard_cache_misses_total";
    pub const BYTES_PROCESSED: &str = "orcher_shard_bytes_processed_total";
    pub const SHARDS_TOTAL: &str = "orcher_domain_shards_total";
    pub const SHARDS_OWNED: &str = "orcher_domain_shards_owned";
}

/// Describe all shard metrics (call at startup)
pub fn describe_shard_metrics() {
    // Workflow metrics
    describe_counter!(
        shard_names::WORKFLOWS_TOTAL,
        "Total workflows started per shard"
    );
    describe_gauge!(
        shard_names::WORKFLOWS_ACTIVE,
        "Currently active workflows per shard"
    );
    describe_counter!(
        shard_names::WORKFLOWS_COMPLETED,
        "Completed workflows per shard"
    );
    describe_counter!(shard_names::WORKFLOWS_FAILED, "Failed workflows per shard");

    // Task metrics
    describe_counter!(shard_names::TASKS_DISPATCHED, "Tasks dispatched per shard");
    describe_counter!(shard_names::TASKS_COMPLETED, "Tasks completed per shard");
    describe_counter!(shard_names::TASKS_FAILED, "Tasks failed per shard");

    // Timer metrics
    describe_counter!(shard_names::TIMERS_SCHEDULED, "Timers scheduled per shard");
    describe_counter!(shard_names::TIMERS_FIRED, "Timers fired per shard");

    // Cache metrics
    describe_counter!(shard_names::CACHE_HITS, "Cache hits per shard");
    describe_counter!(shard_names::CACHE_MISSES, "Cache misses per shard");

    // Throughput metrics
    describe_counter!(shard_names::BYTES_PROCESSED, "Bytes processed per shard");

    // Shard ownership metrics
    describe_gauge!(
        shard_names::SHARDS_TOTAL,
        "Total number of shards configured"
    );
    describe_gauge!(
        shard_names::SHARDS_OWNED,
        "Number of shards owned by this instance"
    );
}

// ============================================================================
// Workflow Recording Functions
// ============================================================================

/// Record a workflow started in a shard
pub fn record_shard_workflow_started(shard_id: u32) {
    let shard_label = shard_id.to_string();
    counter!(shard_names::WORKFLOWS_TOTAL, "shard" => shard_label.clone()).increment(1);
    gauge!(shard_names::WORKFLOWS_ACTIVE, "shard" => shard_label).set(1.0);
}

/// Record a workflow completed in a shard
pub fn record_shard_workflow_completed(shard_id: u32) {
    let shard_label = shard_id.to_string();
    counter!(shard_names::WORKFLOWS_COMPLETED, "shard" => shard_label.clone()).increment(1);
    gauge!(shard_names::WORKFLOWS_ACTIVE, "shard" => shard_label).set(-1.0);
}

/// Record a workflow failed in a shard
pub fn record_shard_workflow_failed(shard_id: u32) {
    let shard_label = shard_id.to_string();
    counter!(shard_names::WORKFLOWS_FAILED, "shard" => shard_label.clone()).increment(1);
    gauge!(shard_names::WORKFLOWS_ACTIVE, "shard" => shard_label).set(-1.0);
}

// ============================================================================
// Task Recording Functions
// ============================================================================

/// Record a task dispatched from a shard
pub fn record_shard_task_dispatched(shard_id: u32) {
    counter!(shard_names::TASKS_DISPATCHED, "shard" => shard_id.to_string()).increment(1);
}

/// Record a task completed in a shard
pub fn record_shard_task_completed(shard_id: u32) {
    counter!(shard_names::TASKS_COMPLETED, "shard" => shard_id.to_string()).increment(1);
}

/// Record a task failed in a shard
pub fn record_shard_task_failed(shard_id: u32) {
    counter!(shard_names::TASKS_FAILED, "shard" => shard_id.to_string()).increment(1);
}

// ============================================================================
// Timer Recording Functions
// ============================================================================

/// Record a timer scheduled in a shard
pub fn record_shard_timer_scheduled(shard_id: u32) {
    counter!(shard_names::TIMERS_SCHEDULED, "shard" => shard_id.to_string()).increment(1);
}

/// Record a timer fired in a shard
pub fn record_shard_timer_fired(shard_id: u32) {
    counter!(shard_names::TIMERS_FIRED, "shard" => shard_id.to_string()).increment(1);
}

// ============================================================================
// Cache Recording Functions
// ============================================================================

/// Record a cache hit in a shard
pub fn record_shard_cache_hit(shard_id: u32) {
    counter!(shard_names::CACHE_HITS, "shard" => shard_id.to_string()).increment(1);
}

/// Record a cache miss in a shard
pub fn record_shard_cache_miss(shard_id: u32) {
    counter!(shard_names::CACHE_MISSES, "shard" => shard_id.to_string()).increment(1);
}

// ============================================================================
// Throughput Recording Functions
// ============================================================================

/// Record bytes processed by a shard
pub fn record_shard_bytes_processed(shard_id: u32, bytes: u64) {
    counter!(shard_names::BYTES_PROCESSED, "shard" => shard_id.to_string()).increment(bytes);
}

// ============================================================================
// Shard Ownership Functions
// ============================================================================

/// Update the total number of shards configured
pub fn update_total_shard_count(count: u64) {
    gauge!(shard_names::SHARDS_TOTAL).set(count as f64);
}

/// Update the number of shards owned by this instance
pub fn update_owned_shard_count(count: u64) {
    gauge!(shard_names::SHARDS_OWNED).set(count as f64);
}

/// Record a shard becoming owned
pub fn record_shard_owned(shard_id: u32) {
    // Initialize the shard's gauge to ensure it appears in metrics
    gauge!(shard_names::WORKFLOWS_ACTIVE, "shard" => shard_id.to_string()).set(0.0);
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_describe_metrics_does_not_panic() {
        describe_shard_metrics();
    }

    #[test]
    fn test_record_workflow_lifecycle() {
        let shard_id = 1;
        record_shard_workflow_started(shard_id);
        record_shard_workflow_completed(shard_id);
    }

    #[test]
    fn test_record_task_lifecycle() {
        let shard_id = 2;
        record_shard_task_dispatched(shard_id);
        record_shard_task_completed(shard_id);
        record_shard_task_failed(shard_id);
    }

    #[test]
    fn test_record_timer_lifecycle() {
        let shard_id = 3;
        record_shard_timer_scheduled(shard_id);
        record_shard_timer_fired(shard_id);
    }

    #[test]
    fn test_record_cache_operations() {
        let shard_id = 4;
        record_shard_cache_hit(shard_id);
        record_shard_cache_miss(shard_id);
    }

    #[test]
    fn test_shard_ownership() {
        update_total_shard_count(16);
        update_owned_shard_count(4);
        record_shard_owned(1);
    }
}
