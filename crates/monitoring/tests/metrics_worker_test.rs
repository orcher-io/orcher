//! Tests for worker metrics functionality
//!
//! This module tests worker pool metrics tracking including pool size,
//! active/idle counts, and utilization calculations.

use orcher_monitoring::metrics::worker::{
    calculate_worker_utilization, update_worker_counts, update_worker_pool_size,
};

#[test]
fn test_update_worker_pool_size() {
    // Test updating pool size with various values
    update_worker_pool_size(10);
    update_worker_pool_size(0);
    update_worker_pool_size(100);
}

#[test]
fn test_update_worker_counts() {
    // Test updating worker counts
    update_worker_counts(5, 5);
    update_worker_counts(8, 2);
    update_worker_counts(0, 10);
}

#[test]
fn test_calculate_worker_utilization() {
    // Test utilization calculation with normal values
    calculate_worker_utilization(8, 10); // 80%
    calculate_worker_utilization(5, 10); // 50%
    calculate_worker_utilization(10, 10); // 100%
}

#[test]
fn test_calculate_worker_utilization_edge_cases() {
    // Test edge cases
    calculate_worker_utilization(0, 10); // 0%
    calculate_worker_utilization(0, 0); // Should not panic
    calculate_worker_utilization(5, 0); // Should not panic
}

#[test]
fn test_worker_counts_transitions() {
    // Test multiple count updates
    update_worker_counts(0, 10);
    update_worker_counts(5, 5);
    update_worker_counts(10, 0);
    update_worker_counts(7, 3);
}
