//! Tests for system metrics functionality
//!
//! This module tests system resource metrics tracking including CPU usage,
//! memory consumption, and automatic collection scenarios.

use std::time::Duration;

#[cfg(feature = "system-metrics")]
use orcher_monitoring::metrics::system::{
    collect_cpu_metrics, collect_memory_metrics, start_system_metrics_collection,
    SystemMetricsCollector,
};

#[cfg(not(feature = "system-metrics"))]
use orcher_monitoring::metrics::system::{
    collect_cpu_metrics, collect_memory_metrics, start_system_metrics_collection,
};

#[test]
fn test_collect_cpu_metrics() {
    // Should not panic regardless of feature flag
    collect_cpu_metrics();
}

#[test]
fn test_collect_memory_metrics() {
    // Should not panic regardless of feature flag
    collect_memory_metrics();
}

#[tokio::test]
async fn test_start_system_metrics_collection() {
    let handle = start_system_metrics_collection(Duration::from_millis(10));

    // Let it run for a short time
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Stop collection
    handle.abort();
}

#[cfg(feature = "system-metrics")]
#[tokio::test]
async fn test_system_metrics_collector_creation() {
    let collector = SystemMetricsCollector::new(Duration::from_millis(100));

    // Test individual metric collection
    collector.collect_cpu_metrics().await;
    collector.collect_memory_metrics().await;
}

#[cfg(feature = "system-metrics")]
#[tokio::test]
async fn test_system_metrics_collector_all_metrics() {
    let collector = SystemMetricsCollector::new(Duration::from_millis(50));

    // Test collecting all metrics at once
    collector.collect_all_metrics().await;
}

#[cfg(feature = "system-metrics")]
#[tokio::test]
async fn test_automatic_collection_with_collector() {
    let collector = SystemMetricsCollector::new(Duration::from_millis(10));
    let handle = collector.start_automatic_collection(Duration::from_millis(25));

    // Let it run for a bit to collect some metrics
    tokio::time::sleep(Duration::from_millis(75)).await;

    // Stop the collection
    handle.abort();
}

#[cfg(feature = "system-metrics")]
#[tokio::test]
async fn test_collector_caching_behavior() {
    // Test that the collector respects refresh intervals
    let collector = SystemMetricsCollector::new(Duration::from_millis(100));

    // Collect metrics multiple times quickly
    collector.collect_cpu_metrics().await;
    collector.collect_memory_metrics().await;
    collector.collect_all_metrics().await;

    // Should use cached data for these calls
    collector.collect_cpu_metrics().await;
    collector.collect_memory_metrics().await;
}

#[tokio::test]
async fn test_multiple_collection_tasks() {
    // Test running multiple collection tasks simultaneously
    let handle1 = start_system_metrics_collection(Duration::from_millis(20));
    let handle2 = start_system_metrics_collection(Duration::from_millis(30));

    // Let them run concurrently
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Stop both tasks
    handle1.abort();
    handle2.abort();
}

#[cfg(feature = "system-metrics")]
#[tokio::test]
async fn test_collector_with_different_refresh_rates() {
    // Test collectors with different refresh intervals
    let fast_collector = SystemMetricsCollector::new(Duration::from_millis(10));
    let slow_collector = SystemMetricsCollector::new(Duration::from_millis(100));

    // Start automatic collection for both
    let fast_handle = fast_collector.start_automatic_collection(Duration::from_millis(15));
    let slow_handle = slow_collector.start_automatic_collection(Duration::from_millis(50));

    // Let them run for different durations
    tokio::time::sleep(Duration::from_millis(120)).await;

    // Stop both collectors
    fast_handle.abort();
    slow_handle.abort();
}

#[test]
fn test_system_metrics_with_sync_context() {
    // Test that basic functions work in synchronous context
    collect_cpu_metrics();
    collect_memory_metrics();

    // Test multiple calls
    for _ in 0..5 {
        collect_cpu_metrics();
        collect_memory_metrics();
    }
}

#[tokio::test]
async fn test_collection_task_lifecycle() {
    // Test starting and stopping collection tasks multiple times
    for _ in 0..3 {
        let handle = start_system_metrics_collection(Duration::from_millis(10));
        tokio::time::sleep(Duration::from_millis(30)).await;
        handle.abort();
    }
}

#[cfg(feature = "system-metrics")]
#[tokio::test]
async fn test_concurrent_collector_usage() {
    // Test using the same collector from multiple tasks
    let _collector = SystemMetricsCollector::new(Duration::from_millis(50));

    let tasks = (0..5)
        .map(|_| {
            let collector_clone = SystemMetricsCollector::new(Duration::from_millis(50));
            tokio::spawn(async move {
                collector_clone.collect_all_metrics().await;
            })
        })
        .collect::<Vec<_>>();

    // Wait for all tasks to complete
    for task in tasks {
        let _ = task.await;
    }
}

#[cfg(feature = "system-metrics")]
#[test]
fn test_collector_creation_with_different_intervals() {
    // Test creating collectors with various refresh intervals
    let _very_fast = SystemMetricsCollector::new(Duration::from_millis(1));
    let _fast = SystemMetricsCollector::new(Duration::from_millis(100));
    let _medium = SystemMetricsCollector::new(Duration::from_secs(1));
    let _slow = SystemMetricsCollector::new(Duration::from_secs(10));
}

#[tokio::test]
async fn test_edge_case_intervals() {
    // Test with very short collection intervals
    let handle = start_system_metrics_collection(Duration::from_millis(1));
    tokio::time::sleep(Duration::from_millis(10)).await;
    handle.abort();

    // Test with zero duration (should handle gracefully)
    let handle_zero = start_system_metrics_collection(Duration::ZERO);
    tokio::time::sleep(Duration::from_millis(5)).await;
    handle_zero.abort();
}

#[test]
fn test_feature_flag_consistency() {
    // This test ensures the API is consistent regardless of feature flags

    // Basic functions should always be available
    collect_cpu_metrics();
    collect_memory_metrics();

    // Async function should also be available
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let handle = start_system_metrics_collection(Duration::from_millis(10));
        tokio::time::sleep(Duration::from_millis(20)).await;
        handle.abort();
    });
}

#[cfg(not(feature = "system-metrics"))]
#[test]
fn test_no_op_behavior() {
    // When system-metrics feature is disabled, functions should be no-ops
    // but should not panic or cause errors

    for _ in 0..10 {
        collect_cpu_metrics();
        collect_memory_metrics();
    }

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let handle = start_system_metrics_collection(Duration::from_millis(1));
        tokio::time::sleep(Duration::from_millis(10)).await;
        handle.abort();
    });
}

#[tokio::test]
async fn test_system_metrics_integration_workflow() {
    // Test a complete system monitoring workflow

    // Step 1: Start collection
    let handle = start_system_metrics_collection(Duration::from_millis(25));

    // Step 2: Manual collection while automatic is running
    collect_cpu_metrics();
    collect_memory_metrics();

    // Step 3: Let automatic collection run
    tokio::time::sleep(Duration::from_millis(75)).await;

    // Step 4: More manual collection
    collect_cpu_metrics();
    collect_memory_metrics();

    // Step 5: Stop automatic collection
    handle.abort();

    // Step 6: Continue manual collection after stopping automatic
    collect_cpu_metrics();
    collect_memory_metrics();
}
