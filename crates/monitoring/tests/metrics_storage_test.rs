//! Tests for storage metrics functionality
//!
//! This module tests storage operation metrics tracking including operation
//! performance, cache hit ratios, and connection pool status.

use orcher_monitoring::metrics::storage::{
    track_storage_operation, update_cache_hit_ratio, update_connection_pool_size,
};

#[tokio::test]
async fn test_track_storage_operation_success() {
    let result = track_storage_operation("read", "task", async { Ok("success") }).await;

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "success");
}

#[tokio::test]
async fn test_track_storage_operation_failure() {
    let result: orcher_monitoring::error::Result<&str> =
        track_storage_operation("write", "workflow", async {
            Err(
                orcher_monitoring::error::MonitoringError::metric_collection_error(
                    "storage_operation",
                    "test error",
                ),
            )
        })
        .await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_track_storage_operation_different_types() {
    // Test different operation types
    let _read_result = track_storage_operation("read", "task", async { Ok(()) }).await;
    let _write_result = track_storage_operation("write", "workflow", async { Ok(()) }).await;
    let _delete_result = track_storage_operation("delete", "config", async { Ok(()) }).await;
    let _update_result = track_storage_operation("update", "user", async { Ok(()) }).await;
}

#[tokio::test]
async fn test_track_storage_operation_different_entities() {
    // Test different entity types
    let _task_result = track_storage_operation("read", "task", async { Ok("task_data") }).await;
    let _workflow_result =
        track_storage_operation("read", "workflow", async { Ok("workflow_data") }).await;
    let _config_result =
        track_storage_operation("read", "config", async { Ok("config_data") }).await;
    let _user_result = track_storage_operation("read", "user", async { Ok("user_data") }).await;
}

#[test]
fn test_update_cache_hit_ratio() {
    // Test normal cache hit ratios
    update_cache_hit_ratio(80, 20); // 80% hit ratio
    update_cache_hit_ratio(50, 50); // 50% hit ratio
    update_cache_hit_ratio(90, 10); // 90% hit ratio
}

#[test]
fn test_update_cache_hit_ratio_edge_cases() {
    // Test edge cases
    update_cache_hit_ratio(0, 0); // No operations (should not panic)
    update_cache_hit_ratio(100, 0); // 100% hit ratio
    update_cache_hit_ratio(0, 100); // 0% hit ratio
    update_cache_hit_ratio(u64::MAX, 1); // Very large numbers
}

#[test]
fn test_update_connection_pool_size() {
    // Test various pool sizes
    update_connection_pool_size(10);
    update_connection_pool_size(0);
    update_connection_pool_size(100);
    update_connection_pool_size(1000);
}

#[test]
fn test_update_connection_pool_size_edge_cases() {
    // Test edge cases
    update_connection_pool_size(0); // Empty pool
    update_connection_pool_size(usize::MAX); // Maximum pool size
}

#[tokio::test]
async fn test_storage_metrics_workflow() {
    // Test a typical storage workflow with metrics

    // Start with empty connection pool
    update_connection_pool_size(0);

    // Initialize connection pool
    update_connection_pool_size(5);

    // Perform some operations
    let _result1 = track_storage_operation("read", "task", async { Ok("task1") }).await;
    let _result2 = track_storage_operation("write", "task", async { Ok("task2") }).await;

    // Update cache metrics
    update_cache_hit_ratio(8, 2); // 80% hit ratio

    // Scale up connection pool
    update_connection_pool_size(10);

    // More operations
    let _result3 = track_storage_operation("update", "workflow", async { Ok("workflow1") }).await;

    // Update cache metrics again
    update_cache_hit_ratio(15, 5); // 75% hit ratio
}

#[tokio::test]
async fn test_concurrent_storage_operations() {
    // Test concurrent operations
    let tasks = vec![
        tokio::spawn(track_storage_operation("read", "task", async { Ok("1") })),
        tokio::spawn(track_storage_operation("write", "task", async { Ok("2") })),
        tokio::spawn(track_storage_operation("delete", "task", async { Ok("3") })),
    ];

    // Wait for all operations to complete
    for task in tasks {
        let _ = task.await.unwrap();
    }

    // Update metrics after concurrent operations
    update_cache_hit_ratio(25, 5); // 83% hit ratio
    update_connection_pool_size(15);
}

#[tokio::test]
async fn test_mixed_success_failure_operations() {
    // Test mix of successful and failed operations
    let _success = track_storage_operation("read", "task", async { Ok("success") }).await;

    let failure: orcher_monitoring::error::Result<&str> =
        track_storage_operation("write", "task", async {
            Err(
                orcher_monitoring::error::MonitoringError::metric_collection_error(
                    "storage_operation",
                    "database connection failed",
                ),
            )
        })
        .await;

    assert!(failure.is_err());

    let _another_success = track_storage_operation("read", "workflow", async { Ok("data") }).await;
}
