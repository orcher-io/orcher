//! Integration with orcher-storage for automatic query instrumentation.
//!
//! This module provides utilities to automatically instrument storage operations,
//! including query tracking, connection pool monitoring, and cache metrics.

use crate::error::Result;
use crate::metrics::storage_names;
use metrics::{counter, gauge, histogram};
use std::time::Instant;
use tracing::{debug, error, instrument};

/// Records a storage operation with automatic metrics collection.
///
/// This function tracks storage operations and emits appropriate metrics
/// for monitoring query performance and success rates.
#[instrument(skip(operation))]
pub async fn track_storage_operation<F, T>(operation_type: &str, operation: F) -> Result<T>
where
    F: std::future::Future<Output = Result<T>>,
{
    let start = Instant::now();

    counter!(
        storage_names::OPERATIONS_TOTAL,
        "operation" => operation_type.to_string()
    )
    .increment(1);

    let result = operation.await;
    let duration = start.elapsed();

    histogram!(
        storage_names::OPERATION_DURATION_SECONDS,
        "operation" => operation_type.to_string()
    )
    .record(duration.as_secs_f64());

    match &result {
        Ok(_) => {
            counter!(
                storage_names::OPERATIONS_TOTAL,
                "operation" => operation_type.to_string(),
                "result" => "success".to_string()
            )
            .increment(1);
            debug!(operation_type, duration_ms = %duration.as_millis(), "Storage operation succeeded");
        }
        Err(e) => {
            counter!(
                storage_names::OPERATIONS_TOTAL,
                "operation" => operation_type.to_string(),
                "result" => "error".to_string()
            )
            .increment(1);
            error!(operation_type, error = %e, "Storage operation failed");
        }
    }

    result
}

/// Updates connection pool size metrics.
pub fn update_pool_size(current: usize, max: usize) {
    gauge!(storage_names::CONNECTION_POOL_SIZE, "type" => "current".to_string())
        .set(current as f64);
    gauge!(storage_names::CONNECTION_POOL_SIZE, "type" => "max".to_string()).set(max as f64);
}

/// Records cache hit or miss for cache performance monitoring.
pub fn record_cache_access(hit: bool) {
    let result = if hit { "hit" } else { "miss" };
    counter!("storage_cache_accesses_total", "result" => result.to_string()).increment(1);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_track_storage_operation_success() {
        let result = track_storage_operation("select", async {
            Ok::<_, crate::error::MonitoringError>(42)
        })
        .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_track_storage_operation_failure() {
        let result = track_storage_operation("insert", async {
            Err::<(), _>(crate::error::MonitoringError::IntegrationError {
                component: "test".to_string(),
                message: "test error".to_string(),
            })
        })
        .await;

        assert!(result.is_err());
    }

    #[test]
    fn test_update_pool_size() {
        update_pool_size(5, 10);
    }

    #[test]
    fn test_record_cache_access() {
        record_cache_access(true);
        record_cache_access(false);
    }
}
