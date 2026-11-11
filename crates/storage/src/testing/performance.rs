//! # Performance Testing Utilities
//!
//! This module provides performance testing and benchmarking utilities
//! for storage operations, integrating with orcher-common performance tools.

use orcher_common::testing::PerformanceTestUtils;
use std::time::{Duration, Instant};

/// Performance metrics for storage operations
#[derive(Debug, Clone)]
pub struct StoragePerformanceMetrics {
    /// Operation name
    pub operation: String,
    /// Total duration
    pub duration: Duration,
    /// Number of operations
    pub operations: usize,
    /// Operations per second
    pub ops_per_sec: f64,
}

impl StoragePerformanceMetrics {
    /// Create new performance metrics
    pub fn new(operation: impl Into<String>, duration: Duration, operations: usize) -> Self {
        let ops_per_sec = if duration.as_secs_f64() > 0.0 {
            operations as f64 / duration.as_secs_f64()
        } else {
            0.0
        };

        Self {
            operation: operation.into(),
            duration,
            operations,
            ops_per_sec,
        }
    }
}

/// Measure storage operation performance
pub fn measure_storage_operation<F, R>(
    operation_name: &str,
    operation: F,
) -> (R, StoragePerformanceMetrics)
where
    F: FnOnce() -> R,
{
    let (result, duration) = PerformanceTestUtils::measure_time(operation);
    let metrics = StoragePerformanceMetrics::new(operation_name, duration, 1);
    (result, metrics)
}

/// Benchmark storage operation with multiple iterations
pub fn benchmark_storage_operation<F>(
    operation_name: &str,
    iterations: usize,
    mut operation: F,
) -> StoragePerformanceMetrics
where
    F: FnMut(),
{
    let start = Instant::now();
    for _ in 0..iterations {
        operation();
    }
    let duration = start.elapsed();
    StoragePerformanceMetrics::new(operation_name, duration, iterations)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_performance_metrics() {
        let metrics = StoragePerformanceMetrics::new("test_op", Duration::from_secs(1), 100);
        assert_eq!(metrics.operation, "test_op");
        assert_eq!(metrics.operations, 100);
        assert!(metrics.ops_per_sec > 0.0);
    }

    #[test]
    fn test_measure_storage_operation() {
        let (result, metrics) = measure_storage_operation("add", || 2 + 2);
        assert_eq!(result, 4);
        assert_eq!(metrics.operation, "add");
        assert_eq!(metrics.operations, 1);
    }

    #[test]
    fn test_benchmark_storage_operation() {
        let mut counter = 0;
        let metrics = benchmark_storage_operation("increment", 10, || {
            counter += 1;
        });
        assert_eq!(counter, 10);
        assert_eq!(metrics.operations, 10);
    }
}
