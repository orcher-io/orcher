//! Integration with orcher-common for automatic instrumentation.
//!
//! This module provides utilities to automatically instrument common operations
//! from the orcher-common crate, including performance tracking and health checks.

use crate::error::Result;
use orcher_common::monitoring::{HealthCheck, OperationRecord, OperationTracker};
use tracing::info;

/// Creates an instrumented operation tracker with automatic logging.
///
/// This function enhances the common OperationTracker by automatically
/// logging when operations start.
pub fn instrumented_operation_tracker(operation_name: &str) -> OperationTracker {
    info!(operation = %operation_name, "Starting tracked operation");
    OperationTracker::start(operation_name)
}

/// Converts a common HealthCheck to a monitoring health check result.
///
/// This provides seamless integration between orcher-common health checks
/// and the monitoring system's health registry.
pub fn convert_health_check(check: &HealthCheck) -> Result<crate::health::HealthCheckResult> {
    let status = check.status;

    Ok(crate::health::HealthCheckResult {
        component_name: check.component.clone(),
        status,
        message: check.message.clone(),
        duration: check
            .check_duration
            .unwrap_or_else(|| std::time::Duration::from_millis(0)),
        metadata: check.details.clone(),
        timestamp: std::time::Instant::now(),
    })
}

/// Logs completion of a common operation record with appropriate level.
pub fn log_operation_record(record: &OperationRecord) {
    let duration_ms = record.duration.map(|d| d.as_millis()).unwrap_or(0);

    info!(
        operation = %record.operation_name,
        status = ?record.status,
        duration_ms = %duration_ms,
        "Operation completed"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_instrumented_operation_tracker() {
        let tracker = instrumented_operation_tracker("test_operation");
        let record = tracker.finish_success();
        assert_eq!(record.operation_name, "test_operation");
    }

    #[test]
    fn test_convert_health_check() {
        let health = HealthCheck::healthy("test_component");
        let result = convert_health_check(&health).unwrap();
        assert!(matches!(
            result.status,
            orcher_common::monitoring::HealthStatus::Healthy
        ));
    }

    #[test]
    fn test_log_operation_record() {
        let tracker = OperationTracker::start("test_op");
        let record = tracker.finish_success();
        log_operation_record(&record);
    }
}
