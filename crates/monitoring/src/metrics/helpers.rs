//! Metric registration and collection utilities.

use crate::error::{MonitoringError, Result};
use metrics::{describe_counter, describe_gauge, describe_histogram, Unit};

pub fn init_metrics_registry() -> Result<()> {
    describe_all_metrics().map_err(|e| {
        MonitoringError::initialization_failed(
            "metrics_registry",
            format!("Failed to initialize metrics registry: {}", e),
        )
    })
}

pub fn describe_all_metrics() -> std::result::Result<(), Box<dyn std::error::Error>> {
    // Task metrics
    describe_counter!(
        super::names::tasks::SUBMITTED_TOTAL,
        Unit::Count,
        "Total number of tasks submitted to the system"
    );

    describe_gauge!(
        super::names::tasks::RUNNING,
        Unit::Count,
        "Current number of running tasks"
    );

    describe_counter!(
        super::names::tasks::COMPLETED_TOTAL,
        Unit::Count,
        "Total number of completed tasks"
    );

    describe_counter!(
        super::names::tasks::FAILED_TOTAL,
        Unit::Count,
        "Total number of failed tasks"
    );

    describe_histogram!(
        super::names::tasks::DURATION_SECONDS,
        Unit::Seconds,
        "Task execution duration in seconds"
    );

    describe_gauge!(
        super::names::tasks::QUEUE_SIZE,
        Unit::Count,
        "Current task queue size"
    );

    // Worker metrics
    describe_gauge!(
        super::names::workers::POOL_SIZE,
        Unit::Count,
        "Current worker pool size"
    );

    describe_gauge!(
        super::names::workers::ACTIVE_COUNT,
        Unit::Count,
        "Current number of active workers"
    );

    describe_gauge!(
        super::names::workers::IDLE_COUNT,
        Unit::Count,
        "Current number of idle workers"
    );

    describe_gauge!(
        super::names::workers::UTILIZATION,
        Unit::Percent,
        "Worker utilization percentage"
    );

    // Scheduler metrics
    describe_gauge!(
        super::names::scheduler::QUEUE_SIZE,
        Unit::Count,
        "Current scheduler queue size"
    );

    describe_counter!(
        super::names::scheduler::TRIGGERS_FIRED_TOTAL,
        Unit::Count,
        "Total number of triggers fired"
    );

    describe_gauge!(
        super::names::scheduler::NEXT_FIRE_TIME,
        Unit::Seconds,
        "Next scheduled fire time in seconds since epoch"
    );

    // Storage metrics
    describe_counter!(
        super::names::storage::OPERATIONS_TOTAL,
        Unit::Count,
        "Total number of storage operations"
    );

    describe_histogram!(
        super::names::storage::OPERATION_DURATION_SECONDS,
        Unit::Seconds,
        "Storage operation duration in seconds"
    );

    describe_gauge!(
        super::names::storage::CACHE_HIT_RATIO,
        Unit::Percent,
        "Cache hit ratio percentage"
    );

    describe_gauge!(
        super::names::storage::CONNECTION_POOL_SIZE,
        Unit::Count,
        "Current connection pool size"
    );

    // Security metrics
    describe_counter!(
        super::names::security::AUTH_ATTEMPTS_TOTAL,
        Unit::Count,
        "Total number of authentication attempts"
    );

    describe_counter!(
        super::names::security::AUTH_FAILURES_TOTAL,
        Unit::Count,
        "Total number of authentication failures"
    );

    describe_counter!(
        super::names::security::RATE_LIMIT_EXCEEDED_TOTAL,
        Unit::Count,
        "Total number of rate limit exceeded events"
    );

    describe_counter!(
        super::names::security::THREATS_DETECTED_TOTAL,
        Unit::Count,
        "Total number of threats detected"
    );

    // System metrics
    describe_gauge!(
        super::names::system::CPU_USAGE,
        Unit::Percent,
        "CPU usage percentage"
    );

    describe_gauge!(
        super::names::system::MEMORY_USAGE,
        Unit::Percent,
        "Memory usage percentage"
    );

    describe_gauge!(
        super::names::system::DISK_USAGE,
        Unit::Percent,
        "Disk usage percentage"
    );

    describe_counter!(
        super::names::system::NETWORK_IO_BYTES,
        Unit::Bytes,
        "Network I/O bytes"
    );

    // Self-monitoring metrics
    describe_counter!(
        super::names::monitoring::METRICS_COLLECTED_TOTAL,
        Unit::Count,
        "Total number of metrics collected"
    );

    describe_counter!(
        super::names::monitoring::EXPORTER_ERRORS_TOTAL,
        Unit::Count,
        "Total number of exporter errors"
    );

    describe_histogram!(
        super::names::monitoring::HEALTH_CHECK_DURATION_SECONDS,
        Unit::Seconds,
        "Health check duration in seconds"
    );

    describe_counter!(
        super::names::monitoring::SPANS_CREATED_TOTAL,
        Unit::Count,
        "Total number of spans created"
    );

    describe_gauge!(
        super::names::monitoring::CONFIG_RELOAD_SUCCESS_RATE,
        Unit::Percent,
        "Configuration reload success rate"
    );

    describe_gauge!(
        super::names::monitoring::CIRCUIT_BREAKER_STATE,
        Unit::Count,
        "Circuit breaker state (0=closed, 1=open, 2=half-open)"
    );

    describe_gauge!(
        super::names::monitoring::COMPONENT_HEALTH_STATUS,
        Unit::Count,
        "Component health status (0=unhealthy, 1=degraded, 2=healthy)"
    );

    describe_gauge!(
        super::names::monitoring::UPTIME_SECONDS,
        Unit::Seconds,
        "Monitoring system uptime in seconds"
    );

    describe_counter!(
        super::names::monitoring::ALERTS_FIRED_TOTAL,
        Unit::Count,
        "Total number of alerts fired"
    );

    // Database connection pool metrics (Task 2.3)
    super::describe_db_pool_metrics();

    // Polling performance metrics for latency tracking
    super::describe_polling_metrics();

    Ok(())
}

// `TaskMetricRecorder` lived here and emitted the task metrics with no labels,
// while the engine emitted the same names with labels; any use of it would have
// failed registration. Task and workflow metrics are emitted through
// `metrics::lifecycle`, which owns their label sets.

pub fn validate_metric_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(MonitoringError::config_validation_error(
            "metric_name",
            "Metric name cannot be empty",
        ));
    }

    if !name.starts_with("orcher_") {
        return Err(MonitoringError::config_validation_error(
            "metric_name",
            "Metric name must start with 'orcher_' prefix",
        ));
    }

    // Check for valid characters (letters, digits, underscores)
    if !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(MonitoringError::config_validation_error(
            "metric_name",
            "Metric name can only contain letters, digits, and underscores",
        ));
    }

    Ok(())
}

pub fn create_metric_name(component: &str, metric: &str) -> String {
    format!("orcher_{}_{}", component, metric)
}

pub fn validate_all_metric_names() -> Result<()> {
    use super::names::*;

    // Validate task metrics
    validate_metric_name(tasks::SUBMITTED_TOTAL)?;
    validate_metric_name(tasks::RUNNING)?;
    validate_metric_name(tasks::COMPLETED_TOTAL)?;
    validate_metric_name(tasks::FAILED_TOTAL)?;
    validate_metric_name(tasks::DURATION_SECONDS)?;
    validate_metric_name(tasks::QUEUE_SIZE)?;

    // Validate workflow metrics
    validate_metric_name(workflows::EXECUTING)?;
    validate_metric_name(workflows::COMPLETED_TOTAL)?;
    validate_metric_name(workflows::STALE_COMPLETIONS_TOTAL)?;
    validate_metric_name(workflows::FAILED_TOTAL)?;
    validate_metric_name(workflows::DURATION_SECONDS)?;
    validate_metric_name(workflows::STEP_DURATION_SECONDS)?;

    // Validate worker metrics
    validate_metric_name(workers::POOL_SIZE)?;
    validate_metric_name(workers::ACTIVE_COUNT)?;
    validate_metric_name(workers::IDLE_COUNT)?;
    validate_metric_name(workers::UTILIZATION)?;

    // Validate monitoring metrics
    validate_metric_name(monitoring::METRICS_COLLECTED_TOTAL)?;
    validate_metric_name(monitoring::EXPORTER_ERRORS_TOTAL)?;
    validate_metric_name(monitoring::UPTIME_SECONDS)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_describe_all_metrics() {
        // Should not panic when describing metrics
        let result = describe_all_metrics();
        assert!(result.is_ok());
    }

    #[test]
    fn test_init_metrics_registry() {
        let result = init_metrics_registry();
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_metric_name() {
        // Valid names
        assert!(validate_metric_name("orcher_tasks_submitted_total").is_ok());
        assert!(validate_metric_name("orcher_worker_pool_size").is_ok());

        // Invalid names
        assert!(validate_metric_name("").is_err());
        assert!(validate_metric_name("tasks_submitted").is_err()); // Missing prefix
        assert!(validate_metric_name("orcher_tasks-submitted").is_err()); // Invalid character
    }

    #[test]
    fn test_create_metric_name() {
        assert_eq!(
            create_metric_name("tasks", "submitted_total"),
            "orcher_tasks_submitted_total"
        );
        assert_eq!(
            create_metric_name("worker", "pool_size"),
            "orcher_worker_pool_size"
        );
    }

    #[test]
    fn test_validate_all_metric_names() {
        let result = validate_all_metric_names();
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_metric_recorders() {

        // Should not panic on creation
    }
}
