//! Error types for the monitoring system

use orcher_common::types::identifiers::*;
use std::time::Duration;
use thiserror::Error;

/// Result type for monitoring operations
pub type Result<T> = std::result::Result<T, MonitoringError>;

/// Comprehensive error types for monitoring system
#[derive(Error, Debug, Clone)]
pub enum MonitoringError {
    #[error("Initialization failed for component '{component}': {message}")]
    InitializationFailed { component: String, message: String },

    #[error("Exporter '{exporter}' error: {message}")]
    ExporterError { exporter: String, message: String },

    #[error("Health check '{check_name}' timed out after {timeout:?}")]
    HealthCheckTimeout {
        check_name: String,
        timeout: Duration,
    },

    #[error("Configuration validation failed for field '{field}': {reason}")]
    ConfigValidationError { field: String, reason: String },

    #[error("Metric collection failed for '{metric_name}': {message}")]
    MetricCollectionError {
        metric_name: String,
        message: String,
    },

    #[error("Circuit breaker '{name}' is open")]
    CircuitBreakerOpen { name: String },

    #[error("Task monitoring error for task '{task_id}': {message}")]
    TaskMonitoringError { task_id: TaskId, message: String },

    #[error("Workflow monitoring error for workflow '{workflow_id}': {message}")]
    WorkflowMonitoringError {
        workflow_id: WorkflowId,
        message: String,
    },

    #[error("Worker monitoring error for worker '{worker_id}': {message}")]
    WorkerMonitoringError {
        worker_id: WorkerId,
        message: String,
    },

    #[error("Tracing error: {message}")]
    TracingError { message: String },

    #[error("Health check error: {message}")]
    HealthCheckError { message: String },

    #[error("System metrics error: {message}")]
    SystemMetricsError { message: String },

    #[error("Integration error with '{component}': {message}")]
    IntegrationError { component: String, message: String },
}

impl MonitoringError {
    /// Returns a string representation of the error type for categorization
    pub fn error_type(&self) -> &'static str {
        match self {
            Self::InitializationFailed { .. } => "initialization_failed",
            Self::ExporterError { .. } => "exporter_error",
            Self::HealthCheckTimeout { .. } => "health_check_timeout",
            Self::ConfigValidationError { .. } => "config_validation_error",
            Self::MetricCollectionError { .. } => "metric_collection_error",
            Self::CircuitBreakerOpen { .. } => "circuit_breaker_open",
            Self::TracingError { .. } => "tracing_error",
            Self::HealthCheckError { .. } => "health_check_error",
            Self::SystemMetricsError { .. } => "system_metrics_error",
            Self::IntegrationError { .. } => "integration_error",
            Self::TaskMonitoringError { .. } => "task_monitoring_error",
            Self::WorkflowMonitoringError { .. } => "workflow_monitoring_error",
            Self::WorkerMonitoringError { .. } => "worker_monitoring_error",
        }
    }

    /// Creates an initialization error
    pub fn initialization_failed(component: impl Into<String>, message: impl Into<String>) -> Self {
        Self::InitializationFailed {
            component: component.into(),
            message: message.into(),
        }
    }

    /// Creates an exporter error
    pub fn exporter_error(exporter: impl Into<String>, message: impl Into<String>) -> Self {
        Self::ExporterError {
            exporter: exporter.into(),
            message: message.into(),
        }
    }

    /// Creates a configuration validation error
    pub fn config_validation_error(field: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::ConfigValidationError {
            field: field.into(),
            reason: reason.into(),
        }
    }

    /// Creates a metric collection error
    pub fn metric_collection_error(
        metric_name: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self::MetricCollectionError {
            metric_name: metric_name.into(),
            message: message.into(),
        }
    }

    /// Creates a health check timeout error
    pub fn health_check_timeout(check_name: impl Into<String>, timeout: Duration) -> Self {
        Self::HealthCheckTimeout {
            check_name: check_name.into(),
            timeout,
        }
    }

    /// Creates a circuit breaker open error
    pub fn circuit_breaker_open(name: impl Into<String>) -> Self {
        Self::CircuitBreakerOpen { name: name.into() }
    }

    /// Creates a task monitoring error
    pub fn task_monitoring_error(task_id: TaskId, message: impl Into<String>) -> Self {
        Self::TaskMonitoringError {
            task_id,
            message: message.into(),
        }
    }

    /// Creates a workflow monitoring error
    pub fn workflow_monitoring_error(workflow_id: WorkflowId, message: impl Into<String>) -> Self {
        Self::WorkflowMonitoringError {
            workflow_id,
            message: message.into(),
        }
    }

    /// Creates a worker monitoring error
    pub fn worker_monitoring_error(worker_id: WorkerId, message: impl Into<String>) -> Self {
        Self::WorkerMonitoringError {
            worker_id,
            message: message.into(),
        }
    }

    /// Creates a health check error
    pub fn health_check_failed(message: impl Into<String>) -> Self {
        Self::HealthCheckError {
            message: message.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn error_type_returns_correct_string() {
        let error = MonitoringError::initialization_failed("test_component", "test message");
        assert_eq!(error.error_type(), "initialization_failed");
    }

    #[test]
    fn initialization_error_creation() {
        let error = MonitoringError::initialization_failed("prometheus", "failed to bind");
        match error {
            MonitoringError::InitializationFailed { component, message } => {
                assert_eq!(component, "prometheus");
                assert_eq!(message, "failed to bind");
            }
            _ => panic!("Expected InitializationFailed"),
        }
    }

    #[test]
    fn timeout_error_preserves_duration() {
        let timeout = Duration::from_secs(5);
        let error = MonitoringError::health_check_timeout("database", timeout);
        match error {
            MonitoringError::HealthCheckTimeout { timeout: t, .. } => {
                assert_eq!(t, timeout);
            }
            _ => panic!("Expected HealthCheckTimeout"),
        }
    }

    #[test]
    fn task_monitoring_error_creation() {
        let task_id = TaskId::new_v4();
        let error = MonitoringError::task_monitoring_error(task_id, "metrics collection failed");
        match error {
            MonitoringError::TaskMonitoringError {
                task_id: id,
                message,
            } => {
                assert_eq!(id, task_id);
                assert_eq!(message, "metrics collection failed");
            }
            _ => panic!("Expected TaskMonitoringError"),
        }
    }
}
