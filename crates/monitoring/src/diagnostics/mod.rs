//! Diagnostic utilities for monitoring system health and error reporting
//!
//! This module provides comprehensive diagnostic capabilities for the monitoring
//! system itself, including error tracking, self-monitoring, and troubleshooting
//! utilities.
//!
//! # Features
//!
//! - **Error Reporting**: Track and categorize monitoring system errors
//! - **Self-Monitoring**: Monitor the monitoring system's own health
//! - **Error Correlation**: Link errors to trace contexts and operations
//! - **Health Reporting**: Generate comprehensive system health reports
//!
//! # Usage
//!
//! ```rust
//! use orcher_monitoring::diagnostics::{ErrorReporter, SelfMonitoring};
//! use orcher_monitoring::error::MonitoringError;
//!
//! // Set up error reporting
//! let mut error_reporter = ErrorReporter::new();
//! let error = MonitoringError::exporter_error("prometheus", "connection failed");
//! error_reporter.report_error(&error);
//!
//! // Set up self-monitoring
//! let self_monitoring = SelfMonitoring::new();
//! self_monitoring.record_metric_collected("tasks_total");
//! self_monitoring.update_uptime();
//! ```

pub mod error_correlation;
pub mod error_reporter;
pub mod health_report;
pub mod self_monitoring;

// Re-export main types for convenience
pub use error_correlation::{
    CorrelatedError, CorrelationSummary, ErrorCorrelation, ErrorCorrelationEngine, ErrorPattern,
};
pub use error_reporter::ErrorReporter;
pub use health_report::{HealthReport, HealthReportGenerator};
pub use self_monitoring::SelfMonitoring;

/// Diagnostic configuration for controlling diagnostic behavior
#[derive(Debug, Clone)]
pub struct DiagnosticConfig {
    /// Maximum number of errors to keep in memory
    pub max_error_history: usize,
    /// Whether to enable detailed error correlation
    pub enable_error_correlation: bool,
    /// Interval for self-monitoring health checks
    pub self_monitoring_interval_secs: u64,
    /// Whether to track performance metrics
    pub track_performance: bool,
}

impl Default for DiagnosticConfig {
    fn default() -> Self {
        Self {
            max_error_history: 100,
            enable_error_correlation: true,
            self_monitoring_interval_secs: 30,
            track_performance: true,
        }
    }
}

/// Result type for diagnostic operations
pub type DiagnosticResult<T> = Result<T, crate::error::MonitoringError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diagnostic_config_defaults() {
        let config = DiagnosticConfig::default();
        assert_eq!(config.max_error_history, 100);
        assert!(config.enable_error_correlation);
        assert_eq!(config.self_monitoring_interval_secs, 30);
        assert!(config.track_performance);
    }

    #[test]
    fn error_reporter_creation() {
        let reporter = ErrorReporter::new();
        assert!(reporter.get_error_summary().is_empty());
    }

    #[test]
    fn self_monitoring_creation() {
        let monitoring = SelfMonitoring::new();
        // Should not panic on creation
        drop(monitoring);
    }
}
