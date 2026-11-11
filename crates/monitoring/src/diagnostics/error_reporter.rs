//! Error reporting and diagnostic utilities for monitoring system errors
//!
//! This module provides comprehensive error tracking, categorization, and
//! diagnostic capabilities to help troubleshoot monitoring system issues.

use crate::error::{MonitoringError, Result};
use orcher_common::types::identifiers::*;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Error statistics for a specific error type
#[derive(Debug, Clone)]
pub struct ErrorStats {
    /// Total count of this error type
    pub count: u64,
    /// First occurrence timestamp
    pub first_seen: Instant,
    /// Last occurrence timestamp
    pub last_seen: Instant,
    /// Recent error rate (errors per minute)
    pub rate_per_minute: f64,
    /// Error frequency trend
    pub trend: ErrorTrend,
}

/// Error frequency trend
#[derive(Debug, Clone, PartialEq)]
pub enum ErrorTrend {
    /// Error rate is increasing
    Increasing,
    /// Error rate is decreasing
    Decreasing,
    /// Error rate is stable
    Stable,
    /// Insufficient data to determine trend
    Unknown,
}

/// Individual error occurrence record
#[derive(Debug, Clone)]
pub struct ErrorOccurrence {
    /// The error that occurred
    pub error: MonitoringError,
    /// When the error occurred
    pub timestamp: Instant,
    /// Optional trace ID for correlation
    pub trace_id: Option<String>,
    /// Optional request ID for correlation
    pub request_id: Option<RequestId>,
    /// Error context metadata
    pub context: HashMap<String, String>,
}

impl ErrorOccurrence {
    /// Create a new error occurrence
    pub fn new(error: MonitoringError) -> Self {
        Self {
            error,
            timestamp: Instant::now(),
            trace_id: None,
            request_id: None,
            context: HashMap::new(),
        }
    }

    /// Add trace ID for correlation
    pub fn with_trace_id(mut self, trace_id: impl Into<String>) -> Self {
        self.trace_id = Some(trace_id.into());
        self
    }

    /// Add request ID for correlation
    pub fn with_request_id(mut self, request_id: RequestId) -> Self {
        self.request_id = Some(request_id);
        self
    }

    /// Add context metadata
    pub fn with_context(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.context.insert(key.into(), value.into());
        self
    }

    /// Get age of this error occurrence
    pub fn age(&self) -> Duration {
        self.timestamp.elapsed()
    }
}

/// Comprehensive error reporter for monitoring system diagnostics
#[derive(Debug, Clone)]
pub struct ErrorReporter {
    inner: Arc<RwLock<ErrorReporterInner>>,
    config: DiagnosticConfig,
}

#[derive(Debug)]
struct ErrorReporterInner {
    /// Error statistics by error type
    error_stats: HashMap<String, ErrorStats>,
    /// Recent error occurrences (circular buffer)
    recent_errors: Vec<ErrorOccurrence>,
    /// Total error count across all types
    total_errors: u64,
    /// When the reporter was created
    start_time: Instant,
    /// Last cleanup timestamp
    last_cleanup: Instant,
}

/// Configuration for error reporting behavior
#[derive(Debug, Clone)]
pub struct DiagnosticConfig {
    /// Maximum number of recent errors to keep
    pub max_recent_errors: usize,
    /// How long to keep error statistics
    pub stats_retention: Duration,
    /// Cleanup interval for old data
    pub cleanup_interval: Duration,
}

impl Default for DiagnosticConfig {
    fn default() -> Self {
        Self {
            max_recent_errors: 100,
            stats_retention: Duration::from_secs(3600), // 1 hour
            cleanup_interval: Duration::from_secs(300), // 5 minutes
        }
    }
}

impl ErrorReporter {
    /// Create a new error reporter with default configuration
    pub fn new() -> Self {
        Self::with_config(DiagnosticConfig::default())
    }

    /// Create a new error reporter with custom configuration
    pub fn with_config(config: DiagnosticConfig) -> Self {
        let now = Instant::now();
        Self {
            inner: Arc::new(RwLock::new(ErrorReporterInner {
                error_stats: HashMap::new(),
                recent_errors: Vec::with_capacity(config.max_recent_errors),
                total_errors: 0,
                start_time: now,
                last_cleanup: now,
            })),
            config,
        }
    }

    /// Report a new error occurrence
    pub fn report_error(&self, error: &MonitoringError) -> Result<()> {
        let occurrence = ErrorOccurrence::new(error.clone());
        self.report_occurrence(occurrence)
    }

    /// Report an error with additional context
    pub fn report_error_with_context(
        &self,
        error: &MonitoringError,
        context: HashMap<String, String>,
    ) -> Result<()> {
        let mut occurrence = ErrorOccurrence::new(error.clone());
        occurrence.context = context;
        self.report_occurrence(occurrence)
    }

    /// Report an error occurrence with full details
    pub fn report_occurrence(&self, occurrence: ErrorOccurrence) -> Result<()> {
        let mut inner = self.inner.write();

        // Update total count
        inner.total_errors += 1;

        // Update statistics for this error type
        let error_type = occurrence.error.error_type();
        let now = occurrence.timestamp;

        let stats = inner
            .error_stats
            .entry(error_type.to_string())
            .or_insert_with(|| ErrorStats {
                count: 0,
                first_seen: now,
                last_seen: now,
                rate_per_minute: 0.0,
                trend: ErrorTrend::Unknown,
            });

        stats.count += 1;
        stats.last_seen = now;

        // Calculate error rate (errors per minute)
        let duration_mins = stats
            .last_seen
            .duration_since(stats.first_seen)
            .as_secs_f64()
            / 60.0;
        if duration_mins > 0.0 {
            stats.rate_per_minute = stats.count as f64 / duration_mins;
        }

        // Add to recent errors (circular buffer)
        if inner.recent_errors.len() >= self.config.max_recent_errors {
            inner.recent_errors.remove(0);
        }
        inner.recent_errors.push(occurrence);

        // Periodic cleanup
        if inner.last_cleanup.elapsed() >= self.config.cleanup_interval {
            self.cleanup_old_data(&mut inner);
            inner.last_cleanup = Instant::now();
        }

        Ok(())
    }

    /// Get error statistics summary
    pub fn get_error_summary(&self) -> HashMap<String, u64> {
        let inner = self.inner.read();
        inner
            .error_stats
            .iter()
            .map(|(error_type, stats)| (error_type.clone(), stats.count))
            .collect()
    }

    /// Get detailed error statistics
    pub fn get_error_stats(&self) -> HashMap<String, ErrorStats> {
        let inner = self.inner.read();
        inner.error_stats.clone()
    }

    /// Get recent error occurrences
    pub fn get_recent_errors(&self, limit: Option<usize>) -> Vec<ErrorOccurrence> {
        let inner = self.inner.read();
        let errors = &inner.recent_errors;

        match limit {
            Some(n) if n < errors.len() => errors[errors.len() - n..].to_vec(),
            _ => errors.clone(),
        }
    }

    /// Get total error count
    pub fn get_total_error_count(&self) -> u64 {
        let inner = self.inner.read();
        inner.total_errors
    }

    /// Get error rate (errors per minute) since startup
    pub fn get_overall_error_rate(&self) -> f64 {
        let inner = self.inner.read();
        let duration_mins = inner.start_time.elapsed().as_secs_f64() / 60.0;
        if duration_mins > 0.0 {
            inner.total_errors as f64 / duration_mins
        } else {
            0.0
        }
    }

    /// Check if error rate is above threshold
    pub fn is_error_rate_high(&self, threshold_per_minute: f64) -> bool {
        self.get_overall_error_rate() > threshold_per_minute
    }

    /// Get errors by type within a time window
    pub fn get_errors_in_window(&self, window: Duration) -> Vec<ErrorOccurrence> {
        let inner = self.inner.read();
        let cutoff = Instant::now() - window;

        inner
            .recent_errors
            .iter()
            .filter(|occurrence| occurrence.timestamp >= cutoff)
            .cloned()
            .collect()
    }

    /// Get the most frequent error type
    pub fn get_most_frequent_error_type(&self) -> Option<(String, u64)> {
        let inner = self.inner.read();
        inner
            .error_stats
            .iter()
            .max_by_key(|(_, stats)| stats.count)
            .map(|(error_type, stats)| (error_type.clone(), stats.count))
    }

    /// Clear all error history and statistics
    pub fn clear(&self) {
        let mut inner = self.inner.write();
        inner.error_stats.clear();
        inner.recent_errors.clear();
        inner.total_errors = 0;
        inner.start_time = Instant::now();
        inner.last_cleanup = Instant::now();
    }

    /// Generate a diagnostic report
    pub fn generate_report(&self) -> DiagnosticReport {
        let inner = self.inner.read();
        let now = Instant::now();

        DiagnosticReport {
            total_errors: inner.total_errors,
            error_rate_per_minute: self.get_overall_error_rate(),
            uptime: inner.start_time.elapsed(),
            error_types: inner.error_stats.len(),
            most_frequent_error: self.get_most_frequent_error_type(),
            recent_error_count: inner.recent_errors.len(),
            timestamp: now,
        }
    }

    /// Internal cleanup of old data
    fn cleanup_old_data(&self, inner: &mut ErrorReporterInner) {
        let cutoff = Instant::now() - self.config.stats_retention;

        // Remove old error occurrences
        inner
            .recent_errors
            .retain(|occurrence| occurrence.timestamp >= cutoff);

        // Update error trends based on recent activity
        for stats in inner.error_stats.values_mut() {
            stats.trend = self.calculate_trend(stats);
        }
    }

    /// Calculate error trend for statistics
    fn calculate_trend(&self, stats: &ErrorStats) -> ErrorTrend {
        // Simple trend calculation based on recent vs. historical rates
        let total_duration = stats
            .last_seen
            .duration_since(stats.first_seen)
            .as_secs_f64()
            / 60.0;
        let recent_duration = 10.0; // Last 10 minutes

        if total_duration < recent_duration {
            return ErrorTrend::Unknown;
        }

        // This is a simplified trend calculation
        // In a real implementation, you'd want more sophisticated trend analysis
        if stats.rate_per_minute > 0.0 {
            ErrorTrend::Stable
        } else {
            ErrorTrend::Unknown
        }
    }
}

impl Default for ErrorReporter {
    fn default() -> Self {
        Self::new()
    }
}

/// Diagnostic report containing error reporting statistics
#[derive(Debug, Clone)]
pub struct DiagnosticReport {
    /// Total number of errors reported
    pub total_errors: u64,
    /// Current error rate per minute
    pub error_rate_per_minute: f64,
    /// How long the reporter has been running
    pub uptime: Duration,
    /// Number of distinct error types seen
    pub error_types: usize,
    /// Most frequent error type and its count
    pub most_frequent_error: Option<(String, u64)>,
    /// Number of recent errors in memory
    pub recent_error_count: usize,
    /// When this report was generated
    pub timestamp: Instant,
}

impl std::fmt::Display for DiagnosticReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "=== Monitoring System Diagnostic Report ===")?;
        writeln!(f, "Uptime: {:.1}s", self.uptime.as_secs_f64())?;
        writeln!(f, "Total Errors: {}", self.total_errors)?;
        writeln!(f, "Error Rate: {:.2}/min", self.error_rate_per_minute)?;
        writeln!(f, "Error Types: {}", self.error_types)?;
        writeln!(f, "Recent Errors: {}", self.recent_error_count)?;

        if let Some((error_type, count)) = &self.most_frequent_error {
            writeln!(f, "Most Frequent: {} ({})", error_type, count)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::MonitoringError;

    #[test]
    fn error_reporter_creation() {
        let reporter = ErrorReporter::new();
        assert_eq!(reporter.get_total_error_count(), 0);
        assert!(reporter.get_error_summary().is_empty());
    }

    #[test]
    fn error_reporting_basic() {
        let reporter = ErrorReporter::new();
        let error = MonitoringError::exporter_error("prometheus", "connection failed");

        reporter.report_error(&error).unwrap();

        assert_eq!(reporter.get_total_error_count(), 1);
        let summary = reporter.get_error_summary();
        assert_eq!(summary.get("exporter_error"), Some(&1));
    }

    #[test]
    fn error_occurrence_with_context() {
        let error = MonitoringError::task_monitoring_error(TaskId::new_v4(), "failed");
        let occurrence = ErrorOccurrence::new(error)
            .with_trace_id("test-trace-123")
            .with_context("component", "task_executor");

        assert_eq!(occurrence.trace_id, Some("test-trace-123".to_string()));
        assert_eq!(
            occurrence.context.get("component"),
            Some(&"task_executor".to_string())
        );
    }

    #[test]
    fn error_stats_tracking() {
        let reporter = ErrorReporter::new();
        let error1 = MonitoringError::exporter_error("prometheus", "error 1");
        let error2 = MonitoringError::exporter_error("jaeger", "error 2");

        reporter.report_error(&error1).unwrap();
        reporter.report_error(&error2).unwrap();
        reporter.report_error(&error1).unwrap(); // Same type again

        let stats = reporter.get_error_stats();
        assert_eq!(stats.get("exporter_error").unwrap().count, 3);
        assert_eq!(reporter.get_total_error_count(), 3);
    }

    #[test]
    fn recent_errors_limit() {
        let config = DiagnosticConfig {
            max_recent_errors: 2,
            ..Default::default()
        };
        let reporter = ErrorReporter::with_config(config);

        let error1 = MonitoringError::initialization_failed("comp1", "msg1");
        let error2 = MonitoringError::initialization_failed("comp2", "msg2");
        let error3 = MonitoringError::initialization_failed("comp3", "msg3");

        reporter.report_error(&error1).unwrap();
        reporter.report_error(&error2).unwrap();
        reporter.report_error(&error3).unwrap();

        let recent = reporter.get_recent_errors(None);
        assert_eq!(recent.len(), 2); // Should only keep last 2
        assert_eq!(recent[1].error.to_string(), error3.to_string());
    }

    #[test]
    fn diagnostic_report_generation() {
        let reporter = ErrorReporter::new();
        let error = MonitoringError::config_validation_error("field", "reason");
        reporter.report_error(&error).unwrap();

        let report = reporter.generate_report();
        assert_eq!(report.total_errors, 1);
        assert_eq!(report.error_types, 1);
        assert_eq!(report.recent_error_count, 1);
    }

    #[test]
    fn error_rate_calculation() {
        let reporter = ErrorReporter::new();

        // Report multiple errors
        for _ in 0..5 {
            let error = MonitoringError::metric_collection_error("test", "failed");
            reporter.report_error(&error).unwrap();
        }

        let rate = reporter.get_overall_error_rate();
        assert!(rate > 0.0);
        assert!(reporter.is_error_rate_high(1.0)); // Should be above 1/min
    }

    #[test]
    fn most_frequent_error_type() {
        let reporter = ErrorReporter::new();

        // Report different error types
        reporter
            .report_error(&MonitoringError::exporter_error("prom", "err"))
            .unwrap();
        reporter
            .report_error(&MonitoringError::exporter_error("jaeger", "err"))
            .unwrap();
        reporter
            .report_error(&MonitoringError::exporter_error("otlp", "err"))
            .unwrap();
        reporter
            .report_error(&MonitoringError::health_check_timeout(
                "db",
                Duration::from_secs(5),
            ))
            .unwrap();

        let (most_frequent_type, count) = reporter.get_most_frequent_error_type().unwrap();
        assert_eq!(most_frequent_type, "exporter_error");
        assert_eq!(count, 3);
    }
}
