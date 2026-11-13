//! Error correlation system for linking errors to trace contexts and spans
//!
//! This module provides functionality to correlate errors with trace contexts,
//! spans, and other monitoring data to provide better troubleshooting insights.

use crate::error::MonitoringError;
use orcher_common::types::identifiers::{TaskId, WorkflowId};
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Error correlation information linking errors to traces and spans
#[derive(Debug, Clone)]
pub struct ErrorCorrelation {
    /// Unique correlation ID for this error
    pub correlation_id: String,
    /// Trace ID if available from tracing context
    pub trace_id: Option<String>,
    /// Span ID if available from tracing context
    pub span_id: Option<String>,
    /// Task ID if error is related to a specific task
    pub task_id: Option<TaskId>,
    /// Workflow ID if error is related to a workflow
    pub workflow_id: Option<WorkflowId>,
    /// Component where the error occurred
    pub component: String,
    /// Operation being performed when error occurred
    pub operation: String,
    /// Timestamp when the error occurred
    pub timestamp: u64,
    /// Additional context metadata
    pub context: HashMap<String, String>,
}

impl ErrorCorrelation {
    /// Create a new error correlation with basic information
    pub fn new(component: impl Into<String>, operation: impl Into<String>) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            correlation_id: format!("err_{}", timestamp),
            trace_id: None,
            span_id: None,
            task_id: None,
            workflow_id: None,
            component: component.into(),
            operation: operation.into(),
            timestamp,
            context: HashMap::new(),
        }
    }

    /// Add trace context information from current span
    pub fn with_trace_context(mut self) -> Self {
        if let Some(trace_id) = crate::tracing::context::current_trace_id() {
            self.trace_id = Some(trace_id);
        }
        if let Some(span_id) = crate::tracing::context::current_span_id() {
            self.span_id = Some(span_id);
        }
        self
    }

    /// Add task context information
    pub fn with_task_context(mut self, task_id: TaskId) -> Self {
        self.task_id = Some(task_id);
        self
    }

    /// Add workflow context information
    pub fn with_workflow_context(mut self, workflow_id: WorkflowId) -> Self {
        self.workflow_id = Some(workflow_id);
        self
    }

    /// Add additional context metadata
    pub fn with_context(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.context.insert(key.into(), value.into());
        self
    }

    /// Check if this correlation has trace information
    pub fn has_trace_context(&self) -> bool {
        self.trace_id.is_some() || self.span_id.is_some()
    }

    /// Get a formatted string representation for logging
    pub fn format_for_log(&self) -> String {
        let mut parts = vec![
            format!("correlation_id={}", self.correlation_id),
            format!("component={}", self.component),
            format!("operation={}", self.operation),
        ];

        if let Some(ref trace_id) = self.trace_id {
            parts.push(format!("trace_id={}", trace_id));
        }

        if let Some(ref span_id) = self.span_id {
            parts.push(format!("span_id={}", span_id));
        }

        if let Some(ref task_id) = self.task_id {
            parts.push(format!("task_id={}", task_id));
        }

        if let Some(ref workflow_id) = self.workflow_id {
            parts.push(format!("workflow_id={}", workflow_id));
        }

        parts.join(", ")
    }
}

/// Correlated error containing both the error and its correlation information
#[derive(Debug, Clone)]
pub struct CorrelatedError {
    /// The underlying monitoring error
    pub error: MonitoringError,
    /// Correlation information linking to traces and context
    pub correlation: ErrorCorrelation,
    /// Duration since error occurred
    pub age: Duration,
}

impl CorrelatedError {
    /// Create a new correlated error
    pub fn new(error: MonitoringError, correlation: ErrorCorrelation) -> Self {
        Self {
            error,
            correlation,
            age: Duration::ZERO,
        }
    }

    /// Update the age of this error
    pub fn update_age(&mut self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        self.age = Duration::from_secs(now.saturating_sub(self.correlation.timestamp));
    }

    /// Check if this error is recent (within specified duration)
    pub fn is_recent(&self, threshold: Duration) -> bool {
        self.age <= threshold
    }
}

/// Error correlation engine for tracking and linking errors
pub struct ErrorCorrelationEngine {
    /// Recent errors with correlation information
    correlated_errors: Vec<CorrelatedError>,
    /// Maximum number of errors to keep
    max_errors: usize,
    /// Error pattern detection threshold
    pattern_threshold: usize,
}

impl ErrorCorrelationEngine {
    /// Create a new error correlation engine
    pub fn new() -> Self {
        Self {
            correlated_errors: Vec::new(),
            max_errors: 100,
            pattern_threshold: 3,
        }
    }

    /// Create an error correlation engine with custom configuration
    pub fn with_config(max_errors: usize, pattern_threshold: usize) -> Self {
        Self {
            correlated_errors: Vec::new(),
            max_errors,
            pattern_threshold,
        }
    }

    /// Correlate an error with current context and store it
    pub fn correlate_error(
        &mut self,
        error: MonitoringError,
        component: impl Into<String>,
        operation: impl Into<String>,
    ) -> ErrorCorrelation {
        let correlation = ErrorCorrelation::new(component, operation).with_trace_context();

        let correlated_error = CorrelatedError::new(error, correlation.clone());

        // Add to collection and maintain size limit
        self.correlated_errors.push(correlated_error);
        if self.correlated_errors.len() > self.max_errors {
            self.correlated_errors.remove(0);
        }

        correlation
    }

    /// Get errors correlated with a specific trace ID
    pub fn get_errors_by_trace(&self, trace_id: &str) -> Vec<&CorrelatedError> {
        self.correlated_errors
            .iter()
            .filter(|err| {
                err.correlation
                    .trace_id
                    .as_ref()
                    .is_some_and(|id| id == trace_id)
            })
            .collect()
    }

    /// Get errors for a specific component
    pub fn get_errors_by_component(&self, component: &str) -> Vec<&CorrelatedError> {
        self.correlated_errors
            .iter()
            .filter(|err| err.correlation.component == component)
            .collect()
    }

    /// Get recent errors within the specified duration
    pub fn get_recent_errors(&mut self, threshold: Duration) -> Vec<&CorrelatedError> {
        // Update ages first
        for err in &mut self.correlated_errors {
            err.update_age();
        }

        self.correlated_errors
            .iter()
            .filter(|err| err.is_recent(threshold))
            .collect()
    }

    /// Detect error patterns by component and operation
    pub fn detect_error_patterns(&self) -> Vec<ErrorPattern> {
        let mut patterns = HashMap::new();

        for err in &self.correlated_errors {
            let key = (
                err.correlation.component.clone(),
                err.correlation.operation.clone(),
            );
            let count = patterns.entry(key).or_insert(0);
            *count += 1;
        }

        patterns
            .into_iter()
            .filter(|(_, count)| *count >= self.pattern_threshold)
            .map(|((component, operation), count)| {
                let sample_errors = self.get_sample_errors(&component, &operation, 3);
                ErrorPattern {
                    component,
                    operation,
                    occurrence_count: count,
                    sample_errors,
                }
            })
            .collect()
    }

    /// Get a summary of all correlated errors
    pub fn get_correlation_summary(&self) -> CorrelationSummary {
        let total_errors = self.correlated_errors.len();
        let errors_with_traces = self
            .correlated_errors
            .iter()
            .filter(|err| err.correlation.has_trace_context())
            .count();

        let mut component_counts = HashMap::new();
        for err in &self.correlated_errors {
            *component_counts
                .entry(err.correlation.component.clone())
                .or_insert(0) += 1;
        }

        CorrelationSummary {
            total_errors,
            errors_with_traces,
            trace_correlation_rate: if total_errors > 0 {
                errors_with_traces as f64 / total_errors as f64
            } else {
                0.0
            },
            component_error_counts: component_counts,
        }
    }

    /// Get sample errors for pattern detection
    fn get_sample_errors(&self, component: &str, operation: &str, limit: usize) -> Vec<String> {
        self.correlated_errors
            .iter()
            .filter(|err| {
                err.correlation.component == component && err.correlation.operation == operation
            })
            .take(limit)
            .map(|err| err.error.to_string())
            .collect()
    }

    /// Clear old errors beyond the retention period
    pub fn cleanup_old_errors(&mut self, retention_period: Duration) {
        let cutoff_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            .saturating_sub(retention_period.as_secs());

        self.correlated_errors
            .retain(|err| err.correlation.timestamp >= cutoff_time);
    }
}

impl Default for ErrorCorrelationEngine {
    fn default() -> Self {
        Self::new()
    }
}

/// Error pattern detected by the correlation engine
#[derive(Debug, Clone)]
pub struct ErrorPattern {
    /// Component where the pattern occurs
    pub component: String,
    /// Operation where the pattern occurs
    pub operation: String,
    /// Number of occurrences of this pattern
    pub occurrence_count: usize,
    /// Sample error messages for this pattern
    pub sample_errors: Vec<String>,
}

/// Summary of error correlation statistics
#[derive(Debug, Clone)]
pub struct CorrelationSummary {
    /// Total number of errors being tracked
    pub total_errors: usize,
    /// Number of errors with trace context
    pub errors_with_traces: usize,
    /// Percentage of errors with trace correlation
    pub trace_correlation_rate: f64,
    /// Error counts by component
    pub component_error_counts: HashMap<String, usize>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_correlation_creation() {
        let correlation = ErrorCorrelation::new("test_component", "test_operation");

        assert_eq!(correlation.component, "test_component");
        assert_eq!(correlation.operation, "test_operation");
        assert!(correlation.correlation_id.starts_with("err_"));
        assert!(correlation.context.is_empty());
    }

    #[test]
    fn test_error_correlation_with_context() {
        let task_id = TaskId::new_v4();
        let correlation = ErrorCorrelation::new("test_component", "test_operation")
            .with_task_context(task_id)
            .with_context("key", "value");

        assert_eq!(correlation.task_id, Some(task_id));
        assert_eq!(correlation.context.get("key"), Some(&"value".to_string()));
    }

    #[test]
    fn test_correlated_error_age_update() {
        let error = MonitoringError::exporter_error("test", "error message");
        let correlation = ErrorCorrelation::new("test_component", "test_operation");
        let mut correlated_error = CorrelatedError::new(error, correlation);

        assert_eq!(correlated_error.age, Duration::ZERO);

        // Simulate some time passing
        correlated_error.correlation.timestamp -= 10; // 10 seconds ago
        correlated_error.update_age();

        assert!(correlated_error.age >= Duration::from_secs(10));
    }

    #[test]
    fn test_error_correlation_engine() {
        let mut engine = ErrorCorrelationEngine::new();

        let error = MonitoringError::exporter_error("prometheus", "connection failed");
        let correlation = engine.correlate_error(error, "prometheus", "export_metrics");

        assert_eq!(correlation.component, "prometheus");
        assert_eq!(correlation.operation, "export_metrics");
        assert_eq!(engine.correlated_errors.len(), 1);
    }

    #[test]
    fn test_get_errors_by_component() {
        let mut engine = ErrorCorrelationEngine::new();

        let error1 = MonitoringError::exporter_error("prometheus", "error 1");
        let error2 = MonitoringError::exporter_error("jaeger", "error 2");
        let error3 = MonitoringError::exporter_error("prometheus", "error 3");

        engine.correlate_error(error1, "prometheus", "operation1");
        engine.correlate_error(error2, "jaeger", "operation2");
        engine.correlate_error(error3, "prometheus", "operation3");

        let prometheus_errors = engine.get_errors_by_component("prometheus");
        assert_eq!(prometheus_errors.len(), 2);

        let jaeger_errors = engine.get_errors_by_component("jaeger");
        assert_eq!(jaeger_errors.len(), 1);
    }

    #[test]
    fn test_error_pattern_detection() {
        let mut engine = ErrorCorrelationEngine::with_config(100, 2); // Lower threshold for testing

        // Create multiple errors with the same pattern
        for i in 0..3 {
            let error = MonitoringError::exporter_error("prometheus", format!("error {}", i));
            engine.correlate_error(error, "prometheus", "export_metrics");
        }

        let patterns = engine.detect_error_patterns();
        assert_eq!(patterns.len(), 1);
        assert_eq!(patterns[0].component, "prometheus");
        assert_eq!(patterns[0].operation, "export_metrics");
        assert_eq!(patterns[0].occurrence_count, 3);
    }

    #[test]
    fn test_correlation_summary() {
        let mut engine = ErrorCorrelationEngine::new();

        let error1 = MonitoringError::exporter_error("prometheus", "error 1");
        let error2 = MonitoringError::health_check_failed("health check failed");

        engine.correlate_error(error1, "prometheus", "export");
        engine.correlate_error(error2, "health", "check");

        let summary = engine.get_correlation_summary();
        assert_eq!(summary.total_errors, 2);
        assert_eq!(summary.component_error_counts.len(), 2);
        assert_eq!(summary.component_error_counts.get("prometheus"), Some(&1));
        assert_eq!(summary.component_error_counts.get("health"), Some(&1));
    }

    #[test]
    fn test_cleanup_old_errors() {
        let mut engine = ErrorCorrelationEngine::new();

        let error = MonitoringError::exporter_error("test", "old error");
        engine.correlate_error(error, "test", "operation");

        // Make the error old
        engine.correlated_errors[0].correlation.timestamp -= 3600; // 1 hour ago

        assert_eq!(engine.correlated_errors.len(), 1);

        // Cleanup errors older than 30 minutes
        engine.cleanup_old_errors(Duration::from_secs(1800));

        assert_eq!(engine.correlated_errors.len(), 0);
    }

    #[test]
    fn test_format_for_log() {
        let task_id = TaskId::new_v4();
        let correlation =
            ErrorCorrelation::new("test_component", "test_operation").with_task_context(task_id);

        let log_format = correlation.format_for_log();
        assert!(log_format.contains("correlation_id="));
        assert!(log_format.contains("component=test_component"));
        assert!(log_format.contains("operation=test_operation"));
        assert!(log_format.contains(&format!("task_id={}", task_id)));
    }
}
