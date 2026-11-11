//! Custom test assertions for monitoring tests
//!
//! This module provides custom assertion helpers for validating metrics,
//! traces, and health checks in tests.

use crate::testing::mock_exporter::MockExporter;
use crate::testing::mock_tracer::{MockSpan, MockTracer, SpanStatus};

/// Assert that a counter has a specific value
///
/// # Examples
///
/// ```rust,ignore
/// # use orcher_monitoring::testing::prelude::*;
/// # let exporter = MockExporter::new();
/// exporter.record_counter("test_counter", 5);
/// assert_counter_eq!(exporter, "test_counter", 5);
/// ```
#[macro_export]
macro_rules! assert_counter_eq {
    ($exporter:expr, $name:expr, $expected:expr) => {
        let actual = $exporter.get_counter($name);
        assert_eq!(
            actual, $expected,
            "Counter '{}' expected to be {}, but was {}",
            $name, $expected, actual
        );
    };
}

/// Assert that a counter is greater than a value
#[macro_export]
macro_rules! assert_counter_gt {
    ($exporter:expr, $name:expr, $threshold:expr) => {
        let actual = $exporter.get_counter($name);
        assert!(
            actual > $threshold,
            "Counter '{}' expected to be greater than {}, but was {}",
            $name,
            $threshold,
            actual
        );
    };
}

/// Assert that a gauge has a specific value
#[macro_export]
macro_rules! assert_gauge_eq {
    ($exporter:expr, $name:expr, $expected:expr) => {
        let actual = $exporter.get_gauge($name);
        assert_eq!(
            actual,
            Some($expected),
            "Gauge '{}' expected to be {:?}, but was {:?}",
            $name,
            Some($expected),
            actual
        );
    };
}

/// Assert that a gauge exists
#[macro_export]
macro_rules! assert_gauge_exists {
    ($exporter:expr, $name:expr) => {
        let actual = $exporter.get_gauge($name);
        assert!(
            actual.is_some(),
            "Gauge '{}' expected to exist, but was not found",
            $name
        );
    };
}

/// Assert that a histogram has recorded values
#[macro_export]
macro_rules! assert_histogram_recorded {
    ($exporter:expr, $name:expr) => {
        let values = $exporter.get_histogram_values($name);
        assert!(
            !values.is_empty(),
            "Histogram '{}' expected to have recorded values, but was empty",
            $name
        );
    };
}

/// Assert that a span with a given name exists
#[macro_export]
macro_rules! assert_span_exists {
    ($tracer:expr, $name:expr) => {
        let spans = $tracer.get_spans_by_name($name);
        assert!(
            !spans.is_empty(),
            "Expected to find span with name '{}', but none found",
            $name
        );
    };
}

/// Assert that a span completed successfully
#[macro_export]
macro_rules! assert_span_ok {
    ($span:expr) => {
        assert!(
            $span.is_completed(),
            "Expected span '{}' to be completed",
            $span.name
        );
        assert_eq!(
            $span.status,
            $crate::testing::mock_tracer::SpanStatus::Ok,
            "Expected span '{}' to have status Ok, but was {:?}",
            $span.name,
            $span.status
        );
    };
}

/// Assert that a span has an error
#[macro_export]
macro_rules! assert_span_error {
    ($span:expr) => {
        assert!(
            $span.has_error(),
            "Expected span '{}' to have an error",
            $span.name
        );
    };
}

/// Helper functions for metric assertions
pub mod metrics {
    use super::*;

    /// Assert that a counter value equals expected
    pub fn assert_counter_value(exporter: &MockExporter, name: &str, expected: u64) {
        let actual = exporter.get_counter(name);
        assert_eq!(
            actual, expected,
            "Counter '{}' expected to be {}, but was {}",
            name, expected, actual
        );
    }

    /// Assert that a counter was incremented
    pub fn assert_counter_incremented(exporter: &MockExporter, name: &str) {
        let value = exporter.get_counter(name);
        assert!(
            value > 0,
            "Counter '{}' expected to be incremented, but was {}",
            name,
            value
        );
    }

    /// Assert that a gauge has a value
    pub fn assert_gauge_value(exporter: &MockExporter, name: &str, expected: f64) {
        let actual = exporter.get_gauge(name);
        assert_eq!(
            actual,
            Some(expected),
            "Gauge '{}' expected to be {:?}, but was {:?}",
            name,
            Some(expected),
            actual
        );
    }

    /// Assert that a gauge is within a range
    pub fn assert_gauge_in_range(exporter: &MockExporter, name: &str, min: f64, max: f64) {
        let actual = exporter
            .get_gauge(name)
            .unwrap_or_else(|| panic!("Gauge '{}' not found", name));
        assert!(
            actual >= min && actual <= max,
            "Gauge '{}' expected to be between {} and {}, but was {}",
            name,
            min,
            max,
            actual
        );
    }

    /// Assert that a histogram has a specific count of values
    pub fn assert_histogram_count(exporter: &MockExporter, name: &str, expected: usize) {
        let values = exporter.get_histogram_values(name);
        assert_eq!(
            values.len(),
            expected,
            "Histogram '{}' expected to have {} values, but had {}",
            name,
            expected,
            values.len()
        );
    }

    /// Assert that all histogram values are within a range
    pub fn assert_histogram_values_in_range(
        exporter: &MockExporter,
        name: &str,
        min: f64,
        max: f64,
    ) {
        let values = exporter.get_histogram_values(name);
        for (i, &value) in values.iter().enumerate() {
            assert!(
                value >= min && value <= max,
                "Histogram '{}' value at index {} expected to be between {} and {}, but was {}",
                name,
                i,
                min,
                max,
                value
            );
        }
    }
}

/// Helper functions for trace assertions
pub mod tracing {
    use super::*;

    /// Assert that a span with given name exists
    pub fn assert_span_exists(tracer: &MockTracer, name: &str) {
        let spans = tracer.get_spans_by_name(name);
        assert!(
            !spans.is_empty(),
            "Expected to find span with name '{}', but none found",
            name
        );
    }

    /// Assert that a specific number of spans exist with given name
    pub fn assert_span_count(tracer: &MockTracer, name: &str, expected: usize) {
        let spans = tracer.get_spans_by_name(name);
        assert_eq!(
            spans.len(),
            expected,
            "Expected {} spans with name '{}', but found {}",
            expected,
            name,
            spans.len()
        );
    }

    /// Assert that a span completed successfully
    pub fn assert_span_completed_ok(span: &MockSpan) {
        assert!(
            span.is_completed(),
            "Expected span '{}' to be completed",
            span.name
        );
        assert_eq!(
            span.status,
            SpanStatus::Ok,
            "Expected span '{}' to have status Ok, but was {:?}",
            span.name,
            span.status
        );
    }

    /// Assert that a span has an error status
    pub fn assert_span_has_error(span: &MockSpan) {
        assert!(
            span.has_error(),
            "Expected span '{}' to have an error",
            span.name
        );
    }

    /// Assert that a span has a specific attribute
    pub fn assert_span_attribute(span: &MockSpan, key: &str, expected: &str) {
        let actual = span.get_attribute(key);
        assert_eq!(
            actual,
            Some(expected),
            "Expected span '{}' to have attribute '{}' with value '{}', but was {:?}",
            span.name,
            key,
            expected,
            actual
        );
    }

    /// Assert that a span has a parent
    pub fn assert_span_has_parent(span: &MockSpan) {
        assert!(
            span.parent_id.is_some(),
            "Expected span '{}' to have a parent span",
            span.name
        );
    }

    /// Assert that a span is a root span (no parent)
    pub fn assert_span_is_root(span: &MockSpan) {
        assert!(
            span.parent_id.is_none(),
            "Expected span '{}' to be a root span, but it has parent {:?}",
            span.name,
            span.parent_id
        );
    }

    /// Assert that a span has child spans
    pub fn assert_span_has_children(tracer: &MockTracer, parent_span: &MockSpan) {
        let children = tracer.get_child_spans(&parent_span.span_id);
        assert!(
            !children.is_empty(),
            "Expected span '{}' to have child spans",
            parent_span.name
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::mock_exporter::MockExporter;
    use crate::testing::mock_tracer::MockTracer;

    #[test]
    fn test_counter_assertions() {
        let exporter = MockExporter::new();
        exporter.record_counter("test_counter", 5);

        metrics::assert_counter_value(&exporter, "test_counter", 5);
        metrics::assert_counter_incremented(&exporter, "test_counter");
    }

    #[test]
    fn test_gauge_assertions() {
        let exporter = MockExporter::new();
        exporter.record_gauge("test_gauge", 42.0);

        metrics::assert_gauge_value(&exporter, "test_gauge", 42.0);
        metrics::assert_gauge_in_range(&exporter, "test_gauge", 40.0, 45.0);
    }

    #[test]
    fn test_histogram_assertions() {
        let exporter = MockExporter::new();
        exporter.record_histogram("test_histogram", 1.0);
        exporter.record_histogram("test_histogram", 2.0);
        exporter.record_histogram("test_histogram", 3.0);

        metrics::assert_histogram_count(&exporter, "test_histogram", 3);
        metrics::assert_histogram_values_in_range(&exporter, "test_histogram", 0.5, 3.5);
    }

    #[test]
    fn test_span_assertions() {
        let tracer = MockTracer::new();
        let span_id = tracer.start_span("test_span");
        tracer.add_attribute(&span_id, "key", "value");
        tracer.end_span(&span_id);

        tracing::assert_span_exists(&tracer, "test_span");
        tracing::assert_span_count(&tracer, "test_span", 1);

        let span = tracer.get_span(&span_id).unwrap();
        tracing::assert_span_completed_ok(&span);
        tracing::assert_span_attribute(&span, "key", "value");
        tracing::assert_span_is_root(&span);
    }

    #[test]
    fn test_child_span_assertions() {
        let tracer = MockTracer::new();
        let parent_id = tracer.start_span("parent");
        let child_id = tracer.start_child_span(&parent_id, "child");

        let parent_span = tracer.get_span(&parent_id).unwrap();
        let child_span = tracer.get_span(&child_id).unwrap();

        tracing::assert_span_is_root(&parent_span);
        tracing::assert_span_has_parent(&child_span);
        tracing::assert_span_has_children(&tracer, &parent_span);
    }
}
