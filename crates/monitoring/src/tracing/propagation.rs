//! Cross-service trace context propagation helpers
//!
//! This module provides utilities for maintaining trace context across
//! service boundaries and async operations in the ORCHER platform.

use tracing::{info_span, Instrument, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Propagate current trace context to a new span
///
/// Creates a new span that inherits the trace context from the current span,
/// ensuring continuity across service boundaries.
///
/// # Arguments
///
/// * `span_name` - Name for the new span
/// * `operation` - Operation being performed
///
/// # Examples
///
/// ```rust
/// use orcher_monitoring::tracing::propagate_context;
/// use tracing::info_span;
///
/// // Start with an existing span
/// let _parent = info_span!("parent_operation").entered();
///
/// // Propagate context to child operation
/// let child_span = propagate_context("child_operation", "process");
/// let _guard = child_span.enter();
/// ```
pub fn propagate_context(span_name: &str, operation: &str) -> Span {
    let parent_context = Span::current().context();

    let span = info_span!(
        target: "orcher_tracing",
        "propagated_span",
        otel.name = span_name,
        otel.kind = "internal",
        operation = operation,
    );

    span.set_parent(parent_context);
    span
}

/// Create a child span with explicit parent context
///
/// Creates a new span as a child of the specified parent, useful for
/// async operations where the parent span may not be current.
///
/// # Arguments
///
/// * `parent` - The parent span to inherit context from
/// * `span_name` - Name for the child span
/// * `operation` - Operation being performed
///
/// # Examples
///
/// ```rust
/// use orcher_monitoring::tracing::create_child_span;
/// use tracing::info_span;
///
/// let parent = info_span!("parent_operation");
/// let child = create_child_span(&parent, "child_operation", "async_task");
/// let _guard = child.enter();
/// ```
pub fn create_child_span(parent: &Span, span_name: &str, operation: &str) -> Span {
    let parent_context = parent.context();

    let span = info_span!(
        target: "orcher_tracing",
        "child_span",
        otel.name = span_name,
        otel.kind = "internal",
        operation = operation,
        parent.name = parent.metadata().map(|m| m.name()).unwrap_or("unknown"),
    );

    span.set_parent(parent_context);
    span
}

/// Async-aware context propagation for tokio tasks
///
/// Wraps an async closure with trace context propagation, ensuring
/// that spawned tasks maintain the current trace context.
///
/// # Arguments
///
/// * `span_name` - Name for the async operation span
/// * `operation` - Operation being performed
/// * `future` - The async closure to execute with context
///
/// # Examples
///
/// ```rust,no_run
/// use orcher_monitoring::tracing::with_async_context;
/// use tracing::info_span;
///
/// async fn example() {
///     let _span = info_span!("parent").entered();
///
///     let result = with_async_context("async_work", "process", async {
///         // This async block maintains the parent trace context
///         42
///     }).await;
/// }
/// ```
pub async fn with_async_context<F, T>(span_name: &str, operation: &str, future: F) -> T
where
    F: std::future::Future<Output = T>,
{
    let span = propagate_context(span_name, operation);
    future.instrument(span).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing::info_span;
    use tracing_subscriber::fmt::TestWriter;

    fn init_test_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_writer(TestWriter::default())
            .with_max_level(tracing::Level::TRACE)
            .try_init();
    }

    #[test]
    fn test_propagate_context() {
        init_test_subscriber();
        let _parent = info_span!("parent").entered();
        let child = propagate_context("child", "test_op");

        assert!(child.metadata().is_some());
    }

    #[test]
    fn test_create_child_span() {
        init_test_subscriber();
        let parent = info_span!("parent");
        let child = create_child_span(&parent, "child", "test_op");

        assert!(child.metadata().is_some());
    }

    #[tokio::test]
    async fn test_async_context_propagation() {
        init_test_subscriber();
        let _parent = info_span!("parent").entered();

        let result = with_async_context("async_test", "compute", async { 42 }).await;

        assert_eq!(result, 42);
    }

    #[test]
    fn test_propagation_without_parent() {
        init_test_subscriber();
        // Should work even without active parent span
        let child = propagate_context("orphan", "test_op");
        assert!(child.metadata().is_some());
    }
}
