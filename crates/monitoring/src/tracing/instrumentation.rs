//! Instrumentation utilities for automatic tracing
//!
//! This module provides utilities for automatic instrumentation of ORCHER
//! components, including macros and helpers for consistent tracing.

use std::time::Instant;
use tracing::{info, warn, Instrument, Span};

/// Instrumentation helper for timing operations
///
/// Records the duration of an operation and logs performance metrics.
/// Useful for tracking operation performance across ORCHER components.
pub struct OperationTimer {
    start_time: Instant,
    operation_name: String,
    span: Span,
}

impl OperationTimer {
    /// Create a new operation timer
    ///
    /// # Arguments
    ///
    /// * `operation_name` - Name of the operation being timed
    /// * `span` - The span to record timing information to
    pub fn new(operation_name: &str, span: Span) -> Self {
        info!(operation = operation_name, "Operation started");

        Self {
            start_time: Instant::now(),
            operation_name: operation_name.to_string(),
            span,
        }
    }

    /// Finish the operation and record its duration
    pub fn finish(self) {
        let duration = self.start_time.elapsed();
        let duration_ms = duration.as_millis();

        self.span.record("operation.duration_ms", duration_ms);

        if duration_ms > 1000 {
            warn!(
                operation = %self.operation_name,
                duration_ms = duration_ms,
                "Slow operation detected"
            );
        } else {
            info!(
                operation = %self.operation_name,
                duration_ms = duration_ms,
                "Operation completed"
            );
        }
    }
}

/// Automatic instrumentation for function execution
///
/// Wraps a function call with automatic span creation and timing.
/// Records function name, arguments (if serializable), and execution time.
///
/// # Arguments
///
/// * `function_name` - Name of the function being instrumented
/// * `f` - The function to execute
///
/// # Examples
///
/// ```rust
/// use orcher_monitoring::tracing::instrumentation::instrument_function;
///
/// fn process_data(data: &str) -> String {
///     data.to_uppercase()
/// }
///
/// let result = instrument_function("process_data", || process_data("hello"));
/// assert_eq!(result, "HELLO");
/// ```
pub fn instrument_function<F, R>(function_name: &str, f: F) -> R
where
    F: FnOnce() -> R,
{
    let span = tracing::info_span!(
        "function_call",
        otel.name = function_name,
        otel.kind = "internal",
        function.name = function_name,
    );

    let _guard = span.enter();
    let timer = OperationTimer::new(function_name, span.clone());

    let result = f();
    timer.finish();

    result
}

/// Async version of function instrumentation
///
/// Wraps an async function call with automatic span creation and timing.
///
/// # Arguments
///
/// * `function_name` - Name of the function being instrumented
/// * `f` - The async function to execute
///
/// # Examples
///
/// ```rust,no_run
/// use orcher_monitoring::tracing::instrumentation::instrument_async_function;
///
/// async fn async_process_data(data: &str) -> String {
///     tokio::time::sleep(std::time::Duration::from_millis(10)).await;
///     data.to_uppercase()
/// }
///
/// # tokio::runtime::Runtime::new().unwrap().block_on(async {
/// let result = instrument_async_function("async_process_data", || async {
///     async_process_data("hello").await
/// }).await;
/// assert_eq!(result, "HELLO");
/// # });
/// ```
pub async fn instrument_async_function<F, Fut, R>(function_name: &str, f: F) -> R
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = R>,
{
    let span = tracing::info_span!(
        "async_function_call",
        otel.name = function_name,
        otel.kind = "internal",
        function.name = function_name,
    );

    let timer = OperationTimer::new(function_name, span.clone());

    let result = f().instrument(span).await;
    timer.finish();

    result
}

/// Error boundary instrumentation
///
/// Wraps a fallible operation with error tracking and logging.
/// Records both successful executions and error conditions.
///
/// # Arguments
///
/// * `operation_name` - Name of the operation
/// * `f` - The fallible function to execute
///
/// # Examples
///
/// ```rust
/// use orcher_monitoring::tracing::instrumentation::instrument_with_error_boundary;
///
/// fn might_fail(should_fail: bool) -> Result<String, &'static str> {
///     if should_fail {
///         Err("Operation failed")
///     } else {
///         Ok("Success".to_string())
///     }
/// }
///
/// let result = instrument_with_error_boundary("test_operation", || {
///     might_fail(false)
/// });
/// assert!(result.is_ok());
/// ```
pub fn instrument_with_error_boundary<F, R, E>(operation_name: &str, f: F) -> Result<R, E>
where
    F: FnOnce() -> Result<R, E>,
    E: std::fmt::Display,
{
    let span = tracing::info_span!(
        "error_boundary",
        otel.name = operation_name,
        otel.kind = "internal",
        operation.name = operation_name,
    );

    let _guard = span.enter();
    let timer = OperationTimer::new(operation_name, span.clone());

    match f() {
        Ok(result) => {
            span.record("operation.status", "success");
            timer.finish();
            Ok(result)
        }
        Err(error) => {
            span.record("operation.status", "error");
            span.record("error.message", format!("{}", error));

            tracing::error!(
                operation = operation_name,
                error = %error,
                "Operation failed"
            );

            timer.finish();
            Err(error)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing::info_span;

    #[test]
    fn test_operation_timer() {
        let span = info_span!("test");
        let timer = OperationTimer::new("test_operation", span);

        // Simulate some work
        std::thread::sleep(std::time::Duration::from_millis(1));

        // Should not panic when finished
        timer.finish();
    }

    #[test]
    fn test_instrument_function() {
        let result = instrument_function("test_func", || 42);

        assert_eq!(result, 42);
    }

    #[tokio::test]
    async fn test_instrument_async_function() {
        let result = instrument_async_function("async_test", || async {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            42
        })
        .await;

        assert_eq!(result, 42);
    }

    #[test]
    fn test_error_boundary_success() {
        let result = instrument_with_error_boundary("success_test", || Ok::<i32, &str>(42));

        assert_eq!(result.unwrap(), 42);
    }

    #[test]
    fn test_error_boundary_failure() {
        let result =
            instrument_with_error_boundary("failure_test", || Err::<i32, &str>("test error"));

        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "test error");
    }
}
