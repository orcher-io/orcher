//! Trace context utilities
//!
//! Provides utilities for extracting trace context information and working
//! with distributed tracing contexts across service boundaries.

use tracing::Span;

/// Extract the current trace ID from the active span
///
/// Returns the trace ID as a string if available, or None if no active span exists.
///
/// # Examples
///
/// ```rust
/// use orcher_monitoring::tracing::current_trace_id;
/// use tracing::info_span;
///
/// let _span = info_span!("test_operation").entered();
/// if let Some(trace_id) = current_trace_id() {
///     println!("Current trace ID: {}", trace_id);
/// }
/// ```
pub fn current_trace_id() -> Option<String> {
    Span::current().id().map(|id| format!("{:?}", id))
}

/// Extract the current span ID from the active span
///
/// Returns the span ID as a string if available, or None if no active span exists.
///
/// # Examples
///
/// ```rust
/// use orcher_monitoring::tracing::current_span_id;
/// use tracing::info_span;
///
/// let _span = info_span!("test_operation").entered();
/// if let Some(span_id) = current_span_id() {
///     println!("Current span ID: {}", span_id);
/// }
/// ```
pub fn current_span_id() -> Option<String> {
    Span::current().id().map(|id| format!("{:?}", id))
}

#[cfg(feature = "tracing-export")]
mod export_utils {
    /// Header injector for OpenTelemetry context propagation
    pub struct HeaderInjector<'a>(pub &'a mut http::HeaderMap);

    impl<'a> opentelemetry::propagation::Injector for HeaderInjector<'a> {
        fn set(&mut self, key: &str, value: String) {
            if let Ok(header_name) = http::HeaderName::from_bytes(key.as_bytes()) {
                if let Ok(header_value) = http::HeaderValue::from_str(&value) {
                    self.0.insert(header_name, header_value);
                }
            }
        }
    }

    /// Header extractor for OpenTelemetry context propagation
    pub struct HeaderExtractor<'a>(pub &'a http::HeaderMap);

    impl<'a> opentelemetry::propagation::Extractor for HeaderExtractor<'a> {
        fn get(&self, key: &str) -> Option<&str> {
            self.0.get(key).and_then(|value| value.to_str().ok())
        }

        fn keys(&self) -> Vec<&str> {
            self.0.keys().map(|key| key.as_str()).collect()
        }
    }
}

#[cfg(feature = "tracing-export")]
pub use export_utils::{HeaderExtractor, HeaderInjector};

#[cfg(feature = "tracing-export")]
/// Inject trace context into HTTP headers
///
/// This function extracts the current OpenTelemetry context and injects it
/// into the provided HTTP headers for cross-service propagation.
///
/// # Arguments
///
/// * `headers` - Mutable reference to HTTP headers to inject context into
///
/// # Examples
///
/// ```rust,no_run
/// use orcher_monitoring::tracing::inject_context;
/// use tracing::info_span;
/// use http::HeaderMap;
///
/// let _span = info_span!("http_request").entered();
/// let mut headers = HeaderMap::new();
/// inject_context(&mut headers);
/// ```
pub fn inject_context(headers: &mut http::HeaderMap) {
    use opentelemetry::global;
    use tracing_opentelemetry::OpenTelemetrySpanExt;

    let context = Span::current().context();
    global::get_text_map_propagator(|propagator| {
        propagator.inject_context(&context, &mut HeaderInjector(headers));
    });
}

#[cfg(feature = "tracing-export")]
/// Extract trace context from HTTP headers
///
/// This function extracts OpenTelemetry context from HTTP headers,
/// allowing continuation of distributed traces across service boundaries.
///
/// # Arguments
///
/// * `headers` - Reference to HTTP headers containing trace context
///
/// # Returns
///
/// Returns an OpenTelemetry context if trace headers are present, None otherwise.
///
/// # Examples
///
/// ```rust,no_run
/// use orcher_monitoring::tracing::extract_context;
/// use http::HeaderMap;
///
/// let headers = HeaderMap::new();
/// if let Some(context) = extract_context(&headers) {
///     println!("Found trace context in headers");
/// }
/// ```
pub fn extract_context(headers: &http::HeaderMap) -> Option<opentelemetry::Context> {
    use opentelemetry::global;

    let extractor = HeaderExtractor(headers);
    Some(global::get_text_map_propagator(|propagator| {
        propagator.extract(&extractor)
    }))
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
    fn test_current_trace_id_no_span() {
        assert!(current_trace_id().is_none());
    }

    #[test]
    fn test_current_span_id_no_span() {
        assert!(current_span_id().is_none());
    }

    #[test]
    fn test_trace_context_with_span() {
        init_test_subscriber();
        let _span = info_span!("test_span").entered();

        // Should have trace and span IDs when span is active
        assert!(current_trace_id().is_some());
        assert!(current_span_id().is_some());
    }

    #[cfg(feature = "tracing-export")]
    #[test]
    fn test_header_injection_extraction() {
        use http::HeaderMap;

        let _span = info_span!("test_span").entered();
        let mut headers = HeaderMap::new();

        // Should not panic when injecting context
        inject_context(&mut headers);

        // Should not panic when extracting context
        let _context = extract_context(&headers);
    }
}
