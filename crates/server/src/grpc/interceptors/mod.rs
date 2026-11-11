//! gRPC interceptors for authentication, tracing, and metrics
//!
//! This module provides interceptors that can be attached to gRPC clients
//! to add cross-cutting concerns like authentication, distributed tracing,
//! and metrics collection.

use tonic::{Request, Status};

/// Authentication interceptor for adding credentials to gRPC requests
#[derive(Clone)]
pub struct AuthInterceptor {
    /// API key or token for authentication
    token: Option<String>,
}

impl AuthInterceptor {
    /// Create a new authentication interceptor
    #[must_use]
    pub fn new(token: Option<String>) -> Self {
        Self { token }
    }

    /// Create an interceptor with a token
    #[must_use]
    pub fn with_token(token: impl Into<String>) -> Self {
        Self {
            token: Some(token.into()),
        }
    }

    /// Intercept a gRPC request and add authentication headers
    pub fn call<T>(&self, mut request: Request<T>) -> Result<Request<T>, Status> {
        if let Some(token) = &self.token {
            request.metadata_mut().insert(
                "authorization",
                token
                    .parse()
                    .map_err(|_| Status::invalid_argument("Invalid authorization token format"))?,
            );
        }
        Ok(request)
    }
}

/// Tracing interceptor for distributed tracing
#[derive(Clone)]
pub struct TracingInterceptor {
    /// Trace ID for the current request context
    trace_id: Option<String>,
}

impl TracingInterceptor {
    /// Create a new tracing interceptor
    #[must_use]
    pub fn new() -> Self {
        Self { trace_id: None }
    }

    /// Create a tracing interceptor with a specific trace ID
    #[must_use]
    pub fn with_trace_id(trace_id: impl Into<String>) -> Self {
        Self {
            trace_id: Some(trace_id.into()),
        }
    }

    /// Intercept a gRPC request and add tracing headers
    pub fn call<T>(&self, mut request: Request<T>) -> Result<Request<T>, Status> {
        let trace_id = self
            .trace_id
            .clone()
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        request.metadata_mut().insert(
            "x-trace-id",
            trace_id
                .parse()
                .map_err(|_| Status::invalid_argument("Invalid trace ID format"))?,
        );

        Ok(request)
    }
}

impl Default for TracingInterceptor {
    fn default() -> Self {
        Self::new()
    }
}

/// Metrics interceptor for collecting request metrics
#[derive(Clone, Default)]
pub struct MetricsInterceptor {
    /// Enable request counting
    enabled: bool,
}

impl MetricsInterceptor {
    /// Create a new metrics interceptor
    #[must_use]
    pub fn new() -> Self {
        Self { enabled: true }
    }

    /// Intercept a gRPC request and record metrics
    pub fn call<T>(&self, request: Request<T>) -> Result<Request<T>, Status> {
        if self.enabled {
            // In a real implementation, this would record metrics
            // For now, we just pass the request through
        }
        Ok(request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auth_interceptor_creation() {
        let interceptor = AuthInterceptor::new(None);
        assert!(interceptor.token.is_none());

        let interceptor = AuthInterceptor::with_token("test-token");
        assert!(interceptor.token.is_some());
    }

    #[test]
    fn test_tracing_interceptor_creation() {
        let interceptor = TracingInterceptor::new();
        assert!(interceptor.trace_id.is_none());

        let interceptor = TracingInterceptor::with_trace_id("trace-123");
        assert!(interceptor.trace_id.is_some());
    }

    #[test]
    fn test_metrics_interceptor_creation() {
        let interceptor = MetricsInterceptor::new();
        assert!(interceptor.enabled);
    }

    #[test]
    fn test_auth_interceptor_adds_header() {
        let interceptor = AuthInterceptor::with_token("Bearer test-token");
        let request = Request::new(());

        let result = interceptor.call(request);
        assert!(result.is_ok());

        let request = result.unwrap();
        let auth_header = request.metadata().get("authorization");
        assert!(auth_header.is_some());
    }

    #[test]
    fn test_tracing_interceptor_adds_trace_id() {
        let interceptor = TracingInterceptor::with_trace_id("trace-456");
        let request = Request::new(());

        let result = interceptor.call(request);
        assert!(result.is_ok());

        let request = result.unwrap();
        let trace_header = request.metadata().get("x-trace-id");
        assert!(trace_header.is_some());
    }
}
