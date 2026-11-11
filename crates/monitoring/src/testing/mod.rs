//! Testing utilities for the monitoring system
//!
//! This module provides mock implementations and test helpers for the monitoring
//! system components when the `test-utils` feature is enabled.
//!
//! Uses orcher-common types for consistency across the platform.

pub mod assertions;
pub mod fixtures;
pub mod mock_exporter;
pub mod mock_tracer;

// Re-export legacy test utilities for backward compatibility
pub use fixtures::{
    config::TestConfigBuilder, health::MockHealthCheck, metrics::MockMetricsCollector,
};

/// Prelude for convenient testing imports
pub mod prelude {
    pub use super::assertions::{metrics as assertion_metrics, tracing as assertion_tracing};
    pub use super::fixtures::{config, health, metrics};
    pub use super::mock_exporter::MockExporter;
    pub use super::mock_tracer::{MockSpan, MockSpanEvent, MockTracer, SpanStatus};
}
