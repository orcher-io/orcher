//! ORCHER Tracing Module
//!
//! Provides distributed tracing capabilities for ORCHER platform components.
//! Built on top of the `tracing` crate with OpenTelemetry integration.
//!
//! # Features
//!
//! - Standardized span creation for ORCHER operations
//! - Trace context utilities and propagation
//! - Integration with OpenTelemetry semantic conventions
//! - Cross-service trace continuity

pub mod context;
pub mod spans;

#[cfg(feature = "tracing-export")]
pub mod propagation;

#[cfg(feature = "tracing-export")]
pub mod instrumentation;

// Re-export commonly used tracing types
pub use tracing::{Level, Span, Subscriber};

// Re-export span helpers
pub use spans::{create_storage_span, create_task_span, create_worker_span, create_workflow_span};

// Re-export context utilities
pub use context::{current_span_id, current_trace_id};

// Feature-gated re-exports
#[cfg(feature = "tracing-export")]
pub use context::{extract_context, inject_context};

#[cfg(feature = "tracing-export")]
pub use propagation::{create_child_span, propagate_context, with_async_context};
