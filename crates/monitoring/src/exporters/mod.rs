//! Exporters for Prometheus, Jaeger, and OTLP backends.

pub mod jaeger;
pub mod multi;
pub mod otlp;
pub mod prometheus;

pub use jaeger::{JaegerConfig, JaegerExporter};
pub use multi::{ExporterGroup, MultiExporter};
pub use otlp::{OtlpConfig, OtlpExporter, OtlpProtocol};
pub use prometheus::{PrometheusConfig, PrometheusExporter};

pub use jaeger::setup_jaeger_tracer;
pub use otlp::setup_otlp_exporter;
pub use prometheus::setup_prometheus_exporter;

use crate::error::MonitoringError;

pub type ExporterResult<T> = Result<T, MonitoringError>;

pub trait ExporterConfig {
    fn validate(&self) -> ExporterResult<()>;
    fn description(&self) -> String;
}

pub trait Exporter {
    fn start(&mut self) -> ExporterResult<()>;
    fn stop(&mut self) -> ExporterResult<()>;
    fn is_running(&self) -> bool;
    fn health_status(&self) -> ExporterResult<String>;
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_module_structure() {
        // Basic test to verify module compiles
        assert!(true);
    }
}
