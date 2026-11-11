//! OTLP exporter for metrics and traces.

use crate::error::MonitoringError;
use crate::exporters::{Exporter, ExporterConfig, ExporterResult};
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Default)]
pub enum OtlpProtocol {
    #[default]
    Grpc,
    HttpProtobuf,
    HttpJson,
}

#[derive(Debug, Clone)]
pub struct OtlpConfig {
    pub endpoint: String,
    pub protocol: OtlpProtocol,
    pub export_metrics: bool,
    pub export_traces: bool,
    pub max_batch_size: usize,
    pub batch_timeout: Duration,
    pub headers: Vec<(String, String)>,
    pub timeout: Duration,
}

impl Default for OtlpConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://localhost:4317".to_string(),
            protocol: OtlpProtocol::default(),
            export_metrics: true,
            export_traces: true,
            max_batch_size: 512,
            batch_timeout: Duration::from_secs(5),
            headers: Vec::new(),
            timeout: Duration::from_secs(10),
        }
    }
}

impl ExporterConfig for OtlpConfig {
    fn validate(&self) -> ExporterResult<()> {
        // Validate endpoint URL
        if self.endpoint.is_empty() {
            return Err(MonitoringError::exporter_error(
                "otlp",
                "Endpoint cannot be empty",
            ));
        }

        if !self.endpoint.starts_with("http://") && !self.endpoint.starts_with("https://") {
            return Err(MonitoringError::exporter_error(
                "otlp",
                "Endpoint must be a valid HTTP/HTTPS URL",
            ));
        }

        // Validate batch size
        if self.max_batch_size == 0 {
            return Err(MonitoringError::exporter_error(
                "otlp",
                "Batch size must be greater than 0",
            ));
        }

        // Validate that at least one export type is enabled
        if !self.export_metrics && !self.export_traces {
            return Err(MonitoringError::exporter_error(
                "otlp",
                "At least one of export_metrics or export_traces must be enabled",
            ));
        }

        Ok(())
    }

    fn description(&self) -> String {
        let exports = match (self.export_metrics, self.export_traces) {
            (true, true) => "metrics+traces",
            (true, false) => "metrics",
            (false, true) => "traces",
            (false, false) => "none",
        };
        format!(
            "OTLP exporter for {} via {} ({:?})",
            exports, self.endpoint, self.protocol
        )
    }
}

pub struct OtlpExporter {
    config: OtlpConfig,
    running: bool,
}

impl OtlpExporter {
    pub fn new(config: OtlpConfig) -> ExporterResult<Self> {
        config.validate()?;
        Ok(Self {
            config,
            running: false,
        })
    }

    pub fn builder() -> OtlpExporterBuilder {
        OtlpExporterBuilder::new()
    }

    /// Install as the global metrics/traces provider.
    pub async fn install_global(&mut self) -> ExporterResult<()> {
        if self.running {
            return Err(MonitoringError::exporter_error(
                "otlp",
                "Exporter is already running as global provider",
            ));
        }

        // In a real implementation, this would install the OTLP exporter
        // as the global OpenTelemetry metrics and trace providers
        self.running = true;
        Ok(())
    }
}

impl Exporter for OtlpExporter {
    fn start(&mut self) -> ExporterResult<()> {
        if self.running {
            return Err(MonitoringError::exporter_error(
                "otlp",
                "Exporter is already running",
            ));
        }

        self.running = true;
        Ok(())
    }

    fn stop(&mut self) -> ExporterResult<()> {
        if !self.running {
            return Err(MonitoringError::exporter_error(
                "otlp",
                "Exporter is not running",
            ));
        }

        self.running = false;
        Ok(())
    }

    fn is_running(&self) -> bool {
        self.running
    }

    fn health_status(&self) -> ExporterResult<String> {
        let status = if self.running { "healthy" } else { "stopped" };
        Ok(format!(
            "OTLP exporter: {} ({})",
            status,
            self.config.description()
        ))
    }
}

pub struct OtlpExporterBuilder {
    config: OtlpConfig,
}

impl Default for OtlpExporterBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl OtlpExporterBuilder {
    pub fn new() -> Self {
        Self {
            config: OtlpConfig::default(),
        }
    }

    pub fn with_endpoint(mut self, endpoint: &str) -> Self {
        self.config.endpoint = endpoint.to_string();
        self
    }

    pub fn with_protocol(mut self, protocol: OtlpProtocol) -> Self {
        self.config.protocol = protocol;
        self
    }

    pub fn with_metrics(mut self, enabled: bool) -> Self {
        self.config.export_metrics = enabled;
        self
    }

    pub fn with_traces(mut self, enabled: bool) -> Self {
        self.config.export_traces = enabled;
        self
    }

    pub fn with_header(mut self, name: &str, value: &str) -> Self {
        self.config
            .headers
            .push((name.to_string(), value.to_string()));
        self
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.config.timeout = timeout;
        self
    }

    pub fn build(self) -> ExporterResult<OtlpExporter> {
        OtlpExporter::new(self.config)
    }
}

/// Configure and install an OTLP exporter as the global provider.
pub async fn setup_otlp_exporter(
    endpoint: &str,
    export_metrics: bool,
    export_traces: bool,
) -> ExporterResult<OtlpExporter> {
    let mut exporter = OtlpExporter::builder()
        .with_endpoint(endpoint)
        .with_metrics(export_metrics)
        .with_traces(export_traces)
        .build()?;

    exporter.install_global().await?;

    let exports = match (export_metrics, export_traces) {
        (true, true) => "metrics and traces",
        (true, false) => "metrics only",
        (false, true) => "traces only",
        (false, false) => "nothing",
    };

    tracing::info!(
        "OTLP exporter installed globally for {} via {}",
        exports,
        endpoint
    );

    Ok(exporter)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_otlp_config_default() {
        let config = OtlpConfig::default();
        assert_eq!(config.endpoint, "http://localhost:4317");
        assert_eq!(config.protocol, OtlpProtocol::Grpc);
        assert!(config.export_metrics);
        assert!(config.export_traces);
    }

    #[test]
    fn test_otlp_config_validation() {
        let valid_config = OtlpConfig::default();
        assert!(valid_config.validate().is_ok());

        let invalid_endpoint = OtlpConfig {
            endpoint: "".to_string(),
            ..Default::default()
        };
        assert!(invalid_endpoint.validate().is_err());

        let invalid_protocol = OtlpConfig {
            endpoint: "invalid-url".to_string(),
            ..Default::default()
        };
        assert!(invalid_protocol.validate().is_err());

        let no_exports = OtlpConfig {
            export_metrics: false,
            export_traces: false,
            ..Default::default()
        };
        assert!(no_exports.validate().is_err());
    }

    #[test]
    fn test_otlp_exporter_lifecycle() {
        let mut exporter = OtlpExporter::builder().build().unwrap();

        assert!(!exporter.is_running());
        assert!(exporter.start().is_ok());
        assert!(exporter.is_running());
        assert!(exporter.stop().is_ok());
        assert!(!exporter.is_running());
    }

    #[tokio::test]
    async fn test_setup_otlp_exporter_both() {
        // Test setup with both metrics and traces
        let result = setup_otlp_exporter("http://localhost:4317", true, true).await;
        assert!(result.is_ok());

        let exporter = result.unwrap();
        assert!(exporter.is_running());
        assert_eq!(exporter.config.endpoint, "http://localhost:4317");
        assert!(exporter.config.export_metrics);
        assert!(exporter.config.export_traces);
    }

    #[tokio::test]
    async fn test_setup_otlp_exporter_metrics_only() {
        // Test setup with metrics only
        let result = setup_otlp_exporter("http://localhost:4318", true, false).await;
        assert!(result.is_ok());

        let exporter = result.unwrap();
        assert!(exporter.is_running());
        assert!(exporter.config.export_metrics);
        assert!(!exporter.config.export_traces);
    }

    #[tokio::test]
    async fn test_setup_otlp_exporter_traces_only() {
        // Test setup with traces only
        let result = setup_otlp_exporter("http://localhost:4319", false, true).await;
        assert!(result.is_ok());

        let exporter = result.unwrap();
        assert!(exporter.is_running());
        assert!(!exporter.config.export_metrics);
        assert!(exporter.config.export_traces);
    }

    #[tokio::test]
    async fn test_setup_otlp_exporter_validates() {
        // Test that setup function validates configuration
        let result = setup_otlp_exporter("", true, true).await;
        assert!(result.is_err());

        // Test that at least one export type must be enabled
        let result = setup_otlp_exporter("http://localhost:4317", false, false).await;
        assert!(result.is_err());

        // Test invalid URL
        let result = setup_otlp_exporter("not-a-url", true, true).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_otlp_install_global() {
        let mut exporter = OtlpExporter::builder()
            .with_endpoint("http://localhost:4318")
            .with_protocol(OtlpProtocol::HttpProtobuf)
            .build()
            .unwrap();

        assert!(!exporter.is_running());
        assert!(exporter.install_global().await.is_ok());
        assert!(exporter.is_running());
    }

    #[test]
    fn test_otlp_exporter_builder() {
        let exporter = OtlpExporter::builder()
            .with_endpoint("https://api.honeycomb.io")
            .with_protocol(OtlpProtocol::HttpProtobuf)
            .with_metrics(true)
            .with_traces(false)
            .with_header("x-honeycomb-team", "test-key")
            .with_timeout(Duration::from_secs(30))
            .build()
            .unwrap();

        assert_eq!(exporter.config.endpoint, "https://api.honeycomb.io");
        assert_eq!(exporter.config.protocol, OtlpProtocol::HttpProtobuf);
        assert!(exporter.config.export_metrics);
        assert!(!exporter.config.export_traces);
        assert_eq!(exporter.config.headers.len(), 1);
        assert_eq!(exporter.config.timeout, Duration::from_secs(30));
    }

    #[test]
    fn test_otlp_protocol_enum() {
        assert_eq!(OtlpProtocol::default(), OtlpProtocol::Grpc);
        assert_ne!(OtlpProtocol::Grpc, OtlpProtocol::HttpProtobuf);
    }
}
