//! Jaeger distributed tracing exporter.

use crate::error::MonitoringError;
use crate::exporters::{Exporter, ExporterConfig, ExporterResult};
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct JaegerConfig {
    /// e.g., "localhost:6831"
    pub agent_endpoint: String,
    /// Alternative to agent endpoint.
    pub collector_endpoint: Option<String>,
    pub service_name: String,
    pub max_batch_size: usize,
    pub batch_timeout: Duration,
    /// 0.0 to 1.0
    pub sample_rate: f64,
}

impl Default for JaegerConfig {
    fn default() -> Self {
        Self {
            agent_endpoint: "localhost:6831".to_string(),
            collector_endpoint: None,
            service_name: "orcher".to_string(),
            max_batch_size: 512,
            batch_timeout: Duration::from_secs(5),
            sample_rate: 1.0,
        }
    }
}

impl ExporterConfig for JaegerConfig {
    fn validate(&self) -> ExporterResult<()> {
        // Validate service name
        if self.service_name.is_empty() {
            return Err(MonitoringError::exporter_error(
                "jaeger",
                "Service name cannot be empty",
            ));
        }

        // Validate sample rate
        if self.sample_rate < 0.0 || self.sample_rate > 1.0 {
            return Err(MonitoringError::exporter_error(
                "jaeger",
                "Sample rate must be between 0.0 and 1.0",
            ));
        }

        // Validate batch size
        if self.max_batch_size == 0 {
            return Err(MonitoringError::exporter_error(
                "jaeger",
                "Batch size must be greater than 0",
            ));
        }

        Ok(())
    }

    fn description(&self) -> String {
        format!(
            "Jaeger exporter for '{}' via {}",
            self.service_name, self.agent_endpoint
        )
    }
}

pub struct JaegerExporter {
    config: JaegerConfig,
    running: bool,
}

impl JaegerExporter {
    pub fn new(config: JaegerConfig) -> ExporterResult<Self> {
        config.validate()?;
        Ok(Self {
            config,
            running: false,
        })
    }

    pub fn builder() -> JaegerExporterBuilder {
        JaegerExporterBuilder::new()
    }

    /// Install as the global tracer.
    pub async fn install_global(&mut self) -> ExporterResult<()> {
        if self.running {
            return Err(MonitoringError::exporter_error(
                "jaeger",
                "Exporter is already running as global tracer",
            ));
        }

        // In a real implementation, this would install the Jaeger tracer
        // as the global OpenTelemetry tracer provider
        self.running = true;
        Ok(())
    }
}

impl Exporter for JaegerExporter {
    fn start(&mut self) -> ExporterResult<()> {
        if self.running {
            return Err(MonitoringError::exporter_error(
                "jaeger",
                "Exporter is already running",
            ));
        }

        self.running = true;
        Ok(())
    }

    fn stop(&mut self) -> ExporterResult<()> {
        if !self.running {
            return Err(MonitoringError::exporter_error(
                "jaeger",
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
            "Jaeger exporter: {} ({})",
            status,
            self.config.description()
        ))
    }
}

pub struct JaegerExporterBuilder {
    config: JaegerConfig,
}

impl Default for JaegerExporterBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl JaegerExporterBuilder {
    pub fn new() -> Self {
        Self {
            config: JaegerConfig::default(),
        }
    }

    pub fn with_agent_endpoint(mut self, endpoint: &str) -> Self {
        self.config.agent_endpoint = endpoint.to_string();
        self
    }

    pub fn with_collector_endpoint(mut self, endpoint: &str) -> Self {
        self.config.collector_endpoint = Some(endpoint.to_string());
        self
    }

    pub fn with_service_name(mut self, name: &str) -> Self {
        self.config.service_name = name.to_string();
        self
    }

    pub fn with_sample_rate(mut self, rate: f64) -> Self {
        self.config.sample_rate = rate;
        self
    }

    pub fn build(self) -> ExporterResult<JaegerExporter> {
        JaegerExporter::new(self.config)
    }
}

/// Configure and install a Jaeger exporter as the global tracer.
pub async fn setup_jaeger_tracer(
    service_name: &str,
    endpoint: &str,
) -> ExporterResult<JaegerExporter> {
    let mut exporter = JaegerExporter::builder()
        .with_service_name(service_name)
        .with_agent_endpoint(endpoint)
        .build()?;

    exporter.install_global().await?;

    tracing::info!(
        "Jaeger tracer installed globally for service '{}' via {}",
        service_name,
        endpoint
    );

    Ok(exporter)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_jaeger_config_default() {
        let config = JaegerConfig::default();
        assert_eq!(config.agent_endpoint, "localhost:6831");
        assert_eq!(config.service_name, "orcher");
        assert_eq!(config.sample_rate, 1.0);
        assert!(config.collector_endpoint.is_none());
    }

    #[test]
    fn test_jaeger_config_validation() {
        let valid_config = JaegerConfig::default();
        assert!(valid_config.validate().is_ok());

        let invalid_config = JaegerConfig {
            service_name: "".to_string(),
            ..Default::default()
        };
        assert!(invalid_config.validate().is_err());

        let invalid_sample_rate = JaegerConfig {
            sample_rate: 1.5,
            ..Default::default()
        };
        assert!(invalid_sample_rate.validate().is_err());
    }

    #[test]
    fn test_jaeger_exporter_lifecycle() {
        let mut exporter = JaegerExporter::builder().build().unwrap();

        assert!(!exporter.is_running());
        assert!(exporter.start().is_ok());
        assert!(exporter.is_running());
        assert!(exporter.stop().is_ok());
        assert!(!exporter.is_running());
    }

    #[tokio::test]
    async fn test_jaeger_install_global() {
        let mut exporter = JaegerExporter::builder()
            .with_service_name("test-service")
            .build()
            .unwrap();

        assert!(!exporter.is_running());
        assert!(exporter.install_global().await.is_ok());
        assert!(exporter.is_running());
    }

    #[test]
    fn test_jaeger_exporter_builder() {
        let exporter = JaegerExporter::builder()
            .with_agent_endpoint("localhost:14268")
            .with_service_name("custom-service")
            .with_sample_rate(0.5)
            .build()
            .unwrap();

        assert_eq!(exporter.config.agent_endpoint, "localhost:14268");
        assert_eq!(exporter.config.service_name, "custom-service");
        assert_eq!(exporter.config.sample_rate, 0.5);
    }

    #[tokio::test]
    async fn test_setup_jaeger_tracer() {
        // Test the convenience setup function
        let result = setup_jaeger_tracer("test-service", "localhost:6831").await;
        assert!(result.is_ok());

        let exporter = result.unwrap();
        assert!(exporter.is_running());
        assert_eq!(exporter.config.service_name, "test-service");
        assert_eq!(exporter.config.agent_endpoint, "localhost:6831");
    }

    #[tokio::test]
    async fn test_setup_jaeger_tracer_validates() {
        // Test that the setup function validates configuration
        let result = setup_jaeger_tracer("", "localhost:6831").await;
        assert!(result.is_err());
    }
}
