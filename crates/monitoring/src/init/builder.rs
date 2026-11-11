//! Monitoring system builder for easy initialization
//!
//! This module provides a fluent builder API for configuring and initializing
//! the ORCHER monitoring system with all its components.

use crate::error::Result;
use crate::exporters::{
    setup_jaeger_tracer, setup_otlp_exporter, setup_prometheus_exporter, Exporter, ExporterGroup,
};
use crate::health::HealthCheckRegistry;
use std::net::SocketAddr;

/// Monitoring system builder for fluent configuration
///
/// This builder provides a convenient way to configure and initialize
/// the ORCHER monitoring system with exporters, health checks, and
/// other observability features.
///
/// # Example
///
/// ```no_run
/// use orcher_monitoring::init::MonitoringBuilder;
/// use std::net::SocketAddr;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let mut monitoring = MonitoringBuilder::new()
///     .with_prometheus("0.0.0.0:9090".parse()?)
///     .with_jaeger("my-service", "localhost:6831")
///     .with_health_checks(true)
///     .build()
///     .await?;
///
/// monitoring.start().await?;
/// # Ok(())
/// # }
/// ```
pub struct MonitoringBuilder {
    pub(crate) prometheus_addr: Option<SocketAddr>,
    pub(crate) jaeger_config: Option<(String, String)>, // (service_name, endpoint)
    pub(crate) otlp_config: Option<(String, bool, bool)>, // (endpoint, metrics, traces)
    pub(crate) enable_health_checks: bool,
    pub(crate) health_registry: Option<HealthCheckRegistry>,
    pub(crate) service_name: String,
}

impl MonitoringBuilder {
    /// Create a new monitoring builder with default configuration
    pub fn new() -> Self {
        Self {
            prometheus_addr: None,
            jaeger_config: None,
            otlp_config: None,
            enable_health_checks: false,
            health_registry: None,
            service_name: "orcher".to_string(),
        }
    }

    /// Set the service name for monitoring
    ///
    /// This name will be used for trace service identification
    /// and metric labeling.
    pub fn with_service_name(mut self, name: impl Into<String>) -> Self {
        self.service_name = name.into();
        self
    }

    /// Enable Prometheus metrics exporter
    ///
    /// # Arguments
    ///
    /// * `addr` - Socket address for the Prometheus HTTP endpoint
    pub fn with_prometheus(mut self, addr: SocketAddr) -> Self {
        self.prometheus_addr = Some(addr);
        self
    }

    /// Enable Jaeger distributed tracing exporter
    ///
    /// # Arguments
    ///
    /// * `service_name` - Name of the service for tracing
    /// * `endpoint` - Jaeger agent endpoint (e.g., "localhost:6831")
    pub fn with_jaeger(
        mut self,
        service_name: impl Into<String>,
        endpoint: impl Into<String>,
    ) -> Self {
        self.jaeger_config = Some((service_name.into(), endpoint.into()));
        self
    }

    /// Enable OTLP (OpenTelemetry Protocol) exporter
    ///
    /// # Arguments
    ///
    /// * `endpoint` - OTLP endpoint URL
    /// * `export_metrics` - Whether to export metrics
    /// * `export_traces` - Whether to export traces
    pub fn with_otlp(
        mut self,
        endpoint: impl Into<String>,
        export_metrics: bool,
        export_traces: bool,
    ) -> Self {
        self.otlp_config = Some((endpoint.into(), export_metrics, export_traces));
        self
    }

    /// Enable or disable health checks
    ///
    /// When enabled, a health check registry will be created
    /// for monitoring system component health.
    pub fn with_health_checks(mut self, enabled: bool) -> Self {
        self.enable_health_checks = enabled;
        self
    }

    /// Provide a custom health registry
    ///
    /// If not provided and health checks are enabled, a new
    /// registry will be created automatically.
    pub fn with_health_registry(mut self, registry: HealthCheckRegistry) -> Self {
        self.health_registry = Some(registry);
        self.enable_health_checks = true;
        self
    }

    /// Build and initialize the monitoring system
    ///
    /// This method creates all configured exporters and initializes
    /// the monitoring system. It returns a `MonitoringSystem` handle
    /// that can be used to control the monitoring lifecycle.
    pub async fn build(self) -> Result<MonitoringSystem> {
        let mut exporters = ExporterGroup::new();

        // Setup Prometheus if configured
        if let Some(addr) = self.prometheus_addr {
            let prometheus = setup_prometheus_exporter(addr)?;
            exporters.add_exporter("prometheus", prometheus)?;
        }

        // Setup Jaeger if configured
        if let Some((service_name, endpoint)) = self.jaeger_config {
            let jaeger = setup_jaeger_tracer(&service_name, &endpoint).await?;
            exporters.add_exporter("jaeger", jaeger)?;
        }

        // Setup OTLP if configured
        if let Some((endpoint, metrics, traces)) = self.otlp_config {
            let otlp = setup_otlp_exporter(&endpoint, metrics, traces).await?;
            exporters.add_exporter("otlp", otlp)?;
        }

        // Setup health checks if enabled
        let health_registry = if self.enable_health_checks {
            self.health_registry
                .or_else(|| Some(HealthCheckRegistry::new()))
        } else {
            None
        };

        Ok(MonitoringSystem {
            exporters,
            health_registry,
            service_name: self.service_name,
        })
    }
}

impl Default for MonitoringBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Monitoring system handle
///
/// This struct represents an initialized monitoring system with
/// all configured exporters and health checks.
pub struct MonitoringSystem {
    exporters: ExporterGroup,
    health_registry: Option<HealthCheckRegistry>,
    service_name: String,
}

impl MonitoringSystem {
    /// Start all configured exporters
    pub async fn start(&mut self) -> Result<()> {
        if !self.exporters.is_empty() {
            self.exporters.start()?;
            tracing::info!(
                service = %self.service_name,
                exporters = self.exporters.len(),
                "Monitoring system started"
            );
        }
        Ok(())
    }

    /// Stop all configured exporters
    pub async fn stop(&mut self) -> Result<()> {
        if !self.exporters.is_empty() {
            self.exporters.stop()?;
            tracing::info!(
                service = %self.service_name,
                "Monitoring system stopped"
            );
        }
        Ok(())
    }

    /// Get a reference to the health registry
    pub fn health_registry(&self) -> Option<&HealthCheckRegistry> {
        self.health_registry.as_ref()
    }

    /// Get a mutable reference to the health registry
    pub fn health_registry_mut(&mut self) -> Option<&mut HealthCheckRegistry> {
        self.health_registry.as_mut()
    }

    /// Get the service name
    pub fn service_name(&self) -> &str {
        &self.service_name
    }

    /// Check if the monitoring system is running
    pub fn is_running(&self) -> bool {
        self.exporters.is_running()
    }

    /// Get the health status of all exporters
    pub fn health_status(&self) -> Result<String> {
        self.exporters.health_status()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_creation() {
        let builder = MonitoringBuilder::new();
        assert_eq!(builder.service_name, "orcher");
        assert!(!builder.enable_health_checks);
        assert!(builder.prometheus_addr.is_none());
        assert!(builder.jaeger_config.is_none());
    }

    #[test]
    fn test_builder_with_service_name() {
        let builder = MonitoringBuilder::new().with_service_name("my-service");
        assert_eq!(builder.service_name, "my-service");
    }

    #[test]
    fn test_builder_with_prometheus() {
        let addr: SocketAddr = "127.0.0.1:9090".parse().unwrap();
        let builder = MonitoringBuilder::new().with_prometheus(addr);
        assert_eq!(builder.prometheus_addr, Some(addr));
    }

    #[test]
    fn test_builder_with_jaeger() {
        let builder = MonitoringBuilder::new().with_jaeger("test-service", "localhost:6831");
        assert!(builder.jaeger_config.is_some());
        let (service, endpoint) = builder.jaeger_config.unwrap();
        assert_eq!(service, "test-service");
        assert_eq!(endpoint, "localhost:6831");
    }

    #[test]
    fn test_builder_with_otlp() {
        let builder = MonitoringBuilder::new().with_otlp("http://localhost:4317", true, true);
        assert!(builder.otlp_config.is_some());
        let (endpoint, metrics, traces) = builder.otlp_config.unwrap();
        assert_eq!(endpoint, "http://localhost:4317");
        assert!(metrics);
        assert!(traces);
    }

    #[test]
    fn test_builder_with_health_checks() {
        let builder = MonitoringBuilder::new().with_health_checks(true);
        assert!(builder.enable_health_checks);
    }

    #[tokio::test]
    async fn test_builder_build_empty() {
        let system = MonitoringBuilder::new().build().await.unwrap();
        assert!(!system.is_running());
        assert_eq!(system.service_name(), "orcher");
    }

    #[tokio::test]
    #[ignore = "requires network binding"]
    async fn test_builder_build_with_prometheus() {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let system = MonitoringBuilder::new()
            .with_prometheus(addr)
            .build()
            .await
            .unwrap();

        // System should not be running initially
        assert!(!system.is_running());

        // Start should succeed but may fail if port is in use
        // In real tests, we'd use test mode
    }

    #[tokio::test]
    async fn test_monitoring_system_lifecycle() {
        let mut system = MonitoringBuilder::new()
            .with_service_name("test-service")
            .with_health_checks(true)
            .build()
            .await
            .unwrap();

        assert_eq!(system.service_name(), "test-service");
        assert!(system.health_registry().is_some());

        // Start and stop should succeed with no exporters
        assert!(system.start().await.is_ok());
        assert!(system.stop().await.is_ok());
    }

    #[tokio::test]
    #[ignore = "requires network binding"]
    async fn test_builder_fluent_api() {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let system = MonitoringBuilder::new()
            .with_service_name("fluent-test")
            .with_prometheus(addr)
            .with_health_checks(true)
            .build()
            .await
            .unwrap();

        assert_eq!(system.service_name(), "fluent-test");
        assert!(system.health_registry().is_some());
    }
}
