//! Default monitoring configurations for different deployment environments
//!
//! This module provides pre-configured monitoring setups optimized for
//! different deployment scenarios (development, staging, production).

use crate::init::MonitoringBuilder;
use std::net::SocketAddr;

/// Default configuration for development environment
///
/// This preset provides basic monitoring suitable for local development:
/// - Prometheus on localhost:9090
/// - Health checks enabled
/// - Minimal overhead
///
/// # Example
///
/// ```no_run
/// use orcher_monitoring::init::defaults::development;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let monitoring = development().build().await?;
/// # Ok(())
/// # }
/// ```
pub fn development() -> MonitoringBuilder {
    MonitoringBuilder::new()
        .with_service_name("orcher-dev")
        .with_prometheus("127.0.0.1:9090".parse().unwrap())
        .with_health_checks(true)
}

/// Default configuration for staging environment
///
/// This preset provides comprehensive monitoring suitable for staging:
/// - Prometheus on 0.0.0.0:9090 for external access
/// - Jaeger tracing for debugging
/// - Health checks enabled
/// - Full observability
///
/// # Example
///
/// ```no_run
/// use orcher_monitoring::init::defaults::staging;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let monitoring = staging().build().await?;
/// # Ok(())
/// # }
/// ```
pub fn staging() -> MonitoringBuilder {
    MonitoringBuilder::new()
        .with_service_name("orcher-staging")
        .with_prometheus("0.0.0.0:9090".parse().unwrap())
        .with_jaeger("orcher-staging", "localhost:6831")
        .with_health_checks(true)
}

/// Default configuration for production environment
///
/// This preset provides production-grade monitoring:
/// - Prometheus on 0.0.0.0:9090
/// - OTLP for both metrics and traces
/// - Health checks enabled
/// - Optimized for reliability
///
/// # Example
///
/// ```no_run
/// use orcher_monitoring::init::defaults::production;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let monitoring = production("http://otel-collector:4317").build().await?;
/// # Ok(())
/// # }
/// ```
pub fn production(otlp_endpoint: &str) -> MonitoringBuilder {
    MonitoringBuilder::new()
        .with_service_name("orcher")
        .with_prometheus("0.0.0.0:9090".parse().unwrap())
        .with_otlp(otlp_endpoint, true, true)
        .with_health_checks(true)
}

/// Minimal monitoring configuration
///
/// This preset provides the absolute minimum monitoring:
/// - Prometheus metrics only
/// - No distributed tracing
/// - No health checks
/// - Lowest overhead
///
/// # Example
///
/// ```no_run
/// use orcher_monitoring::init::defaults::minimal;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let monitoring = minimal().build().await?;
/// # Ok(())
/// # }
/// ```
pub fn minimal() -> MonitoringBuilder {
    MonitoringBuilder::new().with_prometheus("127.0.0.1:9090".parse().unwrap())
}

/// Cloud-native monitoring configuration
///
/// This preset is optimized for cloud deployments:
/// - Prometheus for metrics
/// - OTLP for traces (cloud-native standard)
/// - Health checks for orchestration
/// - Compatible with Kubernetes, ECS, etc.
///
/// # Arguments
///
/// * `otlp_endpoint` - OpenTelemetry collector endpoint
/// * `service_name` - Service name for identification
///
/// # Example
///
/// ```no_run
/// use orcher_monitoring::init::defaults::cloud_native;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let monitoring = cloud_native(
///     "http://otel-collector:4317",
///     "orcher-worker"
/// ).build().await?;
/// # Ok(())
/// # }
/// ```
pub fn cloud_native(otlp_endpoint: &str, service_name: &str) -> MonitoringBuilder {
    MonitoringBuilder::new()
        .with_service_name(service_name)
        .with_prometheus("0.0.0.0:9090".parse().unwrap())
        .with_otlp(otlp_endpoint, true, true)
        .with_health_checks(true)
}

/// Testing configuration
///
/// This preset is designed for automated testing:
/// - Prometheus in test mode (no actual HTTP server)
/// - No external dependencies
/// - Health checks enabled for testing
///
/// # Example
///
/// ```no_run
/// use orcher_monitoring::init::defaults::testing;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let monitoring = testing().build().await?;
/// # Ok(())
/// # }
/// ```
pub fn testing() -> MonitoringBuilder {
    MonitoringBuilder::new()
        .with_service_name("orcher-test")
        .with_health_checks(true)
}

/// Custom configuration builder
///
/// This function provides a starting point for custom configurations
/// based on a specific environment and service name.
///
/// # Arguments
///
/// * `service_name` - Service name for identification
/// * `prometheus_port` - Port for Prometheus metrics endpoint
///
/// # Example
///
/// ```no_run
/// use orcher_monitoring::init::defaults::custom;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let monitoring = custom("my-service", 9091)
///     .with_jaeger("my-service", "jaeger:6831")
///     .build()
///     .await?;
/// # Ok(())
/// # }
/// ```
pub fn custom(service_name: &str, prometheus_port: u16) -> MonitoringBuilder {
    let addr: SocketAddr = format!("0.0.0.0:{}", prometheus_port).parse().unwrap();
    MonitoringBuilder::new()
        .with_service_name(service_name)
        .with_prometheus(addr)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_development_config_builder() {
        let builder = development();
        // Just test that the builder is created without errors
        assert_eq!(builder.service_name, "orcher-dev");
    }

    #[test]
    fn test_staging_config_builder() {
        let builder = staging();
        assert_eq!(builder.service_name, "orcher-staging");
    }

    #[test]
    fn test_production_config_builder() {
        let builder = production("http://localhost:4317");
        assert_eq!(builder.service_name, "orcher");
    }

    #[test]
    fn test_minimal_config_builder() {
        let builder = minimal();
        assert_eq!(builder.service_name, "orcher");
    }

    #[test]
    fn test_cloud_native_config_builder() {
        let builder = cloud_native("http://otel:4317", "my-service");
        assert_eq!(builder.service_name, "my-service");
    }

    #[test]
    fn test_testing_config_builder() {
        let builder = testing();
        assert_eq!(builder.service_name, "orcher-test");
        assert!(builder.enable_health_checks);
    }

    #[test]
    fn test_custom_config_builder() {
        let builder = custom("custom-service", 9091);
        assert_eq!(builder.service_name, "custom-service");
    }
}
