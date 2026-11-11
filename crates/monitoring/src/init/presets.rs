//! Common monitoring presets for typical use cases
//!
//! This module provides ready-to-use monitoring configurations for
//! common scenarios like minimal setup, full observability, and more.

use crate::init::MonitoringBuilder;
use std::net::SocketAddr;

/// Minimal monitoring preset
///
/// Provides basic Prometheus metrics with minimal overhead.
/// Suitable for lightweight services or when you only need metrics.
///
/// Features:
/// - Prometheus metrics on localhost:9090
/// - No distributed tracing
/// - No health checks
/// - Lowest resource usage
///
/// # Example
///
/// ```no_run
/// use orcher_monitoring::init::presets::minimal_monitoring;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let mut monitoring = minimal_monitoring().await?;
/// monitoring.start().await?;
/// # Ok(())
/// # }
/// ```
pub async fn minimal_monitoring() -> crate::error::Result<crate::init::MonitoringSystem> {
    MonitoringBuilder::new()
        .with_prometheus("127.0.0.1:9090".parse().unwrap())
        .build()
        .await
}

/// Full observability preset
///
/// Provides comprehensive monitoring with metrics, traces, and health checks.
/// Suitable for production systems requiring complete observability.
///
/// Features:
/// - Prometheus metrics on 0.0.0.0:9090
/// - Jaeger distributed tracing
/// - Health check registry
/// - Complete visibility into system behavior
///
/// # Arguments
///
/// * `service_name` - Name of the service for identification
/// * `jaeger_endpoint` - Jaeger agent endpoint (e.g., "localhost:6831")
///
/// # Example
///
/// ```no_run
/// use orcher_monitoring::init::presets::full_observability;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let mut monitoring = full_observability("my-service", "localhost:6831").await?;
/// monitoring.start().await?;
/// # Ok(())
/// # }
/// ```
pub async fn full_observability(
    service_name: &str,
    jaeger_endpoint: &str,
) -> crate::error::Result<crate::init::MonitoringSystem> {
    MonitoringBuilder::new()
        .with_service_name(service_name)
        .with_prometheus("0.0.0.0:9090".parse().unwrap())
        .with_jaeger(service_name, jaeger_endpoint)
        .with_health_checks(true)
        .build()
        .await
}

/// Cloud-native preset with OTLP
///
/// Optimized for cloud deployments with OpenTelemetry Protocol.
/// Works seamlessly with cloud observability platforms.
///
/// Features:
/// - Prometheus metrics
/// - OTLP for metrics and traces
/// - Health checks for orchestrators
/// - Compatible with Kubernetes, AWS, GCP, Azure
///
/// # Arguments
///
/// * `service_name` - Name of the service for identification
/// * `otlp_endpoint` - OTLP collector endpoint (e.g., "http://collector:4317")
///
/// # Example
///
/// ```no_run
/// use orcher_monitoring::init::presets::cloud_native_monitoring;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let mut monitoring = cloud_native_monitoring(
///     "orcher-api",
///     "http://otel-collector:4317"
/// ).await?;
/// monitoring.start().await?;
/// # Ok(())
/// # }
/// ```
pub async fn cloud_native_monitoring(
    service_name: &str,
    otlp_endpoint: &str,
) -> crate::error::Result<crate::init::MonitoringSystem> {
    MonitoringBuilder::new()
        .with_service_name(service_name)
        .with_prometheus("0.0.0.0:9090".parse().unwrap())
        .with_otlp(otlp_endpoint, true, true)
        .with_health_checks(true)
        .build()
        .await
}

/// Metrics-only preset
///
/// Provides Prometheus metrics without tracing overhead.
/// Suitable when you only need metrics monitoring.
///
/// Features:
/// - Prometheus metrics on configurable port
/// - Health checks
/// - No distributed tracing
/// - Lower resource usage than full observability
///
/// # Arguments
///
/// * `prometheus_port` - Port for Prometheus metrics endpoint
///
/// # Example
///
/// ```no_run
/// use orcher_monitoring::init::presets::metrics_only;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let mut monitoring = metrics_only(9091).await?;
/// monitoring.start().await?;
/// # Ok(())
/// # }
/// ```
pub async fn metrics_only(
    prometheus_port: u16,
) -> crate::error::Result<crate::init::MonitoringSystem> {
    let addr: SocketAddr = format!("0.0.0.0:{}", prometheus_port).parse().unwrap();
    MonitoringBuilder::new()
        .with_prometheus(addr)
        .with_health_checks(true)
        .build()
        .await
}

/// Tracing-focused preset
///
/// Optimized for distributed tracing with minimal metrics.
/// Suitable when detailed request tracing is the priority.
///
/// Features:
/// - Jaeger distributed tracing
/// - Basic Prometheus metrics
/// - Health checks
/// - Focus on request flow visibility
///
/// # Arguments
///
/// * `service_name` - Name of the service for tracing
/// * `jaeger_endpoint` - Jaeger agent endpoint
///
/// # Example
///
/// ```no_run
/// use orcher_monitoring::init::presets::tracing_focused;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let mut monitoring = tracing_focused("my-service", "localhost:6831").await?;
/// monitoring.start().await?;
/// # Ok(())
/// # }
/// ```
pub async fn tracing_focused(
    service_name: &str,
    jaeger_endpoint: &str,
) -> crate::error::Result<crate::init::MonitoringSystem> {
    MonitoringBuilder::new()
        .with_service_name(service_name)
        .with_prometheus("127.0.0.1:9090".parse().unwrap())
        .with_jaeger(service_name, jaeger_endpoint)
        .with_health_checks(true)
        .build()
        .await
}

/// Development preset with debug features
///
/// Configured for local development with verbose output.
/// Not suitable for production use.
///
/// Features:
/// - Prometheus on localhost
/// - Jaeger for local debugging
/// - Health checks
/// - Optimized for developer experience
///
/// # Example
///
/// ```no_run
/// use orcher_monitoring::init::presets::development_mode;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let mut monitoring = development_mode().await?;
/// monitoring.start().await?;
/// # Ok(())
/// # }
/// ```
pub async fn development_mode() -> crate::error::Result<crate::init::MonitoringSystem> {
    MonitoringBuilder::new()
        .with_service_name("orcher-dev")
        .with_prometheus("127.0.0.1:9090".parse().unwrap())
        .with_jaeger("orcher-dev", "localhost:6831")
        .with_health_checks(true)
        .build()
        .await
}

/// Multi-backend preset
///
/// Sends telemetry to multiple backends simultaneously.
/// Useful for migration scenarios or redundancy.
///
/// Features:
/// - Prometheus metrics
/// - Jaeger tracing
/// - OTLP export
/// - Maximum compatibility
///
/// # Arguments
///
/// * `service_name` - Service name
/// * `jaeger_endpoint` - Jaeger endpoint
/// * `otlp_endpoint` - OTLP endpoint
///
/// # Example
///
/// ```no_run
/// use orcher_monitoring::init::presets::multi_backend;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let mut monitoring = multi_backend(
///     "my-service",
///     "localhost:6831",
///     "http://otel:4317"
/// ).await?;
/// monitoring.start().await?;
/// # Ok(())
/// # }
/// ```
pub async fn multi_backend(
    service_name: &str,
    jaeger_endpoint: &str,
    otlp_endpoint: &str,
) -> crate::error::Result<crate::init::MonitoringSystem> {
    MonitoringBuilder::new()
        .with_service_name(service_name)
        .with_prometheus("0.0.0.0:9090".parse().unwrap())
        .with_jaeger(service_name, jaeger_endpoint)
        .with_otlp(otlp_endpoint, true, true)
        .with_health_checks(true)
        .build()
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore = "requires network binding"]
    async fn test_minimal_monitoring() {
        let result = minimal_monitoring().await;
        assert!(result.is_ok());
        let system = result.unwrap();
        assert_eq!(system.service_name(), "orcher");
    }

    #[tokio::test]
    #[ignore = "requires network binding"]
    async fn test_full_observability() {
        let result = full_observability("test-service", "localhost:6831").await;
        assert!(result.is_ok());
        let system = result.unwrap();
        assert_eq!(system.service_name(), "test-service");
        assert!(system.health_registry().is_some());
    }

    #[tokio::test]
    #[ignore = "requires network binding"]
    async fn test_cloud_native_monitoring() {
        let result = cloud_native_monitoring("cloud-service", "http://localhost:4317").await;
        assert!(result.is_ok());
        let system = result.unwrap();
        assert_eq!(system.service_name(), "cloud-service");
        assert!(system.health_registry().is_some());
    }

    #[tokio::test]
    #[ignore = "requires network binding"]
    async fn test_metrics_only() {
        let result = metrics_only(9091).await;
        assert!(result.is_ok());
        let system = result.unwrap();
        assert!(system.health_registry().is_some());
    }

    #[tokio::test]
    #[ignore = "requires network binding"]
    async fn test_tracing_focused() {
        let result = tracing_focused("trace-service", "localhost:6831").await;
        assert!(result.is_ok());
        let system = result.unwrap();
        assert_eq!(system.service_name(), "trace-service");
    }

    #[tokio::test]
    #[ignore = "requires network binding"]
    async fn test_development_mode() {
        let result = development_mode().await;
        assert!(result.is_ok());
        let system = result.unwrap();
        assert_eq!(system.service_name(), "orcher-dev");
    }

    #[tokio::test]
    #[ignore = "requires network binding"]
    async fn test_multi_backend() {
        let result =
            multi_backend("multi-service", "localhost:6831", "http://localhost:4317").await;
        assert!(result.is_ok());
        let system = result.unwrap();
        assert_eq!(system.service_name(), "multi-service");
        assert!(system.health_registry().is_some());
    }
}
