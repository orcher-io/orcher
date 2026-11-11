//! Integration tests for monitoring exporters
//!
//! This module contains comprehensive integration tests for all exporter types,
//! testing their configuration, lifecycle management, and error handling.

use orcher_monitoring::exporters::otlp::OtlpProtocol;
use orcher_monitoring::exporters::{
    Exporter, ExporterGroup, JaegerExporter, MultiExporter, OtlpExporter, PrometheusExporter,
};
use std::time::Duration;

#[test]
fn test_prometheus_exporter_configuration() {
    // Test default configuration
    let exporter = PrometheusExporter::builder().build().unwrap();
    assert!(!exporter.is_running());

    // Test custom configuration
    let custom_exporter = PrometheusExporter::builder()
        .with_address("127.0.0.1:9091")
        .with_path("/custom/metrics")
        .with_compression(false)
        .build()
        .unwrap();

    assert!(!custom_exporter.is_running());
    assert!(custom_exporter.health_status().is_ok());
}

#[test]
fn test_prometheus_exporter_invalid_config() {
    // Test invalid address
    let result = PrometheusExporter::builder()
        .with_address("invalid-address")
        .build();
    assert!(result.is_err());
}

#[test]
fn test_jaeger_exporter_configuration() {
    // Test default configuration
    let exporter = JaegerExporter::builder().build().unwrap();
    assert!(!exporter.is_running());

    // Test custom configuration
    let custom_exporter = JaegerExporter::builder()
        .with_agent_endpoint("localhost:14268")
        .with_service_name("test-service")
        .with_sample_rate(0.5)
        .build()
        .unwrap();

    assert!(!custom_exporter.is_running());
    assert!(custom_exporter.health_status().is_ok());
}

#[test]
fn test_jaeger_exporter_invalid_config() {
    // Test empty service name
    let result = JaegerExporter::builder().with_service_name("").build();
    assert!(result.is_err());

    // Test invalid sample rate
    let result = JaegerExporter::builder().with_sample_rate(1.5).build();
    assert!(result.is_err());
}

#[tokio::test]
async fn test_jaeger_install_global() {
    let mut exporter = JaegerExporter::builder()
        .with_service_name("integration-test")
        .build()
        .unwrap();

    assert!(!exporter.is_running());
    assert!(exporter.install_global().await.is_ok());
    assert!(exporter.is_running());

    // Test double install fails
    assert!(exporter.install_global().await.is_err());
}

#[test]
fn test_otlp_exporter_configuration() {
    // Test default configuration
    let exporter = OtlpExporter::builder().build().unwrap();
    assert!(!exporter.is_running());

    // Test custom configuration
    let custom_exporter = OtlpExporter::builder()
        .with_endpoint("https://api.honeycomb.io")
        .with_protocol(OtlpProtocol::HttpProtobuf)
        .with_metrics(true)
        .with_traces(false)
        .with_header("x-honeycomb-team", "test-key")
        .with_timeout(Duration::from_secs(30))
        .build()
        .unwrap();

    assert!(!custom_exporter.is_running());
    assert!(custom_exporter.health_status().is_ok());
}

#[test]
fn test_otlp_exporter_invalid_config() {
    // Test empty endpoint
    let result = OtlpExporter::builder().with_endpoint("").build();
    assert!(result.is_err());

    // Test invalid endpoint
    let result = OtlpExporter::builder()
        .with_endpoint("invalid-endpoint")
        .build();
    assert!(result.is_err());

    // Test no exports enabled
    let result = OtlpExporter::builder()
        .with_metrics(false)
        .with_traces(false)
        .build();
    assert!(result.is_err());
}

#[tokio::test]
async fn test_otlp_install_global() {
    let mut exporter = OtlpExporter::builder()
        .with_endpoint("http://localhost:4317")
        .build()
        .unwrap();

    assert!(!exporter.is_running());
    assert!(exporter.install_global().await.is_ok());
    assert!(exporter.is_running());
}

#[test]
fn test_multi_exporter_basic() {
    let prometheus = PrometheusExporter::builder()
        .with_address("127.0.0.1:0")
        .with_test_mode(true)
        .build()
        .unwrap();

    let jaeger = JaegerExporter::builder()
        .with_service_name("multi-test")
        .build()
        .unwrap();

    let multi = MultiExporter::new()
        .with_exporter("prometheus", prometheus)
        .unwrap()
        .with_exporter("jaeger", jaeger)
        .unwrap()
        .build();

    assert_eq!(multi.len(), 2);
    assert!(multi.has_exporter("prometheus"));
    assert!(multi.has_exporter("jaeger"));
}

#[test]
fn test_multi_exporter_lifecycle() {
    let prometheus = PrometheusExporter::builder()
        .with_address("127.0.0.1:0")
        .with_test_mode(true)
        .build()
        .unwrap();
    let jaeger = JaegerExporter::builder().build().unwrap();
    let otlp = OtlpExporter::builder().build().unwrap();

    let mut multi = MultiExporter::new()
        .with_exporter("prometheus", prometheus)
        .unwrap()
        .with_exporter("jaeger", jaeger)
        .unwrap()
        .with_exporter("otlp", otlp)
        .unwrap()
        .build();

    // Initially not running
    assert!(!multi.is_running());

    // Start all exporters
    match multi.start() {
        Ok(_) => {}
        Err(e) => panic!("Failed to start multi-exporter: {}", e),
    }
    assert!(multi.is_running());

    // Health status should include all exporters
    let status = multi.health_status().unwrap();
    assert!(status.contains("prometheus"));
    assert!(status.contains("jaeger"));
    assert!(status.contains("otlp"));

    // Stop all exporters
    assert!(multi.stop().is_ok());
    assert!(!multi.is_running());
}

#[test]
fn test_exporter_group_management() {
    let mut group = ExporterGroup::new();
    assert!(group.is_empty());

    // Add exporters
    let prometheus = PrometheusExporter::builder().build().unwrap();
    group.add_exporter("prometheus", prometheus).unwrap();

    let jaeger = JaegerExporter::builder().build().unwrap();
    group.add_exporter("jaeger", jaeger).unwrap();

    assert_eq!(group.len(), 2);
    assert!(!group.is_empty());

    // Test duplicate name fails
    let duplicate = PrometheusExporter::builder().build().unwrap();
    assert!(group.add_exporter("prometheus", duplicate).is_err());

    // Test removal
    assert!(group.remove_exporter("prometheus").is_ok());
    assert_eq!(group.len(), 1);
    assert!(!group.has_exporter("prometheus"));
    assert!(group.has_exporter("jaeger"));

    // Test removing non-existent
    assert!(group.remove_exporter("non-existent").is_err());
}

#[test]
fn test_exporter_names_and_iteration() {
    let prometheus = PrometheusExporter::builder().build().unwrap();
    let jaeger = JaegerExporter::builder().build().unwrap();

    let multi = MultiExporter::new()
        .with_exporter("prometheus-1", prometheus)
        .unwrap()
        .with_exporter("jaeger-1", jaeger)
        .unwrap()
        .build();

    let names = multi.exporter_names();
    assert_eq!(names.len(), 2);
    assert!(names.contains(&"prometheus-1".to_string()));
    assert!(names.contains(&"jaeger-1".to_string()));
}

#[test]
fn test_all_exporter_types_compatibility() {
    // Test that all exporter types work together
    let prometheus = PrometheusExporter::builder()
        .with_address("127.0.0.1:0")
        .with_test_mode(true)
        .build()
        .unwrap();

    let jaeger = JaegerExporter::builder()
        .with_service_name("compatibility-test")
        .with_agent_endpoint("localhost:6831")
        .build()
        .unwrap();

    let otlp = OtlpExporter::builder()
        .with_endpoint("http://localhost:4317")
        .with_protocol(OtlpProtocol::Grpc)
        .build()
        .unwrap();

    let mut multi = MultiExporter::new()
        .with_exporter("prometheus", prometheus)
        .unwrap()
        .with_exporter("jaeger", jaeger)
        .unwrap()
        .with_exporter("otlp", otlp)
        .unwrap()
        .build();

    // All should start successfully
    match multi.start() {
        Ok(_) => {}
        Err(e) => panic!("Failed to start multi-exporter: {}", e),
    }
    assert!(multi.is_running());

    // Health status should be comprehensive
    let status = multi.health_status().unwrap();
    assert!(status.contains("Multi-exporter"));
    assert!(status.contains("prometheus"));
    assert!(status.contains("jaeger"));
    assert!(status.contains("otlp"));

    // All should stop successfully
    assert!(multi.stop().is_ok());
    assert!(!multi.is_running());
}

#[test]
fn test_exporter_error_scenarios() {
    let mut prometheus = PrometheusExporter::builder().build().unwrap();

    // Starting already started exporter should fail
    assert!(prometheus.start().is_ok());
    assert!(prometheus.start().is_err());

    // Stopping not started exporter should fail after starting and stopping
    assert!(prometheus.stop().is_ok());
    assert!(prometheus.stop().is_err());
}

#[test]
fn test_configuration_descriptions() {
    let prometheus = PrometheusExporter::builder()
        .with_address("127.0.0.1:9090")
        .build()
        .unwrap();

    let status = prometheus.health_status().unwrap();
    assert!(status.contains("Prometheus"));
    assert!(status.contains("127.0.0.1:9090"));

    let jaeger = JaegerExporter::builder()
        .with_service_name("test-service")
        .build()
        .unwrap();

    let status = jaeger.health_status().unwrap();
    assert!(status.contains("Jaeger"));
    assert!(status.contains("test-service"));

    let otlp = OtlpExporter::builder()
        .with_endpoint("https://example.com")
        .build()
        .unwrap();

    let status = otlp.health_status().unwrap();
    assert!(status.contains("OTLP"));
    assert!(status.contains("https://example.com"));
}

#[test]
fn test_prelude_imports() {
    // Test that all exporters are available through the prelude
    use orcher_monitoring::prelude::*;

    // Should be able to create exporters using prelude imports
    let prometheus = PrometheusExporter::builder()
        .with_address("127.0.0.1:0")
        .with_test_mode(true)
        .build()
        .unwrap();

    let jaeger = JaegerExporter::builder()
        .with_service_name("prelude-test")
        .build()
        .unwrap();

    let otlp = OtlpExporter::builder()
        .with_endpoint("http://localhost:4317")
        .build()
        .unwrap();

    // Should be able to use trait methods
    assert!(!prometheus.is_running());
    assert!(!jaeger.is_running());
    assert!(!otlp.is_running());

    // Should be able to create multi-exporter
    let multi = MultiExporter::new()
        .with_exporter("prometheus", prometheus)
        .unwrap()
        .with_exporter("jaeger", jaeger)
        .unwrap()
        .build();

    assert_eq!(multi.len(), 2);
}
