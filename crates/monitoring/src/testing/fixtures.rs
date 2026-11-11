//! Test fixtures and mock helpers for monitoring tests
//!
//! This module provides reusable test fixtures, sample configurations,
//! and mock implementations for testing monitoring functionality.

use orcher_common::monitoring::{HealthCheck, HealthStatus};
use std::time::Duration;

/// Test utilities for metrics
pub mod metrics {
    use std::collections::HashMap;

    /// A mock metrics collector that captures emitted metrics for testing
    #[derive(Debug, Default, Clone)]
    pub struct MockMetricsCollector {
        /// Captured counter values
        pub counters: HashMap<String, u64>,
        /// Captured gauge values
        pub gauges: HashMap<String, f64>,
        /// Captured histogram values
        pub histograms: HashMap<String, Vec<f64>>,
    }

    impl MockMetricsCollector {
        /// Create a new mock metrics collector
        pub fn new() -> Self {
            Self::default()
        }

        /// Record a counter increment
        pub fn record_counter(&mut self, name: &str, value: u64) {
            *self.counters.entry(name.to_string()).or_insert(0) += value;
        }

        /// Record a gauge value
        pub fn record_gauge(&mut self, name: &str, value: f64) {
            self.gauges.insert(name.to_string(), value);
        }

        /// Record a histogram value
        pub fn record_histogram(&mut self, name: &str, value: f64) {
            self.histograms
                .entry(name.to_string())
                .or_default()
                .push(value);
        }

        /// Get the current value of a counter
        pub fn get_counter(&self, name: &str) -> u64 {
            self.counters.get(name).copied().unwrap_or(0)
        }

        /// Get the current value of a gauge
        pub fn get_gauge(&self, name: &str) -> Option<f64> {
            self.gauges.get(name).copied()
        }

        /// Get all recorded histogram values
        pub fn get_histogram(&self, name: &str) -> Option<&[f64]> {
            self.histograms.get(name).map(|v| v.as_slice())
        }

        /// Clear all recorded metrics
        pub fn clear(&mut self) {
            self.counters.clear();
            self.gauges.clear();
            self.histograms.clear();
        }

        /// Get the number of counters recorded
        pub fn counter_count(&self) -> usize {
            self.counters.len()
        }

        /// Get the number of gauges recorded
        pub fn gauge_count(&self) -> usize {
            self.gauges.len()
        }

        /// Get the number of histograms recorded
        pub fn histogram_count(&self) -> usize {
            self.histograms.len()
        }
    }
}

/// Test utilities for health checks
pub mod health {
    use super::*;

    /// A mock health check that wraps orcher-common's HealthCheck
    #[derive(Debug, Clone)]
    pub struct MockHealthCheck {
        health_check: HealthCheck,
        is_critical: bool,
    }

    impl MockHealthCheck {
        /// Create a new mock health check that always returns healthy
        pub fn healthy(component: impl Into<String>) -> Self {
            Self {
                health_check: HealthCheck::healthy(component),
                is_critical: true,
            }
        }

        /// Create a new mock health check that always returns unhealthy
        pub fn unhealthy(component: impl Into<String>, message: impl Into<String>) -> Self {
            Self {
                health_check: HealthCheck::unhealthy(component, message),
                is_critical: true,
            }
        }

        /// Create a degraded health check
        pub fn degraded(component: impl Into<String>, message: impl Into<String>) -> Self {
            Self {
                health_check: HealthCheck::new(component)
                    .with_status(HealthStatus::Degraded)
                    .with_message(message),
                is_critical: false,
            }
        }

        /// Set whether this health check is critical
        pub fn set_critical(mut self, critical: bool) -> Self {
            self.is_critical = critical;
            self
        }

        /// Set a custom message
        pub fn with_message(mut self, message: impl Into<String>) -> Self {
            self.health_check = self.health_check.with_message(message);
            self
        }

        /// Set check duration
        pub fn with_duration(mut self, duration: Duration) -> Self {
            self.health_check = self.health_check.with_duration(duration);
            self
        }

        /// Add details to the health check
        pub fn with_details(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
            self.health_check = self.health_check.with_details(key, value);
            self
        }

        /// Get the component name
        pub fn component(&self) -> &str {
            &self.health_check.component
        }

        /// Get the health status
        pub fn status(&self) -> &HealthStatus {
            &self.health_check.status
        }

        /// Check if this health check is critical
        pub fn is_critical(&self) -> bool {
            self.is_critical
        }

        /// Get the underlying health check
        pub fn health_check(&self) -> &HealthCheck {
            &self.health_check
        }
    }
}

/// Test configuration builders and helpers
pub mod config {
    #[cfg(not(feature = "integration"))]
    use crate::config::BasicMonitoringConfig;
    #[cfg(feature = "integration")]
    use crate::config::MonitoringConfig;

    /// Builder for creating test monitoring configurations
    pub struct TestConfigBuilder {
        #[cfg(feature = "integration")]
        config: MonitoringConfig,
        #[cfg(not(feature = "integration"))]
        config: BasicMonitoringConfig,
    }

    impl TestConfigBuilder {
        /// Create a new test config builder with defaults
        pub fn new() -> Self {
            Self {
                #[cfg(feature = "integration")]
                config: MonitoringConfig::default(),
                #[cfg(not(feature = "integration"))]
                config: BasicMonitoringConfig::default(),
            }
        }

        /// Enable Prometheus with a test address
        pub fn with_prometheus(mut self) -> Self {
            #[cfg(feature = "integration")]
            {
                self.config.metrics.enabled = true;
                self.config.metrics.exporters = vec![crate::config::MetricsExporter::Prometheus {
                    endpoint: "127.0.0.1:0".to_string(),
                    interval_secs: 15,
                }];
            }
            #[cfg(not(feature = "integration"))]
            {
                self.config.prometheus_enabled = true;
                self.config.prometheus_addr = "127.0.0.1:0".to_string();
            }
            self
        }

        /// Enable Jaeger with a test endpoint
        pub fn with_jaeger(mut self) -> Self {
            #[cfg(feature = "integration")]
            {
                self.config.tracing.enabled = true;
                self.config.tracing.exporters = vec![crate::config::TraceExporter::Jaeger {
                    endpoint: "http://localhost:14268/api/traces".to_string(),
                    token: None,
                }];
            }
            #[cfg(not(feature = "integration"))]
            {
                self.config.jaeger_enabled = true;
                self.config.jaeger_endpoint = "http://localhost:14268/api/traces".to_string();
            }
            self
        }

        /// Enable health checks with test settings
        pub fn with_health_checks(mut self) -> Self {
            #[cfg(feature = "integration")]
            {
                self.config.health_checks.enabled = true;
                self.config.health_checks.check_interval_secs = 1;
            }
            #[cfg(not(feature = "integration"))]
            {
                self.config.health_checks_enabled = true;
                self.config.health_check_interval_secs = 1;
            }
            self
        }

        /// Build the configuration
        #[cfg(feature = "integration")]
        pub fn build(self) -> MonitoringConfig {
            self.config
        }

        /// Build the configuration
        #[cfg(not(feature = "integration"))]
        pub fn build(self) -> BasicMonitoringConfig {
            self.config
        }
    }

    impl Default for TestConfigBuilder {
        fn default() -> Self {
            Self::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mock_metrics_collector_works() {
        let mut collector = metrics::MockMetricsCollector::new();

        collector.record_counter("test_counter", 1);
        collector.record_counter("test_counter", 2);
        collector.record_gauge("test_gauge", 42.0);
        collector.record_histogram("test_histogram", 1.5);

        assert_eq!(collector.get_counter("test_counter"), 3);
        assert_eq!(collector.get_gauge("test_gauge"), Some(42.0));
        assert_eq!(collector.get_histogram("test_histogram"), Some(&[1.5][..]));
        assert_eq!(collector.counter_count(), 1);
        assert_eq!(collector.gauge_count(), 1);
        assert_eq!(collector.histogram_count(), 1);
    }

    #[test]
    fn mock_health_check_creation() {
        let healthy = health::MockHealthCheck::healthy("test");
        assert_eq!(healthy.status(), &HealthStatus::Healthy);
        assert_eq!(healthy.component(), "test");
        assert!(healthy.is_critical());

        let unhealthy = health::MockHealthCheck::unhealthy("test2", "Something is wrong");
        assert_eq!(unhealthy.status(), &HealthStatus::Unhealthy);
        assert_eq!(unhealthy.component(), "test2");

        let degraded = health::MockHealthCheck::degraded("test3", "Minor issue")
            .set_critical(false)
            .with_details("key", "value");
        assert_eq!(degraded.status(), &HealthStatus::Degraded);
        assert!(!degraded.is_critical());
    }

    #[test]
    fn test_config_builder_works() {
        let config = config::TestConfigBuilder::new()
            .with_prometheus()
            .with_jaeger()
            .with_health_checks()
            .build();

        #[cfg(feature = "integration")]
        {
            assert!(config.metrics.enabled);
            assert!(config.tracing.enabled);
            assert!(config.health_checks.enabled);
        }
        #[cfg(not(feature = "integration"))]
        {
            assert!(config.prometheus_enabled);
            assert!(config.jaeger_enabled);
            assert!(config.health_checks_enabled);
        }
    }
}
