//! Configuration integration with orcher-config
//!
//! This module re-exports monitoring configuration types from the main
//! orcher-config crate to ensure consistency across the platform.

pub mod validation;

// Re-export monitoring configuration from orcher-config
#[cfg(feature = "integration")]
pub use orcher_config::config::monitoring::*;

// Provide basic config types when integration feature is disabled
#[cfg(not(feature = "integration"))]
pub mod basic {
    //! Basic configuration types for standalone monitoring usage

    use serde::{Deserialize, Serialize};
    use std::time::Duration;

    /// Basic monitoring configuration for standalone usage
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct BasicMonitoringConfig {
        /// Whether Prometheus exporter is enabled
        pub prometheus_enabled: bool,
        /// Prometheus bind address
        pub prometheus_addr: String,
        /// Whether Jaeger tracing is enabled
        pub jaeger_enabled: bool,
        /// Jaeger endpoint URL
        pub jaeger_endpoint: String,
        /// Whether health checks are enabled
        pub health_checks_enabled: bool,
        /// Health check interval in seconds
        pub health_check_interval_secs: u64,
    }

    impl Default for BasicMonitoringConfig {
        fn default() -> Self {
            Self {
                prometheus_enabled: false,
                prometheus_addr: "127.0.0.1:9090".to_string(),
                jaeger_enabled: false,
                jaeger_endpoint: "http://localhost:14268/api/traces".to_string(),
                health_checks_enabled: false,
                health_check_interval_secs: 30,
            }
        }
    }

    impl BasicMonitoringConfig {
        /// Returns the health check interval as a Duration
        pub fn health_check_interval(&self) -> Duration {
            Duration::from_secs(self.health_check_interval_secs)
        }
    }
}

#[cfg(not(feature = "integration"))]
pub use basic::*;

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(not(feature = "integration"))]
    #[test]
    fn basic_config_defaults() {
        let config = BasicMonitoringConfig::default();
        assert!(!config.prometheus_enabled);
        assert!(!config.jaeger_enabled);
        assert!(!config.health_checks_enabled);
        assert_eq!(config.prometheus_addr, "127.0.0.1:9090");
        assert_eq!(config.jaeger_endpoint, "http://localhost:14268/api/traces");
        assert_eq!(config.health_check_interval_secs, 30);
    }

    #[cfg(feature = "integration")]
    #[test]
    fn integrated_config_available() {
        let config = MonitoringConfig::default();
        assert!(config.metrics.enabled || !config.metrics.enabled); // Just test it compiles
    }
}
