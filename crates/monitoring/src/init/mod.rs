//! Monitoring system initialization utilities
//!
//! This module provides convenient builders, defaults, and presets for
//! initializing the ORCHER monitoring system with minimal configuration.
//!
//! # Quick Start
//!
//! The easiest way to get started is with a preset:
//!
//! ```no_run
//! use orcher_monitoring::init::presets;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // For development
//! let mut monitoring = presets::development_mode().await?;
//! monitoring.start().await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Custom Configuration
//!
//! For more control, use the builder:
//!
//! ```no_run
//! use orcher_monitoring::init::MonitoringBuilder;
//! use std::net::SocketAddr;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let mut monitoring = MonitoringBuilder::new()
//!     .with_service_name("my-service")
//!     .with_prometheus("0.0.0.0:9090".parse()?)
//!     .with_jaeger("my-service", "localhost:6831")
//!     .with_health_checks(true)
//!     .build()
//!     .await?;
//!
//! monitoring.start().await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Environment-Specific Defaults
//!
//! Use environment-specific defaults as a starting point:
//!
//! ```no_run
//! use orcher_monitoring::init::defaults;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // For production with custom OTLP endpoint
//! let mut monitoring = defaults::production("http://otel-collector:4317")
//!     .build()
//!     .await?;
//!
//! monitoring.start().await?;
//! # Ok(())
//! # }
//! ```

pub mod builder;
pub mod defaults;
pub mod presets;

// Re-export main types for convenience
pub use builder::{MonitoringBuilder, MonitoringSystem};

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_builder_basic_setup() {
        let system = MonitoringBuilder::new()
            .with_service_name("test")
            .build()
            .await
            .unwrap();

        assert_eq!(system.service_name(), "test");
    }

    #[tokio::test]
    #[ignore = "requires network binding"]
    async fn test_development_default() {
        let system = defaults::development().build().await.unwrap();
        assert_eq!(system.service_name(), "orcher-dev");
    }

    #[tokio::test]
    #[ignore = "requires network binding"]
    async fn test_minimal_preset() {
        let system = presets::minimal_monitoring().await.unwrap();
        assert_eq!(system.service_name(), "orcher");
    }
}
