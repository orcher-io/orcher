//! Health check trait and types for ORCHER monitoring framework
//!
//! This module defines the core health check functionality, building upon
//! the existing health check types from orcher-common while adding
//! monitoring-specific enhancements.

use crate::error::Result;
use orcher_common::monitoring::{HealthCheck, HealthStatus};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, Instant};

/// Async result type for health check operations
pub type HealthCheckFuture<'a> =
    Pin<Box<dyn Future<Output = Result<HealthCheckResult>> + Send + 'a>>;

/// Enhanced health check result with detailed information
#[derive(Debug, Clone)]
pub struct HealthCheckResult {
    /// Name of the component being checked
    pub component_name: String,
    /// Current health status
    pub status: HealthStatus,
    /// Human-readable status message
    pub message: Option<String>,
    /// Execution duration of the health check
    pub duration: Duration,
    /// Additional metadata about the health check
    pub metadata: HashMap<String, String>,
    /// Timestamp when the check was performed
    pub timestamp: Instant,
}

impl HealthCheckResult {
    /// Create a new health check result
    pub fn new(component_name: impl Into<String>, status: HealthStatus) -> Self {
        Self {
            component_name: component_name.into(),
            status,
            message: None,
            duration: Duration::ZERO,
            metadata: HashMap::new(),
            timestamp: Instant::now(),
        }
    }

    /// Set the status message
    pub fn with_message(mut self, message: impl Into<String>) -> Self {
        self.message = Some(message.into());
        self
    }

    /// Set the execution duration
    pub fn with_duration(mut self, duration: Duration) -> Self {
        self.duration = duration;
        self
    }

    /// Add metadata
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Check if the result indicates a healthy status
    pub fn is_healthy(&self) -> bool {
        matches!(self.status, HealthStatus::Healthy)
    }

    /// Check if the result indicates an unhealthy status
    pub fn is_unhealthy(&self) -> bool {
        matches!(self.status, HealthStatus::Unhealthy)
    }

    /// Convert to the common HealthCheck type
    pub fn to_health_check(&self) -> HealthCheck {
        let mut health_check =
            HealthCheck::new(self.component_name.clone()).with_status(self.status);

        if let Some(message) = &self.message {
            health_check = health_check.with_message(message.clone());
        }

        // Add duration to details
        health_check = health_check.with_details(
            "duration_ms".to_string(),
            self.duration.as_millis().to_string(),
        );

        // Add all metadata as details
        for (key, value) in &self.metadata {
            health_check = health_check.with_details(key.clone(), value.clone());
        }

        health_check
    }
}

/// Health check criticality level
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum HealthCheckCriticality {
    /// Critical health checks must pass for the system to be healthy
    Critical,
    /// Important health checks are logged but don't fail the overall system
    #[default]
    Important,
    /// Informational health checks provide status information
    Informational,
}

/// Configuration for a health check
#[derive(Debug, Clone)]
pub struct HealthCheckConfig {
    /// Timeout for the health check execution
    pub timeout: Duration,
    /// How often the health check should be executed
    pub interval: Duration,
    /// Criticality level of this health check
    pub criticality: HealthCheckCriticality,
    /// Number of consecutive failures before marking as unhealthy
    pub failure_threshold: u32,
    /// Number of consecutive successes needed to mark as healthy again
    pub success_threshold: u32,
    /// Whether to enable circuit breaker protection
    pub circuit_breaker_enabled: bool,
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(30),
            interval: Duration::from_secs(60),
            criticality: HealthCheckCriticality::Important,
            failure_threshold: 3,
            success_threshold: 1,
            circuit_breaker_enabled: true,
        }
    }
}

/// Async health check trait for monitoring components
#[async_trait::async_trait]
pub trait AsyncHealthCheck: Send + Sync {
    /// Get the name of the component being checked
    fn name(&self) -> &str;

    /// Get the health check configuration
    fn config(&self) -> HealthCheckConfig {
        HealthCheckConfig::default()
    }

    /// Perform the health check
    async fn check(&self) -> Result<HealthCheckResult>;

    /// Perform a quick health check with default timeout
    async fn quick_check(&self) -> Result<HealthCheckResult> {
        let start = Instant::now();

        match tokio::time::timeout(Duration::from_secs(5), self.check()).await {
            Ok(result) => result,
            Err(_) => Ok(HealthCheckResult::new(self.name(), HealthStatus::Unhealthy)
                .with_message("Health check timed out")
                .with_duration(start.elapsed())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_check_result_creation() {
        let result = HealthCheckResult::new("test_component", HealthStatus::Healthy);

        assert_eq!(result.component_name, "test_component");
        assert_eq!(result.status, HealthStatus::Healthy);
        assert!(result.is_healthy());
        assert!(!result.is_unhealthy());
    }

    #[test]
    fn test_health_check_result_with_metadata() {
        let result = HealthCheckResult::new("test_component", HealthStatus::Healthy)
            .with_message("All systems operational")
            .with_metadata("version", "1.0.0")
            .with_duration(Duration::from_millis(100));

        assert_eq!(result.message, Some("All systems operational".to_string()));
        assert_eq!(result.duration, Duration::from_millis(100));
        assert_eq!(result.metadata.get("version"), Some(&"1.0.0".to_string()));
    }

    #[test]
    fn test_health_check_result_to_health_check() {
        let result = HealthCheckResult::new("test_component", HealthStatus::Healthy)
            .with_message("OK")
            .with_duration(Duration::from_millis(50))
            .with_metadata("key", "value");

        let health_check = result.to_health_check();

        assert_eq!(health_check.component, "test_component");
        assert_eq!(health_check.status, HealthStatus::Healthy);
        assert_eq!(health_check.message, Some("OK".to_string()));
        assert_eq!(
            health_check.details.get("duration_ms"),
            Some(&"50".to_string())
        );
        assert_eq!(health_check.details.get("key"), Some(&"value".to_string()));
    }

    #[test]
    fn test_health_check_criticality() {
        assert_eq!(
            HealthCheckCriticality::default(),
            HealthCheckCriticality::Important
        );

        let critical = HealthCheckCriticality::Critical;
        let important = HealthCheckCriticality::Important;
        let info = HealthCheckCriticality::Informational;

        assert_ne!(critical, important);
        assert_ne!(important, info);
        assert_ne!(critical, info);
    }

    #[test]
    fn test_health_check_config_defaults() {
        let config = HealthCheckConfig::default();

        assert_eq!(config.timeout, Duration::from_secs(30));
        assert_eq!(config.interval, Duration::from_secs(60));
        assert_eq!(config.criticality, HealthCheckCriticality::Important);
        assert_eq!(config.failure_threshold, 3);
        assert_eq!(config.success_threshold, 1);
        assert!(config.circuit_breaker_enabled);
    }
}
