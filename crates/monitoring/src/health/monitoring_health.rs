//! Health checks for monitoring system components
//!
//! This module provides health check implementations for various monitoring
//! components to ensure the monitoring system is operating correctly.

use crate::diagnostics::SelfMonitoring;
use crate::error::Result;
use orcher_common::monitoring::{HealthCheck, HealthStatus};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Trait for components that can perform health checks
pub trait Healthcheckable {
    /// Get the name of the component being checked
    fn name(&self) -> &str;

    /// Perform the health check and return status
    fn check(&self) -> HealthStatus;

    /// Perform the health check and return a full HealthCheck struct
    fn health_check(&self) -> HealthCheck {
        let start = Instant::now();
        let status = self.check();
        let duration = start.elapsed();

        HealthCheck::new(self.name().to_string())
            .with_status(status)
            .with_duration(duration)
    }
}

/// Health check for the overall monitoring system
pub struct MonitoringSystemHealthCheck {
    self_monitoring: Arc<SelfMonitoring>,
    max_unhealthy_components: usize,
    max_error_rate: f64,
}

impl MonitoringSystemHealthCheck {
    /// Create a new monitoring system health check
    pub fn new(self_monitoring: Arc<SelfMonitoring>) -> Self {
        Self {
            self_monitoring,
            max_unhealthy_components: 2,
            max_error_rate: 0.1, // 10% error rate threshold
        }
    }

    /// Create with custom thresholds
    pub fn with_thresholds(
        self_monitoring: Arc<SelfMonitoring>,
        max_unhealthy_components: usize,
        max_error_rate: f64,
    ) -> Self {
        Self {
            self_monitoring,
            max_unhealthy_components,
            max_error_rate,
        }
    }
}

impl Healthcheckable for MonitoringSystemHealthCheck {
    fn name(&self) -> &str {
        "monitoring_system"
    }

    fn check(&self) -> HealthStatus {
        let start = Instant::now();

        // Check if the monitoring system itself is healthy
        let is_healthy = self.self_monitoring.is_healthy();

        // Get component health status
        let component_health = self.self_monitoring.get_component_health();
        let unhealthy_count = component_health
            .values()
            .filter(|status| matches!(status, HealthStatus::Unhealthy))
            .count();

        // Check error rates
        let exporter_errors = self.self_monitoring.get_exporter_error_counts();
        let total_errors: u64 = exporter_errors.values().sum();
        let metrics_collected = self.self_monitoring.get_metrics_collected_count();

        let error_rate = if metrics_collected > 0 {
            total_errors as f64 / metrics_collected as f64
        } else {
            0.0
        };

        // Record health check duration
        let duration = start.elapsed();
        self.self_monitoring
            .record_health_check_duration("monitoring_system", duration);

        // Determine overall health status
        if !is_healthy
            || unhealthy_count > self.max_unhealthy_components
            || error_rate > self.max_error_rate
        {
            HealthStatus::Unhealthy
        } else if unhealthy_count > 0 || error_rate > 0.05 {
            HealthStatus::Degraded
        } else {
            HealthStatus::Healthy
        }
    }
}

/// Health check for metrics exporters
pub struct ExporterHealthCheck {
    exporter_name: String,
    self_monitoring: Arc<SelfMonitoring>,
    max_error_count: u64,
    check_interval: Duration,
    last_check: std::sync::Mutex<Option<Instant>>,
}

impl ExporterHealthCheck {
    /// Create a new exporter health check
    pub fn new(exporter_name: String, self_monitoring: Arc<SelfMonitoring>) -> Self {
        Self {
            exporter_name,
            self_monitoring,
            max_error_count: 10,
            check_interval: Duration::from_secs(30),
            last_check: std::sync::Mutex::new(None),
        }
    }

    /// Create with custom parameters
    pub fn with_config(
        exporter_name: String,
        self_monitoring: Arc<SelfMonitoring>,
        max_error_count: u64,
        check_interval: Duration,
    ) -> Self {
        Self {
            exporter_name,
            self_monitoring,
            max_error_count,
            check_interval,
            last_check: std::sync::Mutex::new(None),
        }
    }

    /// Check if enough time has passed since last check
    fn should_check(&self) -> bool {
        let mut last_check = self.last_check.lock().unwrap();
        let now = Instant::now();

        match *last_check {
            Some(last) if now.duration_since(last) < self.check_interval => false,
            _ => {
                *last_check = Some(now);
                true
            }
        }
    }
}

impl Healthcheckable for ExporterHealthCheck {
    fn name(&self) -> &str {
        &self.exporter_name
    }

    fn check(&self) -> HealthStatus {
        if !self.should_check() {
            return HealthStatus::Healthy; // Assume healthy if not time to check
        }

        let start = Instant::now();

        // Get error count for this specific exporter
        let error_counts = self.self_monitoring.get_exporter_error_counts();
        let error_count = error_counts.get(&self.exporter_name).unwrap_or(&0);

        // Record health check duration
        let duration = start.elapsed();
        self.self_monitoring
            .record_health_check_duration(&format!("exporter_{}", self.exporter_name), duration);

        // Determine health based on error count
        if *error_count >= self.max_error_count {
            HealthStatus::Unhealthy
        } else if *error_count > self.max_error_count / 2 {
            HealthStatus::Degraded
        } else {
            HealthStatus::Healthy
        }
    }
}

/// Create a comprehensive health check for the monitoring system
pub fn create_monitoring_health_check(
    self_monitoring: Arc<SelfMonitoring>,
) -> Result<MonitoringSystemHealthCheck> {
    Ok(MonitoringSystemHealthCheck::new(self_monitoring))
}

/// Create health checks for common exporters
pub fn create_exporter_health_checks(
    self_monitoring: Arc<SelfMonitoring>,
) -> Result<Vec<ExporterHealthCheck>> {
    let exporters = vec!["prometheus", "jaeger", "otlp"];

    let health_checks = exporters
        .into_iter()
        .map(|name| ExporterHealthCheck::new(name.to_string(), self_monitoring.clone()))
        .collect();

    Ok(health_checks)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_monitoring_system_health_check_creation() {
        let self_monitoring = Arc::new(SelfMonitoring::new());
        let health_check = MonitoringSystemHealthCheck::new(self_monitoring);

        assert_eq!(health_check.name(), "monitoring_system");
    }

    #[test]
    fn test_exporter_health_check_creation() {
        let self_monitoring = Arc::new(SelfMonitoring::new());
        let health_check = ExporterHealthCheck::new("prometheus".to_string(), self_monitoring);

        assert_eq!(health_check.name(), "prometheus");
    }

    #[test]
    fn test_monitoring_system_health_check() {
        let self_monitoring = Arc::new(SelfMonitoring::new());
        let health_check = MonitoringSystemHealthCheck::new(self_monitoring);

        let status = health_check.check();
        // Should be healthy for a new monitoring system
        assert!(matches!(status, HealthStatus::Healthy));
    }

    #[test]
    fn test_exporter_health_check() {
        let self_monitoring = Arc::new(SelfMonitoring::new());
        let health_check = ExporterHealthCheck::new("prometheus".to_string(), self_monitoring);

        let status = health_check.check();
        // Should be healthy for a new exporter
        assert!(matches!(status, HealthStatus::Healthy));
    }

    #[test]
    fn test_create_monitoring_health_check() {
        let self_monitoring = Arc::new(SelfMonitoring::new());
        let result = create_monitoring_health_check(self_monitoring);

        assert!(result.is_ok());
        let health_check = result.unwrap();
        assert_eq!(health_check.name(), "monitoring_system");
    }

    #[test]
    fn test_create_exporter_health_checks() {
        let self_monitoring = Arc::new(SelfMonitoring::new());
        let result = create_exporter_health_checks(self_monitoring);

        assert!(result.is_ok());
        let health_checks = result.unwrap();
        assert_eq!(health_checks.len(), 3);

        let names: Vec<&str> = health_checks.iter().map(|hc| hc.name()).collect();
        assert!(names.contains(&"prometheus"));
        assert!(names.contains(&"jaeger"));
        assert!(names.contains(&"otlp"));
    }

    #[test]
    fn test_health_check_with_thresholds() {
        let self_monitoring = Arc::new(SelfMonitoring::new());
        let health_check = MonitoringSystemHealthCheck::with_thresholds(self_monitoring, 5, 0.2);

        assert_eq!(health_check.max_unhealthy_components, 5);
        assert_eq!(health_check.max_error_rate, 0.2);
    }

    #[test]
    fn test_exporter_health_check_with_config() {
        let self_monitoring = Arc::new(SelfMonitoring::new());
        let health_check = ExporterHealthCheck::with_config(
            "custom_exporter".to_string(),
            self_monitoring,
            20,
            Duration::from_secs(60),
        );

        assert_eq!(health_check.name(), "custom_exporter");
        assert_eq!(health_check.max_error_count, 20);
        assert_eq!(health_check.check_interval, Duration::from_secs(60));
    }
}
