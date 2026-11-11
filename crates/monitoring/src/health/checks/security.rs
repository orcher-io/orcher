//! Security system health check implementation
//!
//! This module provides health checks for security systems including authentication services,
//! rate limiting, and threat detection systems.

use crate::error::{MonitoringError, Result};
use crate::health::check::{
    AsyncHealthCheck, HealthCheckConfig, HealthCheckCriticality, HealthCheckResult,
};
use orcher_common::monitoring::HealthStatus;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Configuration for security system health checks
#[derive(Debug, Clone)]
pub struct SecurityHealthCheckConfig {
    /// Timeout for security system checks
    pub check_timeout: Duration,
    /// Maximum allowed authentication failures per minute
    pub max_auth_failures_per_minute: u32,
    /// Maximum allowed rate limit violations per minute
    pub max_rate_limit_violations_per_minute: u32,
    /// Maximum allowed threat detection alerts per minute
    pub max_threats_per_minute: u32,
    /// Minimum required authentication success rate (0.0-1.0)
    pub min_auth_success_rate: f64,
}

impl Default for SecurityHealthCheckConfig {
    fn default() -> Self {
        Self {
            check_timeout: Duration::from_secs(5),
            max_auth_failures_per_minute: 100,
            max_rate_limit_violations_per_minute: 50,
            max_threats_per_minute: 10,
            min_auth_success_rate: 0.9,
        }
    }
}

/// Security system health check implementation
pub struct SecuritySystemHealthCheck {
    name: String,
    config: SecurityHealthCheckConfig,
    // In a real implementation, these would interface with actual security systems
    // For now, we'll simulate the security system status
    auth_service_running: Arc<std::sync::atomic::AtomicBool>,
    rate_limiter_running: Arc<std::sync::atomic::AtomicBool>,
    threat_detector_running: Arc<std::sync::atomic::AtomicBool>,
    auth_attempts: Arc<std::sync::atomic::AtomicU64>,
    auth_successes: Arc<std::sync::atomic::AtomicU64>,
    auth_failures: Arc<std::sync::atomic::AtomicU64>,
    rate_limit_violations: Arc<std::sync::atomic::AtomicU32>,
    threats_detected: Arc<std::sync::atomic::AtomicU32>,
}

impl SecuritySystemHealthCheck {
    /// Create a new security system health check
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            config: SecurityHealthCheckConfig::default(),
            auth_service_running: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            rate_limiter_running: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            threat_detector_running: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            auth_attempts: Arc::new(std::sync::atomic::AtomicU64::new(1000)),
            auth_successes: Arc::new(std::sync::atomic::AtomicU64::new(950)),
            auth_failures: Arc::new(std::sync::atomic::AtomicU64::new(50)),
            rate_limit_violations: Arc::new(std::sync::atomic::AtomicU32::new(5)),
            threats_detected: Arc::new(std::sync::atomic::AtomicU32::new(2)),
        }
    }

    /// Create a security system health check with custom configuration
    pub fn with_config(name: impl Into<String>, config: SecurityHealthCheckConfig) -> Self {
        Self {
            name: name.into(),
            config,
            auth_service_running: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            rate_limiter_running: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            threat_detector_running: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            auth_attempts: Arc::new(std::sync::atomic::AtomicU64::new(1000)),
            auth_successes: Arc::new(std::sync::atomic::AtomicU64::new(950)),
            auth_failures: Arc::new(std::sync::atomic::AtomicU64::new(50)),
            rate_limit_violations: Arc::new(std::sync::atomic::AtomicU32::new(5)),
            threats_detected: Arc::new(std::sync::atomic::AtomicU32::new(2)),
        }
    }

    /// Set service running status (for testing/simulation)
    pub fn set_service_status(&self, auth: bool, rate_limiter: bool, threat_detector: bool) {
        self.auth_service_running
            .store(auth, std::sync::atomic::Ordering::Relaxed);
        self.rate_limiter_running
            .store(rate_limiter, std::sync::atomic::Ordering::Relaxed);
        self.threat_detector_running
            .store(threat_detector, std::sync::atomic::Ordering::Relaxed);
    }

    /// Set authentication statistics (for testing/simulation)
    pub fn set_auth_stats(&self, attempts: u64, successes: u64, failures: u64) {
        self.auth_attempts
            .store(attempts, std::sync::atomic::Ordering::Relaxed);
        self.auth_successes
            .store(successes, std::sync::atomic::Ordering::Relaxed);
        self.auth_failures
            .store(failures, std::sync::atomic::Ordering::Relaxed);
    }

    /// Set security violation counts (for testing/simulation)
    pub fn set_violation_stats(&self, rate_limit_violations: u32, threats: u32) {
        self.rate_limit_violations
            .store(rate_limit_violations, std::sync::atomic::Ordering::Relaxed);
        self.threats_detected
            .store(threats, std::sync::atomic::Ordering::Relaxed);
    }

    /// Check authentication service status
    fn check_auth_service(&self) -> Result<(bool, f64, u64, u64, u64)> {
        let is_running = self
            .auth_service_running
            .load(std::sync::atomic::Ordering::Relaxed);

        if !is_running {
            return Err(MonitoringError::health_check_failed(
                "Authentication service is not running",
            ));
        }

        let attempts = self
            .auth_attempts
            .load(std::sync::atomic::Ordering::Relaxed);
        let successes = self
            .auth_successes
            .load(std::sync::atomic::Ordering::Relaxed);
        let failures = self
            .auth_failures
            .load(std::sync::atomic::Ordering::Relaxed);

        let success_rate = if attempts > 0 {
            successes as f64 / attempts as f64
        } else {
            1.0
        };

        if success_rate < self.config.min_auth_success_rate {
            return Err(MonitoringError::health_check_failed(format!(
                "Authentication success rate too low: {:.1}% (minimum: {:.1}%)",
                success_rate * 100.0,
                self.config.min_auth_success_rate * 100.0
            )));
        }

        Ok((is_running, success_rate, attempts, successes, failures))
    }

    /// Check rate limiting system
    fn check_rate_limiter(&self) -> Result<(bool, u32)> {
        let is_running = self
            .rate_limiter_running
            .load(std::sync::atomic::Ordering::Relaxed);

        if !is_running {
            return Err(MonitoringError::health_check_failed(
                "Rate limiter service is not running",
            ));
        }

        let violations = self
            .rate_limit_violations
            .load(std::sync::atomic::Ordering::Relaxed);

        if violations > self.config.max_rate_limit_violations_per_minute {
            return Err(MonitoringError::health_check_failed(format!(
                "Too many rate limit violations: {} (maximum: {})",
                violations, self.config.max_rate_limit_violations_per_minute
            )));
        }

        Ok((is_running, violations))
    }

    /// Check threat detection system
    fn check_threat_detector(&self) -> Result<(bool, u32)> {
        let is_running = self
            .threat_detector_running
            .load(std::sync::atomic::Ordering::Relaxed);

        if !is_running {
            return Err(MonitoringError::health_check_failed(
                "Threat detection service is not running",
            ));
        }

        let threats = self
            .threats_detected
            .load(std::sync::atomic::Ordering::Relaxed);

        if threats > self.config.max_threats_per_minute {
            return Err(MonitoringError::health_check_failed(format!(
                "Too many threats detected: {} (maximum: {})",
                threats, self.config.max_threats_per_minute
            )));
        }

        Ok((is_running, threats))
    }

    /// Test security system responsiveness
    async fn test_security_responsiveness(&self) -> Result<Duration> {
        let start = Instant::now();

        // Simulate checking security system responsiveness
        let response_future = async {
            tokio::time::sleep(Duration::from_millis(15)).await;
            Ok::<(), MonitoringError>(())
        };

        match tokio::time::timeout(self.config.check_timeout, response_future).await {
            Ok(_) => Ok(start.elapsed()),
            Err(_) => Err(MonitoringError::health_check_failed(
                "Security system responsiveness check timeout",
            )),
        }
    }
}

#[async_trait::async_trait]
impl AsyncHealthCheck for SecuritySystemHealthCheck {
    fn name(&self) -> &str {
        &self.name
    }

    fn config(&self) -> HealthCheckConfig {
        HealthCheckConfig {
            timeout: self.config.check_timeout,
            interval: Duration::from_secs(30),
            criticality: HealthCheckCriticality::Critical,
            failure_threshold: 2,
            success_threshold: 1,
            circuit_breaker_enabled: true,
        }
    }

    async fn check(&self) -> Result<HealthCheckResult> {
        let start = Instant::now();
        let mut result = HealthCheckResult::new(&self.name, HealthStatus::Healthy);

        // Check authentication service
        match self.check_auth_service() {
            Ok((is_running, success_rate, attempts, successes, failures)) => {
                result = result
                    .with_metadata("auth_service_running", is_running.to_string())
                    .with_metadata("auth_success_rate", format!("{:.3}", success_rate))
                    .with_metadata("auth_attempts", attempts.to_string())
                    .with_metadata("auth_successes", successes.to_string())
                    .with_metadata("auth_failures", failures.to_string());
            }
            Err(e) => {
                result.status = HealthStatus::Unhealthy;
                return Ok(result
                    .with_message(format!("Authentication service check failed: {}", e))
                    .with_duration(start.elapsed()));
            }
        }

        // Check rate limiter
        match self.check_rate_limiter() {
            Ok((is_running, violations)) => {
                result = result
                    .with_metadata("rate_limiter_running", is_running.to_string())
                    .with_metadata("rate_limit_violations", violations.to_string());
            }
            Err(e) => {
                result.status = HealthStatus::Unhealthy;
                return Ok(result
                    .with_message(format!("Rate limiter check failed: {}", e))
                    .with_duration(start.elapsed()));
            }
        }

        // Check threat detector
        match self.check_threat_detector() {
            Ok((is_running, threats)) => {
                result = result
                    .with_metadata("threat_detector_running", is_running.to_string())
                    .with_metadata("threats_detected", threats.to_string());
            }
            Err(e) => {
                result.status = HealthStatus::Unhealthy;
                return Ok(result
                    .with_message(format!("Threat detector check failed: {}", e))
                    .with_duration(start.elapsed()));
            }
        }

        // Test security system responsiveness
        match self.test_security_responsiveness().await {
            Ok(response_time) => {
                result = result
                    .with_message(
                        "Security system healthy - auth, rate limiting, and threat detection operational",
                    )
                    .with_metadata("response_time_ms", response_time.as_millis().to_string())
                    .with_metadata("responsiveness_status", "success")
                    .with_duration(start.elapsed());
            }
            Err(e) => {
                result.status = HealthStatus::Unhealthy;
                result = result
                    .with_message(format!("Security system responsiveness test failed: {}", e))
                    .with_duration(start.elapsed());
            }
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_security_health_check_creation() {
        let check = SecuritySystemHealthCheck::new("test_security");
        assert_eq!(check.name(), "test_security");
        assert_eq!(check.config().criticality, HealthCheckCriticality::Critical);
    }

    #[test]
    fn test_security_health_check_with_config() {
        let config = SecurityHealthCheckConfig {
            check_timeout: Duration::from_secs(3),
            max_auth_failures_per_minute: 50,
            max_rate_limit_violations_per_minute: 25,
            max_threats_per_minute: 5,
            min_auth_success_rate: 0.95,
        };

        let check = SecuritySystemHealthCheck::with_config("custom_security", config.clone());
        assert_eq!(check.config.max_auth_failures_per_minute, 50);
        assert_eq!(check.config.min_auth_success_rate, 0.95);
    }

    #[tokio::test]
    async fn test_security_health_check_success() {
        let check = SecuritySystemHealthCheck::new("test_security");
        check.set_service_status(true, true, true);
        check.set_auth_stats(1000, 950, 50);
        check.set_violation_stats(5, 2);

        let result = check.check().await.unwrap();
        assert_eq!(result.status, HealthStatus::Healthy);
        assert!(result.message.unwrap().contains("healthy"));
        assert_eq!(
            result.metadata.get("auth_service_running"),
            Some(&"true".to_string())
        );
        assert_eq!(
            result.metadata.get("rate_limiter_running"),
            Some(&"true".to_string())
        );
        assert_eq!(
            result.metadata.get("threat_detector_running"),
            Some(&"true".to_string())
        );
    }

    #[tokio::test]
    async fn test_security_health_check_auth_service_down() {
        let check = SecuritySystemHealthCheck::new("test_security");
        check.set_service_status(false, true, true);

        let result = check.check().await.unwrap();
        assert_eq!(result.status, HealthStatus::Unhealthy);
        assert!(result
            .message
            .unwrap()
            .contains("Authentication service is not running"));
    }

    #[tokio::test]
    async fn test_security_health_check_low_auth_success_rate() {
        let check = SecuritySystemHealthCheck::new("test_security");
        check.set_service_status(true, true, true);
        check.set_auth_stats(1000, 800, 200); // 80% success rate, below 90% threshold

        let result = check.check().await.unwrap();
        assert_eq!(result.status, HealthStatus::Unhealthy);
        assert!(result.message.unwrap().contains("success rate too low"));
    }

    #[tokio::test]
    async fn test_security_health_check_high_rate_limit_violations() {
        let check = SecuritySystemHealthCheck::new("test_security");
        check.set_service_status(true, true, true);
        check.set_auth_stats(1000, 950, 50);
        check.set_violation_stats(100, 2); // Exceeds default max of 50

        let result = check.check().await.unwrap();
        assert_eq!(result.status, HealthStatus::Unhealthy);
        assert!(result
            .message
            .unwrap()
            .contains("Too many rate limit violations"));
    }

    #[tokio::test]
    async fn test_security_health_check_high_threats() {
        let check = SecuritySystemHealthCheck::new("test_security");
        check.set_service_status(true, true, true);
        check.set_auth_stats(1000, 950, 50);
        check.set_violation_stats(5, 20); // Exceeds default max of 10

        let result = check.check().await.unwrap();
        assert_eq!(result.status, HealthStatus::Unhealthy);
        assert!(result
            .message
            .unwrap()
            .contains("Too many threats detected"));
    }

    #[test]
    fn test_default_config() {
        let config = SecurityHealthCheckConfig::default();
        assert_eq!(config.check_timeout, Duration::from_secs(5));
        assert_eq!(config.max_auth_failures_per_minute, 100);
        assert_eq!(config.max_rate_limit_violations_per_minute, 50);
        assert_eq!(config.max_threats_per_minute, 10);
        assert_eq!(config.min_auth_success_rate, 0.9);
    }
}
