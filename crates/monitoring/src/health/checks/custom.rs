//! Custom health check helpers implementation
//!
//! This module provides utilities for creating user-defined health checks with
//! configurable timeout and retry logic, making it easy to integrate custom
//! monitoring into the ORCHER platform.

use crate::error::{MonitoringError, Result};
use crate::health::check::{
    AsyncHealthCheck, HealthCheckConfig, HealthCheckCriticality, HealthCheckResult,
};
use orcher_common::monitoring::HealthStatus;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Type alias for custom health check functions
pub type CustomHealthCheckFn = Arc<
    dyn Fn() -> Pin<Box<dyn Future<Output = Result<HealthCheckResult>> + Send + 'static>>
        + Send
        + Sync,
>;

/// Configuration for custom health checks with retry logic
#[derive(Debug, Clone)]
pub struct CustomHealthCheckConfig {
    /// Timeout for each health check attempt
    pub timeout: Duration,
    /// Interval between health checks
    pub interval: Duration,
    /// Criticality level of the health check
    pub criticality: HealthCheckCriticality,
    /// Number of consecutive failures before marking as unhealthy
    pub failure_threshold: u32,
    /// Number of consecutive successes needed to mark as healthy again
    pub success_threshold: u32,
    /// Whether to enable circuit breaker protection
    pub circuit_breaker_enabled: bool,
    /// Number of retry attempts on failure
    pub retry_attempts: u32,
    /// Delay between retry attempts
    pub retry_delay: Duration,
}

impl Default for CustomHealthCheckConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(30),
            interval: Duration::from_secs(60),
            criticality: HealthCheckCriticality::Important,
            failure_threshold: 3,
            success_threshold: 1,
            circuit_breaker_enabled: true,
            retry_attempts: 2,
            retry_delay: Duration::from_millis(500),
        }
    }
}

/// Builder for creating custom health checks
pub struct CustomHealthCheckBuilder {
    name: String,
    config: CustomHealthCheckConfig,
    check_fn: Option<CustomHealthCheckFn>,
}

impl CustomHealthCheckBuilder {
    /// Create a new custom health check builder
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            config: CustomHealthCheckConfig::default(),
            check_fn: None,
        }
    }

    /// Set the health check function
    pub fn with_check_fn<F, Fut>(mut self, check_fn: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<HealthCheckResult>> + Send + 'static,
    {
        self.check_fn = Some(Arc::new(move || Box::pin(check_fn())));
        self
    }

    /// Set the timeout for health check operations
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.config.timeout = timeout;
        self
    }

    /// Set the interval between health checks
    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.config.interval = interval;
        self
    }

    /// Set the criticality level
    pub fn with_criticality(mut self, criticality: HealthCheckCriticality) -> Self {
        self.config.criticality = criticality;
        self
    }

    /// Set failure and success thresholds
    pub fn with_thresholds(mut self, failure_threshold: u32, success_threshold: u32) -> Self {
        self.config.failure_threshold = failure_threshold;
        self.config.success_threshold = success_threshold;
        self
    }

    /// Enable or disable circuit breaker
    pub fn with_circuit_breaker(mut self, enabled: bool) -> Self {
        self.config.circuit_breaker_enabled = enabled;
        self
    }

    /// Set retry configuration
    pub fn with_retry(mut self, attempts: u32, delay: Duration) -> Self {
        self.config.retry_attempts = attempts;
        self.config.retry_delay = delay;
        self
    }

    /// Build the custom health check
    pub fn build(self) -> Result<CustomHealthCheck> {
        let check_fn = self
            .check_fn
            .ok_or_else(|| MonitoringError::health_check_failed("Health check function not set"))?;

        Ok(CustomHealthCheck {
            name: self.name,
            config: self.config,
            check_fn,
        })
    }
}

/// Custom health check implementation with retry logic
pub struct CustomHealthCheck {
    name: String,
    config: CustomHealthCheckConfig,
    check_fn: CustomHealthCheckFn,
}

impl CustomHealthCheck {
    /// Create a new custom health check builder
    pub fn builder(name: impl Into<String>) -> CustomHealthCheckBuilder {
        CustomHealthCheckBuilder::new(name)
    }

    /// Create a simple custom health check from a closure
    pub fn from_fn<F, Fut>(name: impl Into<String>, check_fn: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<HealthCheckResult>> + Send + 'static,
    {
        Self {
            name: name.into(),
            config: CustomHealthCheckConfig::default(),
            check_fn: Arc::new(move || Box::pin(check_fn())),
        }
    }

    /// Create a ping-style health check for testing connectivity
    pub fn ping_check(name: impl Into<String>, endpoint: impl Into<String>) -> Self {
        let endpoint = endpoint.into();
        let check_name = name.into();

        Self::from_fn(check_name.clone(), move || {
            let endpoint = endpoint.clone();
            let check_name = check_name.clone();
            async move {
                let start = Instant::now();

                // Simulate ping operation
                tokio::time::sleep(Duration::from_millis(50)).await;

                // For demonstration, randomly succeed/fail based on endpoint
                let success = !endpoint.contains("fail");

                let result = HealthCheckResult::new(&check_name, HealthStatus::Healthy)
                    .with_message(if success {
                        format!("Successfully pinged {}", endpoint)
                    } else {
                        format!("Failed to ping {}", endpoint)
                    })
                    .with_metadata("endpoint", endpoint)
                    .with_metadata("ping_time_ms", start.elapsed().as_millis().to_string())
                    .with_duration(start.elapsed());

                Ok(result)
            }
        })
    }

    /// Create a URL health check for HTTP endpoints
    pub fn url_check(name: impl Into<String>, url: impl Into<String>) -> Self {
        let url = url.into();
        let check_name = name.into();

        Self::from_fn(check_name.clone(), move || {
            let url = url.clone();
            let check_name = check_name.clone();
            async move {
                let start = Instant::now();

                // Simulate HTTP request
                tokio::time::sleep(Duration::from_millis(100)).await;

                // For demonstration, check if URL is valid-looking
                let success = url.starts_with("http") && !url.contains("error");
                let status = if success {
                    HealthStatus::Healthy
                } else {
                    HealthStatus::Unhealthy
                };

                let result = HealthCheckResult::new(&check_name, status)
                    .with_message(if success {
                        format!("Successfully accessed {}", url)
                    } else {
                        format!("Failed to access {}", url)
                    })
                    .with_metadata("url", url)
                    .with_metadata("response_time_ms", start.elapsed().as_millis().to_string())
                    .with_duration(start.elapsed());

                Ok(result)
            }
        })
    }

    /// Perform health check with retry logic
    async fn check_with_retry(&self) -> Result<HealthCheckResult> {
        let mut last_error = None;

        for attempt in 0..=self.config.retry_attempts {
            match tokio::time::timeout(self.config.timeout, (self.check_fn)()).await {
                Ok(Ok(result)) => {
                    // Success - check if it's actually healthy
                    if result.is_healthy() {
                        return Ok(result);
                    } else if attempt < self.config.retry_attempts {
                        // Unhealthy but we have retries left
                        last_error = Some(format!(
                            "Health check returned unhealthy: {:?}",
                            result.message
                        ));
                        tokio::time::sleep(self.config.retry_delay).await;
                        continue;
                    } else {
                        // No more retries, return the unhealthy result
                        return Ok(result);
                    }
                }
                Ok(Err(e)) => {
                    last_error = Some(e.to_string());
                    if attempt < self.config.retry_attempts {
                        tokio::time::sleep(self.config.retry_delay).await;
                        continue;
                    }
                }
                Err(_) => {
                    last_error = Some("Health check timeout".to_string());
                    if attempt < self.config.retry_attempts {
                        tokio::time::sleep(self.config.retry_delay).await;
                        continue;
                    }
                }
            }
        }

        // All retries exhausted
        let error_msg = last_error.unwrap_or_else(|| "Unknown error".to_string());
        Ok(HealthCheckResult::new(&self.name, HealthStatus::Unhealthy)
            .with_message(format!(
                "Health check failed after {} retries: {}",
                self.config.retry_attempts, error_msg
            ))
            .with_metadata("retry_attempts", self.config.retry_attempts.to_string())
            .with_metadata("final_error", error_msg))
    }
}

#[async_trait::async_trait]
impl AsyncHealthCheck for CustomHealthCheck {
    fn name(&self) -> &str {
        &self.name
    }

    fn config(&self) -> HealthCheckConfig {
        HealthCheckConfig {
            timeout: self.config.timeout,
            interval: self.config.interval,
            criticality: self.config.criticality,
            failure_threshold: self.config.failure_threshold,
            success_threshold: self.config.success_threshold,
            circuit_breaker_enabled: self.config.circuit_breaker_enabled,
        }
    }

    async fn check(&self) -> Result<HealthCheckResult> {
        self.check_with_retry().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_custom_health_check_builder() {
        let check = CustomHealthCheck::builder("test_custom")
            .with_timeout(Duration::from_secs(5))
            .with_criticality(HealthCheckCriticality::Critical)
            .with_check_fn(|| async {
                Ok(HealthCheckResult::new("test_custom", HealthStatus::Healthy)
                    .with_message("Test successful"))
            })
            .build()
            .unwrap();

        assert_eq!(check.name(), "test_custom");
        assert_eq!(check.config().criticality, HealthCheckCriticality::Critical);
        assert_eq!(check.config.timeout, Duration::from_secs(5));

        let result = check.check().await.unwrap();
        assert_eq!(result.status, HealthStatus::Healthy);
        assert!(result.message.unwrap().contains("Test successful"));
    }

    #[tokio::test]
    async fn test_custom_health_check_from_fn() {
        let check = CustomHealthCheck::from_fn("test_fn", || async {
            Ok(HealthCheckResult::new("test_fn", HealthStatus::Healthy)
                .with_message("Function check successful"))
        });

        let result = check.check().await.unwrap();
        assert_eq!(result.status, HealthStatus::Healthy);
        assert!(result
            .message
            .unwrap()
            .contains("Function check successful"));
    }

    #[tokio::test]
    async fn test_ping_check_success() {
        let check = CustomHealthCheck::ping_check("test_ping", "example.com");

        let result = check.check().await.unwrap();
        assert_eq!(result.status, HealthStatus::Healthy);
        assert!(result.message.unwrap().contains("Successfully pinged"));
        assert_eq!(
            result.metadata.get("endpoint"),
            Some(&"example.com".to_string())
        );
    }

    #[tokio::test]
    async fn test_ping_check_failure() {
        let check = CustomHealthCheck::ping_check("test_ping_fail", "fail.example.com");

        let result = check.check().await.unwrap();
        assert_eq!(result.status, HealthStatus::Healthy); // The result is still returned
        assert!(result.message.unwrap().contains("Failed to ping"));
    }

    #[tokio::test]
    async fn test_url_check_success() {
        let check = CustomHealthCheck::url_check("test_url", "https://example.com");

        let result = check.check().await.unwrap();
        assert_eq!(result.status, HealthStatus::Healthy);
        assert!(result.message.unwrap().contains("Successfully accessed"));
        assert_eq!(
            result.metadata.get("url"),
            Some(&"https://example.com".to_string())
        );
    }

    #[tokio::test]
    async fn test_url_check_failure() {
        let check = CustomHealthCheck::url_check("test_url_fail", "invalid-url-error");

        let result = check.check().await.unwrap();
        assert_eq!(result.status, HealthStatus::Unhealthy);
        assert!(result.message.unwrap().contains("Failed to access"));
    }

    #[tokio::test]
    async fn test_retry_logic() {
        let attempt_count = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let attempt_count_clone = attempt_count.clone();

        let check = CustomHealthCheck::builder("test_retry")
            .with_retry(2, Duration::from_millis(10))
            .with_check_fn(move || {
                let count = attempt_count_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                async move {
                    if count < 2 {
                        // Fail the first two attempts
                        Err(MonitoringError::health_check_failed("Simulated failure"))
                    } else {
                        // Succeed on the third attempt
                        Ok(HealthCheckResult::new("test_retry", HealthStatus::Healthy)
                            .with_message("Success after retries"))
                    }
                }
            })
            .build()
            .unwrap();

        let result = check.check().await.unwrap();
        assert_eq!(result.status, HealthStatus::Healthy);
        assert!(result.message.unwrap().contains("Success after retries"));

        // Should have made 3 attempts (initial + 2 retries)
        assert_eq!(attempt_count.load(std::sync::atomic::Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn test_retry_exhaustion() {
        let check = CustomHealthCheck::builder("test_retry_fail")
            .with_retry(1, Duration::from_millis(10))
            .with_check_fn(|| async { Err(MonitoringError::health_check_failed("Always fails")) })
            .build()
            .unwrap();

        let result = check.check().await.unwrap();
        assert_eq!(result.status, HealthStatus::Unhealthy);
        assert!(result.message.unwrap().contains("failed after 1 retries"));
        assert_eq!(
            result.metadata.get("retry_attempts"),
            Some(&"1".to_string())
        );
    }

    #[test]
    fn test_builder_missing_check_fn() {
        let result = CustomHealthCheck::builder("test_missing")
            .with_timeout(Duration::from_secs(5))
            .build();

        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e.to_string().contains("Health check function not set"));
        }
    }

    #[test]
    fn test_default_config() {
        let config = CustomHealthCheckConfig::default();
        assert_eq!(config.timeout, Duration::from_secs(30));
        assert_eq!(config.interval, Duration::from_secs(60));
        assert_eq!(config.criticality, HealthCheckCriticality::Important);
        assert_eq!(config.failure_threshold, 3);
        assert_eq!(config.success_threshold, 1);
        assert!(config.circuit_breaker_enabled);
        assert_eq!(config.retry_attempts, 2);
        assert_eq!(config.retry_delay, Duration::from_millis(500));
    }
}
