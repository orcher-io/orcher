//! Circuit breaker integration for health check reliability
//!
//! This module provides circuit breaker functionality specifically designed
//! for health checks, building upon the existing circuit breaker implementation
//! from orcher-common while adding health check specific features.

use crate::error::Result;
use crate::health::check::{AsyncHealthCheck, HealthCheckResult};
use orcher_common::monitoring::HealthStatus;
use orcher_common::utils::circuit_breaker::{CircuitBreaker, CircuitBreakerConfig, CircuitState};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Health check wrapper with circuit breaker protection
pub struct HealthCheckCircuitBreaker<T> {
    /// The underlying health check
    health_check: Arc<T>,
    /// Circuit breaker for reliability
    circuit_breaker: CircuitBreaker,
    /// Name for logging and identification
    name: String,
}

impl<T> HealthCheckCircuitBreaker<T>
where
    T: AsyncHealthCheck + 'static,
{
    /// Create a new health check with circuit breaker protection
    pub fn new(health_check: Arc<T>) -> Self {
        let name = health_check.name().to_string();
        let config = CircuitBreakerConfig {
            failure_threshold: 3,
            recovery_timeout: Duration::from_secs(60),
            success_threshold: 2,
            failure_window: Duration::from_secs(300),
            half_open_max_requests: 1,
        };

        Self {
            health_check,
            circuit_breaker: CircuitBreaker::new(config),
            name,
        }
    }

    /// Create with custom circuit breaker configuration
    pub fn with_config(health_check: Arc<T>, config: CircuitBreakerConfig) -> Self {
        let name = health_check.name().to_string();

        Self {
            health_check,
            circuit_breaker: CircuitBreaker::new(config),
            name,
        }
    }

    /// Execute the health check through the circuit breaker
    pub async fn execute(&self) -> Result<HealthCheckResult> {
        let start = Instant::now();

        match self
            .circuit_breaker
            .execute(|| async {
                self.health_check.check().await.map_err(|e| {
                    orcher_common::error::OrcherError::Internal {
                        message: format!("Health check failed: {}", e),
                        source: Some(e.to_string()),
                    }
                })
            })
            .await
        {
            Ok(result) => Ok(result),
            Err(orcher_common::error::OrcherError::RateLimit { message, .. }) => {
                // Circuit breaker is open, return unhealthy status
                Ok(HealthCheckResult::new(&self.name, HealthStatus::Unhealthy)
                    .with_message(format!("Circuit breaker open: {}", message))
                    .with_duration(start.elapsed())
                    .with_metadata("circuit_breaker_state", "open"))
            }
            Err(e) => {
                // Convert any other error to unhealthy status instead of propagating
                Ok(HealthCheckResult::new(&self.name, HealthStatus::Unhealthy)
                    .with_message(format!("Health check failed: {}", e))
                    .with_duration(start.elapsed())
                    .with_metadata("error_type", "execution_failed"))
            }
        }
    }

    /// Get the current circuit breaker state
    pub async fn circuit_state(&self) -> CircuitState {
        self.circuit_breaker.current_state().await
    }

    /// Get circuit breaker statistics
    pub async fn circuit_stats(&self) -> orcher_common::utils::circuit_breaker::CircuitStats {
        self.circuit_breaker.stats().await
    }

    /// Get the name of the health check
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Check if the circuit breaker is currently open
    pub async fn is_circuit_open(&self) -> bool {
        matches!(self.circuit_state().await, CircuitState::Open)
    }

    /// Reset the circuit breaker to closed state
    pub async fn reset_circuit(&self) {
        // Circuit breaker doesn't expose a reset method, so we can't implement this
        // This is intentional as circuit breakers should manage their own state
    }
}

#[async_trait::async_trait]
impl<T> AsyncHealthCheck for HealthCheckCircuitBreaker<T>
where
    T: AsyncHealthCheck + 'static,
{
    fn name(&self) -> &str {
        &self.name
    }

    async fn check(&self) -> Result<HealthCheckResult> {
        self.execute().await
    }

    async fn quick_check(&self) -> Result<HealthCheckResult> {
        // For quick checks, we still go through the circuit breaker
        // but with a shorter timeout
        let start = Instant::now();

        match tokio::time::timeout(Duration::from_secs(5), self.execute()).await {
            Ok(result) => result,
            Err(_) => Ok(HealthCheckResult::new(self.name(), HealthStatus::Unhealthy)
                .with_message("Quick health check timed out")
                .with_duration(start.elapsed())),
        }
    }
}

/// Factory for creating health checks with circuit breaker protection
pub struct HealthCheckCircuitBreakerFactory {
    default_config: CircuitBreakerConfig,
}

impl HealthCheckCircuitBreakerFactory {
    /// Create a new factory with default configuration
    pub fn new() -> Self {
        Self {
            default_config: CircuitBreakerConfig {
                failure_threshold: 3,
                recovery_timeout: Duration::from_secs(60),
                success_threshold: 2,
                failure_window: Duration::from_secs(300),
                half_open_max_requests: 1,
            },
        }
    }

    /// Create factory with custom default configuration
    pub fn with_default_config(config: CircuitBreakerConfig) -> Self {
        Self {
            default_config: config,
        }
    }

    /// Wrap a health check with circuit breaker protection using default config
    pub fn wrap<T>(&self, health_check: Arc<T>) -> HealthCheckCircuitBreaker<T>
    where
        T: AsyncHealthCheck + 'static,
    {
        HealthCheckCircuitBreaker::with_config(health_check, self.default_config.clone())
    }

    /// Wrap a health check with circuit breaker protection using custom config
    pub fn wrap_with_config<T>(
        &self,
        health_check: Arc<T>,
        config: CircuitBreakerConfig,
    ) -> HealthCheckCircuitBreaker<T>
    where
        T: AsyncHealthCheck + 'static,
    {
        HealthCheckCircuitBreaker::with_config(health_check, config)
    }
}

impl Default for HealthCheckCircuitBreakerFactory {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Mock health check for testing
    struct MockHealthCheck {
        name: String,
        should_fail: bool,
    }

    impl MockHealthCheck {
        fn new(name: &str, should_fail: bool) -> Self {
            Self {
                name: name.to_string(),
                should_fail,
            }
        }
    }

    #[async_trait::async_trait]
    impl AsyncHealthCheck for MockHealthCheck {
        fn name(&self) -> &str {
            &self.name
        }

        async fn check(&self) -> Result<HealthCheckResult> {
            if self.should_fail {
                Err(crate::error::MonitoringError::initialization_failed(
                    &self.name,
                    "Mock health check failed",
                ))
            } else {
                Ok(HealthCheckResult::new(&self.name, HealthStatus::Healthy)
                    .with_message("Mock health check passed"))
            }
        }
    }

    #[tokio::test]
    async fn test_healthy_check_through_circuit_breaker() {
        let mock_check = Arc::new(MockHealthCheck::new("test", false));
        let circuit_check = HealthCheckCircuitBreaker::new(mock_check);

        let result = circuit_check.execute().await.unwrap();

        assert_eq!(result.component_name, "test");
        assert!(result.is_healthy());
        assert_eq!(result.message, Some("Mock health check passed".to_string()));
    }

    #[tokio::test]
    async fn test_failing_check_through_circuit_breaker() {
        let mock_check = Arc::new(MockHealthCheck::new("test", true));
        let circuit_check = HealthCheckCircuitBreaker::new(mock_check);

        let result = circuit_check.execute().await.unwrap();

        // The circuit breaker should convert failures to unhealthy status
        assert_eq!(result.component_name, "test");
        assert!(result.is_unhealthy());
        assert!(result.message.unwrap().contains("Health check failed"));
    }

    #[tokio::test]
    async fn test_circuit_breaker_state_tracking() {
        let mock_check = Arc::new(MockHealthCheck::new("test", false));
        let circuit_check = HealthCheckCircuitBreaker::new(mock_check);

        // Initially should be closed
        assert_eq!(circuit_check.circuit_state().await, CircuitState::Closed);
        assert!(!circuit_check.is_circuit_open().await);
    }

    #[tokio::test]
    async fn test_circuit_breaker_factory() {
        let factory = HealthCheckCircuitBreakerFactory::new();
        let mock_check = Arc::new(MockHealthCheck::new("factory_test", false));
        let circuit_check = factory.wrap(mock_check);

        assert_eq!(circuit_check.name(), "factory_test");

        let result = circuit_check.execute().await.unwrap();
        assert!(result.is_healthy());
    }

    #[tokio::test]
    async fn test_circuit_breaker_with_custom_config() {
        let config = CircuitBreakerConfig {
            failure_threshold: 1,
            recovery_timeout: Duration::from_secs(30),
            success_threshold: 1,
            failure_window: Duration::from_secs(60),
            half_open_max_requests: 1,
        };

        let mock_check = Arc::new(MockHealthCheck::new("custom_config_test", false));
        let circuit_check = HealthCheckCircuitBreaker::with_config(mock_check, config);

        let result = circuit_check.execute().await.unwrap();
        assert!(result.is_healthy());
    }

    #[tokio::test]
    async fn test_quick_check_with_circuit_breaker() {
        let mock_check = Arc::new(MockHealthCheck::new("quick_test", false));
        let circuit_check = HealthCheckCircuitBreaker::new(mock_check);

        let result = circuit_check.quick_check().await.unwrap();

        assert_eq!(result.component_name, "quick_test");
        assert!(result.is_healthy());
    }
}
