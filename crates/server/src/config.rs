//! Configuration module for the Orcher server
//!
//! This module provides configuration structures for various server components,
//! including orchestrator connection settings, retry policies, and circuit breaker configuration.

use crate::error::{Result, ServerError};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Orchestrator service configuration
///
/// Configures the connection and behavior for the gRPC orchestrator client.
///
/// # Environment Variables
///
/// - `ORCHESTRATOR_ENDPOINT` - gRPC endpoint URL (default: <http://localhost:50051>)
/// - `ORCHESTRATOR_NAMESPACE` - Workflow namespace (default: "default")
/// - `ORCHESTRATOR_CONNECT_TIMEOUT` - Connection timeout in seconds (default: 5)
/// - `ORCHESTRATOR_REQUEST_TIMEOUT` - Request timeout in seconds (default: 30)
/// - `ORCHESTRATOR_MAX_RETRIES` - Maximum retry attempts (default: 3)
/// - `ORCHESTRATOR_RETRY_DELAY_MS` - Delay between retries in milliseconds (default: 1000)
/// - `ORCHESTRATOR_CIRCUIT_BREAKER_THRESHOLD` - Circuit breaker failure threshold (default: 5)
/// - `ORCHESTRATOR_CIRCUIT_BREAKER_TIMEOUT` - Circuit breaker timeout in seconds (default: 60)
///
/// # Example
///
/// ```rust,no_run
/// use orcher_server::config::OrchestratorConfig;
///
/// let config = OrchestratorConfig::from_env().unwrap();
/// println!("Orchestrator endpoint: {}", config.endpoint);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrchestratorConfig {
    /// gRPC endpoint URL (e.g., "<http://localhost:50051>")
    pub endpoint: String,

    /// Workflow namespace for multi-tenancy
    pub namespace: String,

    /// Connection timeout duration
    pub connect_timeout: Duration,

    /// Request timeout duration
    pub request_timeout: Duration,

    /// Maximum number of retry attempts
    pub max_retries: u32,

    /// Delay between retry attempts
    pub retry_delay: Duration,

    /// Circuit breaker configuration
    pub circuit_breaker: CircuitBreakerConfig,
}

impl OrchestratorConfig {
    /// Create configuration from environment variables
    ///
    /// # Errors
    ///
    /// Returns an error if required environment variables are invalid
    pub fn from_env() -> Result<Self> {
        Self::from_vars(|name| std::env::var(name).ok())
    }

    /// Read the configuration through `var`, which returns a variable's value
    /// or `None` when it is unset. `from_env` passes the process environment;
    /// tests pass their own values, since the process environment is shared by
    /// every test running at once.
    fn from_vars(var: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let endpoint =
            var("ORCHESTRATOR_ENDPOINT").unwrap_or_else(|| "http://localhost:50051".to_string());

        let namespace = var("ORCHESTRATOR_NAMESPACE").unwrap_or_else(|| "default".to_string());

        let connect_timeout_secs = var("ORCHESTRATOR_CONNECT_TIMEOUT")
            .and_then(|s| s.parse().ok())
            .unwrap_or(5);

        let request_timeout_secs = var("ORCHESTRATOR_REQUEST_TIMEOUT")
            .and_then(|s| s.parse().ok())
            .unwrap_or(30);

        let max_retries = var("ORCHESTRATOR_MAX_RETRIES")
            .and_then(|s| s.parse().ok())
            .unwrap_or(3);

        let retry_delay_ms = var("ORCHESTRATOR_RETRY_DELAY_MS")
            .and_then(|s| s.parse().ok())
            .unwrap_or(1000);

        let circuit_breaker_threshold = var("ORCHESTRATOR_CIRCUIT_BREAKER_THRESHOLD")
            .and_then(|s| s.parse().ok())
            .unwrap_or(5);

        let circuit_breaker_timeout_secs = var("ORCHESTRATOR_CIRCUIT_BREAKER_TIMEOUT")
            .and_then(|s| s.parse().ok())
            .unwrap_or(60);

        Ok(Self {
            endpoint,
            namespace,
            connect_timeout: Duration::from_secs(connect_timeout_secs),
            request_timeout: Duration::from_secs(request_timeout_secs),
            max_retries,
            retry_delay: Duration::from_millis(retry_delay_ms),
            circuit_breaker: CircuitBreakerConfig {
                failure_threshold: circuit_breaker_threshold,
                timeout: Duration::from_secs(circuit_breaker_timeout_secs),
            },
        })
    }

    /// Create a default development configuration
    ///
    /// Uses localhost endpoint and relaxed timeouts for local development.
    #[must_use]
    pub fn development() -> Self {
        Self {
            endpoint: "http://localhost:50051".to_string(),
            namespace: "default".to_string(),
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
            max_retries: 3,
            retry_delay: Duration::from_millis(1000),
            circuit_breaker: CircuitBreakerConfig {
                failure_threshold: 10, // More lenient for development
                timeout: Duration::from_secs(60),
            },
        }
    }

    /// Create a production configuration with stricter settings
    #[must_use]
    pub fn production() -> Self {
        Self {
            endpoint: std::env::var("ORCHESTRATOR_ENDPOINT")
                .expect("ORCHESTRATOR_ENDPOINT must be set in production"),
            namespace: std::env::var("ORCHESTRATOR_NAMESPACE")
                .unwrap_or_else(|_| "default".to_string()),
            connect_timeout: Duration::from_secs(3),
            request_timeout: Duration::from_secs(15),
            max_retries: 2,
            retry_delay: Duration::from_millis(500),
            circuit_breaker: CircuitBreakerConfig {
                failure_threshold: 5,
                timeout: Duration::from_secs(30),
            },
        }
    }

    /// Validate the configuration
    ///
    /// # Errors
    ///
    /// Returns an error if the configuration is invalid
    pub fn validate(&self) -> Result<()> {
        if self.endpoint.is_empty() {
            return Err(ServerError::config("Orchestrator endpoint cannot be empty"));
        }

        if !self.endpoint.starts_with("http://") && !self.endpoint.starts_with("https://") {
            return Err(ServerError::config(
                "Orchestrator endpoint must start with http:// or https://",
            ));
        }

        if self.max_retries > 10 {
            return Err(ServerError::config("Maximum retries cannot exceed 10"));
        }

        if self.connect_timeout.as_secs() > 30 {
            return Err(ServerError::config(
                "Connection timeout cannot exceed 30 seconds",
            ));
        }

        Ok(())
    }
}

impl Default for OrchestratorConfig {
    fn default() -> Self {
        Self::development()
    }
}

/// Circuit breaker configuration
///
/// Prevents cascading failures by opening the circuit after a threshold of failures.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircuitBreakerConfig {
    /// Number of consecutive failures before opening the circuit
    pub failure_threshold: u32,

    /// Duration to wait before attempting to close the circuit
    pub timeout: Duration,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = OrchestratorConfig::default();
        assert_eq!(config.endpoint, "http://localhost:50051");
        assert_eq!(config.max_retries, 3);
    }

    #[test]
    fn test_development_config() {
        let config = OrchestratorConfig::development();
        assert_eq!(config.endpoint, "http://localhost:50051");
        assert_eq!(config.connect_timeout, Duration::from_secs(5));
        assert_eq!(config.circuit_breaker.failure_threshold, 10);
    }

    #[test]
    fn test_config_validation() {
        let mut config = OrchestratorConfig::default();
        assert!(config.validate().is_ok());

        // Test invalid endpoint
        config.endpoint = String::new();
        assert!(config.validate().is_err());

        config.endpoint = "invalid-url".to_string();
        assert!(config.validate().is_err());

        // Test invalid max retries
        config.endpoint = "http://localhost:50051".to_string();
        config.max_retries = 100;
        assert!(config.validate().is_err());
    }

    /// A lookup over `pairs`, standing in for the process environment.
    fn vars(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let pairs: Vec<(String, String)> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |name| {
            pairs
                .iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.clone())
        }
    }

    #[test]
    fn test_from_env_with_defaults() {
        let config = OrchestratorConfig::from_vars(vars(&[])).unwrap();
        assert_eq!(config.endpoint, "http://localhost:50051");
        assert_eq!(config.connect_timeout, Duration::from_secs(5));
    }

    #[test]
    fn test_from_env_with_custom_values() {
        let config = OrchestratorConfig::from_vars(vars(&[
            ("ORCHESTRATOR_ENDPOINT", "http://orchestrator:50051"),
            ("ORCHESTRATOR_CONNECT_TIMEOUT", "10"),
            ("ORCHESTRATOR_MAX_RETRIES", "5"),
        ]))
        .unwrap();
        assert_eq!(config.endpoint, "http://orchestrator:50051");
        assert_eq!(config.connect_timeout, Duration::from_secs(10));
        assert_eq!(config.max_retries, 5);
    }
}
