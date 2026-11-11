//! Connection management for gRPC clients
//!
//! This module provides connection pooling, retry policies, timeouts,
//! circuit breakers, and load balancing for gRPC client connections.

use std::time::Duration;

/// Retry policy module
pub mod retry;

/// Circuit breaker module
pub mod circuit_breaker;

/// Configuration for gRPC client connections
#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    /// Maximum number of concurrent connections in the pool
    pub max_connections: usize,

    /// Timeout for establishing a connection
    pub connect_timeout: Duration,

    /// Timeout for individual requests
    pub request_timeout: Duration,

    /// Maximum number of retry attempts
    pub max_retries: u32,

    /// Initial backoff duration for retries
    pub initial_backoff: Duration,

    /// Maximum backoff duration for retries
    pub max_backoff: Duration,

    /// Circuit breaker failure threshold
    pub circuit_breaker_threshold: u32,

    /// Circuit breaker timeout before attempting to close
    pub circuit_breaker_timeout: Duration,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            max_connections: 100,
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
            max_retries: 3,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(10),
            circuit_breaker_threshold: 5,
            circuit_breaker_timeout: Duration::from_secs(60),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ConnectionConfig::default();
        assert_eq!(config.max_connections, 100);
        assert_eq!(config.connect_timeout, Duration::from_secs(5));
        assert_eq!(config.request_timeout, Duration::from_secs(30));
        assert_eq!(config.max_retries, 3);
    }

    #[test]
    fn test_custom_config() {
        let config = ConnectionConfig {
            max_connections: 50,
            connect_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(60),
            max_retries: 5,
            initial_backoff: Duration::from_millis(200),
            max_backoff: Duration::from_secs(20),
            circuit_breaker_threshold: 10,
            circuit_breaker_timeout: Duration::from_secs(120),
        };
        assert_eq!(config.max_connections, 50);
        assert_eq!(config.max_retries, 5);
    }
}
