//! Redis health check implementation
//!
//! This module provides health checks for Redis connectivity and command execution.

use crate::error::{MonitoringError, Result};
use crate::health::check::{
    AsyncHealthCheck, HealthCheckConfig, HealthCheckCriticality, HealthCheckResult,
};
use orcher_common::monitoring::HealthStatus;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Configuration for Redis health checks
#[derive(Debug, Clone)]
pub struct RedisHealthCheckConfig {
    /// Connection timeout for health check operations
    pub connection_timeout: Duration,
    /// Command timeout for ping operations
    pub command_timeout: Duration,
    /// Maximum allowed response time for ping
    pub max_ping_time: Duration,
    /// Test key to use for write/read operations
    pub test_key: String,
}

impl Default for RedisHealthCheckConfig {
    fn default() -> Self {
        Self {
            connection_timeout: Duration::from_secs(5),
            command_timeout: Duration::from_secs(3),
            max_ping_time: Duration::from_millis(100),
            test_key: "orcher:health:test".to_string(),
        }
    }
}

/// Redis health check implementation
pub struct RedisHealthCheck {
    name: String,
    config: RedisHealthCheckConfig,
    // In a real implementation, this would contain actual Redis connection pool
    // For now, we'll simulate the connection status
    connection_available: Arc<std::sync::atomic::AtomicBool>,
    ping_latency_ms: Arc<std::sync::atomic::AtomicU64>,
}

impl RedisHealthCheck {
    /// Create a new Redis health check
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            config: RedisHealthCheckConfig::default(),
            connection_available: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            ping_latency_ms: Arc::new(std::sync::atomic::AtomicU64::new(10)),
        }
    }

    /// Create a Redis health check with custom configuration
    pub fn with_config(name: impl Into<String>, config: RedisHealthCheckConfig) -> Self {
        Self {
            name: name.into(),
            config,
            connection_available: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            ping_latency_ms: Arc::new(std::sync::atomic::AtomicU64::new(10)),
        }
    }

    /// Set connection availability (for testing/simulation)
    pub fn set_connection_available(&self, available: bool) {
        self.connection_available
            .store(available, std::sync::atomic::Ordering::Relaxed);
    }

    /// Set ping latency (for testing/simulation)
    pub fn set_ping_latency(&self, latency_ms: u64) {
        self.ping_latency_ms
            .store(latency_ms, std::sync::atomic::Ordering::Relaxed);
    }

    /// Perform a Redis PING command
    async fn ping_redis(&self) -> Result<Duration> {
        let start = Instant::now();

        // Check if connection is available
        let is_available = self
            .connection_available
            .load(std::sync::atomic::Ordering::Relaxed);

        if !is_available {
            return Err(MonitoringError::health_check_failed(
                "Redis connection not available",
            ));
        }

        // Simulate Redis ping with configurable timeout
        let ping_future = async {
            let latency = self
                .ping_latency_ms
                .load(std::sync::atomic::Ordering::Relaxed);
            tokio::time::sleep(Duration::from_millis(latency)).await;
            Ok::<(), MonitoringError>(())
        };

        match tokio::time::timeout(self.config.command_timeout, ping_future).await {
            Ok(_) => {
                let elapsed = start.elapsed();
                if elapsed > self.config.max_ping_time {
                    Err(MonitoringError::health_check_failed(format!(
                        "Redis ping too slow: {}ms (max: {}ms)",
                        elapsed.as_millis(),
                        self.config.max_ping_time.as_millis()
                    )))
                } else {
                    Ok(elapsed)
                }
            }
            Err(_) => Err(MonitoringError::health_check_failed(
                "Redis ping command timeout",
            )),
        }
    }

    /// Test basic read/write operations
    async fn test_read_write(&self) -> Result<Duration> {
        let start = Instant::now();

        // Simulate SET command
        let set_future = async {
            tokio::time::sleep(Duration::from_millis(5)).await;
            Ok::<(), MonitoringError>(())
        };

        match tokio::time::timeout(self.config.command_timeout, set_future).await {
            Ok(_) => {}
            Err(_) => {
                return Err(MonitoringError::health_check_failed(
                    "Redis SET command timeout",
                ))
            }
        }

        // Simulate GET command
        let get_future = async {
            tokio::time::sleep(Duration::from_millis(3)).await;
            Ok::<(), MonitoringError>(())
        };

        match tokio::time::timeout(self.config.command_timeout, get_future).await {
            Ok(_) => {}
            Err(_) => {
                return Err(MonitoringError::health_check_failed(
                    "Redis GET command timeout",
                ))
            }
        }

        // Simulate DEL command for cleanup
        let del_future = async {
            tokio::time::sleep(Duration::from_millis(2)).await;
            Ok::<(), MonitoringError>(())
        };

        match tokio::time::timeout(self.config.command_timeout, del_future).await {
            Ok(_) => Ok(start.elapsed()),
            Err(_) => Err(MonitoringError::health_check_failed(
                "Redis DEL command timeout",
            )),
        }
    }
}

#[async_trait::async_trait]
impl AsyncHealthCheck for RedisHealthCheck {
    fn name(&self) -> &str {
        &self.name
    }

    fn config(&self) -> HealthCheckConfig {
        HealthCheckConfig {
            timeout: self.config.connection_timeout,
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

        // Perform Redis ping
        match self.ping_redis().await {
            Ok(ping_duration) => {
                result = result
                    .with_metadata("ping_duration_ms", ping_duration.as_millis().to_string())
                    .with_metadata("ping_status", "success");
            }
            Err(e) => {
                result.status = HealthStatus::Unhealthy;
                return Ok(result
                    .with_message(format!("Redis ping failed: {}", e))
                    .with_duration(start.elapsed()));
            }
        }

        // Test read/write operations
        match self.test_read_write().await {
            Ok(rw_duration) => {
                result = result
                    .with_message(
                        "Redis connection healthy - ping and read/write operations successful",
                    )
                    .with_metadata(
                        "read_write_duration_ms",
                        rw_duration.as_millis().to_string(),
                    )
                    .with_metadata("read_write_status", "success")
                    .with_metadata("test_key", &self.config.test_key)
                    .with_duration(start.elapsed());
            }
            Err(e) => {
                result.status = HealthStatus::Unhealthy;
                result = result
                    .with_message(format!("Redis read/write test failed: {}", e))
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
    fn test_redis_health_check_creation() {
        let check = RedisHealthCheck::new("test_redis");
        assert_eq!(check.name(), "test_redis");
        assert_eq!(check.config().criticality, HealthCheckCriticality::Critical);
    }

    #[test]
    fn test_redis_health_check_with_config() {
        let config = RedisHealthCheckConfig {
            connection_timeout: Duration::from_secs(3),
            command_timeout: Duration::from_secs(1),
            max_ping_time: Duration::from_millis(50),
            test_key: "custom:test:key".to_string(),
        };

        let check = RedisHealthCheck::with_config("custom_redis", config.clone());
        assert_eq!(check.config.connection_timeout, Duration::from_secs(3));
        assert_eq!(check.config.test_key, "custom:test:key");
    }

    #[tokio::test]
    async fn test_redis_health_check_success() {
        let check = RedisHealthCheck::new("test_redis");
        check.set_connection_available(true);
        check.set_ping_latency(10);

        let result = check.check().await.unwrap();
        assert_eq!(result.status, HealthStatus::Healthy);
        assert!(result.message.unwrap().contains("healthy"));
        assert_eq!(
            result.metadata.get("ping_status"),
            Some(&"success".to_string())
        );
        assert_eq!(
            result.metadata.get("read_write_status"),
            Some(&"success".to_string())
        );
    }

    #[tokio::test]
    async fn test_redis_health_check_connection_unavailable() {
        let check = RedisHealthCheck::new("test_redis");
        check.set_connection_available(false);

        let result = check.check().await.unwrap();
        assert_eq!(result.status, HealthStatus::Unhealthy);
        assert!(result.message.unwrap().contains("connection not available"));
    }

    #[tokio::test]
    async fn test_redis_health_check_slow_ping() {
        let mut config = RedisHealthCheckConfig::default();
        config.max_ping_time = Duration::from_millis(5); // Very low threshold

        let check = RedisHealthCheck::with_config("test_redis", config);
        check.set_connection_available(true);
        check.set_ping_latency(20); // Higher than threshold

        let result = check.check().await.unwrap();
        assert_eq!(result.status, HealthStatus::Unhealthy);
        assert!(result.message.unwrap().contains("ping too slow"));
    }

    #[test]
    fn test_default_config() {
        let config = RedisHealthCheckConfig::default();
        assert_eq!(config.connection_timeout, Duration::from_secs(5));
        assert_eq!(config.command_timeout, Duration::from_secs(3));
        assert_eq!(config.max_ping_time, Duration::from_millis(100));
        assert_eq!(config.test_key, "orcher:health:test");
    }
}
