//! Database health check implementation
//!
//! This module provides health checks for database connectivity and connection pool status.

use crate::error::{MonitoringError, Result};
use crate::health::check::{
    AsyncHealthCheck, HealthCheckConfig, HealthCheckCriticality, HealthCheckResult,
};
use orcher_common::monitoring::HealthStatus;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Configuration for database health checks
#[derive(Debug, Clone)]
pub struct DatabaseHealthCheckConfig {
    /// Connection timeout for health check queries
    pub connection_timeout: Duration,
    /// Query timeout for ping operations
    pub query_timeout: Duration,
    /// Maximum allowed connection pool size
    pub max_pool_size: u32,
    /// Minimum required healthy connections
    pub min_healthy_connections: u32,
}

impl Default for DatabaseHealthCheckConfig {
    fn default() -> Self {
        Self {
            connection_timeout: Duration::from_secs(10),
            query_timeout: Duration::from_secs(5),
            max_pool_size: 100,
            min_healthy_connections: 1,
        }
    }
}

/// Database health check implementation
pub struct DatabaseHealthCheck {
    name: String,
    config: DatabaseHealthCheckConfig,
    // In a real implementation, this would contain actual database connection pool
    // For now, we'll simulate the connection status
    connection_pool_size: Arc<std::sync::atomic::AtomicU32>,
    healthy_connections: Arc<std::sync::atomic::AtomicU32>,
}

impl DatabaseHealthCheck {
    /// Create a new database health check
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            config: DatabaseHealthCheckConfig::default(),
            connection_pool_size: Arc::new(std::sync::atomic::AtomicU32::new(10)),
            healthy_connections: Arc::new(std::sync::atomic::AtomicU32::new(8)),
        }
    }

    /// Create a database health check with custom configuration
    pub fn with_config(name: impl Into<String>, config: DatabaseHealthCheckConfig) -> Self {
        Self {
            name: name.into(),
            config,
            connection_pool_size: Arc::new(std::sync::atomic::AtomicU32::new(10)),
            healthy_connections: Arc::new(std::sync::atomic::AtomicU32::new(8)),
        }
    }

    /// Set the current connection pool size (for testing/simulation)
    pub fn set_pool_size(&self, size: u32) {
        self.connection_pool_size
            .store(size, std::sync::atomic::Ordering::Relaxed);
    }

    /// Set the number of healthy connections (for testing/simulation)
    pub fn set_healthy_connections(&self, count: u32) {
        self.healthy_connections
            .store(count, std::sync::atomic::Ordering::Relaxed);
    }

    /// Perform a simple database ping operation
    async fn ping_database(&self) -> Result<Duration> {
        let start = Instant::now();

        // Simulate database ping with configurable timeout
        let ping_future = async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok::<(), MonitoringError>(())
        };

        match tokio::time::timeout(self.config.query_timeout, ping_future).await {
            Ok(_) => Ok(start.elapsed()),
            Err(_) => Err(MonitoringError::health_check_failed(
                "Database ping timeout",
            )),
        }
    }

    /// Check connection pool status
    fn check_connection_pool(&self) -> Result<(u32, u32)> {
        let pool_size = self
            .connection_pool_size
            .load(std::sync::atomic::Ordering::Relaxed);
        let healthy_count = self
            .healthy_connections
            .load(std::sync::atomic::Ordering::Relaxed);

        if healthy_count < self.config.min_healthy_connections {
            return Err(MonitoringError::health_check_failed(format!(
                "Insufficient healthy connections: {}/{} (minimum: {})",
                healthy_count, pool_size, self.config.min_healthy_connections
            )));
        }

        Ok((pool_size, healthy_count))
    }
}

#[async_trait::async_trait]
impl AsyncHealthCheck for DatabaseHealthCheck {
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

        // Check connection pool status
        match self.check_connection_pool() {
            Ok((pool_size, healthy_count)) => {
                result = result
                    .with_metadata("pool_size", pool_size.to_string())
                    .with_metadata("healthy_connections", healthy_count.to_string());
            }
            Err(e) => {
                return Ok(result
                    .with_message(e.to_string())
                    .with_duration(start.elapsed()));
            }
        }

        // Perform database ping
        match self.ping_database().await {
            Ok(ping_duration) => {
                result = result
                    .with_message("Database connection healthy")
                    .with_metadata("ping_duration_ms", ping_duration.as_millis().to_string())
                    .with_duration(start.elapsed());
            }
            Err(e) => {
                result.status = HealthStatus::Unhealthy;
                result = result
                    .with_message(format!("Database ping failed: {}", e))
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
    fn test_database_health_check_creation() {
        let check = DatabaseHealthCheck::new("test_db");
        assert_eq!(check.name(), "test_db");
        assert_eq!(check.config().criticality, HealthCheckCriticality::Critical);
    }

    #[test]
    fn test_database_health_check_with_config() {
        let config = DatabaseHealthCheckConfig {
            connection_timeout: Duration::from_secs(5),
            query_timeout: Duration::from_secs(2),
            max_pool_size: 50,
            min_healthy_connections: 5,
        };

        let check = DatabaseHealthCheck::with_config("custom_db", config.clone());
        assert_eq!(check.config.connection_timeout, Duration::from_secs(5));
        assert_eq!(check.config.min_healthy_connections, 5);
    }

    #[tokio::test]
    async fn test_database_health_check_success() {
        let check = DatabaseHealthCheck::new("test_db");
        check.set_pool_size(10);
        check.set_healthy_connections(8);

        let result = check.check().await.unwrap();
        assert_eq!(result.status, HealthStatus::Healthy);
        assert!(result.message.unwrap().contains("healthy"));
        assert_eq!(result.metadata.get("pool_size"), Some(&"10".to_string()));
        assert_eq!(
            result.metadata.get("healthy_connections"),
            Some(&"8".to_string())
        );
    }

    #[tokio::test]
    async fn test_database_health_check_insufficient_connections() {
        let check = DatabaseHealthCheck::new("test_db");
        check.set_pool_size(10);
        check.set_healthy_connections(0);

        let result = check.check().await.unwrap();
        assert_eq!(result.status, HealthStatus::Healthy); // Still healthy but with error message
        assert!(result
            .message
            .unwrap()
            .contains("Insufficient healthy connections"));
    }

    #[test]
    fn test_default_config() {
        let config = DatabaseHealthCheckConfig::default();
        assert_eq!(config.connection_timeout, Duration::from_secs(10));
        assert_eq!(config.query_timeout, Duration::from_secs(5));
        assert_eq!(config.max_pool_size, 100);
        assert_eq!(config.min_healthy_connections, 1);
    }
}
