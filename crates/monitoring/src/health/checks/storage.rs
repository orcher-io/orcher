//! Storage health check implementation
//!
//! This module provides health checks for storage systems including disk space
//! monitoring and connection pool status.

use crate::error::{MonitoringError, Result};
use crate::health::check::{
    AsyncHealthCheck, HealthCheckConfig, HealthCheckCriticality, HealthCheckResult,
};
use orcher_common::monitoring::HealthStatus;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Configuration for storage health checks
#[derive(Debug, Clone)]
pub struct StorageHealthCheckConfig {
    /// Timeout for storage operations
    pub operation_timeout: Duration,
    /// Minimum required free disk space in bytes
    pub min_free_disk_bytes: u64,
    /// Minimum required free disk space as percentage (0-100)
    pub min_free_disk_percent: f64,
    /// Maximum allowed connection pool size
    pub max_pool_size: u32,
    /// Minimum required healthy connections
    pub min_healthy_connections: u32,
    /// Storage path to monitor
    pub storage_path: String,
}

impl Default for StorageHealthCheckConfig {
    fn default() -> Self {
        Self {
            operation_timeout: Duration::from_secs(10),
            min_free_disk_bytes: 1024 * 1024 * 1024, // 1GB
            min_free_disk_percent: 10.0,
            max_pool_size: 100,
            min_healthy_connections: 2,
            storage_path: "/tmp".to_string(),
        }
    }
}

/// Storage health check implementation
pub struct StorageHealthCheck {
    name: String,
    config: StorageHealthCheckConfig,
    // In a real implementation, these would interface with actual storage systems
    // For now, we'll simulate the storage status
    connection_pool_size: Arc<std::sync::atomic::AtomicU32>,
    healthy_connections: Arc<std::sync::atomic::AtomicU32>,
    available_disk_bytes: Arc<std::sync::atomic::AtomicU64>,
    total_disk_bytes: Arc<std::sync::atomic::AtomicU64>,
}

impl StorageHealthCheck {
    /// Create a new storage health check
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            config: StorageHealthCheckConfig::default(),
            connection_pool_size: Arc::new(std::sync::atomic::AtomicU32::new(20)),
            healthy_connections: Arc::new(std::sync::atomic::AtomicU32::new(18)),
            available_disk_bytes: Arc::new(std::sync::atomic::AtomicU64::new(
                5 * 1024 * 1024 * 1024,
            )), // 5GB
            total_disk_bytes: Arc::new(std::sync::atomic::AtomicU64::new(50 * 1024 * 1024 * 1024)), // 50GB
        }
    }

    /// Create a storage health check with custom configuration
    pub fn with_config(name: impl Into<String>, config: StorageHealthCheckConfig) -> Self {
        Self {
            name: name.into(),
            config,
            connection_pool_size: Arc::new(std::sync::atomic::AtomicU32::new(20)),
            healthy_connections: Arc::new(std::sync::atomic::AtomicU32::new(18)),
            available_disk_bytes: Arc::new(std::sync::atomic::AtomicU64::new(
                5 * 1024 * 1024 * 1024,
            )),
            total_disk_bytes: Arc::new(std::sync::atomic::AtomicU64::new(50 * 1024 * 1024 * 1024)),
        }
    }

    /// Set connection pool size (for testing/simulation)
    pub fn set_pool_size(&self, size: u32) {
        self.connection_pool_size
            .store(size, std::sync::atomic::Ordering::Relaxed);
    }

    /// Set healthy connections count (for testing/simulation)
    pub fn set_healthy_connections(&self, count: u32) {
        self.healthy_connections
            .store(count, std::sync::atomic::Ordering::Relaxed);
    }

    /// Set available disk space (for testing/simulation)
    pub fn set_disk_space(&self, available_bytes: u64, total_bytes: u64) {
        self.available_disk_bytes
            .store(available_bytes, std::sync::atomic::Ordering::Relaxed);
        self.total_disk_bytes
            .store(total_bytes, std::sync::atomic::Ordering::Relaxed);
    }

    /// Check disk space availability
    fn check_disk_space(&self) -> Result<(u64, u64, f64)> {
        let available = self
            .available_disk_bytes
            .load(std::sync::atomic::Ordering::Relaxed);
        let total = self
            .total_disk_bytes
            .load(std::sync::atomic::Ordering::Relaxed);

        if total == 0 {
            return Err(MonitoringError::health_check_failed(
                "Unable to determine disk space",
            ));
        }

        let percent_free = (available as f64 / total as f64) * 100.0;

        // Check absolute minimum
        if available < self.config.min_free_disk_bytes {
            return Err(MonitoringError::health_check_failed(format!(
                "Insufficient disk space: {} bytes available (minimum: {} bytes)",
                available, self.config.min_free_disk_bytes
            )));
        }

        // Check percentage minimum
        if percent_free < self.config.min_free_disk_percent {
            return Err(MonitoringError::health_check_failed(format!(
                "Insufficient disk space: {:.1}% free (minimum: {:.1}%)",
                percent_free, self.config.min_free_disk_percent
            )));
        }

        Ok((available, total, percent_free))
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
                "Insufficient healthy storage connections: {}/{} (minimum: {})",
                healthy_count, pool_size, self.config.min_healthy_connections
            )));
        }

        if pool_size > self.config.max_pool_size {
            return Err(MonitoringError::health_check_failed(format!(
                "Storage connection pool size exceeded: {} (maximum: {})",
                pool_size, self.config.max_pool_size
            )));
        }

        Ok((pool_size, healthy_count))
    }

    /// Test basic storage operations
    async fn test_storage_operations(&self) -> Result<Duration> {
        let start = Instant::now();

        // Simulate write operation
        let write_future = async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            Ok::<(), MonitoringError>(())
        };

        match tokio::time::timeout(self.config.operation_timeout, write_future).await {
            Ok(_) => {}
            Err(_) => {
                return Err(MonitoringError::health_check_failed(
                    "Storage write operation timeout",
                ))
            }
        }

        // Simulate read operation
        let read_future = async {
            tokio::time::sleep(Duration::from_millis(5)).await;
            Ok::<(), MonitoringError>(())
        };

        match tokio::time::timeout(self.config.operation_timeout, read_future).await {
            Ok(_) => Ok(start.elapsed()),
            Err(_) => Err(MonitoringError::health_check_failed(
                "Storage read operation timeout",
            )),
        }
    }
}

#[async_trait::async_trait]
impl AsyncHealthCheck for StorageHealthCheck {
    fn name(&self) -> &str {
        &self.name
    }

    fn config(&self) -> HealthCheckConfig {
        HealthCheckConfig {
            timeout: self.config.operation_timeout,
            interval: Duration::from_secs(60),
            criticality: HealthCheckCriticality::Critical,
            failure_threshold: 2,
            success_threshold: 1,
            circuit_breaker_enabled: true,
        }
    }

    async fn check(&self) -> Result<HealthCheckResult> {
        let start = Instant::now();
        let mut result = HealthCheckResult::new(&self.name, HealthStatus::Healthy);

        // Check disk space
        match self.check_disk_space() {
            Ok((available, total, percent_free)) => {
                result = result
                    .with_metadata("available_bytes", available.to_string())
                    .with_metadata("total_bytes", total.to_string())
                    .with_metadata("percent_free", format!("{:.1}", percent_free))
                    .with_metadata("storage_path", &self.config.storage_path);
            }
            Err(e) => {
                result.status = HealthStatus::Unhealthy;
                return Ok(result
                    .with_message(format!("Disk space check failed: {}", e))
                    .with_duration(start.elapsed()));
            }
        }

        // Check connection pool
        match self.check_connection_pool() {
            Ok((pool_size, healthy_count)) => {
                result = result
                    .with_metadata("pool_size", pool_size.to_string())
                    .with_metadata("healthy_connections", healthy_count.to_string());
            }
            Err(e) => {
                result.status = HealthStatus::Unhealthy;
                return Ok(result
                    .with_message(format!("Connection pool check failed: {}", e))
                    .with_duration(start.elapsed()));
            }
        }

        // Test storage operations
        match self.test_storage_operations().await {
            Ok(op_duration) => {
                result = result
                    .with_message(
                        "Storage system healthy - disk space, connections, and operations OK",
                    )
                    .with_metadata("operation_duration_ms", op_duration.as_millis().to_string())
                    .with_metadata("operation_status", "success")
                    .with_duration(start.elapsed());
            }
            Err(e) => {
                result.status = HealthStatus::Unhealthy;
                result = result
                    .with_message(format!("Storage operation test failed: {}", e))
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
    fn test_storage_health_check_creation() {
        let check = StorageHealthCheck::new("test_storage");
        assert_eq!(check.name(), "test_storage");
        assert_eq!(check.config().criticality, HealthCheckCriticality::Critical);
    }

    #[test]
    fn test_storage_health_check_with_config() {
        let config = StorageHealthCheckConfig {
            operation_timeout: Duration::from_secs(5),
            min_free_disk_bytes: 2 * 1024 * 1024 * 1024, // 2GB
            min_free_disk_percent: 15.0,
            max_pool_size: 50,
            min_healthy_connections: 5,
            storage_path: "/data".to_string(),
        };

        let check = StorageHealthCheck::with_config("custom_storage", config.clone());
        assert_eq!(check.config.min_free_disk_percent, 15.0);
        assert_eq!(check.config.storage_path, "/data");
    }

    #[tokio::test]
    async fn test_storage_health_check_success() {
        let check = StorageHealthCheck::new("test_storage");
        check.set_pool_size(20);
        check.set_healthy_connections(18);
        check.set_disk_space(5 * 1024 * 1024 * 1024, 50 * 1024 * 1024 * 1024); // 5GB/50GB

        let result = check.check().await.unwrap();
        assert_eq!(result.status, HealthStatus::Healthy);
        assert!(result.message.unwrap().contains("healthy"));
        assert_eq!(result.metadata.get("pool_size"), Some(&"20".to_string()));
        assert_eq!(
            result.metadata.get("healthy_connections"),
            Some(&"18".to_string())
        );
        assert!(result.metadata.contains_key("percent_free"));
    }

    #[tokio::test]
    async fn test_storage_health_check_insufficient_disk_space() {
        let check = StorageHealthCheck::new("test_storage");
        check.set_disk_space(100 * 1024 * 1024, 50 * 1024 * 1024 * 1024); // 100MB/50GB (0.2%)

        let result = check.check().await.unwrap();
        assert_eq!(result.status, HealthStatus::Unhealthy);
        assert!(result.message.unwrap().contains("disk space"));
    }

    #[tokio::test]
    async fn test_storage_health_check_insufficient_connections() {
        let check = StorageHealthCheck::new("test_storage");
        check.set_pool_size(10);
        check.set_healthy_connections(1); // Below minimum of 2

        let result = check.check().await.unwrap();
        assert_eq!(result.status, HealthStatus::Unhealthy);
        assert!(result.message.unwrap().contains("connection"));
    }

    #[test]
    fn test_default_config() {
        let config = StorageHealthCheckConfig::default();
        assert_eq!(config.operation_timeout, Duration::from_secs(10));
        assert_eq!(config.min_free_disk_bytes, 1024 * 1024 * 1024); // 1GB
        assert_eq!(config.min_free_disk_percent, 10.0);
        assert_eq!(config.max_pool_size, 100);
        assert_eq!(config.min_healthy_connections, 2);
        assert_eq!(config.storage_path, "/tmp");
    }

    #[test]
    fn test_disk_space_check_zero_total() {
        let check = StorageHealthCheck::new("test_storage");
        check.set_disk_space(1000, 0); // Zero total should trigger error

        let result = check.check_disk_space();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Unable to determine"));
    }
}
