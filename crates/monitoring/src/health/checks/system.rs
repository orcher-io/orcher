//! System resources health check implementation
//!
//! This module provides health checks for system resources including CPU usage,
//! memory usage, and disk usage monitoring with configurable thresholds.

use crate::error::{MonitoringError, Result};
use crate::health::check::{
    AsyncHealthCheck, HealthCheckConfig, HealthCheckCriticality, HealthCheckResult,
};
use orcher_common::monitoring::HealthStatus;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Configuration for system resources health checks
#[derive(Debug, Clone)]
pub struct SystemHealthCheckConfig {
    /// Timeout for system resource checks
    pub check_timeout: Duration,
    /// Maximum allowed CPU usage percentage (0-100)
    pub max_cpu_usage_percent: f64,
    /// Maximum allowed memory usage percentage (0-100)
    pub max_memory_usage_percent: f64,
    /// Minimum required free disk space in bytes
    pub min_free_disk_bytes: u64,
    /// Minimum required free disk space percentage (0-100)
    pub min_free_disk_percent: f64,
    /// Maximum allowed load average (1-minute)
    pub max_load_average: f64,
}

impl Default for SystemHealthCheckConfig {
    fn default() -> Self {
        Self {
            check_timeout: Duration::from_secs(10),
            max_cpu_usage_percent: 90.0,
            max_memory_usage_percent: 85.0,
            min_free_disk_bytes: 1024 * 1024 * 1024, // 1 GB
            min_free_disk_percent: 10.0,
            max_load_average: 8.0,
        }
    }
}

/// System resources health check implementation
pub struct SystemResourcesHealthCheck {
    name: String,
    config: SystemHealthCheckConfig,
    // In a real implementation, these would use actual system monitoring
    // For now, we'll simulate the system resource status
    cpu_usage_percent: Arc<std::sync::atomic::AtomicU64>, // Store as u64 * 100 for precision
    memory_usage_percent: Arc<std::sync::atomic::AtomicU64>,
    memory_total_bytes: Arc<std::sync::atomic::AtomicU64>,
    memory_used_bytes: Arc<std::sync::atomic::AtomicU64>,
    disk_total_bytes: Arc<std::sync::atomic::AtomicU64>,
    disk_free_bytes: Arc<std::sync::atomic::AtomicU64>,
    load_average: Arc<std::sync::atomic::AtomicU64>,
}

impl SystemResourcesHealthCheck {
    /// Create a new system resources health check
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            config: SystemHealthCheckConfig::default(),
            cpu_usage_percent: Arc::new(std::sync::atomic::AtomicU64::new(4500)), // 45.00%
            memory_usage_percent: Arc::new(std::sync::atomic::AtomicU64::new(6500)), // 65.00%
            memory_total_bytes: Arc::new(std::sync::atomic::AtomicU64::new(
                16 * 1024 * 1024 * 1024,
            )), // 16GB
            memory_used_bytes: Arc::new(std::sync::atomic::AtomicU64::new(10 * 1024 * 1024 * 1024)), // 10GB
            disk_total_bytes: Arc::new(std::sync::atomic::AtomicU64::new(500 * 1024 * 1024 * 1024)), // 500GB
            disk_free_bytes: Arc::new(std::sync::atomic::AtomicU64::new(100 * 1024 * 1024 * 1024)), // 100GB
            load_average: Arc::new(std::sync::atomic::AtomicU64::new(200)), // 2.00
        }
    }

    /// Create a system resources health check with custom configuration
    pub fn with_config(name: impl Into<String>, config: SystemHealthCheckConfig) -> Self {
        Self {
            name: name.into(),
            config,
            cpu_usage_percent: Arc::new(std::sync::atomic::AtomicU64::new(4500)),
            memory_usage_percent: Arc::new(std::sync::atomic::AtomicU64::new(6500)),
            memory_total_bytes: Arc::new(std::sync::atomic::AtomicU64::new(
                16 * 1024 * 1024 * 1024,
            )),
            memory_used_bytes: Arc::new(std::sync::atomic::AtomicU64::new(10 * 1024 * 1024 * 1024)),
            disk_total_bytes: Arc::new(std::sync::atomic::AtomicU64::new(500 * 1024 * 1024 * 1024)),
            disk_free_bytes: Arc::new(std::sync::atomic::AtomicU64::new(100 * 1024 * 1024 * 1024)),
            load_average: Arc::new(std::sync::atomic::AtomicU64::new(200)),
        }
    }

    /// Set CPU usage (for testing/simulation) - percentage * 100 for precision
    pub fn set_cpu_usage(&self, percent: f64) {
        self.cpu_usage_percent.store(
            (percent * 100.0) as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
    }

    /// Set memory usage (for testing/simulation)
    pub fn set_memory_usage(&self, total_bytes: u64, used_bytes: u64) {
        let percent = if total_bytes > 0 {
            (used_bytes as f64 / total_bytes as f64) * 10000.0 // * 100 for %, * 100 for precision
        } else {
            0.0
        };

        self.memory_total_bytes
            .store(total_bytes, std::sync::atomic::Ordering::Relaxed);
        self.memory_used_bytes
            .store(used_bytes, std::sync::atomic::Ordering::Relaxed);
        self.memory_usage_percent
            .store(percent as u64, std::sync::atomic::Ordering::Relaxed);
    }

    /// Set disk usage (for testing/simulation)
    pub fn set_disk_usage(&self, total_bytes: u64, free_bytes: u64) {
        self.disk_total_bytes
            .store(total_bytes, std::sync::atomic::Ordering::Relaxed);
        self.disk_free_bytes
            .store(free_bytes, std::sync::atomic::Ordering::Relaxed);
    }

    /// Set load average (for testing/simulation) - load * 100 for precision
    pub fn set_load_average(&self, load: f64) {
        self.load_average
            .store((load * 100.0) as u64, std::sync::atomic::Ordering::Relaxed);
    }

    /// Check CPU usage
    fn check_cpu_usage(&self) -> Result<f64> {
        let cpu_usage_raw = self
            .cpu_usage_percent
            .load(std::sync::atomic::Ordering::Relaxed);
        let cpu_percent = cpu_usage_raw as f64 / 100.0;

        if cpu_percent > self.config.max_cpu_usage_percent {
            return Err(MonitoringError::health_check_failed(format!(
                "CPU usage too high: {:.1}% (maximum: {:.1}%)",
                cpu_percent, self.config.max_cpu_usage_percent
            )));
        }

        Ok(cpu_percent)
    }

    /// Check memory usage
    fn check_memory_usage(&self) -> Result<(f64, u64, u64)> {
        let memory_usage_raw = self
            .memory_usage_percent
            .load(std::sync::atomic::Ordering::Relaxed);
        let memory_percent = memory_usage_raw as f64 / 100.0;

        let total_bytes = self
            .memory_total_bytes
            .load(std::sync::atomic::Ordering::Relaxed);
        let used_bytes = self
            .memory_used_bytes
            .load(std::sync::atomic::Ordering::Relaxed);

        if memory_percent > self.config.max_memory_usage_percent {
            return Err(MonitoringError::health_check_failed(format!(
                "Memory usage too high: {:.1}% (maximum: {:.1}%)",
                memory_percent, self.config.max_memory_usage_percent
            )));
        }

        Ok((memory_percent, total_bytes, used_bytes))
    }

    /// Check disk usage
    fn check_disk_usage(&self) -> Result<(f64, u64, u64)> {
        let total_bytes = self
            .disk_total_bytes
            .load(std::sync::atomic::Ordering::Relaxed);
        let free_bytes = self
            .disk_free_bytes
            .load(std::sync::atomic::Ordering::Relaxed);

        if total_bytes == 0 {
            return Err(MonitoringError::health_check_failed(
                "Unable to determine disk space",
            ));
        }

        let free_percent = (free_bytes as f64 / total_bytes as f64) * 100.0;

        // Check absolute minimum
        if free_bytes < self.config.min_free_disk_bytes {
            return Err(MonitoringError::health_check_failed(format!(
                "Insufficient disk space: {} bytes free (minimum: {} bytes)",
                free_bytes, self.config.min_free_disk_bytes
            )));
        }

        // Check percentage minimum
        if free_percent < self.config.min_free_disk_percent {
            return Err(MonitoringError::health_check_failed(format!(
                "Insufficient disk space: {:.1}% free (minimum: {:.1}%)",
                free_percent, self.config.min_free_disk_percent
            )));
        }

        Ok((free_percent, total_bytes, free_bytes))
    }

    /// Check system load
    fn check_system_load(&self) -> Result<f64> {
        let load_raw = self.load_average.load(std::sync::atomic::Ordering::Relaxed);
        let load_avg = load_raw as f64 / 100.0;

        if load_avg > self.config.max_load_average {
            return Err(MonitoringError::health_check_failed(format!(
                "System load too high: {:.2} (maximum: {:.2})",
                load_avg, self.config.max_load_average
            )));
        }

        Ok(load_avg)
    }

    /// Test system responsiveness
    async fn test_system_responsiveness(&self) -> Result<Duration> {
        let start = Instant::now();

        // Simulate system responsiveness check
        let response_future = async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok::<(), MonitoringError>(())
        };

        match tokio::time::timeout(self.config.check_timeout, response_future).await {
            Ok(_) => Ok(start.elapsed()),
            Err(_) => Err(MonitoringError::health_check_failed(
                "System responsiveness check timeout",
            )),
        }
    }
}

#[async_trait::async_trait]
impl AsyncHealthCheck for SystemResourcesHealthCheck {
    fn name(&self) -> &str {
        &self.name
    }

    fn config(&self) -> HealthCheckConfig {
        HealthCheckConfig {
            timeout: self.config.check_timeout,
            interval: Duration::from_secs(30),
            criticality: HealthCheckCriticality::Important,
            failure_threshold: 3,
            success_threshold: 1,
            circuit_breaker_enabled: true,
        }
    }

    async fn check(&self) -> Result<HealthCheckResult> {
        let start = Instant::now();
        let mut result = HealthCheckResult::new(&self.name, HealthStatus::Healthy);

        // Check CPU usage
        match self.check_cpu_usage() {
            Ok(cpu_percent) => {
                result = result.with_metadata("cpu_usage_percent", format!("{:.1}", cpu_percent));
            }
            Err(e) => {
                result.status = HealthStatus::Unhealthy;
                return Ok(result
                    .with_message(format!("CPU usage check failed: {}", e))
                    .with_duration(start.elapsed()));
            }
        }

        // Check memory usage
        match self.check_memory_usage() {
            Ok((memory_percent, total_bytes, used_bytes)) => {
                result = result
                    .with_metadata("memory_usage_percent", format!("{:.1}", memory_percent))
                    .with_metadata("memory_total_bytes", total_bytes.to_string())
                    .with_metadata("memory_used_bytes", used_bytes.to_string());
            }
            Err(e) => {
                result.status = HealthStatus::Unhealthy;
                return Ok(result
                    .with_message(format!("Memory usage check failed: {}", e))
                    .with_duration(start.elapsed()));
            }
        }

        // Check disk usage
        match self.check_disk_usage() {
            Ok((free_percent, total_bytes, free_bytes)) => {
                result = result
                    .with_metadata("disk_free_percent", format!("{:.1}", free_percent))
                    .with_metadata("disk_total_bytes", total_bytes.to_string())
                    .with_metadata("disk_free_bytes", free_bytes.to_string());
            }
            Err(e) => {
                result.status = HealthStatus::Unhealthy;
                return Ok(result
                    .with_message(format!("Disk usage check failed: {}", e))
                    .with_duration(start.elapsed()));
            }
        }

        // Check system load
        match self.check_system_load() {
            Ok(load_avg) => {
                result = result.with_metadata("load_average", format!("{:.2}", load_avg));
            }
            Err(e) => {
                result.status = HealthStatus::Unhealthy;
                return Ok(result
                    .with_message(format!("System load check failed: {}", e))
                    .with_duration(start.elapsed()));
            }
        }

        // Test system responsiveness
        match self.test_system_responsiveness().await {
            Ok(response_time) => {
                result = result
                    .with_message(
                        "System resources healthy - CPU, memory, disk, and load within limits",
                    )
                    .with_metadata("response_time_ms", response_time.as_millis().to_string())
                    .with_metadata("responsiveness_status", "success")
                    .with_duration(start.elapsed());
            }
            Err(e) => {
                result.status = HealthStatus::Unhealthy;
                result = result
                    .with_message(format!("System responsiveness test failed: {}", e))
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
    fn test_system_health_check_creation() {
        let check = SystemResourcesHealthCheck::new("test_system");
        assert_eq!(check.name(), "test_system");
        assert_eq!(
            check.config().criticality,
            HealthCheckCriticality::Important
        );
    }

    #[test]
    fn test_system_health_check_with_config() {
        let config = SystemHealthCheckConfig {
            check_timeout: Duration::from_secs(5),
            max_cpu_usage_percent: 80.0,
            max_memory_usage_percent: 75.0,
            min_free_disk_bytes: 2 * 1024 * 1024 * 1024, // 2GB
            min_free_disk_percent: 15.0,
            max_load_average: 4.0,
        };

        let check = SystemResourcesHealthCheck::with_config("custom_system", config.clone());
        assert_eq!(check.config.max_cpu_usage_percent, 80.0);
        assert_eq!(check.config.max_memory_usage_percent, 75.0);
    }

    #[tokio::test]
    async fn test_system_health_check_success() {
        let check = SystemResourcesHealthCheck::new("test_system");
        check.set_cpu_usage(45.0);
        check.set_memory_usage(16 * 1024 * 1024 * 1024, 10 * 1024 * 1024 * 1024); // 16GB total, 10GB used
        check.set_disk_usage(500 * 1024 * 1024 * 1024, 100 * 1024 * 1024 * 1024); // 500GB total, 100GB free
        check.set_load_average(2.0);

        let result = check.check().await.unwrap();
        assert_eq!(result.status, HealthStatus::Healthy);
        assert!(result.message.unwrap().contains("healthy"));
        assert_eq!(
            result.metadata.get("cpu_usage_percent"),
            Some(&"45.0".to_string())
        );
        assert!(result.metadata.contains_key("memory_usage_percent"));
        assert!(result.metadata.contains_key("disk_free_percent"));
        assert_eq!(
            result.metadata.get("load_average"),
            Some(&"2.00".to_string())
        );
    }

    #[tokio::test]
    async fn test_system_health_check_high_cpu() {
        let check = SystemResourcesHealthCheck::new("test_system");
        check.set_cpu_usage(95.0); // Exceeds default max of 90%

        let result = check.check().await.unwrap();
        assert_eq!(result.status, HealthStatus::Unhealthy);
        assert!(result.message.unwrap().contains("CPU usage too high"));
    }

    #[tokio::test]
    async fn test_system_health_check_high_memory() {
        let check = SystemResourcesHealthCheck::new("test_system");
        check.set_cpu_usage(45.0);
        check.set_memory_usage(16 * 1024 * 1024 * 1024, 14 * 1024 * 1024 * 1024); // 87.5% usage

        let result = check.check().await.unwrap();
        assert_eq!(result.status, HealthStatus::Unhealthy);
        assert!(result.message.unwrap().contains("Memory usage too high"));
    }

    #[tokio::test]
    async fn test_system_health_check_low_disk_space() {
        let check = SystemResourcesHealthCheck::new("test_system");
        check.set_cpu_usage(45.0);
        check.set_memory_usage(16 * 1024 * 1024 * 1024, 10 * 1024 * 1024 * 1024);
        check.set_disk_usage(100 * 1024 * 1024 * 1024, 500 * 1024 * 1024); // 100GB total, 500MB free (0.5%)

        let result = check.check().await.unwrap();
        assert_eq!(result.status, HealthStatus::Unhealthy);
        assert!(result.message.unwrap().contains("disk space"));
    }

    #[tokio::test]
    async fn test_system_health_check_high_load() {
        let check = SystemResourcesHealthCheck::new("test_system");
        check.set_cpu_usage(45.0);
        check.set_memory_usage(16 * 1024 * 1024 * 1024, 10 * 1024 * 1024 * 1024);
        check.set_disk_usage(500 * 1024 * 1024 * 1024, 100 * 1024 * 1024 * 1024);
        check.set_load_average(10.0); // Exceeds default max of 8.0

        let result = check.check().await.unwrap();
        assert_eq!(result.status, HealthStatus::Unhealthy);
        assert!(result.message.unwrap().contains("System load too high"));
    }

    #[tokio::test]
    async fn test_system_health_check_zero_disk() {
        let check = SystemResourcesHealthCheck::new("test_system");
        check.set_cpu_usage(45.0);
        check.set_memory_usage(16 * 1024 * 1024 * 1024, 10 * 1024 * 1024 * 1024);
        check.set_disk_usage(0, 0); // Zero disk should trigger error

        let result = check.check().await.unwrap();
        assert_eq!(result.status, HealthStatus::Unhealthy);
        assert!(result
            .message
            .unwrap()
            .contains("Unable to determine disk space"));
    }

    #[test]
    fn test_default_config() {
        let config = SystemHealthCheckConfig::default();
        assert_eq!(config.check_timeout, Duration::from_secs(10));
        assert_eq!(config.max_cpu_usage_percent, 90.0);
        assert_eq!(config.max_memory_usage_percent, 85.0);
        assert_eq!(config.min_free_disk_bytes, 1024 * 1024 * 1024); // 1GB
        assert_eq!(config.min_free_disk_percent, 10.0);
        assert_eq!(config.max_load_average, 8.0);
    }
}
