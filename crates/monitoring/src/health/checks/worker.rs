//! Worker pool health check implementation
//!
//! This module provides health checks for worker pool status, monitoring active workers,
//! queue status, and worker utilization.

use crate::error::{MonitoringError, Result};
use crate::health::check::{
    AsyncHealthCheck, HealthCheckConfig, HealthCheckCriticality, HealthCheckResult,
};
use orcher_common::monitoring::HealthStatus;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Configuration for worker pool health checks
#[derive(Debug, Clone)]
pub struct WorkerPoolHealthCheckConfig {
    /// Timeout for worker pool status checks
    pub check_timeout: Duration,
    /// Maximum allowed queue size before considered unhealthy
    pub max_queue_size: usize,
    /// Minimum required active workers
    pub min_active_workers: u32,
    /// Maximum worker utilization threshold (0.0-1.0)
    pub max_utilization_threshold: f64,
    /// Minimum worker utilization for efficiency (0.0-1.0)
    pub min_utilization_threshold: f64,
}

impl Default for WorkerPoolHealthCheckConfig {
    fn default() -> Self {
        Self {
            check_timeout: Duration::from_secs(5),
            max_queue_size: 1000,
            min_active_workers: 1,
            max_utilization_threshold: 0.9,
            min_utilization_threshold: 0.1,
        }
    }
}

/// Worker pool health check implementation
pub struct WorkerPoolHealthCheck {
    name: String,
    config: WorkerPoolHealthCheckConfig,
    // In a real implementation, these would interface with actual worker pool
    // For now, we'll simulate the worker pool status
    total_workers: Arc<std::sync::atomic::AtomicU32>,
    active_workers: Arc<std::sync::atomic::AtomicU32>,
    idle_workers: Arc<std::sync::atomic::AtomicU32>,
    queue_size: Arc<std::sync::atomic::AtomicUsize>,
    processed_tasks: Arc<std::sync::atomic::AtomicU64>,
}

impl WorkerPoolHealthCheck {
    /// Create a new worker pool health check
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            config: WorkerPoolHealthCheckConfig::default(),
            total_workers: Arc::new(std::sync::atomic::AtomicU32::new(10)),
            active_workers: Arc::new(std::sync::atomic::AtomicU32::new(6)),
            idle_workers: Arc::new(std::sync::atomic::AtomicU32::new(4)),
            queue_size: Arc::new(std::sync::atomic::AtomicUsize::new(50)),
            processed_tasks: Arc::new(std::sync::atomic::AtomicU64::new(1000)),
        }
    }

    /// Create a worker pool health check with custom configuration
    pub fn with_config(name: impl Into<String>, config: WorkerPoolHealthCheckConfig) -> Self {
        Self {
            name: name.into(),
            config,
            total_workers: Arc::new(std::sync::atomic::AtomicU32::new(10)),
            active_workers: Arc::new(std::sync::atomic::AtomicU32::new(6)),
            idle_workers: Arc::new(std::sync::atomic::AtomicU32::new(4)),
            queue_size: Arc::new(std::sync::atomic::AtomicUsize::new(50)),
            processed_tasks: Arc::new(std::sync::atomic::AtomicU64::new(1000)),
        }
    }

    /// Set worker pool status (for testing/simulation)
    pub fn set_worker_status(&self, total: u32, active: u32, idle: u32) {
        self.total_workers
            .store(total, std::sync::atomic::Ordering::Relaxed);
        self.active_workers
            .store(active, std::sync::atomic::Ordering::Relaxed);
        self.idle_workers
            .store(idle, std::sync::atomic::Ordering::Relaxed);
    }

    /// Set queue size (for testing/simulation)
    pub fn set_queue_size(&self, size: usize) {
        self.queue_size
            .store(size, std::sync::atomic::Ordering::Relaxed);
    }

    /// Set processed tasks count (for testing/simulation)
    pub fn set_processed_tasks(&self, count: u64) {
        self.processed_tasks
            .store(count, std::sync::atomic::Ordering::Relaxed);
    }

    /// Check worker pool status
    fn check_worker_pool(&self) -> Result<(u32, u32, u32, f64)> {
        let total = self
            .total_workers
            .load(std::sync::atomic::Ordering::Relaxed);
        let active = self
            .active_workers
            .load(std::sync::atomic::Ordering::Relaxed);
        let idle = self.idle_workers.load(std::sync::atomic::Ordering::Relaxed);

        if total == 0 {
            return Err(MonitoringError::health_check_failed(
                "No workers available in pool",
            ));
        }

        if active < self.config.min_active_workers {
            return Err(MonitoringError::health_check_failed(format!(
                "Insufficient active workers: {} (minimum: {})",
                active, self.config.min_active_workers
            )));
        }

        let utilization = active as f64 / total as f64;

        if utilization > self.config.max_utilization_threshold {
            return Err(MonitoringError::health_check_failed(format!(
                "Worker utilization too high: {:.1}% (maximum: {:.1}%)",
                utilization * 100.0,
                self.config.max_utilization_threshold * 100.0
            )));
        }

        Ok((total, active, idle, utilization))
    }

    /// Check task queue status
    fn check_queue_status(&self) -> Result<usize> {
        let queue_size = self.queue_size.load(std::sync::atomic::Ordering::Relaxed);

        if queue_size > self.config.max_queue_size {
            return Err(MonitoringError::health_check_failed(format!(
                "Task queue size exceeded: {} (maximum: {})",
                queue_size, self.config.max_queue_size
            )));
        }

        Ok(queue_size)
    }

    /// Test worker responsiveness
    async fn test_worker_responsiveness(&self) -> Result<Duration> {
        let start = Instant::now();

        // Simulate checking worker responsiveness
        let response_future = async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            Ok::<(), MonitoringError>(())
        };

        match tokio::time::timeout(self.config.check_timeout, response_future).await {
            Ok(_) => Ok(start.elapsed()),
            Err(_) => Err(MonitoringError::health_check_failed(
                "Worker responsiveness check timeout",
            )),
        }
    }
}

#[async_trait::async_trait]
impl AsyncHealthCheck for WorkerPoolHealthCheck {
    fn name(&self) -> &str {
        &self.name
    }

    fn config(&self) -> HealthCheckConfig {
        HealthCheckConfig {
            timeout: self.config.check_timeout,
            interval: Duration::from_secs(30),
            criticality: HealthCheckCriticality::Critical,
            failure_threshold: 3,
            success_threshold: 1,
            circuit_breaker_enabled: true,
        }
    }

    async fn check(&self) -> Result<HealthCheckResult> {
        let start = Instant::now();
        let mut result = HealthCheckResult::new(&self.name, HealthStatus::Healthy);

        // Check worker pool status
        match self.check_worker_pool() {
            Ok((total, active, idle, utilization)) => {
                result = result
                    .with_metadata("total_workers", total.to_string())
                    .with_metadata("active_workers", active.to_string())
                    .with_metadata("idle_workers", idle.to_string())
                    .with_metadata("utilization_percent", format!("{:.1}", utilization * 100.0));
            }
            Err(e) => {
                result.status = HealthStatus::Unhealthy;
                return Ok(result
                    .with_message(format!("Worker pool check failed: {}", e))
                    .with_duration(start.elapsed()));
            }
        }

        // Check queue status
        match self.check_queue_status() {
            Ok(queue_size) => {
                result = result.with_metadata("queue_size", queue_size.to_string());
            }
            Err(e) => {
                result.status = HealthStatus::Unhealthy;
                return Ok(result
                    .with_message(format!("Queue check failed: {}", e))
                    .with_duration(start.elapsed()));
            }
        }

        // Test worker responsiveness
        match self.test_worker_responsiveness().await {
            Ok(response_time) => {
                let processed = self
                    .processed_tasks
                    .load(std::sync::atomic::Ordering::Relaxed);

                result = result
                    .with_message(
                        "Worker pool healthy - workers active, queue manageable, responsiveness OK",
                    )
                    .with_metadata("response_time_ms", response_time.as_millis().to_string())
                    .with_metadata("processed_tasks", processed.to_string())
                    .with_metadata("responsiveness_status", "success")
                    .with_duration(start.elapsed());
            }
            Err(e) => {
                result.status = HealthStatus::Unhealthy;
                result = result
                    .with_message(format!("Worker responsiveness test failed: {}", e))
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
    fn test_worker_pool_health_check_creation() {
        let check = WorkerPoolHealthCheck::new("test_workers");
        assert_eq!(check.name(), "test_workers");
        assert_eq!(check.config().criticality, HealthCheckCriticality::Critical);
    }

    #[test]
    fn test_worker_pool_health_check_with_config() {
        let config = WorkerPoolHealthCheckConfig {
            check_timeout: Duration::from_secs(3),
            max_queue_size: 500,
            min_active_workers: 5,
            max_utilization_threshold: 0.8,
            min_utilization_threshold: 0.2,
        };

        let check = WorkerPoolHealthCheck::with_config("custom_workers", config.clone());
        assert_eq!(check.config.max_queue_size, 500);
        assert_eq!(check.config.min_active_workers, 5);
    }

    #[tokio::test]
    async fn test_worker_pool_health_check_success() {
        let check = WorkerPoolHealthCheck::new("test_workers");
        check.set_worker_status(10, 6, 4);
        check.set_queue_size(50);
        check.set_processed_tasks(1000);

        let result = check.check().await.unwrap();
        assert_eq!(result.status, HealthStatus::Healthy);
        assert!(result.message.unwrap().contains("healthy"));
        assert_eq!(
            result.metadata.get("total_workers"),
            Some(&"10".to_string())
        );
        assert_eq!(
            result.metadata.get("active_workers"),
            Some(&"6".to_string())
        );
        assert_eq!(result.metadata.get("queue_size"), Some(&"50".to_string()));
        assert!(result.metadata.contains_key("utilization_percent"));
    }

    #[tokio::test]
    async fn test_worker_pool_health_check_no_workers() {
        let check = WorkerPoolHealthCheck::new("test_workers");
        check.set_worker_status(0, 0, 0);

        let result = check.check().await.unwrap();
        assert_eq!(result.status, HealthStatus::Unhealthy);
        assert!(result.message.unwrap().contains("No workers available"));
    }

    #[tokio::test]
    async fn test_worker_pool_health_check_insufficient_active_workers() {
        let check = WorkerPoolHealthCheck::new("test_workers");
        check.set_worker_status(10, 0, 10); // No active workers

        let result = check.check().await.unwrap();
        assert_eq!(result.status, HealthStatus::Unhealthy);
        assert!(result
            .message
            .unwrap()
            .contains("Insufficient active workers"));
    }

    #[tokio::test]
    async fn test_worker_pool_health_check_high_utilization() {
        let mut config = WorkerPoolHealthCheckConfig::default();
        config.max_utilization_threshold = 0.5; // 50% max

        let check = WorkerPoolHealthCheck::with_config("test_workers", config);
        check.set_worker_status(10, 8, 2); // 80% utilization

        let result = check.check().await.unwrap();
        assert_eq!(result.status, HealthStatus::Unhealthy);
        assert!(result.message.unwrap().contains("utilization too high"));
    }

    #[tokio::test]
    async fn test_worker_pool_health_check_large_queue() {
        let check = WorkerPoolHealthCheck::new("test_workers");
        check.set_worker_status(10, 6, 4);
        check.set_queue_size(2000); // Exceeds default max of 1000

        let result = check.check().await.unwrap();
        assert_eq!(result.status, HealthStatus::Unhealthy);
        assert!(result.message.unwrap().contains("queue size exceeded"));
    }

    #[test]
    fn test_default_config() {
        let config = WorkerPoolHealthCheckConfig::default();
        assert_eq!(config.check_timeout, Duration::from_secs(5));
        assert_eq!(config.max_queue_size, 1000);
        assert_eq!(config.min_active_workers, 1);
        assert_eq!(config.max_utilization_threshold, 0.9);
        assert_eq!(config.min_utilization_threshold, 0.1);
    }
}
