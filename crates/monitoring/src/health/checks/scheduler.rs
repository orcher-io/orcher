//! Scheduler health check implementation
//!
//! This module provides health checks for scheduler status, validating trigger processing,
//! job queue status, and scheduling accuracy.

use crate::error::{MonitoringError, Result};
use crate::health::check::{
    AsyncHealthCheck, HealthCheckConfig, HealthCheckCriticality, HealthCheckResult,
};
use orcher_common::monitoring::HealthStatus;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Configuration for scheduler health checks
#[derive(Debug, Clone)]
pub struct SchedulerHealthCheckConfig {
    /// Timeout for scheduler status checks
    pub check_timeout: Duration,
    /// Maximum allowed job queue size before considered unhealthy
    pub max_job_queue_size: usize,
    /// Maximum allowed trigger processing delay in seconds
    pub max_trigger_delay_seconds: u64,
    /// Maximum allowed scheduling drift in seconds
    pub max_scheduling_drift_seconds: u64,
    /// Minimum required processed triggers per minute
    pub min_triggers_per_minute: u32,
}

impl Default for SchedulerHealthCheckConfig {
    fn default() -> Self {
        Self {
            check_timeout: Duration::from_secs(5),
            max_job_queue_size: 5000,
            max_trigger_delay_seconds: 300,   // 5 minutes
            max_scheduling_drift_seconds: 60, // 1 minute
            min_triggers_per_minute: 1,
        }
    }
}

/// Scheduler health check implementation
pub struct SchedulerHealthCheck {
    name: String,
    config: SchedulerHealthCheckConfig,
    // In a real implementation, these would interface with actual scheduler
    // For now, we'll simulate the scheduler status
    job_queue_size: Arc<std::sync::atomic::AtomicUsize>,
    pending_triggers: Arc<std::sync::atomic::AtomicU32>,
    processed_triggers: Arc<std::sync::atomic::AtomicU64>,
    last_trigger_time: Arc<std::sync::atomic::AtomicU64>,
    next_scheduled_time: Arc<std::sync::atomic::AtomicU64>,
    scheduler_running: Arc<std::sync::atomic::AtomicBool>,
}

impl SchedulerHealthCheck {
    /// Create a new scheduler health check
    pub fn new(name: impl Into<String>) -> Self {
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            name: name.into(),
            config: SchedulerHealthCheckConfig::default(),
            job_queue_size: Arc::new(std::sync::atomic::AtomicUsize::new(100)),
            pending_triggers: Arc::new(std::sync::atomic::AtomicU32::new(5)),
            processed_triggers: Arc::new(std::sync::atomic::AtomicU64::new(1000)),
            last_trigger_time: Arc::new(std::sync::atomic::AtomicU64::new(current_time - 30)),
            next_scheduled_time: Arc::new(std::sync::atomic::AtomicU64::new(current_time + 60)),
            scheduler_running: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        }
    }

    /// Create a scheduler health check with custom configuration
    pub fn with_config(name: impl Into<String>, config: SchedulerHealthCheckConfig) -> Self {
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            name: name.into(),
            config,
            job_queue_size: Arc::new(std::sync::atomic::AtomicUsize::new(100)),
            pending_triggers: Arc::new(std::sync::atomic::AtomicU32::new(5)),
            processed_triggers: Arc::new(std::sync::atomic::AtomicU64::new(1000)),
            last_trigger_time: Arc::new(std::sync::atomic::AtomicU64::new(current_time - 30)),
            next_scheduled_time: Arc::new(std::sync::atomic::AtomicU64::new(current_time + 60)),
            scheduler_running: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        }
    }

    /// Set scheduler running status (for testing/simulation)
    pub fn set_scheduler_running(&self, running: bool) {
        self.scheduler_running
            .store(running, std::sync::atomic::Ordering::Relaxed);
    }

    /// Set job queue size (for testing/simulation)
    pub fn set_job_queue_size(&self, size: usize) {
        self.job_queue_size
            .store(size, std::sync::atomic::Ordering::Relaxed);
    }

    /// Set trigger counts (for testing/simulation)
    pub fn set_trigger_counts(&self, pending: u32, processed: u64) {
        self.pending_triggers
            .store(pending, std::sync::atomic::Ordering::Relaxed);
        self.processed_triggers
            .store(processed, std::sync::atomic::Ordering::Relaxed);
    }

    /// Set timing information (for testing/simulation)
    pub fn set_timing(&self, last_trigger_seconds_ago: u64, next_trigger_seconds_from_now: u64) {
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        self.last_trigger_time.store(
            current_time - last_trigger_seconds_ago,
            std::sync::atomic::Ordering::Relaxed,
        );
        self.next_scheduled_time.store(
            current_time + next_trigger_seconds_from_now,
            std::sync::atomic::Ordering::Relaxed,
        );
    }

    /// Check scheduler running status
    fn check_scheduler_status(&self) -> Result<bool> {
        let is_running = self
            .scheduler_running
            .load(std::sync::atomic::Ordering::Relaxed);

        if !is_running {
            return Err(MonitoringError::health_check_failed(
                "Scheduler is not running",
            ));
        }

        Ok(is_running)
    }

    /// Check job queue status
    fn check_job_queue(&self) -> Result<usize> {
        let queue_size = self
            .job_queue_size
            .load(std::sync::atomic::Ordering::Relaxed);

        if queue_size > self.config.max_job_queue_size {
            return Err(MonitoringError::health_check_failed(format!(
                "Job queue size exceeded: {} (maximum: {})",
                queue_size, self.config.max_job_queue_size
            )));
        }

        Ok(queue_size)
    }

    /// Check trigger processing status
    fn check_trigger_processing(&self) -> Result<(u32, u64, u64)> {
        let pending = self
            .pending_triggers
            .load(std::sync::atomic::Ordering::Relaxed);
        let processed = self
            .processed_triggers
            .load(std::sync::atomic::Ordering::Relaxed);
        let last_trigger = self
            .last_trigger_time
            .load(std::sync::atomic::Ordering::Relaxed);

        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let time_since_last_trigger = current_time.saturating_sub(last_trigger);

        if time_since_last_trigger > self.config.max_trigger_delay_seconds {
            return Err(MonitoringError::health_check_failed(format!(
                "Trigger processing delay too high: {}s (maximum: {}s)",
                time_since_last_trigger, self.config.max_trigger_delay_seconds
            )));
        }

        Ok((pending, processed, time_since_last_trigger))
    }

    /// Check scheduling accuracy
    fn check_scheduling_accuracy(&self) -> Result<(u64, u64)> {
        let next_scheduled = self
            .next_scheduled_time
            .load(std::sync::atomic::Ordering::Relaxed);

        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Check if we're significantly behind schedule
        if next_scheduled < current_time {
            let drift = current_time - next_scheduled;
            if drift > self.config.max_scheduling_drift_seconds {
                return Err(MonitoringError::health_check_failed(format!(
                    "Scheduling drift too high: {}s behind (maximum: {}s)",
                    drift, self.config.max_scheduling_drift_seconds
                )));
            }
        }

        let time_to_next = next_scheduled.saturating_sub(current_time);

        Ok((time_to_next, next_scheduled))
    }

    /// Test scheduler responsiveness
    async fn test_scheduler_responsiveness(&self) -> Result<Duration> {
        let start = Instant::now();

        // Simulate checking scheduler responsiveness
        let response_future = async {
            tokio::time::sleep(Duration::from_millis(25)).await;
            Ok::<(), MonitoringError>(())
        };

        match tokio::time::timeout(self.config.check_timeout, response_future).await {
            Ok(_) => Ok(start.elapsed()),
            Err(_) => Err(MonitoringError::health_check_failed(
                "Scheduler responsiveness check timeout",
            )),
        }
    }
}

#[async_trait::async_trait]
impl AsyncHealthCheck for SchedulerHealthCheck {
    fn name(&self) -> &str {
        &self.name
    }

    fn config(&self) -> HealthCheckConfig {
        HealthCheckConfig {
            timeout: self.config.check_timeout,
            interval: Duration::from_secs(45),
            criticality: HealthCheckCriticality::Critical,
            failure_threshold: 2,
            success_threshold: 1,
            circuit_breaker_enabled: true,
        }
    }

    async fn check(&self) -> Result<HealthCheckResult> {
        let start = Instant::now();
        let mut result = HealthCheckResult::new(&self.name, HealthStatus::Healthy);

        // Check scheduler running status
        match self.check_scheduler_status() {
            Ok(is_running) => {
                result = result.with_metadata("scheduler_running", is_running.to_string());
            }
            Err(e) => {
                result.status = HealthStatus::Unhealthy;
                return Ok(result
                    .with_message(format!("Scheduler status check failed: {}", e))
                    .with_duration(start.elapsed()));
            }
        }

        // Check job queue
        match self.check_job_queue() {
            Ok(queue_size) => {
                result = result.with_metadata("job_queue_size", queue_size.to_string());
            }
            Err(e) => {
                result.status = HealthStatus::Unhealthy;
                return Ok(result
                    .with_message(format!("Job queue check failed: {}", e))
                    .with_duration(start.elapsed()));
            }
        }

        // Check trigger processing
        match self.check_trigger_processing() {
            Ok((pending, processed, time_since_last)) => {
                result = result
                    .with_metadata("pending_triggers", pending.to_string())
                    .with_metadata("processed_triggers", processed.to_string())
                    .with_metadata("seconds_since_last_trigger", time_since_last.to_string());
            }
            Err(e) => {
                result.status = HealthStatus::Unhealthy;
                return Ok(result
                    .with_message(format!("Trigger processing check failed: {}", e))
                    .with_duration(start.elapsed()));
            }
        }

        // Check scheduling accuracy
        match self.check_scheduling_accuracy() {
            Ok((time_to_next, next_scheduled)) => {
                result = result
                    .with_metadata("seconds_to_next_trigger", time_to_next.to_string())
                    .with_metadata("next_scheduled_timestamp", next_scheduled.to_string());
            }
            Err(e) => {
                result.status = HealthStatus::Unhealthy;
                return Ok(result
                    .with_message(format!("Scheduling accuracy check failed: {}", e))
                    .with_duration(start.elapsed()));
            }
        }

        // Test scheduler responsiveness
        match self.test_scheduler_responsiveness().await {
            Ok(response_time) => {
                result = result
                    .with_message(
                        "Scheduler healthy - running, queue manageable, triggers processing, timing accurate",
                    )
                    .with_metadata("response_time_ms", response_time.as_millis().to_string())
                    .with_metadata("responsiveness_status", "success")
                    .with_duration(start.elapsed());
            }
            Err(e) => {
                result.status = HealthStatus::Unhealthy;
                result = result
                    .with_message(format!("Scheduler responsiveness test failed: {}", e))
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
    fn test_scheduler_health_check_creation() {
        let check = SchedulerHealthCheck::new("test_scheduler");
        assert_eq!(check.name(), "test_scheduler");
        assert_eq!(check.config().criticality, HealthCheckCriticality::Critical);
    }

    #[test]
    fn test_scheduler_health_check_with_config() {
        let config = SchedulerHealthCheckConfig {
            check_timeout: Duration::from_secs(3),
            max_job_queue_size: 1000,
            max_trigger_delay_seconds: 120,
            max_scheduling_drift_seconds: 30,
            min_triggers_per_minute: 5,
        };

        let check = SchedulerHealthCheck::with_config("custom_scheduler", config.clone());
        assert_eq!(check.config.max_job_queue_size, 1000);
        assert_eq!(check.config.max_trigger_delay_seconds, 120);
    }

    #[tokio::test]
    async fn test_scheduler_health_check_success() {
        let check = SchedulerHealthCheck::new("test_scheduler");
        check.set_scheduler_running(true);
        check.set_job_queue_size(100);
        check.set_trigger_counts(5, 1000);
        check.set_timing(30, 60); // Last trigger 30s ago, next in 60s

        let result = check.check().await.unwrap();
        assert_eq!(result.status, HealthStatus::Healthy);
        assert!(result.message.unwrap().contains("healthy"));
        assert_eq!(
            result.metadata.get("scheduler_running"),
            Some(&"true".to_string())
        );
        assert_eq!(
            result.metadata.get("job_queue_size"),
            Some(&"100".to_string())
        );
        assert!(result.metadata.contains_key("pending_triggers"));
        assert!(result.metadata.contains_key("processed_triggers"));
    }

    #[tokio::test]
    async fn test_scheduler_health_check_not_running() {
        let check = SchedulerHealthCheck::new("test_scheduler");
        check.set_scheduler_running(false);

        let result = check.check().await.unwrap();
        assert_eq!(result.status, HealthStatus::Unhealthy);
        assert!(result.message.unwrap().contains("not running"));
    }

    #[tokio::test]
    async fn test_scheduler_health_check_large_queue() {
        let check = SchedulerHealthCheck::new("test_scheduler");
        check.set_scheduler_running(true);
        check.set_job_queue_size(10000); // Exceeds default max of 5000

        let result = check.check().await.unwrap();
        assert_eq!(result.status, HealthStatus::Unhealthy);
        assert!(result.message.unwrap().contains("queue size exceeded"));
    }

    #[tokio::test]
    async fn test_scheduler_health_check_trigger_delay() {
        let check = SchedulerHealthCheck::new("test_scheduler");
        check.set_scheduler_running(true);
        check.set_job_queue_size(100);
        check.set_trigger_counts(5, 1000);
        check.set_timing(400, 60); // Last trigger 400s ago (exceeds 300s limit)

        let result = check.check().await.unwrap();
        assert_eq!(result.status, HealthStatus::Unhealthy);
        assert!(result.message.unwrap().contains("delay too high"));
    }

    #[tokio::test]
    async fn test_scheduler_health_check_scheduling_drift() {
        let mut config = SchedulerHealthCheckConfig::default();
        config.max_scheduling_drift_seconds = 30;

        let check = SchedulerHealthCheck::with_config("test_scheduler", config);
        check.set_scheduler_running(true);
        check.set_job_queue_size(100);
        check.set_trigger_counts(5, 1000);
        check.set_timing(30, 0); // Next trigger should have happened 0s ago (behind schedule)

        // Simulate being behind schedule by more than 30s
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        check
            .next_scheduled_time
            .store(current_time - 45, std::sync::atomic::Ordering::Relaxed);

        let result = check.check().await.unwrap();
        assert_eq!(result.status, HealthStatus::Unhealthy);
        assert!(result.message.unwrap().contains("drift too high"));
    }

    #[test]
    fn test_default_config() {
        let config = SchedulerHealthCheckConfig::default();
        assert_eq!(config.check_timeout, Duration::from_secs(5));
        assert_eq!(config.max_job_queue_size, 5000);
        assert_eq!(config.max_trigger_delay_seconds, 300);
        assert_eq!(config.max_scheduling_drift_seconds, 60);
        assert_eq!(config.min_triggers_per_minute, 1);
    }
}
