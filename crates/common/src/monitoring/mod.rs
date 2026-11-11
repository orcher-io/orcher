//! Monitoring and observability types shared across ORCHER crates.

use crate::types::time::Timestamptz;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Health status of a component.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HealthStatus {
    /// Healthy and operational.
    Healthy,
    /// Degraded but still functional.
    Degraded,
    /// Unhealthy — may not be functional.
    Unhealthy,
    /// Status unknown.
    Unknown,
}

impl HealthStatus {
    pub fn is_operational(&self) -> bool {
        matches!(self, Self::Healthy | Self::Degraded)
    }

    pub fn requires_attention(&self) -> bool {
        matches!(self, Self::Degraded | Self::Unhealthy)
    }

    pub fn http_status_code(&self) -> u16 {
        match self {
            Self::Healthy | Self::Degraded => 200,
            Self::Unhealthy | Self::Unknown => 503,
        }
    }
}

impl std::fmt::Display for HealthStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Healthy => write!(f, "healthy"),
            Self::Degraded => write!(f, "degraded"),
            Self::Unhealthy => write!(f, "unhealthy"),
            Self::Unknown => write!(f, "unknown"),
        }
    }
}

/// Health check result for a component.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheck {
    pub component: String,
    pub status: HealthStatus,
    pub message: Option<String>,
    pub details: HashMap<String, String>,
    pub timestamp: Timestamptz,
    pub check_duration: Option<Duration>,
}

impl HealthCheck {
    pub fn new(component: impl Into<String>) -> Self {
        Self {
            component: component.into(),
            status: HealthStatus::Unknown,
            message: None,
            details: HashMap::new(),
            timestamp: Timestamptz::now(),
            check_duration: None,
        }
    }

    pub fn with_status(mut self, status: HealthStatus) -> Self {
        self.status = status;
        self
    }

    pub fn with_message(mut self, message: impl Into<String>) -> Self {
        self.message = Some(message.into());
        self
    }

    pub fn with_details(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.details.insert(key.into(), value.into());
        self
    }

    pub fn with_duration(mut self, duration: Duration) -> Self {
        self.check_duration = Some(duration);
        self
    }

    pub fn healthy(component: impl Into<String>) -> Self {
        Self::new(component).with_status(HealthStatus::Healthy)
    }

    pub fn unhealthy(component: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(component)
            .with_status(HealthStatus::Unhealthy)
            .with_message(message)
    }
}

/// Snapshot of system performance metrics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceMetrics {
    /// 0.0–100.0
    pub cpu_usage: Option<f64>,
    pub memory_usage: Option<u64>,
    pub memory_available: Option<u64>,
    pub disk_usage: Option<u64>,
    pub network_rx_bytes: Option<u64>,
    pub network_tx_bytes: Option<u64>,
    pub active_connections: Option<u32>,
    pub request_latency_ms: Option<f64>,
    pub requests_per_second: Option<f64>,
    pub error_rate: Option<f64>,
    pub timestamp: Timestamptz,
}

impl PerformanceMetrics {
    pub fn new() -> Self {
        Self {
            cpu_usage: None,
            memory_usage: None,
            memory_available: None,
            disk_usage: None,
            network_rx_bytes: None,
            network_tx_bytes: None,
            active_connections: None,
            request_latency_ms: None,
            requests_per_second: None,
            error_rate: None,
            timestamp: Timestamptz::now(),
        }
    }

    pub fn with_cpu_usage(mut self, cpu_usage: f64) -> Self {
        self.cpu_usage = Some(cpu_usage);
        self
    }

    pub fn with_memory_usage(mut self, memory_usage: u64) -> Self {
        self.memory_usage = Some(memory_usage);
        self
    }

    pub fn with_memory_usage_mb(mut self, memory_mb: u64) -> Self {
        self.memory_usage = Some(memory_mb * 1024 * 1024);
        self
    }

    pub fn with_latency_ms(mut self, latency_ms: f64) -> Self {
        self.request_latency_ms = Some(latency_ms);
        self
    }

    pub fn with_error_rate(mut self, error_rate: f64) -> Self {
        self.error_rate = Some(error_rate);
        self
    }

    pub fn memory_usage_percentage(&self) -> Option<f64> {
        match (self.memory_usage, self.memory_available) {
            (Some(used), Some(available)) if available > 0 => {
                Some((used as f64 / available as f64) * 100.0)
            }
            _ => None,
        }
    }
}

impl Default for PerformanceMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Status of a tracked operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OperationStatus {
    InProgress,
    Success,
    Failed,
    Cancelled,
    TimedOut,
}

impl std::fmt::Display for OperationStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InProgress => write!(f, "in_progress"),
            Self::Success => write!(f, "success"),
            Self::Failed => write!(f, "failed"),
            Self::Cancelled => write!(f, "cancelled"),
            Self::TimedOut => write!(f, "timed_out"),
        }
    }
}

/// Record of an operation's execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationRecord {
    pub operation_id: String,
    pub operation_name: String,
    pub status: OperationStatus,
    pub start_time: Timestamptz,
    pub end_time: Option<Timestamptz>,
    pub duration: Option<Duration>,
    pub error_message: Option<String>,
    pub metadata: HashMap<String, String>,
}

impl OperationRecord {
    pub fn new(operation_name: impl Into<String>) -> Self {
        Self {
            operation_id: uuid::Uuid::new_v4().to_string(),
            operation_name: operation_name.into(),
            status: OperationStatus::InProgress,
            start_time: Timestamptz::now(),
            end_time: None,
            duration: None,
            error_message: None,
            metadata: HashMap::new(),
        }
    }

    pub fn finish_success(mut self) -> Self {
        let now = Timestamptz::now();
        self.status = OperationStatus::Success;
        self.end_time = Some(now);
        self.duration = Some(now.signed_duration_since(self.start_time).unsigned_abs());
        self
    }

    pub fn finish_error(mut self, error_message: impl Into<String>) -> Self {
        let now = Timestamptz::now();
        self.status = OperationStatus::Failed;
        self.end_time = Some(now);
        self.duration = Some(now.signed_duration_since(self.start_time).unsigned_abs());
        self.error_message = Some(error_message.into());
        self
    }

    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }
}

/// Tracks wall-clock duration of an operation, producing an [`OperationRecord`] on completion.
pub struct OperationTracker {
    operation_name: String,
    start_time: Instant,
    start_timestamp: Timestamptz,
    metadata: HashMap<String, String>,
}

impl OperationTracker {
    pub fn start(operation_name: impl Into<String>) -> Self {
        Self {
            operation_name: operation_name.into(),
            start_time: Instant::now(),
            start_timestamp: Timestamptz::now(),
            metadata: HashMap::new(),
        }
    }

    pub fn add_metadata(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.metadata.insert(key.into(), value.into());
    }

    pub fn finish_success(self) -> OperationRecord {
        let duration = self.start_time.elapsed();
        let mut record = OperationRecord::new(self.operation_name);
        record.start_time = self.start_timestamp;
        record.status = OperationStatus::Success;
        record.end_time = Some(Timestamptz::now());
        record.duration = Some(duration);
        record.metadata = self.metadata;
        record
    }

    pub fn finish_error(self, error_message: impl Into<String>) -> OperationRecord {
        let duration = self.start_time.elapsed();
        let mut record = OperationRecord::new(self.operation_name);
        record.start_time = self.start_timestamp;
        record.status = OperationStatus::Failed;
        record.end_time = Some(Timestamptz::now());
        record.duration = Some(duration);
        record.error_message = Some(error_message.into());
        record.metadata = self.metadata;
        record
    }

    pub fn current_duration(&self) -> Duration {
        self.start_time.elapsed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_health_status() {
        assert!(HealthStatus::Healthy.is_operational());
        assert!(HealthStatus::Degraded.is_operational());
        assert!(!HealthStatus::Unhealthy.is_operational());

        assert!(!HealthStatus::Healthy.requires_attention());
        assert!(HealthStatus::Degraded.requires_attention());
        assert!(HealthStatus::Unhealthy.requires_attention());

        assert_eq!(HealthStatus::Healthy.http_status_code(), 200);
        assert_eq!(HealthStatus::Unhealthy.http_status_code(), 503);
    }

    #[test]
    fn test_health_check_creation() {
        let check = HealthCheck::new("database")
            .with_status(HealthStatus::Healthy)
            .with_message("All connections active")
            .with_details("connections", "8/10");

        assert_eq!(check.component, "database");
        assert_eq!(check.status, HealthStatus::Healthy);
        assert_eq!(check.message, Some("All connections active".to_string()));
        assert_eq!(check.details.get("connections"), Some(&"8/10".to_string()));
    }

    #[test]
    fn test_health_check_shortcuts() {
        let healthy = HealthCheck::healthy("service");
        assert_eq!(healthy.status, HealthStatus::Healthy);

        let unhealthy = HealthCheck::unhealthy("service", "Connection failed");
        assert_eq!(unhealthy.status, HealthStatus::Unhealthy);
        assert_eq!(unhealthy.message, Some("Connection failed".to_string()));
    }

    #[test]
    fn test_performance_metrics() {
        let metrics = PerformanceMetrics::new()
            .with_cpu_usage(75.5)
            .with_memory_usage_mb(512)
            .with_latency_ms(125.0)
            .with_error_rate(2.5);

        assert_eq!(metrics.cpu_usage, Some(75.5));
        assert_eq!(metrics.memory_usage, Some(512 * 1024 * 1024));
        assert_eq!(metrics.request_latency_ms, Some(125.0));
        assert_eq!(metrics.error_rate, Some(2.5));
    }

    #[test]
    fn test_memory_usage_percentage() {
        let mut metrics = PerformanceMetrics::new();
        assert!(metrics.memory_usage_percentage().is_none());

        metrics.memory_usage = Some(512 * 1024 * 1024);
        metrics.memory_available = Some(1024 * 1024 * 1024);

        let percentage = metrics.memory_usage_percentage().unwrap();
        assert!((percentage - 50.0).abs() < 0.1);
    }

    #[test]
    fn test_operation_record() {
        let record = OperationRecord::new("test_operation")
            .with_metadata("user", "test-user")
            .finish_success();

        assert_eq!(record.operation_name, "test_operation");
        assert_eq!(record.status, OperationStatus::Success);
        assert!(record.end_time.is_some());
        assert!(record.duration.is_some());
        assert_eq!(record.metadata.get("user"), Some(&"test-user".to_string()));
    }

    #[test]
    fn test_operation_record_error() {
        let record = OperationRecord::new("failing_operation").finish_error("Something went wrong");

        assert_eq!(record.status, OperationStatus::Failed);
        assert_eq!(
            record.error_message,
            Some("Something went wrong".to_string())
        );
    }

    #[test]
    fn test_operation_tracker() {
        let mut tracker = OperationTracker::start("tracked_operation");
        tracker.add_metadata("context", "test");

        thread::sleep(Duration::from_millis(10));

        let record = tracker.finish_success();
        assert_eq!(record.operation_name, "tracked_operation");
        assert_eq!(record.status, OperationStatus::Success);
        assert!(record.duration.unwrap() >= Duration::from_millis(10));
        assert_eq!(record.metadata.get("context"), Some(&"test".to_string()));
    }

    #[test]
    fn test_operation_status_display() {
        assert_eq!(OperationStatus::Success.to_string(), "success");
        assert_eq!(OperationStatus::Failed.to_string(), "failed");
        assert_eq!(OperationStatus::InProgress.to_string(), "in_progress");
    }
}
