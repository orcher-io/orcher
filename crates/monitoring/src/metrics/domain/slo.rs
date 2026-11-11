//! SLO (Service Level Objective) Tracking
//!
//! Provides SLO/SLI tracking using the `metrics` crate histograms for latency
//! and counters for availability tracking.

use metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram};
use std::time::Duration;

/// Metric name constants for SLO metrics
pub mod slo_names {
    pub const LATENCY_SECONDS: &str = "orcher_slo_latency_seconds";
    pub const REQUESTS_TOTAL: &str = "orcher_slo_requests_total";
    pub const ERRORS_TOTAL: &str = "orcher_slo_errors_total";
    pub const VIOLATIONS_TOTAL: &str = "orcher_slo_violations_total";
    pub const STATUS: &str = "orcher_slo_status";
    pub const ERROR_BUDGET_REMAINING: &str = "orcher_slo_error_budget_remaining";
    pub const AVAILABILITY: &str = "orcher_slo_availability";
}

/// SLO status values
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SloStatus {
    Unknown = 0,
    Healthy = 1,
    Warning = 2,
    Critical = 3,
}

impl SloStatus {
    /// Convert status to a numeric value for metrics
    pub fn as_value(&self) -> f64 {
        *self as u8 as f64
    }

    /// Check if status is healthy
    pub fn is_healthy(&self) -> bool {
        matches!(self, SloStatus::Healthy)
    }

    /// Check if action is needed
    pub fn needs_attention(&self) -> bool {
        matches!(self, SloStatus::Warning | SloStatus::Critical)
    }
}

/// SLO configuration
#[derive(Debug, Clone)]
pub struct SloConfig {
    /// Name of the SLO
    pub name: String,

    /// Target percentage (0.0 - 1.0, e.g., 0.999 for 99.9%)
    pub target: f64,

    /// Window duration for calculating SLO
    pub window: Duration,

    /// Warning threshold (percentage of target)
    pub warning_threshold: f64,

    /// Critical threshold (percentage of target)
    pub critical_threshold: f64,
}

impl SloConfig {
    /// Create a new SLO config
    pub fn new(name: impl Into<String>, target: f64, window: Duration) -> Self {
        Self {
            name: name.into(),
            target,
            window,
            warning_threshold: 0.95,
            critical_threshold: 0.90,
        }
    }

    /// Create a latency SLO (e.g., 99th percentile under 100ms)
    pub fn latency_p99(name: impl Into<String>, target_ms: u64) -> Self {
        Self {
            name: name.into(),
            target: target_ms as f64,
            window: Duration::from_secs(300), // 5 minute window
            warning_threshold: 0.9,
            critical_threshold: 0.8,
        }
    }

    /// Create an availability SLO
    pub fn availability(name: impl Into<String>, target: f64) -> Self {
        Self {
            name: name.into(),
            target,
            window: Duration::from_secs(3600), // 1 hour window
            warning_threshold: 0.999,
            critical_threshold: 0.995,
        }
    }

    /// Set warning threshold
    pub fn with_warning_threshold(mut self, threshold: f64) -> Self {
        self.warning_threshold = threshold;
        self
    }

    /// Set critical threshold
    pub fn with_critical_threshold(mut self, threshold: f64) -> Self {
        self.critical_threshold = threshold;
        self
    }
}

/// Describe all SLO metrics (call at startup)
pub fn describe_slo_metrics() {
    describe_histogram!(
        slo_names::LATENCY_SECONDS,
        "Request latency in seconds for SLO tracking"
    );
    describe_counter!(slo_names::REQUESTS_TOTAL, "Total requests tracked for SLO");
    describe_counter!(slo_names::ERRORS_TOTAL, "Total errors tracked for SLO");
    describe_counter!(slo_names::VIOLATIONS_TOTAL, "SLO violations count");
    describe_gauge!(
        slo_names::STATUS,
        "SLO status (0=unknown, 1=healthy, 2=warning, 3=critical)"
    );
    describe_gauge!(
        slo_names::ERROR_BUDGET_REMAINING,
        "Remaining error budget (0.0 - 1.0)"
    );
    describe_gauge!(slo_names::AVAILABILITY, "Current availability (0.0 - 1.0)");
}

// ============================================================================
// Latency Recording Functions
// ============================================================================

/// Record a successful request with latency
pub fn record_slo_success(slo_name: &str, latency: Duration) {
    histogram!(
        slo_names::LATENCY_SECONDS,
        "slo" => slo_name.to_string(),
        "outcome" => "success"
    )
    .record(latency.as_secs_f64());

    counter!(
        slo_names::REQUESTS_TOTAL,
        "slo" => slo_name.to_string(),
        "outcome" => "success"
    )
    .increment(1);
}

/// Record a failed request with latency
pub fn record_slo_failure(slo_name: &str, latency: Duration) {
    histogram!(
        slo_names::LATENCY_SECONDS,
        "slo" => slo_name.to_string(),
        "outcome" => "failure"
    )
    .record(latency.as_secs_f64());

    counter!(
        slo_names::REQUESTS_TOTAL,
        "slo" => slo_name.to_string(),
        "outcome" => "failure"
    )
    .increment(1);

    counter!(slo_names::ERRORS_TOTAL, "slo" => slo_name.to_string()).increment(1);
}

/// Record an SLO violation
pub fn record_slo_violation(slo_name: &str, violation_type: &str) {
    counter!(
        slo_names::VIOLATIONS_TOTAL,
        "slo" => slo_name.to_string(),
        "type" => violation_type.to_string()
    )
    .increment(1);
}

// ============================================================================
// Status Recording Functions
// ============================================================================

/// Update the SLO status gauge
pub fn update_slo_status(slo_name: &str, status: SloStatus) {
    gauge!(slo_names::STATUS, "slo" => slo_name.to_string()).set(status.as_value());
}

/// Update the error budget remaining gauge
pub fn update_error_budget_remaining(slo_name: &str, remaining: f64) {
    gauge!(
        slo_names::ERROR_BUDGET_REMAINING,
        "slo" => slo_name.to_string()
    )
    .set(remaining.clamp(0.0, 1.0));
}

/// Update the availability gauge
pub fn update_availability(slo_name: &str, availability: f64) {
    gauge!(
        slo_names::AVAILABILITY,
        "slo" => slo_name.to_string()
    )
    .set(availability.clamp(0.0, 1.0));
}

// ============================================================================
// RAII Guard for Automatic Latency Recording
// ============================================================================

/// RAII guard for automatic SLO latency recording
pub struct SloLatencyGuard {
    slo_name: String,
    start: std::time::Instant,
    success: bool,
}

impl SloLatencyGuard {
    /// Create a new latency guard
    pub fn new(slo_name: impl Into<String>) -> Self {
        Self {
            slo_name: slo_name.into(),
            start: std::time::Instant::now(),
            success: true,
        }
    }

    /// Mark the operation as failed
    pub fn mark_failure(&mut self) {
        self.success = false;
    }

    /// Mark the operation as successful (default)
    pub fn mark_success(&mut self) {
        self.success = true;
    }
}

impl Drop for SloLatencyGuard {
    fn drop(&mut self) {
        let latency = self.start.elapsed();
        if self.success {
            record_slo_success(&self.slo_name, latency);
        } else {
            record_slo_failure(&self.slo_name, latency);
        }
    }
}

/// Start tracking an SLO operation (returns a guard that records on drop)
pub fn track_slo_operation(slo_name: impl Into<String>) -> SloLatencyGuard {
    SloLatencyGuard::new(slo_name)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_describe_metrics_does_not_panic() {
        describe_slo_metrics();
    }

    #[test]
    fn test_slo_status_values() {
        assert_eq!(SloStatus::Unknown.as_value(), 0.0);
        assert_eq!(SloStatus::Healthy.as_value(), 1.0);
        assert_eq!(SloStatus::Warning.as_value(), 2.0);
        assert_eq!(SloStatus::Critical.as_value(), 3.0);
    }

    #[test]
    fn test_slo_status_helpers() {
        assert!(SloStatus::Healthy.is_healthy());
        assert!(!SloStatus::Warning.is_healthy());
        assert!(!SloStatus::Critical.is_healthy());

        assert!(!SloStatus::Healthy.needs_attention());
        assert!(SloStatus::Warning.needs_attention());
        assert!(SloStatus::Critical.needs_attention());
    }

    #[test]
    fn test_slo_config_creation() {
        let config = SloConfig::new("test-slo", 0.999, Duration::from_secs(3600));
        assert_eq!(config.name, "test-slo");
        assert!((config.target - 0.999).abs() < 0.0001);
    }

    #[test]
    fn test_slo_config_presets() {
        let latency = SloConfig::latency_p99("api-latency", 100);
        assert_eq!(latency.target, 100.0);

        let availability = SloConfig::availability("api-availability", 0.999);
        assert!((availability.target - 0.999).abs() < 0.0001);
    }

    #[test]
    fn test_record_slo_success() {
        record_slo_success("test-slo", Duration::from_millis(50));
    }

    #[test]
    fn test_record_slo_failure() {
        record_slo_failure("test-slo", Duration::from_millis(100));
    }

    #[test]
    fn test_record_slo_violation() {
        record_slo_violation("test-slo", "latency");
        record_slo_violation("test-slo", "availability");
    }

    #[test]
    fn test_update_status_metrics() {
        update_slo_status("test-slo", SloStatus::Healthy);
        update_error_budget_remaining("test-slo", 0.95);
        update_availability("test-slo", 0.9999);
    }

    #[test]
    fn test_slo_latency_guard() {
        {
            let _guard = track_slo_operation("test-operation");
            // Operation completes successfully
        }

        {
            let mut guard = track_slo_operation("test-operation");
            guard.mark_failure();
            // Operation failed
        }
    }
}
