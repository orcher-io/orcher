//! Prometheus metrics exporter with HTTP scrape endpoint.

use crate::error::MonitoringError;
use crate::exporters::{Exporter, ExporterConfig, ExporterResult};
use std::net::SocketAddr;
use std::str::FromStr;

#[cfg(feature = "prometheus")]
pub use prometheus::Registry as PrometheusRegistry;

/// Registers descriptions for all standard ORCHER metrics.
fn describe_orcher_metrics() {
    #[cfg(feature = "prometheus")]
    {
        use crate::metrics::names;
        use metrics::{describe_counter, describe_gauge, describe_histogram};

        // Task metrics
        describe_counter!(
            names::tasks::SUBMITTED_TOTAL,
            "Total number of tasks submitted"
        );
        describe_gauge!(names::tasks::RUNNING, "Number of tasks currently running");
        describe_counter!(
            names::tasks::COMPLETED_TOTAL,
            "Total number of tasks completed"
        );
        describe_counter!(names::tasks::FAILED_TOTAL, "Total number of tasks failed");
        describe_histogram!(
            names::tasks::DURATION_SECONDS,
            "Task execution duration in seconds"
        );
        describe_gauge!(names::tasks::QUEUE_SIZE, "Number of tasks in queue");

        // Worker metrics
        describe_gauge!(names::workers::POOL_SIZE, "Total size of worker pool");
        describe_gauge!(names::workers::ACTIVE_COUNT, "Number of active workers");
        describe_gauge!(names::workers::IDLE_COUNT, "Number of idle workers");
        describe_gauge!(
            names::workers::UTILIZATION,
            "Worker pool utilization percentage"
        );

        // Scheduler metrics
        describe_gauge!(
            names::scheduler::QUEUE_SIZE,
            "Number of items in scheduler queue"
        );
        describe_counter!(
            names::scheduler::TRIGGERS_FIRED_TOTAL,
            "Total number of scheduler triggers fired"
        );
        describe_gauge!(
            names::scheduler::NEXT_FIRE_TIME,
            "Next scheduled fire time as Unix timestamp"
        );

        // Storage metrics
        describe_counter!(
            names::storage::OPERATIONS_TOTAL,
            "Total number of storage operations"
        );
        describe_histogram!(
            names::storage::OPERATION_DURATION_SECONDS,
            "Storage operation duration in seconds"
        );
        describe_gauge!(
            names::storage::CACHE_HIT_RATIO,
            "Storage cache hit ratio (0.0 to 1.0)"
        );
        describe_gauge!(
            names::storage::CONNECTION_POOL_SIZE,
            "Number of connections in storage pool"
        );

        // Security metrics
        describe_counter!(
            names::security::AUTH_ATTEMPTS_TOTAL,
            "Total authentication attempts"
        );
        describe_counter!(
            names::security::AUTH_FAILURES_TOTAL,
            "Total authentication failures"
        );
        describe_counter!(
            names::security::RATE_LIMIT_EXCEEDED_TOTAL,
            "Total number of rate limit violations"
        );
        describe_counter!(
            names::security::THREATS_DETECTED_TOTAL,
            "Total security threats detected"
        );

        // System metrics
        describe_gauge!(names::system::CPU_USAGE, "System CPU usage percentage");
        describe_gauge!(names::system::MEMORY_USAGE, "System memory usage in bytes");
        describe_gauge!(names::system::DISK_USAGE, "System disk usage in bytes");
        describe_counter!(
            names::system::NETWORK_IO_BYTES,
            "System network I/O in bytes (with direction label)"
        );

        // DLQ metrics
        describe_counter!(
            names::dlq::TASKS_MOVED_TO_DLQ_TOTAL,
            "Total number of tasks moved to DLQ"
        );
        describe_counter!(
            names::dlq::TASKS_REPLAYED_TOTAL,
            "Total number of tasks replayed from DLQ"
        );
        describe_counter!(
            names::dlq::REPLAY_FAILURES_TOTAL,
            "Total number of DLQ replay failures"
        );
        describe_counter!(
            names::dlq::TASKS_PURGED_TOTAL,
            "Total number of tasks purged from DLQ"
        );
        describe_gauge!(
            names::dlq::CURRENT_DEPTH,
            "Current DLQ depth (number of entries)"
        );
        describe_counter!(names::dlq::QUERIES_TOTAL, "Total number of DLQ queries");
        describe_gauge!(
            names::dlq::REPLAY_SUCCESS_RATE,
            "DLQ replay success rate (0.0 to 1.0)"
        );
        describe_gauge!(
            names::dlq::NET_GROWTH,
            "Net DLQ growth (moved - replayed - purged)"
        );
        describe_gauge!(
            names::dlq::DEPTH_BY_PARTITION,
            "DLQ depth by partition (with partition label)"
        );
        describe_gauge!(
            names::dlq::ENTRIES_BY_FAILURE_TYPE,
            "DLQ entries by failure type (with failure_type label)"
        );

        // Retry metrics
        describe_counter!(
            names::retry::TOTAL_RETRIES,
            "Total number of retry attempts"
        );
        describe_counter!(
            names::retry::RETRIES_BY_ATTEMPT,
            "Retry count by attempt number (with attempt label)"
        );
        describe_counter!(
            names::retry::RETRY_SUCCESSES_TOTAL,
            "Total number of successful retries"
        );
        describe_counter!(
            names::retry::RETRY_EXHAUSTED_TOTAL,
            "Total number of exhausted retries (moved to DLQ)"
        );
        describe_gauge!(
            names::retry::SUCCESS_RATE,
            "Retry success rate (0.0 to 1.0)"
        );
        describe_gauge!(
            names::retry::AVERAGE_PER_TASK,
            "Average number of retries per task"
        );
        describe_histogram!(
            names::retry::BACKOFF_DURATION_SECONDS,
            "Retry backoff duration in seconds"
        );

        // Sync-match and Task Router metrics for optimized dispatch
        // Call the dedicated function that registers all sync-match metrics
        crate::metrics::describe_sync_match_metrics();
    }
}

#[derive(Debug, Clone)]
pub struct PrometheusConfig {
    pub bind_address: String,
    pub metrics_path: String,
    pub enable_compression: bool,
    pub max_concurrent_requests: usize,
    /// Skips actual HTTP server setup when true.
    pub test_mode: bool,
}

impl Default for PrometheusConfig {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0:9090".to_string(),
            metrics_path: "/metrics".to_string(),
            enable_compression: true,
            max_concurrent_requests: 10,
            test_mode: false,
        }
    }
}

impl ExporterConfig for PrometheusConfig {
    fn validate(&self) -> ExporterResult<()> {
        // Validate bind address
        if SocketAddr::from_str(&self.bind_address).is_err() {
            return Err(MonitoringError::exporter_error(
                "prometheus",
                format!("Invalid bind address: {}", self.bind_address),
            ));
        }

        // Validate metrics path
        if !self.metrics_path.starts_with('/') {
            return Err(MonitoringError::exporter_error(
                "prometheus",
                "Metrics path must start with '/'",
            ));
        }

        Ok(())
    }

    fn description(&self) -> String {
        format!(
            "Prometheus exporter on {} serving {}",
            self.bind_address, self.metrics_path
        )
    }
}

pub struct PrometheusExporter {
    config: PrometheusConfig,
    running: bool,
}

impl PrometheusExporter {
    pub fn new(config: PrometheusConfig) -> ExporterResult<Self> {
        config.validate()?;
        Ok(Self {
            config,
            running: false,
        })
    }

    pub fn builder() -> PrometheusExporterBuilder {
        PrometheusExporterBuilder::new()
    }
}

impl Exporter for PrometheusExporter {
    fn start(&mut self) -> ExporterResult<()> {
        if self.running {
            return Err(MonitoringError::exporter_error(
                "prometheus",
                "Exporter is already running",
            ));
        }

        #[cfg(feature = "prometheus")]
        {
            if !self.config.test_mode {
                // Describe all ORCHER metrics (metric descriptions only, no recorder needed)
                describe_orcher_metrics();

                tracing::info!(
                    "Prometheus exporter started on {} serving {}",
                    self.config.bind_address,
                    self.config.metrics_path
                );
            } else {
                tracing::info!(
                    "Prometheus exporter started (test mode) on {}",
                    self.config.bind_address
                );
            }
        }

        #[cfg(not(feature = "prometheus"))]
        {
            if !self.config.test_mode {
                tracing::warn!("Prometheus exporter started without prometheus feature enabled");
            }
        }

        self.running = true;
        Ok(())
    }

    fn stop(&mut self) -> ExporterResult<()> {
        if !self.running {
            return Err(MonitoringError::exporter_error(
                "prometheus",
                "Exporter is not running",
            ));
        }

        #[cfg(feature = "prometheus")]
        {
            if !self.config.test_mode {
                tracing::info!("Prometheus exporter stopped");
            } else {
                tracing::info!("Prometheus exporter stopped (test mode)");
            }
        }

        self.running = false;
        Ok(())
    }

    fn is_running(&self) -> bool {
        self.running
    }

    fn health_status(&self) -> ExporterResult<String> {
        let status = if self.running { "healthy" } else { "stopped" };
        Ok(format!(
            "Prometheus exporter: {} ({})",
            status,
            self.config.description()
        ))
    }
}

pub struct PrometheusExporterBuilder {
    config: PrometheusConfig,
}

impl Default for PrometheusExporterBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl PrometheusExporterBuilder {
    pub fn new() -> Self {
        Self {
            config: PrometheusConfig::default(),
        }
    }

    pub fn with_address(mut self, address: &str) -> Self {
        self.config.bind_address = address.to_string();
        self
    }

    pub fn with_test_mode(mut self, enabled: bool) -> Self {
        self.config.test_mode = enabled;
        self
    }

    pub fn with_path(mut self, path: &str) -> Self {
        self.config.metrics_path = path.to_string();
        self
    }

    pub fn with_compression(mut self, enabled: bool) -> Self {
        self.config.enable_compression = enabled;
        self
    }

    pub fn build(self) -> ExporterResult<PrometheusExporter> {
        PrometheusExporter::new(self.config)
    }
}

/// Install a global metrics recorder backed by a Prometheus registry.
///
/// Returns the registry handle so the caller can render metrics via
/// `prometheus::TextEncoder`. Must be called exactly once at startup.
#[cfg(feature = "prometheus")]
pub fn install_prometheus_recorder() -> prometheus::Registry {
    let registry = prometheus::Registry::new();
    let recorder = metrics_prometheus::Recorder::builder()
        .with_registry(registry.clone())
        .build();
    metrics::set_global_recorder(recorder).expect("failed to install metrics recorder");

    // Register metric descriptions after the recorder is installed
    describe_orcher_metrics();

    registry
}

/// Configure and start a Prometheus exporter on the given address.
pub fn setup_prometheus_exporter(addr: SocketAddr) -> ExporterResult<PrometheusExporter> {
    let mut exporter = PrometheusExporter::builder()
        .with_address(&addr.to_string())
        .build()?;

    exporter.start()?;
    Ok(exporter)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prometheus_config_default() {
        let config = PrometheusConfig::default();
        assert_eq!(config.bind_address, "0.0.0.0:9090");
        assert_eq!(config.metrics_path, "/metrics");
        assert!(config.enable_compression);
    }

    #[test]
    fn test_prometheus_config_validation() {
        let valid_config = PrometheusConfig::default();
        assert!(valid_config.validate().is_ok());

        let invalid_config = PrometheusConfig {
            bind_address: "invalid_address".to_string(),
            ..Default::default()
        };
        assert!(invalid_config.validate().is_err());
    }

    #[test]
    fn test_prometheus_exporter_lifecycle() {
        let mut exporter = PrometheusExporter::builder().build().unwrap();

        assert!(!exporter.is_running());
        assert!(exporter.start().is_ok());
        assert!(exporter.is_running());
        assert!(exporter.stop().is_ok());
        assert!(!exporter.is_running());
    }

    #[test]
    fn test_prometheus_exporter_builder() {
        let exporter = PrometheusExporter::builder()
            .with_address("127.0.0.1:8080")
            .with_path("/custom/metrics")
            .with_compression(false)
            .build()
            .unwrap();

        assert_eq!(exporter.config.bind_address, "127.0.0.1:8080");
        assert_eq!(exporter.config.metrics_path, "/custom/metrics");
        assert!(!exporter.config.enable_compression);
    }

    #[test]
    fn test_describe_orcher_metrics() {
        // Test that metric description doesn't panic
        describe_orcher_metrics();
        // If we reach here without panicking, the test passes
        assert!(true);
    }

    #[test]
    fn test_setup_prometheus_exporter() {
        use std::net::SocketAddr;

        // Test with a random available port
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();

        // In test mode, this should work
        let mut builder_exporter = PrometheusExporter::builder()
            .with_address(&addr.to_string())
            .with_test_mode(true)
            .build()
            .unwrap();

        assert!(builder_exporter.start().is_ok());
        assert!(builder_exporter.is_running());
        assert!(builder_exporter.stop().is_ok());
    }
}
