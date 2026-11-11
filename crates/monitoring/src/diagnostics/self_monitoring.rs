//! Self-monitoring system for tracking monitoring component health and performance
//!
//! This module provides comprehensive self-monitoring capabilities for the monitoring
//! system itself, tracking metrics collection performance, component health, and
//! system operational status.

use orcher_common::monitoring::{HealthStatus, PerformanceMetrics};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Self-monitoring system for tracking monitoring component health
#[derive(Debug, Clone)]
pub struct SelfMonitoring {
    inner: Arc<SelfMonitoringInner>,
}

#[derive(Debug)]
struct SelfMonitoringInner {
    /// When monitoring started
    start_time: Instant,
    /// Metrics collection counters
    metrics_collected: AtomicU64,
    /// Exporter error counters by exporter name
    exporter_errors: RwLock<HashMap<String, AtomicU64>>,
    /// Health check duration tracking
    health_check_durations: RwLock<HashMap<String, Vec<Duration>>>,
    /// Tracing spans created counter
    spans_created: AtomicU64,
    /// Configuration reload attempts
    config_reloads: RwLock<ConfigReloadStats>,
    /// Circuit breaker states
    circuit_breaker_states: RwLock<HashMap<String, CircuitBreakerState>>,
    /// Component health status
    component_health: RwLock<HashMap<String, ComponentHealthStatus>>,
    /// Performance metrics tracking
    performance_metrics: RwLock<PerformanceMetrics>,
    /// Resource usage tracking
    resource_usage_history: RwLock<Vec<ResourceUsagePoint>>,
    /// Alert counters
    alerts_fired: AtomicU64,
    /// System uptime tracking
    system_info: RwLock<SystemInfo>,
}

/// Configuration reload statistics
#[derive(Debug, Clone)]
pub struct ConfigReloadStats {
    total_attempts: u64,
    successful_reloads: u64,
    failed_reloads: u64,
    last_reload_time: Option<Instant>,
}

/// Circuit breaker state tracking
#[derive(Debug, Clone, PartialEq)]
pub enum CircuitBreakerState {
    Closed,
    Open,
    HalfOpen,
}

impl CircuitBreakerState {
    /// Convert to numeric value for metrics
    pub fn as_metric_value(&self) -> f64 {
        match self {
            Self::Closed => 0.0,
            Self::HalfOpen => 1.0,
            Self::Open => 2.0,
        }
    }
}

/// Component health status tracking
#[derive(Debug, Clone)]
struct ComponentHealthStatus {
    status: HealthStatus,
    last_check: Instant,
    check_count: u64,
    failure_count: u64,
}

/// Resource usage point in time
#[derive(Debug, Clone)]
pub struct ResourceUsagePoint {
    timestamp: Instant,
    cpu_percent: f64,
    memory_bytes: u64,
    disk_bytes: u64,
    network_bytes_rx: u64,
    network_bytes_tx: u64,
}

impl ResourceUsagePoint {
    /// Get timestamp of this resource usage point
    pub fn timestamp(&self) -> Instant {
        self.timestamp
    }

    /// Get CPU usage percentage
    pub fn cpu_percent(&self) -> f64 {
        self.cpu_percent
    }

    /// Get memory usage in bytes
    pub fn memory_bytes(&self) -> u64 {
        self.memory_bytes
    }

    /// Get disk usage in bytes
    pub fn disk_bytes(&self) -> u64 {
        self.disk_bytes
    }

    /// Get network bytes received
    pub fn network_bytes_rx(&self) -> u64 {
        self.network_bytes_rx
    }

    /// Get network bytes transmitted
    pub fn network_bytes_tx(&self) -> u64 {
        self.network_bytes_tx
    }
}

/// System information
#[derive(Debug, Clone)]
pub struct SystemInfo {
    hostname: String,
    version: String,
    pid: u32,
    thread_count: usize,
    fd_count: Option<usize>,
}

impl SystemInfo {
    /// Get system hostname
    pub fn hostname(&self) -> &str {
        &self.hostname
    }

    /// Get monitoring system version
    pub fn version(&self) -> &str {
        &self.version
    }

    /// Get process ID
    pub fn pid(&self) -> u32 {
        self.pid
    }

    /// Get thread count
    pub fn thread_count(&self) -> usize {
        self.thread_count
    }

    /// Get file descriptor count if available
    pub fn fd_count(&self) -> Option<usize> {
        self.fd_count
    }
}

impl SelfMonitoring {
    /// Create a new self-monitoring instance
    pub fn new() -> Self {
        Self {
            inner: Arc::new(SelfMonitoringInner {
                start_time: Instant::now(),
                metrics_collected: AtomicU64::new(0),
                exporter_errors: RwLock::new(HashMap::new()),
                health_check_durations: RwLock::new(HashMap::new()),
                spans_created: AtomicU64::new(0),
                config_reloads: RwLock::new(ConfigReloadStats {
                    total_attempts: 0,
                    successful_reloads: 0,
                    failed_reloads: 0,
                    last_reload_time: None,
                }),
                circuit_breaker_states: RwLock::new(HashMap::new()),
                component_health: RwLock::new(HashMap::new()),
                performance_metrics: RwLock::new(PerformanceMetrics::new()),
                resource_usage_history: RwLock::new(Vec::new()),
                alerts_fired: AtomicU64::new(0),
                system_info: RwLock::new(SystemInfo {
                    hostname: hostname::get()
                        .unwrap_or_default()
                        .to_string_lossy()
                        .to_string(),
                    version: env!("CARGO_PKG_VERSION").to_string(),
                    pid: std::process::id(),
                    thread_count: std::thread::available_parallelism()
                        .map(|p| p.get())
                        .unwrap_or(1),
                    fd_count: None,
                }),
            }),
        }
    }

    /// Record a metric collection event
    pub fn record_metric_collected(&self, metric_name: &str) {
        self.inner.metrics_collected.fetch_add(1, Ordering::Relaxed);

        // Emit the actual monitoring metric
        metrics::counter!(
            super::super::metrics::names::monitoring::METRICS_COLLECTED_TOTAL,
            "metric" => metric_name.to_string()
        )
        .increment(1);
    }

    /// Record an exporter error
    pub fn record_exporter_error(&self, exporter: &str, error_type: &str) {
        let mut exporters = self.inner.exporter_errors.write();
        let counter = exporters
            .entry(exporter.to_string())
            .or_insert_with(|| AtomicU64::new(0));
        counter.fetch_add(1, Ordering::Relaxed);

        // Emit the actual monitoring metric
        metrics::counter!(
            super::super::metrics::names::monitoring::EXPORTER_ERRORS_TOTAL,
            "exporter" => exporter.to_string(),
            "error_type" => error_type.to_string()
        )
        .increment(1);
    }

    /// Record health check duration
    pub fn record_health_check_duration(&self, check_name: &str, duration: Duration) {
        let mut durations = self.inner.health_check_durations.write();
        let check_durations = durations.entry(check_name.to_string()).or_default();

        check_durations.push(duration);

        // Keep only last 100 measurements per check
        if check_durations.len() > 100 {
            check_durations.remove(0);
        }

        // Emit the actual monitoring metric
        metrics::histogram!(
            super::super::metrics::names::monitoring::HEALTH_CHECK_DURATION_SECONDS,
            "check" => check_name.to_string()
        )
        .record(duration.as_secs_f64());
    }

    /// Record span creation
    pub fn record_span_created(&self, span_name: &str) {
        self.inner.spans_created.fetch_add(1, Ordering::Relaxed);

        // Emit the actual monitoring metric
        metrics::counter!(
            super::super::metrics::names::monitoring::SPANS_CREATED_TOTAL,
            "span" => span_name.to_string()
        )
        .increment(1);
    }

    /// Record configuration reload attempt
    pub fn record_config_reload(&self, success: bool) {
        let mut stats = self.inner.config_reloads.write();
        stats.total_attempts += 1;
        stats.last_reload_time = Some(Instant::now());

        if success {
            stats.successful_reloads += 1;
        } else {
            stats.failed_reloads += 1;
        }

        // Emit the actual monitoring metric - track total reloads and success rate separately
        metrics::counter!(
            "orcher_monitoring_config_reload_total",
            "success" => success.to_string()
        )
        .increment(1);

        // Update success rate gauge
        let success_rate = if stats.total_attempts > 0 {
            stats.successful_reloads as f64 / stats.total_attempts as f64
        } else {
            0.0
        };
        metrics::gauge!(super::super::metrics::names::monitoring::CONFIG_RELOAD_SUCCESS_RATE)
            .set(success_rate);
    }

    /// Record circuit breaker state change
    pub fn record_circuit_breaker_state(&self, name: &str, state: CircuitBreakerState) {
        let mut states = self.inner.circuit_breaker_states.write();
        states.insert(name.to_string(), state.clone());

        // Emit the actual monitoring metric
        metrics::gauge!(
            super::super::metrics::names::monitoring::CIRCUIT_BREAKER_STATE,
            "name" => name.to_string(),
            "state" => format!("{:?}", state).to_lowercase()
        )
        .set(state.as_metric_value());
    }

    /// Update component health status
    pub fn update_component_health(&self, component: &str, health: HealthStatus) {
        let mut components = self.inner.component_health.write();
        let status =
            components
                .entry(component.to_string())
                .or_insert_with(|| ComponentHealthStatus {
                    status: HealthStatus::Unknown,
                    last_check: Instant::now(),
                    check_count: 0,
                    failure_count: 0,
                });

        status.status = health;
        status.last_check = Instant::now();
        status.check_count += 1;

        if matches!(health, HealthStatus::Unhealthy) {
            status.failure_count += 1;
        }
    }

    /// Update system uptime metric
    pub fn update_uptime(&self) {
        let uptime_secs = self.inner.start_time.elapsed().as_secs_f64();

        // Emit the actual monitoring metric
        metrics::gauge!(super::super::metrics::names::monitoring::UPTIME_SECONDS).set(uptime_secs);
    }

    /// Record resource usage
    pub fn record_resource_usage(
        &self,
        cpu_percent: f64,
        memory_bytes: u64,
        disk_bytes: u64,
        network_rx: u64,
        network_tx: u64,
    ) {
        let usage_point = ResourceUsagePoint {
            timestamp: Instant::now(),
            cpu_percent,
            memory_bytes,
            disk_bytes,
            network_bytes_rx: network_rx,
            network_bytes_tx: network_tx,
        };

        let mut history = self.inner.resource_usage_history.write();
        history.push(usage_point);

        // Keep only last 1000 points (about 16 minutes at 1-second intervals)
        if history.len() > 1000 {
            history.remove(0);
        }

        // Update performance metrics
        let mut perf_metrics = self.inner.performance_metrics.write();
        *perf_metrics = perf_metrics
            .clone()
            .with_cpu_usage(cpu_percent)
            .with_memory_usage_mb(memory_bytes / 1024 / 1024);
    }

    /// Record alert fired
    pub fn record_alert_fired(&self, alert_type: &str, severity: &str) {
        self.inner.alerts_fired.fetch_add(1, Ordering::Relaxed);

        // Emit the actual monitoring metric
        metrics::counter!(
            super::super::metrics::names::monitoring::ALERTS_FIRED_TOTAL,
            "alert_type" => alert_type.to_string(),
            "severity" => severity.to_string()
        )
        .increment(1);
    }

    /// Get current system uptime
    pub fn get_uptime(&self) -> Duration {
        self.inner.start_time.elapsed()
    }

    /// Get total metrics collected
    pub fn get_metrics_collected_count(&self) -> u64 {
        self.inner.metrics_collected.load(Ordering::Relaxed)
    }

    /// Get exporter error counts
    pub fn get_exporter_error_counts(&self) -> HashMap<String, u64> {
        let exporters = self.inner.exporter_errors.read();
        exporters
            .iter()
            .map(|(name, counter)| (name.clone(), counter.load(Ordering::Relaxed)))
            .collect()
    }

    /// Get average health check durations
    pub fn get_health_check_averages(&self) -> HashMap<String, Duration> {
        let durations = self.inner.health_check_durations.read();
        durations
            .iter()
            .map(|(name, times)| {
                let avg_nanos = if times.is_empty() {
                    0
                } else {
                    times.iter().map(|d| d.as_nanos()).sum::<u128>() / times.len() as u128
                };
                (name.clone(), Duration::from_nanos(avg_nanos as u64))
            })
            .collect()
    }

    /// Get total spans created
    pub fn get_spans_created_count(&self) -> u64 {
        self.inner.spans_created.load(Ordering::Relaxed)
    }

    /// Get configuration reload statistics
    pub fn get_config_reload_stats(&self) -> ConfigReloadStats {
        self.inner.config_reloads.read().clone()
    }

    /// Get circuit breaker states
    pub fn get_circuit_breaker_states(&self) -> HashMap<String, CircuitBreakerState> {
        self.inner.circuit_breaker_states.read().clone()
    }

    /// Get component health statuses
    pub fn get_component_health(&self) -> HashMap<String, HealthStatus> {
        let components = self.inner.component_health.read();
        components
            .iter()
            .map(|(name, status)| (name.clone(), status.status))
            .collect()
    }

    /// Get current performance metrics
    pub fn get_performance_metrics(&self) -> PerformanceMetrics {
        self.inner.performance_metrics.read().clone()
    }

    /// Get recent resource usage (last N points)
    pub fn get_recent_resource_usage(&self, limit: usize) -> Vec<ResourceUsagePoint> {
        let history = self.inner.resource_usage_history.read();
        if history.len() <= limit {
            history.clone()
        } else {
            history[history.len() - limit..].to_vec()
        }
    }

    /// Get total alerts fired
    pub fn get_alerts_fired_count(&self) -> u64 {
        self.inner.alerts_fired.load(Ordering::Relaxed)
    }

    /// Get system information
    pub fn get_system_info(&self) -> SystemInfo {
        self.inner.system_info.read().clone()
    }

    /// Generate comprehensive self-monitoring report
    pub fn generate_health_report(&self) -> SelfMonitoringReport {
        let uptime = self.get_uptime();
        let metrics_collected = self.get_metrics_collected_count();
        let exporter_errors = self.get_exporter_error_counts();
        let component_health = self.get_component_health();
        let performance = self.get_performance_metrics();
        let config_stats = self.get_config_reload_stats();
        let circuit_breakers = self.get_circuit_breaker_states();

        // Calculate overall health score
        let health_score = self.calculate_health_score(&component_health, &exporter_errors);

        SelfMonitoringReport {
            timestamp: Instant::now(),
            uptime,
            health_score,
            metrics_collected,
            total_exporter_errors: exporter_errors.values().sum(),
            component_count: component_health.len(),
            healthy_components: component_health
                .values()
                .filter(|status| matches!(status, HealthStatus::Healthy))
                .count(),
            performance_metrics: performance,
            config_reload_success_rate: if config_stats.total_attempts > 0 {
                config_stats.successful_reloads as f64 / config_stats.total_attempts as f64
            } else {
                1.0
            },
            open_circuit_breakers: circuit_breakers
                .values()
                .filter(|state| matches!(state, CircuitBreakerState::Open))
                .count(),
        }
    }

    /// Calculate overall health score (0.0 to 1.0)
    fn calculate_health_score(
        &self,
        component_health: &HashMap<String, HealthStatus>,
        exporter_errors: &HashMap<String, u64>,
    ) -> f64 {
        if component_health.is_empty() {
            return 1.0; // No components to check
        }

        let healthy_count = component_health
            .values()
            .filter(|status| matches!(status, HealthStatus::Healthy))
            .count();

        let component_score = healthy_count as f64 / component_health.len() as f64;

        // Penalize for exporter errors
        let total_errors: u64 = exporter_errors.values().sum();
        let error_penalty = if total_errors == 0 {
            0.0
        } else {
            // Penalty increases with error count, capped at 0.3
            (total_errors as f64 / 100.0).min(0.3)
        };

        (component_score - error_penalty).max(0.0)
    }

    /// Check if monitoring system is healthy
    pub fn is_healthy(&self) -> bool {
        let report = self.generate_health_report();
        report.health_score > 0.8
            && report.open_circuit_breakers == 0
            && report.config_reload_success_rate > 0.9
    }

    /// Clear all statistics (useful for testing)
    pub fn reset(&self) {
        self.inner.metrics_collected.store(0, Ordering::Relaxed);
        self.inner.spans_created.store(0, Ordering::Relaxed);
        self.inner.alerts_fired.store(0, Ordering::Relaxed);

        self.inner.exporter_errors.write().clear();
        self.inner.health_check_durations.write().clear();
        self.inner.circuit_breaker_states.write().clear();
        self.inner.component_health.write().clear();
        self.inner.resource_usage_history.write().clear();

        let mut config_stats = self.inner.config_reloads.write();
        *config_stats = ConfigReloadStats {
            total_attempts: 0,
            successful_reloads: 0,
            failed_reloads: 0,
            last_reload_time: None,
        };
    }
}

impl Default for SelfMonitoring {
    fn default() -> Self {
        Self::new()
    }
}

/// Comprehensive self-monitoring report
#[derive(Debug, Clone)]
pub struct SelfMonitoringReport {
    /// When this report was generated
    pub timestamp: Instant,
    /// System uptime
    pub uptime: Duration,
    /// Overall health score (0.0 to 1.0)
    pub health_score: f64,
    /// Total metrics collected
    pub metrics_collected: u64,
    /// Total exporter errors across all exporters
    pub total_exporter_errors: u64,
    /// Number of components being monitored
    pub component_count: usize,
    /// Number of healthy components
    pub healthy_components: usize,
    /// Current performance metrics
    pub performance_metrics: PerformanceMetrics,
    /// Configuration reload success rate (0.0 to 1.0)
    pub config_reload_success_rate: f64,
    /// Number of open circuit breakers
    pub open_circuit_breakers: usize,
}

impl std::fmt::Display for SelfMonitoringReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "=== Self-Monitoring Report ===")?;
        writeln!(f, "Uptime: {:.1}s", self.uptime.as_secs_f64())?;
        writeln!(f, "Health Score: {:.2}", self.health_score)?;
        writeln!(f, "Metrics Collected: {}", self.metrics_collected)?;
        writeln!(f, "Exporter Errors: {}", self.total_exporter_errors)?;
        writeln!(
            f,
            "Component Health: {}/{}",
            self.healthy_components, self.component_count
        )?;
        writeln!(
            f,
            "Config Reload Rate: {:.1}%",
            self.config_reload_success_rate * 100.0
        )?;
        writeln!(f, "Open Circuit Breakers: {}", self.open_circuit_breakers)?;

        if self.health_score < 0.8 {
            writeln!(f, "⚠️  MONITORING SYSTEM DEGRADED")?;
        } else {
            writeln!(f, "✅ MONITORING SYSTEM HEALTHY")?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn self_monitoring_creation() {
        let monitoring = SelfMonitoring::new();
        assert_eq!(monitoring.get_metrics_collected_count(), 0);
        assert!(monitoring.get_uptime() > Duration::ZERO);
    }

    #[test]
    fn metric_collection_tracking() {
        let monitoring = SelfMonitoring::new();

        monitoring.record_metric_collected("test_counter");
        monitoring.record_metric_collected("test_gauge");

        assert_eq!(monitoring.get_metrics_collected_count(), 2);
    }

    #[test]
    fn exporter_error_tracking() {
        let monitoring = SelfMonitoring::new();

        monitoring.record_exporter_error("prometheus", "connection_failed");
        monitoring.record_exporter_error("prometheus", "timeout");
        monitoring.record_exporter_error("jaeger", "auth_failed");

        let errors = monitoring.get_exporter_error_counts();
        assert_eq!(errors.get("prometheus"), Some(&2));
        assert_eq!(errors.get("jaeger"), Some(&1));
    }

    #[test]
    fn health_check_duration_tracking() {
        let monitoring = SelfMonitoring::new();

        monitoring.record_health_check_duration("database", Duration::from_millis(50));
        monitoring.record_health_check_duration("database", Duration::from_millis(60));
        monitoring.record_health_check_duration("redis", Duration::from_millis(10));

        let averages = monitoring.get_health_check_averages();
        assert!(averages.contains_key("database"));
        assert!(averages.contains_key("redis"));
        assert_eq!(averages.get("database").unwrap().as_millis(), 55);
    }

    #[test]
    fn circuit_breaker_state_tracking() {
        let monitoring = SelfMonitoring::new();

        monitoring.record_circuit_breaker_state("db_health", CircuitBreakerState::Closed);
        monitoring.record_circuit_breaker_state("api_health", CircuitBreakerState::Open);

        let states = monitoring.get_circuit_breaker_states();
        assert_eq!(states.get("db_health"), Some(&CircuitBreakerState::Closed));
        assert_eq!(states.get("api_health"), Some(&CircuitBreakerState::Open));
    }

    #[test]
    fn component_health_tracking() {
        let monitoring = SelfMonitoring::new();

        monitoring.update_component_health("metrics_collector", HealthStatus::Healthy);
        monitoring.update_component_health("trace_exporter", HealthStatus::Degraded);
        monitoring.update_component_health("health_checker", HealthStatus::Unhealthy);

        let health = monitoring.get_component_health();
        assert_eq!(
            health.get("metrics_collector"),
            Some(&HealthStatus::Healthy)
        );
        assert_eq!(health.get("trace_exporter"), Some(&HealthStatus::Degraded));
        assert_eq!(health.get("health_checker"), Some(&HealthStatus::Unhealthy));
    }

    #[test]
    fn config_reload_stats() {
        let monitoring = SelfMonitoring::new();

        monitoring.record_config_reload(true);
        monitoring.record_config_reload(true);
        monitoring.record_config_reload(false);

        let stats = monitoring.get_config_reload_stats();
        assert_eq!(stats.total_attempts, 3);
        assert_eq!(stats.successful_reloads, 2);
        assert_eq!(stats.failed_reloads, 1);
    }

    #[test]
    fn health_report_generation() {
        let monitoring = SelfMonitoring::new();

        // Add some test data
        monitoring.record_metric_collected("test_metric");
        monitoring.update_component_health("test_component", HealthStatus::Healthy);
        monitoring.record_config_reload(true);

        let report = monitoring.generate_health_report();
        assert_eq!(report.metrics_collected, 1);
        assert_eq!(report.component_count, 1);
        assert_eq!(report.healthy_components, 1);
        assert_eq!(report.config_reload_success_rate, 1.0);
        assert!(report.health_score > 0.9);
    }

    #[test]
    fn health_score_calculation() {
        let monitoring = SelfMonitoring::new();

        // All healthy components should give high score
        monitoring.update_component_health("comp1", HealthStatus::Healthy);
        monitoring.update_component_health("comp2", HealthStatus::Healthy);

        let report1 = monitoring.generate_health_report();
        assert!(report1.health_score > 0.9);

        // Add unhealthy component
        monitoring.update_component_health("comp3", HealthStatus::Unhealthy);

        let report2 = monitoring.generate_health_report();
        assert!(report2.health_score < report1.health_score);

        // Add exporter errors
        monitoring.record_exporter_error("prometheus", "error");

        let report3 = monitoring.generate_health_report();
        assert!(report3.health_score < report2.health_score);
    }

    #[test]
    fn is_healthy_check() {
        let monitoring = SelfMonitoring::new();

        // Should be healthy initially (no components)
        assert!(monitoring.is_healthy());

        // Add healthy component
        monitoring.update_component_health("test", HealthStatus::Healthy);
        monitoring.record_config_reload(true);
        assert!(monitoring.is_healthy());

        // Add open circuit breaker
        monitoring.record_circuit_breaker_state("test_cb", CircuitBreakerState::Open);
        assert!(!monitoring.is_healthy());
    }

    #[test]
    fn resource_usage_tracking() {
        let monitoring = SelfMonitoring::new();

        monitoring.record_resource_usage(50.0, 1024 * 1024 * 512, 0, 0, 0);
        monitoring.record_resource_usage(60.0, 1024 * 1024 * 600, 0, 0, 0);

        let recent = monitoring.get_recent_resource_usage(10);
        assert_eq!(recent.len(), 2);
        assert_eq!(recent[1].cpu_percent, 60.0);

        let perf_metrics = monitoring.get_performance_metrics();
        assert_eq!(perf_metrics.cpu_usage, Some(60.0));
    }

    #[test]
    fn reset_functionality() {
        let monitoring = SelfMonitoring::new();

        // Add some data
        monitoring.record_metric_collected("test");
        monitoring.record_exporter_error("prom", "error");
        monitoring.update_component_health("comp", HealthStatus::Healthy);

        assert_eq!(monitoring.get_metrics_collected_count(), 1);
        assert_eq!(monitoring.get_exporter_error_counts().len(), 1);
        assert_eq!(monitoring.get_component_health().len(), 1);

        // Reset
        monitoring.reset();

        assert_eq!(monitoring.get_metrics_collected_count(), 0);
        assert_eq!(monitoring.get_exporter_error_counts().len(), 0);
        assert_eq!(monitoring.get_component_health().len(), 0);
    }
}
