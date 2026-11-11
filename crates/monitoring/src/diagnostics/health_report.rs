//! Comprehensive health reporting with system diagnostics and troubleshooting information
//!
//! This module provides detailed health reporting capabilities that combine monitoring
//! data with diagnostic insights to help with troubleshooting and system analysis.

use crate::diagnostics::{CorrelationSummary, ErrorCorrelationEngine, ErrorPattern};
use crate::error::MonitoringError;
use crate::health::{HealthCheckRegistry, HealthSummary};
use orcher_common::monitoring::HealthStatus;
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Comprehensive health report containing system diagnostics and troubleshooting information
#[derive(Debug, Clone)]
pub struct HealthReport {
    /// Timestamp when the report was generated
    pub generated_at: u64,
    /// Overall system health summary
    pub health_summary: HealthSummary,
    /// Detailed component status information
    pub component_details: Vec<ComponentHealthDetails>,
    /// System performance metrics
    pub performance_metrics: PerformanceMetrics,
    /// Error correlation analysis
    pub error_analysis: ErrorAnalysis,
    /// Troubleshooting recommendations
    pub recommendations: Vec<TroubleshootingRecommendation>,
    /// System resource utilization
    pub resource_utilization: ResourceUtilization,
}

/// Detailed health information for a specific component
#[derive(Debug, Clone)]
pub struct ComponentHealthDetails {
    /// Component name
    pub name: String,
    /// Current health status
    pub status: HealthStatus,
    /// Last check timestamp
    pub last_check: u64,
    /// Response time for the last check
    pub response_time_ms: u64,
    /// Number of consecutive failures
    pub failure_count: u32,
    /// Component-specific metadata
    pub metadata: HashMap<String, String>,
    /// Recent error messages
    pub recent_errors: Vec<String>,
    /// Health trend (improving, stable, degrading)
    pub trend: HealthTrend,
}

/// System performance metrics included in health reports
#[derive(Debug, Clone)]
pub struct PerformanceMetrics {
    /// Average response time across all health checks
    pub avg_response_time_ms: f64,
    /// 95th percentile response time
    pub p95_response_time_ms: f64,
    /// Success rate percentage
    pub success_rate: f64,
    /// Total number of health checks performed
    pub total_checks: u64,
    /// Uptime percentage
    pub uptime_percentage: f64,
    /// System load metrics
    pub system_load: SystemLoadMetrics,
}

/// Error analysis section of the health report
#[derive(Debug, Clone)]
pub struct ErrorAnalysis {
    /// Error correlation summary
    pub correlation_summary: CorrelationSummary,
    /// Detected error patterns
    pub patterns: Vec<ErrorPattern>,
    /// Most frequent error types
    pub frequent_errors: Vec<FrequentError>,
    /// Critical errors requiring immediate attention
    pub critical_errors: Vec<CriticalError>,
}

/// Troubleshooting recommendation for addressing issues
#[derive(Debug, Clone)]
pub struct TroubleshootingRecommendation {
    /// Severity level of the recommendation
    pub severity: RecommendationSeverity,
    /// Component or area the recommendation applies to
    pub component: String,
    /// Issue description
    pub issue: String,
    /// Recommended action to take
    pub action: String,
    /// Expected impact of taking the action
    pub expected_impact: String,
    /// Priority level (1-5, where 1 is highest)
    pub priority: u8,
}

/// System resource utilization information
#[derive(Debug, Clone)]
pub struct ResourceUtilization {
    /// CPU usage percentage
    pub cpu_usage_percent: f64,
    /// Memory usage percentage
    pub memory_usage_percent: f64,
    /// Disk usage percentage
    pub disk_usage_percent: f64,
    /// Network I/O statistics
    pub network_io: NetworkIOStats,
    /// Thread/connection pool utilization
    pub pool_utilization: PoolUtilization,
    /// System load metrics
    pub system_load: SystemLoadMetrics,
}

/// Health trend indicator
#[derive(Debug, Clone, PartialEq)]
pub enum HealthTrend {
    /// Health is improving
    Improving,
    /// Health is stable
    Stable,
    /// Health is degrading
    Degrading,
    /// Insufficient data to determine trend
    Unknown,
}

/// Recommendation severity levels
#[derive(Debug, Clone, PartialEq)]
pub enum RecommendationSeverity {
    /// Critical issue requiring immediate attention
    Critical,
    /// High priority issue
    High,
    /// Medium priority issue
    Medium,
    /// Low priority optimization
    Low,
    /// Informational recommendation
    Info,
}

/// Frequent error information
#[derive(Debug, Clone)]
pub struct FrequentError {
    /// Error type or category
    pub error_type: String,
    /// Number of occurrences
    pub count: usize,
    /// Percentage of total errors
    pub percentage: f64,
    /// Sample error message
    pub sample_message: String,
}

/// Critical error requiring immediate attention
#[derive(Debug, Clone)]
pub struct CriticalError {
    /// Component where the error occurred
    pub component: String,
    /// Error message
    pub message: String,
    /// When the error first occurred
    pub first_seen: u64,
    /// When the error was last seen
    pub last_seen: u64,
    /// Number of occurrences
    pub occurrence_count: usize,
    /// Impact assessment
    pub impact: String,
}

/// System load metrics
#[derive(Debug, Clone)]
pub struct SystemLoadMetrics {
    /// 1-minute load average
    pub load_1min: f64,
    /// 5-minute load average
    pub load_5min: f64,
    /// 15-minute load average
    pub load_15min: f64,
    /// Number of CPU cores
    pub cpu_cores: u32,
}

/// Network I/O statistics
#[derive(Debug, Clone, Default)]
pub struct NetworkIOStats {
    /// Bytes received per second
    pub bytes_received_per_sec: u64,
    /// Bytes sent per second
    pub bytes_sent_per_sec: u64,
    /// Packets received per second
    pub packets_received_per_sec: u64,
    /// Packets sent per second
    pub packets_sent_per_sec: u64,
}

/// Connection/thread pool utilization
#[derive(Debug, Clone)]
pub struct PoolUtilization {
    /// Database connection pool usage
    pub database_pool_percent: f64,
    /// HTTP connection pool usage
    pub http_pool_percent: f64,
    /// Worker thread pool usage
    pub worker_pool_percent: f64,
}

/// Health report generator for creating comprehensive diagnostic reports
pub struct HealthReportGenerator {
    /// Health check registry for gathering health data
    health_registry: Option<HealthCheckRegistry>,
    /// Error correlation engine for error analysis
    error_engine: Option<ErrorCorrelationEngine>,
    /// Configuration for report generation
    config: HealthReportConfig,
}

/// Configuration for health report generation
#[derive(Debug, Clone)]
pub struct HealthReportConfig {
    /// Include performance metrics in the report
    pub include_performance_metrics: bool,
    /// Include error analysis in the report
    pub include_error_analysis: bool,
    /// Include troubleshooting recommendations
    pub include_recommendations: bool,
    /// Include resource utilization metrics
    pub include_resource_utilization: bool,
    /// Maximum number of recommendations to include
    pub max_recommendations: usize,
    /// Error analysis time window in seconds
    pub error_analysis_window_secs: u64,
    /// Critical error threshold count
    pub critical_error_threshold: usize,
    /// Performance degradation threshold (ms)
    pub performance_threshold_ms: u64,
    /// Resource usage alert thresholds
    pub cpu_threshold_percent: f64,
    pub memory_threshold_percent: f64,
    pub disk_threshold_percent: f64,
}

impl Default for HealthReportConfig {
    fn default() -> Self {
        Self {
            include_performance_metrics: true,
            include_error_analysis: true,
            include_recommendations: true,
            include_resource_utilization: true,
            max_recommendations: 10,
            error_analysis_window_secs: 3600, // 1 hour
            critical_error_threshold: 5,
            performance_threshold_ms: 1000,
            cpu_threshold_percent: 80.0,
            memory_threshold_percent: 85.0,
            disk_threshold_percent: 90.0,
        }
    }
}

impl HealthReportGenerator {
    /// Create a new health report generator
    pub fn new() -> Self {
        Self {
            health_registry: None,
            error_engine: None,
            config: HealthReportConfig::default(),
        }
    }

    /// Create a health report generator with custom configuration
    pub fn with_config(config: HealthReportConfig) -> Self {
        Self {
            health_registry: None,
            error_engine: None,
            config,
        }
    }

    /// Set the health check registry
    pub fn with_health_registry(mut self, registry: HealthCheckRegistry) -> Self {
        self.health_registry = Some(registry);
        self
    }

    /// Set the error correlation engine
    pub fn with_error_engine(mut self, engine: ErrorCorrelationEngine) -> Self {
        self.error_engine = Some(engine);
        self
    }

    /// Generate a comprehensive health report
    pub async fn generate_report(&mut self) -> Result<HealthReport, MonitoringError> {
        let generated_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Gather health summary
        let health_summary = self.gather_health_summary().await?;

        // Gather component details
        let component_details = self.gather_component_details().await?;

        // Gather performance metrics
        let performance_metrics = if self.config.include_performance_metrics {
            self.gather_performance_metrics(&component_details)
        } else {
            PerformanceMetrics::default()
        };

        // Perform error analysis
        let error_analysis = if self.config.include_error_analysis {
            self.perform_error_analysis()
        } else {
            ErrorAnalysis::default()
        };

        // Generate troubleshooting recommendations
        let recommendations = if self.config.include_recommendations {
            self.generate_recommendations(&component_details, &error_analysis)
        } else {
            Vec::new()
        };

        // Gather resource utilization
        let resource_utilization = if self.config.include_resource_utilization {
            self.gather_resource_utilization()
        } else {
            ResourceUtilization::default()
        };

        Ok(HealthReport {
            generated_at,
            health_summary,
            component_details,
            performance_metrics,
            error_analysis,
            recommendations,
            resource_utilization,
        })
    }

    /// Gather overall health summary
    async fn gather_health_summary(&self) -> Result<HealthSummary, MonitoringError> {
        if let Some(ref registry) = self.health_registry {
            let status = registry.check_all().await?;
            Ok(HealthSummary {
                critical_checks: status.summary.critical_checks,
                healthy_critical: status.summary.healthy_critical,
                important_checks: status.summary.important_checks,
                healthy_important: status.summary.healthy_important,
                informational_checks: status.summary.informational_checks,
                healthy_informational: status.summary.healthy_informational,
                total_checks: status.summary.total_checks,
                healthy_total: status.summary.healthy_total,
            })
        } else {
            Ok(HealthSummary::default())
        }
    }

    /// Gather detailed component health information
    async fn gather_component_details(
        &self,
    ) -> Result<Vec<ComponentHealthDetails>, MonitoringError> {
        // In a real implementation, this would gather actual component details
        // For now, return simulated data
        Ok(vec![ComponentHealthDetails {
            name: "database".to_string(),
            status: HealthStatus::Healthy,
            last_check: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            response_time_ms: 50,
            failure_count: 0,
            metadata: [("pool_size".to_string(), "10".to_string())]
                .iter()
                .cloned()
                .collect(),
            recent_errors: Vec::new(),
            trend: HealthTrend::Stable,
        }])
    }

    /// Gather performance metrics from component details
    fn gather_performance_metrics(
        &self,
        components: &[ComponentHealthDetails],
    ) -> PerformanceMetrics {
        if components.is_empty() {
            return PerformanceMetrics::default();
        }

        let total_response_time: u64 = components.iter().map(|c| c.response_time_ms).sum();
        let avg_response_time_ms = total_response_time as f64 / components.len() as f64;

        let mut response_times: Vec<u64> = components.iter().map(|c| c.response_time_ms).collect();
        response_times.sort_unstable();
        let p95_index = (response_times.len() as f64 * 0.95) as usize;
        let p95_response_time_ms = *response_times.get(p95_index).unwrap_or(&0) as f64;

        let healthy_count = components
            .iter()
            .filter(|c| c.status == HealthStatus::Healthy)
            .count();
        let success_rate = if components.is_empty() {
            100.0
        } else {
            (healthy_count as f64 / components.len() as f64) * 100.0
        };

        PerformanceMetrics {
            avg_response_time_ms,
            p95_response_time_ms,
            success_rate,
            total_checks: components.len() as u64,
            uptime_percentage: success_rate, // Simplified calculation
            system_load: SystemLoadMetrics::default(),
        }
    }

    /// Perform error analysis using the error correlation engine
    fn perform_error_analysis(&mut self) -> ErrorAnalysis {
        if let Some(ref mut engine) = self.error_engine {
            let correlation_summary = engine.get_correlation_summary();
            let patterns = engine.detect_error_patterns();

            // Generate frequent errors from patterns
            let frequent_errors = patterns
                .iter()
                .take(5)
                .map(|pattern| FrequentError {
                    error_type: format!("{}:{}", pattern.component, pattern.operation),
                    count: pattern.occurrence_count,
                    percentage: if correlation_summary.total_errors > 0 {
                        (pattern.occurrence_count as f64 / correlation_summary.total_errors as f64)
                            * 100.0
                    } else {
                        0.0
                    },
                    sample_message: pattern.sample_errors.first().cloned().unwrap_or_default(),
                })
                .collect();

            // Generate critical errors from recent high-impact errors
            let critical_errors = Self::extract_critical_errors_from_engine(
                engine,
                self.config.error_analysis_window_secs,
            );

            ErrorAnalysis {
                correlation_summary,
                patterns,
                frequent_errors,
                critical_errors,
            }
        } else {
            ErrorAnalysis::default()
        }
    }

    /// Extract critical errors from the error correlation engine
    fn extract_critical_errors_from_engine(
        engine: &mut ErrorCorrelationEngine,
        window_secs: u64,
    ) -> Vec<CriticalError> {
        let recent_errors = engine.get_recent_errors(Duration::from_secs(window_secs));
        let mut critical_errors = Vec::new();

        for error in recent_errors {
            // Consider errors critical if they occur frequently or affect critical components
            let is_critical = error.correlation.component.contains("database")
                || error.correlation.component.contains("security")
                || error.correlation.component.contains("auth")
                || error.correlation.operation.contains("critical")
                || error.age.as_secs() < 300; // Errors in last 5 minutes

            if is_critical {
                critical_errors.push(CriticalError {
                    component: error.correlation.component.clone(),
                    message: error.error.to_string(),
                    first_seen: error.correlation.timestamp,
                    last_seen: error.correlation.timestamp,
                    occurrence_count: 1,
                    impact: if error.correlation.component.contains("database") {
                        "High - affects data persistence".to_string()
                    } else if error.correlation.component.contains("security") {
                        "Critical - security vulnerability".to_string()
                    } else {
                        "Medium - service degradation".to_string()
                    },
                });
            }
        }

        // Deduplicate and aggregate by component and message
        let mut deduplicated: HashMap<String, CriticalError> = HashMap::new();
        for error in critical_errors {
            let key = format!("{}:{}", error.component, error.message);
            if let Some(existing) = deduplicated.get_mut(&key) {
                existing.occurrence_count += 1;
                existing.last_seen = error.last_seen.max(existing.last_seen);
            } else {
                deduplicated.insert(key, error);
            }
        }

        deduplicated.into_values().collect()
    }

    /// Generate troubleshooting recommendations based on health data and errors
    fn generate_recommendations(
        &self,
        components: &[ComponentHealthDetails],
        error_analysis: &ErrorAnalysis,
    ) -> Vec<TroubleshootingRecommendation> {
        let mut recommendations = Vec::new();

        // Check for unhealthy components
        for component in components {
            if component.status != HealthStatus::Healthy {
                recommendations.push(TroubleshootingRecommendation {
                    severity: RecommendationSeverity::High,
                    component: component.name.clone(),
                    issue: format!("Component '{}' is unhealthy", component.name),
                    action: format!("Check {} logs and configuration", component.name),
                    expected_impact: "Restore component health and prevent cascading failures"
                        .to_string(),
                    priority: 2,
                });
            }

            // Check for high response times using configured threshold
            if component.response_time_ms > self.config.performance_threshold_ms {
                recommendations.push(TroubleshootingRecommendation {
                    severity: RecommendationSeverity::Medium,
                    component: component.name.clone(),
                    issue: format!("High response time: {}ms", component.response_time_ms),
                    action: "Investigate performance bottlenecks and optimize queries".to_string(),
                    expected_impact: "Improve system responsiveness and user experience"
                        .to_string(),
                    priority: 3,
                });
            }
        }

        // Check for error patterns using configured threshold
        for pattern in &error_analysis.patterns {
            if pattern.occurrence_count >= self.config.critical_error_threshold {
                recommendations.push(TroubleshootingRecommendation {
                    severity: RecommendationSeverity::High,
                    component: pattern.component.clone(),
                    issue: format!("Recurring error pattern in {} operation", pattern.operation),
                    action: "Investigate root cause and implement fix".to_string(),
                    expected_impact: "Reduce error rate and improve system stability".to_string(),
                    priority: 1,
                });
            }
        }

        // Check resource utilization and add recommendations
        self.add_resource_recommendations(&mut recommendations);

        // Limit recommendations to configured maximum
        recommendations.truncate(self.config.max_recommendations);
        recommendations
    }

    /// Add resource utilization recommendations
    fn add_resource_recommendations(
        &self,
        recommendations: &mut Vec<TroubleshootingRecommendation>,
    ) {
        let resource_util = self.gather_resource_utilization();

        if resource_util.cpu_usage_percent > self.config.cpu_threshold_percent {
            recommendations.push(TroubleshootingRecommendation {
                severity: RecommendationSeverity::High,
                component: "system".to_string(),
                issue: format!("High CPU usage: {:.1}%", resource_util.cpu_usage_percent),
                action: "Scale up resources or optimize CPU-intensive processes".to_string(),
                expected_impact: "Reduce system load and improve performance".to_string(),
                priority: 2,
            });
        }

        if resource_util.memory_usage_percent > self.config.memory_threshold_percent {
            recommendations.push(TroubleshootingRecommendation {
                severity: RecommendationSeverity::High,
                component: "system".to_string(),
                issue: format!(
                    "High memory usage: {:.1}%",
                    resource_util.memory_usage_percent
                ),
                action: "Increase memory allocation or check for memory leaks".to_string(),
                expected_impact: "Prevent out-of-memory conditions".to_string(),
                priority: 1,
            });
        }

        if resource_util.disk_usage_percent > self.config.disk_threshold_percent {
            recommendations.push(TroubleshootingRecommendation {
                severity: RecommendationSeverity::Critical,
                component: "system".to_string(),
                issue: format!("High disk usage: {:.1}%", resource_util.disk_usage_percent),
                action: "Clean up disk space or expand storage capacity".to_string(),
                expected_impact: "Prevent disk full conditions that could cause system failure"
                    .to_string(),
                priority: 1,
            });
        }
    }

    /// Gather system resource utilization data
    fn gather_resource_utilization(&self) -> ResourceUtilization {
        #[cfg(feature = "system-metrics")]
        {
            use sysinfo::{CpuExt, DiskExt, NetworkExt, NetworksExt, System, SystemExt};

            let mut system = System::new_all();
            system.refresh_all();

            // CPU usage
            let cpu_usage_percent = system.global_cpu_info().cpu_usage() as f64;

            // Memory usage
            let memory_usage_percent = if system.total_memory() > 0 {
                (system.used_memory() as f64 / system.total_memory() as f64) * 100.0
            } else {
                0.0
            };

            // Disk usage - use the root disk or first available
            let disk_usage_percent = system
                .disks()
                .iter()
                .next()
                .map(|disk| {
                    let total = disk.total_space();
                    if total > 0 {
                        let used = total - disk.available_space();
                        (used as f64 / total as f64) * 100.0
                    } else {
                        0.0
                    }
                })
                .unwrap_or(0.0);

            // Network I/O
            let (bytes_received, bytes_sent) =
                system
                    .networks()
                    .iter()
                    .fold((0, 0), |(acc_rx, acc_tx), (_, data)| {
                        (acc_rx + data.received(), acc_tx + data.transmitted())
                    });

            // System load
            let load_avg = system.load_average();
            let cpu_cores = system.cpus().len() as f64;

            ResourceUtilization {
                cpu_usage_percent,
                memory_usage_percent,
                disk_usage_percent,
                network_io: NetworkIOStats {
                    bytes_received_per_sec: bytes_received,
                    bytes_sent_per_sec: bytes_sent,
                    packets_received_per_sec: 0, // sysinfo doesn't provide packet counts
                    packets_sent_per_sec: 0,
                },
                pool_utilization: self.gather_pool_utilization(),
                system_load: SystemLoadMetrics {
                    load_1min: load_avg.one,
                    load_5min: load_avg.five,
                    load_15min: load_avg.fifteen,
                    cpu_cores: cpu_cores as u32,
                },
            }
        }
        #[cfg(not(feature = "system-metrics"))]
        {
            ResourceUtilization::default()
        }
    }

    /// Gather connection and thread pool utilization metrics
    #[cfg(feature = "system-metrics")]
    fn gather_pool_utilization(&self) -> PoolUtilization {
        let mut database_pool_percent = 0.0;
        let mut http_pool_percent = 0.0;
        let mut worker_pool_percent = 0.0;

        // Try to get pool metrics from health registry if available
        if let Some(ref registry) = self.health_registry {
            let cached_results = registry.get_cached_results();

            // Look for database health check to get connection pool info
            for check_name in &["database", "storage", "postgres", "mysql", "redis"] {
                if let Some(result) = cached_results.get(*check_name) {
                    if let Some(pool_usage) = result.metadata.get("connection_pool_usage") {
                        if let Ok(usage) = pool_usage.parse::<f64>() {
                            database_pool_percent = usage;
                            break;
                        }
                    }
                }
            }

            // Look for worker pool metrics
            for check_name in &["worker", "worker_pool", "workers"] {
                if let Some(result) = cached_results.get(*check_name) {
                    if let Some(pool_usage) = result.metadata.get("worker_pool_usage") {
                        if let Ok(usage) = pool_usage.parse::<f64>() {
                            worker_pool_percent = usage;
                            break;
                        }
                    }
                }
            }

            // Look for HTTP connection pool metrics
            for check_name in &["http", "http_pool", "web_server"] {
                if let Some(result) = cached_results.get(*check_name) {
                    if let Some(pool_usage) = result.metadata.get("http_pool_usage") {
                        if let Ok(usage) = pool_usage.parse::<f64>() {
                            http_pool_percent = usage;
                            break;
                        }
                    }
                }
            }
        }

        PoolUtilization {
            database_pool_percent,
            http_pool_percent,
            worker_pool_percent,
        }
    }
}

impl Default for HealthReportGenerator {
    fn default() -> Self {
        Self::new()
    }
}

// Default implementations for various structs
impl Default for PerformanceMetrics {
    fn default() -> Self {
        Self {
            avg_response_time_ms: 0.0,
            p95_response_time_ms: 0.0,
            success_rate: 100.0,
            total_checks: 0,
            uptime_percentage: 100.0,
            system_load: SystemLoadMetrics::default(),
        }
    }
}

impl Default for ErrorAnalysis {
    fn default() -> Self {
        Self {
            correlation_summary: CorrelationSummary {
                total_errors: 0,
                errors_with_traces: 0,
                trace_correlation_rate: 0.0,
                component_error_counts: HashMap::new(),
            },
            patterns: Vec::new(),
            frequent_errors: Vec::new(),
            critical_errors: Vec::new(),
        }
    }
}

impl Default for ResourceUtilization {
    fn default() -> Self {
        Self {
            cpu_usage_percent: 0.0,
            memory_usage_percent: 0.0,
            disk_usage_percent: 0.0,
            network_io: NetworkIOStats::default(),
            pool_utilization: PoolUtilization::default(),
            system_load: SystemLoadMetrics::default(),
        }
    }
}

impl Default for SystemLoadMetrics {
    fn default() -> Self {
        Self {
            load_1min: 0.0,
            load_5min: 0.0,
            load_15min: 0.0,
            cpu_cores: 1,
        }
    }
}

impl Default for PoolUtilization {
    fn default() -> Self {
        Self {
            database_pool_percent: 0.0,
            http_pool_percent: 0.0,
            worker_pool_percent: 0.0,
        }
    }
}

impl HealthReport {
    /// Generate a human-readable summary of the health report
    pub fn format_summary(&self) -> String {
        let status_emoji = if self.health_summary.healthy_total == self.health_summary.total_checks
        {
            "✅"
        } else if self.health_summary.healthy_critical == self.health_summary.critical_checks {
            "⚠️"
        } else {
            "❌"
        };

        format!(
            "{} System Health: {}/{} checks healthy\n\
            Critical: {}/{} healthy | Important: {}/{} healthy | Info: {}/{} healthy\n\
            Performance: {:.1}ms avg response, {:.1}% success rate\n\
            Resource Usage: CPU {:.1}%, Memory {:.1}%, Disk {:.1}%\n\
            Error Analysis: {} patterns detected, {} critical errors",
            status_emoji,
            self.health_summary.healthy_total,
            self.health_summary.total_checks,
            self.health_summary.healthy_critical,
            self.health_summary.critical_checks,
            self.health_summary.healthy_important,
            self.health_summary.important_checks,
            self.health_summary.healthy_informational,
            self.health_summary.informational_checks,
            self.performance_metrics.avg_response_time_ms,
            self.performance_metrics.success_rate,
            self.resource_utilization.cpu_usage_percent,
            self.resource_utilization.memory_usage_percent,
            self.resource_utilization.disk_usage_percent,
            self.error_analysis.patterns.len(),
            self.error_analysis.critical_errors.len()
        )
    }

    /// Get the overall health status
    pub fn overall_status(&self) -> HealthStatus {
        if self.health_summary.healthy_critical != self.health_summary.critical_checks {
            HealthStatus::Unhealthy
        } else if self.health_summary.healthy_total != self.health_summary.total_checks {
            HealthStatus::Degraded
        } else {
            HealthStatus::Healthy
        }
    }

    /// Get top priority recommendations
    pub fn get_top_recommendations(&self, limit: usize) -> Vec<&TroubleshootingRecommendation> {
        let mut sorted_recommendations: Vec<_> = self.recommendations.iter().collect();
        sorted_recommendations.sort_by_key(|r| r.priority);
        sorted_recommendations.into_iter().take(limit).collect()
    }

    /// Generate actionable summary for alerts
    pub fn alert_summary(&self) -> Option<String> {
        let status = self.overall_status();

        match status {
            HealthStatus::Healthy => None,
            HealthStatus::Degraded | HealthStatus::Unhealthy | HealthStatus::Unknown => {
                let top_issues: Vec<String> = self
                    .get_top_recommendations(3)
                    .iter()
                    .map(|r| format!("• {}: {}", r.component, r.issue))
                    .collect();

                Some(format!(
                    "🚨 System Status: {:?}\n\
                    Top Issues:\n{}\n\
                    Immediate Actions Required: {}",
                    status,
                    top_issues.join("\n"),
                    self.recommendations
                        .iter()
                        .filter(|r| matches!(r.severity, RecommendationSeverity::Critical))
                        .count()
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_report_generator_creation() {
        let generator = HealthReportGenerator::new();
        assert!(generator.health_registry.is_none());
        assert!(generator.error_engine.is_none());
    }

    #[test]
    fn test_health_report_config_defaults() {
        let config = HealthReportConfig::default();
        assert!(config.include_performance_metrics);
        assert!(config.include_error_analysis);
        assert!(config.include_recommendations);
        assert!(config.include_resource_utilization);
        assert_eq!(config.max_recommendations, 10);
        assert_eq!(config.error_analysis_window_secs, 3600);
    }

    #[test]
    fn test_performance_metrics_calculation() {
        let generator = HealthReportGenerator::new();
        let components = vec![
            ComponentHealthDetails {
                name: "test1".to_string(),
                status: HealthStatus::Healthy,
                last_check: 0,
                response_time_ms: 100,
                failure_count: 0,
                metadata: HashMap::new(),
                recent_errors: Vec::new(),
                trend: HealthTrend::Stable,
            },
            ComponentHealthDetails {
                name: "test2".to_string(),
                status: HealthStatus::Unhealthy,
                last_check: 0,
                response_time_ms: 200,
                failure_count: 1,
                metadata: HashMap::new(),
                recent_errors: Vec::new(),
                trend: HealthTrend::Degrading,
            },
        ];

        let metrics = generator.gather_performance_metrics(&components);
        assert_eq!(metrics.avg_response_time_ms, 150.0);
        assert_eq!(metrics.success_rate, 50.0);
        assert_eq!(metrics.total_checks, 2);
    }

    #[test]
    fn test_troubleshooting_recommendation_creation() {
        let rec = TroubleshootingRecommendation {
            severity: RecommendationSeverity::High,
            component: "database".to_string(),
            issue: "Connection timeout".to_string(),
            action: "Check network connectivity".to_string(),
            expected_impact: "Restore database access".to_string(),
            priority: 1,
        };

        assert_eq!(rec.severity, RecommendationSeverity::High);
        assert_eq!(rec.component, "database");
        assert_eq!(rec.priority, 1);
    }

    #[test]
    fn test_health_trend_enum() {
        assert_eq!(HealthTrend::Improving, HealthTrend::Improving);
        assert_ne!(HealthTrend::Stable, HealthTrend::Degrading);
    }

    #[test]
    fn test_recommendation_severity_enum() {
        assert_eq!(
            RecommendationSeverity::Critical,
            RecommendationSeverity::Critical
        );
        assert_ne!(RecommendationSeverity::High, RecommendationSeverity::Low);
    }

    #[test]
    fn test_default_implementations() {
        let metrics = PerformanceMetrics::default();
        assert_eq!(metrics.success_rate, 100.0);
        assert_eq!(metrics.total_checks, 0);

        let analysis = ErrorAnalysis::default();
        assert_eq!(analysis.correlation_summary.total_errors, 0);
        assert!(analysis.patterns.is_empty());

        let utilization = ResourceUtilization::default();
        assert_eq!(utilization.cpu_usage_percent, 0.0);
        assert_eq!(utilization.memory_usage_percent, 0.0);
    }

    #[tokio::test]
    async fn test_generate_report_basic() {
        let mut generator = HealthReportGenerator::new();
        let result = generator.generate_report().await;
        assert!(result.is_ok());

        let report = result.unwrap();
        assert!(report.generated_at > 0);
        assert_eq!(report.health_summary.total_checks, 0);
    }

    #[test]
    fn test_component_health_details_creation() {
        let details = ComponentHealthDetails {
            name: "test_component".to_string(),
            status: HealthStatus::Healthy,
            last_check: 1234567890,
            response_time_ms: 50,
            failure_count: 0,
            metadata: HashMap::new(),
            recent_errors: Vec::new(),
            trend: HealthTrend::Stable,
        };

        assert_eq!(details.name, "test_component");
        assert_eq!(details.status, HealthStatus::Healthy);
        assert_eq!(details.response_time_ms, 50);
        assert_eq!(details.trend, HealthTrend::Stable);
    }

    #[test]
    fn test_frequent_error_creation() {
        let error = FrequentError {
            error_type: "connection_timeout".to_string(),
            count: 5,
            percentage: 25.0,
            sample_message: "Connection timed out after 30s".to_string(),
        };

        assert_eq!(error.error_type, "connection_timeout");
        assert_eq!(error.count, 5);
        assert_eq!(error.percentage, 25.0);
    }

    #[test]
    fn test_health_report_formatting_methods() {
        let report = HealthReport {
            generated_at: 1234567890,
            health_summary: HealthSummary {
                critical_checks: 2,
                healthy_critical: 2,
                important_checks: 3,
                healthy_important: 2,
                informational_checks: 1,
                healthy_informational: 1,
                total_checks: 6,
                healthy_total: 5,
            },
            component_details: Vec::new(),
            performance_metrics: PerformanceMetrics {
                avg_response_time_ms: 150.0,
                success_rate: 85.5,
                ..Default::default()
            },
            error_analysis: ErrorAnalysis {
                patterns: vec![ErrorPattern {
                    component: "database".to_string(),
                    operation: "query".to_string(),
                    occurrence_count: 3,
                    sample_errors: vec!["timeout".to_string()],
                }],
                critical_errors: vec![CriticalError {
                    component: "auth".to_string(),
                    message: "login failed".to_string(),
                    first_seen: 1234567890,
                    last_seen: 1234567890,
                    occurrence_count: 1,
                    impact: "High".to_string(),
                }],
                ..Default::default()
            },
            recommendations: vec![
                TroubleshootingRecommendation {
                    severity: RecommendationSeverity::Critical,
                    component: "database".to_string(),
                    issue: "High latency".to_string(),
                    action: "Optimize queries".to_string(),
                    expected_impact: "Improve performance".to_string(),
                    priority: 1,
                },
                TroubleshootingRecommendation {
                    severity: RecommendationSeverity::Medium,
                    component: "cache".to_string(),
                    issue: "Low hit rate".to_string(),
                    action: "Tune cache settings".to_string(),
                    expected_impact: "Better cache efficiency".to_string(),
                    priority: 2,
                },
            ],
            resource_utilization: ResourceUtilization {
                cpu_usage_percent: 75.5,
                memory_usage_percent: 60.2,
                disk_usage_percent: 45.8,
                ..Default::default()
            },
        };

        // Test format_summary
        let summary = report.format_summary();
        assert!(summary.contains("5/6 checks healthy"));
        assert!(summary.contains("CPU 75.5%"));
        assert!(summary.contains("1 patterns detected"));

        // Test overall_status
        assert_eq!(report.overall_status(), HealthStatus::Degraded);

        // Test get_top_recommendations
        let top_recs = report.get_top_recommendations(1);
        assert_eq!(top_recs.len(), 1);
        assert_eq!(top_recs[0].priority, 1);

        // Test alert_summary
        let alert = report.alert_summary();
        assert!(alert.is_some());
        assert!(alert.unwrap().contains("System Status: Degraded"));
    }

    #[test]
    fn test_health_report_config_customization() {
        let config = HealthReportConfig {
            critical_error_threshold: 3,
            performance_threshold_ms: 500,
            cpu_threshold_percent: 70.0,
            memory_threshold_percent: 75.0,
            disk_threshold_percent: 85.0,
            ..Default::default()
        };

        assert_eq!(config.critical_error_threshold, 3);
        assert_eq!(config.performance_threshold_ms, 500);
        assert_eq!(config.cpu_threshold_percent, 70.0);
    }
}
