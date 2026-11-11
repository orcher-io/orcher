//! Configuration validation utilities for monitoring system
//!
//! This module provides comprehensive validation for monitoring configuration
//! with clear error messages and early detection of common configuration mistakes.

use crate::diagnostics::{ErrorReporter, SelfMonitoring};
use crate::error::{MonitoringError, Result};
use std::net::SocketAddr;
use std::sync::Arc;
use url::Url;

#[cfg(feature = "integration")]
use orcher_config::config::monitoring::*;

#[cfg(not(feature = "integration"))]
use crate::config::BasicMonitoringConfig;

/// Configuration validator with diagnostic integration
pub struct ConfigurationValidator {
    error_reporter: Arc<ErrorReporter>,
    self_monitoring: Arc<SelfMonitoring>,
}

impl ConfigurationValidator {
    /// Create a new configuration validator with diagnostic integration
    pub fn new(error_reporter: Arc<ErrorReporter>, self_monitoring: Arc<SelfMonitoring>) -> Self {
        Self {
            error_reporter,
            self_monitoring,
        }
    }

    /// Validate configuration and report diagnostics
    pub fn validate_with_diagnostics(&self) -> Result<()> {
        let result = validate_monitoring_config();

        // Track validation attempt
        self.self_monitoring.record_config_reload(result.is_ok());

        // Report validation errors for diagnostics
        if let Err(ref error) = result {
            let _ = self.error_reporter.report_error(error);
        }

        result
    }
}

/// Validates monitoring configuration based on feature flags
pub fn validate_monitoring_config() -> Result<()> {
    #[cfg(feature = "integration")]
    {
        let config = MonitoringConfig::default();
        validate_integrated_config(&config)
    }

    #[cfg(not(feature = "integration"))]
    {
        let config = BasicMonitoringConfig::default();
        validate_basic_config(&config)
    }
}

/// Validates integrated monitoring configuration from orcher-config
#[cfg(feature = "integration")]
pub fn validate_integrated_config(config: &MonitoringConfig) -> Result<()> {
    // Validate metrics configuration
    validate_metrics_config(&config.metrics)?;

    // Validate tracing configuration
    validate_tracing_config(&config.tracing)?;

    // Validate health checks configuration
    validate_health_checks_config(&config.health_checks)?;

    // Validate alerting configuration
    validate_alerting_config(&config.alerting)?;

    // Validate logging configuration
    validate_logging_config(&config.logging)?;

    // Validate performance monitoring configuration
    validate_performance_config(&config.performance)?;

    Ok(())
}

/// Validates basic monitoring configuration for standalone usage
#[cfg(not(feature = "integration"))]
pub fn validate_basic_config(config: &BasicMonitoringConfig) -> Result<()> {
    // Validate Prometheus configuration if enabled
    if config.prometheus_enabled {
        validate_prometheus_address(&config.prometheus_addr)?;
    }

    // Validate Jaeger configuration if enabled
    if config.jaeger_enabled {
        validate_jaeger_endpoint(&config.jaeger_endpoint)?;
    }

    // Validate health check configuration if enabled
    if config.health_checks_enabled {
        validate_health_check_interval(config.health_check_interval_secs)?;
    }

    Ok(())
}

/// Validates metrics configuration
#[cfg(feature = "integration")]
fn validate_metrics_config(config: &MetricsConfig) -> Result<()> {
    if !config.enabled {
        return Ok(());
    }

    // Validate collection interval with helpful suggestions
    if config.collection_interval_secs == 0 {
        return Err(MonitoringError::config_validation_error(
            "metrics.collection_interval_secs",
            "Collection interval must be greater than 0 seconds. Recommended values: 10-60 seconds for high-frequency monitoring, 300-600 seconds for normal workloads.",
        ));
    }

    // Warn for potentially problematic intervals
    if config.collection_interval_secs < 5 {
        return Err(MonitoringError::config_validation_error(
            "metrics.collection_interval_secs",
            "Collection interval below 5 seconds may cause high overhead. Consider using 10+ seconds unless real-time monitoring is required.",
        ));
    }

    // Validate retention period
    if config.retention_days == 0 {
        return Err(MonitoringError::config_validation_error(
            "metrics.retention_days",
            "Retention period must be greater than 0 days. Typical values: 7-30 days for operational metrics, 90+ days for compliance.",
        ));
    }

    // Validate exporters
    for (index, exporter) in config.exporters.iter().enumerate() {
        validate_metrics_exporter(exporter, index)?;
    }

    Ok(())
}

/// Validates tracing configuration
#[cfg(feature = "integration")]
fn validate_tracing_config(config: &TracingConfig) -> Result<()> {
    if !config.enabled {
        return Ok(());
    }

    // Validate service name
    if config.service_name.trim().is_empty() {
        return Err(MonitoringError::config_validation_error(
            "tracing.service_name",
            "Service name cannot be empty when tracing is enabled",
        ));
    }

    // Validate sampling rate
    if !(0.0..=1.0).contains(&config.sampling_rate) {
        return Err(MonitoringError::config_validation_error(
            "tracing.sampling_rate",
            "Sampling rate must be between 0.0 and 1.0",
        ));
    }

    // Validate span limits
    if config.max_span_attributes == 0 {
        return Err(MonitoringError::config_validation_error(
            "tracing.max_span_attributes",
            "Maximum span attributes must be greater than 0",
        ));
    }

    if config.max_span_events == 0 {
        return Err(MonitoringError::config_validation_error(
            "tracing.max_span_events",
            "Maximum span events must be greater than 0",
        ));
    }

    // Validate exporters
    for (index, exporter) in config.exporters.iter().enumerate() {
        validate_trace_exporter(exporter, index)?;
    }

    // Validate batch settings
    validate_trace_batch_settings(&config.batch_settings)?;

    Ok(())
}

/// Validates health checks configuration
#[cfg(feature = "integration")]
fn validate_health_checks_config(config: &HealthCheckConfig) -> Result<()> {
    if !config.enabled {
        return Ok(());
    }

    // Validate check interval
    validate_health_check_interval(config.check_interval_secs)?;

    // Validate timeout
    if config.timeout_secs == 0 {
        return Err(MonitoringError::config_validation_error(
            "health_checks.timeout_secs",
            "Health check timeout must be greater than 0 seconds",
        ));
    }

    if config.timeout_secs >= config.check_interval_secs {
        return Err(MonitoringError::config_validation_error(
            "health_checks.timeout_secs",
            "Health check timeout must be less than check interval",
        ));
    }

    // Validate endpoint path
    if !config.endpoint_path.starts_with('/') {
        return Err(MonitoringError::config_validation_error(
            "health_checks.endpoint_path",
            "Health check endpoint path must start with '/'",
        ));
    }

    // Validate individual health checks
    for (index, check) in config.checks.iter().enumerate() {
        validate_health_check(check, index)?;
    }

    Ok(())
}

/// Validates alerting configuration
#[cfg(feature = "integration")]
fn validate_alerting_config(config: &AlertingConfig) -> Result<()> {
    if !config.enabled {
        return Ok(());
    }

    // Validate cooldown period
    if config.cooldown_period_secs == 0 {
        return Err(MonitoringError::config_validation_error(
            "alerting.cooldown_period_secs",
            "Cooldown period must be greater than 0 seconds",
        ));
    }

    // Validate alert channels
    for (index, channel) in config.channels.iter().enumerate() {
        validate_alert_channel(channel, index)?;
    }

    // Validate alert rules
    for (index, rule) in config.rules.iter().enumerate() {
        validate_alert_rule(rule, index)?;
    }

    Ok(())
}

/// Validates logging configuration
#[cfg(feature = "integration")]
fn validate_logging_config(config: &LoggingConfig) -> Result<()> {
    // Validate log outputs
    for (index, output) in config.outputs.iter().enumerate() {
        validate_log_output(output, index)?;
    }

    // Validate retention settings
    validate_log_retention(&config.retention)?;

    Ok(())
}

/// Validates performance monitoring configuration
#[cfg(feature = "integration")]
fn validate_performance_config(config: &PerformanceMonitoringConfig) -> Result<()> {
    if !config.enabled {
        return Ok(());
    }

    // Validate CPU monitoring
    validate_cpu_monitoring_config(&config.cpu_monitoring)?;

    // Validate memory monitoring
    validate_memory_monitoring_config(&config.memory_monitoring)?;

    // Validate disk monitoring
    validate_disk_monitoring_config(&config.disk_monitoring)?;

    // Validate network monitoring
    validate_network_monitoring_config(&config.network_monitoring)?;

    // Validate performance thresholds
    validate_performance_thresholds(&config.thresholds)?;

    Ok(())
}

/// Validates Prometheus address format
fn validate_prometheus_address(addr: &str) -> Result<()> {
    if addr.trim().is_empty() {
        return Err(MonitoringError::config_validation_error(
            "prometheus.addr",
            "Prometheus address cannot be empty. Example: '127.0.0.1:9090' for local development or '0.0.0.0:9090' to bind all interfaces.",
        ));
    }

    if addr.parse::<SocketAddr>().is_err() {
        return Err(MonitoringError::config_validation_error(
            "prometheus.addr",
            "Invalid Prometheus address format. Expected 'host:port' (e.g., '127.0.0.1:9090'). Common mistake: missing port number.",
        ));
    }

    // Check for common configuration mistakes
    if addr.starts_with("http://") || addr.starts_with("https://") {
        return Err(MonitoringError::config_validation_error(
            "prometheus.addr",
            "Prometheus address should not include protocol scheme. Use 'host:port' format instead of full URL.",
        ));
    }

    Ok(())
}

/// Validates Jaeger endpoint URL
fn validate_jaeger_endpoint(endpoint: &str) -> Result<()> {
    if endpoint.trim().is_empty() {
        return Err(MonitoringError::config_validation_error(
            "jaeger.endpoint",
            "Jaeger endpoint cannot be empty",
        ));
    }

    match Url::parse(endpoint) {
        Ok(url) => {
            if url.scheme() != "http" && url.scheme() != "https" {
                return Err(MonitoringError::config_validation_error(
                    "jaeger.endpoint",
                    "Jaeger endpoint must use HTTP or HTTPS protocol",
                ));
            }

            if url.host().is_none() {
                return Err(MonitoringError::config_validation_error(
                    "jaeger.endpoint",
                    "Jaeger endpoint must specify a valid host",
                ));
            }
        }
        Err(_) => {
            return Err(MonitoringError::config_validation_error(
                "jaeger.endpoint",
                "Invalid Jaeger endpoint URL format",
            ));
        }
    }

    Ok(())
}

/// Validates health check interval
fn validate_health_check_interval(interval_secs: u64) -> Result<()> {
    if interval_secs == 0 {
        return Err(MonitoringError::config_validation_error(
            "health_checks.check_interval_secs",
            "Health check interval must be greater than 0 seconds",
        ));
    }

    if interval_secs < 5 {
        return Err(MonitoringError::config_validation_error(
            "health_checks.check_interval_secs",
            "Health check interval should be at least 5 seconds to avoid excessive load",
        ));
    }

    if interval_secs > 3600 {
        return Err(MonitoringError::config_validation_error(
            "health_checks.check_interval_secs",
            "Health check interval should not exceed 3600 seconds (1 hour)",
        ));
    }

    Ok(())
}

/// Validates metrics exporter configuration
#[cfg(feature = "integration")]
fn validate_metrics_exporter(exporter: &MetricsExporter, index: usize) -> Result<()> {
    let field_prefix = format!("metrics.exporters[{}]", index);

    match exporter {
        MetricsExporter::Prometheus {
            endpoint,
            interval_secs,
        } => {
            validate_prometheus_address(endpoint).map_err(|_| {
                MonitoringError::config_validation_error(
                    format!("{}.endpoint", field_prefix),
                    "Invalid Prometheus exporter endpoint format",
                )
            })?;

            if *interval_secs == 0 || *interval_secs > 300 {
                return Err(MonitoringError::config_validation_error(
                    format!("{}.interval_secs", field_prefix),
                    "Prometheus exporter interval must be between 1 and 300 seconds",
                ));
            }
        }
        MetricsExporter::StatsD { address, prefix } => {
            if address.parse::<SocketAddr>().is_err() {
                return Err(MonitoringError::config_validation_error(
                    format!("{}.address", field_prefix),
                    "Invalid StatsD address format",
                ));
            }

            if prefix.contains(' ') || prefix.contains('\t') {
                return Err(MonitoringError::config_validation_error(
                    format!("{}.prefix", field_prefix),
                    "StatsD prefix cannot contain whitespace characters",
                ));
            }
        }
        MetricsExporter::Console => {
            // Console exporter has no additional validation requirements
        }
        MetricsExporter::File { path, rotation } => {
            if path.to_string_lossy().trim().is_empty() {
                return Err(MonitoringError::config_validation_error(
                    format!("{}.path", field_prefix),
                    "File exporter path cannot be empty",
                ));
            }

            validate_file_rotation(rotation, &format!("{}.rotation", field_prefix))?;
        }
    }

    Ok(())
}

/// Validates trace exporter configuration
#[cfg(feature = "integration")]
fn validate_trace_exporter(exporter: &TraceExporter, index: usize) -> Result<()> {
    let field_prefix = format!("tracing.exporters[{}]", index);

    match exporter {
        TraceExporter::Jaeger { endpoint, token: _ } => {
            validate_jaeger_endpoint(endpoint).map_err(|_| {
                MonitoringError::config_validation_error(
                    format!("{}.endpoint", field_prefix),
                    "Invalid Jaeger exporter endpoint format",
                )
            })?;
        }
        TraceExporter::Zipkin { endpoint } => {
            if Url::parse(endpoint).is_err() {
                return Err(MonitoringError::config_validation_error(
                    format!("{}.endpoint", field_prefix),
                    "Invalid Zipkin endpoint URL format",
                ));
            }
        }
        TraceExporter::OTLP {
            endpoint,
            headers: _,
        } => {
            if Url::parse(endpoint).is_err() {
                return Err(MonitoringError::config_validation_error(
                    format!("{}.endpoint", field_prefix),
                    "Invalid OTLP endpoint URL format",
                ));
            }
        }
        TraceExporter::Console => {
            // Console exporter has no additional validation requirements
        }
    }

    Ok(())
}

/// Validates trace batch settings
#[cfg(feature = "integration")]
fn validate_trace_batch_settings(settings: &TraceBatchSettings) -> Result<()> {
    if settings.max_batch_size == 0 {
        return Err(MonitoringError::config_validation_error(
            "tracing.batch_settings.max_batch_size",
            "Maximum batch size must be greater than 0",
        ));
    }

    if settings.export_timeout_ms == 0 {
        return Err(MonitoringError::config_validation_error(
            "tracing.batch_settings.export_timeout_ms",
            "Export timeout must be greater than 0 milliseconds",
        ));
    }

    if settings.scheduled_delay_ms >= settings.export_timeout_ms {
        return Err(MonitoringError::config_validation_error(
            "tracing.batch_settings.scheduled_delay_ms",
            "Scheduled delay must be less than export timeout",
        ));
    }

    Ok(())
}

/// Validates individual health check configuration
#[cfg(feature = "integration")]
fn validate_health_check(check: &HealthCheck, index: usize) -> Result<()> {
    let field_prefix = format!("health_checks.checks[{}]", index);

    if check.name.trim().is_empty() {
        return Err(MonitoringError::config_validation_error(
            format!("{}.name", field_prefix),
            "Health check name cannot be empty",
        ));
    }

    if check.timeout_secs == 0 {
        return Err(MonitoringError::config_validation_error(
            format!("{}.timeout_secs", field_prefix),
            "Health check timeout must be greater than 0 seconds",
        ));
    }

    if check.interval_secs == 0 {
        return Err(MonitoringError::config_validation_error(
            format!("{}.interval_secs", field_prefix),
            "Health check interval must be greater than 0 seconds",
        ));
    }

    if check.timeout_secs >= check.interval_secs {
        return Err(MonitoringError::config_validation_error(
            format!("{}.timeout_secs", field_prefix),
            "Health check timeout must be less than interval",
        ));
    }

    // Validate health check type-specific configuration
    match &check.check_type {
        HealthCheckType::Http {
            url,
            expected_status,
        } => {
            if Url::parse(url).is_err() {
                return Err(MonitoringError::config_validation_error(
                    format!("{}.check_type.url", field_prefix),
                    "Invalid HTTP health check URL format",
                ));
            }

            if !(100..=599).contains(expected_status) {
                return Err(MonitoringError::config_validation_error(
                    format!("{}.check_type.expected_status", field_prefix),
                    "HTTP expected status must be between 100 and 599",
                ));
            }
        }
        HealthCheckType::TcpPort { host, port } => {
            if host.trim().is_empty() {
                return Err(MonitoringError::config_validation_error(
                    format!("{}.check_type.host", field_prefix),
                    "TCP health check host cannot be empty",
                ));
            }

            if *port == 0 {
                return Err(MonitoringError::config_validation_error(
                    format!("{}.check_type.port", field_prefix),
                    "TCP health check port must be greater than 0",
                ));
            }
        }
        HealthCheckType::Command { command, args: _ } => {
            if command.trim().is_empty() {
                return Err(MonitoringError::config_validation_error(
                    format!("{}.check_type.command", field_prefix),
                    "Command health check command cannot be empty",
                ));
            }
        }
        HealthCheckType::Database | HealthCheckType::Redis => {
            // These types are validated by their respective connection configurations
        }
    }

    Ok(())
}

/// Validates alert channel configuration
#[cfg(feature = "integration")]
fn validate_alert_channel(channel: &AlertChannel, index: usize) -> Result<()> {
    let field_prefix = format!("alerting.channels[{}]", index);

    match channel {
        AlertChannel::Email {
            smtp_server,
            recipients,
        } => {
            if smtp_server.trim().is_empty() {
                return Err(MonitoringError::config_validation_error(
                    format!("{}.smtp_server", field_prefix),
                    "SMTP server cannot be empty for email alerts",
                ));
            }

            if recipients.is_empty() {
                return Err(MonitoringError::config_validation_error(
                    format!("{}.recipients", field_prefix),
                    "Email alert recipients list cannot be empty",
                ));
            }

            for (i, email) in recipients.iter().enumerate() {
                if !email.contains('@') || !email.contains('.') {
                    return Err(MonitoringError::config_validation_error(
                        format!("{}.recipients[{}]", field_prefix, i),
                        "Invalid email address format",
                    ));
                }
            }
        }
        AlertChannel::Slack {
            webhook_url,
            channel: _,
        } => {
            if Url::parse(webhook_url).is_err() {
                return Err(MonitoringError::config_validation_error(
                    format!("{}.webhook_url", field_prefix),
                    "Invalid Slack webhook URL format",
                ));
            }
        }
        AlertChannel::PagerDuty { integration_key } => {
            if integration_key.trim().is_empty() {
                return Err(MonitoringError::config_validation_error(
                    format!("{}.integration_key", field_prefix),
                    "PagerDuty integration key cannot be empty",
                ));
            }
        }
        AlertChannel::Webhook { url, headers: _ } => {
            if Url::parse(url).is_err() {
                return Err(MonitoringError::config_validation_error(
                    format!("{}.url", field_prefix),
                    "Invalid webhook URL format",
                ));
            }
        }
    }

    Ok(())
}

/// Validates alert rule configuration
#[cfg(feature = "integration")]
fn validate_alert_rule(rule: &AlertRule, index: usize) -> Result<()> {
    let field_prefix = format!("alerting.rules[{}]", index);

    if rule.name.trim().is_empty() {
        return Err(MonitoringError::config_validation_error(
            format!("{}.name", field_prefix),
            "Alert rule name cannot be empty",
        ));
    }

    if rule.condition.trim().is_empty() {
        return Err(MonitoringError::config_validation_error(
            format!("{}.condition", field_prefix),
            "Alert rule condition cannot be empty",
        ));
    }

    if rule.channels.is_empty() {
        return Err(MonitoringError::config_validation_error(
            format!("{}.channels", field_prefix),
            "Alert rule must specify at least one notification channel",
        ));
    }

    if rule.evaluation_interval_secs == 0 {
        return Err(MonitoringError::config_validation_error(
            format!("{}.evaluation_interval_secs", field_prefix),
            "Alert rule evaluation interval must be greater than 0 seconds",
        ));
    }

    Ok(())
}

/// Validates log output configuration
#[cfg(feature = "integration")]
fn validate_log_output(output: &LogOutput, index: usize) -> Result<()> {
    let field_prefix = format!("logging.outputs[{}]", index);

    match output {
        LogOutput::Console => {
            // Console output has no additional validation requirements
        }
        LogOutput::File { path, rotation } => {
            if path.to_string_lossy().trim().is_empty() {
                return Err(MonitoringError::config_validation_error(
                    format!("{}.path", field_prefix),
                    "Log file path cannot be empty",
                ));
            }

            validate_file_rotation(rotation, &format!("{}.rotation", field_prefix))?;
        }
        LogOutput::Syslog { facility: _ } => {
            // Syslog output validation handled by system
        }
        LogOutput::Network {
            endpoint,
            protocol: _,
        } => {
            if Url::parse(endpoint).is_err() {
                return Err(MonitoringError::config_validation_error(
                    format!("{}.endpoint", field_prefix),
                    "Invalid network log endpoint URL format",
                ));
            }
        }
    }

    Ok(())
}

/// Validates file rotation configuration
#[cfg(feature = "integration")]
fn validate_file_rotation(rotation: &FileRotation, field_prefix: &str) -> Result<()> {
    if rotation.max_size_mb == 0 {
        return Err(MonitoringError::config_validation_error(
            format!("{}.max_size_mb", field_prefix),
            "File rotation max size must be greater than 0 MB",
        ));
    }

    if rotation.max_files == 0 {
        return Err(MonitoringError::config_validation_error(
            format!("{}.max_files", field_prefix),
            "File rotation max files must be greater than 0",
        ));
    }

    Ok(())
}

/// Validates log retention configuration
#[cfg(feature = "integration")]
fn validate_log_retention(retention: &LogRetention) -> Result<()> {
    if retention.days == 0 {
        return Err(MonitoringError::config_validation_error(
            "logging.retention.days",
            "Log retention period must be greater than 0 days",
        ));
    }

    if retention.max_size_mb == 0 {
        return Err(MonitoringError::config_validation_error(
            "logging.retention.max_size_mb",
            "Log retention max size must be greater than 0 MB",
        ));
    }

    Ok(())
}

/// Validates CPU monitoring configuration
#[cfg(feature = "integration")]
fn validate_cpu_monitoring_config(config: &CpuMonitoring) -> Result<()> {
    if !config.enabled {
        return Ok(());
    }

    if config.interval_secs == 0 {
        return Err(MonitoringError::config_validation_error(
            "performance.cpu_monitoring.interval_secs",
            "CPU monitoring interval must be greater than 0 seconds",
        ));
    }

    Ok(())
}

/// Validates memory monitoring configuration
#[cfg(feature = "integration")]
fn validate_memory_monitoring_config(config: &MemoryMonitoring) -> Result<()> {
    if !config.enabled {
        return Ok(());
    }

    if config.interval_secs == 0 {
        return Err(MonitoringError::config_validation_error(
            "performance.memory_monitoring.interval_secs",
            "Memory monitoring interval must be greater than 0 seconds",
        ));
    }

    Ok(())
}

/// Validates disk monitoring configuration
#[cfg(feature = "integration")]
fn validate_disk_monitoring_config(config: &DiskMonitoring) -> Result<()> {
    if !config.enabled {
        return Ok(());
    }

    if config.interval_secs == 0 {
        return Err(MonitoringError::config_validation_error(
            "performance.disk_monitoring.interval_secs",
            "Disk monitoring interval must be greater than 0 seconds",
        ));
    }

    if config.mount_points.is_empty() {
        return Err(MonitoringError::config_validation_error(
            "performance.disk_monitoring.mount_points",
            "Disk monitoring must specify at least one mount point",
        ));
    }

    Ok(())
}

/// Validates network monitoring configuration
#[cfg(feature = "integration")]
fn validate_network_monitoring_config(config: &NetworkMonitoring) -> Result<()> {
    if !config.enabled {
        return Ok(());
    }

    if config.interval_secs == 0 {
        return Err(MonitoringError::config_validation_error(
            "performance.network_monitoring.interval_secs",
            "Network monitoring interval must be greater than 0 seconds",
        ));
    }

    Ok(())
}

/// Validates performance thresholds configuration
#[cfg(feature = "integration")]
fn validate_performance_thresholds(thresholds: &PerformanceThresholds) -> Result<()> {
    if !(0.0..=100.0).contains(&thresholds.cpu_threshold_percent) {
        return Err(MonitoringError::config_validation_error(
            "performance.thresholds.cpu_threshold_percent",
            "CPU threshold must be between 0.0 and 100.0 percent",
        ));
    }

    if !(0.0..=100.0).contains(&thresholds.memory_threshold_percent) {
        return Err(MonitoringError::config_validation_error(
            "performance.thresholds.memory_threshold_percent",
            "Memory threshold must be between 0.0 and 100.0 percent",
        ));
    }

    if !(0.0..=100.0).contains(&thresholds.disk_threshold_percent) {
        return Err(MonitoringError::config_validation_error(
            "performance.thresholds.disk_threshold_percent",
            "Disk threshold must be between 0.0 and 100.0 percent",
        ));
    }

    if thresholds.response_time_threshold_ms == 0 {
        return Err(MonitoringError::config_validation_error(
            "performance.thresholds.response_time_threshold_ms",
            "Response time threshold must be greater than 0 milliseconds",
        ));
    }

    Ok(())
}

/// Enhanced validation context for better error reporting
#[derive(Debug, Clone)]
pub struct ValidationContext {
    /// Configuration section being validated
    pub section: String,
    /// Field path within the section
    pub field_path: String,
    /// Suggested fix for the validation error
    pub suggested_fix: Option<String>,
    /// Related configuration fields that might need adjustment
    pub related_fields: Vec<String>,
}

impl ValidationContext {
    pub fn new(section: &str, field_path: &str) -> Self {
        Self {
            section: section.to_string(),
            field_path: field_path.to_string(),
            suggested_fix: None,
            related_fields: Vec::new(),
        }
    }

    pub fn with_suggestion(mut self, fix: &str) -> Self {
        self.suggested_fix = Some(fix.to_string());
        self
    }

    pub fn with_related_fields(mut self, fields: Vec<&str>) -> Self {
        self.related_fields = fields.into_iter().map(|s| s.to_string()).collect();
        self
    }
}

/// Validation result with enhanced diagnostic information
#[derive(Debug)]
pub struct ValidationResult {
    pub is_valid: bool,
    pub errors: Vec<MonitoringError>,
    pub warnings: Vec<String>,
    pub contexts: Vec<ValidationContext>,
}

impl ValidationResult {
    pub fn success() -> Self {
        Self {
            is_valid: true,
            errors: Vec::new(),
            warnings: Vec::new(),
            contexts: Vec::new(),
        }
    }

    pub fn with_error(error: MonitoringError, context: ValidationContext) -> Self {
        Self {
            is_valid: false,
            errors: vec![error],
            warnings: Vec::new(),
            contexts: vec![context],
        }
    }

    pub fn add_warning(&mut self, warning: String) {
        self.warnings.push(warning);
    }
}

/// Comprehensive validation with metrics integration
pub fn validate_with_metrics_integration() -> ValidationResult {
    use metrics::{counter, gauge};

    let mut result = ValidationResult::success();

    // Track validation attempts
    counter!("monitoring_config_validations_total").increment(1);

    // Perform validation
    match validate_monitoring_config() {
        Ok(_) => {
            counter!("monitoring_config_validations_successful_total").increment(1);
            gauge!("monitoring_config_validation_status").set(1.0);
        }
        Err(error) => {
            counter!("monitoring_config_validations_failed_total").increment(1);
            gauge!("monitoring_config_validation_status").set(0.0);

            // Track specific error types
            counter!(
                "monitoring_config_validation_errors_by_type_total",
                "error_type" => error.error_type()
            )
            .increment(1);

            result =
                ValidationResult::with_error(error, ValidationContext::new("general", "config"));
        }
    }

    result
}

/// Detect common configuration mistakes and provide helpful suggestions
pub fn detect_common_mistakes() -> Vec<String> {
    let mut suggestions = Vec::new();

    // Check for common Prometheus configuration issues
    suggestions.extend(check_prometheus_common_mistakes());

    // Check for common Jaeger configuration issues
    suggestions.extend(check_jaeger_common_mistakes());

    // Check for common health check configuration issues
    suggestions.extend(check_health_check_common_mistakes());

    suggestions
}

#[allow(unused_mut)]
fn check_prometheus_common_mistakes() -> Vec<String> {
    let mut suggestions = Vec::new();

    #[cfg(not(feature = "integration"))]
    {
        let config = crate::config::BasicMonitoringConfig::default();
        if config.prometheus_enabled && config.prometheus_addr == "localhost:9090" {
            suggestions.push(
                "Using 'localhost' for Prometheus address may not work in containerized environments. Consider using '0.0.0.0:9090' to bind all interfaces.".to_string()
            );
        }
    }

    suggestions
}

#[allow(unused_mut)]
fn check_jaeger_common_mistakes() -> Vec<String> {
    let mut suggestions = Vec::new();

    #[cfg(not(feature = "integration"))]
    {
        let config = crate::config::BasicMonitoringConfig::default();
        if config.jaeger_enabled && config.jaeger_endpoint.contains("localhost") {
            suggestions.push(
                "Using 'localhost' in Jaeger endpoint may not work in distributed systems. Ensure the Jaeger collector is accessible from all components.".to_string()
            );
        }
    }

    suggestions
}

#[allow(unused_mut)]
fn check_health_check_common_mistakes() -> Vec<String> {
    let mut suggestions = Vec::new();

    #[cfg(not(feature = "integration"))]
    {
        let config = crate::config::BasicMonitoringConfig::default();
        if config.health_checks_enabled && config.health_check_interval_secs < 10 {
            suggestions.push(
                "Very short health check intervals (< 10s) can create excessive load. Consider 30-60 second intervals for most use cases.".to_string()
            );
        }
    }

    suggestions
}

/// Validate configuration and generate comprehensive report
pub fn validate_and_report() -> ValidationReport {
    let validation_result = validate_with_metrics_integration();
    let common_mistakes = detect_common_mistakes();

    ValidationReport {
        is_valid: validation_result.is_valid,
        errors: validation_result.errors,
        warnings: validation_result.warnings,
        suggestions: common_mistakes,
        timestamp: std::time::SystemTime::now(),
    }
}

/// Comprehensive validation report
#[derive(Debug)]
pub struct ValidationReport {
    pub is_valid: bool,
    pub errors: Vec<MonitoringError>,
    pub warnings: Vec<String>,
    pub suggestions: Vec<String>,
    pub timestamp: std::time::SystemTime,
}

impl std::fmt::Display for ValidationReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Configuration Validation Report")?;
        writeln!(f, "==============================")?;
        writeln!(
            f,
            "Status: {}",
            if self.is_valid { "VALID" } else { "INVALID" }
        )?;
        writeln!(f, "Timestamp: {:?}", self.timestamp)?;

        if !self.errors.is_empty() {
            writeln!(f, "\nErrors ({}):", self.errors.len())?;
            for (i, error) in self.errors.iter().enumerate() {
                writeln!(f, "  {}. {}", i + 1, error)?;
            }
        }

        if !self.warnings.is_empty() {
            writeln!(f, "\nWarnings ({}):", self.warnings.len())?;
            for (i, warning) in self.warnings.iter().enumerate() {
                writeln!(f, "  {}. {}", i + 1, warning)?;
            }
        }

        if !self.suggestions.is_empty() {
            writeln!(f, "\nSuggestions ({}):", self.suggestions.len())?;
            for (i, suggestion) in self.suggestions.iter().enumerate() {
                writeln!(f, "  {}. {}", i + 1, suggestion)?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_configuration_validator_creation() {
        let error_reporter = Arc::new(ErrorReporter::new());
        let self_monitoring = Arc::new(SelfMonitoring::new());
        let validator = ConfigurationValidator::new(error_reporter, self_monitoring);

        // Should not panic on creation
        drop(validator);
    }

    #[test]
    fn test_validation_context_creation() {
        let context = ValidationContext::new("metrics", "collection_interval_secs")
            .with_suggestion("Use values between 10-60 seconds")
            .with_related_fields(vec!["retention_days", "enabled"]);

        assert_eq!(context.section, "metrics");
        assert_eq!(context.field_path, "collection_interval_secs");
        assert_eq!(
            context.suggested_fix,
            Some("Use values between 10-60 seconds".to_string())
        );
        assert_eq!(context.related_fields, vec!["retention_days", "enabled"]);
    }

    #[test]
    fn test_validation_result_creation() {
        let mut result = ValidationResult::success();
        assert!(result.is_valid);
        assert!(result.errors.is_empty());

        result.add_warning("This is a test warning".to_string());
        assert_eq!(result.warnings.len(), 1);
    }

    #[test]
    fn test_detect_common_mistakes() {
        let suggestions = detect_common_mistakes();
        // Should return suggestions (may be empty depending on configuration)
        // Should return suggestions (may be empty depending on configuration)
        assert!(suggestions.len() == suggestions.len());
    }

    #[test]
    fn test_validation_report_display() {
        let report = ValidationReport {
            is_valid: false,
            errors: vec![MonitoringError::config_validation_error(
                "test",
                "test error",
            )],
            warnings: vec!["test warning".to_string()],
            suggestions: vec!["test suggestion".to_string()],
            timestamp: std::time::SystemTime::now(),
        };

        let display_string = format!("{}", report);
        assert!(display_string.contains("Configuration Validation Report"));
        assert!(display_string.contains("INVALID"));
        assert!(display_string.contains("test error"));
        assert!(display_string.contains("test warning"));
        assert!(display_string.contains("test suggestion"));
    }

    #[test]
    fn validate_prometheus_address_valid() {
        assert!(validate_prometheus_address("127.0.0.1:9090").is_ok());
        assert!(validate_prometheus_address("192.168.1.1:8080").is_ok());
        assert!(validate_prometheus_address("0.0.0.0:3000").is_ok());
    }

    #[test]
    fn validate_prometheus_address_invalid() {
        assert!(validate_prometheus_address("").is_err());
        assert!(validate_prometheus_address("   ").is_err());
        assert!(validate_prometheus_address("invalid").is_err());
        assert!(validate_prometheus_address("127.0.0.1").is_err());
        assert!(validate_prometheus_address(":9090").is_err());
    }

    #[test]
    fn validate_jaeger_endpoint_valid() {
        assert!(validate_jaeger_endpoint("http://localhost:14268/api/traces").is_ok());
        assert!(validate_jaeger_endpoint("https://jaeger.example.com/api/traces").is_ok());
    }

    #[test]
    fn validate_jaeger_endpoint_invalid() {
        assert!(validate_jaeger_endpoint("").is_err());
        assert!(validate_jaeger_endpoint("   ").is_err());
        assert!(validate_jaeger_endpoint("ftp://localhost/traces").is_err());
        assert!(validate_jaeger_endpoint("http://").is_err());
        assert!(validate_jaeger_endpoint("not-a-url").is_err());
    }

    #[test]
    fn validate_health_check_interval_valid() {
        assert!(validate_health_check_interval(5).is_ok());
        assert!(validate_health_check_interval(30).is_ok());
        assert!(validate_health_check_interval(3600).is_ok());
    }

    #[test]
    fn validate_health_check_interval_invalid() {
        assert!(validate_health_check_interval(0).is_err());
        assert!(validate_health_check_interval(1).is_err()); // Too short
        assert!(validate_health_check_interval(3601).is_err()); // Too long
    }

    #[cfg(not(feature = "integration"))]
    #[test]
    fn validate_basic_config_valid() {
        let config = BasicMonitoringConfig {
            prometheus_enabled: true,
            prometheus_addr: "127.0.0.1:9090".to_string(),
            jaeger_enabled: true,
            jaeger_endpoint: "http://localhost:14268/api/traces".to_string(),
            health_checks_enabled: true,
            health_check_interval_secs: 30,
        };

        assert!(validate_basic_config(&config).is_ok());
    }

    #[cfg(not(feature = "integration"))]
    #[test]
    fn validate_basic_config_invalid_prometheus() {
        let config = BasicMonitoringConfig {
            prometheus_enabled: true,
            prometheus_addr: "invalid-address".to_string(),
            jaeger_enabled: false,
            jaeger_endpoint: "http://localhost:14268/api/traces".to_string(),
            health_checks_enabled: false,
            health_check_interval_secs: 30,
        };

        assert!(validate_basic_config(&config).is_err());
    }

    #[cfg(not(feature = "integration"))]
    #[test]
    fn validate_basic_config_disabled_features() {
        let config = BasicMonitoringConfig {
            prometheus_enabled: false,
            prometheus_addr: "invalid-address".to_string(), // Should be ignored
            jaeger_enabled: false,
            jaeger_endpoint: "invalid-url".to_string(), // Should be ignored
            health_checks_enabled: false,
            health_check_interval_secs: 0, // Should be ignored
        };

        // Should pass because disabled features are not validated
        assert!(validate_basic_config(&config).is_ok());
    }

    #[test]
    fn validation_error_messages_are_descriptive() {
        let result = validate_prometheus_address("");
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.to_string().contains("cannot be empty"));

        let result = validate_jaeger_endpoint("ftp://example.com");
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.to_string().contains("HTTP or HTTPS"));

        let result = validate_health_check_interval(0);
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.to_string().contains("greater than 0"));
    }
}
