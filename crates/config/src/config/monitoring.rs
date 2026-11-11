//! Monitoring configuration (metrics, tracing, health checks, alerting).

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

/// Monitoring configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MonitoringConfig {
    /// Metrics configuration
    pub metrics: MetricsConfig,

    /// Tracing configuration
    pub tracing: TracingConfig,

    /// Health check configuration
    pub health_checks: HealthCheckConfig,

    /// Alerting configuration
    pub alerting: AlertingConfig,

    /// Logging configuration
    pub logging: LoggingConfig,

    /// Performance monitoring configuration
    pub performance: PerformanceMonitoringConfig,
}

/// Metrics configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsConfig {
    /// Enable metrics collection
    pub enabled: bool,

    /// Metrics collection interval in seconds
    pub collection_interval_secs: u64,

    /// Metrics retention period in days
    pub retention_days: u32,

    /// Metrics exporters
    pub exporters: Vec<MetricsExporter>,

    /// Custom metrics definitions
    pub custom_metrics: HashMap<String, CustomMetric>,

    /// Enable system metrics
    pub system_metrics_enabled: bool,

    /// Enable application metrics
    pub application_metrics_enabled: bool,

    /// Metrics aggregation settings
    pub aggregation: MetricsAggregation,
}

/// Tracing configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TracingConfig {
    /// Enable distributed tracing
    pub enabled: bool,

    /// Tracing service name
    pub service_name: String,

    /// Sampling rate (0.0 to 1.0)
    pub sampling_rate: f64,

    /// Trace exporters
    pub exporters: Vec<TraceExporter>,

    /// Maximum span attributes
    pub max_span_attributes: u32,

    /// Maximum span events
    pub max_span_events: u32,

    /// Trace batch settings
    pub batch_settings: TraceBatchSettings,

    /// Enable trace correlation
    pub correlation_enabled: bool,
}

/// Health check configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheckConfig {
    /// Enable health checks
    pub enabled: bool,

    /// Health check interval in seconds
    pub check_interval_secs: u64,

    /// Health check timeout in seconds
    pub timeout_secs: u64,

    /// Health check endpoint path
    pub endpoint_path: String,

    /// Health checks to perform
    pub checks: Vec<HealthCheck>,

    /// Grace period for startup checks in seconds
    pub startup_grace_period_secs: u64,

    /// Include detailed status information
    pub include_details: bool,
}

/// Alerting configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertingConfig {
    /// Enable alerting
    pub enabled: bool,

    /// Alert channels
    pub channels: Vec<AlertChannel>,

    /// Alert rules
    pub rules: Vec<AlertRule>,

    /// Alert cooldown period in seconds
    pub cooldown_period_secs: u64,

    /// Alert escalation settings
    pub escalation: AlertEscalation,

    /// Enable alert grouping
    pub grouping_enabled: bool,
}

/// Logging configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    /// Log level
    pub level: LogLevel,

    /// Log format
    pub format: LogFormat,

    /// Log outputs
    pub outputs: Vec<LogOutput>,

    /// Enable structured logging
    pub structured_logging: bool,

    /// Log correlation ID
    pub correlation_id_enabled: bool,

    /// Log retention settings
    pub retention: LogRetention,

    /// Enable log sampling
    pub sampling_enabled: bool,

    /// Log sampling rate
    pub sampling_rate: f64,
}

/// Performance monitoring configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceMonitoringConfig {
    /// Enable performance monitoring
    pub enabled: bool,

    /// CPU monitoring settings
    pub cpu_monitoring: CpuMonitoring,

    /// Memory monitoring settings
    pub memory_monitoring: MemoryMonitoring,

    /// Disk monitoring settings
    pub disk_monitoring: DiskMonitoring,

    /// Network monitoring settings
    pub network_monitoring: NetworkMonitoring,

    /// Performance thresholds
    pub thresholds: PerformanceThresholds,
}

/// Metrics exporter enumeration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetricsExporter {
    /// Prometheus exporter
    Prometheus {
        /// Prometheus endpoint
        endpoint: String,
        /// Export interval in seconds
        interval_secs: u64,
    },
    /// StatsD exporter
    StatsD {
        /// StatsD server address
        address: String,
        /// Metric prefix
        prefix: String,
    },
    /// Console exporter for debugging
    Console,
    /// File exporter
    File {
        /// Output file path
        path: PathBuf,
        /// Rotation settings
        rotation: FileRotation,
    },
}

/// Custom metric definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomMetric {
    /// Metric name
    pub name: String,

    /// Metric type
    pub metric_type: MetricType,

    /// Metric description
    pub description: String,

    /// Metric labels
    pub labels: Vec<String>,

    /// Metric unit
    pub unit: Option<String>,
}

/// Metrics aggregation settings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsAggregation {
    /// Enable aggregation
    pub enabled: bool,

    /// Aggregation window in seconds
    pub window_secs: u64,

    /// Aggregation functions
    pub functions: Vec<AggregationFunction>,
}

/// Trace exporter enumeration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TraceExporter {
    /// Jaeger exporter
    Jaeger {
        /// Jaeger endpoint
        endpoint: String,
        /// Authentication token
        token: Option<String>,
    },
    /// Zipkin exporter
    Zipkin {
        /// Zipkin endpoint
        endpoint: String,
    },
    /// OTLP exporter
    OTLP {
        /// OTLP endpoint
        endpoint: String,
        /// Headers for authentication
        headers: HashMap<String, String>,
    },
    /// Console exporter for debugging
    Console,
}

/// Trace batch settings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceBatchSettings {
    /// Maximum batch size
    pub max_batch_size: u32,

    /// Export timeout in milliseconds
    pub export_timeout_ms: u64,

    /// Scheduled delay in milliseconds
    pub scheduled_delay_ms: u64,
}

/// Health check definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheck {
    /// Check name
    pub name: String,

    /// Check type
    pub check_type: HealthCheckType,

    /// Check timeout in seconds
    pub timeout_secs: u64,

    /// Check interval in seconds
    pub interval_secs: u64,

    /// Number of retries before marking as unhealthy
    pub retries: u32,
}

/// Alert channel enumeration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlertChannel {
    /// Email notifications
    Email {
        /// SMTP server configuration
        smtp_server: String,
        /// Email recipients
        recipients: Vec<String>,
    },
    /// Slack notifications
    Slack {
        /// Slack webhook URL
        webhook_url: String,
        /// Slack channel
        channel: String,
    },
    /// PagerDuty notifications
    PagerDuty {
        /// PagerDuty integration key
        integration_key: String,
    },
    /// Webhook notifications
    Webhook {
        /// Webhook URL
        url: String,
        /// Custom headers
        headers: HashMap<String, String>,
    },
}

/// Alert rule definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertRule {
    /// Rule name
    pub name: String,

    /// Rule condition
    pub condition: String,

    /// Alert severity
    pub severity: AlertSeverity,

    /// Alert channels for this rule
    pub channels: Vec<String>,

    /// Rule evaluation interval in seconds
    pub evaluation_interval_secs: u64,

    /// Alert description
    pub description: String,
}

/// Alert escalation settings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertEscalation {
    /// Enable escalation
    pub enabled: bool,

    /// Escalation levels
    pub levels: Vec<EscalationLevel>,

    /// Maximum escalation level
    pub max_level: u32,
}

/// Log output enumeration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogOutput {
    /// Console output
    Console,
    /// File output
    File {
        /// File path
        path: PathBuf,
        /// Rotation settings
        rotation: FileRotation,
    },
    /// Syslog output
    Syslog {
        /// Syslog facility
        facility: String,
    },
    /// Network output
    Network {
        /// Network endpoint
        endpoint: String,
        /// Protocol
        protocol: NetworkProtocol,
    },
}

/// Log retention settings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogRetention {
    /// Retention period in days
    pub days: u32,

    /// Maximum log file size in MB
    pub max_size_mb: u64,

    /// Maximum number of log files
    pub max_files: u32,

    /// Enable compression
    pub compression_enabled: bool,
}

/// CPU monitoring settings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CpuMonitoring {
    /// Enable CPU monitoring
    pub enabled: bool,

    /// Monitoring interval in seconds
    pub interval_secs: u64,

    /// Include per-core metrics
    pub per_core_enabled: bool,
}

/// Memory monitoring settings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryMonitoring {
    /// Enable memory monitoring
    pub enabled: bool,

    /// Monitoring interval in seconds
    pub interval_secs: u64,

    /// Include detailed memory metrics
    pub detailed_metrics: bool,
}

/// Disk monitoring settings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiskMonitoring {
    /// Enable disk monitoring
    pub enabled: bool,

    /// Monitoring interval in seconds
    pub interval_secs: u64,

    /// Monitored mount points
    pub mount_points: Vec<String>,
}

/// Network monitoring settings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkMonitoring {
    /// Enable network monitoring
    pub enabled: bool,

    /// Monitoring interval in seconds
    pub interval_secs: u64,

    /// Monitored interfaces
    pub interfaces: Vec<String>,
}

/// Performance thresholds
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceThresholds {
    /// CPU usage threshold percentage
    pub cpu_threshold_percent: f64,

    /// Memory usage threshold percentage
    pub memory_threshold_percent: f64,

    /// Disk usage threshold percentage
    pub disk_threshold_percent: f64,

    /// Response time threshold in milliseconds
    pub response_time_threshold_ms: u64,
}

/// Metric type enumeration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetricType {
    /// Counter metric
    Counter,
    /// Gauge metric
    Gauge,
    /// Histogram metric
    Histogram,
    /// Summary metric
    Summary,
}

/// Aggregation function enumeration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AggregationFunction {
    /// Sum aggregation
    Sum,
    /// Average aggregation
    Average,
    /// Minimum aggregation
    Min,
    /// Maximum aggregation
    Max,
    /// Count aggregation
    Count,
    /// Percentile aggregation
    Percentile(f64),
}

/// Health check type enumeration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HealthCheckType {
    /// Database connectivity check
    Database,
    /// Redis connectivity check
    Redis,
    /// HTTP endpoint check
    Http {
        /// URL to check
        url: String,
        /// Expected status code
        expected_status: u16,
    },
    /// Custom command check
    Command {
        /// Command to execute
        command: String,
        /// Command arguments
        args: Vec<String>,
    },
    /// TCP port check
    TcpPort {
        /// Host to check
        host: String,
        /// Port to check
        port: u16,
    },
}

/// Alert severity enumeration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlertSeverity {
    /// Low severity
    Low,
    /// Medium severity
    Medium,
    /// High severity
    High,
    /// Critical severity
    Critical,
}

/// Escalation level definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EscalationLevel {
    /// Level number
    pub level: u32,

    /// Delay before escalation in seconds
    pub delay_secs: u64,

    /// Channels for this escalation level
    pub channels: Vec<String>,
}

/// Log level enumeration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogLevel {
    /// Trace level
    Trace,
    /// Debug level
    Debug,
    /// Info level
    Info,
    /// Warning level
    Warning,
    /// Error level
    Error,
    /// Critical level
    Critical,
}

/// Log format enumeration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogFormat {
    /// Plain text format
    Text,
    /// JSON format
    JSON,
    /// Structured format
    Structured,
    /// Custom format
    Custom(String),
}

/// File rotation settings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileRotation {
    /// Maximum file size in MB
    pub max_size_mb: u64,

    /// Maximum number of files
    pub max_files: u32,

    /// Enable compression
    pub compress: bool,
}

/// Network protocol enumeration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NetworkProtocol {
    /// TCP protocol
    TCP,
    /// UDP protocol
    UDP,
    /// HTTP protocol
    HTTP,
    /// HTTPS protocol
    HTTPS,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            collection_interval_secs: 60,
            retention_days: 30,
            exporters: vec![MetricsExporter::Console],
            custom_metrics: HashMap::new(),
            system_metrics_enabled: true,
            application_metrics_enabled: true,
            aggregation: MetricsAggregation {
                enabled: true,
                window_secs: 300, // 5 minutes
                functions: vec![
                    AggregationFunction::Average,
                    AggregationFunction::Max,
                    AggregationFunction::Min,
                ],
            },
        }
    }
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            service_name: "orcher".to_string(),
            sampling_rate: 0.1, // 10% sampling
            exporters: vec![TraceExporter::Console],
            max_span_attributes: 128,
            max_span_events: 128,
            batch_settings: TraceBatchSettings {
                max_batch_size: 512,
                export_timeout_ms: 30000,
                scheduled_delay_ms: 5000,
            },
            correlation_enabled: true,
        }
    }
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            check_interval_secs: 30,
            timeout_secs: 10,
            endpoint_path: "/health".to_string(),
            checks: vec![
                HealthCheck {
                    name: "database".to_string(),
                    check_type: HealthCheckType::Database,
                    timeout_secs: 5,
                    interval_secs: 30,
                    retries: 3,
                },
                HealthCheck {
                    name: "redis".to_string(),
                    check_type: HealthCheckType::Redis,
                    timeout_secs: 5,
                    interval_secs: 30,
                    retries: 3,
                },
            ],
            startup_grace_period_secs: 30,
            include_details: true,
        }
    }
}

impl Default for AlertingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            channels: vec![],
            rules: vec![],
            cooldown_period_secs: 300, // 5 minutes
            escalation: AlertEscalation {
                enabled: false,
                levels: vec![],
                max_level: 3,
            },
            grouping_enabled: true,
        }
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: LogLevel::Info,
            format: LogFormat::JSON,
            outputs: vec![LogOutput::Console],
            structured_logging: true,
            correlation_id_enabled: true,
            retention: LogRetention {
                days: 30,
                max_size_mb: 100,
                max_files: 10,
                compression_enabled: true,
            },
            sampling_enabled: false,
            sampling_rate: 1.0,
        }
    }
}

impl Default for PerformanceMonitoringConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            cpu_monitoring: CpuMonitoring {
                enabled: true,
                interval_secs: 60,
                per_core_enabled: false,
            },
            memory_monitoring: MemoryMonitoring {
                enabled: true,
                interval_secs: 60,
                detailed_metrics: false,
            },
            disk_monitoring: DiskMonitoring {
                enabled: true,
                interval_secs: 300, // 5 minutes
                mount_points: vec!["/".to_string()],
            },
            network_monitoring: NetworkMonitoring {
                enabled: true,
                interval_secs: 60,
                interfaces: vec!["eth0".to_string()],
            },
            thresholds: PerformanceThresholds {
                cpu_threshold_percent: 80.0,
                memory_threshold_percent: 85.0,
                disk_threshold_percent: 90.0,
                response_time_threshold_ms: 1000,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_monitoring_config_default() {
        let config = MonitoringConfig::default();
        assert!(config.metrics.enabled);
        assert!(config.tracing.enabled);
        assert!(config.health_checks.enabled);
        assert!(!config.alerting.enabled);
        assert!(config.performance.enabled);
    }

    #[test]
    fn test_metrics_config_default() {
        let config = MetricsConfig::default();
        assert!(config.enabled);
        assert_eq!(config.collection_interval_secs, 60);
        assert_eq!(config.retention_days, 30);
        assert!(config.system_metrics_enabled);
        assert!(config.aggregation.enabled);
    }

    #[test]
    fn test_tracing_config_default() {
        let config = TracingConfig::default();
        assert!(config.enabled);
        assert_eq!(config.service_name, "orcher");
        assert_eq!(config.sampling_rate, 0.1);
        assert!(config.correlation_enabled);
        assert_eq!(config.max_span_attributes, 128);
    }

    #[test]
    fn test_health_check_config_default() {
        let config = HealthCheckConfig::default();
        assert!(config.enabled);
        assert_eq!(config.check_interval_secs, 30);
        assert_eq!(config.endpoint_path, "/health");
        assert_eq!(config.checks.len(), 2);
        assert!(config.include_details);
    }

    #[test]
    fn test_alerting_config_default() {
        let config = AlertingConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.cooldown_period_secs, 300);
        assert!(!config.escalation.enabled);
        assert!(config.grouping_enabled);
    }

    #[test]
    fn test_logging_config_default() {
        let config = LoggingConfig::default();
        assert!(matches!(config.level, LogLevel::Info));
        assert!(matches!(config.format, LogFormat::JSON));
        assert!(config.structured_logging);
        assert!(config.correlation_id_enabled);
        assert_eq!(config.retention.days, 30);
    }

    #[test]
    fn test_performance_monitoring_config_default() {
        let config = PerformanceMonitoringConfig::default();
        assert!(config.enabled);
        assert!(config.cpu_monitoring.enabled);
        assert!(config.memory_monitoring.enabled);
        assert!(config.disk_monitoring.enabled);
        assert!(config.network_monitoring.enabled);
        assert_eq!(config.thresholds.cpu_threshold_percent, 80.0);
    }

    #[test]
    fn test_metric_types() {
        let types = vec![
            MetricType::Counter,
            MetricType::Gauge,
            MetricType::Histogram,
            MetricType::Summary,
        ];

        for metric_type in types {
            let json = serde_json::to_string(&metric_type).unwrap();
            let deserialized: MetricType = serde_json::from_str(&json).unwrap();
            assert!(matches!(
                deserialized,
                MetricType::Counter
                    | MetricType::Gauge
                    | MetricType::Histogram
                    | MetricType::Summary
            ));
        }
    }

    #[test]
    fn test_alert_severities() {
        let severities = vec![
            AlertSeverity::Low,
            AlertSeverity::Medium,
            AlertSeverity::High,
            AlertSeverity::Critical,
        ];

        for severity in severities {
            let json = serde_json::to_string(&severity).unwrap();
            let deserialized: AlertSeverity = serde_json::from_str(&json).unwrap();
            assert!(matches!(
                deserialized,
                AlertSeverity::Low
                    | AlertSeverity::Medium
                    | AlertSeverity::High
                    | AlertSeverity::Critical
            ));
        }
    }

    #[test]
    fn test_log_levels() {
        let levels = vec![
            LogLevel::Trace,
            LogLevel::Debug,
            LogLevel::Info,
            LogLevel::Warning,
            LogLevel::Error,
            LogLevel::Critical,
        ];

        for level in levels {
            let json = serde_json::to_string(&level).unwrap();
            let deserialized: LogLevel = serde_json::from_str(&json).unwrap();
            assert!(matches!(
                deserialized,
                LogLevel::Trace
                    | LogLevel::Debug
                    | LogLevel::Info
                    | LogLevel::Warning
                    | LogLevel::Error
                    | LogLevel::Critical
            ));
        }
    }

    #[test]
    fn test_health_check_types() {
        let database_check = HealthCheckType::Database;
        let redis_check = HealthCheckType::Redis;
        let http_check = HealthCheckType::Http {
            url: "http://example.com".to_string(),
            expected_status: 200,
        };

        let db_json = serde_json::to_string(&database_check).unwrap();
        let redis_json = serde_json::to_string(&redis_check).unwrap();
        let http_json = serde_json::to_string(&http_check).unwrap();

        let db_deserialized: HealthCheckType = serde_json::from_str(&db_json).unwrap();
        let redis_deserialized: HealthCheckType = serde_json::from_str(&redis_json).unwrap();
        let http_deserialized: HealthCheckType = serde_json::from_str(&http_json).unwrap();

        assert!(matches!(db_deserialized, HealthCheckType::Database));
        assert!(matches!(redis_deserialized, HealthCheckType::Redis));
        assert!(matches!(http_deserialized, HealthCheckType::Http { .. }));
    }

    #[test]
    fn test_aggregation_functions() {
        let functions = vec![
            AggregationFunction::Sum,
            AggregationFunction::Average,
            AggregationFunction::Min,
            AggregationFunction::Max,
            AggregationFunction::Count,
            AggregationFunction::Percentile(95.0),
        ];

        for function in functions {
            let json = serde_json::to_string(&function).unwrap();
            let deserialized: AggregationFunction = serde_json::from_str(&json).unwrap();
            assert!(matches!(
                deserialized,
                AggregationFunction::Sum
                    | AggregationFunction::Average
                    | AggregationFunction::Min
                    | AggregationFunction::Max
                    | AggregationFunction::Count
                    | AggregationFunction::Percentile(_)
            ));
        }
    }

    #[test]
    fn test_config_serialization() {
        let config = MonitoringConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        assert!(json.contains("metrics"));
        assert!(json.contains("tracing"));
        assert!(json.contains("health_checks"));
        assert!(json.contains("alerting"));

        let deserialized: MonitoringConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config.metrics.enabled, deserialized.metrics.enabled);
        assert_eq!(config.tracing.enabled, deserialized.tracing.enabled);
        assert_eq!(
            config.health_checks.enabled,
            deserialized.health_checks.enabled
        );
        assert_eq!(config.alerting.enabled, deserialized.alerting.enabled);
    }
}
