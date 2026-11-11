//! Logging format configuration and types
//!
//! Defines the available logging formats and configuration options
//! for structured logging across ORCHER services.

use serde::{Deserialize, Serialize};
use std::str::FromStr;

/// Logging output format
///
/// Determines how log messages are formatted and displayed.
/// Each format is optimized for different use cases.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    /// Compact single-line format (default for development)
    ///
    /// Produces clean, readable single-line output ideal for development
    /// and debugging. Includes timestamp, level, target, and message.
    ///
    /// Example:
    /// ```text
    /// 2024-01-15T10:30:45Z INFO orchestrator: Starting service version=0.1.0
    /// ```
    #[default]
    Compact,

    /// Structured JSON format (recommended for production)
    ///
    /// Produces machine-readable JSON output suitable for log aggregation
    /// systems like ELK, Splunk, or CloudWatch. Includes all metadata.
    ///
    /// Example:
    /// ```json
    /// {"timestamp":"2024-01-15T10:30:45Z","level":"INFO","target":"orchestrator","message":"Starting service","fields":{"version":"0.1.0"}}
    /// ```
    Json,

    /// Pretty multi-line format (for debugging)
    ///
    /// Produces verbose, colorized multi-line output with full context.
    /// Includes file names, line numbers, and span information.
    /// Best used during active debugging sessions.
    ///
    /// Example:
    /// ```text
    ///   2024-01-15T10:30:45Z  INFO orchestrator
    ///     at src/main.rs:42
    ///     in orchestrator::main
    ///   Starting service
    ///     version: 0.1.0
    /// ```
    Pretty,

    /// Bunyan-compatible JSON format
    ///
    /// Produces JSON output compatible with the Bunyan log format
    /// used by Node.js applications. Useful for mixed-language stacks.
    Bunyan,
}

impl LogFormat {
    /// Returns true if this format is suitable for production
    pub fn is_production_ready(&self) -> bool {
        matches!(self, LogFormat::Json | LogFormat::Bunyan)
    }

    /// Returns true if this format includes ANSI color codes
    pub fn uses_colors(&self) -> bool {
        matches!(self, LogFormat::Pretty)
    }

    /// Returns true if this format is machine-readable (structured)
    pub fn is_structured(&self) -> bool {
        matches!(self, LogFormat::Json | LogFormat::Bunyan)
    }

    /// Returns the default format for the given environment
    pub fn default_for_env(is_production: bool) -> Self {
        if is_production {
            LogFormat::Json
        } else {
            LogFormat::Compact
        }
    }
}

impl FromStr for LogFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "compact" => Ok(LogFormat::Compact),
            "json" => Ok(LogFormat::Json),
            "pretty" => Ok(LogFormat::Pretty),
            "bunyan" => Ok(LogFormat::Bunyan),
            _ => Err(format!(
                "Invalid log format '{}'. Valid options: compact, json, pretty, bunyan",
                s
            )),
        }
    }
}

impl std::fmt::Display for LogFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogFormat::Compact => write!(f, "compact"),
            LogFormat::Json => write!(f, "json"),
            LogFormat::Pretty => write!(f, "pretty"),
            LogFormat::Bunyan => write!(f, "bunyan"),
        }
    }
}

/// Logging configuration options
///
/// Provides fine-grained control over logging behavior including
/// format, level, filtering, and metadata inclusion.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    /// Output format for log messages
    pub format: LogFormat,

    /// Default log level (trace, debug, info, warn, error)
    pub level: String,

    /// Service name for log identification
    pub service_name: String,

    /// Custom environment filter override
    ///
    /// If provided, this completely replaces the default filter.
    /// Format: `target=level,target2=level2,...`
    ///
    /// Example: `orchestrator=debug,orcher_core=info,sqlx=warn`
    pub custom_filter: Option<String>,

    /// Additional targets to include in the default filter
    ///
    /// These will be added to the automatically generated filter
    /// with the default log level.
    pub additional_targets: Vec<String>,

    /// Include file name and line number in logs
    pub include_location: bool,

    /// Include thread ID in logs
    pub include_thread_id: bool,

    /// Include thread name in logs
    pub include_thread_name: bool,

    /// Include span information in logs
    pub include_span_info: bool,

    /// Include target (module path) in logs
    pub include_target: bool,

    /// Enable ANSI colors (auto-detected for TTY, disabled for structured formats)
    pub enable_colors: bool,
}

impl LoggingConfig {
    /// Create a new logging configuration
    pub fn new(service_name: impl Into<String>) -> Self {
        Self {
            format: LogFormat::default(),
            level: "info".to_string(),
            service_name: service_name.into(),
            custom_filter: None,
            additional_targets: Vec::new(),
            include_location: false,
            include_thread_id: false,
            include_thread_name: false,
            include_span_info: true,
            include_target: true,
            enable_colors: false, // Auto-enabled for TTY in init; false here for portability
        }
    }

    /// Set the log format
    pub fn with_format(mut self, format: LogFormat) -> Self {
        self.format = format;
        self
    }

    /// Set the log level
    pub fn with_level(mut self, level: impl Into<String>) -> Self {
        self.level = level.into();
        self
    }

    /// Set a custom environment filter
    pub fn with_custom_filter(mut self, filter: impl Into<String>) -> Self {
        self.custom_filter = Some(filter.into());
        self
    }

    /// Add additional targets to the default filter
    pub fn with_additional_targets(mut self, targets: Vec<String>) -> Self {
        self.additional_targets = targets;
        self
    }

    /// Add a single additional target
    pub fn add_target(mut self, target: impl Into<String>) -> Self {
        self.additional_targets.push(target.into());
        self
    }

    /// Enable file location (file name and line number)
    pub fn with_location(mut self, enable: bool) -> Self {
        self.include_location = enable;
        self
    }

    /// Enable thread information
    pub fn with_thread_info(mut self, enable: bool) -> Self {
        self.include_thread_id = enable;
        self.include_thread_name = enable;
        self
    }

    /// Enable span information
    pub fn with_span_info(mut self, enable: bool) -> Self {
        self.include_span_info = enable;
        self
    }

    /// Enable ANSI colors
    pub fn with_colors(mut self, enable: bool) -> Self {
        self.enable_colors = enable;
        self
    }

    /// Configure for production environment
    ///
    /// Sets sensible defaults for production:
    /// - JSON format
    /// - Info level
    /// - Minimal metadata overhead
    pub fn for_production(service_name: impl Into<String>) -> Self {
        Self::new(service_name)
            .with_format(LogFormat::Json)
            .with_level("info")
            .with_location(false)
            .with_thread_info(false)
            .with_colors(false)
    }

    /// Configure for development environment
    ///
    /// Sets sensible defaults for development:
    /// - Compact format
    /// - Debug level
    /// - Readable output with colors (auto-detected for TTY)
    pub fn for_development(service_name: impl Into<String>) -> Self {
        Self::new(service_name)
            .with_format(LogFormat::Compact)
            .with_level("debug")
            .with_location(false)
            .with_thread_info(false)
            .with_colors(true) // Will be auto-enabled for TTY in init
    }

    /// Configure for debugging
    ///
    /// Sets maximum verbosity for debugging:
    /// - Pretty format
    /// - Trace level
    /// - All metadata enabled
    /// - Colors enabled (auto-detected for TTY)
    pub fn for_debugging(service_name: impl Into<String>) -> Self {
        Self::new(service_name)
            .with_format(LogFormat::Pretty)
            .with_level("trace")
            .with_location(true)
            .with_thread_info(true)
            .with_colors(true) // Will be auto-enabled for TTY in init
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self::new("orcher")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_format_from_str() {
        assert_eq!("compact".parse::<LogFormat>().unwrap(), LogFormat::Compact);
        assert_eq!("json".parse::<LogFormat>().unwrap(), LogFormat::Json);
        assert_eq!("pretty".parse::<LogFormat>().unwrap(), LogFormat::Pretty);
        assert_eq!("bunyan".parse::<LogFormat>().unwrap(), LogFormat::Bunyan);

        // Case insensitive
        assert_eq!("JSON".parse::<LogFormat>().unwrap(), LogFormat::Json);
        assert_eq!("COMPACT".parse::<LogFormat>().unwrap(), LogFormat::Compact);

        // Invalid
        assert!("invalid".parse::<LogFormat>().is_err());
    }

    #[test]
    fn test_log_format_display() {
        assert_eq!(LogFormat::Compact.to_string(), "compact");
        assert_eq!(LogFormat::Json.to_string(), "json");
        assert_eq!(LogFormat::Pretty.to_string(), "pretty");
        assert_eq!(LogFormat::Bunyan.to_string(), "bunyan");
    }

    #[test]
    fn test_log_format_properties() {
        assert!(LogFormat::Json.is_production_ready());
        assert!(LogFormat::Bunyan.is_production_ready());
        assert!(!LogFormat::Compact.is_production_ready());
        assert!(!LogFormat::Pretty.is_production_ready());

        assert!(LogFormat::Pretty.uses_colors());
        assert!(!LogFormat::Json.uses_colors());

        assert!(LogFormat::Json.is_structured());
        assert!(LogFormat::Bunyan.is_structured());
        assert!(!LogFormat::Compact.is_structured());
    }

    #[test]
    fn test_log_format_default_for_env() {
        assert_eq!(LogFormat::default_for_env(true), LogFormat::Json);
        assert_eq!(LogFormat::default_for_env(false), LogFormat::Compact);
    }

    #[test]
    fn test_logging_config_builder() {
        let config = LoggingConfig::new("test-service")
            .with_format(LogFormat::Json)
            .with_level("debug")
            .with_location(true)
            .with_thread_info(true);

        assert_eq!(config.service_name, "test-service");
        assert_eq!(config.format, LogFormat::Json);
        assert_eq!(config.level, "debug");
        assert!(config.include_location);
        assert!(config.include_thread_id);
        assert!(config.include_thread_name);
    }

    #[test]
    fn test_logging_config_presets() {
        let prod = LoggingConfig::for_production("prod-service");
        assert_eq!(prod.format, LogFormat::Json);
        assert_eq!(prod.level, "info");
        assert!(!prod.include_location);
        assert!(!prod.enable_colors);

        let dev = LoggingConfig::for_development("dev-service");
        assert_eq!(dev.format, LogFormat::Compact);
        assert_eq!(dev.level, "debug");
        assert!(dev.enable_colors);

        let debug = LoggingConfig::for_debugging("debug-service");
        assert_eq!(debug.format, LogFormat::Pretty);
        assert_eq!(debug.level, "trace");
        assert!(debug.include_location);
        assert!(debug.include_thread_id);
        assert!(debug.include_thread_name);
    }

    #[test]
    fn test_logging_config_additional_targets() {
        let config = LoggingConfig::new("test")
            .add_target("custom_crate")
            .add_target("another_crate");

        assert_eq!(config.additional_targets.len(), 2);
        assert!(config
            .additional_targets
            .contains(&"custom_crate".to_string()));
    }
}
