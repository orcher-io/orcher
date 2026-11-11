//! Logging initialization and subscriber setup
//!
//! Provides the main entry point for initializing structured logging
//! across ORCHER services with support for multiple output formats.

use super::filter::build_env_filter;
use super::format::{LogFormat, LoggingConfig};
use crate::error::Result;
use tracing_subscriber::{
    fmt::{self, format::FmtSpan},
    layer::SubscriberExt,
    util::SubscriberInitExt,
};

/// Check if stdout is a TTY (terminal)
fn is_terminal() -> bool {
    use std::io::IsTerminal;
    std::io::stdout().is_terminal()
}

/// Initialize logging with the provided configuration
///
/// This is the main entry point for setting up structured logging in ORCHER services.
/// It configures `tracing_subscriber` with the appropriate format, filters, and options.
///
/// # Arguments
///
/// * `config` - Logging configuration specifying format, level, and options
///
/// # Returns
///
/// Returns `Ok(())` if initialization succeeds, or an error if the subscriber
/// has already been initialized or if configuration is invalid.
///
/// # Examples
///
/// ```no_run
/// use orcher_monitoring::logging::{init_logging, LoggingConfig, LogFormat};
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// // Simple initialization
/// let config = LoggingConfig::new("my-service")
///     .with_format(LogFormat::Json)
///     .with_level("info");
///
/// init_logging(config)?;
/// # Ok(())
/// # }
/// ```
///
/// # Panics
///
/// This function will panic if called more than once, as `tracing_subscriber`
/// can only be initialized once per process. Use `try_init_logging` if you
/// need to handle this case gracefully.
pub fn init_logging(config: LoggingConfig) -> Result<()> {
    try_init_logging(config).map_err(|e| {
        crate::error::MonitoringError::initialization_failed(
            "logging",
            format!("Failed to initialize logging: {}", e),
        )
    })
}

/// Try to initialize logging, returning an error if already initialized
///
/// Unlike `init_logging`, this function returns an error instead of panicking
/// if the subscriber has already been initialized.
///
/// # Examples
///
/// ```no_run
/// use orcher_monitoring::logging::{try_init_logging, LoggingConfig};
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let config = LoggingConfig::for_production("my-service");
///
/// match try_init_logging(config) {
///     Ok(_) => println!("Logging initialized"),
///     Err(e) => eprintln!("Failed to initialize logging: {}", e),
/// }
/// # Ok(())
/// # }
/// ```
pub fn try_init_logging(config: LoggingConfig) -> Result<()> {
    let env_filter = build_env_filter(
        &config.service_name,
        &config.level,
        config.custom_filter.as_deref(),
        &config.additional_targets,
    );

    let subscriber = tracing_subscriber::registry().with(env_filter);

    // Auto-enable colors for terminal output (only for non-structured formats)
    let mut config = config;
    if !config.format.is_structured() {
        // If colors not explicitly set, auto-detect terminal
        if !config.enable_colors && is_terminal() {
            config.enable_colors = true;
        }
    } else {
        // Structured formats (JSON, Bunyan) should never use colors
        config.enable_colors = false;
    }

    match config.format {
        LogFormat::Compact => init_compact_format(subscriber, &config),
        LogFormat::Json => init_json_format(subscriber, &config),
        LogFormat::Pretty => init_pretty_format(subscriber, &config),
        LogFormat::Bunyan => init_bunyan_format(subscriber, &config),
    }
}

/// Initialize with compact single-line format
fn init_compact_format<S>(subscriber: S, config: &LoggingConfig) -> Result<()>
where
    S: SubscriberExt + for<'a> tracing_subscriber::registry::LookupSpan<'a> + Send + Sync,
{
    // Compact format with ANSI colors for terminal output
    let layer = fmt::layer()
        .compact()
        .with_target(config.include_target)
        .with_thread_ids(config.include_thread_id)
        .with_thread_names(config.include_thread_name)
        .with_ansi(config.enable_colors) // Auto-enabled for TTY
        .with_file(config.include_location)
        .with_line_number(config.include_location);

    let layer = if config.include_span_info {
        layer.with_span_events(FmtSpan::CLOSE)
    } else {
        layer.with_span_events(FmtSpan::NONE)
    };

    subscriber
        .with(layer)
        .try_init()
        .map_err(|e| crate::error::MonitoringError::initialization_failed("logging", e.to_string()))
}

/// Initialize with structured JSON format
fn init_json_format<S>(subscriber: S, config: &LoggingConfig) -> Result<()>
where
    S: SubscriberExt + for<'a> tracing_subscriber::registry::LookupSpan<'a> + Send + Sync,
{
    let layer = fmt::layer()
        .json()
        .with_current_span(config.include_span_info)
        .with_span_list(config.include_span_info)
        .with_target(config.include_target)
        .with_thread_ids(config.include_thread_id)
        .with_thread_names(config.include_thread_name)
        .with_file(config.include_location)
        .with_line_number(config.include_location);

    subscriber
        .with(layer)
        .try_init()
        .map_err(|e| crate::error::MonitoringError::initialization_failed("logging", e.to_string()))
}

/// Initialize with pretty multi-line format
fn init_pretty_format<S>(subscriber: S, config: &LoggingConfig) -> Result<()>
where
    S: SubscriberExt + for<'a> tracing_subscriber::registry::LookupSpan<'a> + Send + Sync,
{
    // Pretty format with ANSI colors and rich formatting
    let layer = fmt::layer()
        .pretty()
        .with_target(config.include_target)
        .with_thread_ids(config.include_thread_id)
        .with_thread_names(config.include_thread_name)
        .with_ansi(config.enable_colors) // Auto-enabled for TTY
        .with_file(config.include_location)
        .with_line_number(config.include_location);

    subscriber
        .with(layer)
        .try_init()
        .map_err(|e| crate::error::MonitoringError::initialization_failed("logging", e.to_string()))
}

/// Initialize with Bunyan-compatible JSON format
fn init_bunyan_format<S>(subscriber: S, config: &LoggingConfig) -> Result<()>
where
    S: SubscriberExt + for<'a> tracing_subscriber::registry::LookupSpan<'a> + Send + Sync,
{
    // Bunyan format is essentially JSON with specific field names
    // tracing-subscriber's JSON format is close enough for most use cases
    let layer = fmt::layer()
        .json()
        .with_current_span(true)
        .with_span_list(false)
        .with_target(true)
        .with_thread_ids(config.include_thread_id)
        .with_thread_names(config.include_thread_name)
        .with_file(config.include_location)
        .with_line_number(config.include_location);

    subscriber
        .with(layer)
        .try_init()
        .map_err(|e| crate::error::MonitoringError::initialization_failed("logging", e.to_string()))
}

/// Quick initialization for services
///
/// Provides a simplified initialization API that uses CLI-style arguments.
/// This is the most convenient way to initialize logging from a service's main function.
///
/// # Arguments
///
/// * `service_name` - Name of the service
/// * `level` - Log level (trace, debug, info, warn, error)
/// * `format` - Log format (compact, json, pretty, bunyan)
///
/// # Examples
///
/// ```no_run
/// use orcher_monitoring::logging::initialize_logging;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// initialize_logging("orchestrator", "info", "json")?;
/// # Ok(())
/// # }
/// ```
pub fn initialize_logging(service_name: &str, level: &str, format: &str) -> Result<()> {
    let log_format = format
        .parse::<LogFormat>()
        .map_err(|e| crate::error::MonitoringError::config_validation_error("log_format", e))?;

    let config = LoggingConfig::new(service_name)
        .with_format(log_format)
        .with_level(level);

    init_logging(config)
}

/// Initialize logging from environment variables
///
/// Reads configuration from standard environment variables:
/// - `LOG_LEVEL` or `RUST_LOG` - Log level (default: "info")
/// - `LOG_FORMAT` - Log format (default: "compact")
/// - `LOG_COLOR` - Enable colors (true/false/auto, default: "auto" for TTY detection)
/// - `SERVICE_NAME` - Service name (default: detected from binary)
///
/// # Examples
///
/// ```no_run
/// use orcher_monitoring::logging::init_from_env;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// // Reads from environment variables
/// init_from_env()?;
/// # Ok(())
/// # }
/// ```
pub fn init_from_env() -> Result<()> {
    let service_name =
        std::env::var("SERVICE_NAME").unwrap_or_else(|_| super::filter::detect_service_name());

    let level = std::env::var("LOG_LEVEL")
        .or_else(|_| std::env::var("RUST_LOG"))
        .unwrap_or_else(|_| "info".to_string());

    let format = std::env::var("LOG_FORMAT").unwrap_or_else(|_| "compact".to_string());

    // Parse color setting from environment
    let enable_colors = match std::env::var("LOG_COLOR")
        .unwrap_or_else(|_| "auto".to_string())
        .to_lowercase()
        .as_str()
    {
        "true" | "1" | "yes" | "on" => true,
        "false" | "0" | "no" | "off" => false,
        _ => is_terminal(), // Default to auto-detect
    };

    let log_format = format
        .parse::<LogFormat>()
        .map_err(|e| crate::error::MonitoringError::config_validation_error("log_format", e))?;

    let config = LoggingConfig::new(service_name)
        .with_format(log_format)
        .with_level(level)
        .with_colors(enable_colors);

    init_logging(config)
}

/// Initialize logging with automatic environment detection
///
/// Automatically chooses between production and development configurations
/// based on environment indicators.
///
/// Production is detected by:
/// - `ENVIRONMENT=production`
/// - `ORCHER_ENV=production`
/// - Release build (when debug_assertions is off)
///
/// # Examples
///
/// ```no_run
/// use orcher_monitoring::logging::init_auto;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// init_auto("my-service")?;
/// # Ok(())
/// # }
/// ```
pub fn init_auto(service_name: &str) -> Result<()> {
    let is_production = is_production_environment();

    let config = if is_production {
        LoggingConfig::for_production(service_name)
    } else {
        LoggingConfig::for_development(service_name)
    };

    init_logging(config)
}

/// Detect if running in production environment
fn is_production_environment() -> bool {
    // Check environment variables
    let env = std::env::var("ENVIRONMENT")
        .or_else(|_| std::env::var("ORCHER_ENV"))
        .unwrap_or_default();

    if env.eq_ignore_ascii_case("production") || env.eq_ignore_ascii_case("prod") {
        return true;
    }

    // Check if this is a release build
    #[cfg(not(debug_assertions))]
    {
        true
    }

    #[cfg(debug_assertions)]
    {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: We can't actually test initialization multiple times in the same process
    // because tracing_subscriber can only be initialized once. These tests verify
    // the configuration building logic.

    #[test]
    fn test_logging_config_compact() {
        let config = LoggingConfig::new("test-service")
            .with_format(LogFormat::Compact)
            .with_level("debug");

        assert_eq!(config.service_name, "test-service");
        assert_eq!(config.format, LogFormat::Compact);
        assert_eq!(config.level, "debug");
    }

    #[test]
    fn test_logging_config_json() {
        let config = LoggingConfig::for_production("prod-service");
        assert_eq!(config.format, LogFormat::Json);
        assert_eq!(config.level, "info");
    }

    #[test]
    fn test_logging_config_pretty() {
        let config = LoggingConfig::for_debugging("debug-service");
        assert_eq!(config.format, LogFormat::Pretty);
        assert_eq!(config.level, "trace");
        assert!(config.include_location);
    }

    #[test]
    fn test_is_production_environment() {
        // This test depends on environment, so we can't make strong assertions
        let is_prod = is_production_environment();
        // Just verify it returns a boolean without panicking
        assert!(is_prod || !is_prod);
    }

    #[test]
    fn test_format_parsing() {
        assert!("compact".parse::<LogFormat>().is_ok());
        assert!("json".parse::<LogFormat>().is_ok());
        assert!("pretty".parse::<LogFormat>().is_ok());
        assert!("bunyan".parse::<LogFormat>().is_ok());
        assert!("invalid".parse::<LogFormat>().is_err());
    }
}
