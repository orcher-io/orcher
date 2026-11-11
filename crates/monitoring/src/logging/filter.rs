//! Environment filter construction for logging
//!
//! Provides utilities for building `EnvFilter` instances with smart defaults
//! for ORCHER services.

use tracing_subscriber::EnvFilter;

/// Build an environment filter for ORCHER services
///
/// This function creates a `tracing_subscriber::EnvFilter` with sensible
/// defaults for ORCHER services. It automatically includes common ORCHER
/// crates and third-party dependencies at appropriate log levels.
///
/// # Filter Priority
///
/// 1. `RUST_LOG` environment variable (if set)
/// 2. Custom filter string (if provided)
/// 3. Generated default filter based on service name and level
///
/// # Arguments
///
/// * `service_name` - Name of the service (e.g., "orchestrator", "gateway", "worker")
/// * `default_level` - Default log level (e.g., "info", "debug", "trace")
/// * `custom_filter` - Optional custom filter string to override defaults
/// * `additional_targets` - Additional crate/module targets to include
///
/// # Examples
///
/// ```
/// use orcher_monitoring::logging::filter::build_env_filter;
///
/// // Simple filter
/// let filter = build_env_filter("orchestrator", "info", None, &[]);
///
/// // With additional targets
/// let filter = build_env_filter(
///     "gateway",
///     "debug",
///     None,
///     &["custom_crate".to_string()]
/// );
///
/// // With custom filter (overrides defaults)
/// let filter = build_env_filter(
///     "worker",
///     "info",
///     Some("worker=trace,sqlx=debug"),
///     &[]
/// );
/// ```
pub fn build_env_filter(
    service_name: &str,
    default_level: &str,
    custom_filter: Option<&str>,
    additional_targets: &[String],
) -> EnvFilter {
    // Try RUST_LOG environment variable first
    if let Ok(filter) = EnvFilter::try_from_default_env() {
        return filter;
    }

    // Use custom filter if provided
    if let Some(custom) = custom_filter {
        return EnvFilter::new(custom);
    }

    // Build default filter
    let filter_string = build_default_filter(service_name, default_level, additional_targets);
    EnvFilter::new(filter_string)
}

/// Build a default filter string for ORCHER services
///
/// Creates a filter string that includes:
/// - The service itself at the specified level
/// - All ORCHER crates at the specified level
/// - Common third-party dependencies at appropriate levels
/// - Additional user-specified targets
fn build_default_filter(
    service_name: &str,
    default_level: &str,
    additional_targets: &[String],
) -> String {
    let mut parts = Vec::new();

    // Service-specific logging
    parts.push(format!("{}={}", service_name, default_level));

    // ORCHER crates - all at the same level as the service. Every crate the
    // engine links must be here: one left out logs nothing at all, its errors
    // included, as the dispatch crate's claimer and prefetch buffer did.
    let orcher_crates = [
        "orcher_common",
        "orcher_core",
        "orcher_storage",
        "orcher_monitoring",
        "orcher_server",
        "orcher_config",
        "orcher_cluster",
        "orcher_dispatch",
        "orcher_events",
    ];

    for crate_name in &orcher_crates {
        parts.push(format!("{}={}", crate_name, default_level));
    }

    // Third-party dependencies - set at reasonable levels
    let third_party = get_third_party_defaults(default_level);
    parts.extend(third_party);

    // Additional user-specified targets
    for target in additional_targets {
        // If target doesn't include level, use default level
        if target.contains('=') {
            parts.push(target.clone());
        } else {
            parts.push(format!("{}={}", target, default_level));
        }
    }

    parts.join(",")
}

/// Get default log levels for common third-party dependencies
///
/// Returns a list of "target=level" strings for third-party crates
/// commonly used in ORCHER services. Levels are chosen to reduce noise
/// while maintaining visibility into important issues.
fn get_third_party_defaults(service_level: &str) -> Vec<String> {
    // Map service level to third-party level
    // Generally we want third-party crates one level quieter than the service
    let third_party_level = match service_level {
        "trace" => "debug",
        "debug" => "info",
        _ => "info", // info/warn/error stay at info
    };

    vec![
        // HTTP frameworks
        format!("tower_http={}", third_party_level),
        format!("axum={}", third_party_level),
        format!("hyper={}", third_party_level),
        format!("tonic={}", third_party_level),
        format!("h2={}", third_party_level),
        // Database
        "sqlx=warn".to_string(), // SQL queries can be very verbose
        "sea_orm=warn".to_string(),
        // Async runtime
        format!("tokio={}", "warn"), // Tokio is very noisy even at info
        format!("async_std={}", "warn"),
        // Serialization
        format!("serde={}", "warn"),
        // Message queues
        format!("lapin={}", third_party_level),   // RabbitMQ
        format!("rdkafka={}", third_party_level), // Kafka
        // Tracing/metrics
        format!("tracing_subscriber={}", "warn"),
        format!("opentelemetry={}", third_party_level),
        format!("metrics={}", third_party_level),
        // gRPC and protobuf
        format!("prost={}", "warn"),
        format!("tower={}", third_party_level),
        // TLS
        format!("rustls={}", "warn"),
        format!("native_tls={}", "warn"),
    ]
}

/// Build a minimal filter for testing or simple scenarios
///
/// Only includes the service name and basic ORCHER crates.
pub fn build_minimal_filter(service_name: &str, level: &str) -> EnvFilter {
    let filter = format!(
        "{}={},orcher_common={},orcher_core={}",
        service_name, level, level, level
    );
    EnvFilter::new(filter)
}

/// Build a verbose filter for debugging
///
/// Sets everything to trace level except known-noisy crates.
pub fn build_debug_filter(service_name: &str) -> EnvFilter {
    let filter = format!(
        "{}=trace,orcher_common=trace,orcher_core=trace,orcher_storage=trace,\
         orcher_monitoring=trace,orcher_server=trace,orcher_config=trace,\
         orcher_cluster=trace,orcher_dispatch=trace,orcher_events=trace,\
         tower_http=debug,axum=debug,tonic=debug,sqlx=debug,tokio=info,\
         tracing_subscriber=info,h2=info,hyper=info",
        service_name
    );
    EnvFilter::new(filter)
}

/// Detect service name from cargo package name or binary name
///
/// Attempts to extract a reasonable service name from the current
/// binary or package name. Falls back to "orcher" if detection fails.
pub fn detect_service_name() -> String {
    // Try to get from CARGO_PKG_NAME at compile time
    let pkg_name = option_env!("CARGO_PKG_NAME").unwrap_or("orcher");

    // Strip common prefixes
    pkg_name
        .trim_start_matches("orcher-")
        .trim_start_matches("orcher_")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_default_filter() {
        let filter = build_default_filter("orchestrator", "info", &[]);
        assert!(filter.contains("orchestrator=info"));
        assert!(filter.contains("orcher_core=info"));
        assert!(filter.contains("orcher_storage=info"));
        assert!(filter.contains("orcher_dispatch=info"));
        assert!(filter.contains("orcher_events=info"));
        assert!(filter.contains("tower_http="));
        assert!(filter.contains("sqlx=warn"));
    }

    #[test]
    fn test_build_default_filter_with_additional_targets() {
        let additional = vec!["my_crate".to_string(), "another=debug".to_string()];
        let filter = build_default_filter("gateway", "debug", &additional);

        assert!(filter.contains("my_crate=debug")); // Inherits default level
        assert!(filter.contains("another=debug")); // Uses explicit level
    }

    #[test]
    fn test_third_party_defaults_trace_level() {
        let defaults = get_third_party_defaults("trace");
        assert!(defaults.iter().any(|s| s.contains("tower_http=debug")));
        assert!(defaults.iter().any(|s| s == "sqlx=warn"));
    }

    #[test]
    fn test_third_party_defaults_debug_level() {
        let defaults = get_third_party_defaults("debug");
        assert!(defaults.iter().any(|s| s.contains("tower_http=info")));
    }

    #[test]
    fn test_third_party_defaults_info_level() {
        let defaults = get_third_party_defaults("info");
        assert!(defaults.iter().any(|s| s.contains("tower_http=info")));
        assert!(defaults.iter().any(|s| s == "tokio=warn"));
    }

    #[test]
    fn test_build_minimal_filter() {
        let filter = build_minimal_filter("worker", "debug");
        // EnvFilter doesn't expose its internal string, so we just verify it doesn't panic
        let _ = filter;
    }

    #[test]
    fn test_build_debug_filter() {
        let filter = build_debug_filter("orchestrator");
        // EnvFilter doesn't expose its internal string, so we just verify it doesn't panic
        // The actual filter string is tested indirectly through log output
        let _ = filter;
    }

    #[test]
    fn test_detect_service_name() {
        let name = detect_service_name();
        // Should return something reasonable
        assert!(!name.is_empty());
    }

    #[test]
    fn test_build_env_filter_custom() {
        let filter = build_env_filter("test", "info", Some("custom=trace,other=debug"), &[]);
        // EnvFilter doesn't expose its internal string, so we just verify it doesn't panic
        let _ = filter;
    }

    #[test]
    fn test_build_env_filter_with_additional() {
        let additional = vec!["extra_crate".to_string()];
        let filter = build_env_filter("test", "info", None, &additional);
        // EnvFilter doesn't expose its internal string, so we just verify it doesn't panic
        let _ = filter;
    }
}
