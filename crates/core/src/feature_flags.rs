//! Runtime feature flags for gradual rollout.

use serde::{Deserialize, Serialize};
use std::env;
use std::str::FromStr;
use tracing::{debug, info, warn};

/// Feature flags for controlling ORCHER behavior at runtime
///
/// These flags allow gradual rollout of new features and quick rollback
/// if issues occur. Flags can be set via environment variables or configuration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FeatureFlags {
    /// Enable queue-based task dispatch instead of direct gRPC/HTTP
    ///
    /// Environment variable: `ORCHER_QUEUE_DISPATCH`
    /// Default: `false`
    pub queue_based_dispatch: bool,

    /// Specific queue provider to use (overrides config)
    ///
    /// Valid values: "redis", "rabbitmq", "postgres", "sqs", "inmemory"
    /// Environment variable: `ORCHER_QUEUE_PROVIDER`
    /// Default: `None` (use config file)
    pub queue_provider: Option<String>,

    /// Emergency rollback to direct dispatch
    ///
    /// When enabled, immediately switches to direct dispatch regardless of
    /// queue_based_dispatch flag. Used for emergency rollback scenarios.
    /// Environment variable: `ORCHER_ROLLBACK_DIRECT`
    /// Default: `false`
    pub rollback_to_direct: bool,

    /// Dispatch strategy override
    ///
    /// Valid values: "direct", "queue", "hybrid"
    /// Environment variable: `ORCHER_DISPATCH_STRATEGY`
    /// Default: `None` (use config file)
    pub dispatch_strategy: Option<DispatchStrategy>,

    /// Enable experimental features
    ///
    /// Environment variable: `ORCHER_EXPERIMENTAL`
    /// Default: `false`
    pub experimental: bool,

    /// Enable detailed tracing for debugging
    ///
    /// Environment variable: `ORCHER_DEBUG_TRACING`
    /// Default: `false`
    pub debug_tracing: bool,

    /// Circuit breaker for queue operations
    ///
    /// Automatically fallback to direct dispatch if queue operations fail
    /// repeatedly. Useful for handling queue provider outages.
    /// Environment variable: `ORCHER_QUEUE_CIRCUIT_BREAKER`
    /// Default: `true`
    pub queue_circuit_breaker: bool,

    /// Maximum queue depth before triggering backpressure
    ///
    /// Environment variable: `ORCHER_MAX_QUEUE_DEPTH`
    /// Default: `None` (no limit)
    pub max_queue_depth: Option<usize>,

    /// Enable metrics collection for queue operations
    ///
    /// Environment variable: `ORCHER_QUEUE_METRICS`
    /// Default: `true`
    pub queue_metrics: bool,
}

/// Dispatch strategy for task routing
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DispatchStrategy {
    /// Direct gRPC/HTTP dispatch to workers
    Direct,
    /// All tasks routed through queue
    Queue,
    /// Hybrid: Critical tasks direct, others through queue
    Hybrid,
}

impl FromStr for DispatchStrategy {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "direct" => Ok(Self::Direct),
            "queue" => Ok(Self::Queue),
            "hybrid" => Ok(Self::Hybrid),
            _ => Err(format!("Invalid dispatch strategy: {}", s)),
        }
    }
}

impl std::fmt::Display for DispatchStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Direct => write!(f, "direct"),
            Self::Queue => write!(f, "queue"),
            Self::Hybrid => write!(f, "hybrid"),
        }
    }
}

impl Default for FeatureFlags {
    fn default() -> Self {
        Self {
            queue_based_dispatch: false,
            queue_provider: None,
            rollback_to_direct: false,
            dispatch_strategy: None,
            experimental: false,
            debug_tracing: false,
            queue_circuit_breaker: true,
            max_queue_depth: None,
            queue_metrics: true,
        }
    }
}

impl FeatureFlags {
    /// Create feature flags from environment variables
    ///
    /// # Examples
    ///
    /// ```bash
    /// export ORCHER_QUEUE_DISPATCH=true
    /// export ORCHER_QUEUE_PROVIDER=redis
    /// export ORCHER_DISPATCH_STRATEGY=hybrid
    /// ```
    ///
    /// ```rust,ignore
    /// let flags = FeatureFlags::from_env();
    /// assert!(flags.queue_based_dispatch);
    /// ```
    pub fn from_env() -> Self {
        Self::from_vars(|name| env::var(name).ok())
    }

    /// Read the flags through `var`, which returns a variable's value or `None`
    /// when it is unset. `from_env` passes the process environment; tests pass
    /// their own values, since the process environment is shared by every test
    /// running at once.
    fn from_vars(var: impl Fn(&str) -> Option<String>) -> Self {
        let mut flags = Self::default();

        // Queue-based dispatch
        if let Some(value) = var("ORCHER_QUEUE_DISPATCH") {
            flags.queue_based_dispatch = parse_bool(&value);
            debug!(
                queue_dispatch = flags.queue_based_dispatch,
                "Loaded queue dispatch flag from environment"
            );
        }

        // Queue provider
        if let Some(provider) = var("ORCHER_QUEUE_PROVIDER") {
            let provider = provider.trim().to_lowercase();
            if !provider.is_empty() {
                flags.queue_provider = Some(provider.clone());
                debug!(provider = %provider, "Loaded queue provider from environment");
            }
        }

        // Rollback to direct
        if let Some(value) = var("ORCHER_ROLLBACK_DIRECT") {
            flags.rollback_to_direct = parse_bool(&value);
            if flags.rollback_to_direct {
                warn!("ROLLBACK TO DIRECT DISPATCH ENABLED - Queue dispatch will be bypassed");
            }
        }

        // Dispatch strategy
        if let Some(strategy) = var("ORCHER_DISPATCH_STRATEGY") {
            match DispatchStrategy::from_str(&strategy) {
                Ok(s) => {
                    flags.dispatch_strategy = Some(s);
                    info!(strategy = %s, "Loaded dispatch strategy from environment");
                }
                Err(e) => {
                    warn!(
                        strategy = %strategy,
                        error = %e,
                        "Invalid dispatch strategy in environment, ignoring"
                    );
                }
            }
        }

        // Experimental features
        if let Some(value) = var("ORCHER_EXPERIMENTAL") {
            flags.experimental = parse_bool(&value);
            if flags.experimental {
                info!("Experimental features enabled");
            }
        }

        // Debug tracing
        if let Some(value) = var("ORCHER_DEBUG_TRACING") {
            flags.debug_tracing = parse_bool(&value);
            if flags.debug_tracing {
                info!("Debug tracing enabled");
            }
        }

        // Circuit breaker
        if let Some(value) = var("ORCHER_QUEUE_CIRCUIT_BREAKER") {
            flags.queue_circuit_breaker = parse_bool(&value);
            debug!(
                circuit_breaker = flags.queue_circuit_breaker,
                "Loaded circuit breaker flag from environment"
            );
        }

        // Max queue depth
        if let Some(value) = var("ORCHER_MAX_QUEUE_DEPTH") {
            if let Ok(depth) = value.parse::<usize>() {
                flags.max_queue_depth = Some(depth);
                debug!(max_depth = depth, "Loaded max queue depth from environment");
            }
        }

        // Queue metrics
        if let Some(value) = var("ORCHER_QUEUE_METRICS") {
            flags.queue_metrics = parse_bool(&value);
            debug!(
                metrics = flags.queue_metrics,
                "Loaded queue metrics flag from environment"
            );
        }

        flags
    }

    /// Check if queue-based dispatch should be used
    ///
    /// Takes into account both the queue_based_dispatch flag and rollback_to_direct.
    /// If rollback is active, always returns false.
    pub fn should_use_queue(&self) -> bool {
        if self.rollback_to_direct {
            return false;
        }
        self.queue_based_dispatch
    }

    /// Get the effective dispatch strategy
    ///
    /// Returns the dispatch strategy considering rollback and configuration.
    pub fn effective_dispatch_strategy(&self) -> DispatchStrategy {
        if self.rollback_to_direct {
            return DispatchStrategy::Direct;
        }

        self.dispatch_strategy
            .unwrap_or(if self.queue_based_dispatch {
                DispatchStrategy::Queue
            } else {
                DispatchStrategy::Direct
            })
    }

    /// Merge with configuration-based flags
    ///
    /// Environment variables take precedence over configuration.
    pub fn merge_with_config(&mut self, config_flags: &FeatureFlags) {
        self.merge_with_config_from(config_flags, |name| env::var(name).ok());
    }

    /// `merge_with_config`, judging which variables are set through `var`.
    fn merge_with_config_from(
        &mut self,
        config_flags: &FeatureFlags,
        var: impl Fn(&str) -> Option<String>,
    ) {
        // Only use config values if env var not set
        if var("ORCHER_QUEUE_DISPATCH").is_none() {
            self.queue_based_dispatch = config_flags.queue_based_dispatch;
        }

        if self.queue_provider.is_none() && config_flags.queue_provider.is_some() {
            self.queue_provider = config_flags.queue_provider.clone();
        }

        if var("ORCHER_ROLLBACK_DIRECT").is_none() {
            self.rollback_to_direct = config_flags.rollback_to_direct;
        }

        if self.dispatch_strategy.is_none() && config_flags.dispatch_strategy.is_some() {
            self.dispatch_strategy = config_flags.dispatch_strategy;
        }
    }

    /// Create a flag configuration for safe rollout
    ///
    /// Starts with conservative defaults for gradual rollout.
    pub fn safe_rollout() -> Self {
        Self {
            queue_based_dispatch: true,
            dispatch_strategy: Some(DispatchStrategy::Hybrid),
            queue_circuit_breaker: true,
            queue_metrics: true,
            debug_tracing: true,
            ..Default::default()
        }
    }

    /// Create a flag configuration for emergency rollback
    pub fn emergency_rollback() -> Self {
        Self {
            rollback_to_direct: true,
            queue_based_dispatch: false,
            dispatch_strategy: Some(DispatchStrategy::Direct),
            ..Default::default()
        }
    }

    /// Validate flag configuration
    ///
    /// Returns an error if the configuration is invalid or inconsistent.
    pub fn validate(&self) -> Result<(), String> {
        // Cannot have both queue dispatch and rollback enabled
        if self.queue_based_dispatch && self.rollback_to_direct {
            return Err(
                "Invalid configuration: queue_based_dispatch and rollback_to_direct \
                 cannot both be true"
                    .to_string(),
            );
        }

        // Validate queue provider name if set
        if let Some(ref provider) = self.queue_provider {
            let valid_providers = ["redis", "rabbitmq", "postgres", "sqs", "inmemory"];
            if !valid_providers.contains(&provider.as_str()) {
                return Err(format!(
                    "Invalid queue provider '{}'. Valid options: {}",
                    provider,
                    valid_providers.join(", ")
                ));
            }
        }

        // Validate max queue depth
        if let Some(depth) = self.max_queue_depth {
            if depth == 0 {
                return Err("max_queue_depth must be greater than 0".to_string());
            }
        }

        Ok(())
    }

    /// Log current feature flag state
    pub fn log_state(&self) {
        info!("Feature Flags Configuration:");
        info!("  Queue Dispatch: {}", self.queue_based_dispatch);
        info!(
            "  Queue Provider: {}",
            self.queue_provider.as_deref().unwrap_or("(not set)")
        );
        info!("  Rollback to Direct: {}", self.rollback_to_direct);
        info!(
            "  Dispatch Strategy: {}",
            self.dispatch_strategy
                .map(|s| s.to_string())
                .unwrap_or_else(|| "(not set)".to_string())
        );
        info!("  Experimental: {}", self.experimental);
        info!("  Debug Tracing: {}", self.debug_tracing);
        info!("  Circuit Breaker: {}", self.queue_circuit_breaker);
        info!(
            "  Max Queue Depth: {}",
            self.max_queue_depth
                .map(|d| d.to_string())
                .unwrap_or_else(|| "(unlimited)".to_string())
        );
        info!("  Queue Metrics: {}", self.queue_metrics);

        if self.rollback_to_direct {
            warn!("EMERGENCY ROLLBACK ACTIVE - Queue dispatch bypassed");
        }
    }
}

/// Parse boolean from string (supports various formats)
fn parse_bool(value: &str) -> bool {
    matches!(
        value.trim().to_lowercase().as_str(),
        "true" | "1" | "yes" | "on" | "enabled"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_flags() {
        let flags = FeatureFlags::default();
        assert!(!flags.queue_based_dispatch);
        assert!(!flags.rollback_to_direct);
        assert!(flags.queue_circuit_breaker);
        assert!(flags.queue_metrics);
        assert_eq!(flags.queue_provider, None);
    }

    #[test]
    fn test_parse_bool() {
        assert!(parse_bool("true"));
        assert!(parse_bool("True"));
        assert!(parse_bool("TRUE"));
        assert!(parse_bool("1"));
        assert!(parse_bool("yes"));
        assert!(parse_bool("on"));
        assert!(parse_bool("enabled"));

        assert!(!parse_bool("false"));
        assert!(!parse_bool("0"));
        assert!(!parse_bool("no"));
        assert!(!parse_bool("off"));
    }

    /// A lookup over `pairs`, standing in for the process environment.
    fn vars(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let pairs: Vec<(String, String)> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |name| {
            pairs
                .iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.clone())
        }
    }

    #[test]
    fn test_from_env() {
        let flags = FeatureFlags::from_vars(vars(&[
            ("ORCHER_QUEUE_DISPATCH", "true"),
            ("ORCHER_QUEUE_PROVIDER", "redis"),
            ("ORCHER_DISPATCH_STRATEGY", "hybrid"),
        ]));

        assert!(flags.queue_based_dispatch);
        assert_eq!(flags.queue_provider, Some("redis".to_string()));
        assert_eq!(flags.dispatch_strategy, Some(DispatchStrategy::Hybrid));
    }

    #[test]
    fn test_should_use_queue() {
        let mut flags = FeatureFlags::default();

        // Default: no queue
        assert!(!flags.should_use_queue());

        // Enable queue
        flags.queue_based_dispatch = true;
        assert!(flags.should_use_queue());

        // Rollback overrides queue setting
        flags.rollback_to_direct = true;
        assert!(!flags.should_use_queue());
    }

    #[test]
    fn test_effective_dispatch_strategy() {
        let mut flags = FeatureFlags::default();

        // Default: direct
        assert_eq!(
            flags.effective_dispatch_strategy(),
            DispatchStrategy::Direct
        );

        // Queue enabled -> Queue strategy
        flags.queue_based_dispatch = true;
        assert_eq!(flags.effective_dispatch_strategy(), DispatchStrategy::Queue);

        // Explicit strategy overrides
        flags.dispatch_strategy = Some(DispatchStrategy::Hybrid);
        assert_eq!(
            flags.effective_dispatch_strategy(),
            DispatchStrategy::Hybrid
        );

        // Rollback overrides everything
        flags.rollback_to_direct = true;
        assert_eq!(
            flags.effective_dispatch_strategy(),
            DispatchStrategy::Direct
        );
    }

    #[test]
    fn test_validate() {
        let mut flags = FeatureFlags::default();

        // Valid default
        assert!(flags.validate().is_ok());

        // Invalid: both queue and rollback
        flags.queue_based_dispatch = true;
        flags.rollback_to_direct = true;
        assert!(flags.validate().is_err());

        // Valid: queue only
        flags.rollback_to_direct = false;
        assert!(flags.validate().is_ok());

        // Invalid provider
        flags.queue_provider = Some("invalid".to_string());
        assert!(flags.validate().is_err());

        // Valid provider
        flags.queue_provider = Some("redis".to_string());
        assert!(flags.validate().is_ok());

        // Invalid max queue depth
        flags.max_queue_depth = Some(0);
        assert!(flags.validate().is_err());

        // Valid max queue depth
        flags.max_queue_depth = Some(1000);
        assert!(flags.validate().is_ok());
    }

    #[test]
    fn test_safe_rollout() {
        let flags = FeatureFlags::safe_rollout();

        assert!(flags.queue_based_dispatch);
        assert_eq!(flags.dispatch_strategy, Some(DispatchStrategy::Hybrid));
        assert!(flags.queue_circuit_breaker);
        assert!(flags.queue_metrics);
        assert!(flags.debug_tracing);
    }

    #[test]
    fn test_emergency_rollback() {
        let flags = FeatureFlags::emergency_rollback();

        assert!(flags.rollback_to_direct);
        assert!(!flags.queue_based_dispatch);
        assert_eq!(flags.dispatch_strategy, Some(DispatchStrategy::Direct));
        assert!(!flags.should_use_queue());
    }

    #[test]
    fn test_dispatch_strategy_from_str() {
        assert_eq!(
            DispatchStrategy::from_str("direct"),
            Ok(DispatchStrategy::Direct)
        );
        assert_eq!(
            DispatchStrategy::from_str("queue"),
            Ok(DispatchStrategy::Queue)
        );
        assert_eq!(
            DispatchStrategy::from_str("hybrid"),
            Ok(DispatchStrategy::Hybrid)
        );
        assert_eq!(
            DispatchStrategy::from_str("DIRECT"),
            Ok(DispatchStrategy::Direct)
        );
        assert!(DispatchStrategy::from_str("invalid").is_err());
    }

    #[test]
    fn test_merge_with_config() {
        let unset = vars(&[]);
        let mut env_flags = FeatureFlags::from_vars(&unset);
        let config_flags = FeatureFlags {
            queue_based_dispatch: true,
            queue_provider: Some("redis".to_string()),
            ..Default::default()
        };

        env_flags.merge_with_config_from(&config_flags, &unset);

        // Should use config values when env not set
        assert!(env_flags.queue_based_dispatch);
        assert_eq!(env_flags.queue_provider, Some("redis".to_string()));
    }

    #[test]
    fn a_set_variable_wins_over_the_config() {
        let set = vars(&[("ORCHER_QUEUE_DISPATCH", "false")]);
        let mut env_flags = FeatureFlags::from_vars(&set);
        let config_flags = FeatureFlags {
            queue_based_dispatch: true,
            ..Default::default()
        };

        env_flags.merge_with_config_from(&config_flags, &set);

        assert!(!env_flags.queue_based_dispatch);
    }
}
