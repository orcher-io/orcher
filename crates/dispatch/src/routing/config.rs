//! Configuration for sync-match dispatch and worker affinity.

use crate::routing::affinity::WorkerAffinityConfig;
use crate::sharding::HorizontalShardingConfig;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RoutingConfig {
    #[serde(default = "default_enabled")]
    pub enabled: bool,

    /// How long a worker can hold a claim before it expires (seconds).
    #[serde(default = "default_claim_timeout_secs")]
    pub claim_timeout_secs: i64,

    /// Workers beyond this limit fall back to Redis polling (~200 bytes each).
    #[serde(default = "default_max_waiters_per_queue")]
    pub max_waiters_per_queue: usize,

    #[serde(default)]
    pub affinity: AffinityConfig,

    #[serde(default = "default_cleanup_interval_secs")]
    pub cleanup_interval_secs: u64,

    #[serde(default)]
    pub sharding: HorizontalShardingConfig,
}

impl Default for RoutingConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            claim_timeout_secs: default_claim_timeout_secs(),
            max_waiters_per_queue: default_max_waiters_per_queue(),
            affinity: AffinityConfig::default(),
            cleanup_interval_secs: default_cleanup_interval_secs(),
            sharding: HorizontalShardingConfig::default(),
        }
    }
}

impl RoutingConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn disabled() -> Self {
        Self {
            enabled: false,
            ..Default::default()
        }
    }

    pub fn cleanup_interval(&self) -> Duration {
        Duration::from_secs(self.cleanup_interval_secs)
    }

    pub fn to_affinity_config(&self) -> WorkerAffinityConfig {
        WorkerAffinityConfig {
            enabled: self.affinity.enabled,
            default_lease: Duration::from_secs(self.affinity.default_lease_secs),
            max_lease: Duration::from_secs(self.affinity.max_lease_secs),
            cleanup_interval: self.cleanup_interval(),
        }
    }

    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    pub fn with_claim_timeout_secs(mut self, secs: i64) -> Self {
        self.claim_timeout_secs = secs;
        self
    }

    pub fn with_max_waiters_per_queue(mut self, max: usize) -> Self {
        self.max_waiters_per_queue = max;
        self
    }

    pub fn with_affinity_enabled(mut self, enabled: bool) -> Self {
        self.affinity.enabled = enabled;
        self
    }

    pub fn with_cleanup_interval_secs(mut self, secs: u64) -> Self {
        self.cleanup_interval_secs = secs;
        self
    }

    pub fn with_sharding(mut self, sharding: HorizontalShardingConfig) -> Self {
        self.sharding = sharding;
        self
    }

    pub fn is_sharding_enabled(&self) -> bool {
        self.sharding.enabled
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AffinityConfig {
    #[serde(default = "default_affinity_enabled")]
    pub enabled: bool,

    /// How long an affinity mapping stays valid after the last task (seconds).
    #[serde(default = "default_affinity_lease_secs")]
    pub default_lease_secs: u64,

    /// Hard cap on lease duration even with renewals (seconds).
    #[serde(default = "default_affinity_max_lease_secs")]
    pub max_lease_secs: u64,
}

impl Default for AffinityConfig {
    fn default() -> Self {
        Self {
            enabled: default_affinity_enabled(),
            default_lease_secs: default_affinity_lease_secs(),
            max_lease_secs: default_affinity_max_lease_secs(),
        }
    }
}

impl AffinityConfig {
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            ..Default::default()
        }
    }

    pub fn default_lease(&self) -> Duration {
        Duration::from_secs(self.default_lease_secs)
    }

    pub fn max_lease(&self) -> Duration {
        Duration::from_secs(self.max_lease_secs)
    }
}

fn default_enabled() -> bool {
    true
}

fn default_claim_timeout_secs() -> i64 {
    300 // 5 minutes
}

fn default_max_waiters_per_queue() -> usize {
    10_000
}

fn default_cleanup_interval_secs() -> u64 {
    30
}

fn default_affinity_enabled() -> bool {
    true
}

fn default_affinity_lease_secs() -> u64 {
    30
}

fn default_affinity_max_lease_secs() -> u64 {
    300 // 5 minutes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = RoutingConfig::default();

        assert!(config.enabled);
        assert_eq!(config.claim_timeout_secs, 300);
        assert_eq!(config.max_waiters_per_queue, 10_000);
        assert!(config.affinity.enabled);
        assert_eq!(config.affinity.default_lease_secs, 30);
        assert_eq!(config.affinity.max_lease_secs, 300);
        assert_eq!(config.cleanup_interval_secs, 30);
    }

    #[test]
    fn test_disabled_config() {
        let config = RoutingConfig::disabled();

        assert!(!config.enabled);
        // Other values should still have defaults
        assert_eq!(config.max_waiters_per_queue, 10_000);
    }

    #[test]
    fn test_builder_pattern() {
        let config = RoutingConfig::new()
            .with_enabled(true)
            .with_claim_timeout_secs(600)
            .with_max_waiters_per_queue(5000)
            .with_affinity_enabled(false)
            .with_cleanup_interval_secs(60);

        assert!(config.enabled);
        assert_eq!(config.claim_timeout_secs, 600);
        assert_eq!(config.max_waiters_per_queue, 5000);
        assert!(!config.affinity.enabled);
        assert_eq!(config.cleanup_interval_secs, 60);
    }

    #[test]
    fn test_cleanup_interval_duration() {
        let config = RoutingConfig::default();
        assert_eq!(config.cleanup_interval(), Duration::from_secs(30));
    }

    #[test]
    fn test_to_affinity_config() {
        let routing = RoutingConfig::default();
        let affinity = routing.to_affinity_config();

        assert!(affinity.enabled);
        assert_eq!(affinity.default_lease, Duration::from_secs(30));
        assert_eq!(affinity.max_lease, Duration::from_secs(300));
        assert_eq!(affinity.cleanup_interval, Duration::from_secs(30));
    }

    #[test]
    fn test_serde_with_defaults() {
        let json = r#"{"enabled": true}"#;
        let config: RoutingConfig = serde_json::from_str(json).unwrap();

        assert!(config.enabled);
        // Defaults should be applied
        assert_eq!(config.max_waiters_per_queue, 10_000);
        assert!(config.affinity.enabled);
    }

    #[test]
    fn test_serde_full() {
        let json = r#"{
            "enabled": false,
            "claim_timeout_secs": 600,
            "max_waiters_per_queue": 5000,
            "affinity": {
                "enabled": true,
                "default_lease_secs": 60,
                "max_lease_secs": 600
            },
            "cleanup_interval_secs": 45
        }"#;

        let config: RoutingConfig = serde_json::from_str(json).unwrap();

        assert!(!config.enabled);
        assert_eq!(config.claim_timeout_secs, 600);
        assert_eq!(config.max_waiters_per_queue, 5000);
        assert!(config.affinity.enabled);
        assert_eq!(config.affinity.default_lease_secs, 60);
        assert_eq!(config.affinity.max_lease_secs, 600);
        assert_eq!(config.cleanup_interval_secs, 45);
    }

    #[test]
    fn test_affinity_config_disabled() {
        let config = AffinityConfig::disabled();
        assert!(!config.enabled);
    }

    #[test]
    fn test_affinity_lease_durations() {
        let config = AffinityConfig::default();
        assert_eq!(config.default_lease(), Duration::from_secs(30));
        assert_eq!(config.max_lease(), Duration::from_secs(300));
    }
}
