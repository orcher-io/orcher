//! Configuration types for sync-match routing, worker affinity, and sharding.

use serde::{Deserialize, Serialize};

/// Sync-match routing configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingConfig {
    pub enabled: bool,
    pub max_waiters_per_queue: usize,
    pub claim_timeout_secs: i64,
    pub affinity: AffinityConfig,
    pub cleanup_interval_secs: u64,
    pub sharding: ShardingConfig,
}

impl Default for RoutingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_waiters_per_queue: 1000,
            claim_timeout_secs: 300,
            affinity: AffinityConfig::default(),
            cleanup_interval_secs: 60,
            sharding: ShardingConfig::default(),
        }
    }
}

/// Worker affinity (sticky routing) configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AffinityConfig {
    pub enabled: bool,
    pub default_lease_secs: i64,
    pub max_lease_secs: i64,
}

impl Default for AffinityConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            default_lease_secs: 60,
            max_lease_secs: 300,
        }
    }
}

/// Sharding configuration for horizontal orchestrator scaling.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardingConfig {
    pub enabled: bool,
    pub total_shards: u32,
    pub assigned_shards: Vec<u32>,
}

impl Default for ShardingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            total_shards: 1,
            assigned_shards: vec![0],
        }
    }
}

/// Horizontal sharding configuration for distributed orchestrators.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HorizontalShardingConfig {
    pub enabled: bool,
    pub total_shards: u32,
    pub assigned_shards: Vec<u32>,
    pub rebalance_interval_secs: u64,
    pub health_check_interval_secs: u64,
}

impl Default for HorizontalShardingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            total_shards: 16,
            assigned_shards: vec![],
            rebalance_interval_secs: 30,
            health_check_interval_secs: 10,
        }
    }
}

/// Information for redirecting requests to another shard.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardRedirectInfo {
    pub target_shard: u32,
    pub target_address: String,
    pub reason: String,
}

impl ShardRedirectInfo {
    pub fn new(
        target_shard: u32,
        target_address: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            target_shard,
            target_address: target_address.into(),
            reason: reason.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_routing_config_defaults() {
        let config = RoutingConfig::default();
        assert!(config.enabled);
        assert_eq!(config.max_waiters_per_queue, 1000);
        assert_eq!(config.claim_timeout_secs, 300);
        assert!(config.affinity.enabled);
        assert_eq!(config.cleanup_interval_secs, 60);
    }

    #[test]
    fn test_affinity_config_defaults() {
        let config = AffinityConfig::default();
        assert!(config.enabled);
        assert_eq!(config.default_lease_secs, 60);
        assert_eq!(config.max_lease_secs, 300);
    }

    #[test]
    fn test_sharding_config_defaults() {
        let config = ShardingConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.total_shards, 1);
        assert_eq!(config.assigned_shards, vec![0]);
    }

    #[test]
    fn test_horizontal_sharding_config_defaults() {
        let config = HorizontalShardingConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.total_shards, 16);
        assert!(config.assigned_shards.is_empty());
    }

    #[test]
    fn test_shard_redirect_info() {
        let info = ShardRedirectInfo::new(5, "orchestrator-2:50051", "shard reassignment");
        assert_eq!(info.target_shard, 5);
        assert_eq!(info.target_address, "orchestrator-2:50051");
        assert_eq!(info.reason, "shard reassignment");
    }
}
