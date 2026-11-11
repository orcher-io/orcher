//! Cluster configuration (nodes, leadership, discovery, sharding).

use orcher_common::uuid::Uuid;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

/// Cluster configuration for distributed ORCHER deployments
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ClusterConfig {
    /// Cluster node configuration
    pub node: NodeConfig,

    /// Leadership election configuration
    pub leadership: LeadershipConfig,

    /// Node discovery configuration
    pub discovery: DiscoveryConfig,

    /// Cluster health monitoring
    pub health: ClusterHealthConfig,

    /// Sharding configuration for horizontal scaling
    pub sharding: ShardingConfig,
}

// ================================================================================================
// SHARDING CONFIGURATION
// ================================================================================================

/// Sharding configuration for horizontal scaling
///
/// Enables partitioning of workflow data across multiple orchestrator instances.
/// Each shard is responsible for a subset of workflows determined by hash-based assignment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardingConfig {
    /// Enable sharding
    pub enabled: bool,

    /// Total number of shards (must be power of 2 for efficient hashing)
    pub total_shards: u32,

    /// Shards owned by this node (empty means auto-assign based on cluster membership)
    pub owned_shards: Vec<u32>,

    /// Sharding strategy
    pub strategy: ShardingStrategy,

    /// Shard rebalancing configuration
    pub rebalancing: ShardRebalancingConfig,

    /// Namespace-specific shard overrides
    pub namespace_overrides: HashMap<String, NamespaceShardConfig>,

    /// Shard coordination backend
    pub coordinator: ShardCoordinatorConfig,
}

/// Sharding strategy for assigning workflows to shards
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ShardingStrategy {
    /// Hash-based sharding using workflow_id
    /// Shard = hash(workflow_id) % total_shards
    HashBased,

    /// Range-based sharding using workflow_id prefix
    RangeBased {
        /// Range boundaries (e.g., ["a-m", "n-z"])
        ranges: Vec<String>,
    },

    /// Namespace-based sharding where each namespace maps to specific shards
    NamespaceBased,

    /// Custom sharding using a user-defined function
    Custom {
        /// Name of the custom sharding function
        function_name: String,
    },
}

/// Namespace-specific shard configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamespaceShardConfig {
    /// Dedicated shards for this namespace (overrides hash-based assignment)
    pub dedicated_shards: Option<Vec<u32>>,

    /// Shard affinity preference (soft constraint)
    pub preferred_shards: Option<Vec<u32>>,

    /// Maximum workflows per shard for this namespace
    pub max_workflows_per_shard: Option<u64>,

    /// Isolation level for this namespace
    pub isolation: NamespaceIsolation,
}

/// Namespace isolation levels for sharding
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub enum NamespaceIsolation {
    /// No isolation - workflows can share shards with other namespaces
    #[default]
    Shared,

    /// Soft isolation - prefer dedicated shards but allow sharing under load
    Preferred,

    /// Hard isolation - namespace has exclusive access to its shards
    Exclusive,
}

/// Shard rebalancing configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardRebalancingConfig {
    /// Enable automatic shard rebalancing
    pub enabled: bool,

    /// Rebalancing check interval
    pub check_interval: Duration,

    /// Threshold for triggering rebalancing (load imbalance percentage)
    pub imbalance_threshold_percent: u32,

    /// Maximum concurrent shard migrations
    pub max_concurrent_migrations: u32,

    /// Drain timeout for shard handoff
    pub drain_timeout: Duration,

    /// Minimum time between rebalancing operations
    pub cooldown_period: Duration,
}

/// Shard coordinator backend configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardCoordinatorConfig {
    /// Coordinator backend type
    pub backend: ShardCoordinatorBackend,

    /// Shard lease duration
    pub lease_duration: Duration,

    /// Lease renewal interval
    pub lease_renewal_interval: Duration,

    /// Maximum lease renewal failures before releasing shard
    pub max_renewal_failures: u32,
}

/// Shard coordinator backend types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ShardCoordinatorBackend {
    /// PostgreSQL-based coordination using advisory locks
    PostgreSQL,

    /// Redis-based coordination using distributed locks
    Redis,

    /// etcd-based coordination
    Etcd {
        /// etcd endpoints
        endpoints: Vec<String>,
    },

    /// Kubernetes-based coordination using leases
    Kubernetes {
        /// Kubernetes namespace for lease objects
        namespace: String,
    },
}

impl Default for ShardingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            total_shards: 16,
            owned_shards: Vec::new(),
            strategy: ShardingStrategy::HashBased,
            rebalancing: ShardRebalancingConfig::default(),
            namespace_overrides: HashMap::new(),
            coordinator: ShardCoordinatorConfig::default(),
        }
    }
}

impl Default for ShardRebalancingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            check_interval: Duration::from_secs(60),
            imbalance_threshold_percent: 20,
            max_concurrent_migrations: 2,
            drain_timeout: Duration::from_secs(300),
            cooldown_period: Duration::from_secs(300),
        }
    }
}

impl Default for ShardCoordinatorConfig {
    fn default() -> Self {
        Self {
            backend: ShardCoordinatorBackend::PostgreSQL,
            lease_duration: Duration::from_secs(30),
            lease_renewal_interval: Duration::from_secs(10),
            max_renewal_failures: 3,
        }
    }
}

impl Default for NamespaceShardConfig {
    fn default() -> Self {
        Self {
            dedicated_shards: None,
            preferred_shards: None,
            max_workflows_per_shard: None,
            isolation: NamespaceIsolation::Shared,
        }
    }
}

impl ShardingConfig {
    /// Calculate shard ID for a workflow based on the configured strategy
    pub fn calculate_shard(&self, workflow_id: &str, namespace: &str) -> u32 {
        // Check for namespace override first
        if let Some(ns_config) = self.namespace_overrides.get(namespace) {
            if let Some(ref dedicated) = ns_config.dedicated_shards {
                if !dedicated.is_empty() {
                    // Hash within dedicated shards
                    let hash = Self::hash_workflow_id(workflow_id);
                    return dedicated[(hash as usize) % dedicated.len()];
                }
            }
        }

        // Use default strategy
        match &self.strategy {
            ShardingStrategy::HashBased => Self::hash_workflow_id(workflow_id) % self.total_shards,
            ShardingStrategy::NamespaceBased => {
                // Hash namespace to shard
                Self::hash_string(namespace) % self.total_shards
            }
            ShardingStrategy::RangeBased { ranges } => {
                // Find range that contains the workflow_id prefix
                let first_char = workflow_id
                    .chars()
                    .next()
                    .unwrap_or('a')
                    .to_ascii_lowercase();
                for (idx, range) in ranges.iter().enumerate() {
                    if range.contains(first_char) {
                        return idx as u32;
                    }
                }
                0
            }
            ShardingStrategy::Custom { .. } => {
                // Custom strategy - fallback to hash-based
                Self::hash_workflow_id(workflow_id) % self.total_shards
            }
        }
    }

    /// Check if this node owns a specific shard
    pub fn owns_shard(&self, shard_id: u32) -> bool {
        if self.owned_shards.is_empty() {
            // Auto-assign mode - check with coordinator
            true // Will be determined by coordinator at runtime
        } else {
            self.owned_shards.contains(&shard_id)
        }
    }

    /// Get all shards owned by this node
    pub fn get_owned_shards(&self) -> &[u32] {
        &self.owned_shards
    }

    /// Simple hash function for workflow IDs
    fn hash_workflow_id(workflow_id: &str) -> u32 {
        Self::hash_string(workflow_id)
    }

    /// FNV-1a hash for strings
    fn hash_string(s: &str) -> u32 {
        const FNV_OFFSET: u32 = 2166136261;
        const FNV_PRIME: u32 = 16777619;

        let mut hash = FNV_OFFSET;
        for byte in s.bytes() {
            hash ^= byte as u32;
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        hash
    }
}

/// Configuration for a cluster node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    /// Unique node identifier
    pub node_id: String,

    /// Node bind address
    pub bind_address: SocketAddr,

    /// Node advertise address (external address)
    pub advertise_address: Option<SocketAddr>,

    /// Node region (for geographic distribution)
    pub region: Option<String>,

    /// Node zone (for availability zone distribution)
    pub zone: Option<String>,

    /// Node labels for workload assignment
    pub labels: std::collections::HashMap<String, String>,
}

/// Leadership election configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeadershipConfig {
    /// Leadership election algorithm
    pub algorithm: LeadershipAlgorithm,

    /// Election timeout duration
    pub election_timeout: Duration,

    /// Heartbeat interval
    pub heartbeat_interval: Duration,

    /// Leader lease duration
    pub lease_duration: Duration,

    /// Maximum failed heartbeats before re-election
    pub max_failed_heartbeats: u32,
}

/// Leadership election algorithms
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum LeadershipAlgorithm {
    /// Raft consensus algorithm
    Raft,
    /// Simple leader election with external coordination
    Simple,
    /// Kubernetes-style leader election
    Kubernetes,
}

/// Node discovery configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryConfig {
    /// Discovery method
    pub method: DiscoveryMethod,

    /// Discovery refresh interval
    pub refresh_interval: Duration,

    /// Node registration TTL
    pub registration_ttl: Duration,

    /// Discovery retry configuration
    pub retry: DiscoveryRetryConfig,
}

/// Node discovery methods
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DiscoveryMethod {
    /// Static node list
    Static {
        /// List of node addresses
        nodes: Vec<SocketAddr>,
    },
    /// DNS-based discovery
    Dns {
        /// DNS service name
        service_name: String,
        /// DNS query interval
        query_interval: Duration,
    },
    /// Consul-based discovery
    Consul {
        /// Consul address
        address: String,
        /// Service name in Consul
        service_name: String,
    },
    /// Kubernetes-based discovery
    Kubernetes {
        /// Kubernetes namespace
        namespace: String,
        /// Service name
        service_name: String,
    },
}

/// Discovery retry configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryRetryConfig {
    /// Maximum retry attempts
    pub max_attempts: u32,

    /// Initial retry delay
    pub initial_delay: Duration,

    /// Maximum retry delay
    pub max_delay: Duration,

    /// Retry backoff multiplier
    pub backoff_multiplier: f64,
}

/// Cluster health monitoring configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterHealthConfig {
    /// Health check interval
    pub check_interval: Duration,

    /// Health check timeout
    pub check_timeout: Duration,

    /// Number of failed checks before marking unhealthy
    pub unhealthy_threshold: u32,

    /// Number of successful checks before marking healthy
    pub healthy_threshold: u32,

    /// Enable cluster partitioning detection
    pub partition_detection: bool,

    /// Minimum cluster size for operation
    pub min_cluster_size: u32,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            node_id: Uuid::new_v4().to_string(),
            bind_address: "127.0.0.1:7000".parse().unwrap(),
            advertise_address: None,
            region: None,
            zone: None,
            labels: std::collections::HashMap::new(),
        }
    }
}

impl Default for LeadershipConfig {
    fn default() -> Self {
        Self {
            algorithm: LeadershipAlgorithm::Raft,
            election_timeout: Duration::from_secs(5),
            heartbeat_interval: Duration::from_secs(1),
            lease_duration: Duration::from_secs(30),
            max_failed_heartbeats: 3,
        }
    }
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self {
            method: DiscoveryMethod::Static {
                nodes: vec!["127.0.0.1:7000".parse().unwrap()],
            },
            refresh_interval: Duration::from_secs(30),
            registration_ttl: Duration::from_secs(60),
            retry: DiscoveryRetryConfig::default(),
        }
    }
}

impl Default for DiscoveryRetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            backoff_multiplier: 2.0,
        }
    }
}

impl Default for ClusterHealthConfig {
    fn default() -> Self {
        Self {
            check_interval: Duration::from_secs(10),
            check_timeout: Duration::from_secs(5),
            unhealthy_threshold: 3,
            healthy_threshold: 2,
            partition_detection: true,
            min_cluster_size: 1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_config_default() {
        let config = ClusterConfig::default();

        assert!(!config.node.node_id.is_empty());
        assert_eq!(config.node.bind_address.port(), 7000);
        assert_eq!(config.leadership.algorithm, LeadershipAlgorithm::Raft);
        assert_eq!(config.leadership.election_timeout, Duration::from_secs(5));
    }

    #[test]
    fn test_node_config_default() {
        let config = NodeConfig::default();

        assert!(!config.node_id.is_empty());
        assert_eq!(config.bind_address.port(), 7000);
        assert_eq!(config.bind_address.ip().to_string(), "127.0.0.1");
        assert!(config.advertise_address.is_none());
        assert!(config.labels.is_empty());
    }

    #[test]
    fn test_leadership_config_default() {
        let config = LeadershipConfig::default();

        assert_eq!(config.algorithm, LeadershipAlgorithm::Raft);
        assert_eq!(config.election_timeout, Duration::from_secs(5));
        assert_eq!(config.heartbeat_interval, Duration::from_secs(1));
        assert_eq!(config.max_failed_heartbeats, 3);
    }

    #[test]
    fn test_discovery_config_default() {
        let config = DiscoveryConfig::default();

        assert_eq!(config.refresh_interval, Duration::from_secs(30));
        assert_eq!(config.registration_ttl, Duration::from_secs(60));

        match config.method {
            DiscoveryMethod::Static { nodes } => {
                assert_eq!(nodes.len(), 1);
                assert_eq!(nodes[0].port(), 7000);
            }
            _ => panic!("Expected static discovery method"),
        }
    }

    #[test]
    fn test_cluster_health_config_default() {
        let config = ClusterHealthConfig::default();

        assert_eq!(config.check_interval, Duration::from_secs(10));
        assert_eq!(config.check_timeout, Duration::from_secs(5));
        assert_eq!(config.unhealthy_threshold, 3);
        assert_eq!(config.healthy_threshold, 2);
        assert!(config.partition_detection);
        assert_eq!(config.min_cluster_size, 1);
    }

    #[test]
    fn test_config_serialization() {
        let config = ClusterConfig::default();

        // Test JSON serialization
        let json = serde_json::to_string(&config).unwrap();
        assert!(json.contains("node"));
        assert!(json.contains("leadership"));
        assert!(json.contains("discovery"));

        // Test deserialization
        let deserialized: ClusterConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config.node.bind_address, deserialized.node.bind_address);
        assert_eq!(
            config.leadership.algorithm,
            deserialized.leadership.algorithm
        );
    }

    #[test]
    fn test_discovery_methods() {
        // Test DNS discovery
        let dns_discovery = DiscoveryMethod::Dns {
            service_name: "orcher.local".to_string(),
            query_interval: Duration::from_secs(60),
        };

        match dns_discovery {
            DiscoveryMethod::Dns { service_name, .. } => {
                assert_eq!(service_name, "orcher.local");
            }
            _ => panic!("Expected DNS discovery method"),
        }

        // Test Consul discovery
        let consul_discovery = DiscoveryMethod::Consul {
            address: "127.0.0.1:8500".to_string(),
            service_name: "orcher".to_string(),
        };

        let json = serde_json::to_string(&consul_discovery).unwrap();
        assert!(json.contains("Consul"));
        assert!(json.contains("orcher"));
    }

    #[test]
    fn test_leadership_algorithms() {
        let algorithms = vec![
            LeadershipAlgorithm::Raft,
            LeadershipAlgorithm::Simple,
            LeadershipAlgorithm::Kubernetes,
        ];

        for algorithm in algorithms {
            let json = serde_json::to_string(&algorithm).unwrap();
            let deserialized: LeadershipAlgorithm = serde_json::from_str(&json).unwrap();
            assert_eq!(algorithm, deserialized);
        }
    }

    // ========== Sharding Configuration Tests ==========

    #[test]
    fn test_sharding_config_default() {
        let config = ShardingConfig::default();

        assert!(!config.enabled);
        assert_eq!(config.total_shards, 16);
        assert!(config.owned_shards.is_empty());
        assert_eq!(config.strategy, ShardingStrategy::HashBased);
        assert!(config.namespace_overrides.is_empty());
        assert_eq!(
            config.coordinator.backend,
            ShardCoordinatorBackend::PostgreSQL
        );
    }

    #[test]
    fn test_sharding_hash_based_calculation() {
        let config = ShardingConfig {
            enabled: true,
            total_shards: 16,
            strategy: ShardingStrategy::HashBased,
            ..Default::default()
        };

        // Same workflow_id should always hash to same shard
        let shard1 = config.calculate_shard("workflow-abc-123", "default");
        let shard2 = config.calculate_shard("workflow-abc-123", "default");
        assert_eq!(shard1, shard2);

        // Different workflow_ids should (likely) hash to different shards
        let shard3 = config.calculate_shard("workflow-xyz-789", "default");
        // Just verify it's within range
        assert!(shard3 < 16);
    }

    #[test]
    fn test_sharding_namespace_override() {
        let mut config = ShardingConfig {
            enabled: true,
            total_shards: 16,
            strategy: ShardingStrategy::HashBased,
            ..Default::default()
        };

        // Add namespace override with dedicated shards
        config.namespace_overrides.insert(
            "production".to_string(),
            NamespaceShardConfig {
                dedicated_shards: Some(vec![0, 1, 2, 3]),
                ..Default::default()
            },
        );

        // Workflow in production namespace should only use shards 0-3
        let shard = config.calculate_shard("workflow-abc-123", "production");
        assert!(shard <= 3, "Production shard should be 0-3, got {}", shard);

        // Workflow in other namespace uses all shards
        let shard2 = config.calculate_shard("workflow-abc-123", "staging");
        assert!(shard2 < 16);
    }

    #[test]
    fn test_sharding_namespace_based_strategy() {
        let config = ShardingConfig {
            enabled: true,
            total_shards: 16,
            strategy: ShardingStrategy::NamespaceBased,
            ..Default::default()
        };

        // Same namespace should always hash to same shard
        let shard1 = config.calculate_shard("workflow-1", "production");
        let shard2 = config.calculate_shard("workflow-2", "production");
        assert_eq!(shard1, shard2, "Same namespace should use same shard");

        // Different namespace should (likely) use different shard
        let shard3 = config.calculate_shard("workflow-1", "staging");
        assert!(shard3 < 16);
    }

    #[test]
    fn test_sharding_owns_shard() {
        let config = ShardingConfig {
            enabled: true,
            total_shards: 16,
            owned_shards: vec![0, 4, 8, 12],
            ..Default::default()
        };

        assert!(config.owns_shard(0));
        assert!(config.owns_shard(4));
        assert!(config.owns_shard(8));
        assert!(config.owns_shard(12));
        assert!(!config.owns_shard(1));
        assert!(!config.owns_shard(5));
    }

    #[test]
    fn test_sharding_config_serialization() {
        let mut config = ShardingConfig {
            enabled: true,
            total_shards: 32,
            owned_shards: vec![0, 1, 2, 3],
            strategy: ShardingStrategy::HashBased,
            ..Default::default()
        };

        config.namespace_overrides.insert(
            "production".to_string(),
            NamespaceShardConfig {
                dedicated_shards: Some(vec![0, 1]),
                isolation: NamespaceIsolation::Exclusive,
                ..Default::default()
            },
        );

        let json = serde_json::to_string(&config).unwrap();
        assert!(json.contains("enabled"));
        assert!(json.contains("total_shards"));
        assert!(json.contains("production"));
        assert!(json.contains("Exclusive"));

        let deserialized: ShardingConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config.enabled, deserialized.enabled);
        assert_eq!(config.total_shards, deserialized.total_shards);
        assert_eq!(config.owned_shards, deserialized.owned_shards);
    }

    #[test]
    fn test_shard_coordinator_backends() {
        let backends = vec![
            ShardCoordinatorBackend::PostgreSQL,
            ShardCoordinatorBackend::Redis,
            ShardCoordinatorBackend::Etcd {
                endpoints: vec!["localhost:2379".to_string()],
            },
            ShardCoordinatorBackend::Kubernetes {
                namespace: "orcher".to_string(),
            },
        ];

        for backend in backends {
            let json = serde_json::to_string(&backend).unwrap();
            let deserialized: ShardCoordinatorBackend = serde_json::from_str(&json).unwrap();
            assert_eq!(backend, deserialized);
        }
    }

    #[test]
    fn test_namespace_isolation_levels() {
        assert_eq!(NamespaceIsolation::default(), NamespaceIsolation::Shared);

        let levels = vec![
            NamespaceIsolation::Shared,
            NamespaceIsolation::Preferred,
            NamespaceIsolation::Exclusive,
        ];

        for level in levels {
            let json = serde_json::to_string(&level).unwrap();
            let deserialized: NamespaceIsolation = serde_json::from_str(&json).unwrap();
            assert_eq!(level, deserialized);
        }
    }

    #[test]
    fn test_rebalancing_config_default() {
        let config = ShardRebalancingConfig::default();

        assert!(config.enabled);
        assert_eq!(config.check_interval, Duration::from_secs(60));
        assert_eq!(config.imbalance_threshold_percent, 20);
        assert_eq!(config.max_concurrent_migrations, 2);
        assert_eq!(config.drain_timeout, Duration::from_secs(300));
        assert_eq!(config.cooldown_period, Duration::from_secs(300));
    }
}
