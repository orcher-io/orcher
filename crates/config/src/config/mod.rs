//! Top-level configuration types.

pub mod cluster;
pub mod monitoring;
pub mod security;
pub mod storage;
pub mod workflow;

use serde::{Deserialize, Serialize};

/// Main ORCHER configuration structure
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct OrcherConfig {
    /// Storage configuration
    pub storage: storage::StorageConfig,

    /// Security configuration
    pub security: security::SecurityConfig,

    /// Monitoring configuration
    pub monitoring: monitoring::MonitoringConfig,

    /// Cluster configuration
    pub cluster: cluster::ClusterConfig,

    /// Workflow configuration
    pub workflow: workflow::WorkflowConfig,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_orcher_config_default() {
        let config = OrcherConfig::default();
        assert!(config.storage.database.postgres_url.contains("postgresql"));
        assert!(config.storage.redis.url.contains("redis"));
    }

    #[test]
    fn test_config_serialization() {
        let config = OrcherConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        assert!(json.contains("storage"));
        assert!(json.contains("security"));

        let deserialized: OrcherConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(
            config.storage.database.postgres_url,
            deserialized.storage.database.postgres_url
        );
    }
}
