//! Tenant configuration types.

use std::collections::HashMap;
use std::time::Duration;

/// Unique identifier for a tenant
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TenantId(String);

impl TenantId {
    /// Create a new tenant ID
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Get the tenant ID as a string
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for TenantId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&str> for TenantId {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

impl From<String> for TenantId {
    fn from(s: String) -> Self {
        Self::new(s)
    }
}

/// Tenant tier for predefined limit profiles
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TenantTier {
    /// Free tier with minimal limits
    #[default]
    Free,

    /// Starter tier for small teams
    Starter,

    /// Professional tier for growing teams
    Professional,

    /// Enterprise tier with high limits
    Enterprise,

    /// Unlimited tier (no limits)
    Unlimited,
}

impl TenantTier {
    /// Get default workflow limit for this tier
    pub fn default_workflow_limit(&self) -> Option<u64> {
        match self {
            TenantTier::Free => Some(100),
            TenantTier::Starter => Some(1_000),
            TenantTier::Professional => Some(10_000),
            TenantTier::Enterprise => Some(100_000),
            TenantTier::Unlimited => None,
        }
    }

    /// Get default rate limit (requests per second) for this tier
    pub fn default_rate_limit(&self) -> Option<u64> {
        match self {
            TenantTier::Free => Some(10),
            TenantTier::Starter => Some(50),
            TenantTier::Professional => Some(200),
            TenantTier::Enterprise => Some(1000),
            TenantTier::Unlimited => None,
        }
    }

    /// Get default memory limit in bytes for this tier
    pub fn default_memory_limit(&self) -> Option<u64> {
        match self {
            TenantTier::Free => Some(256 * 1024 * 1024), // 256 MB
            TenantTier::Starter => Some(1024 * 1024 * 1024), // 1 GB
            TenantTier::Professional => Some(4 * 1024 * 1024 * 1024), // 4 GB
            TenantTier::Enterprise => Some(16 * 1024 * 1024 * 1024), // 16 GB
            TenantTier::Unlimited => None,
        }
    }
}

/// Isolation level for tenant resources
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IsolationLevel {
    /// Shared resources with soft limits
    #[default]
    Shared,

    /// Dedicated resources with guaranteed allocation
    Dedicated,

    /// Isolated resources with strict boundaries
    Isolated,
}

/// Configuration for a single tenant
#[derive(Debug, Clone)]
pub struct TenantConfig {
    /// Tenant identifier
    pub id: TenantId,

    /// Display name
    pub name: String,

    /// Tenant tier
    pub tier: TenantTier,

    /// Isolation level
    pub isolation_level: IsolationLevel,

    /// Maximum concurrent workflows
    pub max_concurrent_workflows: Option<u64>,

    /// Maximum concurrent tasks
    pub max_concurrent_tasks: Option<u64>,

    /// Maximum workflow executions per day
    pub max_workflows_per_day: Option<u64>,

    /// Maximum tasks per day
    pub max_tasks_per_day: Option<u64>,

    /// Maximum rate (requests per second)
    pub max_rate_per_second: Option<u64>,

    /// Maximum burst size for rate limiting
    pub max_burst_size: Option<u64>,

    /// Maximum memory usage in bytes
    pub max_memory_bytes: Option<u64>,

    /// Maximum payload size in bytes
    pub max_payload_size: Option<u64>,

    /// Maximum workflow execution time
    pub max_workflow_duration: Option<Duration>,

    /// Maximum task execution time
    pub max_task_duration: Option<Duration>,

    /// Maximum number of child workflows per execution
    pub max_child_workflows: Option<u64>,

    /// Maximum journal size per workflow
    pub max_journal_entries: Option<u64>,

    /// Whether the tenant is enabled
    pub enabled: bool,

    /// Custom metadata
    pub metadata: HashMap<String, String>,
}

impl TenantConfig {
    /// Create a new tenant config with tier defaults
    pub fn new(id: impl Into<TenantId>, name: impl Into<String>, tier: TenantTier) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            tier,
            isolation_level: IsolationLevel::default(),
            max_concurrent_workflows: tier.default_workflow_limit(),
            max_concurrent_tasks: tier.default_workflow_limit().map(|w| w * 10),
            max_workflows_per_day: tier.default_workflow_limit().map(|w| w * 100),
            max_tasks_per_day: tier.default_workflow_limit().map(|w| w * 1000),
            max_rate_per_second: tier.default_rate_limit(),
            max_burst_size: tier.default_rate_limit().map(|r| r * 2),
            max_memory_bytes: tier.default_memory_limit(),
            max_payload_size: Some(10 * 1024 * 1024), // 10 MB default
            max_workflow_duration: Some(Duration::from_secs(24 * 60 * 60)), // 24 hours
            max_task_duration: Some(Duration::from_secs(60 * 60)), // 1 hour
            max_child_workflows: Some(100),
            max_journal_entries: Some(10_000),
            enabled: true,
            metadata: HashMap::new(),
        }
    }

    /// Create a free tier tenant
    pub fn free(id: impl Into<TenantId>, name: impl Into<String>) -> Self {
        Self::new(id, name, TenantTier::Free)
    }

    /// Create a starter tier tenant
    pub fn starter(id: impl Into<TenantId>, name: impl Into<String>) -> Self {
        Self::new(id, name, TenantTier::Starter)
    }

    /// Create a professional tier tenant
    pub fn professional(id: impl Into<TenantId>, name: impl Into<String>) -> Self {
        Self::new(id, name, TenantTier::Professional)
    }

    /// Create an enterprise tier tenant
    pub fn enterprise(id: impl Into<TenantId>, name: impl Into<String>) -> Self {
        Self::new(id, name, TenantTier::Enterprise)
    }

    /// Create an unlimited tier tenant
    pub fn unlimited(id: impl Into<TenantId>, name: impl Into<String>) -> Self {
        Self::new(id, name, TenantTier::Unlimited)
    }

    /// Set isolation level
    pub fn with_isolation(mut self, level: IsolationLevel) -> Self {
        self.isolation_level = level;
        self
    }

    /// Set maximum concurrent workflows
    pub fn with_max_workflows(mut self, limit: u64) -> Self {
        self.max_concurrent_workflows = Some(limit);
        self
    }

    /// Set maximum rate per second
    pub fn with_rate_limit(mut self, rate: u64) -> Self {
        self.max_rate_per_second = Some(rate);
        self.max_burst_size = Some(rate * 2);
        self
    }

    /// Set maximum memory
    pub fn with_max_memory(mut self, bytes: u64) -> Self {
        self.max_memory_bytes = Some(bytes);
        self
    }

    /// Add metadata
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Disable the tenant
    pub fn disabled(mut self) -> Self {
        self.enabled = false;
        self
    }

    /// Check if a workflow limit is set
    pub fn has_workflow_limit(&self) -> bool {
        self.max_concurrent_workflows.is_some()
    }

    /// Check if a rate limit is set
    pub fn has_rate_limit(&self) -> bool {
        self.max_rate_per_second.is_some()
    }

    /// Check if a memory limit is set
    pub fn has_memory_limit(&self) -> bool {
        self.max_memory_bytes.is_some()
    }
}

impl Default for TenantConfig {
    fn default() -> Self {
        Self::new("default", "Default Tenant", TenantTier::Free)
    }
}

// ================================================================================================
// TESTS
// ================================================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tenant_id() {
        let id = TenantId::new("org-123");
        assert_eq!(id.as_str(), "org-123");
        assert_eq!(id.to_string(), "org-123");
    }

    #[test]
    fn test_tenant_id_from() {
        let id1: TenantId = "org-a".into();
        let id2: TenantId = String::from("org-b").into();
        assert_eq!(id1.as_str(), "org-a");
        assert_eq!(id2.as_str(), "org-b");
    }

    #[test]
    fn test_tier_defaults() {
        assert_eq!(TenantTier::Free.default_workflow_limit(), Some(100));
        assert_eq!(TenantTier::Starter.default_rate_limit(), Some(50));
        assert_eq!(
            TenantTier::Enterprise.default_memory_limit(),
            Some(16 * 1024 * 1024 * 1024)
        );
        assert_eq!(TenantTier::Unlimited.default_workflow_limit(), None);
    }

    #[test]
    fn test_tenant_config_free() {
        let config = TenantConfig::free("tenant-1", "Test Tenant");
        assert_eq!(config.tier, TenantTier::Free);
        assert_eq!(config.max_concurrent_workflows, Some(100));
        assert_eq!(config.max_rate_per_second, Some(10));
        assert!(config.enabled);
    }

    #[test]
    fn test_tenant_config_enterprise() {
        let config = TenantConfig::enterprise("big-corp", "Big Corporation");
        assert_eq!(config.tier, TenantTier::Enterprise);
        assert_eq!(config.max_concurrent_workflows, Some(100_000));
        assert_eq!(config.max_rate_per_second, Some(1000));
    }

    #[test]
    fn test_tenant_config_unlimited() {
        let config = TenantConfig::unlimited("vip", "VIP Customer");
        assert_eq!(config.tier, TenantTier::Unlimited);
        assert!(config.max_concurrent_workflows.is_none());
        assert!(config.max_rate_per_second.is_none());
        assert!(!config.has_workflow_limit());
        assert!(!config.has_rate_limit());
    }

    #[test]
    fn test_tenant_config_builder() {
        let config = TenantConfig::starter("custom", "Custom Tenant")
            .with_isolation(IsolationLevel::Dedicated)
            .with_max_workflows(5000)
            .with_rate_limit(100)
            .with_max_memory(2 * 1024 * 1024 * 1024)
            .with_metadata("team", "engineering");

        assert_eq!(config.isolation_level, IsolationLevel::Dedicated);
        assert_eq!(config.max_concurrent_workflows, Some(5000));
        assert_eq!(config.max_rate_per_second, Some(100));
        assert_eq!(
            config.metadata.get("team"),
            Some(&"engineering".to_string())
        );
    }

    #[test]
    fn test_tenant_config_disabled() {
        let config = TenantConfig::free("disabled", "Disabled Tenant").disabled();
        assert!(!config.enabled);
    }

    #[test]
    fn test_default_tenant_config() {
        let config = TenantConfig::default();
        assert_eq!(config.tier, TenantTier::Free);
        assert!(config.enabled);
    }

    #[test]
    fn test_has_limits() {
        let free = TenantConfig::free("t1", "T1");
        assert!(free.has_workflow_limit());
        assert!(free.has_rate_limit());
        assert!(free.has_memory_limit());

        let unlimited = TenantConfig::unlimited("t2", "T2");
        assert!(!unlimited.has_workflow_limit());
        assert!(!unlimited.has_rate_limit());
    }
}
