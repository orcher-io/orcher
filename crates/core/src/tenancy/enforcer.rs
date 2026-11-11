//! Tenant resource limit enforcement.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

use super::config::{TenantConfig, TenantId};
use super::limits::{RateLimiter, ResourceQuota};

/// Result of an enforcement check
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnforcementResult {
    /// Request is allowed
    Allowed,

    /// Request is denied due to workflow limit
    WorkflowLimitExceeded {
        /// Current workflow count
        current: u64,
        /// Workflow limit
        limit: u64,
    },

    /// Request is denied due to task limit
    TaskLimitExceeded {
        /// Current task count
        current: u64,
        /// Task limit
        limit: u64,
    },

    /// Request is denied due to rate limit
    RateLimitExceeded {
        /// Rate limit per second
        rate: u64,
    },

    /// Request is denied due to memory limit
    MemoryLimitExceeded {
        /// Current memory usage
        current: u64,
        /// Memory limit
        limit: u64,
    },

    /// Request is denied due to payload size limit
    PayloadTooLarge {
        /// Payload size
        size: u64,
        /// Size limit
        limit: u64,
    },

    /// Tenant is disabled
    TenantDisabled,

    /// Tenant not found
    TenantNotFound,
}

impl EnforcementResult {
    /// Check if the result is allowed
    pub fn is_allowed(&self) -> bool {
        matches!(self, EnforcementResult::Allowed)
    }

    /// Check if the result is denied
    pub fn is_denied(&self) -> bool {
        !self.is_allowed()
    }

    /// Convert to Result
    pub fn into_result(self) -> Result<(), EnforcementResult> {
        if self.is_allowed() {
            Ok(())
        } else {
            Err(self)
        }
    }
}

/// Current usage for a tenant
#[derive(Debug)]
pub struct TenantUsage {
    /// Workflow quota
    pub workflows: ResourceQuota,

    /// Task quota
    pub tasks: ResourceQuota,

    /// Memory quota
    pub memory: ResourceQuota,

    /// Rate limiter
    pub rate_limiter: Option<RateLimiter>,

    /// Total requests
    pub total_requests: AtomicU64,

    /// Denied requests
    pub denied_requests: AtomicU64,
}

impl TenantUsage {
    /// Create new usage tracking for a tenant
    pub fn new(config: &TenantConfig) -> Self {
        let rate_limiter = match (config.max_rate_per_second, config.max_burst_size) {
            (Some(rate), Some(burst)) => Some(RateLimiter::new(rate, burst)),
            (Some(rate), None) => Some(RateLimiter::new(rate, rate * 2)),
            _ => None,
        };

        Self {
            workflows: ResourceQuota::new(config.max_concurrent_workflows),
            tasks: ResourceQuota::new(config.max_concurrent_tasks),
            memory: ResourceQuota::new(config.max_memory_bytes),
            rate_limiter,
            total_requests: AtomicU64::new(0),
            denied_requests: AtomicU64::new(0),
        }
    }

    /// Record a request
    pub fn record_request(&self, allowed: bool) {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
        if !allowed {
            self.denied_requests.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Get denial rate
    pub fn denial_rate(&self) -> f64 {
        let total = self.total_requests.load(Ordering::Relaxed);
        let denied = self.denied_requests.load(Ordering::Relaxed);
        if total == 0 {
            0.0
        } else {
            denied as f64 / total as f64
        }
    }
}

/// Tenant resource enforcer metrics
#[derive(Debug, Default)]
pub struct EnforcerMetrics {
    /// Total checks performed
    pub total_checks: AtomicU64,

    /// Checks that passed
    pub checks_allowed: AtomicU64,

    /// Checks that failed
    pub checks_denied: AtomicU64,

    /// Workflow limit denials
    pub workflow_denials: AtomicU64,

    /// Task limit denials
    pub task_denials: AtomicU64,

    /// Rate limit denials
    pub rate_denials: AtomicU64,

    /// Memory limit denials
    pub memory_denials: AtomicU64,
}

impl EnforcerMetrics {
    /// Record a check result
    pub fn record(&self, result: &EnforcementResult) {
        self.total_checks.fetch_add(1, Ordering::Relaxed);
        match result {
            EnforcementResult::Allowed => {
                self.checks_allowed.fetch_add(1, Ordering::Relaxed);
            }
            EnforcementResult::WorkflowLimitExceeded { .. } => {
                self.checks_denied.fetch_add(1, Ordering::Relaxed);
                self.workflow_denials.fetch_add(1, Ordering::Relaxed);
            }
            EnforcementResult::TaskLimitExceeded { .. } => {
                self.checks_denied.fetch_add(1, Ordering::Relaxed);
                self.task_denials.fetch_add(1, Ordering::Relaxed);
            }
            EnforcementResult::RateLimitExceeded { .. } => {
                self.checks_denied.fetch_add(1, Ordering::Relaxed);
                self.rate_denials.fetch_add(1, Ordering::Relaxed);
            }
            EnforcementResult::MemoryLimitExceeded { .. } => {
                self.checks_denied.fetch_add(1, Ordering::Relaxed);
                self.memory_denials.fetch_add(1, Ordering::Relaxed);
            }
            _ => {
                self.checks_denied.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Get allow rate
    pub fn allow_rate(&self) -> f64 {
        let total = self.total_checks.load(Ordering::Relaxed);
        let allowed = self.checks_allowed.load(Ordering::Relaxed);
        if total == 0 {
            1.0
        } else {
            allowed as f64 / total as f64
        }
    }
}

/// Tenant resource enforcer
///
/// Centralized enforcement of tenant resource limits and quotas.
pub struct TenantResourceEnforcer {
    /// Tenant configurations
    configs: RwLock<HashMap<TenantId, TenantConfig>>,

    /// Tenant usage tracking
    usage: RwLock<HashMap<TenantId, TenantUsage>>,

    /// Metrics
    metrics: EnforcerMetrics,

    /// Default tenant config (for unknown tenants)
    #[allow(dead_code)]
    default_config: TenantConfig,
}

impl TenantResourceEnforcer {
    /// Create a new enforcer
    pub fn new() -> Self {
        Self {
            configs: RwLock::new(HashMap::new()),
            usage: RwLock::new(HashMap::new()),
            metrics: EnforcerMetrics::default(),
            default_config: TenantConfig::default(),
        }
    }

    /// Create with a custom default config
    pub fn with_default_config(default_config: TenantConfig) -> Self {
        Self {
            configs: RwLock::new(HashMap::new()),
            usage: RwLock::new(HashMap::new()),
            metrics: EnforcerMetrics::default(),
            default_config,
        }
    }

    /// Register a tenant
    pub fn register_tenant(&self, config: TenantConfig) {
        let id = config.id.clone();
        let usage = TenantUsage::new(&config);

        self.configs.write().unwrap().insert(id.clone(), config);
        self.usage.write().unwrap().insert(id, usage);
    }

    /// Remove a tenant
    pub fn remove_tenant(&self, tenant_id: &TenantId) {
        self.configs.write().unwrap().remove(tenant_id);
        self.usage.write().unwrap().remove(tenant_id);
    }

    /// Get tenant config
    pub fn get_config(&self, tenant_id: &TenantId) -> Option<TenantConfig> {
        self.configs.read().unwrap().get(tenant_id).cloned()
    }

    /// Check if a workflow can be created
    pub fn check_workflow(&self, tenant_id: &TenantId) -> EnforcementResult {
        let result = self.check_workflow_internal(tenant_id);
        self.metrics.record(&result);

        if let Some(usage) = self.usage.read().unwrap().get(tenant_id) {
            usage.record_request(result.is_allowed());
        }

        result
    }

    fn check_workflow_internal(&self, tenant_id: &TenantId) -> EnforcementResult {
        let configs = self.configs.read().unwrap();
        let config = match configs.get(tenant_id) {
            Some(c) => c,
            None => return EnforcementResult::TenantNotFound,
        };

        if !config.enabled {
            return EnforcementResult::TenantDisabled;
        }

        let usage = self.usage.read().unwrap();
        let tenant_usage = match usage.get(tenant_id) {
            Some(u) => u,
            None => return EnforcementResult::TenantNotFound,
        };

        // Check rate limit first
        if let Some(ref rate_limiter) = tenant_usage.rate_limiter {
            if !rate_limiter.try_acquire() {
                return EnforcementResult::RateLimitExceeded {
                    rate: rate_limiter.rate(),
                };
            }
        }

        // Check workflow quota
        if !tenant_usage.workflows.try_acquire(1) {
            return EnforcementResult::WorkflowLimitExceeded {
                current: tenant_usage.workflows.current(),
                limit: tenant_usage.workflows.limit().unwrap_or(0),
            };
        }

        EnforcementResult::Allowed
    }

    /// Release a workflow slot
    pub fn release_workflow(&self, tenant_id: &TenantId) {
        if let Some(usage) = self.usage.read().unwrap().get(tenant_id) {
            usage.workflows.release(1);
        }
    }

    /// Check if a task can be created
    pub fn check_task(&self, tenant_id: &TenantId) -> EnforcementResult {
        let result = self.check_task_internal(tenant_id);
        self.metrics.record(&result);

        if let Some(usage) = self.usage.read().unwrap().get(tenant_id) {
            usage.record_request(result.is_allowed());
        }

        result
    }

    fn check_task_internal(&self, tenant_id: &TenantId) -> EnforcementResult {
        let configs = self.configs.read().unwrap();
        let config = match configs.get(tenant_id) {
            Some(c) => c,
            None => return EnforcementResult::TenantNotFound,
        };

        if !config.enabled {
            return EnforcementResult::TenantDisabled;
        }

        let usage = self.usage.read().unwrap();
        let tenant_usage = match usage.get(tenant_id) {
            Some(u) => u,
            None => return EnforcementResult::TenantNotFound,
        };

        // Check task quota
        if !tenant_usage.tasks.try_acquire(1) {
            return EnforcementResult::TaskLimitExceeded {
                current: tenant_usage.tasks.current(),
                limit: tenant_usage.tasks.limit().unwrap_or(0),
            };
        }

        EnforcementResult::Allowed
    }

    /// Release a task slot
    pub fn release_task(&self, tenant_id: &TenantId) {
        if let Some(usage) = self.usage.read().unwrap().get(tenant_id) {
            usage.tasks.release(1);
        }
    }

    /// Check if a payload size is acceptable
    pub fn check_payload_size(&self, tenant_id: &TenantId, size: u64) -> EnforcementResult {
        let configs = self.configs.read().unwrap();
        let config = match configs.get(tenant_id) {
            Some(c) => c,
            None => return EnforcementResult::TenantNotFound,
        };

        if let Some(limit) = config.max_payload_size {
            if size > limit {
                return EnforcementResult::PayloadTooLarge { size, limit };
            }
        }

        EnforcementResult::Allowed
    }

    /// Check rate limit only
    pub fn check_rate(&self, tenant_id: &TenantId) -> EnforcementResult {
        let usage = self.usage.read().unwrap();
        let tenant_usage = match usage.get(tenant_id) {
            Some(u) => u,
            None => return EnforcementResult::TenantNotFound,
        };

        if let Some(ref rate_limiter) = tenant_usage.rate_limiter {
            if !rate_limiter.try_acquire() {
                return EnforcementResult::RateLimitExceeded {
                    rate: rate_limiter.rate(),
                };
            }
        }

        EnforcementResult::Allowed
    }

    /// Get usage stats for a tenant
    pub fn get_usage_stats(&self, tenant_id: &TenantId) -> Option<TenantUsageStats> {
        let usage = self.usage.read().unwrap();
        usage.get(tenant_id).map(|u| TenantUsageStats {
            current_workflows: u.workflows.current(),
            workflow_limit: u.workflows.limit(),
            current_tasks: u.tasks.current(),
            task_limit: u.tasks.limit(),
            current_memory: u.memory.current(),
            memory_limit: u.memory.limit(),
            denial_rate: u.denial_rate(),
        })
    }

    /// Get the enforcer metrics
    pub fn metrics(&self) -> &EnforcerMetrics {
        &self.metrics
    }

    /// Get tenant count
    pub fn tenant_count(&self) -> usize {
        self.configs.read().unwrap().len()
    }
}

impl Default for TenantResourceEnforcer {
    fn default() -> Self {
        Self::new()
    }
}

/// Usage statistics for a tenant
#[derive(Debug, Clone)]
pub struct TenantUsageStats {
    /// Current concurrent workflows
    pub current_workflows: u64,

    /// Workflow limit
    pub workflow_limit: Option<u64>,

    /// Current concurrent tasks
    pub current_tasks: u64,

    /// Task limit
    pub task_limit: Option<u64>,

    /// Current memory usage
    pub current_memory: u64,

    /// Memory limit
    pub memory_limit: Option<u64>,

    /// Denial rate
    pub denial_rate: f64,
}

impl TenantUsageStats {
    /// Calculate workflow utilization
    pub fn workflow_utilization(&self) -> Option<f64> {
        self.workflow_limit.map(|l| {
            if l == 0 {
                0.0
            } else {
                self.current_workflows as f64 / l as f64
            }
        })
    }

    /// Calculate task utilization
    pub fn task_utilization(&self) -> Option<f64> {
        self.task_limit.map(|l| {
            if l == 0 {
                0.0
            } else {
                self.current_tasks as f64 / l as f64
            }
        })
    }
}

// ================================================================================================
// TESTS
// ================================================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_enforcer() -> TenantResourceEnforcer {
        let enforcer = TenantResourceEnforcer::new();
        enforcer.register_tenant(TenantConfig::starter("tenant-a", "Tenant A"));
        enforcer.register_tenant(TenantConfig::enterprise("tenant-b", "Tenant B"));
        enforcer.register_tenant(TenantConfig::free("tenant-disabled", "Disabled").disabled());
        enforcer
    }

    #[test]
    fn test_enforcer_creation() {
        let enforcer = TenantResourceEnforcer::new();
        assert_eq!(enforcer.tenant_count(), 0);
    }

    #[test]
    fn test_register_tenant() {
        let enforcer = create_test_enforcer();
        assert_eq!(enforcer.tenant_count(), 3);
        assert!(enforcer.get_config(&TenantId::new("tenant-a")).is_some());
    }

    #[test]
    fn test_check_workflow_allowed() {
        let enforcer = create_test_enforcer();
        let result = enforcer.check_workflow(&TenantId::new("tenant-a"));
        assert!(result.is_allowed());
    }

    #[test]
    fn test_check_workflow_unknown_tenant() {
        let enforcer = create_test_enforcer();
        let result = enforcer.check_workflow(&TenantId::new("unknown"));
        assert_eq!(result, EnforcementResult::TenantNotFound);
    }

    #[test]
    fn test_check_workflow_disabled_tenant() {
        let enforcer = create_test_enforcer();
        let result = enforcer.check_workflow(&TenantId::new("tenant-disabled"));
        assert_eq!(result, EnforcementResult::TenantDisabled);
    }

    #[test]
    fn test_workflow_limit_enforcement() {
        let enforcer = TenantResourceEnforcer::new();
        enforcer.register_tenant(
            TenantConfig::new("limited", "Limited", super::super::config::TenantTier::Free)
                .with_max_workflows(3),
        );

        let tenant = TenantId::new("limited");

        // Should allow first 3
        assert!(enforcer.check_workflow(&tenant).is_allowed());
        assert!(enforcer.check_workflow(&tenant).is_allowed());
        assert!(enforcer.check_workflow(&tenant).is_allowed());

        // 4th should fail
        let result = enforcer.check_workflow(&tenant);
        assert!(matches!(
            result,
            EnforcementResult::WorkflowLimitExceeded { .. }
        ));

        // Release one and try again
        enforcer.release_workflow(&tenant);
        assert!(enforcer.check_workflow(&tenant).is_allowed());
    }

    #[test]
    fn test_task_limit_enforcement() {
        let enforcer = TenantResourceEnforcer::new();
        enforcer.register_tenant(TenantConfig::starter("tenant", "Tenant"));

        let tenant = TenantId::new("tenant");

        // Should allow tasks
        assert!(enforcer.check_task(&tenant).is_allowed());

        // Release
        enforcer.release_task(&tenant);
    }

    #[test]
    fn test_payload_size_check() {
        let enforcer = create_test_enforcer();
        let tenant = TenantId::new("tenant-a");

        // Small payload should be allowed
        assert!(enforcer.check_payload_size(&tenant, 1000).is_allowed());

        // Huge payload should be denied
        let result = enforcer.check_payload_size(&tenant, 100 * 1024 * 1024);
        assert!(matches!(result, EnforcementResult::PayloadTooLarge { .. }));
    }

    #[test]
    fn test_usage_stats() {
        let enforcer = create_test_enforcer();
        let tenant = TenantId::new("tenant-a");

        // Check workflow to consume a slot
        enforcer.check_workflow(&tenant);

        let stats = enforcer.get_usage_stats(&tenant).unwrap();
        assert_eq!(stats.current_workflows, 1);
        assert!(stats.workflow_limit.is_some());
    }

    #[test]
    fn test_enforcer_metrics() {
        let enforcer = create_test_enforcer();
        let tenant = TenantId::new("tenant-a");

        enforcer.check_workflow(&tenant);
        enforcer.check_task(&tenant);

        let metrics = enforcer.metrics();
        assert!(metrics.total_checks.load(Ordering::Relaxed) >= 2);
    }

    #[test]
    fn test_remove_tenant() {
        let enforcer = create_test_enforcer();
        assert_eq!(enforcer.tenant_count(), 3);

        enforcer.remove_tenant(&TenantId::new("tenant-a"));
        assert_eq!(enforcer.tenant_count(), 2);
        assert!(enforcer.get_config(&TenantId::new("tenant-a")).is_none());
    }

    #[test]
    fn test_enforcement_result_helpers() {
        let allowed = EnforcementResult::Allowed;
        assert!(allowed.is_allowed());
        assert!(!allowed.is_denied());
        assert!(allowed.into_result().is_ok());

        let denied = EnforcementResult::TenantDisabled;
        assert!(!denied.is_allowed());
        assert!(denied.is_denied());
        assert!(denied.into_result().is_err());
    }

    #[test]
    fn test_usage_stats_utilization() {
        let stats = TenantUsageStats {
            current_workflows: 50,
            workflow_limit: Some(100),
            current_tasks: 250,
            task_limit: Some(1000),
            current_memory: 0,
            memory_limit: None,
            denial_rate: 0.0,
        };

        assert!((stats.workflow_utilization().unwrap() - 0.5).abs() < 0.01);
        assert!((stats.task_utilization().unwrap() - 0.25).abs() < 0.01);
    }
}
