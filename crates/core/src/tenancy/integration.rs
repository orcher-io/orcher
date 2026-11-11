//! Tenant enforcement integration.

use std::sync::Arc;

use super::config::{TenantConfig, TenantId};
use super::enforcer::{EnforcementResult, TenantResourceEnforcer, TenantUsageStats};

// Use the monitoring crate for metrics (stateless approach)
use orcher_monitoring::prelude::{
    record_tenant_api_request, record_tenant_quota_exceeded, record_tenant_rate_limited,
    record_tenant_task_executed, record_tenant_task_failed, record_tenant_workflow_completed,
    record_tenant_workflow_failed, record_tenant_workflow_started,
};

/// Integration layer for tenant enforcement with workflow coordination
///
/// This provides a simplified interface for enforcing tenant limits
/// during workflow operations.
pub struct TenantEnforcementIntegration {
    /// The tenant resource enforcer
    enforcer: Arc<TenantResourceEnforcer>,

    /// Whether integration is enabled
    enabled: bool,

    /// Whether metrics collection is enabled
    metrics_enabled: bool,
}

impl TenantEnforcementIntegration {
    /// Create a new integration with the given enforcer
    pub fn new(enforcer: Arc<TenantResourceEnforcer>) -> Self {
        Self {
            enforcer,
            enabled: true,
            metrics_enabled: false,
        }
    }

    /// Create with metrics collection enabled
    pub fn with_metrics(enforcer: Arc<TenantResourceEnforcer>) -> Self {
        Self {
            enforcer,
            enabled: true,
            metrics_enabled: true,
        }
    }

    /// Enable or disable integration
    pub fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
    }

    /// Check if integration is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Enable or disable metrics collection
    pub fn set_metrics_enabled(&mut self, enabled: bool) {
        self.metrics_enabled = enabled;
    }

    /// Check if metrics collection is enabled
    pub fn is_metrics_enabled(&self) -> bool {
        self.metrics_enabled
    }

    /// Check if a workflow can be started for a tenant
    ///
    /// Returns Ok(()) if allowed, or an enforcement error if denied.
    pub fn check_start_workflow(&self, tenant_id: &TenantId) -> Result<(), EnforcementResult> {
        if !self.enabled {
            return Ok(());
        }

        let result = self.enforcer.check_workflow(tenant_id);

        // Record metrics if enabled
        if self.metrics_enabled {
            if result.is_allowed() {
                record_tenant_workflow_started(tenant_id.as_str());
            } else {
                record_tenant_quota_exceeded(tenant_id.as_str(), "workflow");
            }
        }

        result.into_result()
    }

    /// Release a workflow slot when workflow completes
    pub fn release_workflow(&self, tenant_id: &TenantId, succeeded: bool) {
        if !self.enabled {
            return;
        }

        self.enforcer.release_workflow(tenant_id);

        // Record metrics if enabled
        if self.metrics_enabled {
            if succeeded {
                record_tenant_workflow_completed(tenant_id.as_str());
            } else {
                record_tenant_workflow_failed(tenant_id.as_str());
            }
        }
    }

    /// Check if a task can be executed for a tenant
    pub fn check_execute_task(&self, tenant_id: &TenantId) -> Result<(), EnforcementResult> {
        if !self.enabled {
            return Ok(());
        }

        let result = self.enforcer.check_task(tenant_id);

        // Record metrics if denied
        if self.metrics_enabled && result.is_denied() {
            record_tenant_quota_exceeded(tenant_id.as_str(), "task");
        }

        result.into_result()
    }

    /// Release a task slot when task completes
    pub fn release_task(&self, tenant_id: &TenantId, succeeded: bool) {
        if !self.enabled {
            return;
        }

        self.enforcer.release_task(tenant_id);

        // Record metrics if enabled
        if self.metrics_enabled {
            if succeeded {
                record_tenant_task_executed(tenant_id.as_str());
            } else {
                record_tenant_task_failed(tenant_id.as_str());
            }
        }
    }

    /// Check if a payload size is acceptable
    pub fn check_payload_size(
        &self,
        tenant_id: &TenantId,
        size: u64,
    ) -> Result<(), EnforcementResult> {
        if !self.enabled {
            return Ok(());
        }

        self.enforcer
            .check_payload_size(tenant_id, size)
            .into_result()
    }

    /// Check rate limit for an API request
    pub fn check_rate_limit(&self, tenant_id: &TenantId) -> Result<(), EnforcementResult> {
        if !self.enabled {
            return Ok(());
        }

        let result = self.enforcer.check_rate(tenant_id);

        // Record metrics if enabled
        if self.metrics_enabled {
            record_tenant_api_request(tenant_id.as_str(), "rate_check");
            if result.is_denied() {
                record_tenant_rate_limited(tenant_id.as_str());
            }
        }

        result.into_result()
    }

    /// Register a tenant with the enforcer
    pub fn register_tenant(&self, config: TenantConfig) {
        self.enforcer.register_tenant(config);
    }

    /// Get usage stats for a tenant
    pub fn get_usage_stats(&self, tenant_id: &TenantId) -> Option<TenantUsageStats> {
        self.enforcer.get_usage_stats(tenant_id)
    }

    /// Get the underlying enforcer
    pub fn enforcer(&self) -> &Arc<TenantResourceEnforcer> {
        &self.enforcer
    }

    /// Check if a tenant is healthy (not being throttled)
    pub fn is_tenant_healthy(&self, tenant_id: &TenantId) -> bool {
        if let Some(stats) = self.get_usage_stats(tenant_id) {
            stats.denial_rate < 0.1 // Less than 10% denial rate
        } else {
            false // Unknown tenant is not healthy
        }
    }

    /// Get enforcement summary for a tenant
    pub fn get_tenant_summary(&self, tenant_id: &TenantId) -> Option<TenantSummary> {
        let usage = self.get_usage_stats(tenant_id)?;

        Some(TenantSummary {
            tenant_id: tenant_id.clone(),
            current_workflows: usage.current_workflows,
            workflow_limit: usage.workflow_limit,
            current_tasks: usage.current_tasks,
            task_limit: usage.task_limit,
            denial_rate: usage.denial_rate,
            is_healthy: usage.denial_rate < 0.1,
        })
    }
}

impl Default for TenantEnforcementIntegration {
    fn default() -> Self {
        Self::new(Arc::new(TenantResourceEnforcer::new()))
    }
}

/// Summary of tenant enforcement state
#[derive(Debug, Clone)]
pub struct TenantSummary {
    /// Tenant identifier
    pub tenant_id: TenantId,

    /// Current concurrent workflows
    pub current_workflows: u64,

    /// Workflow limit
    pub workflow_limit: Option<u64>,

    /// Current concurrent tasks
    pub current_tasks: u64,

    /// Task limit
    pub task_limit: Option<u64>,

    /// Request denial rate
    pub denial_rate: f64,

    /// Whether tenant is healthy
    pub is_healthy: bool,
}

impl TenantSummary {
    /// Calculate workflow utilization
    pub fn workflow_utilization(&self) -> Option<f64> {
        self.workflow_limit.map(|limit| {
            if limit == 0 {
                0.0
            } else {
                self.current_workflows as f64 / limit as f64
            }
        })
    }

    /// Calculate task utilization
    pub fn task_utilization(&self) -> Option<f64> {
        self.task_limit.map(|limit| {
            if limit == 0 {
                0.0
            } else {
                self.current_tasks as f64 / limit as f64
            }
        })
    }
}

/// Builder for TenantEnforcementIntegration
pub struct TenantEnforcementIntegrationBuilder {
    enforcer: Option<Arc<TenantResourceEnforcer>>,
    enabled: bool,
    metrics_enabled: bool,
    default_tenants: Vec<TenantConfig>,
}

impl TenantEnforcementIntegrationBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            enforcer: None,
            enabled: true,
            metrics_enabled: false,
            default_tenants: Vec::new(),
        }
    }

    /// Set the enforcer
    pub fn enforcer(mut self, enforcer: Arc<TenantResourceEnforcer>) -> Self {
        self.enforcer = Some(enforcer);
        self
    }

    /// Enable metrics collection
    pub fn with_metrics(mut self) -> Self {
        self.metrics_enabled = true;
        self
    }

    /// Set whether integration is enabled
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Add a default tenant to register on build
    pub fn add_tenant(mut self, config: TenantConfig) -> Self {
        self.default_tenants.push(config);
        self
    }

    /// Build the integration
    pub fn build(self) -> TenantEnforcementIntegration {
        let enforcer = self
            .enforcer
            .unwrap_or_else(|| Arc::new(TenantResourceEnforcer::new()));

        // Register default tenants
        for config in self.default_tenants {
            enforcer.register_tenant(config);
        }

        let mut integration = TenantEnforcementIntegration::new(enforcer);
        integration.set_enabled(self.enabled);
        integration.set_metrics_enabled(self.metrics_enabled);

        integration
    }
}

impl Default for TenantEnforcementIntegrationBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ================================================================================================
// TESTS
// ================================================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tenancy::config::TenantTier;

    fn create_test_integration() -> TenantEnforcementIntegration {
        let enforcer = Arc::new(TenantResourceEnforcer::new());
        enforcer.register_tenant(TenantConfig::starter("test-tenant", "Test Tenant"));
        enforcer.register_tenant(TenantConfig::enterprise("enterprise", "Enterprise"));
        TenantEnforcementIntegration::new(enforcer)
    }

    #[test]
    fn test_integration_creation() {
        let integration = TenantEnforcementIntegration::default();
        assert!(integration.is_enabled());
    }

    #[test]
    fn test_check_start_workflow_allowed() {
        let integration = create_test_integration();
        let tenant = TenantId::new("test-tenant");

        let result = integration.check_start_workflow(&tenant);
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_start_workflow_unknown_tenant() {
        let integration = create_test_integration();
        let tenant = TenantId::new("unknown");

        let result = integration.check_start_workflow(&tenant);
        assert!(result.is_err());
    }

    #[test]
    fn test_check_start_workflow_disabled() {
        let mut integration = create_test_integration();
        integration.set_enabled(false);

        let tenant = TenantId::new("unknown");
        let result = integration.check_start_workflow(&tenant);
        assert!(result.is_ok()); // Always allowed when disabled
    }

    #[test]
    fn test_release_workflow() {
        let integration = create_test_integration();
        let tenant = TenantId::new("test-tenant");

        // Start a workflow
        integration.check_start_workflow(&tenant).unwrap();

        // Release it
        integration.release_workflow(&tenant, true);

        // Should be able to start another
        assert!(integration.check_start_workflow(&tenant).is_ok());
    }

    #[test]
    fn test_check_execute_task() {
        let integration = create_test_integration();
        let tenant = TenantId::new("test-tenant");

        let result = integration.check_execute_task(&tenant);
        assert!(result.is_ok());
    }

    #[test]
    fn test_release_task() {
        let integration = create_test_integration();
        let tenant = TenantId::new("test-tenant");

        integration.check_execute_task(&tenant).unwrap();
        integration.release_task(&tenant, true);
    }

    #[test]
    fn test_check_payload_size() {
        let integration = create_test_integration();
        let tenant = TenantId::new("test-tenant");

        // Small payload should be allowed
        assert!(integration.check_payload_size(&tenant, 1000).is_ok());

        // Huge payload should be denied
        assert!(integration
            .check_payload_size(&tenant, 100 * 1024 * 1024)
            .is_err());
    }

    #[test]
    fn test_check_rate_limit() {
        let integration = create_test_integration();
        let tenant = TenantId::new("test-tenant");

        let result = integration.check_rate_limit(&tenant);
        assert!(result.is_ok());
    }

    #[test]
    fn test_get_usage_stats() {
        let integration = create_test_integration();
        let tenant = TenantId::new("test-tenant");

        // Start some workflows
        integration.check_start_workflow(&tenant).unwrap();

        let stats = integration.get_usage_stats(&tenant);
        assert!(stats.is_some());
        assert_eq!(stats.unwrap().current_workflows, 1);
    }

    #[test]
    fn test_is_tenant_healthy() {
        let integration = create_test_integration();
        let tenant = TenantId::new("test-tenant");

        // New tenant should be healthy
        assert!(integration.is_tenant_healthy(&tenant));
    }

    #[test]
    fn test_tenant_summary() {
        let integration = create_test_integration();
        let tenant = TenantId::new("test-tenant");

        // Start a workflow
        integration.check_start_workflow(&tenant).unwrap();

        let summary = integration.get_tenant_summary(&tenant);
        assert!(summary.is_some());

        let summary = summary.unwrap();
        assert_eq!(summary.current_workflows, 1);
        assert!(summary.is_healthy);
    }

    #[test]
    fn test_builder_default() {
        let integration = TenantEnforcementIntegrationBuilder::new().build();
        assert!(integration.is_enabled());
    }

    #[test]
    fn test_builder_disabled() {
        let integration = TenantEnforcementIntegrationBuilder::new()
            .enabled(false)
            .build();

        assert!(!integration.is_enabled());
    }

    #[test]
    fn test_builder_with_tenants() {
        let integration = TenantEnforcementIntegrationBuilder::new()
            .add_tenant(TenantConfig::starter("tenant-a", "Tenant A"))
            .add_tenant(TenantConfig::enterprise("tenant-b", "Tenant B"))
            .build();

        assert!(integration
            .check_start_workflow(&TenantId::new("tenant-a"))
            .is_ok());
        assert!(integration
            .check_start_workflow(&TenantId::new("tenant-b"))
            .is_ok());
    }

    #[test]
    fn test_summary_calculations() {
        let summary = TenantSummary {
            tenant_id: TenantId::new("test"),
            current_workflows: 50,
            workflow_limit: Some(100),
            current_tasks: 250,
            task_limit: Some(1000),
            denial_rate: 0.05,
            is_healthy: true,
        };

        assert!((summary.workflow_utilization().unwrap() - 0.5).abs() < 0.01);
        assert!((summary.task_utilization().unwrap() - 0.25).abs() < 0.01);
    }

    #[test]
    fn test_workflow_limit_enforcement() {
        let enforcer = Arc::new(TenantResourceEnforcer::new());
        enforcer.register_tenant(
            TenantConfig::new("limited", "Limited", TenantTier::Free).with_max_workflows(2),
        );

        let integration = TenantEnforcementIntegration::new(enforcer);
        let tenant = TenantId::new("limited");

        // Should allow first 2
        assert!(integration.check_start_workflow(&tenant).is_ok());
        assert!(integration.check_start_workflow(&tenant).is_ok());

        // 3rd should fail
        assert!(integration.check_start_workflow(&tenant).is_err());

        // Release one and try again
        integration.release_workflow(&tenant, true);
        assert!(integration.check_start_workflow(&tenant).is_ok());
    }

    #[test]
    fn test_register_tenant() {
        let integration = TenantEnforcementIntegration::default();

        integration.register_tenant(TenantConfig::starter("new-tenant", "New Tenant"));

        let tenant = TenantId::new("new-tenant");
        assert!(integration.check_start_workflow(&tenant).is_ok());
    }

    #[test]
    fn test_metrics_enabled() {
        let integration = TenantEnforcementIntegrationBuilder::new()
            .with_metrics()
            .build();

        assert!(integration.is_metrics_enabled());
    }
}
