//! Per-Tenant Metrics
//!
//! Provides metrics tracking for each tenant in the system using the `metrics` crate.

use metrics::{counter, describe_counter, describe_gauge, gauge};
use std::time::Duration;

/// Metric name constants for tenant metrics
pub mod tenant_names {
    pub const WORKFLOWS_STARTED: &str = "orcher_tenant_workflows_started_total";
    pub const WORKFLOWS_ACTIVE: &str = "orcher_tenant_workflows_active";
    pub const WORKFLOWS_COMPLETED: &str = "orcher_tenant_workflows_completed_total";
    pub const WORKFLOWS_FAILED: &str = "orcher_tenant_workflows_failed_total";
    pub const TASKS_EXECUTED: &str = "orcher_tenant_tasks_executed_total";
    pub const TASKS_FAILED: &str = "orcher_tenant_tasks_failed_total";
    pub const API_REQUESTS: &str = "orcher_tenant_api_requests_total";
    pub const RATE_LIMITED: &str = "orcher_tenant_rate_limited_total";
    pub const QUOTA_EXCEEDED: &str = "orcher_tenant_quota_exceeded_total";
    pub const BYTES_STORED: &str = "orcher_tenant_bytes_stored";
    pub const BYTES_TRANSFERRED: &str = "orcher_tenant_bytes_transferred_total";
    pub const CPU_TIME_SECONDS: &str = "orcher_tenant_cpu_time_seconds_total";
    pub const TENANTS_TOTAL: &str = "orcher_tenants_total";
    pub const TENANTS_ACTIVE: &str = "orcher_tenants_active";
}

/// Describe all tenant metrics (call at startup)
pub fn describe_tenant_metrics() {
    // Workflow metrics
    describe_counter!(
        tenant_names::WORKFLOWS_STARTED,
        "Total workflows started per tenant"
    );
    describe_gauge!(
        tenant_names::WORKFLOWS_ACTIVE,
        "Currently active workflows per tenant"
    );
    describe_counter!(
        tenant_names::WORKFLOWS_COMPLETED,
        "Completed workflows per tenant"
    );
    describe_counter!(
        tenant_names::WORKFLOWS_FAILED,
        "Failed workflows per tenant"
    );

    // Task metrics
    describe_counter!(tenant_names::TASKS_EXECUTED, "Tasks executed per tenant");
    describe_counter!(tenant_names::TASKS_FAILED, "Tasks failed per tenant");

    // API metrics
    describe_counter!(tenant_names::API_REQUESTS, "API requests per tenant");
    describe_counter!(
        tenant_names::RATE_LIMITED,
        "Rate limited requests per tenant"
    );
    describe_counter!(
        tenant_names::QUOTA_EXCEEDED,
        "Quota exceeded events per tenant"
    );

    // Resource usage metrics
    describe_gauge!(tenant_names::BYTES_STORED, "Bytes stored per tenant");
    describe_counter!(
        tenant_names::BYTES_TRANSFERRED,
        "Bytes transferred per tenant"
    );
    describe_counter!(
        tenant_names::CPU_TIME_SECONDS,
        "CPU time consumed per tenant in seconds"
    );

    // Tenant count metrics
    describe_gauge!(
        tenant_names::TENANTS_TOTAL,
        "Total number of registered tenants"
    );
    describe_gauge!(tenant_names::TENANTS_ACTIVE, "Number of active tenants");
}

// ============================================================================
// Workflow Recording Functions
// ============================================================================

/// Record a workflow started for a tenant
pub fn record_tenant_workflow_started(tenant_id: &str) {
    counter!(tenant_names::WORKFLOWS_STARTED, "tenant_id" => tenant_id.to_string()).increment(1);
    gauge!(tenant_names::WORKFLOWS_ACTIVE, "tenant_id" => tenant_id.to_string()).set(1.0);
}

/// Record a workflow completed for a tenant
pub fn record_tenant_workflow_completed(tenant_id: &str) {
    counter!(tenant_names::WORKFLOWS_COMPLETED, "tenant_id" => tenant_id.to_string()).increment(1);
    gauge!(tenant_names::WORKFLOWS_ACTIVE, "tenant_id" => tenant_id.to_string()).set(-1.0);
}

/// Record a workflow failed for a tenant
pub fn record_tenant_workflow_failed(tenant_id: &str) {
    counter!(tenant_names::WORKFLOWS_FAILED, "tenant_id" => tenant_id.to_string()).increment(1);
    gauge!(tenant_names::WORKFLOWS_ACTIVE, "tenant_id" => tenant_id.to_string()).set(-1.0);
}

// ============================================================================
// Task Recording Functions
// ============================================================================

/// Record a task executed for a tenant
pub fn record_tenant_task_executed(tenant_id: &str) {
    counter!(tenant_names::TASKS_EXECUTED, "tenant_id" => tenant_id.to_string()).increment(1);
}

/// Record a task failed for a tenant
pub fn record_tenant_task_failed(tenant_id: &str) {
    counter!(tenant_names::TASKS_FAILED, "tenant_id" => tenant_id.to_string()).increment(1);
}

// ============================================================================
// API Recording Functions
// ============================================================================

/// Record an API request for a tenant
pub fn record_tenant_api_request(tenant_id: &str, endpoint: &str) {
    counter!(
        tenant_names::API_REQUESTS,
        "tenant_id" => tenant_id.to_string(),
        "endpoint" => endpoint.to_string()
    )
    .increment(1);
}

/// Record a rate limited request for a tenant
pub fn record_tenant_rate_limited(tenant_id: &str) {
    counter!(tenant_names::RATE_LIMITED, "tenant_id" => tenant_id.to_string()).increment(1);
}

/// Record a quota exceeded event for a tenant
pub fn record_tenant_quota_exceeded(tenant_id: &str, quota_type: &str) {
    counter!(
        tenant_names::QUOTA_EXCEEDED,
        "tenant_id" => tenant_id.to_string(),
        "quota_type" => quota_type.to_string()
    )
    .increment(1);
}

// ============================================================================
// Resource Usage Recording Functions
// ============================================================================

/// Update bytes stored for a tenant
pub fn update_tenant_bytes_stored(tenant_id: &str, bytes: u64) {
    gauge!(tenant_names::BYTES_STORED, "tenant_id" => tenant_id.to_string()).set(bytes as f64);
}

/// Record bytes transferred for a tenant
pub fn record_tenant_bytes_transferred(tenant_id: &str, bytes: u64) {
    counter!(tenant_names::BYTES_TRANSFERRED, "tenant_id" => tenant_id.to_string())
        .increment(bytes);
}

/// Record CPU time consumed by a tenant
pub fn record_tenant_cpu_time(tenant_id: &str, duration: Duration) {
    counter!(
        tenant_names::CPU_TIME_SECONDS,
        "tenant_id" => tenant_id.to_string()
    )
    .increment(duration.as_secs());
}

// ============================================================================
// Tenant Count Functions
// ============================================================================

/// Update the total number of tenants
pub fn update_tenant_count(count: u64) {
    gauge!(tenant_names::TENANTS_TOTAL).set(count as f64);
}

/// Update the number of active tenants
pub fn update_active_tenant_count(count: u64) {
    gauge!(tenant_names::TENANTS_ACTIVE).set(count as f64);
}

/// Record a tenant becoming active
pub fn record_tenant_active(tenant_id: &str) {
    // Initialize the tenant's gauge to ensure it appears in metrics
    gauge!(tenant_names::WORKFLOWS_ACTIVE, "tenant_id" => tenant_id.to_string()).set(0.0);
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_describe_metrics_does_not_panic() {
        describe_tenant_metrics();
    }

    #[test]
    fn test_record_workflow_lifecycle() {
        let tenant_id = "test-tenant";
        record_tenant_workflow_started(tenant_id);
        record_tenant_workflow_completed(tenant_id);
    }

    #[test]
    fn test_record_task_lifecycle() {
        let tenant_id = "test-tenant";
        record_tenant_task_executed(tenant_id);
        record_tenant_task_failed(tenant_id);
    }

    #[test]
    fn test_record_api_operations() {
        let tenant_id = "test-tenant";
        record_tenant_api_request(tenant_id, "/workflows");
        record_tenant_rate_limited(tenant_id);
        record_tenant_quota_exceeded(tenant_id, "workflows");
    }

    #[test]
    fn test_record_resource_usage() {
        let tenant_id = "test-tenant";
        update_tenant_bytes_stored(tenant_id, 1024);
        record_tenant_bytes_transferred(tenant_id, 2048);
        record_tenant_cpu_time(tenant_id, Duration::from_secs(60));
    }

    #[test]
    fn test_tenant_counts() {
        update_tenant_count(100);
        update_active_tenant_count(50);
        record_tenant_active("new-tenant");
    }
}
