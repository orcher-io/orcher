//! Domain-level metrics for ORCHER
//!
//! Business-level observability including:
//! - Per-tenant metrics (workflows, tasks, quotas)
//! - Per-shard metrics (load distribution)
//! - SLO/SLI tracking (latency, availability)
//! - Coordinator operations

pub mod coordinator;
pub mod shard;
pub mod slo;
pub mod tenant;

// Re-export functions (but not `names` modules to avoid conflicts)
pub use coordinator::{
    coordinator_names, describe_coordinator_metrics, record_coordinator_cache_hit,
    record_coordinator_cache_miss, record_coordinator_invalidation_sent,
    record_coordinator_lock_acquired, record_coordinator_operation_duration,
    record_coordinator_outbox_created, record_coordinator_outbox_processed,
    record_coordinator_state_reconstruction, record_coordinator_state_update,
    record_coordinator_timer_fired, record_coordinator_timer_scheduled,
    track_coordinator_operation_domain, CoordinatorOperationGuard,
};
pub use shard::{
    describe_shard_metrics, record_shard_bytes_processed, record_shard_cache_hit,
    record_shard_cache_miss, record_shard_owned, record_shard_task_completed,
    record_shard_task_dispatched, record_shard_task_failed, record_shard_timer_fired,
    record_shard_timer_scheduled, record_shard_workflow_completed, record_shard_workflow_failed,
    record_shard_workflow_started, shard_names, update_owned_shard_count, update_total_shard_count,
};
pub use slo::{
    describe_slo_metrics, record_slo_failure, record_slo_success, record_slo_violation, slo_names,
    track_slo_operation, update_availability, update_error_budget_remaining, update_slo_status,
    SloConfig, SloLatencyGuard, SloStatus,
};
pub use tenant::{
    describe_tenant_metrics, record_tenant_active, record_tenant_api_request,
    record_tenant_bytes_transferred, record_tenant_cpu_time, record_tenant_quota_exceeded,
    record_tenant_rate_limited, record_tenant_task_executed, record_tenant_task_failed,
    record_tenant_workflow_completed, record_tenant_workflow_failed,
    record_tenant_workflow_started, tenant_names, update_active_tenant_count,
    update_tenant_bytes_stored, update_tenant_count,
};

/// Describe all domain metrics (call at startup)
pub fn describe_domain_metrics() {
    tenant::describe_tenant_metrics();
    shard::describe_shard_metrics();
    slo::describe_slo_metrics();
    coordinator::describe_coordinator_metrics();
}
