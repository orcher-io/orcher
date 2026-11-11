//! ORCHER Monitoring
//!
//! Observability and monitoring framework for ORCHER task orchestration platform.
//!
//! This crate provides ORCHER-specific instrumentation on top of industry-standard
//! tools like Prometheus, Grafana, and Jaeger. It focuses on monitoring tasks,
//! workflows, workers, and other ORCHER components without replacing existing
//! observability infrastructure.
//!
//! # Features
//!
//! - **Metrics**: ORCHER-specific metrics collection with Prometheus integration
//! - **Tracing**: Distributed tracing with OpenTelemetry support
//! - **Health Checks**: Component health monitoring with circuit breakers
//! - **Self-Monitoring**: Monitor the monitoring system itself
//! - **Common Types**: Re-exports from orcher-common for type consistency
//!
//! # Quick Start
//!
//! ```rust
//! use orcher_monitoring::prelude::*;
//!
//! // Create a monitoring configuration
//! #[cfg(feature = "integration")]
//! let config = orcher_monitoring::config::MonitoringConfig::default();
//! #[cfg(not(feature = "integration"))]
//! let config = orcher_monitoring::config::BasicMonitoringConfig::default();
//!
//! // Use common types
//! let task_id = TaskId::new_v4();
//! let health = HealthCheck::healthy("test_component");
//!
//! println!("Monitoring version: {}", orcher_monitoring::version());
//! ```

// Re-export industry-standard crates for convenience
pub use metrics as metrics_crate;
pub use tracing::{Level, Span};

// Re-export common types for consistency across ORCHER platform
pub use orcher_common::monitoring::{
    HealthCheck, HealthStatus, OperationRecord, OperationStatus, OperationTracker,
    PerformanceMetrics,
};
pub use orcher_common::types::identifiers::*;

// Core modules
pub mod config;
pub mod diagnostics;
pub mod error;
pub mod exporters;
pub mod health;
pub mod init;
pub mod integration;
pub mod logging;
pub mod metrics;
pub mod tracing;

// Feature-gated modules
#[cfg(feature = "test-utils")]
pub mod testing;

/// Convenience prelude for common imports
pub mod prelude {
    // Configuration types
    #[cfg(not(feature = "integration"))]
    pub use crate::config::BasicMonitoringConfig;
    #[cfg(feature = "integration")]
    pub use crate::config::MonitoringConfig;

    // Error handling
    pub use crate::error::{MonitoringError, Result};

    // Metrics and tracing
    pub use metrics::{counter, gauge, histogram};
    pub use tracing::{error, info, instrument, warn};

    // Common ORCHER types
    pub use orcher_common::types::identifiers::{NodeId, TaskId, WorkerId, WorkflowId};

    // Monitoring utilities from common
    pub use orcher_common::monitoring::{
        HealthCheck, HealthStatus, OperationTracker, PerformanceMetrics,
    };

    // Diagnostics utilities
    pub use crate::diagnostics::{ErrorReporter, SelfMonitoring};

    // Health checking utilities
    pub use crate::health::{
        AggregatedHealthStatus, AsyncHealthCheck, ExporterHealthCheck, HealthCheckCriticality,
        HealthCheckRegistry, HealthCheckResult, HealthEndpointConfig, HealthEndpoints,
        HealthRegistryConfig, HealthResponse, HealthSummary, MonitoringSystemHealthCheck,
    };

    // Metrics utilities
    pub use crate::metrics::{
        create_metric_name, describe_all_metrics, init_metrics_registry, validate_metric_name,
    };

    // Task metrics
    pub use crate::metrics::{track_task_execution, track_task_submitted};

    // Database pool monitoring (Task 2.3)
    pub use crate::metrics::{
        describe_db_pool_metrics, record_connection_acquisition, record_connection_timeout,
        track_query_duration, update_connection_metrics, update_db_health_status,
        update_db_pool_metrics, update_last_health_check, DbPoolMetrics, DbPoolMonitor,
    };

    // Polling performance metrics for latency tracking
    pub use crate::metrics::{
        describe_polling_metrics, record_batch_claim, record_claim_latency, record_poll_latency,
        record_postgres_fallback, record_push_to_claim_latency, record_redis_pop_latency,
        record_redis_push_latency, record_redis_retry, track_poll_operation,
        update_poll_queue_depth, ClaimGuard, PollDurationGuard, PollOutcome, RedisPopGuard,
    };

    // Sync-match and Task Router metrics for optimized dispatch
    pub use crate::metrics::{
        describe_sync_match_metrics, record_affinity_attempt, record_affinity_miss,
        record_affinity_success, record_dispatch_latency, record_fast_path_success,
        record_state_cache_eviction, record_state_cache_hit, record_state_cache_miss,
        record_sync_match_attempt, record_sync_match_miss, record_sync_match_success,
        track_waiter_dispatch_failure, track_waiter_expired, track_waiter_registered,
        track_waiter_rejected, track_waiter_served, update_affinity_lease_count,
        update_state_cache_size, update_waiter_queue_depth, SyncMatchGuard, SyncMatchOutcome,
        SyncMatchStats, WaiterGuard,
    };

    // Horizontal sharding metrics for distributed orchestrator scaling
    pub use crate::metrics::{
        describe_sharding_metrics, record_claim_failed, record_claim_success,
        record_heartbeat_failed, record_heartbeat_success, record_non_owned_request,
        record_owned_request, record_redirect, record_registration, record_shard_gained,
        record_shard_lookup, record_shard_lost, update_owned_shards, update_ownership_rate,
        update_total_shards, ShardingStats,
    };

    // Domain metrics (per-tenant, per-shard, SLO, coordinator)
    pub use crate::metrics::{
        coordinator_names,
        describe_domain_metrics,
        record_coordinator_cache_hit,
        record_coordinator_cache_miss,
        record_coordinator_invalidation_sent,
        record_coordinator_lock_acquired,
        record_coordinator_operation_duration,
        record_coordinator_outbox_created,
        record_coordinator_outbox_processed,
        record_coordinator_state_reconstruction,
        // Coordinator metrics
        record_coordinator_state_update,
        record_coordinator_timer_fired,
        record_coordinator_timer_scheduled,
        record_shard_bytes_processed,
        record_shard_cache_hit,
        record_shard_cache_miss,
        record_shard_owned,
        record_shard_task_completed,
        record_shard_task_dispatched,
        record_shard_task_failed,
        record_shard_timer_fired,
        record_shard_timer_scheduled,
        record_shard_workflow_completed,
        record_shard_workflow_failed,
        // Shard domain metrics
        record_shard_workflow_started,
        record_slo_failure,
        // SLO metrics
        record_slo_success,
        record_slo_violation,
        record_tenant_active,
        record_tenant_api_request,
        record_tenant_bytes_transferred,
        record_tenant_cpu_time,
        record_tenant_quota_exceeded,
        record_tenant_rate_limited,
        record_tenant_task_executed,
        record_tenant_task_failed,
        record_tenant_workflow_completed,
        record_tenant_workflow_failed,
        // Tenant domain metrics
        record_tenant_workflow_started,
        shard_names,
        slo_names,
        tenant_names,
        track_coordinator_operation_domain,
        track_slo_operation,
        update_active_tenant_count,
        update_availability,
        update_error_budget_remaining,
        update_owned_shard_count,
        update_slo_status,
        update_tenant_bytes_stored,
        update_tenant_count,
        update_total_shard_count,
        CoordinatorOperationGuard,
        SloConfig,
        SloLatencyGuard,
        SloStatus,
    };

    // Tracing utilities
    pub use crate::tracing::{
        create_storage_span, create_task_span, create_worker_span, create_workflow_span,
        current_span_id, current_trace_id,
    };

    // Exporters for external observability systems
    pub use crate::exporters::{
        Exporter, ExporterConfig, ExporterGroup, ExporterResult, JaegerExporter, MultiExporter,
        OtlpExporter, PrometheusExporter,
    };

    // Feature-gated tracing exports
    #[cfg(feature = "tracing-export")]
    pub use crate::tracing::{
        create_child_span, extract_context, inject_context, propagate_context, with_async_context,
    };

    // Logging utilities
    pub use crate::logging::{
        init_auto, init_from_env, init_logging, initialize_logging, try_init_logging, LogFormat,
        LoggingConfig,
    };
}

/// Returns the version of the monitoring crate
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
