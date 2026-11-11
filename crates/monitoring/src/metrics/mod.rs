//! Metric definitions, labels, and helpers for ORCHER monitoring.

pub mod async_writer;
pub mod domain;
pub mod events;
pub mod helpers;
pub mod labels;
pub mod lifecycle;
pub mod names;
pub mod orchestrator;
pub mod polling;
pub mod queue;
pub mod scheduler;
pub mod security;
pub mod sharding;
pub mod storage;
pub mod sync_match;
pub mod system;
pub mod task;
pub mod worker;

// Re-export commonly used items for convenience
pub use async_writer::names as async_writer_names;
pub use async_writer::{
    describe_async_writer_metrics, record_async_writer_dropped, record_async_writer_enqueue,
    record_async_writer_error, record_async_writer_flush, update_async_writer_buffer_depth,
    AsyncWriterStats,
};
pub use events::{
    record_event_persistence_latency, record_event_publish_latency, track_event_persisted,
    track_event_persisted_with_latency, track_event_persistence_error, track_event_published,
    track_event_published_with_latency,
};
pub use helpers::{
    create_metric_name, describe_all_metrics, init_metrics_registry, validate_all_metric_names,
    validate_metric_name,
};
pub use labels::{
    monitoring as monitoring_labels, operations, scheduler as scheduler_labels,
    security as security_labels, storage as storage_labels, system as system_labels,
    tasks as task_labels, workers, workflows as workflow_labels,
};
pub use names::{
    db_pool as db_pool_names, events as event_names, monitoring as monitoring_names,
    orchestrator as orchestrator_names, queue as queue_names, scheduler as scheduler_names,
    security as security_names, storage as storage_names, system as system_names,
    tasks as task_names, workers as worker_names, workflows as workflow_names,
};
pub use orchestrator::{
    track_coordinator_operation, track_task_cancelled, track_task_submitted,
    track_workflow_cancelled, track_workflow_paused, track_workflow_resumed, update_active_tasks,
    update_active_workflows,
};
pub use polling::names as polling_names;
pub use polling::{
    describe_polling_metrics, record_batch_claim, record_claim_latency, record_poll_latency,
    record_postgres_fallback, record_push_to_claim_latency, record_redis_pop_latency,
    record_redis_push_latency, record_redis_retry, track_poll_operation, update_poll_queue_depth,
    ClaimGuard, PollDurationGuard, PollOutcome, RedisPopGuard,
};
pub use queue::{
    record_queue_publish_latency, track_consumer_operation, track_dispatcher_operation,
    track_dlq_message, track_event_consumed, track_event_type, track_health_check,
    track_queue_consume_error, track_queue_message_consumed, track_queue_message_published,
    track_queue_publish_error, track_retry_attempt, track_task_dispatched, update_connection_pool,
    update_consumer_lag, update_queue_depth, update_queue_health,
};
pub use scheduler::{track_trigger_fired, update_next_fire_time, update_scheduler_queue_size};
pub use security::{track_auth_attempt, track_rate_limit_exceeded, track_threat_detected};
pub use sharding::names as sharding_names;
pub use sharding::{
    describe_sharding_metrics, record_claim_failed, record_claim_success, record_heartbeat_failed,
    record_heartbeat_success, record_non_owned_request, record_owned_request, record_redirect,
    record_registration, record_shard_gained, record_shard_lookup, record_shard_lost,
    update_owned_shards, update_ownership_rate, update_total_shards, ShardingStats,
};
pub use storage::{
    describe_db_pool_metrics, record_connection_acquisition, record_connection_timeout,
    track_query_duration, track_storage_operation, update_cache_hit_ratio,
    update_connection_metrics, update_connection_pool_size, update_db_health_status,
    update_db_pool_metrics, update_last_health_check, DbPoolMetrics, DbPoolMonitor,
};
pub use sync_match::names as sync_match_names;
pub use sync_match::{
    describe_sync_match_metrics, record_affinity_attempt, record_affinity_miss,
    record_affinity_success, record_dispatch_latency, record_fast_path_success, record_poll_served,
    record_state_cache_eviction, record_state_cache_hit, record_state_cache_miss,
    record_sync_match_attempt, record_sync_match_miss, record_sync_match_success,
    track_waiter_dispatch_failure, track_waiter_expired, track_waiter_registered,
    track_waiter_rejected, track_waiter_served, update_affinity_lease_count,
    update_state_cache_size, update_waiter_queue_depth, SyncMatchGuard, SyncMatchOutcome,
    SyncMatchStats, WaiterGuard,
};
#[cfg(not(feature = "system-metrics"))]
pub use system::{collect_cpu_metrics, collect_memory_metrics, start_system_metrics_collection};
#[cfg(feature = "system-metrics")]
pub use system::{
    collect_cpu_metrics, collect_memory_metrics, start_system_metrics_collection,
    SystemMetricsCollector,
};
pub use task::track_task_execution;
pub use worker::{calculate_worker_utilization, update_worker_counts, update_worker_pool_size};

// Domain metrics (per-tenant, per-shard, SLO, coordinator)
pub use domain::{
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
    // Shard metrics
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
    // Tenant metrics
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
