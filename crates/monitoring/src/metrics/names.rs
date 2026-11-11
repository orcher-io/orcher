//! Metric name constants following Prometheus naming conventions.

pub mod actors {
    pub const OPERATIONS_TOTAL: &str = "orcher_actor_operations_total";
    pub const OPERATIONS_FAILED_TOTAL: &str = "orcher_actor_operations_failed_total";
    pub const OPERATIONS_EXECUTING: &str = "orcher_actor_operations_executing";
    pub const OPERATION_DURATION_SECONDS: &str = "orcher_actor_operation_duration_seconds";
}

pub mod tasks {
    pub const SUBMITTED_TOTAL: &str = "orcher_tasks_submitted_total";
    pub const RUNNING: &str = "orcher_tasks_running";
    pub const COMPLETED_TOTAL: &str = "orcher_tasks_completed_total";
    pub const FAILED_TOTAL: &str = "orcher_tasks_failed_total";
    pub const DURATION_SECONDS: &str = "orcher_task_duration_seconds";
    pub const QUEUE_SIZE: &str = "orcher_task_queue_size";
}

pub mod workflows {
    pub const EXECUTING: &str = "orcher_workflows_executing";
    pub const COMPLETED_TOTAL: &str = "orcher_workflows_completed_total";
    pub const FAILED_TOTAL: &str = "orcher_workflows_failed_total";
    pub const DURATION_SECONDS: &str = "orcher_workflow_duration_seconds";
    pub const STEP_DURATION_SECONDS: &str = "orcher_workflow_step_duration_seconds";
    /// Workflow completions acknowledged without being applied, because the
    /// activation they answer had been handed out again before they arrived.
    pub const STALE_COMPLETIONS_TOTAL: &str = "orcher_stale_completions_total";
}

pub mod workers {
    pub const POOL_SIZE: &str = "orcher_worker_pool_size";
    pub const ACTIVE_COUNT: &str = "orcher_worker_active_count";
    pub const IDLE_COUNT: &str = "orcher_worker_idle_count";
    pub const UTILIZATION: &str = "orcher_worker_utilization";
}

pub mod scheduler {
    pub const QUEUE_SIZE: &str = "orcher_scheduler_queue_size";
    pub const TRIGGERS_FIRED_TOTAL: &str = "orcher_scheduler_triggers_fired_total";
    /// Seconds since epoch.
    pub const NEXT_FIRE_TIME: &str = "orcher_scheduler_next_fire_time";
}

pub mod storage {
    pub const OPERATIONS_TOTAL: &str = "orcher_storage_operations_total";
    pub const OPERATION_DURATION_SECONDS: &str = "orcher_storage_operation_duration_seconds";
    pub const CACHE_HIT_RATIO: &str = "orcher_storage_cache_hit_ratio";
    pub const CONNECTION_POOL_SIZE: &str = "orcher_storage_connection_pool_size";
}

pub mod journal_cache {
    pub const CACHE_HITS_TOTAL: &str = "orcher_journal_cache_hits_total";
    pub const CACHE_MISSES_TOTAL: &str = "orcher_journal_cache_misses_total";
    /// Cache hit but needed DB load for new entries.
    pub const INCREMENTAL_LOADS_TOTAL: &str = "orcher_journal_cache_incremental_loads_total";
    pub const INCREMENTAL_ENTRIES_LOADED: &str = "orcher_journal_cache_incremental_entries_loaded";
    pub const HIT_RATIO: &str = "orcher_journal_cache_hit_ratio";
    pub const CACHED_JOURNALS: &str = "orcher_journal_cache_size";
    pub const DB_READS_AVOIDED: &str = "orcher_journal_cache_db_reads_avoided";
}

pub mod db_pool {
    pub const CONNECTIONS_ACTIVE: &str = "orcher_db_connections_active";
    pub const CONNECTIONS_IDLE: &str = "orcher_db_connections_idle";
    pub const CONNECTIONS_MAX: &str = "orcher_db_connections_max";
    pub const CONNECTIONS_TOTAL: &str = "orcher_db_connections_total";
    /// active / max
    pub const UTILIZATION_RATIO: &str = "orcher_db_pool_utilization_ratio";
    pub const ACQUISITIONS_TOTAL: &str = "orcher_db_pool_acquisitions_total";
    pub const ACQUISITION_TIMEOUTS_TOTAL: &str = "orcher_db_pool_acquisition_timeouts_total";
    pub const ACQUISITION_DURATION_SECONDS: &str = "orcher_db_pool_acquisition_duration_seconds";
    pub const QUERY_DURATION_SECONDS: &str = "orcher_db_query_duration_seconds";
    pub const QUERIES_TOTAL: &str = "orcher_db_queries_total";
    pub const QUERY_ERRORS_TOTAL: &str = "orcher_db_query_errors_total";
    pub const LAST_HEALTH_CHECK_SECONDS: &str = "orcher_db_last_health_check_seconds";
    /// 1 = healthy, 0 = unhealthy
    pub const HEALTH_STATUS: &str = "orcher_db_health_status";
}

pub mod security {
    pub const AUTH_ATTEMPTS_TOTAL: &str = "orcher_security_auth_attempts_total";
    pub const AUTH_FAILURES_TOTAL: &str = "orcher_security_auth_failures_total";
    pub const RATE_LIMIT_EXCEEDED_TOTAL: &str = "orcher_security_rate_limit_exceeded_total";
    pub const THREATS_DETECTED_TOTAL: &str = "orcher_security_threats_detected_total";
}

pub mod system {
    pub const CPU_USAGE: &str = "orcher_system_cpu_usage";
    pub const MEMORY_USAGE: &str = "orcher_system_memory_usage";
    pub const DISK_USAGE: &str = "orcher_system_disk_usage";
    pub const NETWORK_IO_BYTES: &str = "orcher_system_network_io_bytes";
}

pub mod orchestrator {
    pub const TASKS_SUBMITTED_TOTAL: &str = "orcher_orchestrator_tasks_submitted_total";
    pub const TASKS_CANCELLED_TOTAL: &str = "orcher_orchestrator_tasks_cancelled_total";
    pub const WORKFLOWS_PAUSED_TOTAL: &str = "orcher_orchestrator_workflows_paused_total";
    pub const WORKFLOWS_RESUMED_TOTAL: &str = "orcher_orchestrator_workflows_resumed_total";
    pub const WORKFLOWS_CANCELLED_TOTAL: &str = "orcher_orchestrator_workflows_cancelled_total";
    pub const ACTIVE_WORKFLOWS: &str = "orcher_orchestrator_active_workflows";
    pub const ACTIVE_TASKS: &str = "orcher_orchestrator_active_tasks";
    pub const COORDINATOR_OPERATIONS_TOTAL: &str =
        "orcher_orchestrator_coordinator_operations_total";
}

pub mod events {
    pub const PUBLISHED_TOTAL: &str = "orcher_events_published_total";
    pub const PERSISTED_TOTAL: &str = "orcher_events_persisted_total";
    pub const PERSISTENCE_ERRORS_TOTAL: &str = "orcher_events_persistence_errors_total";
    pub const PUBLISH_LATENCY_SECONDS: &str = "orcher_events_publish_latency_seconds";
    pub const PERSISTENCE_LATENCY_SECONDS: &str = "orcher_events_persistence_latency_seconds";
}

pub mod queue {
    pub const DEPTH: &str = "orcher_queue_depth";
    pub const MESSAGES_PUBLISHED_TOTAL: &str = "orcher_queue_messages_published_total";
    pub const MESSAGES_CONSUMED_TOTAL: &str = "orcher_queue_messages_consumed_total";
    pub const PUBLISH_ERRORS_TOTAL: &str = "orcher_queue_publish_errors_total";
    pub const CONSUME_ERRORS_TOTAL: &str = "orcher_queue_consume_errors_total";
    pub const PUBLISH_LATENCY_SECONDS: &str = "orcher_queue_publish_latency_seconds";
}

pub mod monitoring {
    pub const METRICS_COLLECTED_TOTAL: &str = "orcher_monitoring_metrics_collected_total";
    pub const EXPORTER_ERRORS_TOTAL: &str = "orcher_monitoring_exporter_errors_total";
    pub const HEALTH_CHECK_DURATION_SECONDS: &str =
        "orcher_monitoring_health_check_duration_seconds";
    pub const SPANS_CREATED_TOTAL: &str = "orcher_monitoring_spans_created_total";
    pub const CONFIG_RELOAD_SUCCESS_RATE: &str = "orcher_monitoring_config_reload_success_rate";
    /// 0=closed, 1=open, 2=half-open
    pub const CIRCUIT_BREAKER_STATE: &str = "orcher_monitoring_circuit_breaker_state";
    /// 0=unhealthy, 1=degraded, 2=healthy
    pub const COMPONENT_HEALTH_STATUS: &str = "orcher_monitoring_component_health_status";
    pub const UPTIME_SECONDS: &str = "orcher_monitoring_uptime_seconds";
    pub const ALERTS_FIRED_TOTAL: &str = "orcher_monitoring_alerts_fired_total";
}

pub mod dlq {
    pub const TASKS_MOVED_TO_DLQ_TOTAL: &str = "orcher_dlq_tasks_moved_total";
    pub const TASKS_REPLAYED_TOTAL: &str = "orcher_dlq_tasks_replayed_total";
    pub const REPLAY_FAILURES_TOTAL: &str = "orcher_dlq_replay_failures_total";
    pub const TASKS_PURGED_TOTAL: &str = "orcher_dlq_tasks_purged_total";
    pub const CURRENT_DEPTH: &str = "orcher_dlq_current_depth";
    pub const QUERIES_TOTAL: &str = "orcher_dlq_queries_total";
    /// 0.0 to 1.0
    pub const REPLAY_SUCCESS_RATE: &str = "orcher_dlq_replay_success_rate";
    /// moved - replayed - purged
    pub const NET_GROWTH: &str = "orcher_dlq_net_growth";
    pub const DEPTH_BY_PARTITION: &str = "orcher_dlq_depth_by_partition";
    pub const ENTRIES_BY_FAILURE_TYPE: &str = "orcher_dlq_entries_by_failure_type";
}

pub mod polling {
    pub const POLL_DURATION_SECONDS: &str = "orcher_poll_duration_seconds";
    pub const POLL_TOTAL: &str = "orcher_poll_total";
    pub const POLL_SUCCESS_TOTAL: &str = "orcher_poll_success_total";
    pub const POLL_TIMEOUT_TOTAL: &str = "orcher_poll_timeout_total";
    pub const POLL_ERROR_TOTAL: &str = "orcher_poll_error_total";
    pub const REDIS_PUSH_DURATION_SECONDS: &str = "orcher_redis_push_duration_seconds";
    pub const REDIS_POP_DURATION_SECONDS: &str = "orcher_redis_pop_duration_seconds";
    pub const CLAIM_DURATION_SECONDS: &str = "orcher_claim_duration_seconds";
    pub const POLL_QUEUE_DEPTH: &str = "orcher_poll_queue_depth";
    /// Time from LPUSH to BRPOP return.
    pub const PUSH_TO_CLAIM_LATENCY_SECONDS: &str = "orcher_push_to_claim_latency_seconds";
    pub const REDIS_RETRY_TOTAL: &str = "orcher_redis_retry_total";
    pub const POSTGRES_FALLBACK_TOTAL: &str = "orcher_postgres_fallback_total";
    pub const BATCH_CLAIM_SIZE: &str = "orcher_batch_claim_size";
    pub const BATCH_CLAIM_SUCCESS_RATIO: &str = "orcher_batch_claim_success_ratio";
}

pub mod retry {
    pub const TOTAL_RETRIES: &str = "orcher_retry_total_retries";
    pub const RETRIES_BY_ATTEMPT: &str = "orcher_retry_by_attempt";
    pub const RETRY_SUCCESSES_TOTAL: &str = "orcher_retry_successes_total";
    /// Retries exhausted, moved to DLQ.
    pub const RETRY_EXHAUSTED_TOTAL: &str = "orcher_retry_exhausted_total";
    /// 0.0 to 1.0
    pub const SUCCESS_RATE: &str = "orcher_retry_success_rate";
    pub const AVERAGE_PER_TASK: &str = "orcher_retry_average_per_task";
    pub const BACKOFF_DURATION_SECONDS: &str = "orcher_retry_backoff_duration_seconds";
}
