//! A migrated database holds none of the tables and views of the features
//! the engine never had.
//!
//! V006 created an alerting and notification system's tables, V007 a plugin
//! system's, V008 an integrations and incoming-webhooks feature's and V010 a
//! message queue's, with functions; nothing ever wrote, read or called any of
//! them. They stay in the history (applied migrations are checked by
//! checksum) and V080, V071, V079 and V081 drop what they made, so a database
//! the engine migrates, new or upgraded, carries none of it. V082 does the
//! same for views, tables and functions other early migrations made that
//! nothing came to use, and V083 for the event store the engine never wrote
//! to and the idempotency records nothing used.
//!
//! Requires PostgreSQL. Run with:
//!
//! ```bash
//! export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/orcher_test"
//! cargo test -p orcher-storage --test dropped_tables -- --ignored --test-threads=1
//! ```

use sqlx::PgPool;

/// Tables and views nothing uses: what V006, V007, V008 and V010 created, and
/// what other early migrations created and nothing came to use.
const DROPPED: [&str; 62] = [
    // V011, V012, V013: the event store and idempotency records
    "events",
    "event_snapshots",
    "snapshots",
    "snapshot_metadata",
    "execution_idempotency",
    // V001, V003, V004, V005, V012, V014, V016: unused views and tables
    "v_active_workflow_executions",
    "v_workflow_execution_summary",
    "v_failed_retryable_executions",
    "v_outbox_active_tasks",
    "v_timer_active_tasks",
    "v_dlq_summary",
    "actor_state_by_type",
    "actor_state_stats",
    "v_api_key_permissions",
    "system_metrics",
    "worker_heartbeats",
    "cron_validations",
    "api_key_permissions",
    "projection_snapshots",
    // V010
    "queue_config",
    "queue_messages",
    "queue_stats",
    "queue_consumers",
    // V006
    "notification_categories",
    "notification_channels",
    "user_notification_preferences",
    "user_notification_category_preferences",
    "notifications",
    "notification_deliveries",
    "notification_templates",
    "notification_digests",
    "system_alerts",
    "alert_rules",
    "alert_rule_evaluations",
    "v_user_notification_settings",
    "v_active_alerts",
    "v_pending_notifications",
    // V007
    "plugins",
    "plugin_configurations",
    "plugin_dependencies",
    "plugin_permissions",
    "plugin_executions",
    "plugin_health_checks",
    "plugin_events",
    "plugin_resource_usage",
    "plugin_schedules",
    "plugin_api_endpoints",
    "v_active_plugins",
    "v_plugin_health_summary",
    "v_plugin_execution_stats",
    "v_plugin_resource_current",
    // V008
    "integration_providers",
    "integrations",
    "integration_operations",
    "webhook_configurations",
    "webhook_requests",
    "integration_events",
    "integration_schedules",
    "v_active_integrations",
    "v_integration_health_summary",
    "v_webhook_request_summary",
    "v_integration_operation_stats",
];

/// Functions nothing calls, V010's queue functions among them.
const DROPPED_FUNCTIONS: [&str; 26] = [
    "update_snapshot_metadata",
    "cleanup_old_snapshots",
    "cleanup_snapshots_older_than",
    "can_parent_close",
    "cleanup_expired_actor_state",
    "get_workflow_execution_stats",
    "cleanup_expired_cron_validations",
    "cleanup_expired_idempotency",
    "cleanup_completed_queue_tasks",
    "execution_log_mark_completed",
    "execution_log_mark_failed",
    "get_pending_child_workflows",
    "get_queue_metrics",
    "move_outbox_to_dlq",
    "move_timer_to_dlq",
    "get_next_stream_position",
    "get_current_global_position",
    "extract_aggregate_id",
    "extract_category",
    "queue_publish",
    "queue_consume",
    "queue_ack",
    "queue_nack",
    "queue_depth",
    "queue_cleanup_expired",
    "update_queue_config_timestamp",
];

#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_migrated_database_holds_none_of_the_unused_features_tables() {
    let url = std::env::var("TEST_DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:postgres@localhost:5432/orcher_test".to_string()
    });
    let pool = PgPool::connect(&url)
        .await
        .expect("connect to the test database");
    orcher_storage::setup::run_migrations(&pool)
        .await
        .expect("apply migrations");

    let mut left = Vec::new();
    for name in DROPPED {
        let found: Option<String> = sqlx::query_scalar("SELECT to_regclass($1)::text")
            .bind(name)
            .fetch_one(&pool)
            .await
            .expect("look the relation up");
        if found.is_some() {
            left.push(name);
        }
    }
    for name in DROPPED_FUNCTIONS {
        let found: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace \
             WHERE n.nspname = current_schema() AND p.proname = $1)",
        )
        .bind(name)
        .fetch_one(&pool)
        .await
        .expect("look the function up");
        if found {
            left.push(name);
        }
    }
    assert!(left.is_empty(), "still in the migrated database: {left:?}");
}
