//! Database setup and migration utilities
//!
//! This module provides utilities for database initialization and migration management
//! with different behaviors for development and production environments.
//!
//! # Production Safety
//!
//! - Database creation is disabled by default in production
//! - Auto-migration can be controlled via configuration
//! - Includes health checks for migration status
//! - Supports dry-run mode for verification
//!
//! # Examples
//!
//! ## Development Setup (Auto-create and migrate)
//!
//! ```rust,no_run
//! use orcher_storage::setup::{setup_database, SetupConfig};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = SetupConfig::development("postgresql://orcher:orcher@localhost:5432/orcher");
//!     let backend = setup_database(config).await?;
//!     Ok(())
//! }
//! ```
//!
//! ## Production Setup (Connect only, fail if not migrated)
//!
//! ```rust,no_run
//! use orcher_storage::setup::{setup_database, SetupConfig};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = SetupConfig::production("postgresql://orcher:orcher@prod:5432/orcher");
//!     let backend = setup_database(config).await?;
//!     Ok(())
//! }
//! ```
//!
//! ## Separate Migration Tool
//!
//! ```rust,no_run
//! use orcher_storage::setup::run_migrations_with_config;
//! use orcher_storage::backends::postgres::PostgresConfig;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let pg_config = PostgresConfig::default();
//!     run_migrations_with_config(&pg_config).await?;
//!     Ok(())
//! }
//! ```

use crate::backends::postgres::{PostgresBackend, PostgresConfig};
use crate::migrations::sql::postgres::{
    V001_INITIAL_SCHEMA, V002_ORGANIZATION_TABLES, V003_WORKFLOW_TABLES, V004_SCHEDULING_TABLES,
    V005_AUTH_TABLES, V006_MONITORING_TABLES, V007_PLUGIN_TABLES, V008_INTEGRATION_TABLES,
    V009_CLUSTER_TABLES, V010_QUEUE_TABLES, V011_EVENT_STORE, V012_SNAPSHOTS, V013_EXECUTION_LOG,
    V014_COORDINATION_QUEUES, V015_CHILD_WORKFLOW_SUPPORT, V016_ACTOR_STATE, V017_TASK_EXECUTIONS,
    V018_WORKERS_TABLE, V019_NAMESPACES, V020_EAGER_TASK_CLAIM,
    V021_ADD_CANCELLED_EXECUTION_LOG_STATUS, V022_WORKFLOW_RETRY_POLICY, V023_WORKFLOW_TYPE_COLUMN,
    V024_SCHEDULE_WORKFLOW_LINK, V025_BATCH_OPERATIONS, V026_RESTARTED_FRESH_STATUS,
    V027_TIMER_QUEUE_SHARD_ID, V028_CLUSTER_MEMBERSHIP, V029_DROP_UNUSED_WFEXEC_INDEXES,
    V030_DROP_UNUSED_WFEXEC_GIN_INDEXES, V031_MATCHING_PARTITIONS, V032_CONCURRENCY_SAFE_EVENT_ID,
    V033_RESET_STATUS, V034_BIND_EXECUTION_VERSION, V035_TASK_QUEUE_CURRENT_RELEASE,
    V036_OUTBOX_INDEX_DIET, V037_STEP_COUNTS_OVERLAP, V038_SCHEDULE_NAME_PER_ORGANIZATION,
    V039_RETRY_DISPATCH_LEASE, V040_BATCH_OPERATIONS_ORGANIZATION,
    V041_TASK_QUEUE_RELEASE_PER_ORGANIZATION, V042_TASK_TOKEN_INDEX,
    V043_API_KEY_ORGANIZATION_DELEGATION, V044_CHILD_WORKFLOW_TRACKING,
    V045_WORKFLOW_RETRIES_SETTLED, V046_CHILD_WORKFLOW_INDEXES, V047_RETRY_INDEXES,
    V048_ACTIVATION_FENCE, V049_TASK_SCHEDULE_ONCE, V050_UNDISPATCHED_TASK_SWEEP,
    V051_CLAIMED_ON_NODE, V052_CLAIMED_ON_NODE_INDEX, V053_WORKER_SHUTDOWNS,
    V054_MATCHING_PARTITION_ORGANIZATION, V055_STAMP_TASK_ORGANIZATION,
    V056_WORKFLOW_CLAIM_BY_ORGANIZATION, V057_OUTBOX_PENDING_BY_ORGANIZATION,
    V058_AUTO_HEARTBEAT_TIMEOUT, V059_WORKFLOW_ID_HOLDS, V060_WORKER_INSTANCES,
    V061_WORKER_INSTANCE_CLAIMS_INDEX, V062_WORKFLOW_CANCEL_REQUESTS,
    V063_WORKFLOW_CANCEL_DEADLINE_INDEX, V064_ENDED_RUNS_CARRY_COMPLETED_AT,
    V065_WORKFLOW_RUN_AND_TASK_TIMEOUTS, V066_RUN_LOOKUP_BY_WORKFLOW_ID,
    V067_RUN_TIMEOUT_RETRY_AND_EXECUTION_DEADLINE, V068_OUTBOX_PENDING_DISPATCH_BY_QUEUE,
    V069_START_DELAY_ANNOTATIONS_AND_REQUEST_ID, V070_STEP_ID_SCHEME_AND_HISTORY_LINEAGE,
    V071_DROP_PLUGIN_TABLES, V072_DROP_SUPERSEDED_CLUSTER_TABLES,
    V073_API_KEY_SCOPES_AND_REVOCATION, V074_API_KEY_LOOKUP_INDEXES, V075_DROP_EVENT_GATES,
    V076_TIMERS_MAY_BE_RECORDED_AFTER_THEY_FALL_DUE, V077_JOURNAL_TASK_COMPLETED_LOOKUP,
    V078_STEP_RESULTS_IN_THE_JOURNAL, V079_DROP_INTEGRATION_TABLES,
    V080_DROP_ALERT_AND_NOTIFICATION_TABLES, V081_DROP_QUEUE_TABLES,
    V082_DROP_UNUSED_VIEWS_TABLES_AND_FUNCTIONS, V083_DROP_EVENT_STORE_AND_IDEMPOTENCY_TABLES,
    V084_DROP_WORKFLOW_FAILED_NOTIFY, V085_ORGANIZATION_IDS_ARE_OPAQUE,
    V086_JOURNAL_HOLDER_PREFERENCE, V087_ACTIVATION_JOURNAL_POSITION,
    V088_TASK_SCHEDULED_THROUGH_ENTRY,
};
use crate::OrcherResult;
use sqlx::migrate::MigrateDatabase;
use sqlx::Executor;
use tracing::{debug, error, info, warn};

/// Configuration for database setup behavior
#[derive(Debug, Clone)]
pub struct SetupConfig {
    /// Database connection URL
    pub database_url: String,

    /// Whether to automatically create the database if it doesn't exist
    /// **IMPORTANT**: Should be `false` in production
    pub auto_create_database: bool,

    /// Whether to automatically run migrations on startup
    /// **IMPORTANT**: Consider setting to `false` in production and running migrations separately
    pub auto_migrate: bool,

    /// Whether to fail if there are pending migrations
    /// Recommended: `true` in production to ensure database is up to date
    pub fail_on_pending_migrations: bool,

    /// Whether this is a dry run (check only, don't apply)
    pub dry_run: bool,

    /// PostgreSQL connection configuration
    pub postgres_config: PostgresConfig,
}

impl SetupConfig {
    /// Create a development-friendly configuration
    ///
    /// - Auto-creates database
    /// - Auto-runs migrations
    /// - Does not fail on pending migrations (applies them)
    pub fn development(database_url: impl Into<String>) -> Self {
        let database_url = database_url.into();
        Self {
            postgres_config: PostgresConfig {
                url: database_url.clone(),
                ..Default::default()
            },
            database_url,
            auto_create_database: true,
            auto_migrate: true,
            fail_on_pending_migrations: false,
            dry_run: false,
        }
    }

    /// Create a production-safe configuration
    ///
    /// - Does NOT auto-create database
    /// - Does NOT auto-run migrations
    /// - FAILS if there are pending migrations
    ///
    /// This ensures that:
    /// 1. Database exists (created by infrastructure)
    /// 2. Migrations are up to date (run by deployment pipeline)
    /// 3. Application won't start with an out-of-date schema
    pub fn production(database_url: impl Into<String>) -> Self {
        let database_url = database_url.into();
        Self {
            postgres_config: PostgresConfig {
                url: database_url.clone(),
                ..Default::default()
            },
            database_url,
            auto_create_database: false,
            auto_migrate: false,
            fail_on_pending_migrations: true,
            dry_run: false,
        }
    }

    /// Create a configuration for migration tool
    ///
    /// - Does NOT auto-create database
    /// - DOES run migrations
    /// - Does not fail on pending migrations (applies them)
    pub fn migration_tool(database_url: impl Into<String>) -> Self {
        let database_url = database_url.into();
        Self {
            postgres_config: PostgresConfig {
                url: database_url.clone(),
                ..Default::default()
            },
            database_url,
            auto_create_database: false,
            auto_migrate: true,
            fail_on_pending_migrations: false,
            dry_run: false,
        }
    }

    /// Set dry run mode (check migrations without applying)
    pub fn with_dry_run(mut self, dry_run: bool) -> Self {
        self.dry_run = dry_run;
        self
    }

    /// Set custom PostgreSQL configuration
    pub fn with_postgres_config(mut self, config: PostgresConfig) -> Self {
        self.postgres_config = config;
        self
    }
}

/// Result of migration check
#[derive(Debug, Clone)]
pub struct MigrationStatus {
    /// Whether all migrations are applied
    pub up_to_date: bool,

    /// Number of pending migrations
    pub pending_count: usize,

    /// Latest applied migration version
    pub current_version: Option<i64>,

    /// List of pending migrations (if any)
    pub pending_migrations: Vec<String>,
}

impl MigrationStatus {
    /// Check if migrations are up to date
    pub fn is_up_to_date(&self) -> bool {
        self.up_to_date
    }

    /// Check if there are pending migrations
    pub fn has_pending(&self) -> bool {
        self.pending_count > 0
    }
}

/// Setup database with the given configuration
///
/// This is the main entry point for database initialization.
/// The behavior depends on the `SetupConfig` provided.
///
/// # Errors
///
/// Returns an error if:
/// - Database doesn't exist and `auto_create_database` is false
/// - There are pending migrations and `fail_on_pending_migrations` is true
/// - Connection or migration fails
///
/// # Examples
///
/// ```rust,no_run
/// use orcher_storage::setup::{setup_database, SetupConfig};
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     // Development
///     let config = SetupConfig::development("postgresql://localhost/orcher");
///     let backend = setup_database(config).await?;
///
///     // Production
///     let config = SetupConfig::production("postgresql://prod/orcher");
///     let backend = setup_database(config).await?;
///
///     Ok(())
/// }
/// ```
pub async fn setup_database(config: SetupConfig) -> OrcherResult<PostgresBackend> {
    info!(
        "Setting up database: {}",
        redact_connection_url(&config.database_url)
    );
    debug!(
        "Setup config: auto_create={}, auto_migrate={}, fail_on_pending={}, dry_run={}",
        config.auto_create_database,
        config.auto_migrate,
        config.fail_on_pending_migrations,
        config.dry_run
    );

    // Check if database exists
    let db_exists = sqlx::Postgres::database_exists(&config.database_url)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to check database existence: {}",
                e
            ))
        })?;

    if !db_exists {
        if config.auto_create_database {
            warn!("Database doesn't exist, creating it...");
            warn!("Auto-creating database - this should NOT happen in production!");

            sqlx::Postgres::create_database(&config.database_url)
                .await
                .map_err(|e| {
                    orcher_common::error::OrcherError::internal(format!(
                        "Failed to create database: {}",
                        e
                    ))
                })?;

            info!("Database created successfully");
        } else {
            error!("Database doesn't exist and auto_create_database is disabled");
            return Err(orcher_common::error::OrcherError::internal(
                "Database does not exist. Please create it manually or enable auto_create_database for development.",
            ));
        }
    } else {
        debug!("Database already exists");
    }

    // Create connection pool
    let backend = PostgresBackend::new(config.postgres_config).await?;

    // Handle migrations based on configuration
    if config.dry_run {
        info!("Dry run mode: checking migration status...");
        let status = check_migration_status(backend.pool()).await?;
        print_migration_status(&status);

        if !status.up_to_date {
            warn!("Database has pending migrations (dry run - not applied)");
        }
    } else if config.auto_migrate {
        info!("Running database migrations...");
        run_migrations(backend.pool()).await?;
        info!("Migrations completed successfully");
    } else if config.fail_on_pending_migrations {
        info!("Checking migration status...");
        let status = check_migration_status(backend.pool()).await?;

        if !status.up_to_date {
            error!("Database has pending migrations!");
            print_migration_status(&status);
            return Err(orcher_common::error::OrcherError::internal(format!(
                "Database has {} pending migration(s). Run migrations before starting the application.",
                status.pending_count
            )));
        }

        info!("Database migrations are up to date");
    } else {
        debug!("Skipping migration check (auto_migrate=false, fail_on_pending_migrations=false)");
    }

    Ok(backend)
}

/// Migration definition
/// One migration the engine applies, in version order.
#[derive(Debug, Clone, Copy)]
pub struct MigrationDef {
    /// Its version: the order it is applied in.
    pub version: i64,
    /// What it is recorded as, beside its version.
    pub description: &'static str,
    /// Its SQL.
    pub sql: &'static str,
}

/// Every migration the engine applies, in version order: the one list the
/// runner and the `orcher-migrate` tool both read.
pub fn migrations() -> Vec<MigrationDef> {
    get_migrations()
}

/// Get all available migrations
fn get_migrations() -> Vec<MigrationDef> {
    vec![
        MigrationDef {
            version: 1,
            description: "initial_schema",
            sql: V001_INITIAL_SCHEMA,
        },
        MigrationDef {
            version: 2,
            description: "organization_tables",
            sql: V002_ORGANIZATION_TABLES,
        },
        MigrationDef {
            version: 3,
            description: "workflow_tables",
            sql: V003_WORKFLOW_TABLES,
        },
        MigrationDef {
            version: 4,
            description: "scheduling_tables",
            sql: V004_SCHEDULING_TABLES,
        },
        MigrationDef {
            version: 5,
            description: "auth_tables",
            sql: V005_AUTH_TABLES,
        },
        MigrationDef {
            version: 6,
            description: "monitoring_tables",
            sql: V006_MONITORING_TABLES,
        },
        MigrationDef {
            version: 7,
            description: "plugin_tables",
            sql: V007_PLUGIN_TABLES,
        },
        MigrationDef {
            version: 8,
            description: "integration_tables",
            sql: V008_INTEGRATION_TABLES,
        },
        MigrationDef {
            version: 9,
            description: "cluster_tables",
            sql: V009_CLUSTER_TABLES,
        },
        MigrationDef {
            version: 10,
            description: "queue_tables",
            sql: V010_QUEUE_TABLES,
        },
        MigrationDef {
            version: 11,
            description: "event_store",
            sql: V011_EVENT_STORE,
        },
        MigrationDef {
            version: 12,
            description: "snapshots",
            sql: V012_SNAPSHOTS,
        },
        MigrationDef {
            version: 13,
            description: "execution_log",
            sql: V013_EXECUTION_LOG,
        },
        MigrationDef {
            version: 14,
            description: "coordination_queues",
            sql: V014_COORDINATION_QUEUES,
        },
        MigrationDef {
            version: 15,
            description: "child_workflow_support",
            sql: V015_CHILD_WORKFLOW_SUPPORT,
        },
        MigrationDef {
            version: 16,
            description: "actor_state",
            sql: V016_ACTOR_STATE,
        },
        MigrationDef {
            version: 17,
            description: "task_executions",
            sql: V017_TASK_EXECUTIONS,
        },
        MigrationDef {
            version: 18,
            description: "workers_table",
            sql: V018_WORKERS_TABLE,
        },
        MigrationDef {
            version: 19,
            description: "namespaces",
            sql: V019_NAMESPACES,
        },
        MigrationDef {
            version: 20,
            description: "eager_task_claim",
            sql: V020_EAGER_TASK_CLAIM,
        },
        MigrationDef {
            version: 21,
            description: "add_cancelled_execution_log_status",
            sql: V021_ADD_CANCELLED_EXECUTION_LOG_STATUS,
        },
        MigrationDef {
            version: 22,
            description: "workflow_retry_policy",
            sql: V022_WORKFLOW_RETRY_POLICY,
        },
        MigrationDef {
            version: 23,
            description: "workflow_type_column",
            sql: V023_WORKFLOW_TYPE_COLUMN,
        },
        MigrationDef {
            version: 24,
            description: "schedule_workflow_link",
            sql: V024_SCHEDULE_WORKFLOW_LINK,
        },
        MigrationDef {
            version: 25,
            description: "batch_operations",
            sql: V025_BATCH_OPERATIONS,
        },
        MigrationDef {
            version: 26,
            description: "restarted_fresh_status",
            sql: V026_RESTARTED_FRESH_STATUS,
        },
        MigrationDef {
            version: 27,
            description: "timer_queue_shard_id",
            sql: V027_TIMER_QUEUE_SHARD_ID,
        },
        MigrationDef {
            version: 28,
            description: "cluster_membership",
            sql: V028_CLUSTER_MEMBERSHIP,
        },
        MigrationDef {
            version: 29,
            description: "drop_unused_wfexec_indexes",
            sql: V029_DROP_UNUSED_WFEXEC_INDEXES,
        },
        MigrationDef {
            version: 30,
            description: "drop_unused_wfexec_gin_indexes",
            sql: V030_DROP_UNUSED_WFEXEC_GIN_INDEXES,
        },
        MigrationDef {
            version: 31,
            description: "matching_partitions",
            sql: V031_MATCHING_PARTITIONS,
        },
        MigrationDef {
            version: 32,
            description: "concurrency_safe_event_id",
            sql: V032_CONCURRENCY_SAFE_EVENT_ID,
        },
        MigrationDef {
            version: 33,
            description: "reset_status",
            sql: V033_RESET_STATUS,
        },
        MigrationDef {
            version: 34,
            description: "bind_execution_version",
            sql: V034_BIND_EXECUTION_VERSION,
        },
        MigrationDef {
            version: 35,
            description: "task_queue_current_release",
            sql: V035_TASK_QUEUE_CURRENT_RELEASE,
        },
        MigrationDef {
            version: 36,
            description: "outbox_index_diet",
            sql: V036_OUTBOX_INDEX_DIET,
        },
        MigrationDef {
            version: 37,
            description: "step_counts_overlap",
            sql: V037_STEP_COUNTS_OVERLAP,
        },
        MigrationDef {
            version: 38,
            description: "schedule_name_per_organization",
            sql: V038_SCHEDULE_NAME_PER_ORGANIZATION,
        },
        MigrationDef {
            version: 39,
            description: "retry_dispatch_lease",
            sql: V039_RETRY_DISPATCH_LEASE,
        },
        MigrationDef {
            version: 40,
            description: "batch_operations_organization",
            sql: V040_BATCH_OPERATIONS_ORGANIZATION,
        },
        MigrationDef {
            version: 41,
            description: "task_queue_release_per_organization",
            sql: V041_TASK_QUEUE_RELEASE_PER_ORGANIZATION,
        },
        MigrationDef {
            version: 42,
            description: "task_token_index",
            sql: V042_TASK_TOKEN_INDEX,
        },
        MigrationDef {
            version: 43,
            description: "api_key_organization_delegation",
            sql: V043_API_KEY_ORGANIZATION_DELEGATION,
        },
        MigrationDef {
            version: 44,
            description: "child_workflow_tracking",
            sql: V044_CHILD_WORKFLOW_TRACKING,
        },
        MigrationDef {
            version: 45,
            description: "workflow_retries_settled",
            sql: V045_WORKFLOW_RETRIES_SETTLED,
        },
        MigrationDef {
            version: 46,
            description: "child_workflow_indexes",
            sql: V046_CHILD_WORKFLOW_INDEXES,
        },
        MigrationDef {
            version: 47,
            description: "retry_indexes",
            sql: V047_RETRY_INDEXES,
        },
        MigrationDef {
            version: 48,
            description: "activation_fence",
            sql: V048_ACTIVATION_FENCE,
        },
        MigrationDef {
            version: 49,
            description: "task_schedule_once",
            sql: V049_TASK_SCHEDULE_ONCE,
        },
        MigrationDef {
            version: 50,
            description: "undispatched_task_sweep",
            sql: V050_UNDISPATCHED_TASK_SWEEP,
        },
        MigrationDef {
            version: 51,
            description: "claimed_on_node",
            sql: V051_CLAIMED_ON_NODE,
        },
        MigrationDef {
            version: 52,
            description: "claimed_on_node_index",
            sql: V052_CLAIMED_ON_NODE_INDEX,
        },
        MigrationDef {
            version: 53,
            description: "worker_shutdowns",
            sql: V053_WORKER_SHUTDOWNS,
        },
        MigrationDef {
            version: 54,
            description: "matching_partition_organization",
            sql: V054_MATCHING_PARTITION_ORGANIZATION,
        },
        MigrationDef {
            version: 55,
            description: "stamp_task_organization",
            sql: V055_STAMP_TASK_ORGANIZATION,
        },
        MigrationDef {
            version: 56,
            description: "workflow_claim_by_organization",
            sql: V056_WORKFLOW_CLAIM_BY_ORGANIZATION,
        },
        MigrationDef {
            version: 57,
            description: "outbox_pending_by_organization",
            sql: V057_OUTBOX_PENDING_BY_ORGANIZATION,
        },
        MigrationDef {
            version: 58,
            description: "auto_heartbeat_timeout",
            sql: V058_AUTO_HEARTBEAT_TIMEOUT,
        },
        MigrationDef {
            version: 59,
            description: "workflow_id_holds",
            sql: V059_WORKFLOW_ID_HOLDS,
        },
        MigrationDef {
            version: 60,
            description: "worker_instances",
            sql: V060_WORKER_INSTANCES,
        },
        MigrationDef {
            version: 61,
            description: "worker_instance_claims_index",
            sql: V061_WORKER_INSTANCE_CLAIMS_INDEX,
        },
        MigrationDef {
            version: 62,
            description: "workflow_cancel_requests",
            sql: V062_WORKFLOW_CANCEL_REQUESTS,
        },
        MigrationDef {
            version: 63,
            description: "workflow_cancel_deadline_index",
            sql: V063_WORKFLOW_CANCEL_DEADLINE_INDEX,
        },
        MigrationDef {
            version: 64,
            description: "ended_runs_carry_completed_at",
            sql: V064_ENDED_RUNS_CARRY_COMPLETED_AT,
        },
        MigrationDef {
            version: 65,
            description: "workflow_run_and_task_timeouts",
            sql: V065_WORKFLOW_RUN_AND_TASK_TIMEOUTS,
        },
        MigrationDef {
            version: 66,
            description: "run_lookup_by_workflow_id",
            sql: V066_RUN_LOOKUP_BY_WORKFLOW_ID,
        },
        MigrationDef {
            version: 67,
            description: "run_timeout_retry_and_execution_deadline",
            sql: V067_RUN_TIMEOUT_RETRY_AND_EXECUTION_DEADLINE,
        },
        MigrationDef {
            version: 68,
            description: "outbox_pending_dispatch_by_queue",
            sql: V068_OUTBOX_PENDING_DISPATCH_BY_QUEUE,
        },
        MigrationDef {
            version: 69,
            description: "start_delay_annotations_and_request_id",
            sql: V069_START_DELAY_ANNOTATIONS_AND_REQUEST_ID,
        },
        MigrationDef {
            version: 70,
            description: "step_id_scheme_and_history_lineage",
            sql: V070_STEP_ID_SCHEME_AND_HISTORY_LINEAGE,
        },
        MigrationDef {
            version: 71,
            description: "drop_plugin_tables",
            sql: V071_DROP_PLUGIN_TABLES,
        },
        MigrationDef {
            version: 72,
            description: "drop_superseded_cluster_tables",
            sql: V072_DROP_SUPERSEDED_CLUSTER_TABLES,
        },
        MigrationDef {
            version: 73,
            description: "api_key_scopes_and_revocation",
            sql: V073_API_KEY_SCOPES_AND_REVOCATION,
        },
        MigrationDef {
            version: 74,
            description: "api_key_lookup_indexes",
            sql: V074_API_KEY_LOOKUP_INDEXES,
        },
        MigrationDef {
            version: 75,
            description: "drop_event_gates",
            sql: V075_DROP_EVENT_GATES,
        },
        MigrationDef {
            version: 76,
            description: "timers_may_be_recorded_after_they_fall_due",
            sql: V076_TIMERS_MAY_BE_RECORDED_AFTER_THEY_FALL_DUE,
        },
        MigrationDef {
            version: 77,
            description: "journal_task_completed_lookup",
            sql: V077_JOURNAL_TASK_COMPLETED_LOOKUP,
        },
        MigrationDef {
            version: 78,
            description: "step_results_in_the_journal",
            sql: V078_STEP_RESULTS_IN_THE_JOURNAL,
        },
        MigrationDef {
            version: 79,
            description: "drop_integration_tables",
            sql: V079_DROP_INTEGRATION_TABLES,
        },
        MigrationDef {
            version: 80,
            description: "drop_alert_and_notification_tables",
            sql: V080_DROP_ALERT_AND_NOTIFICATION_TABLES,
        },
        MigrationDef {
            version: 81,
            description: "drop_queue_tables",
            sql: V081_DROP_QUEUE_TABLES,
        },
        MigrationDef {
            version: 82,
            description: "drop_unused_views_tables_and_functions",
            sql: V082_DROP_UNUSED_VIEWS_TABLES_AND_FUNCTIONS,
        },
        MigrationDef {
            version: 83,
            description: "drop_event_store_and_idempotency_tables",
            sql: V083_DROP_EVENT_STORE_AND_IDEMPOTENCY_TABLES,
        },
        MigrationDef {
            version: 84,
            description: "drop_workflow_failed_notify",
            sql: V084_DROP_WORKFLOW_FAILED_NOTIFY,
        },
        MigrationDef {
            version: 85,
            description: "organization_ids_are_opaque",
            sql: V085_ORGANIZATION_IDS_ARE_OPAQUE,
        },
        MigrationDef {
            version: 86,
            description: "journal_holder_preference",
            sql: V086_JOURNAL_HOLDER_PREFERENCE,
        },
        MigrationDef {
            version: 87,
            description: "activation_journal_position",
            sql: V087_ACTIVATION_JOURNAL_POSITION,
        },
        MigrationDef {
            version: 88,
            description: "task_scheduled_through_entry",
            sql: V088_TASK_SCHEDULED_THROUGH_ENTRY,
        },
    ]
}

/// Run all pending migrations
///
/// This function applies all pending migrations to the database.
/// It should typically be called from a separate migration tool in production.
///
/// # Errors
///
/// Returns an error if migration execution fails.
/// Return `sql` with line (`--`) and block (`/* */`) comments removed, so keyword
/// detection (e.g. CONCURRENTLY) never matches a mention inside a comment. All
/// comment delimiters are ASCII, so non-comment ranges (which may contain UTF-8)
/// are copied byte-for-byte and stay valid.
fn sql_without_comments(sql: &str) -> String {
    let b = sql.as_bytes();
    let n = b.len();
    let mut out: Vec<u8> = Vec::with_capacity(n);
    let mut i = 0;
    while i < n {
        if b[i] == b'-' && i + 1 < n && b[i + 1] == b'-' {
            while i < n && b[i] != b'\n' {
                i += 1;
            }
        } else if b[i] == b'/' && i + 1 < n && b[i + 1] == b'*' {
            i += 2;
            while i + 1 < n && !(b[i] == b'*' && b[i + 1] == b'/') {
                i += 1;
            }
            i = (i + 2).min(n);
            out.push(b' ');
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// If `b` starts with a dollar-quote opening tag (`$$` or `$tag$`), return its
/// byte length (both `$` included), else `None`. Tags follow identifier rules and
/// cannot start with a digit (so a positional `$1` is not treated as a tag).
fn dollar_tag_len(b: &[u8]) -> Option<usize> {
    if b.is_empty() || b[0] != b'$' {
        return None;
    }
    if b.len() > 1 && b[1].is_ascii_digit() {
        return None;
    }
    let mut k = 1;
    while k < b.len() && (b[k] == b'_' || b[k].is_ascii_alphanumeric()) {
        k += 1;
    }
    if k < b.len() && b[k] == b'$' {
        Some(k + 1)
    } else {
        None
    }
}

/// Split a SQL script into top-level statements. A `;` inside a single-quoted
/// string, a dollar-quoted body (`$$...$$` / `$tag$...$tag$`), or a line/block
/// comment does NOT terminate a statement — so a plpgsql function body survives.
/// Used only on the rare CONCURRENTLY path (statements run outside a transaction);
/// the normal path executes the whole script at once via `raw_sql`.
fn split_sql_statements(sql: &str) -> Vec<String> {
    let b = sql.as_bytes();
    let n = b.len();
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut i = 0usize;
    while i < n {
        match b[i] {
            b'-' if i + 1 < n && b[i + 1] == b'-' => {
                while i < n && b[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if i + 1 < n && b[i + 1] == b'*' => {
                i += 2;
                while i + 1 < n && !(b[i] == b'*' && b[i + 1] == b'/') {
                    i += 1;
                }
                i = (i + 2).min(n);
            }
            b'\'' => {
                i += 1;
                while i < n {
                    if b[i] == b'\'' {
                        if i + 1 < n && b[i + 1] == b'\'' {
                            i += 2; // escaped quote
                            continue;
                        }
                        i += 1;
                        break;
                    }
                    i += 1;
                }
            }
            b'$' => {
                if let Some(tag_len) = dollar_tag_len(&b[i..]) {
                    let tag = &b[i..i + tag_len];
                    let mut j = i + tag_len;
                    let mut closed = false;
                    while j + tag_len <= n {
                        if &b[j..j + tag_len] == tag {
                            j += tag_len;
                            closed = true;
                            break;
                        }
                        j += 1;
                    }
                    i = if closed { j } else { n };
                } else {
                    i += 1;
                }
            }
            b';' => {
                let stmt = sql[start..i].trim();
                if !stmt.is_empty() {
                    out.push(stmt.to_string());
                }
                i += 1;
                start = i;
            }
            _ => i += 1,
        }
    }
    let stmt = sql[start..].trim();
    if !stmt.is_empty() {
        out.push(stmt.to_string());
    }
    out
}

/// The line a migration starts with to be run outside a transaction without
/// building an index concurrently: a batched backfill, which commits after
/// each batch so a large table is never held in one long transaction.
const OUTSIDE_A_TRANSACTION: &str = "-- orcher: outside a transaction";

/// Whether `sql` must be run statement by statement outside a transaction:
/// because it builds an index concurrently, which cannot run inside one, or
/// because it says so with [`OUTSIDE_A_TRANSACTION`]. The keyword is looked
/// for in the comment-stripped SQL, so mentioning it in a comment does not
/// count; the directive is a comment, and must be a line of its own.
fn runs_outside_a_transaction(sql: &str) -> bool {
    sql_without_comments(sql)
        .to_uppercase()
        .contains("CONCURRENTLY")
        || sql.lines().any(|line| line.trim() == OUTSIDE_A_TRANSACTION)
}

/// Apply every pending migration, in version order.
///
/// Only what `get_migrations()` lists is executed — a file on disk that nobody
/// registered is skipped silently, which is what
/// `every_migration_file_is_registered` exists to catch.
pub async fn run_migrations(pool: &sqlx::PgPool) -> OrcherResult<()> {
    info!("Applying database migrations...");

    ensure_migrations_table(pool).await?;

    let all_migrations = get_migrations();

    // Before applying anything, confirm that what the database says it has
    // applied is what this build calls by those versions. The runner used to
    // track version numbers only, so a different migration sharing a number —
    // two branches each adding "V042" — was treated as already applied and
    // never ran, and an applied migration edited afterwards never re-ran either.
    // Both reported "All migrations already applied".
    let applied = verify_applied_migrations(pool, &all_migrations).await?;

    let mut applied_count = 0;
    for migration in &all_migrations {
        if applied.contains(&migration.version) {
            continue;
        }

        info!(
            "Applying migration V{:03}: {}",
            migration.version, migration.description
        );

        // CREATE INDEX CONCURRENTLY cannot run inside a transaction block.
        // Detect such migrations and execute them directly on the pool.
        // Check the comment-stripped SQL so a "concurrently" mention in a
        // comment does not trigger the (statement-splitting) non-transactional
        // path — which would break a plpgsql $$...$$ body.
        let outside_a_transaction = runs_outside_a_transaction(migration.sql);

        // Any failure stops the run. Errors mentioning "already exists" or
        // "duplicate key" used to be logged as ignorable and the migration
        // reported as skipped — but a skipped migration was never recorded, and
        // in the transactional path the whole migration was rolled back, so
        // every statement after the one that tripped never ran, on every boot,
        // silently. Migrations here are written to be re-runnable; one that
        // collides with existing schema is drift, and drift has to be seen.
        let failed = |e: sqlx::Error| {
            error!("Migration V{:03} failed: {}", migration.version, e);
            orcher_common::error::OrcherError::internal(format!(
                "Migration V{:03} ({}) failed: {}",
                migration.version, migration.description, e
            ))
        };

        if outside_a_transaction {
            info!(
                "Migration V{:03} runs outside a transaction — executing statements individually",
                migration.version
            );

            // A raw connection, so CREATE INDEX CONCURRENTLY is never wrapped
            // in a transaction.
            let mut conn = pool.acquire().await.map_err(|e| {
                orcher_common::error::OrcherError::internal(format!(
                    "Failed to acquire connection: {}",
                    e
                ))
            })?;

            for stmt in split_sql_statements(migration.sql) {
                let stmt = stmt.trim();
                // A segment is comment-only if every non-empty line starts with "--".
                let is_comment_only = stmt.is_empty()
                    || stmt
                        .lines()
                        .filter(|l| !l.trim().is_empty())
                        .all(|l| l.trim_start().starts_with("--"));
                if is_comment_only {
                    continue;
                }
                conn.execute(stmt).await.map_err(&failed)?;
            }

            // A CONCURRENTLY build that fails partway leaves an INVALID index
            // behind, and a re-run's IF NOT EXISTS sees it and skips — so the
            // index stays invalid, unused, and reported as created, forever.
            // Refuse to record the migration while any such index exists.
            let invalid: Vec<String> = sqlx::query_scalar(
                "SELECT c.relname::text FROM pg_index i \
                 JOIN pg_class c ON c.oid = i.indexrelid \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE NOT i.indisvalid AND n.nspname = current_schema()",
            )
            .fetch_all(&mut *conn)
            .await
            .map_err(&failed)?;
            if !invalid.is_empty() {
                return Err(orcher_common::error::OrcherError::internal(format!(
                    "Migration V{:03} left invalid indexes {:?}. A concurrent index build \
                     failed partway; drop them (DROP INDEX CONCURRENTLY ...) and restart so \
                     the migration rebuilds them.",
                    migration.version, invalid
                )));
            }

            record_migration(&mut conn, migration).await?;
        } else {
            let mut tx = pool.begin().await.map_err(|e| {
                orcher_common::error::OrcherError::internal(format!(
                    "Failed to start transaction: {}",
                    e
                ))
            })?;

            sqlx::raw_sql(migration.sql)
                .execute(&mut *tx)
                .await
                .map_err(&failed)?;
            record_migration(&mut tx, migration).await?;

            tx.commit().await.map_err(|e| {
                orcher_common::error::OrcherError::internal(format!(
                    "Failed to commit migration: {}",
                    e
                ))
            })?;
        }

        info!("Migration V{:03} applied successfully", migration.version);
        applied_count += 1;
    }

    if applied_count == 0 {
        info!("All migrations already applied");
    } else {
        info!("Applied {} migration(s) successfully", applied_count);
    }

    Ok(())
}

/// The fingerprint recorded for a migration: SHA-256 of its SQL.
fn migration_checksum(sql: &str) -> Vec<u8> {
    use sha2::{Digest, Sha256};
    Sha256::digest(sql.as_bytes()).to_vec()
}

/// Record a migration as applied, with the checksum of what was run.
async fn record_migration(
    conn: &mut sqlx::PgConnection,
    migration: &MigrationDef,
) -> OrcherResult<()> {
    sqlx::query(
        "INSERT INTO _sqlx_migrations (version, description, installed_on, success, checksum)
         VALUES ($1, $2, NOW(), true, $3)",
    )
    .bind(migration.version)
    .bind(migration.description)
    .bind(migration_checksum(migration.sql))
    .execute(conn)
    .await
    .map_err(|e| {
        orcher_common::error::OrcherError::internal(format!("Failed to record migration: {}", e))
    })?;
    Ok(())
}

/// Check the applied migrations against this build, and return their versions.
///
/// - A recorded description that differs from this build's for the same
///   version means two different migrations share a number. Refused.
/// - A recorded checksum that differs means the migration's SQL changed after
///   it ran. Refused: the change will never be applied, and the file no longer
///   describes the schema.
/// - A row with no checksum predates checksums being recorded. Its description
///   is still checked, and the checksum is filled in from this build, so any
///   later edit is caught. This cannot prove the row matches what originally
///   ran, only that nothing changes from here on.
/// - A version this build does not know is left alone with a warning: that is a
///   database migrated by a newer build, which is what a rollback looks like.
async fn verify_applied_migrations(
    pool: &sqlx::PgPool,
    all_migrations: &[MigrationDef],
) -> OrcherResult<Vec<i64>> {
    let rows: Vec<(i64, String, Option<Vec<u8>>)> = sqlx::query_as(
        "SELECT version, description, checksum FROM _sqlx_migrations ORDER BY version",
    )
    .fetch_all(pool)
    .await
    .map_err(|e| {
        orcher_common::error::OrcherError::internal(format!(
            "Failed to get applied migrations: {}",
            e
        ))
    })?;

    let mut backfilled = 0;
    for (version, description, checksum) in &rows {
        let Some(migration) = all_migrations.iter().find(|m| m.version == *version) else {
            warn!(
                "Database has migration V{:03} ({}) which this build does not know — \
                 migrated by a newer build?",
                version, description
            );
            continue;
        };

        if migration.description != description {
            return Err(orcher_common::error::OrcherError::internal(format!(
                "Migration V{:03} is recorded as '{}' but this build's V{:03} is '{}'. Two \
                 different migrations share a version number, so this build's would never run. \
                 Renumber one of them.",
                version, description, version, migration.description
            )));
        }

        let expected = migration_checksum(migration.sql);
        match checksum {
            Some(recorded) if *recorded != expected => {
                return Err(orcher_common::error::OrcherError::internal(format!(
                    "Migration V{:03} ({}) has changed since it was applied. An applied \
                     migration is never re-run, so the edit would silently not take effect; put \
                     the change in a new migration instead.",
                    version, description
                )));
            }
            Some(_) => {}
            None => {
                sqlx::query("UPDATE _sqlx_migrations SET checksum = $1 WHERE version = $2")
                    .bind(&expected)
                    .bind(version)
                    .execute(pool)
                    .await
                    .map_err(|e| {
                        orcher_common::error::OrcherError::internal(format!(
                            "Failed to record checksum for V{:03}: {}",
                            version, e
                        ))
                    })?;
                backfilled += 1;
            }
        }
    }

    if backfilled > 0 {
        info!(
            "Recorded checksums for {} migration(s) applied before checksums were kept",
            backfilled
        );
    }

    Ok(rows.into_iter().map(|(version, _, _)| version).collect())
}

/// Versions recorded as applied, for status reporting.
async fn get_applied_migrations(pool: &sqlx::PgPool) -> OrcherResult<Vec<i64>> {
    sqlx::query_scalar("SELECT version FROM _sqlx_migrations ORDER BY version")
        .fetch_all(pool)
        .await
        .map_err(|e| {
            orcher_common::error::OrcherError::internal(format!(
                "Failed to get applied migrations: {}",
                e
            ))
        })
}

/// Ensure migrations tracking table exists
async fn ensure_migrations_table(pool: &sqlx::PgPool) -> OrcherResult<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS _sqlx_migrations (
            version BIGINT PRIMARY KEY,
            description TEXT NOT NULL,
            installed_on TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            success BOOLEAN NOT NULL,
            checksum BYTEA,
            execution_time BIGINT
        )",
    )
    .execute(pool)
    .await
    .map_err(|e| {
        orcher_common::error::OrcherError::internal(format!(
            "Failed to create migrations table: {}",
            e
        ))
    })?;

    Ok(())
}

/// Run migrations with a PostgreSQL configuration
///
/// Helper function for migration tools that creates a temporary connection pool,
/// runs migrations, and cleans up.
pub async fn run_migrations_with_config(config: &PostgresConfig) -> OrcherResult<()> {
    info!("Creating temporary connection pool for migrations...");
    let backend = PostgresBackend::new(config.clone()).await?;

    run_migrations(backend.pool()).await?;

    backend.close().await;
    info!("Connection pool closed");

    Ok(())
}

/// Check migration status without applying
///
/// Returns information about current migration state and pending migrations.
/// Refuse to run against a schema this build does not match.
///
/// Migrations are applied by the `orcher-migrate` tool (and by the k8s bundle's
/// migration Job), not by the engine at startup. Nothing previously checked
/// that they had actually been run, so a service would boot happily against a
/// schema missing the columns its code expects and fail later at the point of
/// use — as a query error, a missing table, or a feature that silently does
/// nothing. Those look exactly like product defects and cost hours to
/// misdiagnose as such.
///
/// # What counts as "behind"
///
/// Every registered migration from [`APPLIED_BY_THE_RUNNER_FROM`] on that the
/// database has not recorded, whether above its highest applied version or
/// below it. A gap below it is a migration that never ran: branches developed
/// side by side number their migrations apart, and one merged after another
/// with a higher number has already been applied lands below the high-water
/// mark. Comparing against that mark alone reported such a schema current.
///
/// Older versions are not held to this: real databases have gaps there from
/// migrations applied out of band before this bookkeeping existed. A
/// development database here reports 22 such gaps while being schema-complete
/// at V036 — refusing to start on that would take out healthy deployments to
/// report history that no longer matters.
///
/// Set `ORCHER_ALLOW_SCHEMA_DRIFT=true` to downgrade the refusal to a warning,
/// for local work against a partially migrated database. Not the default: in
/// production, starting on the wrong schema is worse than not starting.
pub async fn ensure_schema_current(pool: &sqlx::PgPool) -> OrcherResult<()> {
    ensure_schemas_current(&[("database", pool)]).await
}

/// [`ensure_schema_current`] for every database the engine writes to: the
/// primary and each shard database, named by `label` in what is reported
/// (never by URL, which carries credentials).
///
/// Shard databases hold workflows, their tasks and their journals, so a shard
/// behind this build fails the same way the primary would — only for the
/// workflows that live there, which makes it harder still to tell from a
/// product defect. Checking the primary alone started the engine on one.
pub async fn ensure_schemas_current(databases: &[(&str, &sqlx::PgPool)]) -> OrcherResult<()> {
    let registered: Vec<i64> = get_migrations().iter().map(|m| m.version).collect();
    let mut behind = Vec::new();
    for (label, pool) in databases {
        ensure_migrations_table(pool).await?;
        let applied_versions = get_applied_migrations(pool).await?;
        let applied = applied_versions.iter().copied().max().unwrap_or(0);
        let missing = missing_migrations(&registered, &applied_versions);
        if missing.is_empty() {
            info!(
                database = %label,
                applied_version = applied,
                "Database schema is current for this build"
            );
            continue;
        }
        let listed: Vec<String> = missing.iter().map(|v| format!("V{v:03}")).collect();
        behind.push(format!(
            "{label} is {} migration(s) behind ({}), with V{:03} the highest applied",
            missing.len(),
            listed.join(", "),
            applied,
        ));
    }

    if behind.is_empty() {
        return Ok(());
    }

    let summary = format!(
        "database schema is behind this build: {}. Apply them with `orcher-migrate run`, \
         or set ORCHER_ALLOW_SCHEMA_DRIFT=true to start anyway.",
        behind.join("; "),
    );

    let allow_drift = std::env::var("ORCHER_ALLOW_SCHEMA_DRIFT")
        .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
        .unwrap_or(false);

    if allow_drift {
        warn!(
            "ORCHER_ALLOW_SCHEMA_DRIFT is set — starting anyway. {}",
            summary
        );
        return Ok(());
    }

    error!("{}", summary);
    Err(orcher_common::error::OrcherError::internal(summary))
}

/// The first migration every database has had applied by this runner, which
/// records what it applies; see [`ensure_schema_current`]. Everything from here
/// on must be recorded.
pub const APPLIED_BY_THE_RUNNER_FROM: i64 = 50;

/// The registered migrations a database recording `applied` still needs: every
/// one from [`APPLIED_BY_THE_RUNNER_FROM`] on that it has not recorded, above
/// or below the highest it has.
fn missing_migrations(registered: &[i64], applied: &[i64]) -> Vec<i64> {
    let highest = applied.iter().copied().max().unwrap_or(0);
    registered
        .iter()
        .copied()
        .filter(|v| (*v >= APPLIED_BY_THE_RUNNER_FROM || *v > highest) && !applied.contains(v))
        .collect()
}

/// Which migrations this database has, and which it still needs.
pub async fn check_migration_status(pool: &sqlx::PgPool) -> OrcherResult<MigrationStatus> {
    debug!("Checking migration status...");

    // Ensure migrations table exists
    ensure_migrations_table(pool).await?;

    // Get current migration version
    let current_version: Option<(i64,)> =
        sqlx::query_as("SELECT version FROM _sqlx_migrations ORDER BY version DESC LIMIT 1")
            .fetch_optional(pool)
            .await
            .map_err(|e| {
                orcher_common::error::OrcherError::internal(format!(
                    "Failed to check migrations: {}",
                    e
                ))
            })?;

    let current_version = current_version.map(|v| v.0);

    // Get all applied migrations
    let applied_versions: Vec<i64> = get_applied_migrations(pool).await?;

    // Compare with available migrations
    let all_migrations = get_migrations();
    let total_migrations = all_migrations.len();
    let applied_count = applied_versions.len();
    let pending_count = total_migrations.saturating_sub(applied_count);

    // Get pending migration names
    let pending_migrations: Vec<String> = all_migrations
        .iter()
        .filter(|m| !applied_versions.contains(&m.version))
        .map(|m| format!("V{:03}: {}", m.version, m.description))
        .collect();

    let status = MigrationStatus {
        up_to_date: pending_count == 0,
        pending_count,
        current_version,
        pending_migrations,
    };

    debug!(
        "Migration status: up_to_date={}, applied={}, pending={}",
        status.up_to_date, applied_count, pending_count
    );

    Ok(status)
}

/// Print migration status in a human-readable format
fn print_migration_status(status: &MigrationStatus) {
    if status.up_to_date {
        info!("Database is up to date");
        if let Some(version) = status.current_version {
            info!("   Current version: V{:03}", version);
        }
    } else {
        warn!("Database has pending migrations");
        if let Some(version) = status.current_version {
            warn!("   Current version: V{:03}", version);
        } else {
            warn!("   Current version: none (no migrations applied)");
        }
        warn!("   Pending count: {}", status.pending_count);

        if !status.pending_migrations.is_empty() {
            warn!("   Pending migrations:");
            for migration in &status.pending_migrations {
                warn!("     - {}", migration);
            }
        }
    }
}

/// A connection URL (Postgres, Redis) safe to write to a log.
///
/// The password — in the userinfo or as a `password`/`sslpassword` query
/// parameter — is replaced with `****`. A URL that does not parse is replaced
/// entirely rather than echoed, because a string that is not a well-formed URL
/// cannot be trusted to have its secret where a parser would look.
///
/// This is the one implementation, for every connection string logged. There were three private copies — in this
/// file, the migrate binary and the gateway — and the orchestrator used none of
/// them, so it logged its connection string, password included, at INFO on
/// every start. The copies also found the password by the last `:` before the
/// `@`, which for a URL with no password masked the scheme instead.
pub fn redact_connection_url(url: &str) -> String {
    const SECRET_PARAMS: [&str; 2] = ["password", "sslpassword"];

    let Ok(mut parsed) = url::Url::parse(url) else {
        return "<connection url not shown: could not be parsed>".to_string();
    };

    if parsed.password().is_some() {
        // Only fails for URLs that cannot carry credentials, which a parsed
        // database URL with a password can.
        let _ = parsed.set_password(Some("****"));
    }

    if parsed.query().is_some() {
        let pairs: Vec<(String, String)> = parsed
            .query_pairs()
            .map(|(k, v)| {
                let value = if SECRET_PARAMS.contains(&k.to_ascii_lowercase().as_str()) {
                    "****".to_string()
                } else {
                    v.into_owned()
                };
                (k.into_owned(), value)
            })
            .collect();
        parsed.query_pairs_mut().clear().extend_pairs(pairs);
    }

    parsed.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every `.sql` file in the migrations directory must be registered.
    ///
    /// A migration lives in three places: the `.sql` file, an `include_str!`
    /// constant in `migrations/mod.rs`, and an entry in `get_migrations()`
    /// below. Miss the third and the file is **silently ignored** — the runner
    /// reports success, the schema never changes, and the feature fails later
    /// in a way that looks like a product bug rather than a missing table.
    ///
    /// That has cost real time twice: once when a new migration did nothing at
    /// all, and once when a rebase dropped an entry while resolving a conflict
    /// in this very list. Both were invisible until something downstream broke.
    #[test]
    fn every_migration_file_is_registered() {
        let dir =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/migrations/sql/postgres");

        let mut on_disk: Vec<(i64, String)> = std::fs::read_dir(&dir)
            .expect("migrations directory should exist")
            .filter_map(|entry| {
                let name = entry.ok()?.file_name().to_string_lossy().into_owned();
                let stem = name.strip_suffix(".sql")?;
                // V035__task_queue_current_release -> (35, "task_queue_current_release")
                let (version, description) = stem.split_once("__")?;
                let version = version.trim_start_matches('V').parse::<i64>().ok()?;
                Some((version, description.to_string()))
            })
            .collect();
        on_disk.sort();

        assert!(
            !on_disk.is_empty(),
            "found no migration files — the directory layout must have changed, \
             which would make this guard silently vacuous"
        );

        let registered = get_migrations();

        for (version, description) in &on_disk {
            let found = registered.iter().find(|m| m.version == *version);
            let found = found.unwrap_or_else(|| {
                panic!(
                    "migration V{version:03}__{description}.sql exists on disk but is not in \
                     get_migrations(). It will be silently skipped: the runner will report \
                     success and the schema will never change. Register it in \
                     migrations/mod.rs and in the list in setup.rs."
                )
            });
            assert_eq!(
                found.description, description,
                "V{version:03} is registered as '{}' but its file is named '{description}'. \
                 The mismatch makes the migration hard to find from a log line.",
                found.description
            );
        }
    }

    /// A migration merged below one already applied is still owed: a gap under
    /// the highest applied version counts, not only what lies above it. Older
    /// gaps, from before the runner recorded everything, do not.
    #[test]
    fn a_gap_below_the_highest_applied_migration_is_missing() {
        let registered = [30, 31, 50, 51, 52, 53, 54];
        assert_eq!(
            missing_migrations(&registered, &[30, 50, 53]),
            vec![51, 52, 54],
            "the gap below V053 and what lies above it"
        );
        assert_eq!(
            missing_migrations(&registered, &[30, 50, 51, 52, 53, 54]),
            Vec::<i64>::new(),
            "an old gap (V031) is tolerated"
        );
        assert_eq!(
            missing_migrations(&[1, 2, 3], &[1]),
            vec![2, 3],
            "anything above the highest applied is still missing"
        );
        assert!(
            get_migrations()
                .iter()
                .any(|m| m.version == APPLIED_BY_THE_RUNNER_FROM),
            "the threshold names a registered migration"
        );
    }

    /// Two entries claiming the same version would make which one runs depend on
    /// list order, and the second would be recorded as applied without running.
    #[test]
    fn migration_versions_are_unique() {
        let registered = get_migrations();
        let mut seen = std::collections::HashSet::new();
        for m in &registered {
            assert!(
                seen.insert(m.version),
                "migration version {} is registered twice ('{}' collides with an earlier entry)",
                m.version,
                m.description
            );
        }
    }

    #[test]
    fn concurrently_in_comment_is_not_detected() {
        // A "concurrently" mention in a comment must NOT trigger the
        // statement-splitting non-transactional path (regression: V032).
        let sql = "-- serialize concurrently appended events\n\
                   CREATE OR REPLACE FUNCTION f() RETURNS int AS $$ BEGIN RETURN 1; END; $$ LANGUAGE plpgsql;";
        assert!(!sql_without_comments(sql)
            .to_uppercase()
            .contains("CONCURRENTLY"));
    }

    #[test]
    fn a_backfill_can_ask_to_run_outside_a_transaction() {
        assert!(runs_outside_a_transaction(V055_STAMP_TASK_ORGANIZATION));
        assert!(runs_outside_a_transaction(
            V056_WORKFLOW_CLAIM_BY_ORGANIZATION
        ));
        assert!(!runs_outside_a_transaction(V053_WORKER_SHUTDOWNS));
        // Only as a line of its own.
        assert!(!runs_outside_a_transaction(
            "SELECT 1; -- orcher: outside a transaction, as it happens"
        ));
    }

    #[test]
    fn real_concurrently_keyword_is_detected() {
        let sql = "CREATE INDEX CONCURRENTLY idx_foo ON bar (baz);";
        assert!(sql_without_comments(sql)
            .to_uppercase()
            .contains("CONCURRENTLY"));
    }

    #[test]
    fn sql_without_comments_strips_line_and_block_comments() {
        let sql = "SELECT 1; -- trailing\n/* block\n comment */ SELECT 2;";
        let stripped = sql_without_comments(sql);
        assert!(!stripped.contains("trailing"));
        assert!(!stripped.contains("block"));
        assert!(stripped.contains("SELECT 1"));
        assert!(stripped.contains("SELECT 2"));
    }

    #[test]
    fn split_does_not_break_dollar_quoted_body() {
        // The function body has internal semicolons inside $$...$$ — must stay one
        // statement.
        let sql = "CREATE FUNCTION f() RETURNS int AS $$\n\
                   DECLARE x int;\n\
                   BEGIN\n  PERFORM 1; RETURN 1;\nEND;\n$$ LANGUAGE plpgsql;\n\
                   CREATE INDEX i ON t (c);";
        let stmts = split_sql_statements(sql);
        assert_eq!(stmts.len(), 2, "got: {:?}", stmts);
        assert!(stmts[0].contains("CREATE FUNCTION"));
        assert!(stmts[0].contains("RETURN 1"));
        assert!(stmts[1].contains("CREATE INDEX"));
    }

    #[test]
    fn split_respects_tagged_dollar_quotes_and_strings() {
        let sql = "INSERT INTO t VALUES ('a;b', $tag$ c;d $tag$); SELECT 2;";
        let stmts = split_sql_statements(sql);
        assert_eq!(stmts.len(), 2, "got: {:?}", stmts);
        assert!(stmts[0].contains("'a;b'"));
        assert!(stmts[0].contains("c;d"));
    }

    #[test]
    fn split_handles_escaped_single_quotes() {
        let sql = "SELECT 'it''s; ok'; SELECT 2;";
        let stmts = split_sql_statements(sql);
        assert_eq!(stmts.len(), 2, "got: {:?}", stmts);
        assert!(stmts[0].contains("it''s; ok"));
    }

    #[test]
    fn test_setup_config_development() {
        let config = SetupConfig::development("postgresql://localhost/test");
        assert!(config.auto_create_database);
        assert!(config.auto_migrate);
        assert!(!config.fail_on_pending_migrations);
        assert!(!config.dry_run);
    }

    #[test]
    fn test_setup_config_production() {
        let config = SetupConfig::production("postgresql://localhost/test");
        assert!(!config.auto_create_database);
        assert!(!config.auto_migrate);
        assert!(config.fail_on_pending_migrations);
        assert!(!config.dry_run);
    }

    #[test]
    fn test_setup_config_migration_tool() {
        let config = SetupConfig::migration_tool("postgresql://localhost/test");
        assert!(!config.auto_create_database);
        assert!(config.auto_migrate);
        assert!(!config.fail_on_pending_migrations);
        assert!(!config.dry_run);
    }

    #[test]
    fn a_password_is_redacted() {
        assert_eq!(
            redact_connection_url("postgresql://orcher:s3cret@db.internal:5432/orcher"),
            "postgresql://orcher:****@db.internal:5432/orcher"
        );
    }

    /// The old copies masked from the last `:` before `@`, which for a URL with
    /// no password is the scheme's.
    #[test]
    fn a_url_without_a_password_is_left_intact() {
        let url = "postgresql://orcher@db.internal:5432/orcher";
        assert_eq!(redact_connection_url(url), url);
    }

    #[test]
    fn a_password_in_the_query_is_redacted() {
        let out =
            redact_connection_url("postgresql://db.internal/orcher?user=orcher&password=s3cret");
        assert!(!out.contains("s3cret"), "{out}");
        assert!(out.contains("user=orcher"), "{out}");
    }

    /// Redis carries its password in the userinfo too, often with no user.
    #[test]
    fn a_redis_password_is_redacted() {
        let out = redact_connection_url("redis://:s3cret@cache.internal:6379/0");
        assert!(!out.contains("s3cret"), "{out}");
        assert!(out.contains("cache.internal:6379"), "{out}");
    }

    #[test]
    fn an_unparsable_url_is_not_echoed() {
        let out = redact_connection_url("not a url but contains s3cret");
        assert!(!out.contains("s3cret"));
    }

    #[test]
    fn test_migration_status() {
        let status = MigrationStatus {
            up_to_date: true,
            pending_count: 0,
            current_version: Some(20),
            pending_migrations: vec![],
        };

        assert!(status.is_up_to_date());
        assert!(!status.has_pending());
    }

    #[test]
    fn test_migration_status_pending() {
        let status = MigrationStatus {
            up_to_date: false,
            pending_count: 2,
            current_version: Some(18),
            pending_migrations: vec![
                "V019: add_feature".to_string(),
                "V020: event_store".to_string(),
            ],
        };

        assert!(!status.is_up_to_date());
        assert!(status.has_pending());
        assert_eq!(status.pending_count, 2);
    }

    /// A shard database behind this build refuses the start, as the primary
    /// does, and says which database it is; once it is migrated, the start
    /// goes ahead. Only the primary used to be checked.
    #[tokio::test]
    #[ignore] // Requires PostgreSQL: TEST_DATABASE_URL
    async fn a_shard_behind_this_build_refuses_the_start() {
        use sqlx::postgres::PgConnectOptions;
        use std::str::FromStr;

        let url = std::env::var("TEST_DATABASE_URL")
            .unwrap_or_else(|_| "postgresql://postgres:postgres@localhost:5432/orcher_test".into());
        let options = PgConnectOptions::from_str(&url).expect("TEST_DATABASE_URL");
        let primary = sqlx::PgPool::connect_with(options.clone())
            .await
            .expect("connect to the test database");
        run_migrations(&primary).await.expect("migrate the primary");

        let name = format!(
            "{}_behind_shard",
            options.get_database().unwrap_or("orcher_test")
        );
        sqlx::query(&format!("DROP DATABASE IF EXISTS \"{name}\" WITH (FORCE)"))
            .execute(&primary)
            .await
            .expect("drop a leftover shard database");
        sqlx::query(&format!("CREATE DATABASE \"{name}\""))
            .execute(&primary)
            .await
            .expect("create the shard database");
        let shard = sqlx::PgPool::connect_with(options.database(&name))
            .await
            .expect("connect to the shard database");
        run_migrations(&shard).await.expect("migrate the shard");
        let newest = get_migrations().iter().map(|m| m.version).max().unwrap();
        sqlx::query("DELETE FROM _sqlx_migrations WHERE version = $1")
            .bind(newest)
            .execute(&shard)
            .await
            .expect("put the shard one migration behind");

        std::env::remove_var("ORCHER_ALLOW_SCHEMA_DRIFT");
        let refused =
            ensure_schemas_current(&[("database", &primary), ("shard database 1", &shard)])
                .await
                .expect_err("a shard behind this build refuses the start");
        let message = refused.to_string();
        assert!(
            message.contains("shard database 1") && message.contains(&format!("V{newest:03}")),
            "the refusal names the shard and what it lacks: {message}"
        );
        assert!(
            !message.contains("database is"),
            "the current primary is not reported behind: {message}"
        );

        run_migrations(&shard)
            .await
            .expect("bring the shard up to date");
        ensure_schemas_current(&[("database", &primary), ("shard database 1", &shard)])
            .await
            .expect("a current shard lets the start go ahead");

        shard.close().await;
        let _ = sqlx::query(&format!("DROP DATABASE IF EXISTS \"{name}\" WITH (FORCE)"))
            .execute(&primary)
            .await;
    }

    #[tokio::test]
    #[ignore] // Run only with --ignored flag and a test database
    async fn test_database_setup_development() {
        let config =
            SetupConfig::development("postgresql://orcher:orcher@localhost:5432/orcher_test_dev");
        let result = setup_database(config).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    #[ignore] // Run only with --ignored flag and a test database
    async fn test_migration_status_check() {
        let config = PostgresConfig {
            url: "postgresql://orcher:orcher@localhost:5432/orcher_test".to_string(),
            ..Default::default()
        };

        let backend = PostgresBackend::new(config).await.unwrap();
        let status = check_migration_status(backend.pool()).await;

        // Should succeed or fail gracefully
        match status {
            Ok(s) => {
                println!("Migration status: {:?}", s);
            }
            Err(e) => {
                println!("Expected error (no migrations table): {}", e);
            }
        }
    }
}
