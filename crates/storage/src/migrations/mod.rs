//! # Database Migrations Module
//!
//! This module provides database schema migration capabilities for orcher-storage.
//! It handles version management, migration execution, and rollback functionality.
//!
//! Migrations are SQL files located in `migrations/sql/postgres/` following the naming
//! convention: `V{version}__{description}.sql` (e.g., `V003__workflow_tables.sql`)

use sha2::{Digest, Sha256};
use std::fmt;

/// Migration version identifier
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct MigrationVersion(pub u32);

/// Migration metadata and content
#[derive(Debug, Clone)]
pub struct Migration {
    /// Migration version identifier
    pub version: MigrationVersion,
    /// Human-readable migration name
    pub name: String,
    /// SQL content to execute
    pub sql: &'static str,
    /// SHA256 checksum of the SQL content
    pub checksum: String,
}

/// Migration execution result
#[derive(Debug)]
pub enum MigrationResult {
    /// Migration was successfully applied
    Applied,
    /// Migration was skipped (already applied)
    Skipped,
    /// Migration failed with error message
    Failed(String),
}

/// SQL migration files are embedded in the binary
pub mod sql {
    /// PostgreSQL migration files
    pub mod postgres {
        /// V001 — Core infrastructure tables (worker_heartbeats, system_metrics) + indexes
        pub const V001_INITIAL_SCHEMA: &str = include_str!("sql/postgres/V001__initial_schema.sql");

        /// V002 — Organizations and multi-tenancy (moved early so all tables can reference it)
        pub const V002_ORGANIZATION_TABLES: &str =
            include_str!("sql/postgres/V002__organization_tables.sql");

        /// V003 — Workflow execution tables (includes claim columns, requeue, failure notifications)
        pub const V003_WORKFLOW_TABLES: &str =
            include_str!("sql/postgres/V003__workflow_tables.sql");

        /// V004 — Scheduling tables (schedules, schedule_executions, schedule_instances)
        pub const V004_SCHEDULING_TABLES: &str =
            include_str!("sql/postgres/V004__scheduling_tables.sql");

        /// V005 — Authentication tables (users, sessions, API keys)
        pub const V005_AUTH_TABLES: &str = include_str!("sql/postgres/V005__auth_tables.sql");

        /// V006 — Monitoring tables (notifications, alerts)
        pub const V006_MONITORING_TABLES: &str =
            include_str!("sql/postgres/V006__monitoring_tables.sql");

        /// V007 — Plugin system tables
        pub const V007_PLUGIN_TABLES: &str = include_str!("sql/postgres/V007__plugin_tables.sql");

        /// V008 — Integration tables (webhooks, external integrations)
        pub const V008_INTEGRATION_TABLES: &str =
            include_str!("sql/postgres/V008__integration_tables.sql");

        /// V009 — Cluster tables (nodes, cluster state)
        pub const V009_CLUSTER_TABLES: &str = include_str!("sql/postgres/V009__cluster_tables.sql");

        /// V010 — Queue configuration tables
        pub const V010_QUEUE_TABLES: &str = include_str!("sql/postgres/V010__queue_tables.sql");

        /// V011 — Event store (stream-based event sourcing infrastructure)
        pub const V011_EVENT_STORE: &str = include_str!("sql/postgres/V011__event_store.sql");

        /// V012 — Snapshots (aggregate and projection state snapshots)
        pub const V012_SNAPSHOTS: &str = include_str!("sql/postgres/V012__snapshots.sql");

        /// V013 — Execution log (durable step-level execution tracking)
        pub const V013_EXECUTION_LOG: &str = include_str!("sql/postgres/V013__execution_log.sql");

        /// V014 — Coordination queues (outbox, timer, dead-letter; includes lease columns)
        pub const V014_COORDINATION_QUEUES: &str =
            include_str!("sql/postgres/V014__coordination_queues.sql");

        /// V015 — Child workflow support and execution journal
        pub const V015_CHILD_WORKFLOW_SUPPORT: &str =
            include_str!("sql/postgres/V015__child_workflow_support.sql");

        /// V016 — Actor state storage (virtual object state persistence)
        pub const V016_ACTOR_STATE: &str = include_str!("sql/postgres/V016__actor_state.sql");

        /// V017 — Task executions (lifecycle tracking for workflow tasks)
        pub const V017_TASK_EXECUTIONS: &str =
            include_str!("sql/postgres/V017__task_executions.sql");

        /// V018 — Workers table (SDK worker registry)
        pub const V018_WORKERS_TABLE: &str = include_str!("sql/postgres/V018__workers_table.sql");

        /// V019 — Namespaces (first-class namespace management)
        pub const V019_NAMESPACES: &str = include_str!("sql/postgres/V019__namespaces.sql");

        /// V020 — Eager task claim (eager_claim_expires_at column on task_executions)
        pub const V020_EAGER_TASK_CLAIM: &str =
            include_str!("sql/postgres/V020__eager_task_claim.sql");

        /// V021 — Add 'cancelled' value to execution_log_status enum
        pub const V021_ADD_CANCELLED_EXECUTION_LOG_STATUS: &str =
            include_str!("sql/postgres/V021__add_cancelled_execution_log_status.sql");

        /// V022 — Per-workflow retry policy (retry_policy JSONB on workflow_executions)
        pub const V022_WORKFLOW_RETRY_POLICY: &str =
            include_str!("sql/postgres/V022__workflow_retry_policy.sql");

        /// V023 — Workflow type column on workflow_executions
        pub const V023_WORKFLOW_TYPE_COLUMN: &str =
            include_str!("sql/postgres/V023__workflow_type_column.sql");

        /// V024 — Schedule ↔ workflow execution link + materialized schedule columns
        pub const V024_SCHEDULE_WORKFLOW_LINK: &str =
            include_str!("sql/postgres/V024__schedule_workflow_link.sql");

        /// V025 — Batch operations table (bulk cancel/terminate tracking)
        pub const V025_BATCH_OPERATIONS: &str =
            include_str!("sql/postgres/V025__batch_operations.sql");

        /// V026 — 'restarted_fresh' workflow status (restart-fresh chaining)
        pub const V026_RESTARTED_FRESH_STATUS: &str =
            include_str!("sql/postgres/V026__restarted_fresh_status.sql");

        /// V027 — add shard_id to timer_queue (consistency; fixes timer claim deserialization)
        pub const V027_TIMER_QUEUE_SHARD_ID: &str =
            include_str!("sql/postgres/V027__timer_queue_shard_id.sql");

        /// V028 — cluster membership + shard fencing substrate (ownership + fencing token)
        pub const V028_CLUSTER_MEMBERSHIP: &str =
            include_str!("sql/postgres/V028__cluster_membership.sql");

        /// V029 — drop unused workflow_executions indexes (write-minimization, tier 1)
        pub const V029_DROP_UNUSED_WFEXEC_INDEXES: &str =
            include_str!("sql/postgres/V029__drop_unused_wfexec_indexes.sql");

        /// V030 — drop unused workflow_executions GIN/JSONB indexes (write-minimization, tier 3)
        pub const V030_DROP_UNUSED_WFEXEC_GIN_INDEXES: &str =
            include_str!("sql/postgres/V030__drop_unused_wfexec_gin_indexes.sql");

        /// V031 — matching partitions: delivery-side ready-queue by (namespace, task_queue)
        pub const V031_MATCHING_PARTITIONS: &str =
            include_str!("sql/postgres/V031__matching_partitions.sql");

        /// V032 — concurrency-safe get_next_event_id (per-execution advisory lock)
        pub const V032_CONCURRENCY_SAFE_EVENT_ID: &str =
            include_str!("sql/postgres/V032__concurrency_safe_event_id.sql");

        /// V033 — allow the 'reset' terminal status on workflow_executions
        pub const V033_RESET_STATUS: &str = include_str!("sql/postgres/V033__reset_status.sql");

        /// V034 — bind an execution to the code release that first ran it
        pub const V034_BIND_EXECUTION_VERSION: &str =
            include_str!("sql/postgres/V034__bind_execution_version.sql");

        /// V035 — per-task-queue policy: which release new executions bind to
        pub const V035_TASK_QUEUE_CURRENT_RELEASE: &str =
            include_str!("sql/postgres/V035__task_queue_current_release.sql");

        /// V036 — drop an unusable GIN and two duplicate indexes from outbox_queue
        pub const V036_OUTBOX_INDEX_DIET: &str =
            include_str!("sql/postgres/V036__outbox_index_diet.sql");

        /// V037 — step counts: retried overlaps completed/failed, so it cannot be
        /// summed with them against the total
        pub const V037_STEP_COUNTS_OVERLAP: &str =
            include_str!("sql/postgres/V037__step_counts_overlap.sql");

        /// V038 — a schedule name is unique per organisation, not per
        /// deployment, so one tenant's cron schedule cannot overwrite another's.
        pub const V038_SCHEDULE_NAME_PER_ORGANIZATION: &str =
            include_str!("sql/postgres/V038__schedule_name_per_organization.sql");

        /// V039 — records when a task retry was last handed to the outbox, so a
        /// backlog of retries nobody will run cannot starve the ones that
        /// arrive after them.
        pub const V039_RETRY_DISPATCH_LEASE: &str =
            include_str!("sql/postgres/V039__retry_dispatch_lease.sql");

        /// V040 — a batch operation records the organisation it acts within, so
        /// one cannot match another tenant's workflows through a shared
        /// namespace name.
        pub const V040_BATCH_OPERATIONS_ORGANIZATION: &str =
            include_str!("sql/postgres/V040__batch_operations_organization.sql");

        /// V041 — a task queue's current release is scoped to an organisation,
        /// so one tenant's deploy cannot steer another tenant's new work onto a
        /// release that has no workers.
        pub const V041_TASK_QUEUE_RELEASE_PER_ORGANIZATION: &str =
            include_str!("sql/postgres/V041__task_queue_release_per_organization.sql");

        /// V042 — index task_token, which every task completion and heartbeat
        /// looks up by, so those calls stop scanning the whole table.
        pub const V042_TASK_TOKEN_INDEX: &str =
            include_str!("sql/postgres/V042__task_token_index.sql");

        /// V043 — acting for any organisation is a capability an API key is
        /// granted, not a side effect of the key having no owner.
        pub const V043_API_KEY_ORGANIZATION_DELEGATION: &str =
            include_str!("sql/postgres/V043__api_key_organization_delegation.sql");

        /// V044 — a child's tracking row names the run actually executing it,
        /// so a finished run can be traced back to its parent. Backfills the
        /// ids written before that.
        pub const V044_CHILD_WORKFLOW_TRACKING: &str =
            include_str!("sql/postgres/V044__child_workflow_tracking.sql");

        /// V045 — a failure the retry policy gave up on is marked final, without
        /// overwriting the policy the run was started with.
        pub const V045_WORKFLOW_RETRIES_SETTLED: &str =
            include_str!("sql/postgres/V045__workflow_retries_settled.sql");

        /// V046 — index child tracking by the run it names, and the rows whose
        /// outcome is still to be delivered. Built concurrently.
        pub const V046_CHILD_WORKFLOW_INDEXES: &str =
            include_str!("sql/postgres/V046__child_workflow_indexes.sql");

        /// V047 — index the failed runs the retry scheduler polls for, and the
        /// retries whose successor was not recorded as created. Built
        /// concurrently.
        pub const V047_RETRY_INDEXES: &str = include_str!("sql/postgres/V047__retry_indexes.sql");

        /// V048 — number each activation a claim hands out and record the
        /// latest one applied, so a completion is applied once and only for
        /// the activation that is current. Marks the task rows a workflow
        /// schedules, for V049.
        pub const V048_ACTIVATION_FENCE: &str =
            include_str!("sql/postgres/V048__activation_fence.sql");

        /// V049 — a task row a workflow schedules is unique per execution, task
        /// and attempt, so a repeated ScheduleTask cannot run the task twice.
        /// Built concurrently.
        pub const V049_TASK_SCHEDULE_ONCE: &str =
            include_str!("sql/postgres/V049__task_schedule_once.sql");

        /// V050 — index the first attempts still waiting for their outbox
        /// dispatch, for the sweep that delivers a task an engine stopped
        /// before dispatching. Built concurrently.
        pub const V050_UNDISPATCHED_TASK_SWEEP: &str =
            include_str!("sql/postgres/V050__undispatched_task_sweep.sql");

        /// V051 — record which engine handed each claim out, so a surviving
        /// engine can release the claims of one that died.
        pub const V051_CLAIMED_ON_NODE: &str =
            include_str!("sql/postgres/V051__claimed_on_node.sql");

        /// V052 — index the claims by the engine that handed them out, for
        /// that release. Built concurrently.
        pub const V052_CLAIMED_ON_NODE_INDEX: &str =
            include_str!("sql/postgres/V052__claimed_on_node_index.sql");

        /// V053 — the workers that have said they are shutting down, kept for
        /// a few minutes so every replica refuses them work.
        pub const V053_WORKER_SHUTDOWNS: &str =
            include_str!("sql/postgres/V053__worker_shutdowns.sql");

        /// V054 — the organization a task in a matching partition belongs to,
        /// so a claim takes only its own organization's tasks.
        pub const V054_MATCHING_PARTITION_ORGANIZATION: &str =
            include_str!("sql/postgres/V054__matching_partition_organization.sql");

        /// V055 — tasks, their dispatches and their deliveries stamped with
        /// their workflow's organization, in batches.
        pub const V055_STAMP_TASK_ORGANIZATION: &str =
            include_str!("sql/postgres/V055__stamp_task_organization.sql");

        /// V056 — the claim index with the organization in its key, for polls
        /// made for an organization. Built concurrently.
        pub const V056_WORKFLOW_CLAIM_BY_ORGANIZATION: &str =
            include_str!("sql/postgres/V056__workflow_claim_by_organization.sql");

        /// V057 — the pending dispatches by organization, and the unstamped
        /// ones, for the task claim of a poll made for an organization. Built
        /// concurrently.
        pub const V057_OUTBOX_PENDING_BY_ORGANIZATION: &str =
            include_str!("sql/postgres/V057__outbox_pending_by_organization.sql");

        /// V058 — the heartbeat timeout an attempt runs under when its task
        /// sets none and its worker heartbeats on its own.
        pub const V058_AUTO_HEARTBEAT_TIMEOUT: &str =
            include_str!("sql/postgres/V058__auto_heartbeat_timeout.sql");
        /// V059 — which runs hold their workflow id, and the unique index that
        /// allows one open run per id in an organization's namespace. Built
        /// concurrently.
        pub const V059_WORKFLOW_ID_HOLDS: &str =
            include_str!("sql/postgres/V059__workflow_id_holds.sql");

        /// V060 — the worker process each claim was handed to, and when each
        /// process was last seen polling, so a dead process's claims can be
        /// released.
        pub const V060_WORKER_INSTANCES: &str =
            include_str!("sql/postgres/V060__worker_instances.sql");

        /// V061 — index the claims by the worker process holding them, for
        /// that release. Built concurrently.
        pub const V061_WORKER_INSTANCE_CLAIMS_INDEX: &str =
            include_str!("sql/postgres/V061__worker_instance_claims_index.sql");

        /// V062 — a cancellation of a running workflow is a request it can
        /// clean up after: when it was made, why, and its optional deadline.
        pub const V062_WORKFLOW_CANCEL_REQUESTS: &str =
            include_str!("sql/postgres/V062__workflow_cancel_requests.sql");

        /// V063 — index the cancellation deadlines for the watcher that
        /// enforces them. Built concurrently.
        pub const V063_WORKFLOW_CANCEL_DEADLINE_INDEX: &str =
            include_str!("sql/postgres/V063__workflow_cancel_deadline_index.sql");

        /// V064 — an ended run carries the time it ended: give runs cancelled
        /// before cancellation recorded it their cancellation time.
        pub const V064_ENDED_RUNS_CARRY_COMPLETED_AT: &str =
            include_str!("sql/postgres/V064__ended_runs_carry_completed_at.sql");

        /// V065 — the run timeout and the activation (task) timeout a
        /// workflow was started with, so that both can be enforced.
        pub const V065_WORKFLOW_RUN_AND_TASK_TIMEOUTS: &str =
            include_str!("sql/postgres/V065__workflow_run_and_task_timeouts.sql");

        /// V066 — find a workflow id's latest run in one index probe, in
        /// place of the single-column workflow_id index. Built concurrently.
        pub const V066_RUN_LOOKUP_BY_WORKFLOW_ID: &str =
            include_str!("sql/postgres/V066__run_lookup_by_workflow_id.sql");

        /// V067 — a run timeout is retried under the workflow's retry policy,
        /// and the execution timeout is one deadline every run shares.
        pub const V067_RUN_TIMEOUT_RETRY_AND_EXECUTION_DEADLINE: &str =
            include_str!("sql/postgres/V067__run_timeout_retry_and_execution_deadline.sql");

        /// V068 — an organization's pending dispatches by queue, oldest first,
        /// in place of V057's two indexes. Built concurrently.
        pub const V068_OUTBOX_PENDING_DISPATCH_BY_QUEUE: &str =
            include_str!("sql/postgres/V068__outbox_pending_dispatch_by_queue.sql");

        /// V069 — a start's annotations, its request id (unique per
        /// organization and namespace), and its delay: when its run is due,
        /// and whether it is still waiting, which the claim indexes are
        /// rebuilt to leave out. Built concurrently.
        pub const V069_START_DELAY_ANNOTATIONS_AND_REQUEST_ID: &str =
            include_str!("sql/postgres/V069__start_delay_annotations_and_request_id.sql");

        /// V070 — a run's step-id scheme and history lineage, bound at its
        /// first activation, and whether a worker reporting no schemes has
        /// activated it.
        pub const V070_STEP_ID_SCHEME_AND_HISTORY_LINEAGE: &str =
            include_str!("sql/postgres/V070__step_id_scheme_and_history_lineage.sql");

        /// V071 — drops the plugin tables V007 created, which nothing ever
        /// used, and V008's link to them from webhook configurations.
        pub const V071_DROP_PLUGIN_TABLES: &str =
            include_str!("sql/postgres/V071__drop_plugin_tables.sql");

        /// V072 — drops the tables of the first cluster model V009 created,
        /// replaced by V028's members and shard leases and never used.
        pub const V072_DROP_SUPERSEDED_CLUSTER_TABLES: &str =
            include_str!("sql/postgres/V072__drop_superseded_cluster_tables.sql");

        /// V073 — API keys the engine issues: scopes, an owner that need not be
        /// an account, the issuer, and revocation that keeps the row.
        pub const V073_API_KEY_SCOPES_AND_REVOCATION: &str =
            include_str!("sql/postgres/V073__api_key_scopes_and_revocation.sql");

        /// V074 — find an API key by its hash, and an organisation's keys, by
        /// index; one row per key hash.
        pub const V074_API_KEY_LOOKUP_INDEXES: &str =
            include_str!("sql/postgres/V074__api_key_lookup_indexes.sql");

        /// V075 — drop the event gates table, which nothing ever wrote; events
        /// are delivered through the journal.
        pub const V075_DROP_EVENT_GATES: &str =
            include_str!("sql/postgres/V075__drop_event_gates.sql");

        /// V076 — a timer may be recorded after it falls due: drop the CHECK
        /// that refused it, and with it the activation that scheduled it.
        pub const V076_TIMERS_MAY_BE_RECORDED_AFTER_THEY_FALL_DUE: &str =
            include_str!("sql/postgres/V076__timers_may_be_recorded_after_they_fall_due.sql");

        /// V077 — answer a task's completion checks (an earlier TaskCompleted,
        /// any unfinished task) by index, at a cost independent of the run's
        /// length.
        pub const V077_JOURNAL_TASK_COMPLETED_LOOKUP: &str =
            include_str!("sql/postgres/V077__journal_task_completed_lookup.sql");

        /// V078 — a run's step results are StepCompleted entries of its
        /// journal, flagged per run; and the index that keeps each step's
        /// entry single.
        pub const V078_STEP_RESULTS_IN_THE_JOURNAL: &str =
            include_str!("sql/postgres/V078__step_results_in_the_journal.sql");

        /// V079 — drops the integration and webhook tables V008 created,
        /// which nothing ever wrote or read.
        pub const V079_DROP_INTEGRATION_TABLES: &str =
            include_str!("sql/postgres/V079__drop_integration_tables.sql");

        /// V080 — drops the alert and notification tables V006 created,
        /// which nothing ever wrote or read.
        pub const V080_DROP_ALERT_AND_NOTIFICATION_TABLES: &str =
            include_str!("sql/postgres/V080__drop_alert_and_notification_tables.sql");

        /// V081 — drops the message queue V010 created in the database,
        /// which nothing ever used.
        pub const V081_DROP_QUEUE_TABLES: &str =
            include_str!("sql/postgres/V081__drop_queue_tables.sql");

        /// V082 — drops views, tables and functions of early migrations that
        /// nothing uses.
        pub const V082_DROP_UNUSED_VIEWS_TABLES_AND_FUNCTIONS: &str =
            include_str!("sql/postgres/V082__drop_unused_views_tables_and_functions.sql");

        /// V083 — drops the event store the engine never wrote to (V011,
        /// V012), the idempotency records nothing used (V013), and functions
        /// nothing calls.
        pub const V083_DROP_EVENT_STORE_AND_IDEMPOTENCY_TABLES: &str =
            include_str!("sql/postgres/V083__drop_event_store_and_idempotency_tables.sql");

        /// V084 — drops the trigger that sent a failed run's error with
        /// pg_notify, which refused errors over 8000 bytes and with them the
        /// update marking the run failed.
        pub const V084_DROP_WORKFLOW_FAILED_NOTIFY: &str =
            include_str!("sql/postgres/V084__drop_workflow_failed_notify.sql");

        /// V085 — drops the engine tables' foreign keys to `organizations` and
        /// `users`: an organization id is a tenancy key the engine never looks
        /// up.
        pub const V085_ORGANIZATION_IDS_ARE_OPAQUE: &str =
            include_str!("sql/postgres/V085__organization_ids_are_opaque.sql");

        /// V086 — `workflow_executions.preferred_until`: until then, a run
        /// released to resume may be claimed only by the worker process that
        /// keeps its journal.
        pub const V086_JOURNAL_HOLDER_PREFERENCE: &str =
            include_str!("sql/postgres/V086__journal_holder_preference.sql");
    }
}

impl fmt::Display for MigrationVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "V{:03}", self.0)
    }
}

impl Migration {
    /// Create a new migration instance
    pub fn new(version: u32, name: &str, sql: &'static str) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(sql.as_bytes());
        let checksum = format!("{:x}", hasher.finalize());
        Self {
            version: MigrationVersion(version),
            name: name.to_string(),
            sql,
            checksum,
        }
    }
}

/// Schema version management
pub mod versioning;

/// Migration execution engine
pub mod runner;

/// Migration rollback capabilities
pub mod rollback;

/// Migration validation and testing
pub mod validation;

/// Migration generation tools
pub mod generators;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_migration_v001_loads() {
        let m = sql::postgres::V001_INITIAL_SCHEMA;
        assert!(!m.is_empty());
        assert!(m.contains("CREATE TABLE worker_heartbeats"));
        assert!(m.contains("CREATE TABLE system_metrics"));
    }

    #[test]
    fn test_migration_v002_loads() {
        let m = sql::postgres::V002_ORGANIZATION_TABLES;
        assert!(!m.is_empty());
        assert!(m.contains("CREATE TABLE organizations"));
        assert!(m.contains("CREATE TABLE organization_members"));
    }

    #[test]
    fn test_migration_v003_loads() {
        let m = sql::postgres::V003_WORKFLOW_TABLES;
        assert!(!m.is_empty());
        assert!(m.contains("CREATE TABLE workflow_executions"));
        assert!(m.contains("claimed_by"));
        assert!(m.contains("pending_requeue"));
        assert!(m.contains("terminated"));
    }

    #[test]
    fn test_migration_v004_loads() {
        let m = sql::postgres::V004_SCHEDULING_TABLES;
        assert!(!m.is_empty());
        assert!(m.contains("CREATE TABLE schedules"));
    }

    #[test]
    fn test_migration_v005_loads() {
        let m = sql::postgres::V005_AUTH_TABLES;
        assert!(!m.is_empty());
        assert!(m.contains("CREATE TABLE users"));
    }

    #[test]
    fn test_migration_v006_loads() {
        let m = sql::postgres::V006_MONITORING_TABLES;
        assert!(!m.is_empty());
        assert!(m.contains("notifications") || m.contains("monitoring"));
    }

    #[test]
    fn test_migration_v007_loads() {
        let m = sql::postgres::V007_PLUGIN_TABLES;
        assert!(!m.is_empty());
        assert!(m.contains("CREATE TABLE plugins"));
    }

    #[test]
    fn test_migration_v008_loads() {
        let m = sql::postgres::V008_INTEGRATION_TABLES;
        assert!(!m.is_empty());
        assert!(m.contains("integrations"));
    }

    #[test]
    fn test_migration_v009_loads() {
        let m = sql::postgres::V009_CLUSTER_TABLES;
        assert!(!m.is_empty());
        assert!(m.contains("CREATE TABLE clusters"));
    }

    #[test]
    fn test_migration_v010_loads() {
        let m = sql::postgres::V010_QUEUE_TABLES;
        assert!(!m.is_empty());
        assert!(m.contains("queue"));
    }

    #[test]
    fn test_migration_v011_loads() {
        let m = sql::postgres::V011_EVENT_STORE;
        assert!(!m.is_empty());
        assert!(m.contains("CREATE TABLE events"));
    }

    #[test]
    fn test_migration_v012_loads() {
        let m = sql::postgres::V012_SNAPSHOTS;
        assert!(!m.is_empty());
        assert!(m.contains("snapshot"));
    }

    #[test]
    fn test_migration_v013_loads() {
        let m = sql::postgres::V013_EXECUTION_LOG;
        assert!(!m.is_empty());
        assert!(m.contains("execution_log"));
        assert!(m.contains("execution_idempotency"));
        assert!(m.contains("event_gates"));
    }

    #[test]
    fn test_migration_v014_loads() {
        let m = sql::postgres::V014_COORDINATION_QUEUES;
        assert!(!m.is_empty());
        assert!(m.contains("CREATE TABLE outbox_queue"));
        assert!(m.contains("lease_expires_at"));
    }

    #[test]
    fn test_migration_v015_loads() {
        let m = sql::postgres::V015_CHILD_WORKFLOW_SUPPORT;
        assert!(!m.is_empty());
        assert!(m.contains("child_workflow_executions"));
        assert!(m.contains("workflow_execution_journal"));
    }

    #[test]
    fn test_migration_v016_loads() {
        let m = sql::postgres::V016_ACTOR_STATE;
        assert!(!m.is_empty());
        assert!(m.contains("CREATE TABLE") && m.contains("actor_state"));
    }

    #[test]
    fn test_migration_v017_loads() {
        let m = sql::postgres::V017_TASK_EXECUTIONS;
        assert!(!m.is_empty());
        assert!(m.contains("CREATE TABLE task_executions"));
    }

    #[test]
    fn test_migration_v018_loads() {
        let m = sql::postgres::V018_WORKERS_TABLE;
        assert!(!m.is_empty());
        assert!(m.contains("CREATE TABLE") && m.contains("workers"));
    }

    #[test]
    fn test_migration_v019_loads() {
        let m = sql::postgres::V019_NAMESPACES;
        assert!(!m.is_empty());
        assert!(m.contains("CREATE TABLE namespaces"));
    }

    #[test]
    fn test_migration_v020_loads() {
        let m = sql::postgres::V020_EAGER_TASK_CLAIM;
        assert!(!m.is_empty());
        assert!(m.contains("eager_claim_expires_at"));
    }

    #[test]
    fn test_migration_v021_loads() {
        let m = sql::postgres::V021_ADD_CANCELLED_EXECUTION_LOG_STATUS;
        assert!(!m.is_empty());
        assert!(m.contains("execution_log_status"));
        assert!(m.contains("cancelled"));
    }

    #[test]
    fn test_migration_v022_loads() {
        let m = sql::postgres::V022_WORKFLOW_RETRY_POLICY;
        assert!(!m.is_empty());
        assert!(m.contains("retry_policy"));
    }

    #[test]
    fn test_migration_v023_loads() {
        let m = sql::postgres::V023_WORKFLOW_TYPE_COLUMN;
        assert!(!m.is_empty());
        assert!(m.contains("workflow_type"));
    }

    #[test]
    fn test_migration_v024_loads() {
        let m = sql::postgres::V024_SCHEDULE_WORKFLOW_LINK;
        assert!(!m.is_empty());
        assert!(m.contains("workflow_execution_id"));
    }

    #[test]
    fn test_migration_v025_loads() {
        let m = sql::postgres::V025_BATCH_OPERATIONS;
        assert!(!m.is_empty());
        assert!(m.contains("CREATE TABLE batch_operations"));
    }

    #[test]
    fn test_all_migrations_have_content() {
        assert!(sql::postgres::V001_INITIAL_SCHEMA.len() > 100);
        assert!(sql::postgres::V002_ORGANIZATION_TABLES.len() > 100);
        assert!(sql::postgres::V003_WORKFLOW_TABLES.len() > 100);
        assert!(sql::postgres::V004_SCHEDULING_TABLES.len() > 100);
        assert!(sql::postgres::V005_AUTH_TABLES.len() > 100);
        assert!(sql::postgres::V006_MONITORING_TABLES.len() > 100);
        assert!(sql::postgres::V007_PLUGIN_TABLES.len() > 100);
        assert!(sql::postgres::V008_INTEGRATION_TABLES.len() > 100);
        assert!(sql::postgres::V009_CLUSTER_TABLES.len() > 100);
        assert!(sql::postgres::V010_QUEUE_TABLES.len() > 100);
        assert!(sql::postgres::V011_EVENT_STORE.len() > 100);
        assert!(sql::postgres::V012_SNAPSHOTS.len() > 100);
        assert!(sql::postgres::V013_EXECUTION_LOG.len() > 100);
        assert!(sql::postgres::V014_COORDINATION_QUEUES.len() > 100);
        assert!(sql::postgres::V015_CHILD_WORKFLOW_SUPPORT.len() > 100);
        assert!(sql::postgres::V016_ACTOR_STATE.len() > 100);
        assert!(sql::postgres::V017_TASK_EXECUTIONS.len() > 100);
        assert!(sql::postgres::V018_WORKERS_TABLE.len() > 100);
        assert!(sql::postgres::V019_NAMESPACES.len() > 100);
        assert!(sql::postgres::V020_EAGER_TASK_CLAIM.len() > 100);
        assert!(sql::postgres::V021_ADD_CANCELLED_EXECUTION_LOG_STATUS.len() > 100);
        assert!(sql::postgres::V022_WORKFLOW_RETRY_POLICY.len() > 100);
        assert!(sql::postgres::V023_WORKFLOW_TYPE_COLUMN.len() > 100);
        assert!(sql::postgres::V024_SCHEDULE_WORKFLOW_LINK.len() > 100);
        assert!(sql::postgres::V025_BATCH_OPERATIONS.len() > 100);
        assert!(sql::postgres::V026_RESTARTED_FRESH_STATUS.len() > 100);
        assert!(sql::postgres::V027_TIMER_QUEUE_SHARD_ID.len() > 100);
        assert!(sql::postgres::V028_CLUSTER_MEMBERSHIP.len() > 100);
        assert!(sql::postgres::V029_DROP_UNUSED_WFEXEC_INDEXES.len() > 100);
        assert!(sql::postgres::V030_DROP_UNUSED_WFEXEC_GIN_INDEXES.len() > 100);
        assert!(sql::postgres::V031_MATCHING_PARTITIONS.len() > 100);
    }

    #[test]
    fn test_migration_version_display() {
        assert_eq!(MigrationVersion(1).to_string(), "V001");
        assert_eq!(MigrationVersion(11).to_string(), "V011");
        assert_eq!(MigrationVersion(19).to_string(), "V019");
    }

    #[test]
    fn test_migration_creation() {
        let migration = Migration::new(1, "initial_schema", "CREATE TABLE test;");
        assert_eq!(migration.version.0, 1);
        assert_eq!(migration.name, "initial_schema");
        assert_eq!(migration.sql, "CREATE TABLE test;");
        assert!(!migration.checksum.is_empty());
        assert_eq!(migration.checksum.len(), 64); // SHA256 hex
    }

    #[test]
    fn test_migration_checksum_consistency() {
        let m1 = Migration::new(1, "test", "CREATE TABLE test;");
        let m2 = Migration::new(1, "test", "CREATE TABLE test;");
        assert_eq!(m1.checksum, m2.checksum);
    }
}
