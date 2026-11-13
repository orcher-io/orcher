//! The columns each repository reads back, named rather than `*`.
//!
//! A statement that selects or returns `*` has its result shape fixed when
//! it is prepared, and sqlx keeps prepared statements per connection. When a
//! migration adds a column, Postgres refuses every later execution of such a
//! statement on a connection that prepared it before ("cached plan must not
//! change result type", SQLSTATE 0A000) — which during a rolling upgrade is
//! every connection of every engine still on the old build. A named list is
//! the same before and after an additive migration, so the old build keeps
//! working while the new one rolls out.
//!
//! Each macro expands to a string literal, for `concat!` (and so for
//! `format!` through it). A column added by a migration is not read until it
//! is added here, which is the point: add it in the release whose row type
//! reads it. `scripts/ci.sh` refuses a `SELECT *` or `RETURNING *` in the
//! storage crate's SQL.

/// `workflow_executions`.
macro_rules! workflow_executions {
    () => {
        concat!(
            "id, execution_id, workflow_id, registry_id, workflow_name, workflow_version, ",
            "namespace, status, parameters, result, error, error_details, context, ",
            "environment, progress, current_step, statistics, total_steps, completed_steps, ",
            "failed_steps, retried_steps, duration_ms, cpu_time_ms, memory_peak_bytes, ",
            "average_step_duration_ms, idempotency_key, priority, timeout_secs, retry_count, ",
            "max_retries, tags, labels, trigger_type, triggered_by, parent_execution_id, ",
            "root_execution_id, user_id, organization_id, shard_id, claimed_by, claimed_at, ",
            "claim_timeout_at, task_queue, pending_requeue, pending_requeue_at, submitted_at, ",
            "started_at, completed_at, cancelled_at, last_retry_at, next_retry_at, ",
            "created_at, updated_at, retry_policy, workflow_type, bound_version_id, ",
            "retries_settled_at, activation_seq, completed_activation, claimed_on_node, ",
            "holds_workflow_id, claimed_by_instance, run_timeout_secs, task_timeout_secs, ",
            "execution_deadline, annotations, start_request_id, start_after, awaiting_start, ",
            "step_id_scheme, history_lineage, activated_without_scheme, step_results_journaled, ",
            "claimed_through_entry, observed_through_entry",
        )
    };
    ($p:literal) => {
        concat!(
            $p,
            ".id, ",
            $p,
            ".execution_id, ",
            $p,
            ".workflow_id, ",
            $p,
            ".registry_id, ",
            $p,
            ".workflow_name, ",
            $p,
            ".workflow_version, ",
            $p,
            ".namespace, ",
            $p,
            ".status, ",
            $p,
            ".parameters, ",
            $p,
            ".result, ",
            $p,
            ".error, ",
            $p,
            ".error_details, ",
            $p,
            ".context, ",
            $p,
            ".environment, ",
            $p,
            ".progress, ",
            $p,
            ".current_step, ",
            $p,
            ".statistics, ",
            $p,
            ".total_steps, ",
            $p,
            ".completed_steps, ",
            $p,
            ".failed_steps, ",
            $p,
            ".retried_steps, ",
            $p,
            ".duration_ms, ",
            $p,
            ".cpu_time_ms, ",
            $p,
            ".memory_peak_bytes, ",
            $p,
            ".average_step_duration_ms, ",
            $p,
            ".idempotency_key, ",
            $p,
            ".priority, ",
            $p,
            ".timeout_secs, ",
            $p,
            ".retry_count, ",
            $p,
            ".max_retries, ",
            $p,
            ".tags, ",
            $p,
            ".labels, ",
            $p,
            ".trigger_type, ",
            $p,
            ".triggered_by, ",
            $p,
            ".parent_execution_id, ",
            $p,
            ".root_execution_id, ",
            $p,
            ".user_id, ",
            $p,
            ".organization_id, ",
            $p,
            ".shard_id, ",
            $p,
            ".claimed_by, ",
            $p,
            ".claimed_at, ",
            $p,
            ".claim_timeout_at, ",
            $p,
            ".task_queue, ",
            $p,
            ".pending_requeue, ",
            $p,
            ".pending_requeue_at, ",
            $p,
            ".submitted_at, ",
            $p,
            ".started_at, ",
            $p,
            ".completed_at, ",
            $p,
            ".cancelled_at, ",
            $p,
            ".last_retry_at, ",
            $p,
            ".next_retry_at, ",
            $p,
            ".created_at, ",
            $p,
            ".updated_at, ",
            $p,
            ".retry_policy, ",
            $p,
            ".workflow_type, ",
            $p,
            ".bound_version_id, ",
            $p,
            ".retries_settled_at, ",
            $p,
            ".activation_seq, ",
            $p,
            ".completed_activation, ",
            $p,
            ".claimed_on_node, ",
            $p,
            ".holds_workflow_id, ",
            $p,
            ".claimed_by_instance, ",
            $p,
            ".run_timeout_secs, ",
            $p,
            ".task_timeout_secs, ",
            $p,
            ".execution_deadline, ",
            $p,
            ".annotations, ",
            $p,
            ".start_request_id, ",
            $p,
            ".start_after, ",
            $p,
            ".awaiting_start, ",
            $p,
            ".step_id_scheme, ",
            $p,
            ".history_lineage, ",
            $p,
            ".activated_without_scheme, ",
            $p,
            ".step_results_journaled, ",
            $p,
            ".claimed_through_entry, ",
            $p,
            ".observed_through_entry",
        )
    };
}
pub(crate) use workflow_executions;

/// `workflow_execution_journal`.
macro_rules! workflow_execution_journal {
    () => {
        concat!(
            "id, workflow_id, execution_id, event_id, event_type, timestamp, payload, ",
            "version, task_id, organization_id, shard_id, created_at",
        )
    };
}
pub(crate) use workflow_execution_journal;

/// `workflow_execution_logs`.
macro_rules! workflow_execution_logs {
    () => {
        concat!(
            "id, execution_id, timestamp, level, message, step, task_id, worker_id, metadata, ",
            "source, organization_id, created_at",
        )
    };
}
pub(crate) use workflow_execution_logs;

/// `outbox_queue`.
macro_rules! outbox_queue {
    () => {
        concat!(
            "task_id, workflow_id, task_type, payload, idempotency_key, status, created_at, ",
            "processing_started_at, completed_at, attempt_count, max_retries, next_retry_at, ",
            "backoff_multiplier, last_error, error_details, lease_expires_at, lease_owner, ",
            "organization_id, shard_id",
        )
    };
    ($p:literal) => {
        concat!(
            $p,
            ".task_id, ",
            $p,
            ".workflow_id, ",
            $p,
            ".task_type, ",
            $p,
            ".payload, ",
            $p,
            ".idempotency_key, ",
            $p,
            ".status, ",
            $p,
            ".created_at, ",
            $p,
            ".processing_started_at, ",
            $p,
            ".completed_at, ",
            $p,
            ".attempt_count, ",
            $p,
            ".max_retries, ",
            $p,
            ".next_retry_at, ",
            $p,
            ".backoff_multiplier, ",
            $p,
            ".last_error, ",
            $p,
            ".error_details, ",
            $p,
            ".lease_expires_at, ",
            $p,
            ".lease_owner, ",
            $p,
            ".organization_id, ",
            $p,
            ".shard_id",
        )
    };
}
pub(crate) use outbox_queue;

/// `timer_queue`.
macro_rules! timer_queue {
    () => {
        concat!(
            "task_id, workflow_id, task_type, payload, fire_at, created_at, status, ",
            "processing_started_at, completed_at, attempt_count, max_retries, last_error, ",
            "organization_id, shard_id",
        )
    };
}
pub(crate) use timer_queue;

/// `dead_letter_queue`.
macro_rules! dead_letter_queue {
    () => {
        concat!(
            "dlq_id, original_task_id, original_task_type, workflow_id, payload, ",
            "attempt_count, last_error, error_details, original_created_at, failed_at, ",
            "processing_history, organization_id",
        )
    };
}
pub(crate) use dead_letter_queue;

/// `child_workflow_executions`.
macro_rules! child_workflow_executions {
    () => {
        concat!(
            "id, parent_workflow_id, parent_execution_id, parent_initiated_event_id, ",
            "parent_started_event_id, child_workflow_id, child_execution_id, ",
            "child_workflow_type, task_queue, input, parent_close_policy, state, result, ",
            "failure_reason, failure_details, initiated_at, started_at, completed_at, ",
            "organization_id, shard_id, created_at, updated_at",
        )
    };
}
pub(crate) use child_workflow_executions;

/// `namespaces`.
macro_rules! namespaces {
    () => {
        concat!(
            "id, name, description, status, owner_email, retention_period_days, data, ",
            "organization_id, created_at, updated_at, deleted_at",
        )
    };
}
pub(crate) use namespaces;
