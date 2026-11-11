//! Data retention and cleanup job.

use jiff::SignedDuration;
use orcher_common::types::time::Timestamptz;
use sqlx::PgPool;
use std::time::Instant;
use tracing::{info, warn};

use crate::jobs::RetentionDays;

/// Metric names this job emits. See the module docs in `jobs/mod.rs` for why
/// these are constants rather than string literals at the call site.
pub(super) const METRIC_RUNS: &str = "jobs_data_retention_runs_total";
pub(super) const METRIC_DURATION: &str = "jobs_data_retention_duration_seconds";
pub(super) const METRIC_ROWS_DELETED: &str = "jobs_data_retention_rows_deleted_total";
pub(super) const METRIC_ERRORS: &str = "jobs_data_retention_errors_total";

/// Run data retention cleanup
///
/// Deletes old data from various tables based on the configured retention periods.
/// Each cleanup operation is independent - if one fails, others will still run.
///
/// # Arguments
///
/// * `pool` - Database connection pool
/// * `config` - Retention periods for different data types
///
/// # Returns
///
/// `Ok(())` on success, or an error if any critical cleanup fails
///
/// # Example
///
/// ```rust,ignore
/// use orcher_core::jobs::data_retention;
///
/// let config = RetentionDays::default();
/// data_retention::run(&pool, &config).await?;
/// ```
pub async fn run(pool: &PgPool, config: &RetentionDays) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();

    info!("Starting data retention cleanup");

    let mut total_deleted = 0;
    let mut errors = Vec::new();

    // Clean up old workflow executions
    match cleanup_workflow_executions(pool, config.workflow_executions).await {
        Ok(deleted) => {
            total_deleted += deleted;
        }
        Err(e) => {
            warn!("Failed to cleanup workflow executions: {}", e);
            errors.push(("workflow_executions", e.to_string()));
        }
    }

    // Clean up old execution logs
    match cleanup_execution_logs(pool, config.execution_logs).await {
        Ok(deleted) => {
            total_deleted += deleted;
        }
        Err(e) => {
            warn!("Failed to cleanup execution logs: {}", e);
            errors.push(("execution_logs", e.to_string()));
        }
    }

    // Clean up old journal entries
    match cleanup_journal_entries(pool, config.journal_entries).await {
        Ok(deleted) => {
            total_deleted += deleted;
        }
        Err(e) => {
            warn!("Failed to cleanup journal entries: {}", e);
            errors.push(("journal_entries", e.to_string()));
        }
    }

    // Clean up old task executions
    match cleanup_task_executions(pool, config.task_executions).await {
        Ok(deleted) => {
            total_deleted += deleted;
        }
        Err(e) => {
            warn!("Failed to cleanup task executions: {}", e);
            errors.push(("task_executions", e.to_string()));
        }
    }

    // Clean up completed outbox tasks
    match cleanup_outbox_queue(pool, config.outbox_queue).await {
        Ok(deleted) => {
            total_deleted += deleted;
        }
        Err(e) => {
            warn!("Failed to cleanup outbox queue: {}", e);
            errors.push(("outbox_queue", e.to_string()));
        }
    }

    // Clean up matching rows no poll will come back for
    match cleanup_matching_partitions(pool, config.outbox_queue).await {
        Ok(deleted) => {
            total_deleted += deleted;
        }
        Err(e) => {
            warn!("Failed to cleanup matching partitions: {}", e);
            errors.push(("matching_partitions", e.to_string()));
        }
    }

    let duration = start.elapsed();

    if errors.is_empty() {
        info!(
            "Data retention completed successfully: {} total rows deleted in {:?}",
            total_deleted, duration
        );
    } else {
        warn!(
            "Data retention completed with {} errors: {} rows deleted in {:?}",
            errors.len(),
            total_deleted,
            duration
        );
        for (table, error) in &errors {
            warn!("  - {}: {}", table, error);
        }
    }

    // Emit metrics about the job
    metrics::counter!(METRIC_RUNS).increment(1);
    metrics::histogram!(METRIC_DURATION).record(duration.as_secs_f64());
    metrics::gauge!(METRIC_ROWS_DELETED).set(total_deleted as f64);
    metrics::gauge!(METRIC_ERRORS).set(errors.len() as f64);

    Ok(())
}

/// Clean up old workflow executions
///
/// Deletes workflow executions that:
/// - Have completed_at timestamp older than retention period
/// - Are in terminal state (completed, failed, cancelled)
///
/// Running workflows are never deleted.
async fn cleanup_workflow_executions(pool: &PgPool, days: i32) -> Result<u64, sqlx::Error> {
    let cutoff = Timestamptz::now() - SignedDuration::from_hours(days as i64 * 24);

    let result = sqlx::query(
        r#"
        DELETE FROM workflow_executions
        WHERE completed_at < $1
          AND status IN ('completed', 'failed', 'cancelled')
        "#,
    )
    .bind(cutoff)
    .execute(pool)
    .await?;

    let deleted = result.rows_affected();
    if deleted > 0 {
        info!(
            "Deleted {} workflow executions older than {} days",
            deleted, days
        );
    } else {
        info!(
            "No workflow executions to delete (retention: {} days)",
            days
        );
    }

    Ok(deleted)
}

/// Clean up old execution logs
///
/// Deletes workflow execution logs older than the retention period.
/// These are user-facing logs that can grow very large.
async fn cleanup_execution_logs(pool: &PgPool, days: i32) -> Result<u64, sqlx::Error> {
    let cutoff = Timestamptz::now() - SignedDuration::from_hours(days as i64 * 24);

    let result = sqlx::query(
        r#"
        DELETE FROM workflow_execution_logs
        WHERE created_at < $1
        "#,
    )
    .bind(cutoff)
    .execute(pool)
    .await?;

    let deleted = result.rows_affected();
    if deleted > 0 {
        info!(
            "Deleted {} execution log entries older than {} days",
            deleted, days
        );
    } else {
        info!("No execution logs to delete (retention: {} days)", days);
    }

    Ok(deleted)
}

/// Clean up old journal entries
///
/// Deletes workflow execution journal entries older than the retention period.
/// Journal entries are kept longer than logs for audit purposes.
async fn cleanup_journal_entries(pool: &PgPool, days: i32) -> Result<u64, sqlx::Error> {
    let cutoff = Timestamptz::now() - SignedDuration::from_hours(days as i64 * 24);

    let result = sqlx::query(
        r#"
        DELETE FROM workflow_execution_journal
        WHERE created_at < $1
        "#,
    )
    .bind(cutoff)
    .execute(pool)
    .await?;

    let deleted = result.rows_affected();
    if deleted > 0 {
        info!(
            "Deleted {} journal entries older than {} days",
            deleted, days
        );
    } else {
        info!("No journal entries to delete (retention: {} days)", days);
    }

    Ok(deleted)
}

/// Clean up old task executions
///
/// Deletes task executions that:
/// - Have completed_at timestamp older than retention period
/// - Are in terminal state (completed, failed)
async fn cleanup_task_executions(pool: &PgPool, days: i32) -> Result<u64, sqlx::Error> {
    let cutoff = Timestamptz::now() - SignedDuration::from_hours(days as i64 * 24);

    let result = sqlx::query(
        r#"
        DELETE FROM task_executions
        WHERE completed_at < $1
          AND status IN ('completed', 'failed')
        "#,
    )
    .bind(cutoff)
    .execute(pool)
    .await?;

    let deleted = result.rows_affected();
    if deleted > 0 {
        info!(
            "Deleted {} task executions older than {} days",
            deleted, days
        );
    } else {
        info!("No task executions to delete (retention: {} days)", days);
    }

    Ok(deleted)
}

/// Clean up old outbox queue tasks
///
/// Deletes completed outbox tasks older than the retention period.
/// Only deletes tasks with status = 'completed' to avoid deleting
/// pending or processing tasks.
async fn cleanup_outbox_queue(pool: &PgPool, days: i32) -> Result<u64, sqlx::Error> {
    let cutoff = Timestamptz::now() - SignedDuration::from_hours(days as i64 * 24);

    let result = sqlx::query(
        r#"
        DELETE FROM outbox_queue
        WHERE completed_at < $1
          AND status = 'completed'
        "#,
    )
    .bind(cutoff)
    .execute(pool)
    .await?;

    let deleted = result.rows_affected();
    if deleted > 0 {
        info!(
            "Deleted {} outbox queue tasks older than {} days",
            deleted, days
        );
    } else {
        info!("No outbox queue tasks to delete (retention: {} days)", days);
    }

    Ok(deleted)
}

/// Clean up matching partition rows left claimed long past their lease
///
/// A row is deleted as soon as its task is taken, and one whose lease runs out
/// first is returned to its partition by the polling reclaimer. A row still
/// claimed a whole retention period after its lease ended is one neither
/// reached — left by an engine that stopped delivering from the matching layer
/// — and would otherwise stay for ever. Rows still waiting to be claimed are
/// never deleted here: each is a task not yet delivered.
async fn cleanup_matching_partitions(pool: &PgPool, days: i32) -> Result<u64, sqlx::Error> {
    let cutoff = Timestamptz::now() - SignedDuration::from_hours(days as i64 * 24);

    let result = sqlx::query(
        r#"
        DELETE FROM matching_partitions
        WHERE status = 'claimed'
          AND lease_expires_at < $1
        "#,
    )
    .bind(cutoff)
    .execute(pool)
    .await?;

    let deleted = result.rows_affected();
    if deleted > 0 {
        info!(
            "Deleted {} matching rows whose lease ended over {} days ago",
            deleted, days
        );
    }

    Ok(deleted)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retention_config_values() {
        let config = RetentionDays::default();

        // Verify default values are reasonable
        assert!(config.workflow_executions > 0);
        assert!(config.execution_logs > 0);
        assert!(config.journal_entries > 0);
        assert!(config.task_executions > 0);
        assert!(config.outbox_queue > 0);

        // Journal should be kept longer than logs for audit
        assert!(config.journal_entries >= config.execution_logs);
    }
}
