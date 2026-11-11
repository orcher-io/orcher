//! Metrics aggregation job.

use jiff::SignedDuration;
use orcher_common::types::time::Timestamptz;
use sqlx::PgPool;
use std::time::Instant;
use tracing::{debug, info};

/// Metric names this job emits. See the module docs in `jobs/mod.rs` for why
/// these are constants rather than string literals at the call site.
pub(super) const METRIC_RUNS: &str = "jobs_metrics_aggregation_runs_total";
pub(super) const METRIC_DURATION: &str = "jobs_metrics_aggregation_duration_seconds";
pub(super) const METRIC_TYPES_PROCESSED: &str = "jobs_metrics_aggregation_workflow_types_processed";

/// Run metrics aggregation for the last complete hour
///
/// This function:
/// 1. Calculates the time window (last complete hour)
/// 2. Queries workflow_executions for all completed workflows in that window
/// 3. Aggregates metrics (counts, durations, percentiles)
/// 4. Inserts/updates the workflow_execution_metrics table
///
/// # Arguments
///
/// * `pool` - Database connection pool
///
/// # Returns
///
/// `Ok(())` on success, or an error if aggregation fails
///
/// # Example
///
/// ```rust,ignore
/// use orcher_core::jobs::metrics_aggregation;
///
/// // Run aggregation
/// metrics_aggregation::run(&pool).await?;
/// ```
pub async fn run(pool: &PgPool) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();

    info!("Starting workflow metrics aggregation");

    // Calculate time window (last complete hour)
    let (start_time, end_time) = calculate_time_window();

    debug!(
        "Aggregating metrics for time window: {} to {}",
        start_time, end_time
    );

    // Perform the aggregation query
    let result = aggregate_metrics(pool, start_time, end_time).await?;

    let rows_affected = result.rows_affected();
    let duration = start.elapsed();

    info!(
        "Metrics aggregation completed: {} workflow types aggregated in {:?}",
        rows_affected, duration
    );

    // Emit metrics about the job itself
    metrics::counter!(METRIC_RUNS).increment(1);
    metrics::histogram!(METRIC_DURATION).record(duration.as_secs_f64());
    metrics::gauge!(METRIC_TYPES_PROCESSED).set(rows_affected as f64);

    Ok(())
}

/// Calculate the time window for aggregation (last complete hour)
///
/// For example:
/// - If current time is 14:23, returns (13:00, 14:00)
/// - If current time is 09:05, returns (08:00, 09:00)
///
/// This ensures we only aggregate complete hours of data.
fn calculate_time_window() -> (Timestamptz, Timestamptz) {
    let now = Timestamptz::now();

    // Round down to the start of the current hour by truncating minutes/seconds
    let secs = now.timestamp();
    let current_hour_start =
        Timestamptz::from_second(secs - (secs % 3600)).expect("valid hour-aligned timestamp");

    // Last complete hour ends at current hour start
    let end_time = current_hour_start;

    // Last complete hour starts 1 hour before
    let start_time = end_time - SignedDuration::from_hours(1);

    (start_time, end_time)
}

/// Perform the actual aggregation query
///
/// This query:
/// 1. Groups workflow executions by namespace, name, version, date, and hour
/// 2. Calculates aggregate statistics (counts, averages, percentiles)
/// 3. Inserts or updates the workflow_execution_metrics table
///
/// The query uses PostgreSQL's PERCENTILE_CONT for accurate percentile calculations.
async fn aggregate_metrics(
    pool: &PgPool,
    start_time: Timestamptz,
    end_time: Timestamptz,
) -> Result<sqlx::postgres::PgQueryResult, sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO workflow_execution_metrics (
            namespace,
            workflow_name,
            workflow_version,
            metric_date,
            hour_of_day,
            total_executions,
            completed_executions,
            failed_executions,
            cancelled_executions,
            timed_out_executions,
            avg_duration_ms,
            min_duration_ms,
            max_duration_ms,
            p50_duration_ms,
            p95_duration_ms,
            p99_duration_ms,
            avg_total_steps,
            avg_failed_steps,
            avg_retried_steps
        )
        SELECT
            namespace,
            workflow_name,
            workflow_version,
            DATE(completed_at AT TIME ZONE 'UTC') as metric_date,
            EXTRACT(HOUR FROM completed_at AT TIME ZONE 'UTC')::INTEGER as hour_of_day,
            COUNT(*) as total_executions,
            COUNT(*) FILTER (WHERE status = 'completed') as completed_executions,
            COUNT(*) FILTER (WHERE status = 'failed') as failed_executions,
            COUNT(*) FILTER (WHERE status = 'cancelled') as cancelled_executions,
            COUNT(*) FILTER (WHERE status = 'timed_out') as timed_out_executions,
            ROUND(AVG(duration_ms)) as avg_duration_ms,
            MIN(duration_ms) as min_duration_ms,
            MAX(duration_ms) as max_duration_ms,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY duration_ms)::BIGINT as p50_duration_ms,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms)::BIGINT as p95_duration_ms,
            PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY duration_ms)::BIGINT as p99_duration_ms,
            ROUND(AVG(total_steps), 2) as avg_total_steps,
            ROUND(AVG(failed_steps), 2) as avg_failed_steps,
            ROUND(AVG(retried_steps), 2) as avg_retried_steps
        FROM workflow_executions
        WHERE completed_at >= $1
          AND completed_at < $2
          AND status IN ('completed', 'failed', 'cancelled', 'timed_out')
          AND duration_ms IS NOT NULL
        GROUP BY namespace, workflow_name, workflow_version, metric_date, hour_of_day
        ON CONFLICT (namespace, workflow_name, workflow_version, metric_date, hour_of_day)
        DO UPDATE SET
            total_executions = EXCLUDED.total_executions,
            completed_executions = EXCLUDED.completed_executions,
            failed_executions = EXCLUDED.failed_executions,
            cancelled_executions = EXCLUDED.cancelled_executions,
            timed_out_executions = EXCLUDED.timed_out_executions,
            avg_duration_ms = EXCLUDED.avg_duration_ms,
            min_duration_ms = EXCLUDED.min_duration_ms,
            max_duration_ms = EXCLUDED.max_duration_ms,
            p50_duration_ms = EXCLUDED.p50_duration_ms,
            p95_duration_ms = EXCLUDED.p95_duration_ms,
            p99_duration_ms = EXCLUDED.p99_duration_ms,
            avg_total_steps = EXCLUDED.avg_total_steps,
            avg_failed_steps = EXCLUDED.avg_failed_steps,
            avg_retried_steps = EXCLUDED.avg_retried_steps,
            updated_at = NOW()
        "#,
    )
    .bind(start_time)
    .bind(end_time)
    .execute(pool)
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_time_window() {
        let (start, end) = calculate_time_window();

        // End time should be start of current hour (aligned to 3600s)
        assert_eq!(end.timestamp() % 3600, 0);

        // Start time should be 1 hour before end time
        let duration = end - start;
        assert_eq!(duration.as_hours(), 1);

        // Start should be before end
        assert!(start < end);
    }

    #[test]
    fn test_time_window_is_complete_hour() {
        let (start, end) = calculate_time_window();

        // Both should be at the start of an hour (aligned to 3600s)
        assert_eq!(start.timestamp() % 3600, 0);
        assert_eq!(end.timestamp() % 3600, 0);
    }

    #[test]
    fn test_time_window_is_in_past() {
        let (_start, end) = calculate_time_window();
        let now = Timestamptz::now();

        // End time should be in the past (we only aggregate complete hours)
        assert!(end <= now);
    }
}
