//! Integration with orcher-scheduler for automatic job scheduling and trigger monitoring.
//!
//! This module provides utilities to automatically instrument scheduler operations,
//! including job scheduling, trigger firing, and queue management.

use crate::error::Result;
use crate::metrics::scheduler_names;
use metrics::{counter, gauge};
use std::time::Instant;
use tracing::{debug, info, instrument};

/// Tracks a job being scheduled.
pub fn track_job_scheduled(job_id: &str, job_type: &str, schedule: &str) {
    counter!(
        "scheduler_jobs_scheduled_total",
        "job_id" => job_id.to_string(),
        "job_type" => job_type.to_string(),
        "schedule" => schedule.to_string()
    )
    .increment(1);

    info!(job_id = %job_id, job_type = %job_type, schedule = %schedule, "Job scheduled");
}

/// Tracks a trigger being fired by the scheduler.
pub fn track_trigger_fired(trigger_id: &str, job_id: &str, success: bool) {
    counter!(
        scheduler_names::TRIGGERS_FIRED_TOTAL,
        "trigger_id" => trigger_id.to_string(),
        "job_id" => job_id.to_string(),
        "result" => if success { "success".to_string() } else { "failure".to_string() }
    )
    .increment(1);

    if success {
        info!(trigger_id = %trigger_id, job_id = %job_id, "Trigger fired successfully");
    } else {
        debug!(trigger_id = %trigger_id, job_id = %job_id, "Trigger failed to fire");
    }
}

/// Updates the scheduler queue size metric.
pub fn update_scheduler_queue_size(pending: usize, running: usize) {
    gauge!(scheduler_names::QUEUE_SIZE, "status" => "pending".to_string()).set(pending as f64);
    gauge!(scheduler_names::QUEUE_SIZE, "status" => "running".to_string()).set(running as f64);
}

/// Updates the next scheduled fire time metric.
pub fn update_next_fire_time(seconds_until_next: i64) {
    gauge!(scheduler_names::NEXT_FIRE_TIME).set(seconds_until_next as f64);
}

/// Instruments a scheduled job execution with automatic metrics collection.
#[instrument(skip(job_execution))]
pub async fn track_scheduled_job_execution<F, T>(
    job_id: &str,
    job_type: &str,
    job_execution: F,
) -> Result<T>
where
    F: std::future::Future<Output = Result<T>>,
{
    let start = Instant::now();

    debug!(job_id = %job_id, job_type = %job_type, "Starting scheduled job execution");

    let result = job_execution.await;
    let duration = start.elapsed();

    match &result {
        Ok(_) => {
            counter!(
                "scheduler_job_executions_total",
                "job_id" => job_id.to_string(),
                "job_type" => job_type.to_string(),
                "result" => "success".to_string()
            )
            .increment(1);

            info!(
                job_id = %job_id,
                job_type = %job_type,
                duration_ms = %duration.as_millis(),
                "Scheduled job completed successfully"
            );
        }
        Err(_) => {
            counter!(
                "scheduler_job_executions_total",
                "job_id" => job_id.to_string(),
                "job_type" => job_type.to_string(),
                "result" => "error".to_string()
            )
            .increment(1);

            debug!(
                job_id = %job_id,
                job_type = %job_type,
                duration_ms = %duration.as_millis(),
                "Scheduled job execution failed"
            );
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_track_job_scheduled() {
        track_job_scheduled("job-1", "backup", "0 0 * * *");
    }

    #[test]
    fn test_track_trigger_fired_success() {
        track_trigger_fired("trigger-1", "job-1", true);
    }

    #[test]
    fn test_track_trigger_fired_failure() {
        track_trigger_fired("trigger-1", "job-1", false);
    }

    #[test]
    fn test_update_scheduler_queue_size() {
        update_scheduler_queue_size(10, 5);
        update_scheduler_queue_size(0, 0);
    }

    #[test]
    fn test_update_next_fire_time() {
        update_next_fire_time(3600);
        update_next_fire_time(0);
    }

    #[tokio::test]
    async fn test_track_scheduled_job_execution_success() {
        let result = track_scheduled_job_execution("job-1", "backup", async {
            Ok::<_, crate::error::MonitoringError>(())
        })
        .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_track_scheduled_job_execution_failure() {
        let result = track_scheduled_job_execution("job-1", "backup", async {
            Err::<(), _>(crate::error::MonitoringError::IntegrationError {
                component: "test".to_string(),
                message: "job failed".to_string(),
            })
        })
        .await;

        assert!(result.is_err());
    }
}
