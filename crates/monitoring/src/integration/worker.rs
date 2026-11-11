//! Integration with orcher-worker for automatic lifecycle and task execution monitoring.
//!
//! This module provides utilities to automatically instrument worker operations,
//! including worker pool management, task execution tracking, and worker health monitoring.

use crate::error::Result;
use crate::metrics::worker_names;
use metrics::{counter, gauge};
use std::time::Instant;
use tracing::{debug, info, instrument};

/// Tracks worker registration in the worker pool.
pub fn track_worker_registered(worker_id: &str, worker_type: &str) {
    counter!(
        "worker_lifecycle_events_total",
        "worker_id" => worker_id.to_string(),
        "worker_type" => worker_type.to_string(),
        "event" => "registered".to_string()
    )
    .increment(1);

    info!(worker_id = %worker_id, worker_type = %worker_type, "Worker registered");
}

/// Tracks worker deregistration from the worker pool.
pub fn track_worker_deregistered(worker_id: &str, worker_type: &str) {
    counter!(
        "worker_lifecycle_events_total",
        "worker_id" => worker_id.to_string(),
        "worker_type" => worker_type.to_string(),
        "event" => "deregistered".to_string()
    )
    .increment(1);

    info!(worker_id = %worker_id, worker_type = %worker_type, "Worker deregistered");
}

/// Updates the worker pool size metrics.
pub fn update_worker_pool_metrics(active: usize, idle: usize, total: usize) {
    gauge!(worker_names::ACTIVE_COUNT).set(active as f64);
    gauge!(worker_names::IDLE_COUNT).set(idle as f64);
    gauge!(worker_names::POOL_SIZE).set(total as f64);

    if total > 0 {
        let utilization = (active as f64) / (total as f64);
        gauge!(worker_names::UTILIZATION).set(utilization);
    }
}

/// Instruments a task execution by a worker with automatic metrics collection.
#[instrument(skip(task_execution))]
pub async fn track_worker_task_execution<F, T>(
    worker_id: &str,
    task_id: &str,
    task_execution: F,
) -> Result<T>
where
    F: std::future::Future<Output = Result<T>>,
{
    let start = Instant::now();

    debug!(worker_id = %worker_id, task_id = %task_id, "Worker starting task execution");

    let result = task_execution.await;
    let duration = start.elapsed();

    match &result {
        Ok(_) => {
            counter!(
                "worker_task_executions_total",
                "worker_id" => worker_id.to_string(),
                "result" => "success".to_string()
            )
            .increment(1);

            info!(
                worker_id = %worker_id,
                task_id = %task_id,
                duration_ms = %duration.as_millis(),
                "Worker completed task successfully"
            );
        }
        Err(_) => {
            counter!(
                "worker_task_executions_total",
                "worker_id" => worker_id.to_string(),
                "result" => "error".to_string()
            )
            .increment(1);

            debug!(
                worker_id = %worker_id,
                task_id = %task_id,
                duration_ms = %duration.as_millis(),
                "Worker task execution failed"
            );
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_track_worker_registered() {
        track_worker_registered("worker-1", "cpu");
    }

    #[test]
    fn test_track_worker_deregistered() {
        track_worker_deregistered("worker-1", "cpu");
    }

    #[test]
    fn test_update_worker_pool_metrics() {
        update_worker_pool_metrics(5, 3, 8);
        update_worker_pool_metrics(0, 0, 0);
    }

    #[tokio::test]
    async fn test_track_worker_task_execution_success() {
        let result = track_worker_task_execution("worker-1", "task-1", async {
            Ok::<_, crate::error::MonitoringError>(42)
        })
        .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_track_worker_task_execution_failure() {
        let result = track_worker_task_execution("worker-1", "task-1", async {
            Err::<(), _>(crate::error::MonitoringError::IntegrationError {
                component: "test".to_string(),
                message: "task failed".to_string(),
            })
        })
        .await;

        assert!(result.is_err());
    }
}
