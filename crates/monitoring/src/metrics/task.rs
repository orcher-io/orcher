//! Task execution metrics.

use crate::error::Result;
use crate::metrics::task_names;
use metrics::{counter, histogram};
use std::time::Instant;

/// Wraps a task future with automatic duration and completion status tracking.
pub async fn track_task_execution<F, T>(f: F) -> Result<T>
where
    F: std::future::Future<Output = Result<T>>,
{
    let start = Instant::now();
    let result = f.await;
    let duration = start.elapsed();

    histogram!(task_names::DURATION_SECONDS).record(duration.as_secs_f64());

    match &result {
        Ok(_) => {
            counter!(task_names::COMPLETED_TOTAL).increment(1);
        }
        Err(_) => {
            counter!(task_names::FAILED_TOTAL).increment(1);
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_track_task_execution_success() {
        let result = track_task_execution(async { Ok("success") }).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "success");
    }

    #[tokio::test]
    async fn test_track_task_execution_failure() {
        let result: Result<&str> = track_task_execution(async {
            Err(crate::error::MonitoringError::MetricCollectionError {
                metric_name: "test".to_string(),
                message: "test error".to_string(),
            })
        })
        .await;

        assert!(result.is_err());
    }
}
