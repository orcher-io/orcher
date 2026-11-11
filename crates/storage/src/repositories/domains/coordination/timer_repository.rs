//! # Timer Queue Repository Implementation
//!
//! Repository implementation for timer queue operations supporting durable scheduling
//! with guaranteed execution.
//!
//! This implementation provides persistence operations for timer tasks that survive
//! coordinator crashes and ensure scheduled operations fire at the correct time.

use jiff::SignedDuration;
use orcher_common::types::time::Timestamptz;
use orcher_common::OrcherResult;
use sqlx::{PgPool, Postgres, Transaction};
use std::sync::Arc;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::schema::types::coordination::{QueueMetrics, TimerTask};

/// Timer queue repository for durable scheduling operations
#[derive(Clone)]
pub struct TimerRepository {
    pool: Arc<PgPool>,
}

impl TimerRepository {
    /// Create a new timer repository
    pub fn new(pool: Arc<PgPool>) -> Self {
        Self { pool }
    }

    /// Create a new timer task
    pub async fn create(&self, task: TimerTask) -> OrcherResult<TimerTask> {
        let result = sqlx::query_as::<_, TimerTask>(concat!(
            r#"
            INSERT INTO timer_queue (
                task_id, organization_id, workflow_id, task_type, payload, fire_at,
                created_at, status, attempt_count, max_retries
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            RETURNING "#,
            crate::repositories::columns::timer_queue!(),
            r#"
            "#
        ))
        .bind(task.task_id)
        .bind(task.organization_id)
        .bind(task.workflow_id)
        .bind(task.task_type.to_string())
        .bind(&task.payload)
        .bind(task.fire_at)
        .bind(task.created_at)
        .bind(task.status.to_string())
        .bind(task.attempt_count)
        .bind(task.max_retries)
        .fetch_one(self.pool.as_ref())
        .await
        .map_err(|e| {
            error!("Failed to create timer task: {}", e);
            orcher_common::error::OrcherError::Internal {
                message: format!("Failed to create timer task: {}", e),
                source: None,
            }
        })?;

        debug!(
            task_id = %result.task_id,
            workflow_id = %result.workflow_id,
            task_type = %result.task_type,
            fire_at = %result.fire_at,
            "Created timer task"
        );

        Ok(result)
    }

    /// Whether a fire-timer task with this business timer name has already
    /// been created for this workflow execution row, in any status.
    ///
    /// Keyed by the execution row, not the journal: a reset successor copies
    /// its predecessor's `TimerStarted` entries but none of its timers, and
    /// must still schedule its own.
    pub async fn fire_timer_exists_in_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        workflow_id: Uuid,
        timer_name: &str,
    ) -> OrcherResult<bool> {
        sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS(
                SELECT 1 FROM timer_queue
                WHERE workflow_id = $1
                  AND task_type = 'fire_timer'
                  AND payload->>'timer_name' = $2
            )
            "#,
        )
        .bind(workflow_id)
        .bind(timer_name)
        .fetch_one(&mut **tx)
        .await
        .map_err(|e| {
            error!("Failed to look up an existing timer: {}", e);
            orcher_common::error::OrcherError::Internal {
                message: format!("Failed to look up an existing timer: {}", e),
                source: None,
            }
        })
    }

    /// Create a new timer task within an existing transaction
    pub async fn create_in_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        task: TimerTask,
    ) -> OrcherResult<Uuid> {
        let task_id = sqlx::query_scalar::<_, Uuid>(
            r#"
            INSERT INTO timer_queue (
                task_id, organization_id, workflow_id, task_type, payload, fire_at,
                created_at, status, attempt_count, max_retries
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            RETURNING task_id
            "#,
        )
        .bind(task.task_id)
        .bind(task.organization_id)
        .bind(task.workflow_id)
        .bind(task.task_type.to_string())
        .bind(&task.payload)
        .bind(task.fire_at)
        .bind(task.created_at)
        .bind(task.status.to_string())
        .bind(task.attempt_count)
        .bind(task.max_retries)
        .fetch_one(&mut **tx)
        .await
        .map_err(|e| {
            error!("Failed to create timer task in transaction: {}", e);
            orcher_common::error::OrcherError::Internal {
                message: format!("Failed to create timer task: {}", e),
                source: None,
            }
        })?;

        debug!(
            task_id = %task_id,
            workflow_id = %task.workflow_id,
            task_type = %task.task_type,
            fire_at = %task.fire_at,
            "Created timer task in transaction"
        );

        Ok(task_id)
    }

    /// Find a timer task by ID
    pub async fn find_by_id(&self, task_id: Uuid) -> OrcherResult<Option<TimerTask>> {
        let result = sqlx::query_as::<_, TimerTask>(concat!(
            r#"
            SELECT "#,
            crate::repositories::columns::timer_queue!(),
            r#" FROM timer_queue
            WHERE task_id = $1
            "#
        ))
        .bind(task_id)
        .fetch_optional(self.pool.as_ref())
        .await
        .map_err(|e| {
            error!("Failed to find timer task: {}", e);
            orcher_common::error::OrcherError::Internal {
                message: format!("Failed to find timer task: {}", e),
                source: None,
            }
        })?;

        Ok(result)
    }

    /// Claim timer tasks that are ready to fire
    ///
    /// # Arguments
    /// * `organization_id` - Optional organization ID for multi-tenant filtering
    /// * `batch_size` - Maximum number of tasks to claim
    pub async fn claim_ready_tasks(
        &self,
        organization_id: Option<Uuid>,
        batch_size: usize,
    ) -> OrcherResult<Vec<TimerTask>> {
        let tasks = sqlx::query_as::<_, TimerTask>(concat!(
            r#"
            UPDATE timer_queue
            SET status = 'processing',
                processing_started_at = NOW(),
                attempt_count = attempt_count + 1
            WHERE task_id IN (
                SELECT task_id
                FROM timer_queue
                WHERE ($1::uuid IS NULL OR organization_id = $1)
                  AND status = 'pending'
                  AND fire_at <= NOW()
                ORDER BY fire_at ASC
                LIMIT $2
                FOR UPDATE SKIP LOCKED
            )
            RETURNING "#,
            crate::repositories::columns::timer_queue!(),
            r#"
            "#
        ))
        .bind(organization_id)
        .bind(batch_size as i64)
        .fetch_all(self.pool.as_ref())
        .await
        .map_err(|e| {
            error!("Failed to claim ready timer tasks: {}", e);
            orcher_common::error::OrcherError::Internal {
                message: format!("Failed to claim ready timer tasks: {}", e),
                source: None,
            }
        })?;

        if !tasks.is_empty() {
            debug!(
                organization_id = ?organization_id,
                count = tasks.len(),
                "Claimed ready timer tasks"
            );
        }

        Ok(tasks)
    }

    /// Get the next timer fire time
    ///
    /// # Arguments
    /// * `organization_id` - Optional organization ID for multi-tenant filtering
    pub async fn get_next_fire_time(
        &self,
        organization_id: Option<Uuid>,
    ) -> OrcherResult<Option<Timestamptz>> {
        let result = sqlx::query_scalar::<_, Timestamptz>(
            r#"
            SELECT fire_at
            FROM timer_queue
            WHERE ($1::uuid IS NULL OR organization_id = $1)
              AND status = 'pending'
            ORDER BY fire_at ASC
            LIMIT 1
            "#,
        )
        .bind(organization_id)
        .fetch_optional(self.pool.as_ref())
        .await
        .map_err(|e| {
            error!("Failed to get next timer fire time: {}", e);
            orcher_common::error::OrcherError::Internal {
                message: format!("Failed to get next timer fire time: {}", e),
                source: None,
            }
        })?;

        if let Some(fire_at) = result {
            debug!(
                organization_id = ?organization_id,
                fire_at = %fire_at,
                "Next timer fire time"
            );
        }

        Ok(result)
    }

    /// Mark a timer task as completed
    pub async fn complete(&self, task_id: Uuid) -> OrcherResult<()> {
        let rows_affected = sqlx::query(
            r#"
            UPDATE timer_queue
            SET status = 'completed',
                completed_at = NOW()
            WHERE task_id = $1
            "#,
        )
        .bind(task_id)
        .execute(self.pool.as_ref())
        .await
        .map_err(|e| {
            error!("Failed to complete timer task: {}", e);
            orcher_common::error::OrcherError::Internal {
                message: format!("Failed to complete timer task: {}", e),
                source: None,
            }
        })?
        .rows_affected();

        if rows_affected > 0 {
            debug!(task_id = %task_id, "Completed timer task");
        } else {
            warn!(task_id = %task_id, "Timer task not found for completion");
        }

        Ok(())
    }

    /// Mark a timer task as failed and schedule retry or move to failed status
    pub async fn fail(&self, task_id: Uuid, error_message: String) -> OrcherResult<()> {
        // First, get the task to check retry logic
        let task = self.find_by_id(task_id).await?;

        let task = match task {
            Some(t) => t,
            None => {
                warn!(task_id = %task_id, "Timer task not found for failure");
                return Ok(());
            }
        };

        // Check if we can retry
        if task.can_retry() {
            // Reschedule for immediate retry (timers don't use exponential backoff)
            sqlx::query(
                r#"
                UPDATE timer_queue
                SET status = 'pending',
                    processing_started_at = NULL,
                    last_error = $2
                WHERE task_id = $1
                "#,
            )
            .bind(task_id)
            .bind(&error_message)
            .execute(self.pool.as_ref())
            .await
            .map_err(|e| {
                error!("Failed to schedule retry for timer task: {}", e);
                orcher_common::error::OrcherError::Internal {
                    message: format!("Failed to schedule retry: {}", e),
                    source: None,
                }
            })?;

            debug!(
                task_id = %task_id,
                attempt = task.attempt_count,
                "Scheduled timer task for retry"
            );
        } else {
            // No more retries, mark as failed
            sqlx::query(
                r#"
                UPDATE timer_queue
                SET status = 'failed',
                    completed_at = NOW(),
                    last_error = $2
                WHERE task_id = $1
                "#,
            )
            .bind(task_id)
            .bind(&error_message)
            .execute(self.pool.as_ref())
            .await
            .map_err(|e| {
                error!("Failed to mark timer task as failed: {}", e);
                orcher_common::error::OrcherError::Internal {
                    message: format!("Failed to mark timer task as failed: {}", e),
                    source: None,
                }
            })?;

            warn!(
                task_id = %task_id,
                error = %error_message,
                "Timer task failed after all retries"
            );
        }

        Ok(())
    }

    /// Cancel a timer task
    pub async fn cancel(&self, task_id: Uuid) -> OrcherResult<()> {
        let rows_affected = sqlx::query(
            r#"
            UPDATE timer_queue
            SET status = 'cancelled',
                completed_at = NOW()
            WHERE task_id = $1
              AND status IN ('pending', 'processing')
            "#,
        )
        .bind(task_id)
        .execute(self.pool.as_ref())
        .await
        .map_err(|e| {
            error!("Failed to cancel timer task: {}", e);
            orcher_common::error::OrcherError::Internal {
                message: format!("Failed to cancel timer task: {}", e),
                source: None,
            }
        })?
        .rows_affected();

        if rows_affected > 0 {
            debug!(task_id = %task_id, "Cancelled timer task");
        } else {
            warn!(task_id = %task_id, "Timer task not found or already terminal");
        }

        Ok(())
    }

    /// Cancel all timer tasks for a workflow
    pub async fn cancel_workflow_timers(&self, workflow_id: Uuid) -> OrcherResult<i64> {
        let rows_affected = sqlx::query(
            r#"
            UPDATE timer_queue
            SET status = 'cancelled',
                completed_at = NOW()
            WHERE workflow_id = $1
              AND status IN ('pending', 'processing')
            "#,
        )
        .bind(workflow_id)
        .execute(self.pool.as_ref())
        .await
        .map_err(|e| {
            error!("Failed to cancel workflow timers: {}", e);
            orcher_common::error::OrcherError::Internal {
                message: format!("Failed to cancel workflow timers: {}", e),
                source: None,
            }
        })?
        .rows_affected();

        if rows_affected > 0 {
            info!(
                workflow_id = %workflow_id,
                count = rows_affected,
                "Cancelled workflow timers"
            );
        }

        Ok(rows_affected as i64)
    }

    /// Reset stuck processing tasks back to pending
    pub async fn reset_stuck_tasks(&self, stuck_threshold: SignedDuration) -> OrcherResult<i64> {
        let stuck_time = Timestamptz::now() - stuck_threshold;

        let rows_affected = sqlx::query(
            r#"
            UPDATE timer_queue
            SET status = 'pending',
                processing_started_at = NULL
            WHERE status = 'processing'
              AND processing_started_at < $1
            "#,
        )
        .bind(stuck_time)
        .execute(self.pool.as_ref())
        .await
        .map_err(|e| {
            error!("Failed to reset stuck timer tasks: {}", e);
            orcher_common::error::OrcherError::Internal {
                message: format!("Failed to reset stuck timer tasks: {}", e),
                source: None,
            }
        })?
        .rows_affected();

        if rows_affected > 0 {
            info!(count = rows_affected, "Reset stuck timer tasks");
        }

        Ok(rows_affected as i64)
    }

    /// List all timer tasks for a workflow
    pub async fn list_workflow_timers(
        &self,
        workflow_id: Uuid,
        include_completed: bool,
    ) -> OrcherResult<Vec<TimerTask>> {
        let tasks = if include_completed {
            sqlx::query_as::<_, TimerTask>(concat!(
                r#"
                SELECT "#,
                crate::repositories::columns::timer_queue!(),
                r#" FROM timer_queue
                WHERE workflow_id = $1
                ORDER BY fire_at ASC
                "#
            ))
            .bind(workflow_id)
            .fetch_all(self.pool.as_ref())
            .await
        } else {
            sqlx::query_as::<_, TimerTask>(concat!(
                r#"
                SELECT "#,
                crate::repositories::columns::timer_queue!(),
                r#" FROM timer_queue
                WHERE workflow_id = $1
                  AND status IN ('pending', 'processing')
                ORDER BY fire_at ASC
                "#
            ))
            .bind(workflow_id)
            .fetch_all(self.pool.as_ref())
            .await
        }
        .map_err(|e| {
            error!("Failed to list workflow timers: {}", e);
            orcher_common::error::OrcherError::Internal {
                message: format!("Failed to list workflow timers: {}", e),
                source: None,
            }
        })?;

        Ok(tasks)
    }

    /// Get queue metrics for monitoring
    pub async fn get_metrics(&self) -> OrcherResult<QueueMetrics> {
        #[derive(sqlx::FromRow)]
        struct MetricsRow {
            pending_count: Option<i64>,
            processing_count: Option<i64>,
            completed_count: Option<i64>,
            failed_count: Option<i64>,
            avg_processing_time_ms: Option<i64>,
            oldest_pending_age_seconds: Option<i64>,
        }

        let row = sqlx::query_as::<_, MetricsRow>(
            r#"
            SELECT
                COUNT(CASE WHEN status = 'pending' THEN 1 END) as pending_count,
                COUNT(CASE WHEN status = 'processing' THEN 1 END) as processing_count,
                COUNT(CASE WHEN status = 'completed' AND completed_at > NOW() - INTERVAL '1 hour' THEN 1 END) as completed_count,
                COUNT(CASE WHEN status = 'failed' AND completed_at > NOW() - INTERVAL '1 hour' THEN 1 END) as failed_count,
                AVG(CASE WHEN status = 'completed' AND completed_at > NOW() - INTERVAL '1 hour'
                    THEN EXTRACT(EPOCH FROM (completed_at - processing_started_at)) * 1000 END)::BIGINT as avg_processing_time_ms,
                MAX(CASE WHEN status = 'pending' AND fire_at <= NOW() THEN EXTRACT(EPOCH FROM (NOW() - created_at)) END)::BIGINT as oldest_pending_age_seconds
            FROM timer_queue
            "#
        )
        .fetch_one(self.pool.as_ref())
        .await
        .map_err(|e| {
            error!("Failed to get timer queue metrics: {}", e);
            orcher_common::error::OrcherError::Internal {
                message: format!("Failed to get timer queue metrics: {}", e),
                source: None,
            }
        })?;

        Ok(QueueMetrics {
            pending_count: row.pending_count.unwrap_or(0),
            processing_count: row.processing_count.unwrap_or(0),
            completed_count: row.completed_count.unwrap_or(0),
            failed_count: row.failed_count.unwrap_or(0),
            avg_processing_time_ms: row.avg_processing_time_ms,
            oldest_pending_age_seconds: row.oldest_pending_age_seconds,
        })
    }

    /// Cleanup old completed tasks
    pub async fn cleanup_completed(&self, retention_days: i32) -> OrcherResult<i64> {
        let rows_affected = sqlx::query(
            r#"
            DELETE FROM timer_queue
            WHERE status IN ('completed', 'cancelled')
              AND completed_at < NOW() - ($1 || ' days')::INTERVAL
            "#,
        )
        .bind(retention_days)
        .execute(self.pool.as_ref())
        .await
        .map_err(|e| {
            error!("Failed to cleanup completed timer tasks: {}", e);
            orcher_common::error::OrcherError::Internal {
                message: format!("Failed to cleanup completed timer tasks: {}", e),
                source: None,
            }
        })?
        .rows_affected();

        if rows_affected > 0 {
            info!(
                count = rows_affected,
                retention_days = retention_days,
                "Cleaned up completed timer tasks"
            );
        }

        Ok(rows_affected as i64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::types::coordination::TimerTaskType;

    // Helper function to create test pool (would be implemented in test setup)
    // For now, these are just structure tests

    #[test]
    fn test_timer_repository_creation() {
        // This is a compile-time test to ensure the repository can be created
        fn _assert_can_create(pool: Arc<PgPool>) {
            let _repo = TimerRepository::new(pool);
        }
    }

    #[test]
    fn test_timer_task_types() {
        // Ensure all task types are valid
        let fire_at = Timestamptz::now() + SignedDuration::from_secs(60);
        let task = TimerTask::new(
            Uuid::new_v4(),
            TimerTaskType::FireTimer,
            serde_json::json!({"timer_id": "test"}),
            fire_at,
        );

        assert!(task.is_pending());
        assert!(!task.is_processing());
        assert!(task.can_retry());
        assert!(!task.is_ready()); // Not ready yet (60 seconds in future)
    }

    #[test]
    fn test_timer_fire_time_calculation() {
        let fire_at = Timestamptz::now() + SignedDuration::from_secs(120);
        let task = TimerTask::new(
            Uuid::new_v4(),
            TimerTaskType::FireTimer,
            serde_json::json!({}),
            fire_at,
        );

        let seconds_until_fire = task.seconds_until_fire();
        assert!((115..=125).contains(&seconds_until_fire));
    }
}
