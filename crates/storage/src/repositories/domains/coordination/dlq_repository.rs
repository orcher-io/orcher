//! # Dead Letter Queue Repository Implementation
//!
//! Repository implementation for dead letter queue operations handling failed tasks
//! after exhausting all retry attempts.
//!
//! This implementation provides persistence operations for tracking and managing
//! permanently failed tasks that require manual intervention.

use orcher_common::OrcherResult;
use sqlx::PgPool;
use std::sync::Arc;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::schema::types::coordination::{DeadLetterTask, OutboxTask, TimerTask};

/// Dead letter queue repository for failed task tracking
#[derive(Clone)]
pub struct DeadLetterQueueRepository {
    pool: Arc<PgPool>,
}

impl DeadLetterQueueRepository {
    /// Create a new dead letter queue repository
    pub fn new(pool: Arc<PgPool>) -> Self {
        Self { pool }
    }

    /// Move a failed outbox task to the dead letter queue
    pub async fn move_outbox_task_to_dlq(
        &self,
        task: &OutboxTask,
        error_details: serde_json::Value,
    ) -> OrcherResult<DeadLetterTask> {
        let dlq_task = DeadLetterTask::from_outbox_task(task, error_details);

        let result = sqlx::query_as::<_, DeadLetterTask>(concat!(
            r#"
            INSERT INTO dead_letter_queue (
                dlq_id, organization_id, original_task_id, original_task_type, workflow_id,
                payload, attempt_count, last_error, error_details, failed_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            RETURNING "#,
            crate::repositories::columns::dead_letter_queue!(),
            r#"
            "#
        ))
        .bind(dlq_task.dlq_id)
        .bind(dlq_task.organization_id)
        .bind(dlq_task.original_task_id)
        .bind(&dlq_task.original_task_type)
        .bind(dlq_task.workflow_id)
        .bind(&dlq_task.payload)
        .bind(dlq_task.attempt_count)
        .bind(&dlq_task.last_error)
        .bind(&dlq_task.error_details)
        .bind(dlq_task.failed_at)
        .fetch_one(self.pool.as_ref())
        .await
        .map_err(|e| {
            error!("Failed to move outbox task to DLQ: {}", e);
            orcher_common::error::OrcherError::Internal {
                message: format!("Failed to move task to dead letter queue: {}", e),
                source: None,
            }
        })?;

        warn!(
            dlq_id = %result.dlq_id,
            original_task_id = %result.original_task_id,
            workflow_id = %result.workflow_id,
            task_type = %result.original_task_type,
            "Moved outbox task to dead letter queue"
        );

        Ok(result)
    }

    /// Move a failed timer task to the dead letter queue
    pub async fn move_timer_task_to_dlq(
        &self,
        task: &TimerTask,
        error_details: serde_json::Value,
    ) -> OrcherResult<DeadLetterTask> {
        let dlq_task = DeadLetterTask::from_timer_task(task, error_details);

        let result = sqlx::query_as::<_, DeadLetterTask>(concat!(
            r#"
            INSERT INTO dead_letter_queue (
                dlq_id, organization_id, original_task_id, original_task_type, workflow_id,
                payload, attempt_count, last_error, error_details, failed_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            RETURNING "#,
            crate::repositories::columns::dead_letter_queue!(),
            r#"
            "#
        ))
        .bind(dlq_task.dlq_id)
        .bind(dlq_task.organization_id)
        .bind(dlq_task.original_task_id)
        .bind(&dlq_task.original_task_type)
        .bind(dlq_task.workflow_id)
        .bind(&dlq_task.payload)
        .bind(dlq_task.attempt_count)
        .bind(&dlq_task.last_error)
        .bind(&dlq_task.error_details)
        .bind(dlq_task.failed_at)
        .fetch_one(self.pool.as_ref())
        .await
        .map_err(|e| {
            error!("Failed to move timer task to DLQ: {}", e);
            orcher_common::error::OrcherError::Internal {
                message: format!("Failed to move timer task to dead letter queue: {}", e),
                source: None,
            }
        })?;

        warn!(
            dlq_id = %result.dlq_id,
            original_task_id = %result.original_task_id,
            workflow_id = %result.workflow_id,
            task_type = %result.original_task_type,
            "Moved timer task to dead letter queue"
        );

        Ok(result)
    }

    /// Find a dead letter task by ID
    pub async fn find_by_id(&self, dlq_id: Uuid) -> OrcherResult<Option<DeadLetterTask>> {
        let result = sqlx::query_as::<_, DeadLetterTask>(concat!(
            r#"
            SELECT "#,
            crate::repositories::columns::dead_letter_queue!(),
            r#" FROM dead_letter_queue
            WHERE dlq_id = $1
            "#
        ))
        .bind(dlq_id)
        .fetch_optional(self.pool.as_ref())
        .await
        .map_err(|e| {
            error!("Failed to find dead letter task: {}", e);
            orcher_common::error::OrcherError::Internal {
                message: format!("Failed to find dead letter task: {}", e),
                source: None,
            }
        })?;

        Ok(result)
    }

    /// List dead letter tasks for a workflow
    pub async fn list_workflow_tasks(
        &self,
        workflow_id: Uuid,
        limit: usize,
    ) -> OrcherResult<Vec<DeadLetterTask>> {
        let tasks = sqlx::query_as::<_, DeadLetterTask>(concat!(
            r#"
            SELECT "#,
            crate::repositories::columns::dead_letter_queue!(),
            r#" FROM dead_letter_queue
            WHERE workflow_id = $1
            ORDER BY failed_at DESC
            LIMIT $2
            "#
        ))
        .bind(workflow_id)
        .bind(limit as i64)
        .fetch_all(self.pool.as_ref())
        .await
        .map_err(|e| {
            error!("Failed to list workflow dead letter tasks: {}", e);
            orcher_common::error::OrcherError::Internal {
                message: format!("Failed to list workflow dead letter tasks: {}", e),
                source: None,
            }
        })?;

        Ok(tasks)
    }

    /// List all dead letter tasks with pagination
    pub async fn list_all(&self, limit: usize, offset: usize) -> OrcherResult<Vec<DeadLetterTask>> {
        let tasks = sqlx::query_as::<_, DeadLetterTask>(concat!(
            r#"
            SELECT "#,
            crate::repositories::columns::dead_letter_queue!(),
            r#" FROM dead_letter_queue
            ORDER BY failed_at DESC
            LIMIT $1 OFFSET $2
            "#
        ))
        .bind(limit as i64)
        .bind(offset as i64)
        .fetch_all(self.pool.as_ref())
        .await
        .map_err(|e| {
            error!("Failed to list dead letter tasks: {}", e);
            orcher_common::error::OrcherError::Internal {
                message: format!("Failed to list dead letter tasks: {}", e),
                source: None,
            }
        })?;

        Ok(tasks)
    }

    /// Add a note to a dead letter task
    pub async fn add_note(&self, dlq_id: Uuid, note: String) -> OrcherResult<()> {
        let rows_affected = sqlx::query(
            r#"
            UPDATE dead_letter_queue
            SET note = $2
            WHERE dlq_id = $1
            "#,
        )
        .bind(dlq_id)
        .bind(&note)
        .execute(self.pool.as_ref())
        .await
        .map_err(|e| {
            error!("Failed to add note to dead letter task: {}", e);
            orcher_common::error::OrcherError::Internal {
                message: format!("Failed to add note to dead letter task: {}", e),
                source: None,
            }
        })?
        .rows_affected();

        if rows_affected > 0 {
            debug!(dlq_id = %dlq_id, "Added note to dead letter task");
        } else {
            warn!(dlq_id = %dlq_id, "Dead letter task not found for note");
        }

        Ok(())
    }

    /// Delete a dead letter task
    pub async fn delete(&self, dlq_id: Uuid) -> OrcherResult<()> {
        let rows_affected = sqlx::query(
            r#"
            DELETE FROM dead_letter_queue
            WHERE dlq_id = $1
            "#,
        )
        .bind(dlq_id)
        .execute(self.pool.as_ref())
        .await
        .map_err(|e| {
            error!("Failed to delete dead letter task: {}", e);
            orcher_common::error::OrcherError::Internal {
                message: format!("Failed to delete dead letter task: {}", e),
                source: None,
            }
        })?
        .rows_affected();

        if rows_affected > 0 {
            info!(dlq_id = %dlq_id, "Deleted dead letter task");
        } else {
            warn!(dlq_id = %dlq_id, "Dead letter task not found for deletion");
        }

        Ok(())
    }

    /// Get count of dead letter tasks by workflow
    pub async fn count_workflow_tasks(&self, workflow_id: Uuid) -> OrcherResult<i64> {
        let count = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)
            FROM dead_letter_queue
            WHERE workflow_id = $1
            "#,
        )
        .bind(workflow_id)
        .fetch_one(self.pool.as_ref())
        .await
        .map_err(|e| {
            error!("Failed to count workflow dead letter tasks: {}", e);
            orcher_common::error::OrcherError::Internal {
                message: format!("Failed to count workflow dead letter tasks: {}", e),
                source: None,
            }
        })?;

        Ok(count)
    }

    /// Get count of all dead letter tasks
    pub async fn count_all(&self) -> OrcherResult<i64> {
        let count = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)
            FROM dead_letter_queue
            "#,
        )
        .fetch_one(self.pool.as_ref())
        .await
        .map_err(|e| {
            error!("Failed to count dead letter tasks: {}", e);
            orcher_common::error::OrcherError::Internal {
                message: format!("Failed to count dead letter tasks: {}", e),
                source: None,
            }
        })?;

        Ok(count)
    }

    /// List dead letter tasks by task type
    pub async fn list_by_task_type(
        &self,
        task_type: &str,
        limit: usize,
    ) -> OrcherResult<Vec<DeadLetterTask>> {
        let tasks = sqlx::query_as::<_, DeadLetterTask>(concat!(
            r#"
            SELECT "#,
            crate::repositories::columns::dead_letter_queue!(),
            r#" FROM dead_letter_queue
            WHERE original_task_type = $1
            ORDER BY failed_at DESC
            LIMIT $2
            "#
        ))
        .bind(task_type)
        .bind(limit as i64)
        .fetch_all(self.pool.as_ref())
        .await
        .map_err(|e| {
            error!("Failed to list dead letter tasks by type: {}", e);
            orcher_common::error::OrcherError::Internal {
                message: format!("Failed to list dead letter tasks by type: {}", e),
                source: None,
            }
        })?;

        Ok(tasks)
    }

    /// Get dead letter task statistics grouped by task type
    pub async fn get_statistics_by_type(&self) -> OrcherResult<Vec<(String, i64)>> {
        let stats = sqlx::query_as::<_, (String, i64)>(
            r#"
            SELECT original_task_type, COUNT(*) as count
            FROM dead_letter_queue
            GROUP BY original_task_type
            ORDER BY count DESC
            "#,
        )
        .fetch_all(self.pool.as_ref())
        .await
        .map_err(|e| {
            error!("Failed to get dead letter queue statistics: {}", e);
            orcher_common::error::OrcherError::Internal {
                message: format!("Failed to get dead letter queue statistics: {}", e),
                source: None,
            }
        })?;

        Ok(stats)
    }

    /// Cleanup old dead letter tasks
    pub async fn cleanup_old(&self, retention_days: i32) -> OrcherResult<i64> {
        let rows_affected = sqlx::query(
            r#"
            DELETE FROM dead_letter_queue
            WHERE failed_at < NOW() - ($1 || ' days')::INTERVAL
            "#,
        )
        .bind(retention_days)
        .execute(self.pool.as_ref())
        .await
        .map_err(|e| {
            error!("Failed to cleanup old dead letter tasks: {}", e);
            orcher_common::error::OrcherError::Internal {
                message: format!("Failed to cleanup old dead letter tasks: {}", e),
                source: None,
            }
        })?
        .rows_affected();

        if rows_affected > 0 {
            info!(
                count = rows_affected,
                retention_days = retention_days,
                "Cleaned up old dead letter tasks"
            );
        }

        Ok(rows_affected as i64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jiff::SignedDuration;

    // Helper function to create test pool (would be implemented in test setup)
    // For now, these are just structure tests

    #[test]
    fn test_dlq_repository_creation() {
        // This is a compile-time test to ensure the repository can be created
        fn _assert_can_create(pool: Arc<PgPool>) {
            let _repo = DeadLetterQueueRepository::new(pool);
        }
    }

    #[test]
    fn test_dead_letter_task_from_outbox() {
        use crate::schema::types::coordination::{OutboxTask, OutboxTaskType};

        let outbox_task = OutboxTask::new(
            Uuid::new_v4(),
            OutboxTaskType::DispatchTask,
            serde_json::json!({"task": "data"}),
            None,
        );

        let dlq_task =
            DeadLetterTask::from_outbox_task(&outbox_task, serde_json::json!({"error": "details"}));

        assert_eq!(dlq_task.original_task_id, outbox_task.task_id);
        assert_eq!(dlq_task.workflow_id, outbox_task.workflow_id);
        assert_eq!(dlq_task.payload, outbox_task.payload);
    }

    #[test]
    fn test_dead_letter_task_from_timer() {
        use crate::schema::types::coordination::{TimerTask, TimerTaskType};
        use orcher_common::types::time::Timestamptz;

        let fire_at = Timestamptz::now() + SignedDuration::from_secs(60);
        let timer_task = TimerTask::new(
            Uuid::new_v4(),
            TimerTaskType::FireTimer,
            serde_json::json!({"timer_id": "test"}),
            fire_at,
        );

        let dlq_task =
            DeadLetterTask::from_timer_task(&timer_task, serde_json::json!({"error": "timeout"}));

        assert_eq!(dlq_task.original_task_id, timer_task.task_id);
        assert_eq!(dlq_task.workflow_id, timer_task.workflow_id);
        assert_eq!(dlq_task.payload, timer_task.payload);
    }
}
