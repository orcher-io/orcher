//! # ExecutionLog Repository
//!
//! Storage layer for durable step-level execution tracking (ctx.execute() API).
//!
//! This repository provides persistence for the ExecutionLog feature in Orcher v2.2,
//! enabling step-level replay protection and result caching.
//!
//! ## Features
//!
//! - Step-level result storage and retrieval
//! - Execution status tracking (pending, running, completed, failed)
//! - Retry attempt counting
//! - Input/output serialization (JSONB)
//! - Query capabilities for workflow introspection
//!
//! ## Usage
//!
//! ```rust,no_run
//! use orcher_storage::repositories::execution_log::{
//!     ExecutionLogRepository, PostgresExecutionLogRepository, ExecutionLogEntry, ExecutionLogStatus
//! };
//! use sqlx::PgPool;
//! use uuid::Uuid;
//!
//! # async fn example(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
//! let repo = PostgresExecutionLogRepository::new(pool);
//! let workflow_id = Uuid::new_v4();
//!
//! // Check if step has already completed
//! if let Some(entry) = repo.get_entry(workflow_id, "fetch_user").await? {
//!     if entry.status == ExecutionLogStatus::Completed {
//!         println!("Step already completed, replaying result");
//!         return Ok(());
//!     }
//! }
//!
//! // Store new execution entry
//! let entry = ExecutionLogEntry {
//!     workflow_id,
//!     step_name: "fetch_user".to_string(),
//!     status: ExecutionLogStatus::Running,
//!     input: Some(serde_json::json!({"user_id": 123})),
//!     ..Default::default()
//! };
//! repo.store_entry(&entry).await?;
//! # Ok(())
//! # }
//! ```

use async_trait::async_trait;
use orcher_common::types::time::Timestamptz;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::fmt;
use uuid::Uuid;

use crate::error::{StorageError, StorageResult};

// ============================================================================
// Types and Enums
// ============================================================================

/// ExecutionLog entry status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "execution_log_status", rename_all = "lowercase")]
pub enum ExecutionLogStatus {
    /// Step is queued for execution
    Pending,
    /// Step is currently executing
    Running,
    /// Step completed successfully
    Completed,
    /// Step failed with error
    Failed,
    /// Step was cancelled before completion
    Cancelled,
}

/// ExecutionLog step type (Option C: Generic step model)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "execution_step_type")]
pub enum ExecutionStepType {
    /// Distributed task execution (ctx.execute_task)
    #[sqlx(rename = "task")]
    Task,
    /// Inline closure execution (ctx.execute)
    #[sqlx(rename = "closure")]
    Closure,
    /// Child workflow execution
    #[sqlx(rename = "child_workflow")]
    ChildWorkflow,
    /// Side effects, local activities, etc.
    #[sqlx(rename = "side_effect")]
    SideEffect,
}

impl fmt::Display for ExecutionStepType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Task => write!(f, "task"),
            Self::Closure => write!(f, "closure"),
            Self::ChildWorkflow => write!(f, "child_workflow"),
            Self::SideEffect => write!(f, "side_effect"),
        }
    }
}

impl fmt::Display for ExecutionLogStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pending => write!(f, "pending"),
            Self::Running => write!(f, "running"),
            Self::Completed => write!(f, "completed"),
            Self::Failed => write!(f, "failed"),
            Self::Cancelled => write!(f, "cancelled"),
        }
    }
}

/// ExecutionLog entry (database row)
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct ExecutionLogEntry {
    /// Workflow execution identifier
    pub workflow_id: Uuid,
    /// Unique step name within the workflow
    pub step_name: String,
    /// Step execution type (task, closure, child_workflow, side_effect)
    pub step_type: ExecutionStepType,
    /// Execution attempt number
    pub execution_attempt: i32,
    /// Current status
    pub status: ExecutionLogStatus,
    /// Step input (JSONB)
    pub input: Option<serde_json::Value>,
    /// Step result (JSONB, NULL until completed)
    pub result: Option<serde_json::Value>,
    /// Error message (if failed)
    pub error_message: Option<String>,
    /// Error details (JSONB)
    pub error_details: Option<serde_json::Value>,
    /// Entry creation timestamp
    pub created_at: Timestamptz,
    /// Step started timestamp
    pub started_at: Option<Timestamptz>,
    /// Step completed/failed timestamp
    pub completed_at: Option<Timestamptz>,
    /// Duration in milliseconds
    pub duration_ms: Option<i64>,
    /// Retry count
    pub retry_count: i32,
    /// Last error message
    pub last_error: Option<String>,
}

impl Default for ExecutionLogEntry {
    fn default() -> Self {
        Self {
            workflow_id: Uuid::nil(),
            step_name: String::new(),
            step_type: ExecutionStepType::Task,
            execution_attempt: 1,
            status: ExecutionLogStatus::Pending,
            input: None,
            result: None,
            error_message: None,
            error_details: None,
            created_at: Timestamptz::now(),
            started_at: None,
            completed_at: None,
            duration_ms: None,
            retry_count: 0,
            last_error: None,
        }
    }
}

/// Query filter for execution log entries
#[derive(Debug, Clone, Default)]
pub struct ExecutionLogFilter {
    /// Filter by workflow ID
    pub workflow_id: Option<Uuid>,
    /// Filter by status
    pub status: Option<ExecutionLogStatus>,
    /// Limit results
    pub limit: Option<i64>,
    /// Offset for pagination
    pub offset: Option<i64>,
}

// ============================================================================
// Repository Trait
// ============================================================================

/// ExecutionLog storage repository trait
#[async_trait]
pub trait ExecutionLogRepository: Send + Sync {
    /// Get a single execution log entry by workflow_id + step_name
    async fn get_entry(
        &self,
        workflow_id: Uuid,
        step_name: &str,
    ) -> StorageResult<Option<ExecutionLogEntry>>;

    /// Store a new execution log entry (or update existing)
    async fn store_entry(&self, entry: &ExecutionLogEntry) -> StorageResult<()>;

    /// Store a step's terminal failure or cancellation, unless the step has
    /// already completed. Returns whether the row was written.
    ///
    /// A failure can be recorded while the same attempt's completion races
    /// it; the completion is the step's outcome, and an upsert that replaced
    /// it would hand the workflow a failure for a task that succeeded.
    async fn store_failure(&self, entry: &ExecutionLogEntry) -> StorageResult<bool>;

    /// Mark an entry as running (set started_at)
    async fn mark_running(&self, workflow_id: Uuid, step_name: &str) -> StorageResult<()>;

    /// Mark an entry as completed (set result, completed_at, duration)
    async fn mark_completed(
        &self,
        workflow_id: Uuid,
        step_name: &str,
        result: serde_json::Value,
    ) -> StorageResult<()>;

    /// Mark an entry as failed (set error, completed_at, duration, increment retry_count)
    async fn mark_failed(
        &self,
        workflow_id: Uuid,
        step_name: &str,
        error_message: &str,
        error_details: Option<serde_json::Value>,
    ) -> StorageResult<()>;

    /// List all entries for a workflow
    async fn list_entries(&self, workflow_id: Uuid) -> StorageResult<Vec<ExecutionLogEntry>>;

    /// List entries with filter
    async fn list_entries_filtered(
        &self,
        filter: &ExecutionLogFilter,
    ) -> StorageResult<Vec<ExecutionLogEntry>>;

    /// Count entries matching filter
    async fn count_entries(&self, filter: &ExecutionLogFilter) -> StorageResult<i64>;

    /// Delete all entries for a workflow (for cleanup)
    async fn delete_workflow_entries(&self, workflow_id: Uuid) -> StorageResult<u64>;

    /// Get execution statistics for a workflow
    async fn get_workflow_stats(&self, workflow_id: Uuid) -> StorageResult<WorkflowExecutionStats>;
}

/// Execution statistics for a workflow
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowExecutionStats {
    /// The workflow execution ID
    pub workflow_id: Uuid,
    /// Total number of steps in the workflow
    pub total_steps: i64,
    /// Number of steps that have completed
    pub completed_steps: i64,
    /// Number of steps that have failed
    pub failed_steps: i64,
    /// Number of steps currently running
    pub running_steps: i64,
    /// Number of steps that are pending execution
    pub pending_steps: i64,
    /// Total duration of all steps in milliseconds
    pub total_duration_ms: i64,
    /// Average duration per step in milliseconds
    pub average_step_duration_ms: Option<i64>,
}

// ============================================================================
// PostgreSQL Implementation
// ============================================================================

/// PostgreSQL implementation of ExecutionLogRepository
#[derive(Clone)]
pub struct PostgresExecutionLogRepository {
    pool: PgPool,
}

impl PostgresExecutionLogRepository {
    /// Create a new PostgreSQL ExecutionLog repository
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Get reference to the underlying database pool
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

#[async_trait]
impl ExecutionLogRepository for PostgresExecutionLogRepository {
    async fn get_entry(
        &self,
        workflow_id: Uuid,
        step_name: &str,
    ) -> StorageResult<Option<ExecutionLogEntry>> {
        let result = sqlx::query_as::<_, ExecutionLogEntry>(
            r#"
            SELECT
                workflow_id,
                step_name,
                step_type,
                execution_attempt,
                status,
                input,
                result,
                error_message,
                error_details,
                created_at,
                started_at,
                completed_at,
                duration_ms,
                retry_count,
                last_error
            FROM execution_log
            WHERE workflow_id = $1 AND step_name = $2
            "#,
        )
        .bind(workflow_id)
        .bind(step_name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| StorageError::DatabaseError(e.to_string()))?;

        Ok(result)
    }

    async fn store_entry(&self, entry: &ExecutionLogEntry) -> StorageResult<()> {
        sqlx::query(
            r#"
            INSERT INTO execution_log (
                workflow_id,
                step_name,
                step_type,
                execution_attempt,
                status,
                input,
                result,
                error_message,
                error_details,
                created_at,
                started_at,
                completed_at,
                duration_ms,
                retry_count,
                last_error
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
            ON CONFLICT (workflow_id, step_name) DO UPDATE SET
                step_type = EXCLUDED.step_type,
                execution_attempt = EXCLUDED.execution_attempt,
                status = EXCLUDED.status,
                input = EXCLUDED.input,
                result = EXCLUDED.result,
                error_message = EXCLUDED.error_message,
                error_details = EXCLUDED.error_details,
                started_at = EXCLUDED.started_at,
                completed_at = EXCLUDED.completed_at,
                duration_ms = EXCLUDED.duration_ms,
                retry_count = EXCLUDED.retry_count,
                last_error = EXCLUDED.last_error
            "#,
        )
        .bind(entry.workflow_id)
        .bind(&entry.step_name)
        .bind(entry.step_type)
        .bind(entry.execution_attempt)
        .bind(entry.status)
        .bind(&entry.input)
        .bind(&entry.result)
        .bind(&entry.error_message)
        .bind(&entry.error_details)
        .bind(entry.created_at)
        .bind(entry.started_at)
        .bind(entry.completed_at)
        .bind(entry.duration_ms)
        .bind(entry.retry_count)
        .bind(&entry.last_error)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::DatabaseError(e.to_string()))?;

        Ok(())
    }

    async fn store_failure(&self, entry: &ExecutionLogEntry) -> StorageResult<bool> {
        let written = sqlx::query(
            r#"
            INSERT INTO execution_log (
                workflow_id,
                step_name,
                step_type,
                execution_attempt,
                status,
                input,
                result,
                error_message,
                error_details,
                created_at,
                started_at,
                completed_at,
                duration_ms,
                retry_count,
                last_error
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
            ON CONFLICT (workflow_id, step_name) DO UPDATE SET
                step_type = EXCLUDED.step_type,
                execution_attempt = EXCLUDED.execution_attempt,
                status = EXCLUDED.status,
                input = EXCLUDED.input,
                result = EXCLUDED.result,
                error_message = EXCLUDED.error_message,
                error_details = EXCLUDED.error_details,
                started_at = EXCLUDED.started_at,
                completed_at = EXCLUDED.completed_at,
                duration_ms = EXCLUDED.duration_ms,
                retry_count = EXCLUDED.retry_count,
                last_error = EXCLUDED.last_error
            WHERE execution_log.status <> 'completed'::execution_log_status
            "#,
        )
        .bind(entry.workflow_id)
        .bind(&entry.step_name)
        .bind(entry.step_type)
        .bind(entry.execution_attempt)
        .bind(entry.status)
        .bind(&entry.input)
        .bind(&entry.result)
        .bind(&entry.error_message)
        .bind(&entry.error_details)
        .bind(entry.created_at)
        .bind(entry.started_at)
        .bind(entry.completed_at)
        .bind(entry.duration_ms)
        .bind(entry.retry_count)
        .bind(&entry.last_error)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::DatabaseError(e.to_string()))?
        .rows_affected();
        Ok(written > 0)
    }

    async fn mark_running(&self, workflow_id: Uuid, step_name: &str) -> StorageResult<()> {
        sqlx::query(
            r#"
            UPDATE execution_log
            SET
                status = 'running'::execution_log_status,
                started_at = NOW()
            WHERE workflow_id = $1 AND step_name = $2
            "#,
        )
        .bind(workflow_id)
        .bind(step_name)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::DatabaseError(e.to_string()))?;

        Ok(())
    }

    async fn mark_completed(
        &self,
        workflow_id: Uuid,
        step_name: &str,
        result: serde_json::Value,
    ) -> StorageResult<()> {
        sqlx::query(
            r#"
            UPDATE execution_log
            SET
                status = 'completed'::execution_log_status,
                result = $3,
                completed_at = NOW(),
                duration_ms = EXTRACT(EPOCH FROM (NOW() - started_at)) * 1000
            WHERE workflow_id = $1 AND step_name = $2
            "#,
        )
        .bind(workflow_id)
        .bind(step_name)
        .bind(result)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::DatabaseError(e.to_string()))?;

        Ok(())
    }

    async fn mark_failed(
        &self,
        workflow_id: Uuid,
        step_name: &str,
        error_message: &str,
        error_details: Option<serde_json::Value>,
    ) -> StorageResult<()> {
        sqlx::query(
            r#"
            UPDATE execution_log
            SET
                status = 'failed'::execution_log_status,
                error_message = $3,
                error_details = $4,
                completed_at = NOW(),
                duration_ms = EXTRACT(EPOCH FROM (NOW() - started_at)) * 1000,
                retry_count = retry_count + 1,
                last_error = $3
            WHERE workflow_id = $1 AND step_name = $2
            "#,
        )
        .bind(workflow_id)
        .bind(step_name)
        .bind(error_message)
        .bind(error_details)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::DatabaseError(e.to_string()))?;

        Ok(())
    }

    async fn list_entries(&self, workflow_id: Uuid) -> StorageResult<Vec<ExecutionLogEntry>> {
        let results = sqlx::query_as::<_, ExecutionLogEntry>(
            r#"
            SELECT
                workflow_id,
                step_name,
                step_type,
                execution_attempt,
                status,
                input,
                result,
                error_message,
                error_details,
                created_at,
                started_at,
                completed_at,
                duration_ms,
                retry_count,
                last_error
            FROM execution_log
            WHERE workflow_id = $1
            ORDER BY created_at ASC
            "#,
        )
        .bind(workflow_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::DatabaseError(e.to_string()))?;

        Ok(results)
    }

    async fn list_entries_filtered(
        &self,
        filter: &ExecutionLogFilter,
    ) -> StorageResult<Vec<ExecutionLogEntry>> {
        let mut query = String::from(
            r#"
            SELECT
                workflow_id,
                step_name,
                step_type,
                execution_attempt,
                status,
                input,
                result,
                error_message,
                error_details,
                created_at,
                started_at,
                completed_at,
                duration_ms,
                retry_count,
                last_error
            FROM execution_log
            WHERE 1=1
            "#,
        );

        if filter.workflow_id.is_some() {
            query.push_str(" AND workflow_id = $1");
        }
        if filter.status.is_some() {
            query.push_str(" AND status = $2");
        }

        query.push_str(" ORDER BY created_at ASC");

        if let Some(limit) = filter.limit {
            query.push_str(&format!(" LIMIT {}", limit));
        }
        if let Some(offset) = filter.offset {
            query.push_str(&format!(" OFFSET {}", offset));
        }

        let mut q = sqlx::query_as::<_, ExecutionLogEntry>(&query);

        if let Some(wf_id) = filter.workflow_id {
            q = q.bind(wf_id);
        }
        if let Some(status) = filter.status {
            q = q.bind(status);
        }

        let entries = q
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::DatabaseError(e.to_string()))?;

        Ok(entries)
    }

    async fn count_entries(&self, filter: &ExecutionLogFilter) -> StorageResult<i64> {
        let mut query = String::from("SELECT COUNT(*) as count FROM execution_log WHERE 1=1");

        if filter.workflow_id.is_some() {
            query.push_str(" AND workflow_id = $1");
        }
        if filter.status.is_some() {
            query.push_str(" AND status = $2");
        }

        let mut q = sqlx::query_scalar::<_, i64>(&query);

        if let Some(wf_id) = filter.workflow_id {
            q = q.bind(wf_id);
        }
        if let Some(status) = filter.status {
            q = q.bind(status);
        }

        let count = q
            .fetch_one(&self.pool)
            .await
            .map_err(|e| StorageError::DatabaseError(e.to_string()))?;

        Ok(count)
    }

    async fn delete_workflow_entries(&self, workflow_id: Uuid) -> StorageResult<u64> {
        let result = sqlx::query(
            r#"
            DELETE FROM execution_log
            WHERE workflow_id = $1
            "#,
        )
        .bind(workflow_id)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::DatabaseError(e.to_string()))?;

        Ok(result.rows_affected())
    }

    async fn get_workflow_stats(&self, workflow_id: Uuid) -> StorageResult<WorkflowExecutionStats> {
        #[derive(sqlx::FromRow)]
        struct StatsRow {
            total_steps: i64,
            completed_steps: i64,
            failed_steps: i64,
            running_steps: i64,
            pending_steps: i64,
            total_duration_ms: i64,
            avg_duration_ms: Option<f64>,
        }

        let stats = sqlx::query_as::<_, StatsRow>(
            r#"
            SELECT
                COUNT(*) as total_steps,
                COUNT(*) FILTER (WHERE status = 'completed'::execution_log_status) as completed_steps,
                COUNT(*) FILTER (WHERE status = 'failed'::execution_log_status) as failed_steps,
                COUNT(*) FILTER (WHERE status = 'running'::execution_log_status) as running_steps,
                COUNT(*) FILTER (WHERE status = 'pending'::execution_log_status) as pending_steps,
                COALESCE(SUM(duration_ms), 0) as total_duration_ms,
                AVG(duration_ms) as avg_duration_ms
            FROM execution_log
            WHERE workflow_id = $1
            "#,
        )
        .bind(workflow_id)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| StorageError::DatabaseError(e.to_string()))?;

        Ok(WorkflowExecutionStats {
            workflow_id,
            total_steps: stats.total_steps,
            completed_steps: stats.completed_steps,
            failed_steps: stats.failed_steps,
            running_steps: stats.running_steps,
            pending_steps: stats.pending_steps,
            total_duration_ms: stats.total_duration_ms,
            average_step_duration_ms: stats.avg_duration_ms.map(|v| v as i64),
        })
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_execution_log_status_display() {
        assert_eq!(ExecutionLogStatus::Pending.to_string(), "pending");
        assert_eq!(ExecutionLogStatus::Running.to_string(), "running");
        assert_eq!(ExecutionLogStatus::Completed.to_string(), "completed");
        assert_eq!(ExecutionLogStatus::Failed.to_string(), "failed");
    }

    #[test]
    fn test_execution_log_entry_default() {
        let entry = ExecutionLogEntry::default();
        assert_eq!(entry.execution_attempt, 1);
        assert_eq!(entry.status, ExecutionLogStatus::Pending);
        assert_eq!(entry.retry_count, 0);
        assert!(entry.result.is_none());
    }

    #[test]
    fn test_execution_log_filter_default() {
        let filter = ExecutionLogFilter::default();
        assert!(filter.workflow_id.is_none());
        assert!(filter.status.is_none());
        assert!(filter.limit.is_none());
        assert!(filter.offset.is_none());
    }
}
