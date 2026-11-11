//! # Child Workflow Execution Repository
//!
//! Repository for child workflow executions and parent-child relationships.
//! Provides CRUD operations for:
//! - Creating child workflow records
//! - Tracking child workflow lifecycle
//! - Querying parent-child relationships
//! - Managing child workflow state transitions
//!
//! ## Usage
//!
//! ```rust,ignore
//! use orcher_storage::repositories::domains::child_workflow::ChildWorkflowRepository;
//! use orcher_storage::schema::types::child_workflow::{ChildWorkflowExecution, OrphanPolicy};
//!
//! let repo = ChildWorkflowRepository::new(pool);
//!
//! // Create child workflow record
//! let child = ChildWorkflowExecution::new(
//!     "parent-wf".to_string(),
//!     "parent-exec".to_string(),
//!     1,
//!     "child-wf".to_string(),
//!     "child-exec".to_string(),
//!     "ChildWorkflow".to_string(),
//!     "default".to_string(),
//!     serde_json::json!({"input": "data"}),
//!     OrphanPolicy::Wait,
//! );
//! let created = repo.create(&child).await?;
//!
//! // Get children for parent
//! let children = repo.get_by_parent("parent-wf", "parent-exec").await?;
//!
//! // Update child state
//! repo.update_state(child.id, ChildWorkflowState::Started, None).await?;
//! ```

use orcher_common::types::time::Timestamptz;
use orcher_common::{error::OrcherError, OrcherResult};
use sqlx::PgPool;
use tracing::{debug, info, warn};

use crate::schema::types::child_workflow::{
    ChildWorkflowExecution, ChildWorkflowQueryFilters, ChildWorkflowState, OrphanPolicy,
};

/// Database row representation for child workflow executions
#[derive(Debug, Clone, sqlx::FromRow)]
struct ChildWorkflowExecutionRow {
    id: i64,
    parent_workflow_id: String,
    parent_execution_id: String,
    parent_initiated_event_id: i64,
    parent_started_event_id: Option<i64>,
    child_workflow_id: String,
    child_execution_id: String,
    child_workflow_type: String,
    task_queue: String,
    input: serde_json::Value,
    // Physical column is `parent_close_policy` (V015); the domain type is
    // `OrphanPolicy`. Keep the field name matching the column so the
    // column list read back (`columns::child_workflow_executions`) maps.
    parent_close_policy: String,
    state: String,
    result: Option<serde_json::Value>,
    failure_reason: Option<String>,
    failure_details: Option<serde_json::Value>,
    initiated_at: Timestamptz,
    started_at: Option<Timestamptz>,
    completed_at: Option<Timestamptz>,
    created_at: Timestamptz,
    updated_at: Timestamptz,
    // The shard this row belongs to
    shard_id: Option<i32>,
}

impl From<ChildWorkflowExecutionRow> for ChildWorkflowExecution {
    fn from(row: ChildWorkflowExecutionRow) -> Self {
        Self {
            id: row.id,
            parent_workflow_id: row.parent_workflow_id,
            parent_execution_id: row.parent_execution_id,
            parent_initiated_event_id: row.parent_initiated_event_id,
            parent_started_event_id: row.parent_started_event_id,
            child_workflow_id: row.child_workflow_id,
            child_execution_id: row.child_execution_id,
            child_workflow_type: row.child_workflow_type,
            task_queue: row.task_queue,
            input: row.input,
            orphan_policy: OrphanPolicy::from_str(&row.parent_close_policy)
                .unwrap_or(OrphanPolicy::Abandon),
            state: ChildWorkflowState::from_str(&row.state)
                .unwrap_or(ChildWorkflowState::Initiated),
            result: row.result,
            failure_reason: row.failure_reason,
            failure_details: row.failure_details,
            initiated_at: row.initiated_at,
            started_at: row.started_at,
            completed_at: row.completed_at,
            created_at: row.created_at,
            updated_at: row.updated_at,
            shard_id: row.shard_id,
        }
    }
}

/// Repository for child workflow execution persistence operations
pub struct ChildWorkflowRepository {
    pool: PgPool,
}

impl ChildWorkflowRepository {
    /// Create a new child workflow repository
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Create a new child workflow execution record
    ///
    /// # Arguments
    ///
    /// * `child` - The child workflow execution to create
    ///
    /// # Returns
    ///
    /// The created child workflow execution with database-generated ID
    pub async fn create(
        &self,
        child: &ChildWorkflowExecution,
    ) -> OrcherResult<ChildWorkflowExecution> {
        debug!(
            parent_workflow_id = %child.parent_workflow_id,
            parent_execution_id = %child.parent_execution_id,
            child_workflow_id = %child.child_workflow_id,
            child_execution_id = %child.child_execution_id,
            "Creating child workflow execution record"
        );

        let row = sqlx::query_as::<_, ChildWorkflowExecutionRow>(concat!(
            r#"
            INSERT INTO child_workflow_executions (
                parent_workflow_id, parent_execution_id, parent_initiated_event_id,
                parent_started_event_id, child_workflow_id, child_execution_id,
                child_workflow_type, task_queue, input, parent_close_policy,
                state, result, failure_reason, failure_details,
                initiated_at, started_at, completed_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
            RETURNING "#,
            crate::repositories::columns::child_workflow_executions!(),
            r#"
            "#
        ))
        .bind(&child.parent_workflow_id)
        .bind(&child.parent_execution_id)
        .bind(child.parent_initiated_event_id)
        .bind(child.parent_started_event_id)
        .bind(&child.child_workflow_id)
        .bind(&child.child_execution_id)
        .bind(&child.child_workflow_type)
        .bind(&child.task_queue)
        .bind(&child.input)
        .bind(child.orphan_policy.as_str())
        .bind(child.state.as_str())
        .bind(&child.result)
        .bind(&child.failure_reason)
        .bind(&child.failure_details)
        .bind(child.initiated_at)
        .bind(child.started_at)
        .bind(child.completed_at)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| {
            warn!(
                error = %e,
                parent_workflow_id = %child.parent_workflow_id,
                child_workflow_id = %child.child_workflow_id,
                "Failed to create child workflow execution"
            );
            OrcherError::internal(format!("Failed to create child workflow execution: {}", e))
        })?;

        info!(
            parent_workflow_id = %child.parent_workflow_id,
            child_workflow_id = %child.child_workflow_id,
            id = row.id,
            "Child workflow execution record created successfully"
        );

        Ok(row.into())
    }

    /// Get child workflow execution by ID
    ///
    /// # Arguments
    ///
    /// * `id` - The database ID
    pub async fn get_by_id(&self, id: i64) -> OrcherResult<Option<ChildWorkflowExecution>> {
        debug!(id = id, "Getting child workflow execution by ID");

        let row = sqlx::query_as::<_, ChildWorkflowExecutionRow>(concat!(
            "SELECT ",
            crate::repositories::columns::child_workflow_executions!(),
            " FROM child_workflow_executions WHERE id = $1"
        ))
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| {
            warn!(error = %e, "Failed to get child workflow execution");
            OrcherError::internal(format!("Failed to get child workflow execution: {}", e))
        })?;

        Ok(row.map(Into::into))
    }

    /// Get child workflow execution by child identifiers
    ///
    /// # Arguments
    ///
    /// * `child_workflow_id` - The child workflow identifier
    /// * `child_execution_id` - The child execution identifier
    pub async fn get_by_child(
        &self,
        child_workflow_id: &str,
        child_execution_id: &str,
    ) -> OrcherResult<Option<ChildWorkflowExecution>> {
        debug!(
            child_workflow_id = %child_workflow_id,
            child_execution_id = %child_execution_id,
            "Getting child workflow execution by child identifiers"
        );

        let row = sqlx::query_as::<_, ChildWorkflowExecutionRow>(concat!(
            r#"
            SELECT "#,
            crate::repositories::columns::child_workflow_executions!(),
            r#" FROM child_workflow_executions
            WHERE child_workflow_id = $1 AND child_execution_id = $2
            "#
        ))
        .bind(child_workflow_id)
        .bind(child_execution_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| {
            warn!(error = %e, "Failed to get child workflow execution by child identifiers");
            OrcherError::internal(format!(
                "Failed to get child workflow execution by child identifiers: {}",
                e
            ))
        })?;

        Ok(row.map(Into::into))
    }

    /// Get all child workflow executions for a parent
    ///
    /// # Arguments
    ///
    /// * `parent_workflow_id` - The parent workflow identifier
    /// * `parent_execution_id` - The parent execution identifier
    pub async fn get_by_parent(
        &self,
        parent_workflow_id: &str,
        parent_execution_id: &str,
    ) -> OrcherResult<Vec<ChildWorkflowExecution>> {
        debug!(
            parent_workflow_id = %parent_workflow_id,
            parent_execution_id = %parent_execution_id,
            "Getting child workflow executions for parent"
        );

        let rows = sqlx::query_as::<_, ChildWorkflowExecutionRow>(concat!(
            r#"
            SELECT "#,
            crate::repositories::columns::child_workflow_executions!(),
            r#" FROM child_workflow_executions
            WHERE parent_workflow_id = $1 AND parent_execution_id = $2
            ORDER BY parent_initiated_event_id ASC
            "#
        ))
        .bind(parent_workflow_id)
        .bind(parent_execution_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            warn!(error = %e, "Failed to get child workflow executions for parent");
            OrcherError::internal(format!(
                "Failed to get child workflow executions for parent: {}",
                e
            ))
        })?;

        debug!(
            parent_workflow_id = %parent_workflow_id,
            parent_execution_id = %parent_execution_id,
            child_count = rows.len(),
            "Retrieved child workflow executions for parent"
        );

        Ok(rows.into_iter().map(Into::into).collect())
    }

    /// Get a child workflow execution by its parent + business child id.
    ///
    /// The business `child_workflow_id` is parent-assigned and unique within a
    /// parent execution (e.g. `child_1`), so this identifies at most one child.
    /// Used to make StartChildWorkflow idempotent: a parent replaying after a
    /// suspend re-emits the start command for children that are still pending,
    /// and this lookup lets the engine return the existing child instead of
    /// creating a duplicate execution.
    ///
    /// # Arguments
    ///
    /// * `parent_execution_id` - The parent execution identifier
    /// * `child_workflow_id` - The parent-assigned business child id
    pub async fn get_by_parent_and_child_id(
        &self,
        parent_execution_id: &str,
        child_workflow_id: &str,
    ) -> OrcherResult<Option<ChildWorkflowExecution>> {
        debug!(
            parent_execution_id = %parent_execution_id,
            child_workflow_id = %child_workflow_id,
            "Getting child workflow execution by parent + child id"
        );

        // ORDER BY + LIMIT 1 defensively collapses any pre-existing duplicates
        // (from before starts were idempotent) to the earliest-initiated child.
        let row = sqlx::query_as::<_, ChildWorkflowExecutionRow>(concat!(
            r#"
            SELECT "#,
            crate::repositories::columns::child_workflow_executions!(),
            r#" FROM child_workflow_executions
            WHERE parent_execution_id = $1 AND child_workflow_id = $2
            ORDER BY parent_initiated_event_id ASC
            LIMIT 1
            "#
        ))
        .bind(parent_execution_id)
        .bind(child_workflow_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| {
            warn!(error = %e, "Failed to get child workflow execution by parent + child id");
            OrcherError::internal(format!(
                "Failed to get child workflow execution by parent + child id: {}",
                e
            ))
        })?;

        Ok(row.map(Into::into))
    }

    /// Get pending (non-terminal) child workflows for a parent
    ///
    /// Returns children in INITIATED or STARTED states.
    ///
    /// # Arguments
    ///
    /// * `parent_workflow_id` - The parent workflow identifier
    /// * `parent_execution_id` - The parent execution identifier
    pub async fn get_pending_for_parent(
        &self,
        parent_workflow_id: &str,
        parent_execution_id: &str,
    ) -> OrcherResult<Vec<ChildWorkflowExecution>> {
        debug!(
            parent_workflow_id = %parent_workflow_id,
            parent_execution_id = %parent_execution_id,
            "Getting pending child workflows for parent"
        );

        let rows = sqlx::query_as::<_, ChildWorkflowExecutionRow>(concat!(
            r#"
            SELECT "#,
            crate::repositories::columns::child_workflow_executions!(),
            r#" FROM child_workflow_executions
            WHERE parent_workflow_id = $1 AND parent_execution_id = $2
              AND state IN ('INITIATED', 'STARTED')
            ORDER BY parent_initiated_event_id ASC
            "#
        ))
        .bind(parent_workflow_id)
        .bind(parent_execution_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            warn!(error = %e, "Failed to get pending child workflows");
            OrcherError::internal(format!("Failed to get pending child workflows: {}", e))
        })?;

        Ok(rows.into_iter().map(Into::into).collect())
    }

    /// Get child workflows with a specific parent close policy
    ///
    /// # Arguments
    ///
    /// * `parent_workflow_id` - The parent workflow identifier
    /// * `parent_execution_id` - The parent execution identifier
    /// * `policy` - The parent close policy to filter by
    pub async fn get_by_parent_and_policy(
        &self,
        parent_workflow_id: &str,
        parent_execution_id: &str,
        policy: OrphanPolicy,
    ) -> OrcherResult<Vec<ChildWorkflowExecution>> {
        debug!(
            parent_workflow_id = %parent_workflow_id,
            parent_execution_id = %parent_execution_id,
            policy = %policy,
            "Getting child workflows by parent and policy"
        );

        let rows = sqlx::query_as::<_, ChildWorkflowExecutionRow>(concat!(
            r#"
            SELECT "#,
            crate::repositories::columns::child_workflow_executions!(),
            r#" FROM child_workflow_executions
            WHERE parent_workflow_id = $1 AND parent_execution_id = $2
              AND parent_close_policy = $3
            ORDER BY parent_initiated_event_id ASC
            "#
        ))
        .bind(parent_workflow_id)
        .bind(parent_execution_id)
        .bind(policy.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            warn!(error = %e, "Failed to get child workflows by policy");
            OrcherError::internal(format!("Failed to get child workflows by policy: {}", e))
        })?;

        Ok(rows.into_iter().map(Into::into).collect())
    }

    /// Query child workflow executions with filters
    ///
    /// # Arguments
    ///
    /// * `filters` - Query filters to apply
    pub async fn query(
        &self,
        filters: ChildWorkflowQueryFilters,
    ) -> OrcherResult<Vec<ChildWorkflowExecution>> {
        let mut query = String::from(concat!(
            "SELECT ",
            crate::repositories::columns::child_workflow_executions!(),
            " FROM child_workflow_executions WHERE 1=1"
        ));
        let mut bind_index = 1;

        // Build dynamic query based on filters
        if filters.parent_workflow_id.is_some() {
            query.push_str(&format!(" AND parent_workflow_id = ${}", bind_index));
            bind_index += 1;
        }

        if filters.parent_execution_id.is_some() {
            query.push_str(&format!(" AND parent_execution_id = ${}", bind_index));
            bind_index += 1;
        }

        if filters.child_workflow_id.is_some() {
            query.push_str(&format!(" AND child_workflow_id = ${}", bind_index));
            bind_index += 1;
        }

        if filters.child_execution_id.is_some() {
            query.push_str(&format!(" AND child_execution_id = ${}", bind_index));
            bind_index += 1;
        }

        if filters.state.is_some() {
            query.push_str(&format!(" AND state = ${}", bind_index));
            bind_index += 1;
        }

        if filters.orphan_policy.is_some() {
            query.push_str(&format!(" AND parent_close_policy = ${}", bind_index));
            bind_index += 1;
        }

        if filters.only_running {
            query.push_str(" AND state IN ('INITIATED', 'STARTED')");
        }

        if filters.only_terminal {
            query.push_str(
                " AND state IN ('COMPLETED', 'FAILED', 'TIMED_OUT', 'CANCELED', 'TERMINATED')",
            );
        }

        // Add ordering
        query.push_str(" ORDER BY parent_initiated_event_id ASC");

        // Add pagination
        if let Some(_limit) = filters.limit {
            query.push_str(&format!(" LIMIT ${}", bind_index));
            bind_index += 1;
        }

        if let Some(_offset) = filters.offset {
            query.push_str(&format!(" OFFSET ${}", bind_index));
        }

        // Build query with bindings
        let mut sql_query = sqlx::query_as::<_, ChildWorkflowExecutionRow>(&query);

        if let Some(ref parent_wf_id) = filters.parent_workflow_id {
            sql_query = sql_query.bind(parent_wf_id);
        }
        if let Some(ref parent_exec_id) = filters.parent_execution_id {
            sql_query = sql_query.bind(parent_exec_id);
        }
        if let Some(ref child_wf_id) = filters.child_workflow_id {
            sql_query = sql_query.bind(child_wf_id);
        }
        if let Some(ref child_exec_id) = filters.child_execution_id {
            sql_query = sql_query.bind(child_exec_id);
        }
        if let Some(state) = filters.state {
            sql_query = sql_query.bind(state.as_str());
        }
        if let Some(policy) = filters.orphan_policy {
            sql_query = sql_query.bind(policy.as_str());
        }
        if let Some(lim) = filters.limit {
            sql_query = sql_query.bind(lim);
        }
        if let Some(off) = filters.offset {
            sql_query = sql_query.bind(off);
        }

        let rows = sql_query.fetch_all(&self.pool).await.map_err(|e| {
            warn!(error = %e, "Failed to query child workflow executions");
            OrcherError::internal(format!("Failed to query child workflow executions: {}", e))
        })?;

        Ok(rows.into_iter().map(Into::into).collect())
    }

    /// Update the state of a child workflow execution
    ///
    /// # Arguments
    ///
    /// * `id` - The database ID
    /// * `state` - The new state
    /// * `result` - Optional result data (for completed state)
    pub async fn update_state(
        &self,
        id: i64,
        state: ChildWorkflowState,
        result: Option<serde_json::Value>,
    ) -> OrcherResult<()> {
        debug!(
            id = id,
            state = %state,
            "Updating child workflow execution state"
        );

        let now = Timestamptz::now();

        let mut update_query = String::from(
            "UPDATE child_workflow_executions SET state = $1, result = $2, updated_at = $3",
        );
        let mut bind_index = 4;

        // Set completed_at for terminal states
        if state.is_terminal() {
            update_query.push_str(&format!(", completed_at = ${}", bind_index));
            bind_index += 1;
        }

        update_query.push_str(&format!(" WHERE id = ${}", bind_index));

        let mut query = sqlx::query(&update_query)
            .bind(state.as_str())
            .bind(&result)
            .bind(now);

        if state.is_terminal() {
            query = query.bind(now);
        }

        query = query.bind(id);

        let result = query.execute(&self.pool).await.map_err(|e| {
            warn!(error = %e, "Failed to update child workflow execution state");
            OrcherError::internal(format!(
                "Failed to update child workflow execution state: {}",
                e
            ))
        })?;

        if result.rows_affected() == 0 {
            return Err(OrcherError::not_found(
                "ChildWorkflowExecution",
                id.to_string(),
            ));
        }

        info!(
            id = id,
            state = %state,
            "Child workflow execution state updated successfully"
        );

        Ok(())
    }

    /// The tracking row for the run `child_execution_id`, if that run is
    /// somebody's child.
    ///
    /// The row follows its child from run to run — a retry, a restart-fresh
    /// or a reset moves it to the successor — so this is how a run that has
    /// just finished finds its parent. A run with no row is not a child; in
    /// particular a successor is not the child of the run it replaced, even
    /// though its `parent_execution_id` points there.
    ///
    /// Rows duplicated before starts were idempotent can share a run; the
    /// earliest is returned, and the reconciliation sweep settles the rest.
    pub async fn get_by_child_execution_id(
        &self,
        child_execution_id: &str,
    ) -> OrcherResult<Option<ChildWorkflowExecution>> {
        let row = sqlx::query_as::<_, ChildWorkflowExecutionRow>(concat!(
            r#"
            SELECT "#,
            crate::repositories::columns::child_workflow_executions!(),
            r#" FROM child_workflow_executions
            WHERE child_execution_id = $1
            ORDER BY id ASC
            LIMIT 1
            "#
        ))
        .bind(child_execution_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| {
            OrcherError::internal(format!(
                "Failed to get child workflow execution by run {}: {}",
                child_execution_id, e
            ))
        })?;

        Ok(row.map(Into::into))
    }

    /// Move every row tracking run `old` to its successor run `new`.
    ///
    /// Called when a child's run is replaced — retried, continued as new or
    /// reset — so the row keeps naming the run whose outcome the parent is
    /// waiting for. A no-op for a run that is not a child. Returns how many
    /// rows moved.
    pub async fn repoint_child_execution(&self, old: &str, new: &str) -> OrcherResult<u64> {
        let moved = sqlx::query(
            r#"
            UPDATE child_workflow_executions
            SET child_execution_id = $2
            WHERE child_execution_id = $1
            "#,
        )
        .bind(old)
        .bind(new)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            OrcherError::internal(format!(
                "Failed to move child tracking from run {} to {}: {}",
                old, new, e
            ))
        })?
        .rows_affected();

        if moved > 0 {
            debug!(old = %old, new = %new, moved, "Child tracking moved to successor run");
        }
        Ok(moved)
    }

    /// Close the open row `id` with how its child ended, if it is still open.
    ///
    /// Conditional on the row being open (INITIATED or STARTED), so of any
    /// number of callers delivering the same outcome at once — the completion
    /// path and a sweep on every replica — exactly one gets `true`. That one
    /// wakes the parent; the others must not, since by then the parent may be
    /// running again, and releasing its claim would hand it to a second worker.
    pub async fn settle_open(
        &self,
        id: i64,
        state: ChildWorkflowState,
        result: Option<serde_json::Value>,
        failure_reason: Option<String>,
    ) -> OrcherResult<bool> {
        let settled = sqlx::query(
            r#"
            UPDATE child_workflow_executions
            SET state = $2,
                result = COALESCE($3, result),
                failure_reason = COALESCE($4, failure_reason),
                completed_at = GREATEST(NOW(), initiated_at, COALESCE(started_at, initiated_at)),
                updated_at = NOW()
            WHERE id = $1
              AND state IN ('INITIATED', 'STARTED')
            "#,
        )
        .bind(id)
        .bind(state.as_str())
        .bind(&result)
        .bind(&failure_reason)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            OrcherError::internal(format!("Failed to settle child workflow {}: {}", id, e))
        })?
        .rows_affected();
        Ok(settled > 0)
    }

    /// Up to `limit` rows whose outcome has not reached their parent yet
    /// (INITIATED or STARTED), in id order after `after_id`.
    ///
    /// Paged by id so a sweep walks the whole set a batch at a time and wraps
    /// around, rather than re-reading the same first batch on every pass.
    pub async fn list_open_after(
        &self,
        after_id: i64,
        limit: i64,
    ) -> OrcherResult<Vec<ChildWorkflowExecution>> {
        let rows = sqlx::query_as::<_, ChildWorkflowExecutionRow>(concat!(
            r#"
            SELECT "#,
            crate::repositories::columns::child_workflow_executions!(),
            r#" FROM child_workflow_executions
            WHERE state IN ('INITIATED', 'STARTED')
              AND id > $1
            ORDER BY id ASC
            LIMIT $2
            "#
        ))
        .bind(after_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            OrcherError::internal(format!("Failed to list open child workflows: {}", e))
        })?;

        Ok(rows.into_iter().map(Into::into).collect())
    }

    /// Delete a child workflow execution record
    ///
    /// ⚠️ WARNING: This is a destructive operation. Use with caution.
    /// Typically only used for testing or data cleanup.
    ///
    /// # Arguments
    ///
    /// * `id` - The database ID
    pub async fn delete(&self, id: i64) -> OrcherResult<()> {
        warn!(
            id = id,
            "Deleting child workflow execution (DESTRUCTIVE OPERATION)"
        );

        let result = sqlx::query("DELETE FROM child_workflow_executions WHERE id = $1")
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                warn!(error = %e, "Failed to delete child workflow execution");
                OrcherError::internal(format!("Failed to delete child workflow execution: {}", e))
            })?;

        if result.rows_affected() == 0 {
            return Err(OrcherError::not_found(
                "ChildWorkflowExecution",
                id.to_string(),
            ));
        }

        warn!(id = id, "Child workflow execution deleted");

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: These tests require a PostgreSQL database with the V024 migration applied.
    // Run with: cargo test --package orcher-storage --lib repositories::domains::child_workflow

    #[test]
    fn test_repository_creation() {
        // This is a compile-time test to ensure the repository can be constructed
        fn _assert_repository_constructible() {
            let pool =
                PgPool::connect_lazy("postgresql://localhost/test").expect("Failed to create pool");
            let _repo = ChildWorkflowRepository::new(pool);
        }
    }
}
