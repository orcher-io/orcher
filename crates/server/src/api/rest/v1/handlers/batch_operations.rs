//! Batch Operations Handlers
//!
//! This module implements REST API handlers for batch operation management.
//! Batch operations are queried and mutated directly via the database using sqlx.

use crate::error::{Result, ServerError};
use crate::state::AppState;
use axum::{
    extract::{Path, Query, State},
    Json,
};
use serde::{Deserialize, Serialize};
use tracing::debug;
use uuid::Uuid;

// ─── Request / Query structs ─────────────────────────────────────────────────

/// Request body for creating a new batch operation
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateBatchOperationRequest {
    /// Operation type, e.g. "terminate", "cancel", "send_event", "reset"
    pub operation_type: String,
    /// Namespace the batch operation targets
    pub namespace: String,
    /// Optional: only include executions in this status
    pub filter_status: Option<String>,
    /// Optional: only include executions of this workflow type
    pub filter_workflow_type: Option<String>,
    /// Optional: max operations per second (rate limiting)
    pub rate_limit_per_second: Option<i32>,
    /// Event name (used when operation_type = "send_event")
    pub event_name: Option<String>,
    /// Event payload JSON (used when operation_type = "send_event")
    pub event_payload: Option<serde_json::Value>,
    /// Journal entry id to reset to (used when operation_type = "reset")
    pub reset_event_id: Option<i64>,
    /// Human-readable reason for reset
    pub reset_reason: Option<String>,
}

/// Query parameters for listing batch operations
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListBatchOperationsQuery {
    /// Filter by status (pending, processing, completed, failed, terminated)
    pub status: Option<String>,
    /// Maximum number of results
    #[serde(default = "default_limit")]
    pub limit: i64,
    /// Offset for pagination
    #[serde(default)]
    pub offset: i64,
}

fn default_limit() -> i64 {
    100
}

// ─── Response structs ─────────────────────────────────────────────────────────

/// Single batch operation returned to callers
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BatchOperationResponse {
    pub id: Uuid,
    pub operation_type: String,
    pub namespace: String,
    pub filter_status: Option<String>,
    pub filter_workflow_type: Option<String>,
    pub filter_started_after: Option<String>,
    pub filter_started_before: Option<String>,
    pub event_name: Option<String>,
    pub event_payload: Option<serde_json::Value>,
    pub reset_event_id: Option<i64>,
    pub reset_reason: Option<String>,
    pub status: String,
    pub total_count: Option<i64>,
    pub processed_count: Option<i64>,
    pub success_count: Option<i64>,
    pub failure_count: Option<i64>,
    pub rate_limit_per_second: Option<i32>,
    pub error: Option<String>,
    pub created_at: Option<String>,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
    pub created_by: Option<String>,
}

/// List response with pagination metadata
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ListBatchOperationsResponse {
    pub batch_operations: Vec<BatchOperationResponse>,
    pub total: i64,
    pub count: usize,
    pub offset: i64,
}

// ─── Internal DB row ──────────────────────────────────────────────────────────

struct BatchOperationRow {
    id: Uuid,
    operation_type: String,
    namespace: String,
    filter_status: Option<String>,
    filter_workflow_type: Option<String>,
    filter_started_after: Option<orcher_common::types::time::Timestamptz>,
    filter_started_before: Option<orcher_common::types::time::Timestamptz>,
    event_name: Option<String>,
    event_payload: Option<serde_json::Value>,
    reset_event_id: Option<i64>,
    reset_reason: Option<String>,
    status: String,
    total_count: Option<i64>,
    processed_count: Option<i64>,
    success_count: Option<i64>,
    failure_count: Option<i64>,
    rate_limit_per_second: Option<i32>,
    error: Option<String>,
    created_at: Option<orcher_common::types::time::Timestamptz>,
    started_at: Option<orcher_common::types::time::Timestamptz>,
    completed_at: Option<orcher_common::types::time::Timestamptz>,
    created_by: Option<String>,
}

impl sqlx::FromRow<'_, sqlx::postgres::PgRow> for BatchOperationRow {
    fn from_row(row: &sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(BatchOperationRow {
            id: row.try_get("id")?,
            operation_type: row.try_get("operation_type")?,
            namespace: row.try_get("namespace")?,
            filter_status: row.try_get("filter_status")?,
            filter_workflow_type: row.try_get("filter_workflow_type")?,
            filter_started_after: row.try_get("filter_started_after")?,
            filter_started_before: row.try_get("filter_started_before")?,
            event_name: row.try_get("event_name")?,
            event_payload: row.try_get("event_payload")?,
            reset_event_id: row.try_get("reset_event_id")?,
            reset_reason: row.try_get("reset_reason")?,
            status: row.try_get("status")?,
            total_count: row.try_get("total_count")?,
            processed_count: row.try_get("processed_count")?,
            success_count: row.try_get("success_count")?,
            failure_count: row.try_get("failure_count")?,
            rate_limit_per_second: row.try_get("rate_limit_per_second")?,
            error: row.try_get("error")?,
            created_at: row.try_get("created_at")?,
            started_at: row.try_get("started_at")?,
            completed_at: row.try_get("completed_at")?,
            created_by: row.try_get("created_by")?,
        })
    }
}

fn row_to_response(row: BatchOperationRow) -> BatchOperationResponse {
    BatchOperationResponse {
        id: row.id,
        operation_type: row.operation_type,
        namespace: row.namespace,
        filter_status: row.filter_status,
        filter_workflow_type: row.filter_workflow_type,
        filter_started_after: row.filter_started_after.map(|t| t.to_string()),
        filter_started_before: row.filter_started_before.map(|t| t.to_string()),
        event_name: row.event_name,
        event_payload: row.event_payload,
        reset_event_id: row.reset_event_id,
        reset_reason: row.reset_reason,
        status: row.status,
        total_count: row.total_count,
        processed_count: row.processed_count,
        success_count: row.success_count,
        failure_count: row.failure_count,
        rate_limit_per_second: row.rate_limit_per_second,
        error: row.error,
        created_at: row.created_at.map(|t| t.to_string()),
        started_at: row.started_at.map(|t| t.to_string()),
        completed_at: row.completed_at.map(|t| t.to_string()),
        created_by: row.created_by,
    }
}

// ─── Handlers ─────────────────────────────────────────────────────────────────

/// Create a batch operation
///
/// # Request
///
/// POST /api/v1/batch-operations
///
/// ```json
/// {
///   "operationType": "terminate",
///   "namespace": "default",
///   "filterStatus": "running",
///   "filterWorkflowType": "my-workflow",
///   "rateLimitPerSecond": 10
/// }
/// ```
///
/// # Response
///
/// Returns the created `BatchOperationResponse`.
/// Refuse an operation that would fail on every workflow it matched, before
/// it is queued: an unknown or unsupported type, or an event with no name.
fn validate_operation(body: &CreateBatchOperationRequest) -> Result<()> {
    match body.operation_type.as_str() {
        "cancel" | "terminate" => Ok(()),
        "send_event" => match body.event_name.as_deref() {
            Some(name) if !name.trim().is_empty() => Ok(()),
            _ => Err(ServerError::validation(
                "eventName is required for a send_event batch operation",
            )),
        },
        "reset" => Err(ServerError::validation(
            "reset is not supported as a batch operation; reset workflows one at a time",
        )),
        other => Err(ServerError::validation(format!(
            "unknown operationType {other:?}: cancel, terminate or send_event"
        ))),
    }
}

pub async fn create_batch_operation(
    State(state): State<AppState>,
    Json(body): Json<CreateBatchOperationRequest>,
) -> Result<Json<BatchOperationResponse>> {
    debug!(
        operation_type = %body.operation_type,
        namespace = %body.namespace,
        "Creating batch operation"
    );
    validate_operation(&body)?;

    let pool = state.pool();

    let row = sqlx::query_as::<_, BatchOperationRow>(
        r#"
        INSERT INTO batch_operations (
            id,
            organization_id,
            operation_type,
            namespace,
            filter_status,
            filter_workflow_type,
            event_name,
            event_payload,
            reset_event_id,
            reset_reason,
            rate_limit_per_second,
            status,
            created_at
        ) VALUES (
            gen_random_uuid(),
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
            'pending',
            NOW()
        )
        RETURNING
            id, operation_type, namespace,
            filter_status, filter_workflow_type,
            filter_started_after, filter_started_before,
            event_name, event_payload,
            reset_event_id, reset_reason,
            status,
            total_count, processed_count, success_count, failure_count,
            rate_limit_per_second, error,
            created_at, started_at, completed_at, created_by
        "#,
    )
    // Stamp the owner. Without it the operation is unowned, and an unowned
    // operation matches workflows by namespace name across every organisation
    // that happens to use that name.
    .bind(orcher_core::grpc::tenant::current_organization())
    .bind(&body.operation_type)
    .bind(&body.namespace)
    .bind(&body.filter_status)
    .bind(&body.filter_workflow_type)
    .bind(&body.event_name)
    .bind(&body.event_payload)
    .bind(body.reset_event_id)
    .bind(&body.reset_reason)
    .bind(body.rate_limit_per_second)
    .fetch_one(pool)
    .await
    .map_err(|e| ServerError::internal(format!("Failed to create batch operation: {}", e)))?;

    Ok(Json(row_to_response(row)))
}

/// List batch operations
///
/// # Request
///
/// GET /api/v1/batch-operations?status=pending&limit=50&offset=0
///
/// # Response
///
/// ```json
/// {
///   "batchOperations": [...],
///   "total": 5,
///   "count": 5,
///   "offset": 0
/// }
/// ```
pub async fn list_batch_operations(
    State(state): State<AppState>,
    Query(mut query): Query<ListBatchOperationsQuery>,
) -> Result<Json<ListBatchOperationsResponse>> {
    // Clamp client-supplied pagination to a safe range (see MAX_PAGE_SIZE).
    query.limit = query.limit.clamp(1, super::MAX_PAGE_SIZE);
    query.offset = query.offset.max(0);

    debug!(
        limit = query.limit,
        offset = query.offset,
        status = ?query.status,
        "Listing batch operations"
    );

    let pool = state.pool();

    // Scoped to the caller. A NULL caller is an unscoped deployment and sees
    // everything, which is that deployment's own data; a caller acting for an
    // organisation sees only its own, where before it saw every tenant's
    // operations without needing to guess a single id.
    let caller = orcher_core::grpc::tenant::current_organization();

    // Count total matching rows
    let total: i64 = sqlx::query_scalar(
        r#"
        SELECT COUNT(*)
        FROM batch_operations
        WHERE ($1::text IS NULL OR status = $1)
          AND ($2::uuid IS NULL OR organization_id = $2)
        "#,
    )
    .bind(&query.status)
    .bind(caller)
    .fetch_one(pool)
    .await
    .map_err(|e| ServerError::internal(format!("Failed to count batch operations: {}", e)))?;

    // Fetch paginated rows
    let rows = sqlx::query_as::<_, BatchOperationRow>(
        r#"
        SELECT
            id, operation_type, namespace,
            filter_status, filter_workflow_type,
            filter_started_after, filter_started_before,
            event_name, event_payload,
            reset_event_id, reset_reason,
            status,
            total_count, processed_count, success_count, failure_count,
            rate_limit_per_second, error,
            created_at, started_at, completed_at, created_by
        FROM batch_operations
        WHERE ($1::text IS NULL OR status = $1)
          AND ($4::uuid IS NULL OR organization_id = $4)
        ORDER BY created_at DESC
        LIMIT $2 OFFSET $3
        "#,
    )
    .bind(&query.status)
    .bind(query.limit)
    .bind(query.offset)
    .bind(caller)
    .fetch_all(pool)
    .await
    .map_err(|e| ServerError::internal(format!("Failed to list batch operations: {}", e)))?;

    let batch_operations: Vec<BatchOperationResponse> =
        rows.into_iter().map(row_to_response).collect();
    let count = batch_operations.len();

    Ok(Json(ListBatchOperationsResponse {
        batch_operations,
        total,
        count,
        offset: query.offset,
    }))
}

/// Get a single batch operation by ID
///
/// # Request
///
/// GET /api/v1/batch-operations/:id
///
/// # Response
///
/// Returns the `BatchOperationResponse` or 404 if not found.
pub async fn get_batch_operation(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<BatchOperationResponse>> {
    debug!(%id, "Getting batch operation");

    let pool = state.pool();

    let row = sqlx::query_as::<_, BatchOperationRow>(
        r#"
        SELECT
            id, operation_type, namespace,
            filter_status, filter_workflow_type,
            filter_started_after, filter_started_before,
            event_name, event_payload,
            reset_event_id, reset_reason,
            status,
            total_count, processed_count, success_count, failure_count,
            rate_limit_per_second, error,
            created_at, started_at, completed_at, created_by
        FROM batch_operations
        WHERE id = $1
          AND ($2::uuid IS NULL OR organization_id = $2)
        "#,
    )
    .bind(id)
    .bind(orcher_core::grpc::tenant::current_organization())
    .fetch_optional(pool)
    .await
    .map_err(|e| ServerError::internal(format!("Failed to get batch operation: {}", e)))?
    .ok_or_else(|| ServerError::not_found(format!("Batch operation {} not found", id)))?;

    Ok(Json(row_to_response(row)))
}

/// Terminate a batch operation
///
/// # Request
///
/// POST /api/v1/batch-operations/:id/terminate
///
/// Sets status to "terminated" only when the operation is currently
/// "pending" or "processing". Returns 404 if the operation does not
/// exist, or 409 if it cannot be terminated in its current state.
pub async fn terminate_batch_operation(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<BatchOperationResponse>> {
    debug!(%id, "Terminating batch operation");

    let pool = state.pool();

    // Distinguish "doesn't exist" (404) from "exists but can't be terminated"
    // (409) — the previous single UPDATE reported both as 404.
    let current_status: Option<(String,)> = sqlx::query_as(
        "SELECT status FROM batch_operations \
             WHERE id = $1 AND ($2::uuid IS NULL OR organization_id = $2)",
    )
    .bind(id)
    .bind(orcher_core::grpc::tenant::current_organization())
    .fetch_optional(pool)
    .await
    .map_err(|e| ServerError::internal(format!("Failed to query batch operation: {}", e)))?;
    let Some((current_status,)) = current_status else {
        return Err(ServerError::not_found(format!(
            "Batch operation {} not found",
            id
        )));
    };

    let row = sqlx::query_as::<_, BatchOperationRow>(
        r#"
        UPDATE batch_operations
        SET status = 'terminated'
        WHERE id = $1
          AND ($2::uuid IS NULL OR organization_id = $2)
          AND status IN ('pending', 'processing')
        RETURNING
            id, operation_type, namespace,
            filter_status, filter_workflow_type,
            filter_started_after, filter_started_before,
            event_name, event_payload,
            reset_event_id, reset_reason,
            status,
            total_count, processed_count, success_count, failure_count,
            rate_limit_per_second, error,
            created_at, started_at, completed_at, created_by
        "#,
    )
    .bind(id)
    .bind(orcher_core::grpc::tenant::current_organization())
    .fetch_optional(pool)
    .await
    .map_err(|e| ServerError::internal(format!("Failed to terminate batch operation: {}", e)))?
    .ok_or_else(|| {
        ServerError::conflict(format!(
            "Batch operation {} cannot be terminated in its current state ({})",
            id, current_status
        ))
    })?;

    Ok(Json(row_to_response(row)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_limit() {
        assert_eq!(default_limit(), 100);
    }

    /// An operation that would fail on every workflow is refused when it is
    /// created, not queued to fail row by row.
    #[test]
    fn an_operation_that_cannot_run_is_refused_when_created() {
        let request = |operation: &str, event: Option<&str>| {
            serde_json::from_value::<CreateBatchOperationRequest>(serde_json::json!({
                "operationType": operation,
                "namespace": "default",
                "eventName": event,
            }))
            .unwrap()
        };
        assert!(validate_operation(&request("cancel", None)).is_ok());
        assert!(validate_operation(&request("terminate", None)).is_ok());
        assert!(validate_operation(&request("send_event", Some("go"))).is_ok());
        for refused in [
            request("send_event", None),
            request("send_event", Some("  ")),
            request("reset", None),
            request("teleport", None),
        ] {
            let error = validate_operation(&refused).unwrap_err();
            assert_eq!(
                error.status_code(),
                axum::http::StatusCode::BAD_REQUEST,
                "{error}"
            );
        }
    }
}
