//! Schedule Management Handlers
//!
//! REST API handlers for schedule CRUD, pause/resume, and listing.

use crate::api::rest::v1::models::schedules::{
    CreateScheduleRequest, ListSchedulesResponse, ScheduleActionResponse, ScheduleResponse,
    UpdateScheduleRequest,
};
use crate::error::{Result, ServerError};
use crate::state::AppState;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};
use tracing::{debug, info};
use uuid::Uuid;

/// Query parameters for listing schedules
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListSchedulesQuery {
    /// Filter by status (active, paused, disabled, completed, failed)
    pub status: Option<String>,

    /// Filter by name pattern (case-insensitive substring match)
    pub name: Option<String>,

    /// Number of results per page
    #[serde(default = "default_limit")]
    pub limit: u32,

    /// Offset for pagination
    #[serde(default)]
    pub offset: u32,
}

fn default_limit() -> u32 {
    50
}

/// Create a new schedule
///
/// POST /api/v1/schedules
pub async fn create_schedule(
    State(state): State<AppState>,
    Json(request): Json<CreateScheduleRequest>,
) -> Result<(StatusCode, Json<ScheduleResponse>)> {
    info!(name = %request.name, "Creating schedule");

    let response = state.schedule_service().create_schedule(request).await?;

    info!(schedule_id = %response.id, "Schedule created");
    Ok((StatusCode::CREATED, Json(response)))
}

/// Get schedule details
///
/// GET /api/v1/schedules/:id
pub async fn get_schedule(
    State(state): State<AppState>,
    Path(schedule_id): Path<String>,
) -> Result<Json<ScheduleResponse>> {
    debug!(schedule_id = %schedule_id, "Getting schedule");

    let response = state.schedule_service().get_schedule(&schedule_id).await?;
    Ok(Json(response))
}

/// Update a schedule
///
/// PUT /api/v1/schedules/:id
pub async fn update_schedule(
    State(state): State<AppState>,
    Path(schedule_id): Path<String>,
    Json(request): Json<UpdateScheduleRequest>,
) -> Result<Json<ScheduleResponse>> {
    info!(schedule_id = %schedule_id, "Updating schedule");

    let response = state
        .schedule_service()
        .update_schedule(&schedule_id, request)
        .await?;
    Ok(Json(response))
}

/// Delete a schedule
///
/// DELETE /api/v1/schedules/:id
pub async fn delete_schedule(
    State(state): State<AppState>,
    Path(schedule_id): Path<String>,
) -> Result<Json<ScheduleActionResponse>> {
    info!(schedule_id = %schedule_id, "Deleting schedule");

    let response = state
        .schedule_service()
        .delete_schedule(&schedule_id)
        .await?;
    Ok(Json(response))
}

/// List schedules
///
/// GET /api/v1/schedules
pub async fn list_schedules(
    State(state): State<AppState>,
    Query(mut query): Query<ListSchedulesQuery>,
) -> Result<Json<ListSchedulesResponse>> {
    // Clamp client-supplied limit to a safe ceiling (see MAX_PAGE_SIZE).
    query.limit = query.limit.clamp(1, super::MAX_PAGE_SIZE as u32);

    debug!(
        status = ?query.status,
        name = ?query.name,
        limit = query.limit,
        offset = query.offset,
        "Listing schedules"
    );

    let response = state
        .schedule_service()
        .list_schedules(query.status, query.name, query.limit, query.offset)
        .await?;
    Ok(Json(response))
}

/// Pause a schedule
///
/// POST /api/v1/schedules/:id/pause
pub async fn pause_schedule(
    State(state): State<AppState>,
    Path(schedule_id): Path<String>,
) -> Result<Json<ScheduleActionResponse>> {
    info!(schedule_id = %schedule_id, "Pausing schedule");

    let response = state
        .schedule_service()
        .pause_schedule(&schedule_id)
        .await?;
    Ok(Json(response))
}

/// Resume a schedule
///
/// POST /api/v1/schedules/:id/resume
pub async fn resume_schedule(
    State(state): State<AppState>,
    Path(schedule_id): Path<String>,
) -> Result<Json<ScheduleActionResponse>> {
    info!(schedule_id = %schedule_id, "Resuming schedule");

    let response = state
        .schedule_service()
        .resume_schedule(&schedule_id)
        .await?;
    Ok(Json(response))
}

/// Query parameters for listing schedule executions
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListScheduleExecutionsQuery {
    /// Maximum number of results per page
    #[serde(default = "default_executions_limit")]
    pub limit: i64,

    /// Offset for pagination
    #[serde(default)]
    pub offset: i64,
}

fn default_executions_limit() -> i64 {
    50
}

/// A single schedule execution entry returned to callers
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ScheduleExecutionResponse {
    pub id: Uuid,
    pub execution_number: Option<i64>,
    pub scheduled_at: Option<String>,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
    pub status: Option<String>,
    pub duration_ms: Option<i64>,
    pub trigger_type: Option<String>,
    pub error_message: Option<String>,
    pub workflow_execution_id: Option<Uuid>,
    pub workflow_name: Option<String>,
    pub workflow_status: Option<String>,
}

/// Paginated list of schedule executions
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ListScheduleExecutionsResponse {
    pub executions: Vec<ScheduleExecutionResponse>,
    pub count: usize,
    pub offset: i64,
}

/// Raw row returned from the schedule_executions join query
struct ScheduleExecutionRow {
    id: Uuid,
    execution_number: Option<i64>,
    scheduled_at: Option<orcher_common::types::time::Timestamptz>,
    started_at: Option<orcher_common::types::time::Timestamptz>,
    completed_at: Option<orcher_common::types::time::Timestamptz>,
    status: Option<String>,
    duration_ms: Option<i64>,
    trigger_type: Option<String>,
    error_message: Option<String>,
    workflow_execution_id: Option<Uuid>,
    workflow_name: Option<String>,
    workflow_status: Option<String>,
}

impl sqlx::FromRow<'_, sqlx::postgres::PgRow> for ScheduleExecutionRow {
    fn from_row(row: &sqlx::postgres::PgRow) -> sqlx::Result<Self> {
        use sqlx::Row;
        Ok(ScheduleExecutionRow {
            id: row.try_get("id")?,
            execution_number: row.try_get("execution_number")?,
            scheduled_at: row.try_get("scheduled_at")?,
            started_at: row.try_get("started_at")?,
            completed_at: row.try_get("completed_at")?,
            status: row.try_get("status")?,
            duration_ms: row.try_get("duration_ms")?,
            trigger_type: row.try_get("trigger_type")?,
            error_message: row.try_get("error_message")?,
            workflow_execution_id: row.try_get("workflow_execution_id")?,
            workflow_name: row.try_get("workflow_name")?,
            workflow_status: row.try_get("workflow_status")?,
        })
    }
}

fn execution_row_to_response(row: ScheduleExecutionRow) -> ScheduleExecutionResponse {
    ScheduleExecutionResponse {
        id: row.id,
        execution_number: row.execution_number,
        scheduled_at: row.scheduled_at.map(|t| t.to_string()),
        started_at: row.started_at.map(|t| t.to_string()),
        completed_at: row.completed_at.map(|t| t.to_string()),
        status: row.status,
        duration_ms: row.duration_ms,
        trigger_type: row.trigger_type,
        error_message: row.error_message,
        workflow_execution_id: row.workflow_execution_id,
        workflow_name: row.workflow_name,
        workflow_status: row.workflow_status,
    }
}

/// List executions for a schedule
///
/// GET /api/v1/schedules/:id/executions
pub async fn list_schedule_executions(
    State(state): State<AppState>,
    Path(schedule_id_str): Path<String>,
    Query(mut query): Query<ListScheduleExecutionsQuery>,
) -> Result<Json<ListScheduleExecutionsResponse>> {
    // Clamp client-supplied pagination to a safe range (see MAX_PAGE_SIZE).
    query.limit = query.limit.clamp(1, super::MAX_PAGE_SIZE);
    query.offset = query.offset.max(0);

    let raw = schedule_id_str
        .strip_prefix("sch_")
        .unwrap_or(&schedule_id_str);
    let schedule_id = Uuid::parse_str(raw)
        .map_err(|_| ServerError::validation(format!("Invalid schedule ID: {schedule_id_str}")))?;

    // Prove the caller owns the parent schedule before reading its run history.
    // This handler queries the pool directly rather than going through the
    // service, so it inherited none of the service's scoping: the predicate was
    // `WHERE se.schedule_id = $1` and nothing else, which handed any caller
    // another organisation's run history — workflow names, statuses and error
    // messages — for the price of a schedule id. Reusing `get_schedule` keeps
    // one definition of what "yours" means and returns the same not-found for a
    // schedule that is absent as for one that is someone else's.
    state
        .schedule_service()
        .get_schedule(&schedule_id_str)
        .await?;

    debug!(
        schedule_id = %schedule_id,
        limit = query.limit,
        offset = query.offset,
        "Listing schedule executions"
    );

    let pool = state.pool();

    let rows = sqlx::query_as::<_, ScheduleExecutionRow>(
        r#"
        SELECT
            se.id,
            se.execution_number,
            se.scheduled_at,
            se.started_at,
            se.completed_at,
            se.status,
            se.duration_ms,
            se.trigger_type,
            se.error_message,
            se.workflow_execution_id,
            we.workflow_name,
            we.status as workflow_status
        FROM schedule_executions se
        LEFT JOIN workflow_executions we ON we.id = se.workflow_execution_id
        WHERE se.schedule_id = $1
        ORDER BY se.scheduled_at DESC
        LIMIT $2 OFFSET $3
        "#,
    )
    .bind(schedule_id)
    .bind(query.limit)
    .bind(query.offset)
    .fetch_all(pool)
    .await
    .map_err(|e| ServerError::internal(format!("Failed to list schedule executions: {}", e)))?;

    let executions: Vec<ScheduleExecutionResponse> =
        rows.into_iter().map(execution_row_to_response).collect();
    let count = executions.len();

    Ok(Json(ListScheduleExecutionsResponse {
        executions,
        count,
        offset: query.offset,
    }))
}
