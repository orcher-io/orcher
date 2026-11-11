//! Workflow Execution Handlers
//!
//! This module implements REST API handlers for workflow execution operations.
//! All operations communicate with the workflow service layer.

use crate::api::rest::v1::models::workflows::{
    CancelExecutionResponse, ExecuteWorkflowRequest, ExecuteWorkflowResponse,
    ExecutionResultResponse, ExecutionStatusResponse, ListExecutionsResponse, LogEntry,
};
use crate::error::Result;
use crate::state::AppState;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{sse::Event, Sse},
    Json,
};
use futures::stream::Stream;
use serde::Deserialize;
use std::collections::HashMap;
use std::convert::Infallible;
use std::time::Duration;
use tracing::{debug, info, warn};

use crate::services::workflow::LogStreamFilters;

/// Query parameters for listing executions
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListExecutionsQuery {
    /// Filter by workflow name
    pub workflow_name: Option<String>,

    /// Filter by status
    pub status: Option<String>,

    /// Filter by namespace
    pub namespace: Option<String>,

    /// Number of results per page
    #[serde(default = "default_limit")]
    pub limit: u32,

    /// Offset for pagination
    #[serde(default)]
    pub offset: u32,

    /// Only return executions submitted at or after this ISO 8601 timestamp
    pub since: Option<String>,
}

fn default_limit() -> u32 {
    1000
}

/// Query parameters for cancellation
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CancelQuery {
    /// Optional reason for cancellation
    pub reason: Option<String>,
}

/// Query parameters for log streaming
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogStreamQuery {
    /// Filter by log level (DEBUG, INFO, WARN, ERROR)
    pub level: Option<String>,
    /// Filter by workflow step name
    pub step: Option<String>,
    /// Limit number of historical logs (default: 1000)
    pub limit: Option<usize>,
}

/// Execute a workflow
///
/// Creates a new workflow execution and submits it to the orchestrator.
///
/// # Request
///
/// POST /api/v1/workflows/execute
///
/// ```json
/// {
///   "workflowName": "order-processor",
///   "workflowVersion": "1.0.0",
///   "namespace": "production",
///   "parameters": {
///     "orderId": 123,
///     "amount": 99.99
///   },
///   "timeoutSecs": 300,
///   "priority": 10,
///   "idempotencyKey": "unique-key-123",
///   "tags": {
///     "environment": "prod"
///   }
/// }
/// ```
///
/// # Response
///
/// ```json
/// {
///   "executionId": "550e8400-e29b-41d4-a716-446655440000",
///   "workflowName": "order-processor",
///   "workflowVersion": "1.0.0",
///   "status": "PENDING",
///   "submittedAt": "2024-01-15T10:30:00Z",
///   "idempotencyKey": "unique-key-123"
/// }
/// ```
///
/// # Arguments
///
/// * `service` - Workflow service instance
/// * `request` - Workflow execution request containing workflow details and parameters
///
/// # Returns
///
/// Returns 201 Created with the execution response containing execution ID and initial status.
///
/// # Errors
///
/// - 400 Bad Request - Invalid request parameters
/// - 404 Not Found - Workflow not found
/// - 409 Conflict - Idempotency key conflict
/// - 500 Internal Server Error - Execution submission failed
pub async fn execute_workflow(
    State(state): State<AppState>,
    Json(request): Json<ExecuteWorkflowRequest>,
) -> Result<(StatusCode, Json<ExecuteWorkflowResponse>)> {
    info!(
        workflow_name = %request.workflow_name,
        workflow_version = %request.workflow_version,
        "Handling workflow execution request"
    );

    let response = state.workflow_service().execute_workflow(request).await?;

    info!(
        execution_id = %response.execution_id,
        workflow_name = %response.workflow_name,
        "Workflow execution submitted successfully"
    );

    Ok((StatusCode::CREATED, Json(response)))
}

/// Get workflow execution status
///
/// Retrieves the current status and progress of a workflow execution.
///
/// # Request
///
/// GET /`api/v1/executions/{execution_id}/status`
///
/// # Response
///
/// ```json
/// {
///   "executionId": "550e8400-e29b-41d4-a716-446655440000",
///   "workflowName": "order-processor",
///   "workflowVersion": "1.0.0",
///   "status": "RUNNING",
///   "submittedAt": "2024-01-15T10:30:00Z",
///   "startedAt": "2024-01-15T10:30:01Z",
///   "progress": {
///     "currentStep": "processing",
///     "totalSteps": 3,
///     "completedSteps": 1,
///     "progressPercent": 33.3,
///     "message": "Processing workflow"
///   },
///   "tags": {
///     "environment": "prod"
///   }
/// }
/// ```
///
/// # Arguments
///
/// * `service` - Workflow service instance
/// * `execution_id` - Unique execution identifier
///
/// # Returns
///
/// Returns 200 OK with the execution status and progress information.
///
/// # Errors
///
/// - 404 Not Found - Execution not found
/// - 500 Internal Server Error - Status retrieval failed
pub async fn get_execution_status(
    State(state): State<AppState>,
    Path(execution_id): Path<String>,
) -> Result<Json<ExecutionStatusResponse>> {
    debug!(execution_id = %execution_id, "Getting execution status");

    let response = state
        .workflow_service()
        .get_execution_status(&execution_id)
        .await?;

    Ok(Json(response))
}

/// Get workflow execution result
///
/// Retrieves the final result of a completed workflow execution.
///
/// # Request
///
/// GET /`api/v1/executions/{execution_id}/result`
///
/// # Response
///
/// ```json
/// {
///   "executionId": "550e8400-e29b-41d4-a716-446655440000",
///   "workflowName": "order-processor",
///   "workflowVersion": "1.0.0",
///   "status": "COMPLETED",
///   "result": {
///     "status": "success",
///     "message": "Order processed successfully",
///     "orderId": 123
///   },
///   "submittedAt": "2024-01-15T10:30:00Z",
///   "startedAt": "2024-01-15T10:30:01Z",
///   "completedAt": "2024-01-15T10:30:03Z",
///   "durationMs": 2500,
///   "statistics": {
///     "totalSteps": 3,
///     "successfulSteps": 3,
///     "failedSteps": 0,
///     "retriedSteps": 0,
///     "totalDurationMs": 2500,
///     "averageStepDurationMs": 833
///   }
/// }
/// ```
///
/// # Arguments
///
/// * `service` - Workflow service instance
/// * `execution_id` - Unique execution identifier
///
/// # Returns
///
/// Returns 200 OK with the execution result and statistics.
///
/// # Errors
///
/// - 404 Not Found - Execution not found
/// - 500 Internal Server Error - Result retrieval failed
pub async fn get_execution_result(
    State(state): State<AppState>,
    Path(execution_id): Path<String>,
) -> Result<Json<ExecutionResultResponse>> {
    debug!(execution_id = %execution_id, "Getting execution result");

    let response = state
        .workflow_service()
        .get_execution_result(&execution_id)
        .await?;

    Ok(Json(response))
}

/// Cancel a workflow execution
///
/// Requests cancellation of a running or pending workflow execution.
///
/// # Request
///
/// POST /`api/v1/executions/{execution_id}/cancel?reason=User%20requested`
///
/// # Response
///
/// ```json
/// {
///   "executionId": "550e8400-e29b-41d4-a716-446655440000",
///   "status": "CANCELLED",
///   "message": "Execution cancelled: User requested",
///   "cancelledAt": "2024-01-15T10:30:05Z"
/// }
/// ```
///
/// # Arguments
///
/// * `service` - Workflow service instance
/// * `execution_id` - Unique execution identifier
/// * `query` - Query parameters including optional cancellation reason
///
/// # Returns
///
/// Returns 200 OK with cancellation confirmation.
///
/// # Errors
///
/// - 400 Bad Request - Execution already in terminal state
/// - 404 Not Found - Execution not found
/// - 500 Internal Server Error - Cancellation failed
pub async fn cancel_execution(
    State(state): State<AppState>,
    Path(execution_id): Path<String>,
    Query(query): Query<CancelQuery>,
) -> Result<Json<CancelExecutionResponse>> {
    info!(
        execution_id = %execution_id,
        reason = ?query.reason,
        "Cancelling execution"
    );

    let response = state
        .workflow_service()
        .cancel_execution(&execution_id, query.reason)
        .await?;

    Ok(Json(response))
}

/// List workflow executions
///
/// Retrieves a paginated list of workflow executions with optional filtering.
///
/// # Request
///
/// GET /api/v1/executions?workflowName=order-processor&status=RUNNING&limit=50&offset=0
///
/// # Response
///
/// ```json
/// {
///   "executions": [
///     {
///       "executionId": "550e8400-e29b-41d4-a716-446655440000",
///       "workflowName": "order-processor",
///       "workflowVersion": "1.0.0",
///       "status": "RUNNING",
///       "submittedAt": "2024-01-15T10:30:00Z",
///       "tags": {
///         "environment": "prod"
///       }
///     }
///   ],
///   "total": 100,
///   "count": 1,
///   "offset": 0
/// }
/// ```
///
/// # Arguments
///
/// * `service` - Workflow service instance
/// * `query` - Query parameters for filtering and pagination
///
/// # Returns
///
/// Returns 200 OK with paginated list of execution summaries.
///
/// # Errors
///
/// - 400 Bad Request - Invalid query parameters
/// - 500 Internal Server Error - List retrieval failed
pub async fn list_executions(
    State(state): State<AppState>,
    Query(mut query): Query<ListExecutionsQuery>,
) -> Result<Json<ListExecutionsResponse>> {
    // Clamp client-supplied limit to a safe ceiling (see MAX_PAGE_SIZE).
    query.limit = query.limit.clamp(1, super::MAX_PAGE_SIZE as u32);

    debug!(
        workflow_name = ?query.workflow_name,
        status = ?query.status,
        limit = query.limit,
        offset = query.offset,
        "Listing executions"
    );

    // Parse status if provided
    let status = query
        .status
        .as_deref()
        .map(parse_execution_status)
        .transpose()?;

    // Parse since timestamp if provided
    let submitted_after = query
        .since
        .as_ref()
        .and_then(|s| orcher_common::types::time::Timestamptz::parse_rfc3339(s).ok());

    let response = state
        .workflow_service()
        .list_executions(
            query.workflow_name,
            status,
            query.namespace,
            HashMap::new(),
            query.limit,
            query.offset,
            submitted_after,
        )
        .await?;

    Ok(Json(response))
}

/// Get workflow execution logs
///
/// Retrieves logs for a workflow execution.
///
/// # Request
///
/// GET /`api/v1/executions/{execution_id}/logs`
///
/// # Response
///
/// ```json
/// [
///   {
///     "timestamp": "2024-01-15T10:30:00Z",
///     "level": "INFO",
///     "message": "Workflow execution started",
///     "step": "initialize"
///   },
///   {
///     "timestamp": "2024-01-15T10:30:01Z",
///     "level": "INFO",
///     "message": "Processing input parameters",
///     "step": "validate"
///   }
/// ]
/// ```
///
/// # Arguments
///
/// * `service` - Workflow service instance
/// * `execution_id` - Unique execution identifier
///
/// # Returns
///
/// Returns 200 OK with array of log entries.
///
/// # Errors
///
/// - 404 Not Found - Execution not found
/// - 500 Internal Server Error - Log retrieval failed
///
/// # Notes
///
/// In production, this endpoint would support Server-Sent Events (SSE)
/// for real-time log streaming. The current implementation returns
/// a static list of logs.
pub async fn get_execution_logs(
    State(state): State<AppState>,
    Path(execution_id): Path<String>,
) -> Result<Json<Vec<LogEntry>>> {
    debug!(execution_id = %execution_id, "Getting execution logs");

    let logs = state
        .workflow_service()
        .get_execution_logs(&execution_id)
        .await?;

    Ok(Json(logs))
}

/// Stream execution logs in real-time via Server-Sent Events
///
/// Creates a persistent SSE connection that streams log entries as they are generated.
/// The stream automatically closes when the workflow execution completes.
///
/// # Endpoint
///
/// GET /`api/v1/workflows/{execution_id}/logs/stream?level=ERROR&step=process_order&limit=100`
///
/// # Query Parameters
///
/// * `level` - Filter by log level (DEBUG, INFO, WARN, ERROR)
/// * `step` - Filter by workflow step name
/// * `limit` - Limit number of historical logs (default: 1000)
///
/// # Response
///
/// Server-Sent Events stream with log entries:
///
/// ```text
/// data: {"timestamp":"2024-01-15T10:30:00Z","level":"INFO","message":"Workflow started","step":null}
///
/// data: {"timestamp":"2024-01-15T10:30:05Z","level":"INFO","message":"Step 1 completed","step":"process_order"}
///
/// data: {"timestamp":"2024-01-15T10:30:10Z","level":"INFO","message":"Workflow completed","step":null}
/// ```
///
/// # Arguments
///
/// * `state` - Application state
/// * `execution_id` - Workflow execution ID
/// * `query` - Query parameters for filtering
///
/// # Returns
///
/// Returns an SSE stream of log entries that updates in real-time.
///
/// # Example
///
/// ```javascript
/// // Stream only ERROR logs
/// const eventSource = new EventSource('/api/v1/workflows/exec-123/logs/stream?level=ERROR');
/// eventSource.onmessage = (event) => {
///   const log = JSON.parse(event.data);
///   console.log(`[${log.level}] ${log.message}`);
/// };
/// ```
pub async fn stream_execution_logs(
    State(state): State<AppState>,
    Path(execution_id): Path<String>,
    Query(query): Query<LogStreamQuery>,
) -> Result<Sse<impl Stream<Item = std::result::Result<Event, Infallible>>>> {
    info!(
        execution_id = %execution_id,
        level = ?query.level,
        step = ?query.step,
        limit = ?query.limit,
        "Starting SSE log stream with filters"
    );

    // Build filters from query parameters
    let filters = LogStreamFilters {
        level: query.level,
        step: query.step,
        limit: query.limit,
    };

    // Get the log stream receiver with filters
    let mut log_receiver = state
        .workflow_service()
        .stream_execution_logs(&execution_id, filters)
        .await?;

    // Create SSE stream from the receiver
    let stream = async_stream::stream! {
        while let Some(log) = log_receiver.recv().await {
            // Serialize log entry to JSON
            match serde_json::to_string(&log) {
                Ok(json) => {
                    yield std::result::Result::<Event, Infallible>::Ok(Event::default().data(json));
                }
                Err(e) => {
                    warn!(
                        execution_id = %execution_id,
                        error = %e,
                        "Failed to serialize log entry"
                    );
                }
            }
        }

        // Send a final "done" event when stream completes
        let done_event = serde_json::json!({
            "type": "stream_complete",
            "execution_id": execution_id
        });
        if let Ok(json) = serde_json::to_string(&done_event) {
            yield std::result::Result::<Event, Infallible>::Ok(Event::default().event("complete").data(json));
        }
    };

    Ok(Sse::new(stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keep-alive"),
    ))
}

/// Parse execution status from string
/// The status a `status` filter names, in any case; a name that is not a
/// status is refused rather than ignored, which would list every execution as
/// if no filter had been given. Read through the enum's own names, so a status
/// added to it is a filter at once.
fn parse_execution_status(
    status: &str,
) -> crate::error::Result<crate::api::rest::v1::models::workflows::ExecutionStatus> {
    serde_json::from_value(serde_json::Value::String(status.to_uppercase())).map_err(|_| {
        crate::error::ServerError::validation(format!(
            "unknown status {status:?}: pending, running, completed, failed, cancelled, \
             terminated, timed_out, restarted_fresh or reset"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::rest::v1::models::workflows::ExecutionStatus;

    #[test]
    fn test_parse_execution_status() {
        assert_eq!(
            parse_execution_status("PENDING").ok(),
            Some(ExecutionStatus::Pending)
        );
        assert_eq!(
            parse_execution_status("running").ok(),
            Some(ExecutionStatus::Running)
        );
        assert_eq!(
            parse_execution_status("COMPLETED").ok(),
            Some(ExecutionStatus::Completed)
        );
        assert_eq!(
            parse_execution_status("terminated").ok(),
            Some(ExecutionStatus::Terminated)
        );
        assert_eq!(
            parse_execution_status("restarted_fresh").ok(),
            Some(ExecutionStatus::RestartedFresh)
        );
        assert_eq!(
            parse_execution_status("canceled").ok(),
            Some(ExecutionStatus::Cancelled)
        );
        assert!(parse_execution_status("invalid").is_err());
    }

    #[test]
    fn test_default_limit() {
        assert_eq!(default_limit(), 1000);
    }
}

/// Journal entry for REST API response
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JournalEntryResponse {
    pub event_id: i64,
    pub event_type: String,
    pub timestamp: String,
    pub payload: serde_json::Value,
    pub task_id: i64,
}

/// Journal response
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JournalResponse {
    pub entries: Vec<JournalEntryResponse>,
    pub total: usize,
}

/// Get execution journal (event-sourced history)
///
/// # Request
///
/// GET /api/v1/executions/{execution_id}/journal
pub async fn get_execution_journal(
    State(state): State<AppState>,
    Path(execution_id): Path<String>,
) -> Result<Json<JournalResponse>> {
    debug!(execution_id = %execution_id, "Getting execution journal");

    // Look up the execution to get the internal workflow_id for journal lookup,
    // and to establish that the journal is the caller's to read. The journal is
    // the whole event-sourced history of a workflow — every step's input and
    // output — so this was the broadest read in the API, and it was reachable
    // with nothing but an execution id.
    let pool = state.pool();
    let execution = sqlx::query_as::<_, (uuid::Uuid, String, Option<uuid::Uuid>)>(
        "SELECT id, execution_id, organization_id FROM workflow_executions WHERE execution_id = $1",
    )
    .bind(&execution_id)
    .fetch_optional(pool)
    .await
    .map_err(|e| crate::error::ServerError::internal(format!("Database error: {}", e)))?
    .filter(|execution| {
        orcher_core::grpc::tenant::may_act_on(
            execution.2,
            orcher_core::grpc::tenant::current_organization(),
        )
    })
    .ok_or_else(|| {
        crate::error::ServerError::not_found(format!("Execution not found: {}", execution_id))
    })?;

    let workflow_id = execution.0.to_string();

    // Query journal entries
    let rows = sqlx::query_as::<
        _,
        (
            i64,
            String,
            orcher_common::types::time::Timestamptz,
            serde_json::Value,
            i64,
        ),
    >(
        r#"
        SELECT event_id, event_type, timestamp, payload, task_id
        FROM workflow_execution_journal
        WHERE workflow_id = $1 AND execution_id = $2
        ORDER BY event_id ASC
        "#,
    )
    .bind(&workflow_id)
    .bind(&execution_id)
    .fetch_all(pool)
    .await
    .map_err(|e| crate::error::ServerError::internal(format!("Failed to query journal: {}", e)))?;

    let entries: Vec<JournalEntryResponse> = rows
        .into_iter()
        .map(
            |(event_id, event_type, timestamp, payload, task_id)| JournalEntryResponse {
                event_id,
                event_type,
                timestamp: timestamp.to_string(),
                payload,
                task_id,
            },
        )
        .collect();

    let total = entries.len();

    info!(
        execution_id = %execution_id,
        entries = total,
        "Journal retrieved"
    );

    Ok(Json(JournalResponse { entries, total }))
}
