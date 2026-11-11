//! Workflow Execution Service Layer
//!
//! This service layer provides business logic for workflow execution operations.
//! It communicates with the orchestrator service via gRPC clients and manages
//! workflow execution state, monitoring, and control.
//!
//! # Architecture
//!
//! ```text
//! API Handlers → WorkflowService → Repositories → Database
//!                              ↓
//!                         gRPC Client → Orchestrator Service (Week 2)
//! ```
//!
//! The service layer:
//! - Validates requests before sending to orchestrator
//! - Transforms between REST models and database entities
//! - Handles business logic and error conversion
//! - Manages workflow execution lifecycle with database persistence

use crate::api::rest::v1::models::workflows::{
    CancelExecutionResponse, ExecuteWorkflowRequest, ExecuteWorkflowResponse, ExecutionProgress,
    ExecutionResultResponse, ExecutionStatistics, ExecutionStatus, ExecutionStatusResponse,
    ExecutionSummary, ListExecutionsResponse, LogEntry, LogLevel, ResetExecutionRequest,
    ResetExecutionResponse,
};
use crate::error::{Result, ServerError};
use crate::grpc::clients::workflow::WorkflowServiceClient;
use orcher_common::types::time::Timestamptz;
use orcher_storage::repositories::domains::workflow_execution::logs::{
    WorkflowExecutionLog, WorkflowExecutionLogsRepository,
};
use orcher_storage::repositories::domains::workflow_execution::repository::{
    ListExecutionsFilters, WorkflowExecutionRepository,
};
use orcher_storage::schema::types::execution::WorkflowExecution;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

/// Filters for log streaming
#[derive(Debug, Clone, Default)]
pub struct LogStreamFilters {
    /// Filter by log level (DEBUG, INFO, WARN, ERROR)
    pub level: Option<String>,
    /// Filter by workflow step name
    pub step: Option<String>,
    /// Limit number of historical logs
    pub limit: Option<usize>,
}

/// Workflow execution service
///
/// This service manages workflow execution operations including submission,
/// status tracking, result retrieval, and cancellation. It uses database
/// repositories for persistence and will integrate with the orchestrator
/// gRPC service in Week 2.
#[derive(Clone)]
pub struct WorkflowService {
    /// Repository for workflow execution persistence
    execution_repository: Arc<WorkflowExecutionRepository>,

    /// Repository for execution logs
    logs_repository: Arc<WorkflowExecutionLogsRepository>,

    /// Modern workflow service gRPC client
    workflow_client: Option<Arc<WorkflowServiceClient>>,
}

impl WorkflowService {
    /// Create a new workflow service with modern gRPC clients
    ///
    /// # Arguments
    ///
    /// * `execution_repository` - Repository for workflow execution persistence
    /// * `logs_repository` - Repository for execution logs
    /// * `workflow_client` - Modern workflow service gRPC client
    ///
    /// # Returns
    ///
    /// Returns a new workflow service instance.
    #[must_use]
    pub fn new(
        execution_repository: Arc<WorkflowExecutionRepository>,
        logs_repository: Arc<WorkflowExecutionLogsRepository>,
        workflow_client: Arc<WorkflowServiceClient>,
    ) -> Self {
        Self {
            execution_repository,
            logs_repository,
            workflow_client: Some(workflow_client),
        }
    }

    /// Create a new workflow service stub (for testing or when orchestrator is unavailable)
    ///
    /// # Arguments
    ///
    /// * `execution_repository` - Repository for workflow execution persistence
    /// * `logs_repository` - Repository for execution logs
    ///
    /// # Returns
    ///
    /// Returns a new workflow service instance without gRPC client.
    #[must_use]
    pub fn new_stub(
        execution_repository: Arc<WorkflowExecutionRepository>,
        logs_repository: Arc<WorkflowExecutionLogsRepository>,
    ) -> Self {
        Self {
            execution_repository,
            logs_repository,
            workflow_client: None,
        }
    }

    /// Execute a workflow
    ///
    /// Submits a workflow for execution and returns immediately with an execution ID.
    /// The actual execution happens asynchronously in the orchestrator service.
    ///
    /// # Arguments
    ///
    /// * `request` - Workflow execution request
    ///
    /// # Returns
    ///
    /// Returns the execution response with execution ID and initial status.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The workflow cannot be found
    /// - The workflow binary is invalid
    /// - An idempotency key conflict occurs
    /// - Communication with orchestrator fails
    /// - Database persistence fails
    ///
    /// # Example
    ///
    /// ```ignore
    /// let request = ExecuteWorkflowRequest {
    ///     workflow_name: "order-processor".to_string(),
    ///     workflow_version: "1.0.0".to_string(),
    ///     namespace: "production".to_string(),
    ///     parameters: serde_json::json!({"order_id": 123}),
    ///     ..Default::default()
    /// };
    ///
    /// let response = service.execute_workflow(request).await?;
    /// println!("Execution ID: {}", response.execution_id);
    /// ```
    pub async fn execute_workflow(
        &self,
        request: ExecuteWorkflowRequest,
    ) -> Result<ExecuteWorkflowResponse> {
        info!(
            workflow_name = %request.workflow_name,
            namespace = %request.namespace,
            task_queue = %request.task_queue,
            "Received workflow execution request"
        );

        // Refused rather than ignored: the engine has no way to honour either,
        // and a caller who set one would otherwise believe it took effect.
        if request.priority.is_some() {
            return Err(ServerError::validation(
                "priority is not supported: runs are handed to workers in the order they \
                 become ready",
            ));
        }
        if request.workflow_version != "latest" {
            return Err(ServerError::validation(
                "workflowVersion is not chosen at start: a run is bound to the release of \
                 the first worker that claims it",
            ));
        }

        let Some(ref client) = self.workflow_client else {
            warn!(
                workflow_name = %request.workflow_name,
                "Orchestrator unavailable — rejecting workflow execution"
            );
            return Err(ServerError::service_unavailable(
                "Orchestrator is unavailable; the workflow was not submitted. \
                 Ensure the orchestrator is running and try again.",
            ));
        };

        let input = serde_json::to_vec(&request.parameters)
            .map_err(|e| ServerError::validation(format!("parameters: {e}")))?;
        // The engine records the run, in the caller's organisation, and is
        // the one place that does: this layer writes nothing of its own, so
        // what a listing shows is what the engine runs.
        let started = client
            .start_workflow(orcher_proto::orcher::v1::StartWorkflowRequest {
                workflow_id: request.workflow_id.clone().unwrap_or_default(),
                workflow_type: request.workflow_name.clone(),
                task_queue: request.task_queue.clone(),
                namespace: request.namespace.clone(),
                input,
                execution_timeout: request.timeout_secs.map(|secs| prost_types::Duration {
                    seconds: i64::try_from(secs).unwrap_or(i64::MAX),
                    nanos: 0,
                }),
                labels: request.tags.clone(),
                // A repeat with the same key answers the run the first one
                // started.
                request_id: request.idempotency_key.clone().unwrap_or_default(),
                ..Default::default()
            })
            .await
            .map_err(|status| {
                warn!(
                    workflow_name = %request.workflow_name,
                    code = ?status.code(),
                    message = status.message(),
                    "The engine refused the workflow start"
                );
                ServerError::from_status(status)
            })?;

        Ok(ExecuteWorkflowResponse {
            execution_id: started.execution_id,
            workflow_id: Some(started.workflow_id),
            workflow_name: request.workflow_name,
            workflow_version: request.workflow_version,
            status: ExecutionStatus::Running,
            submitted_at: started
                .started_at
                .and_then(|at| Timestamptz::from_second(at.seconds))
                .unwrap_or_else(Timestamptz::now),
            idempotency_key: request.idempotency_key,
        })
    }

    /// Get execution status
    ///
    /// Retrieves the current status and progress of a workflow execution.
    ///
    /// # Arguments
    ///
    /// * `execution_id` - Unique execution identifier
    ///
    /// # Returns
    ///
    /// Returns the current execution status with progress information.
    ///
    /// # Errors
    ///
    /// Returns an error if the execution ID is not found.
    /// Fetch an execution the caller is entitled to see, or report it missing.
    ///
    /// `cancel_execution` checked ownership; the reads beside it did not, so an
    /// execution id — which appears in every listing and API response its owner
    /// sees — was enough to read another organisation's status, return value
    /// and application logs, or to hold open a stream of their logs as they
    /// were written.
    ///
    /// Not-found rather than forbidden, so the reply cannot be used to discover
    /// which ids exist.
    async fn owned_execution(&self, execution_id: &str) -> Result<WorkflowExecution> {
        self.execution_repository
            .find_by_execution_id(execution_id)
            .await
            .map_err(|e| {
                error!(
                    execution_id = %execution_id,
                    error = %e,
                    "Failed to fetch execution from database"
                );
                ServerError::internal(format!("Failed to fetch execution: {e}"))
            })?
            .filter(|execution| {
                orcher_core::grpc::tenant::may_act_on(
                    execution.organization_id,
                    orcher_core::grpc::tenant::current_organization(),
                )
            })
            .ok_or_else(|| ServerError::NotFound(execution_id.to_string()))
    }

    pub async fn get_execution_status(
        &self,
        execution_id: &str,
    ) -> Result<ExecutionStatusResponse> {
        debug!(execution_id = %execution_id, "Getting execution status");

        let execution = self.owned_execution(execution_id).await?;

        Ok(Self::entity_to_status_response(&execution))
    }

    /// Get execution result
    ///
    /// Retrieves the final result of a completed workflow execution.
    ///
    /// # Arguments
    ///
    /// * `execution_id` - Unique execution identifier
    ///
    /// # Returns
    ///
    /// Returns the execution result if completed, or current status if not.
    ///
    /// # Errors
    ///
    /// Returns an error if the execution ID is not found.
    pub async fn get_execution_result(
        &self,
        execution_id: &str,
    ) -> Result<ExecutionResultResponse> {
        debug!(execution_id = %execution_id, "Getting execution result");

        let execution = self.owned_execution(execution_id).await?;

        Ok(Self::entity_to_result_response(&execution))
    }

    /// Reset a workflow execution to a journal entry
    ///
    /// The engine keeps the journal up to and including the entry and runs
    /// the workflow again from there, as a new execution. Its answer keeps its
    /// meaning: a bad entry is 400, an unknown execution 404, a run that may
    /// not be reset now 409.
    ///
    /// # Errors
    ///
    /// Returns an error if the execution is not found or not the caller's, if
    /// there is no engine to reset it, or if the engine refuses.
    pub async fn reset_execution(
        &self,
        execution_id: &str,
        request: ResetExecutionRequest,
    ) -> Result<ResetExecutionResponse> {
        info!(
            execution_id = %execution_id,
            target_event_id = request.target_event_id,
            "Resetting execution"
        );

        let execution = self.owned_execution(execution_id).await?;
        let client = self.workflow_client.as_ref().ok_or_else(|| {
            ServerError::ServiceUnavailable(
                "Resetting needs the engine, and this server has no connection to it".to_string(),
            )
        })?;
        let reason = request
            .reason
            .filter(|r| !r.trim().is_empty())
            .unwrap_or_else(|| "Reset through the REST API".to_string());

        let reset = client
            .reset_workflow(
                &execution.execution_id,
                &execution.namespace,
                request.target_event_id,
                reason,
            )
            .await
            .map_err(|status| {
                if !matches!(
                    status.code(),
                    tonic::Code::InvalidArgument | tonic::Code::NotFound | tonic::Code::FailedPrecondition
                ) {
                    error!(execution_id = %execution_id, error = %status, "The engine did not reset the execution");
                }
                reset_error(&status)
            })?;

        Ok(ResetExecutionResponse {
            execution_id: reset.old_execution_id,
            new_execution_id: reset.new_execution_id,
        })
    }

    /// Cancel a workflow execution
    ///
    /// Requests cancellation of a running or pending workflow execution.
    ///
    /// # Arguments
    ///
    /// * `execution_id` - Unique execution identifier
    /// * `reason` - Optional reason for cancellation
    ///
    /// # Returns
    ///
    /// Returns confirmation of cancellation.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The execution ID is not found
    /// - The execution is already in a terminal state
    pub async fn cancel_execution(
        &self,
        execution_id: &str,
        reason: Option<String>,
    ) -> Result<CancelExecutionResponse> {
        info!(
            execution_id = %execution_id,
            reason = ?reason,
            "Cancelling execution"
        );

        // Fetch current execution
        let execution = self.owned_execution(execution_id).await?;

        // Ownership is enforced by `owned_execution` above, which every read in
        // this service now shares with this write.

        // Check if execution can be cancelled
        if execution.has_ended() {
            return Err(ServerError::BadRequest(format!(
                "Execution {} is already in terminal state: {}",
                execution_id, execution.status
            )));
        }

        // Through the engine, the path every cancellation takes: a workflow
        // whose code has run is asked to cancel and may clean up, one whose code
        // never ran is cancelled at once. Writing the status here first, as this
        // used to, ended every run before the engine could ask it, and handed the
        // engine a run that had already ended. Without an engine client the
        // gateway can only end the run itself.
        match self.workflow_client {
            Some(ref client) => {
                client
                    .cancel_workflow(
                        "",
                        &execution.execution_id,
                        &execution.namespace,
                        reason.clone(),
                    )
                    .await
                    .map_err(|e| {
                        error!(
                            execution_id = %execution_id,
                            error = %e,
                            "The engine did not cancel the execution"
                        );
                        ServerError::internal(format!("Failed to cancel execution: {e}"))
                    })?;
            }
            None => {
                warn!(
                    execution_id = %execution_id,
                    "No engine client: cancelling the execution directly, without letting it clean up"
                );
                self.execution_repository
                    .cancel(execution_id)
                    .await
                    .map_err(|e| {
                        error!(
                            execution_id = %execution_id,
                            error = %e,
                            "Failed to cancel execution in database"
                        );
                        ServerError::internal(format!("Failed to cancel execution: {e}"))
                    })?;
            }
        }

        // The execution as it now stands: cancelled, or still running while it
        // cleans up.
        let cancelled = self
            .execution_repository
            .find_by_execution_id(execution_id)
            .await
            .map_err(|e| {
                error!(
                    execution_id = %execution_id,
                    error = %e,
                    "Failed to fetch cancelled execution"
                );
                ServerError::internal(format!("Failed to fetch cancelled execution: {e}"))
            })?
            .ok_or_else(|| ServerError::NotFound(execution_id.to_string()))?;

        // Add log entry
        let log_message = format!(
            "Execution cancelled: {}",
            reason.as_ref().unwrap_or(&"User requested".to_string())
        );
        let log = WorkflowExecutionLog::info(execution_id.to_string(), log_message)
            .with_source("workflow_service".to_string());

        if let Err(e) = self.logs_repository.add_log(&log).await {
            warn!(
                execution_id = %execution_id,
                error = %e,
                "Failed to add cancellation log entry"
            );
        }

        Ok(CancelExecutionResponse {
            execution_id: cancelled.execution_id.clone(),
            status: Self::parse_status(&cancelled.status),
            message: format!(
                "{}: {}",
                if cancelled.status == "cancelled" {
                    "Execution cancelled"
                } else {
                    "Cancellation requested; the workflow is cleaning up"
                },
                reason.unwrap_or_else(|| "User requested".to_string())
            ),
            cancelled_at: cancelled.cancelled_at.unwrap_or_else(Timestamptz::now),
        })
    }

    /// List workflow executions
    ///
    /// Retrieves a paginated list of workflow executions with optional filtering.
    ///
    /// # Arguments
    ///
    /// * `workflow_name` - Optional filter by workflow name
    /// * `status` - Optional filter by execution status
    /// * `tags` - Optional filter by tags
    /// * `limit` - Maximum number of results per page
    /// * `offset` - Offset for pagination
    ///
    /// # Returns
    ///
    /// Returns paginated list of execution summaries.
    pub async fn list_executions(
        &self,
        workflow_name: Option<String>,
        status: Option<ExecutionStatus>,
        namespace: Option<String>,
        tags: HashMap<String, String>,
        limit: u32,
        offset: u32,
        submitted_after: Option<orcher_common::types::time::Timestamptz>,
    ) -> Result<ListExecutionsResponse> {
        debug!(
            workflow_name = ?workflow_name,
            status = ?status,
            ?namespace,
            limit = limit,
            offset = offset,
            "Listing executions"
        );

        // Build filters
        let filters = ListExecutionsFilters {
            workflow_name,
            workflow_version: None,
            namespace,
            task_queue: None,
            status: status.map(|s| Self::status_to_string(&s)),
            user_id: None,
            // Only this caller's executions.
            //
            // The gateway resolves the calling organisation and runs the
            // request inside it precisely so the engine can scope by it — its
            // own comment says as much. This filter has been supported by the
            // repository all along and was passed None here, so the listing
            // behind the console's executions page spanned every tenant on the
            // deployment.
            //
            // None still means unscoped, which is what a single-tenant
            // deployment needs: nobody claims an organisation there, and
            // filtering on NULL would return nothing at all.
            organization_id: orcher_core::grpc::tenant::current_organization(),
            parent_execution_id: None,
            labels: None,
            submitted_after,
            limit: i64::from(limit),
            offset: i64::from(offset),
            cursor_execution_id: None,
            cursor_submitted_at: None,
        };

        // Fetch from database (paginated). Keep a copy of the filters for the
        // total-count query below, since `list` consumes them.
        let count_filters = filters.clone();
        let executions = self.execution_repository.list(filters).await.map_err(|e| {
            error!(error = %e, "Failed to list executions from database");
            ServerError::internal(format!("Failed to list executions: {e}"))
        })?;

        // Filter by tags if provided (database doesn't support JSONB tag filtering yet)
        let filtered_executions: Vec<_> = if tags.is_empty() {
            executions
        } else {
            executions
                .into_iter()
                .filter(|e| {
                    let exec_tags = Self::parse_tags(&e.tags);
                    tags.iter().all(|(k, v)| exec_tags.get(k) == Some(v))
                })
                .collect()
        };

        // Total number of executions matching the query, across all pages.
        // Tag filtering happens in-app (the DB has no JSONB tag filtering yet),
        // so when tags are requested the DB-side COUNT can't reflect them — fall
        // back to the filtered length in that case. Making `total` exact for
        // tag-filtered lists needs tag filtering pushed into the query.
        let total = if tags.is_empty() {
            self.execution_repository
                .count(count_filters)
                .await
                .map_err(|e| {
                    error!(error = %e, "Failed to count executions from database");
                    ServerError::internal(format!("Failed to count executions: {e}"))
                })? as u64
        } else {
            filtered_executions.len() as u64
        };

        // Convert to summaries
        let summaries: Vec<ExecutionSummary> = filtered_executions
            .iter()
            .map(Self::entity_to_summary)
            .collect();

        let count = summaries.len() as u32;

        Ok(ListExecutionsResponse {
            executions: summaries,
            total,
            count,
            offset,
        })
    }

    /// Get execution logs
    ///
    /// Retrieves all logs for a workflow execution.
    ///
    /// # Arguments
    ///
    /// * `execution_id` - Unique execution identifier
    ///
    /// # Returns
    ///
    /// Returns a vector of log entries.
    ///
    /// # Errors
    ///
    /// Returns an error if the execution ID is not found.
    pub async fn get_execution_logs(&self, execution_id: &str) -> Result<Vec<LogEntry>> {
        debug!(execution_id = %execution_id, "Getting execution logs");

        // Verify execution exists
        let _execution = self.owned_execution(execution_id).await?;

        // Get logs from repository (limit to 1000)
        let logs = self
            .logs_repository
            .get_logs(execution_id, 1000)
            .await
            .map_err(|e| {
                error!(
                    execution_id = %execution_id,
                    error = %e,
                    "Failed to fetch execution logs"
                );
                ServerError::internal(format!("Failed to fetch logs: {e}"))
            })?;

        Ok(logs.into_iter().map(Self::log_to_api_model).collect())
    }

    /// Stream execution logs in real-time with optional filtering
    ///
    /// Creates a channel that streams log entries as they are added to the database.
    /// This is useful for real-time log viewing in UIs via Server-Sent Events or WebSocket.
    ///
    /// # Arguments
    ///
    /// * `execution_id` - Unique execution identifier
    /// * `filters` - Optional filters for log level and step name
    ///
    /// # Returns
    ///
    /// Returns a receiver channel that yields log entries as they arrive.
    ///
    /// # Errors
    ///
    /// Returns an error if the execution ID is not found.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let filters = LogStreamFilters {
    ///     level: Some("ERROR".to_string()),
    ///     step: Some("process_order".to_string()),
    ///     limit: Some(100),
    /// };
    /// let mut log_stream = service.stream_execution_logs("exec-123", filters).await?;
    /// while let Some(log) = log_stream.recv().await {
    ///     println!("New log: {}", log.message);
    /// }
    /// ```
    pub async fn stream_execution_logs(
        &self,
        execution_id: &str,
        filters: LogStreamFilters,
    ) -> Result<tokio::sync::mpsc::Receiver<LogEntry>> {
        info!(
            execution_id = %execution_id,
            "Starting real-time log stream"
        );

        // Verify execution exists
        let _execution = self.owned_execution(execution_id).await?;

        // Create channel for streaming logs
        let (tx, rx) = tokio::sync::mpsc::channel(100);

        // First, send the lines already written, with filtering. The stream
        // then follows the log by id from the last of them, so each line is
        // sent once. It used to re-read the whole log on every poll and send
        // what lay past a count of lines that started at zero, which sent the
        // backlog a second time, and drained the end of the log unfiltered.
        let backlog = self
            .logs_repository
            .get_logs(execution_id, 1000)
            .await
            .map_err(|e| {
                error!(
                    execution_id = %execution_id,
                    error = %e,
                    "Failed to fetch execution logs"
                );
                ServerError::internal(format!("Failed to fetch logs: {e}"))
            })?;
        let mut last_id = backlog.iter().map(|log| log.id).max().unwrap_or(0);
        let mut existing_logs = Self::apply_filters(
            backlog.into_iter().map(Self::log_to_api_model).collect(),
            &filters,
        );

        // Apply limit if specified
        if let Some(limit) = filters.limit {
            existing_logs.truncate(limit);
        }

        for log in existing_logs {
            if tx.send(log).await.is_err() {
                warn!(
                    execution_id = %execution_id,
                    "Log stream receiver dropped before all existing logs were sent"
                );
                return Ok(rx);
            }
        }

        // Clone repositories, execution_id, and filters for background task
        let logs_repository = self.logs_repository.clone();
        let execution_repository = self.execution_repository.clone();
        let execution_id_owned = execution_id.to_string();
        let filters_clone = filters.clone();

        // Spawn background task to poll for new logs
        tokio::spawn(async move {
            let poll_interval = tokio::time::Duration::from_millis(500);

            loop {
                tokio::time::sleep(poll_interval).await;

                // Check if receiver is still alive
                if tx.is_closed() {
                    debug!(
                        execution_id = %execution_id_owned,
                        "Log stream receiver closed, stopping polling"
                    );
                    break;
                }

                if !Self::send_logs_after(
                    &logs_repository,
                    &execution_id_owned,
                    &mut last_id,
                    &filters_clone,
                    &tx,
                )
                .await
                {
                    return;
                }

                // Stop when the execution reaches a terminal status. Detect it
                // from the execution's actual status — NOT log-message text: an
                // ordinary step log like "step 2 completed" or "validation
                // failed, retrying" must not close a live stream mid-execution.
                let is_terminal = execution_repository
                    .find_by_execution_id(&execution_id_owned)
                    .await
                    .ok()
                    .flatten()
                    .map(|e| {
                        matches!(
                            e.status.to_lowercase().as_str(),
                            "completed"
                                | "failed"
                                | "cancelled"
                                | "canceled"
                                | "terminated"
                                | "timed_out"
                        )
                    })
                    .unwrap_or(false);

                if is_terminal {
                    // Wait a bit for any final logs, drain them, then close.
                    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                    Self::send_logs_after(
                        &logs_repository,
                        &execution_id_owned,
                        &mut last_id,
                        &filters_clone,
                        &tx,
                    )
                    .await;

                    info!(
                        execution_id = %execution_id_owned,
                        "Execution complete, closing log stream"
                    );
                    break;
                }
            }
        });

        Ok(rx)
    }

    /// Send `tx` every line of the execution's log written after `last_id`
    /// that passes `filters`, moving `last_id` past each line read. `false`
    /// once the receiver has gone; a failed read is left to the next poll.
    async fn send_logs_after(
        logs_repository: &WorkflowExecutionLogsRepository,
        execution_id: &str,
        last_id: &mut i64,
        filters: &LogStreamFilters,
        tx: &tokio::sync::mpsc::Sender<LogEntry>,
    ) -> bool {
        const PAGE: i64 = 1000;
        loop {
            let page = match logs_repository
                .get_logs_after(execution_id, *last_id, PAGE)
                .await
            {
                Ok(page) => page,
                Err(e) => {
                    warn!(
                        execution_id = %execution_id,
                        error = %e,
                        "Failed to fetch logs in stream"
                    );
                    return true;
                }
            };
            let Some(last) = page.last() else {
                return true;
            };
            *last_id = last.id;
            let more = page.len() as i64 == PAGE;
            let new_logs = Self::apply_filters(
                page.into_iter().map(Self::log_to_api_model).collect(),
                filters,
            );
            for log_entry in new_logs {
                if tx.send(log_entry).await.is_err() {
                    debug!(
                        execution_id = %execution_id,
                        "Log stream receiver dropped"
                    );
                    return false;
                }
            }
            if !more {
                return true;
            }
        }
    }

    // ========== Status Synchronization Methods ==========

    // ========== Conversion Methods ==========

    /// Convert entity to status response
    fn entity_to_status_response(entity: &WorkflowExecution) -> ExecutionStatusResponse {
        ExecutionStatusResponse {
            execution_id: entity.execution_id.clone(),
            workflow_name: entity.workflow_name.clone(),
            workflow_version: entity.workflow_version.clone(),
            namespace: entity.namespace.clone(),
            task_queue: entity.task_queue.clone(),
            status: Self::parse_status(&entity.status),
            submitted_at: entity.submitted_at,
            started_at: entity.started_at,
            completed_at: entity.completed_at,
            progress: entity.progress.as_ref().map(Self::parse_progress),
            error: entity.error.clone(),
            tags: Self::parse_tags(&entity.tags),
        }
    }

    /// Convert entity to result response
    fn entity_to_result_response(entity: &WorkflowExecution) -> ExecutionResultResponse {
        ExecutionResultResponse {
            execution_id: entity.execution_id.clone(),
            workflow_name: entity.workflow_name.clone(),
            workflow_version: entity.workflow_version.clone(),
            status: Self::parse_status(&entity.status),
            result: entity.result.clone(),
            error: entity.error.clone(),
            submitted_at: entity.submitted_at,
            started_at: entity.started_at,
            completed_at: entity.completed_at,
            duration_ms: entity.duration_ms.map(|d| d as u64),
            statistics: entity.statistics.as_ref().map(Self::parse_statistics),
        }
    }

    /// Convert entity to execution summary
    fn entity_to_summary(entity: &WorkflowExecution) -> ExecutionSummary {
        ExecutionSummary {
            execution_id: entity.execution_id.clone(),
            workflow_name: entity.workflow_name.clone(),
            workflow_version: entity.workflow_version.clone(),
            status: Self::parse_status(&entity.status),
            submitted_at: entity.submitted_at,
            completed_at: entity.completed_at,
            duration_ms: entity.duration_ms.map(|d| d as u64),
            namespace: Some(entity.namespace.clone()),
            task_queue: Some(entity.task_queue.clone()),
            tags: Self::parse_tags(&entity.tags),
        }
    }

    /// Convert log entity to API model
    fn log_to_api_model(log: WorkflowExecutionLog) -> LogEntry {
        LogEntry {
            timestamp: log.timestamp,
            level: Self::parse_log_level(&log.level),
            message: log.message,
            step: log.step,
        }
    }

    /// Parse a database status string into the API `ExecutionStatus`.
    ///
    /// Covers every status the engine writes (see `core` status_to_proto):
    /// terminal timeouts must not be silently reported as `Pending`.
    fn parse_status(status: &str) -> ExecutionStatus {
        match status.to_lowercase().as_str() {
            "pending" | "waiting" | "queued" | "paused" => ExecutionStatus::Pending,
            "running" | "claimed" | "executing" | "retrying" => ExecutionStatus::Running,
            "completed" | "success" => ExecutionStatus::Completed,
            "failed" | "error" => ExecutionStatus::Failed,
            "cancelled" | "canceled" => ExecutionStatus::Cancelled,
            "terminated" => ExecutionStatus::Terminated,
            "timed_out" | "timeout" => ExecutionStatus::TimedOut,
            "restarted_fresh" => ExecutionStatus::RestartedFresh,
            "reset" => ExecutionStatus::Reset,
            _ => ExecutionStatus::Pending,
        }
    }

    /// Convert status enum to string
    fn status_to_string(status: &ExecutionStatus) -> String {
        match status {
            ExecutionStatus::Pending => "pending".to_string(),
            ExecutionStatus::Running => "running".to_string(),
            ExecutionStatus::Completed => "completed".to_string(),
            ExecutionStatus::Failed => "failed".to_string(),
            ExecutionStatus::Cancelled => "cancelled".to_string(),
            ExecutionStatus::Terminated => "terminated".to_string(),
            ExecutionStatus::TimedOut => "timed_out".to_string(),
            ExecutionStatus::RestartedFresh => "restarted_fresh".to_string(),
            ExecutionStatus::Reset => "reset".to_string(),
        }
    }

    /// Parse log level string to enum
    fn parse_log_level(level: &str) -> LogLevel {
        match level.to_lowercase().as_str() {
            "debug" => LogLevel::Debug,
            "info" => LogLevel::Info,
            "warn" => LogLevel::Warn,
            "error" => LogLevel::Error,
            _ => LogLevel::Info,
        }
    }

    /// Parse progress JSONB to struct
    fn parse_progress(progress: &serde_json::Value) -> ExecutionProgress {
        serde_json::from_value(progress.clone()).unwrap_or_else(|_| ExecutionProgress {
            current_step: String::new(),
            total_steps: 0,
            completed_steps: 0,
            progress_percent: 0.0,
            message: None,
        })
    }

    /// Parse statistics JSONB to struct
    fn parse_statistics(statistics: &serde_json::Value) -> ExecutionStatistics {
        serde_json::from_value(statistics.clone()).unwrap_or(ExecutionStatistics {
            total_steps: 0,
            successful_steps: 0,
            failed_steps: 0,
            retried_steps: 0,
            total_duration_ms: 0,
            average_step_duration_ms: None,
        })
    }

    /// Parse tags JSONB to `HashMap`
    fn parse_tags(tags: &serde_json::Value) -> HashMap<String, String> {
        if tags.is_null() {
            return HashMap::new();
        }

        serde_json::from_value(tags.clone()).unwrap_or_default()
    }

    /// Apply filters to log entries
    fn apply_filters(logs: Vec<LogEntry>, filters: &LogStreamFilters) -> Vec<LogEntry> {
        logs.into_iter()
            .filter(|log| {
                // Filter by level if specified
                if let Some(ref level_filter) = filters.level {
                    let log_level_str = format!("{:?}", log.level).to_uppercase();
                    if log_level_str != level_filter.to_uppercase() {
                        return false;
                    }
                }

                // Filter by step if specified
                if let Some(ref step_filter) = filters.step {
                    match &log.step {
                        Some(log_step) if log_step == step_filter => {}
                        _ => return false,
                    }
                }

                true
            })
            .collect()
    }
}

impl Default for WorkflowService {
    fn default() -> Self {
        panic!("WorkflowService cannot be created with default(). Use new() with repositories and orchestrator client.");
    }
}

/// The REST error for an engine that refused a reset: what the caller sent is
/// wrong (400), names nothing (404), or may not be reset now (409); anything
/// else is the server's (500).
fn reset_error(status: &tonic::Status) -> ServerError {
    let message = status.message().to_string();
    match status.code() {
        tonic::Code::InvalidArgument => ServerError::BadRequest(message),
        tonic::Code::NotFound => ServerError::NotFound(message),
        tonic::Code::FailedPrecondition => ServerError::Conflict(message),
        _ => ServerError::internal(format!("Failed to reset execution: {message}")),
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn a_refused_reset_keeps_its_meaning() {
        use axum::http::StatusCode;
        for (code, want) in [
            (tonic::Code::InvalidArgument, StatusCode::BAD_REQUEST),
            (tonic::Code::NotFound, StatusCode::NOT_FOUND),
            (tonic::Code::FailedPrecondition, StatusCode::CONFLICT),
            (tonic::Code::Internal, StatusCode::INTERNAL_SERVER_ERROR),
            (tonic::Code::Unavailable, StatusCode::INTERNAL_SERVER_ERROR),
        ] {
            let error = reset_error(&tonic::Status::new(code, "why"));
            assert_eq!(error.status_code(), want, "{code:?}");
        }
    }

    use super::*;

    #[test]
    fn test_parse_status() {
        assert!(matches!(
            WorkflowService::parse_status("pending"),
            ExecutionStatus::Pending
        ));
        assert!(matches!(
            WorkflowService::parse_status("running"),
            ExecutionStatus::Running
        ));
        assert!(matches!(
            WorkflowService::parse_status("completed"),
            ExecutionStatus::Completed
        ));
        assert!(matches!(
            WorkflowService::parse_status("failed"),
            ExecutionStatus::Failed
        ));
        assert!(matches!(
            WorkflowService::parse_status("cancelled"),
            ExecutionStatus::Cancelled
        ));
    }

    /// Every way a run ends reads as itself over REST: a terminate is not a
    /// cancel, and a run that restarted fresh or was reset is over, not
    /// pending. Each comes back as the status the engine wrote.
    #[test]
    fn every_final_status_the_engine_writes_reads_as_itself() {
        for written in [
            "completed",
            "failed",
            "cancelled",
            "terminated",
            "timed_out",
            "restarted_fresh",
            "reset",
        ] {
            let status = WorkflowService::parse_status(written);
            assert!(status.is_terminal(), "{written} reads as {status:?}");
            assert_eq!(WorkflowService::status_to_string(&status), written);
        }
        assert_ne!(
            WorkflowService::parse_status("terminated"),
            WorkflowService::parse_status("cancelled")
        );
    }

    #[test]
    fn test_status_to_string() {
        assert_eq!(
            WorkflowService::status_to_string(&ExecutionStatus::Pending),
            "pending"
        );
        assert_eq!(
            WorkflowService::status_to_string(&ExecutionStatus::Running),
            "running"
        );
        assert_eq!(
            WorkflowService::status_to_string(&ExecutionStatus::Completed),
            "completed"
        );
        assert_eq!(
            WorkflowService::status_to_string(&ExecutionStatus::Failed),
            "failed"
        );
        assert_eq!(
            WorkflowService::status_to_string(&ExecutionStatus::Cancelled),
            "cancelled"
        );
    }

    #[test]
    fn test_parse_log_level() {
        assert!(matches!(
            WorkflowService::parse_log_level("debug"),
            LogLevel::Debug
        ));
        assert!(matches!(
            WorkflowService::parse_log_level("info"),
            LogLevel::Info
        ));
        assert!(matches!(
            WorkflowService::parse_log_level("warn"),
            LogLevel::Warn
        ));
        assert!(matches!(
            WorkflowService::parse_log_level("error"),
            LogLevel::Error
        ));
    }

    #[test]
    fn test_parse_tags() {
        let tags = serde_json::json!({
            "env": "production",
            "team": "backend"
        });

        let parsed = WorkflowService::parse_tags(&tags);
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed.get("env"), Some(&"production".to_string()));
        assert_eq!(parsed.get("team"), Some(&"backend".to_string()));
    }

    #[test]
    fn test_parse_tags_empty() {
        let tags = serde_json::json!({});
        let parsed = WorkflowService::parse_tags(&tags);
        assert!(parsed.is_empty());
    }

    #[test]
    fn test_parse_tags_null() {
        let tags = serde_json::Value::Null;
        let parsed = WorkflowService::parse_tags(&tags);
        assert!(parsed.is_empty());
    }
}
