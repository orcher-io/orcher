//! Query gRPC service.

use crate::grpc::runs::find_named_run;
use crate::workflows::JournalMapper;
use orcher_common::types::identifiers::WorkflowId;
use orcher_proto::orcher::v1::{
    query_service_server::{QueryService as QueryServiceTrait, QueryServiceServer},
    CountWorkflowsRequest, CountWorkflowsResponse, DescribeWorkflowExecutionRequest,
    DescribeWorkflowExecutionResponse, ExecutionLogEntry, GetExecutionJournalRequest,
    GetExecutionJournalResponse, GetExecutionLogsRequest, GetExecutionLogsResponse,
    GetTaskExecutionsRequest, GetTaskExecutionsResponse, GetWorkflowExecutionStatsRequest,
    GetWorkflowExecutionStatsResponse, ListWorkflowsRequest, ListWorkflowsResponse,
    SearchWorkflowsRequest, SearchWorkflowsResponse, TaskExecutionInfo, WorkflowExecutionInfo,
    WorkflowExecutionStatus,
};
use orcher_proto::tonic::{Request, Response, Status};
use orcher_storage::repositories::domains::coordination::CoordinationRepository;
use orcher_storage::repositories::domains::journal::JournalRepository;
use orcher_storage::repositories::domains::task_execution::TaskExecutionRepository;
use orcher_storage::repositories::domains::workflow_execution::{
    ListExecutionsFilters, WorkflowExecutionLogsRepository, WorkflowExecutionRepository,
};
use orcher_storage::schema::types::execution::WorkflowExecution;
use orcher_storage::traits::domain::CoordinationRepositoryTrait;
use std::sync::Arc;
use tracing::{debug, error, info, instrument};

/// QueryService gRPC implementation
///
/// Handles requests for querying and searching workflow executions.
/// This complements WorkflowService by providing read-only query operations.
pub struct QueryService {
    /// Repository for querying workflow executions
    workflow_execution_repository: Arc<WorkflowExecutionRepository>,

    /// Repository for querying execution journals
    journal_repository: Arc<JournalRepository>,

    /// Repository for querying execution logs (text logs)
    logs_repository: Arc<WorkflowExecutionLogsRepository>,

    /// Repository for querying task executions
    task_execution_repository: Arc<TaskExecutionRepository>,

    /// Repository for coordination queues (timers, outbox, DLQ)
    coordination_repository: Arc<CoordinationRepository>,
}
/// The filters a SQL-like search query names (see `parse_search_query`).
#[derive(Debug, Default, PartialEq)]
struct SearchQuery {
    workflow_name: Option<String>,
    status: Option<String>,
    workflow_version: Option<String>,
    task_queue: Option<String>,
    labels: Option<serde_json::Value>,
}

impl QueryService {
    /// Create a new QueryService
    pub fn new(
        workflow_execution_repository: Arc<WorkflowExecutionRepository>,
        journal_repository: Arc<JournalRepository>,
        logs_repository: Arc<WorkflowExecutionLogsRepository>,
        task_execution_repository: Arc<TaskExecutionRepository>,
        coordination_repository: Arc<CoordinationRepository>,
    ) -> Self {
        Self {
            workflow_execution_repository,
            journal_repository,
            logs_repository,
            task_execution_repository,
            coordination_repository,
        }
    }

    /// Create a gRPC server for this service
    pub fn into_server(self) -> QueryServiceServer<Self> {
        QueryServiceServer::new(self)
    }

    /// Resolve the execution a read RPC was asked about, refusing one that
    /// belongs to another organisation.
    ///
    /// The direct (execution id) form used to skip resolution entirely — the
    /// id was passed to the logs or task repository as-is, so it was its own
    /// authorisation. An execution id is not a secret; it appears in every
    /// listing, log line and API response the owner sees, and is quoted in
    /// support threads. A row the caller may not see is reported as absent,
    /// so the reply cannot be used to test whether an id is real.
    async fn owned_execution(
        &self,
        namespace: &str,
        workflow_id: &str,
        execution_id: &str,
        caller: Option<uuid::Uuid>,
    ) -> Result<WorkflowExecution, Status> {
        find_named_run(
            &self.workflow_execution_repository,
            caller,
            namespace,
            workflow_id,
            execution_id,
        )
        .await?
        .ok_or_else(|| Status::not_found("No execution found for workflow"))
    }

    /// Convert database status string to proto WorkflowExecutionStatus enum
    fn status_to_proto(status: &str) -> WorkflowExecutionStatus {
        match status.to_lowercase().as_str() {
            "running" | "claimed" | "executing" | "queued" => WorkflowExecutionStatus::Running,
            "completed" | "success" => WorkflowExecutionStatus::Completed,
            "failed" | "error" => WorkflowExecutionStatus::Failed,
            "canceled" | "cancelled" => WorkflowExecutionStatus::Canceled,
            "terminated" => WorkflowExecutionStatus::Terminated,
            "restarted_fresh" => WorkflowExecutionStatus::RestartedFresh,
            "timed_out" | "timeout" => WorkflowExecutionStatus::TimedOut,
            "reset" => WorkflowExecutionStatus::Reset,
            "pending" => WorkflowExecutionStatus::Pending,
            _ => WorkflowExecutionStatus::Unspecified,
        }
    }

    /// A run as DescribeWorkflowExecution, ListWorkflows and SearchWorkflows
    /// report it: one conversion, so a field filled in is filled in for all.
    ///
    /// `attempt` is the run's attempt counted from 1, the number its
    /// activations carry: 1 for a first run, 2 for the run of its first
    /// workflow-level retry. It was never set, and read 0 for every run.
    fn execution_info(execution: WorkflowExecution) -> WorkflowExecutionInfo {
        let timestamp = |t: orcher_common::types::time::Timestamptz| prost_types::Timestamp {
            seconds: t.timestamp(),
            nanos: t.timestamp_subsec_nanos() as i32,
        };
        WorkflowExecutionInfo {
            labels: Self::search_attrs_to_proto(&execution.labels),
            annotations: Self::search_attrs_to_proto(&execution.annotations),
            status: Self::status_to_proto(&execution.status).into(),
            attempt: crate::engine::dispatch::converter::workflow_attempt(execution.retry_count),
            start_time: execution.started_at.map(timestamp),
            close_time: execution.completed_at.map(timestamp),
            workflow_id: execution.workflow_id.unwrap_or_default(),
            execution_id: execution.execution_id,
            workflow_type: execution.workflow_type,
            task_queue: execution.task_queue,
            namespace: execution.namespace,
            ..Default::default()
        }
    }

    /// Convert JSONB labels to proto map<string, string>
    fn search_attrs_to_proto(
        attrs: &serde_json::Value,
    ) -> std::collections::HashMap<String, String> {
        attrs
            .as_object()
            .map(|obj| {
                obj.iter()
                    .map(|(k, v)| {
                        let val = match v {
                            serde_json::Value::String(s) => s.clone(),
                            other => other.to_string(),
                        };
                        (k.clone(), val)
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Encode a pagination cursor into an opaque base64 token.
    ///
    /// Token format (base64 of JSON): `{"ms":<submitted_at_millis>,"e":"<execution_id>"}`
    fn encode_page_token(
        submitted_at: orcher_common::types::time::Timestamptz,
        execution_id: &str,
    ) -> Vec<u8> {
        use base64::{engine::general_purpose::STANDARD, Engine};
        let json = format!(
            r#"{{"ms":{},"e":"{}"}}"#,
            submitted_at.timestamp_millis(),
            execution_id
        );
        STANDARD.encode(json).into_bytes()
    }

    /// Decode an opaque pagination token back into cursor fields.
    fn decode_page_token(
        token: &[u8],
    ) -> Option<(orcher_common::types::time::Timestamptz, String)> {
        use base64::{engine::general_purpose::STANDARD, Engine};
        if token.is_empty() {
            return None;
        }
        let json_bytes = STANDARD.decode(token).ok()?;
        let v: serde_json::Value = serde_json::from_slice(&json_bytes).ok()?;
        let ms = v.get("ms")?.as_i64()?;
        let e = v.get("e")?.as_str()?.to_string();
        let ts = orcher_common::types::time::Timestamptz::from_millisecond(ms)?;
        Some((ts, e))
    }

    /// Parse a SQL-like search query string into list execution filters.
    ///
    /// Supports AND-joined equality clauses, e.g.:
    /// - `"WorkflowType = 'OrderProcessing' AND Status = 'Running'"`
    /// - `"Status = 'completed'"`
    /// - `"CustomerId = '123' AND Region = 'us-east-1'"` (unknown fields → labels)
    ///
    /// Known fields (case-insensitive):
    /// - `WorkflowType` / `WorkflowName` → `workflow_name`
    /// - `Status`                        → `status` (lowercased)
    /// - `WorkflowVersion` / `Version`   → `workflow_version`
    /// - `TaskQueue`                     → `task_queue`
    /// - anything else                   → stored as a label filter
    fn parse_search_query(query: &str) -> SearchQuery {
        if query.trim().is_empty() {
            return SearchQuery::default();
        }

        let mut workflow_name: Option<String> = None;
        let mut status: Option<String> = None;
        let mut workflow_version: Option<String> = None;
        let mut task_queue: Option<String> = None;
        let mut label_map: serde_json::Map<String, serde_json::Value> = serde_json::Map::new();

        // Split on " AND " case-insensitively.
        // to_ascii_uppercase() preserves byte offsets, so indices are valid in the original.
        let upper = query.to_ascii_uppercase();
        let sentinel = " AND ";
        let mut clauses: Vec<&str> = Vec::new();
        let mut start = 0;
        let mut search_from = 0;
        while let Some(rel) = upper[search_from..].find(sentinel) {
            let abs = search_from + rel;
            clauses.push(query[start..abs].trim());
            search_from = abs + sentinel.len();
            start = search_from;
        }
        clauses.push(query[start..].trim());

        for clause in clauses {
            let eq_pos = match clause.find('=') {
                Some(p) => p,
                None => continue,
            };
            let field = clause[..eq_pos].trim().to_ascii_lowercase();
            let raw = clause[eq_pos + 1..].trim();
            // Strip surrounding single or double quotes
            let value = if raw.len() >= 2
                && ((raw.starts_with('\'') && raw.ends_with('\''))
                    || (raw.starts_with('"') && raw.ends_with('"')))
            {
                raw[1..raw.len() - 1].to_string()
            } else {
                raw.to_string()
            };

            match field.as_str() {
                "workflowtype" | "workflowname" | "type" => {
                    workflow_name = Some(value);
                }
                "status" => {
                    status = Some(value.to_ascii_lowercase());
                }
                "workflowversion" | "version" => {
                    workflow_version = Some(value);
                }
                "taskqueue" | "task_queue" => {
                    task_queue = Some(value);
                }
                _ => {
                    label_map.insert(field, serde_json::Value::String(value));
                }
            }
        }

        let labels = if label_map.is_empty() {
            None
        } else {
            Some(serde_json::Value::Object(label_map))
        };

        SearchQuery {
            workflow_name,
            status,
            workflow_version,
            task_queue,
            labels,
        }
    }

    /// Convert proto WorkflowExecutionStatus to database status string
    fn proto_to_status(status: WorkflowExecutionStatus) -> Option<String> {
        match status {
            WorkflowExecutionStatus::Unspecified => None,
            WorkflowExecutionStatus::Running => Some("running".to_string()),
            WorkflowExecutionStatus::Completed => Some("completed".to_string()),
            WorkflowExecutionStatus::Failed => Some("failed".to_string()),
            WorkflowExecutionStatus::Canceled => Some("cancelled".to_string()),
            WorkflowExecutionStatus::Terminated => Some("terminated".to_string()),
            WorkflowExecutionStatus::RestartedFresh => Some("restarted_fresh".to_string()),
            WorkflowExecutionStatus::TimedOut => Some("timed_out".to_string()),
            WorkflowExecutionStatus::Reset => Some("reset".to_string()),
            WorkflowExecutionStatus::Pending => Some("pending".to_string()),
        }
    }
}

#[tonic::async_trait]
impl QueryServiceTrait for QueryService {
    /// List workflow executions with optional filtering
    #[instrument(skip(self, request), fields(
        namespace = %request.get_ref().namespace,
        workflow_type = %request.get_ref().workflow_type
    ))]
    async fn list_workflows(
        &self,
        request: Request<ListWorkflowsRequest>,
    ) -> Result<Response<ListWorkflowsResponse>, Status> {
        // The calling tenant, read before the request is consumed.
        let organization_id = crate::grpc::tenant::organization_from_metadata(request.metadata());
        let req = request.into_inner();

        debug!(
            namespace = %req.namespace,
            workflow_type = %req.workflow_type,
            page_size = req.page_size,
            "Received ListWorkflows request"
        );

        // Parse namespace (default to "default")
        let namespace = if req.namespace.is_empty() {
            "default".to_string()
        } else {
            req.namespace
        };

        // Decode cursor from next_page_token if provided
        let cursor = Self::decode_page_token(&req.next_page_token);

        // Build filters
        let mut filters = ListExecutionsFilters {
            // Scoped to the caller. Without this a list spans every
            // organisation that shares a namespace name.
            organization_id,
            namespace: Some(namespace),
            limit: req.page_size.clamp(1, 1000) as i64,
            offset: 0,
            cursor_submitted_at: cursor.as_ref().map(|(ts, _)| *ts),
            cursor_execution_id: cursor.map(|(_, e)| e),
            ..Default::default()
        };

        if !req.workflow_type.is_empty() {
            filters.workflow_name = Some(req.workflow_type);
        }
        if !req.task_queue.is_empty() {
            filters.task_queue = Some(req.task_queue);
        }

        // Handle status filter (take first status if multiple provided)
        if !req.status_filter.is_empty() {
            if let Some(status) = req.status_filter.first() {
                let proto_status = WorkflowExecutionStatus::try_from(*status)
                    .unwrap_or(WorkflowExecutionStatus::Unspecified);
                filters.status = Self::proto_to_status(proto_status);
            }
        }

        let page_limit = filters.limit as usize;

        // Query executions
        let executions = self
            .workflow_execution_repository
            .list(filters)
            .await
            .map_err(|e| {
                error!(error = %e, "Failed to list workflow executions");
                Status::internal(format!("Failed to list workflows: {}", e))
            })?;

        // Build next_page_token before consuming executions
        let next_page_token = if executions.len() == page_limit {
            executions
                .last()
                .map(|e| Self::encode_page_token(e.submitted_at, &e.execution_id))
                .unwrap_or_default()
        } else {
            vec![]
        };

        // Convert to proto format
        let execution_infos: Vec<WorkflowExecutionInfo> =
            executions.into_iter().map(Self::execution_info).collect();

        info!(
            count = execution_infos.len(),
            has_more = !next_page_token.is_empty(),
            "ListWorkflows completed successfully"
        );

        Ok(Response::new(ListWorkflowsResponse {
            executions: execution_infos,
            next_page_token,
        }))
    }

    /// Search workflows with advanced query syntax
    #[instrument(skip(self, request), fields(
        namespace = %request.get_ref().namespace,
        query = %request.get_ref().query
    ))]
    async fn search_workflows(
        &self,
        request: Request<SearchWorkflowsRequest>,
    ) -> Result<Response<SearchWorkflowsResponse>, Status> {
        // The calling tenant, read before the request is consumed.
        let organization_id = crate::grpc::tenant::organization_from_metadata(request.metadata());
        let req = request.into_inner();

        debug!(
            namespace = %req.namespace,
            query = %req.query,
            page_size = req.page_size,
            "Received SearchWorkflows request"
        );

        // Parse namespace
        let namespace = if req.namespace.is_empty() {
            "default".to_string()
        } else {
            req.namespace
        };

        // Parse the SQL-like query string into filter fields
        let search = Self::parse_search_query(&req.query);

        // Decode cursor from next_page_token if provided
        let cursor = Self::decode_page_token(&req.next_page_token);

        let filters = ListExecutionsFilters {
            // Scoped to the caller. Without this a list spans every
            // organisation that shares a namespace name.
            organization_id,
            namespace: Some(namespace),
            workflow_name: search.workflow_name,
            status: search.status,
            workflow_version: search.workflow_version,
            task_queue: search.task_queue,
            labels: search.labels,
            limit: req.page_size.clamp(1, 1000) as i64,
            offset: 0,
            cursor_submitted_at: cursor.as_ref().map(|(ts, _)| *ts),
            cursor_execution_id: cursor.map(|(_, e)| e),
            ..Default::default()
        };

        let page_limit = filters.limit as usize;

        let executions = self
            .workflow_execution_repository
            .list(filters)
            .await
            .map_err(|e| {
                error!(error = %e, "Failed to search workflow executions");
                Status::internal(format!("Failed to search workflows: {}", e))
            })?;

        // Build next_page_token before consuming executions
        let next_page_token = if executions.len() == page_limit {
            executions
                .last()
                .map(|e| Self::encode_page_token(e.submitted_at, &e.execution_id))
                .unwrap_or_default()
        } else {
            vec![]
        };

        let execution_infos: Vec<WorkflowExecutionInfo> =
            executions.into_iter().map(Self::execution_info).collect();

        info!(
            count = execution_infos.len(),
            has_more = !next_page_token.is_empty(),
            "SearchWorkflows completed successfully"
        );

        Ok(Response::new(SearchWorkflowsResponse {
            executions: execution_infos,
            next_page_token,
        }))
    }

    /// Get execution journal for a workflow
    #[instrument(skip(self, request), fields(
        workflow_id = %request.get_ref().workflow_id,
        execution_id = %request.get_ref().execution_id
    ))]
    async fn get_execution_journal(
        &self,
        request: Request<GetExecutionJournalRequest>,
    ) -> Result<Response<GetExecutionJournalResponse>, Status> {
        // The calling tenant, read before the request is consumed.
        let organization_id = crate::grpc::tenant::organization_from_metadata(request.metadata());
        let req = request.into_inner();

        debug!(
            workflow_id = %req.workflow_id,
            execution_id = %req.execution_id,
            page_size = req.page_size,
            "Received GetExecutionJournal request"
        );

        if req.workflow_id.is_empty() && req.execution_id.is_empty() {
            return Err(Status::invalid_argument(
                "workflow_id or execution_id is required",
            ));
        }

        // The run the request names, if the caller may see it. The journal
        // is keyed by the run's internal id (`id`, a UUID with dashes) and its
        // execution id.
        let exec = find_named_run(
            &self.workflow_execution_repository,
            organization_id,
            &req.namespace,
            &req.workflow_id,
            &req.execution_id,
        )
        .await?
        .ok_or_else(|| Status::not_found("No execution found for workflow"))?;
        let (workflow_id, execution_id) = (exec.id.as_uuid().to_string(), exec.execution_id);

        // Get journal entries
        debug!(
            journal_workflow_id = %workflow_id,
            journal_execution_id = %execution_id,
            "Querying journal with resolved IDs"
        );

        let journal_entries = self
            .journal_repository
            .get_journal(&workflow_id, &execution_id)
            .await
            .map_err(|e| {
                error!(error = %e, "Failed to get execution journal");
                Status::internal(format!("Failed to get execution journal: {}", e))
            })?;

        // Convert to proto JournalEntry format (attributes fully populated via JournalMapper)
        let proto_entries = JournalMapper::new()
            .to_proto_vec(&journal_entries)
            .map_err(|e| {
                error!(error = %e, "Failed to map journal entries to proto");
                Status::internal(format!("Failed to map journal entries: {}", e))
            })?;

        info!(
            workflow_id = %req.workflow_id,
            execution_id = %execution_id,
            entry_count = proto_entries.len(),
            "GetExecutionJournal completed successfully"
        );

        Ok(Response::new(GetExecutionJournalResponse {
            journal: proto_entries,
            next_page_token: vec![],
            archived: false,
        }))
    }

    /// Describe a workflow execution with detailed info
    #[instrument(skip(self, request), fields(
        workflow_id = %request.get_ref().workflow_id,
        execution_id = %request.get_ref().execution_id
    ))]
    async fn describe_workflow_execution(
        &self,
        request: Request<DescribeWorkflowExecutionRequest>,
    ) -> Result<Response<DescribeWorkflowExecutionResponse>, Status> {
        // The calling tenant, read before the request is consumed.
        let organization_id = crate::grpc::tenant::organization_from_metadata(request.metadata());
        let req = request.into_inner();

        debug!(
            workflow_id = %req.workflow_id,
            execution_id = %req.execution_id,
            "Received DescribeWorkflowExecution request"
        );

        if req.workflow_id.is_empty() && req.execution_id.is_empty() {
            return Err(Status::invalid_argument(
                "Either workflow_id or execution_id is required",
            ));
        }

        // The run the request names, if the caller may see it: the exact run
        // for an execution id, else the workflow's latest run in its namespace.
        let execution = find_named_run(
            &self.workflow_execution_repository,
            organization_id,
            &req.namespace,
            &req.workflow_id,
            &req.execution_id,
        )
        .await?
        .ok_or_else(|| Status::not_found("Execution not found"))?;

        let workflow_row_id = execution.id.as_uuid();
        let execution_info = Self::execution_info(execution);

        let pending_tasks = self
            .task_execution_repository
            .get_workflow_task_summary(WorkflowId::from_uuid(workflow_row_id))
            .await
            .map(|s| (s.running_tasks + s.scheduled_tasks) as i32)
            .unwrap_or(0);

        let pending_timers = self
            .coordination_repository
            .list_workflow_timers(workflow_row_id, false)
            .await
            .map(|t| t.len() as i32)
            .unwrap_or(0);

        info!(
            execution_id = %execution_info.execution_id,
            pending_tasks,
            pending_timers,
            "DescribeWorkflowExecution completed successfully"
        );

        Ok(Response::new(DescribeWorkflowExecutionResponse {
            execution_info: Some(execution_info),
            pending_tasks,
            pending_timers,
            pending_events: 0, // EventGate not yet wired into QueryService
            execution_config: None,
        }))
    }

    /// Count workflows matching a query
    #[instrument(skip(self, request), fields(
        namespace = %request.get_ref().namespace,
        query = %request.get_ref().query
    ))]
    async fn count_workflows(
        &self,
        request: Request<CountWorkflowsRequest>,
    ) -> Result<Response<CountWorkflowsResponse>, Status> {
        // The calling tenant, read before the request is consumed.
        let organization_id = crate::grpc::tenant::organization_from_metadata(request.metadata());
        let req = request.into_inner();

        debug!(
            namespace = %req.namespace,
            query = %req.query,
            "Received CountWorkflows request"
        );

        let namespace = if req.namespace.is_empty() {
            "default".to_string()
        } else {
            req.namespace
        };

        let search = Self::parse_search_query(&req.query);

        let filters = ListExecutionsFilters {
            // Scoped to the caller. Without this a list spans every
            // organisation that shares a namespace name.
            organization_id,
            namespace: Some(namespace),
            workflow_name: search.workflow_name,
            status: search.status,
            workflow_version: search.workflow_version,
            task_queue: search.task_queue,
            labels: search.labels,
            ..Default::default()
        };

        let count = self
            .workflow_execution_repository
            .count(filters)
            .await
            .map_err(|e| {
                error!(error = %e, "Failed to count workflows");
                Status::internal(format!("Failed to count workflows: {}", e))
            })?;

        info!(count = count, "CountWorkflows completed successfully");

        Ok(Response::new(CountWorkflowsResponse { count }))
    }

    /// Get workflow execution statistics
    #[instrument(skip(self, request), fields(
        namespace = %request.get_ref().namespace
    ))]
    async fn get_workflow_execution_stats(
        &self,
        request: Request<GetWorkflowExecutionStatsRequest>,
    ) -> Result<Response<GetWorkflowExecutionStatsResponse>, Status> {
        // The calling tenant, read before the request is consumed.
        let organization_id = crate::grpc::tenant::organization_from_metadata(request.metadata());
        let req = request.into_inner();

        debug!(
            namespace = %req.namespace,
            workflow_type = %req.workflow_type,
            task_queue = %req.task_queue,
            "Received GetWorkflowExecutionStats request"
        );

        let namespace = if req.namespace.is_empty() {
            "default".to_string()
        } else {
            req.namespace
        };

        // Get total count
        let mut filters = ListExecutionsFilters {
            // Scoped to the caller. Without this a list spans every
            // organisation that shares a namespace name.
            organization_id,
            namespace: Some(namespace.clone()),
            ..Default::default()
        };

        if !req.workflow_type.is_empty() {
            filters.workflow_name = Some(req.workflow_type);
        }

        let total_count = self
            .workflow_execution_repository
            .count(filters.clone())
            .await
            .map_err(|e| {
                error!(error = %e, "Failed to get workflow stats");
                Status::internal(format!("Failed to get workflow stats: {}", e))
            })?;

        // Get counts by status
        let mut status_counts = std::collections::HashMap::new();

        for status in ["running", "completed", "failed", "cancelled", "terminated"] {
            let mut status_filters = filters.clone();
            status_filters.status = Some(status.to_string());

            let count = self
                .workflow_execution_repository
                .count(status_filters)
                .await
                .unwrap_or(0);

            if count > 0 {
                status_counts.insert(status.to_string(), count);
            }
        }

        let (avg_ms, median_ms, p95_ms, p99_ms) = self
            .workflow_execution_repository
            .get_execution_time_percentiles(
                &namespace,
                organization_id,
                filters.workflow_name.as_deref(),
            )
            .await
            .unwrap_or((None, None, None, None));

        let ms_to_proto_duration = |ms: Option<f64>| {
            ms.map(|m| prost_types::Duration {
                seconds: (m / 1000.0) as i64,
                nanos: ((m % 1000.0) * 1_000_000.0) as i32,
            })
        };

        info!(
            total = total_count,
            avg_ms = avg_ms,
            "GetWorkflowExecutionStats completed successfully"
        );

        Ok(Response::new(GetWorkflowExecutionStatsResponse {
            total_count,
            status_counts,
            average_execution_time: ms_to_proto_duration(avg_ms),
            median_execution_time: ms_to_proto_duration(median_ms),
            p95_execution_time: ms_to_proto_duration(p95_ms),
            p99_execution_time: ms_to_proto_duration(p99_ms),
        }))
    }

    /// Get execution logs (human-readable text logs)
    #[instrument(skip(self, request), fields(
        workflow_id = %request.get_ref().workflow_id,
        execution_id = %request.get_ref().execution_id
    ))]
    async fn get_execution_logs(
        &self,
        request: Request<GetExecutionLogsRequest>,
    ) -> Result<Response<GetExecutionLogsResponse>, Status> {
        // The calling tenant, read before the request is consumed.
        let caller = crate::grpc::tenant::organization_from_metadata(request.metadata());
        let req = request.into_inner();

        debug!(
            workflow_id = %req.workflow_id,
            execution_id = %req.execution_id,
            page_size = req.page_size,
            level = %req.level,
            "Received GetExecutionLogs request"
        );

        if req.workflow_id.is_empty() && req.execution_id.is_empty() {
            return Err(Status::invalid_argument(
                "workflow_id or execution_id is required",
            ));
        }

        // Resolve the execution and prove it is the caller's before reading a
        // line of it. Previously the `execution_id` branch below resolved
        // nothing at all — the id went straight to the logs repository — so an
        // execution id was sufficient authority to read another organisation's
        // application logs, which routinely carry business data.
        let execution_id = self
            .owned_execution(&req.namespace, &req.workflow_id, &req.execution_id, caller)
            .await?
            .execution_id;

        let limit = req.page_size.clamp(1, 1000) as i64;

        // Get logs based on filters
        let logs = if !req.level.is_empty() {
            self.logs_repository
                .get_logs_by_level(&execution_id, &req.level, limit)
                .await
        } else if !req.step.is_empty() {
            self.logs_repository
                .get_logs_by_step(&execution_id, &req.step, limit)
                .await
        } else if req.after_id > 0 {
            self.logs_repository
                .get_logs_after(&execution_id, req.after_id, limit)
                .await
        } else {
            self.logs_repository.get_logs(&execution_id, limit).await
        }
        .map_err(|e| {
            error!(error = %e, "Failed to get execution logs");
            Status::internal(format!("Failed to get execution logs: {}", e))
        })?;

        // Get total count
        let total_count = self
            .logs_repository
            .count_logs(&execution_id)
            .await
            .unwrap_or(0);

        // Convert to proto format
        let proto_logs: Vec<ExecutionLogEntry> = logs
            .into_iter()
            .map(|log| ExecutionLogEntry {
                id: log.id,
                execution_id: log.execution_id,
                timestamp: Some(prost_types::Timestamp {
                    seconds: log.timestamp.timestamp(),
                    nanos: log.timestamp.timestamp_subsec_nanos() as i32,
                }),
                level: log.level,
                message: log.message,
                step: log.step.unwrap_or_default(),
                task_id: log.task_id.map(|id| id.to_string()).unwrap_or_default(),
                worker_id: log.worker_id.unwrap_or_default(),
                source: log.source.unwrap_or_default(),
                metadata: log.metadata.to_string(),
            })
            .collect();

        info!(
            execution_id = %execution_id,
            log_count = proto_logs.len(),
            total_count = total_count,
            "GetExecutionLogs completed successfully"
        );

        Ok(Response::new(GetExecutionLogsResponse {
            logs: proto_logs,
            next_page_token: vec![],
            total_count,
        }))
    }

    /// Get task executions for a workflow
    #[instrument(skip(self, request), fields(
        workflow_id = %request.get_ref().workflow_id,
        execution_id = %request.get_ref().execution_id
    ))]
    async fn get_task_executions(
        &self,
        request: Request<GetTaskExecutionsRequest>,
    ) -> Result<Response<GetTaskExecutionsResponse>, Status> {
        // The calling tenant, read before the request is consumed.
        let caller = crate::grpc::tenant::organization_from_metadata(request.metadata());
        let req = request.into_inner();

        debug!(
            workflow_id = %req.workflow_id,
            execution_id = %req.execution_id,
            page_size = req.page_size,
            status = %req.status,
            "Received GetTaskExecutions request"
        );

        if req.workflow_id.is_empty() && req.execution_id.is_empty() {
            return Err(Status::invalid_argument(
                "workflow_id or execution_id is required",
            ));
        }

        // Resolve execution to get internal workflow_id (UUID), refusing one
        // that is not the caller's. This call returns `task_id`, `execution_id`
        // and `service_identity` for every task, which is precisely what a task
        // token is made of — so leaving it unscoped handed one tenant the
        // material to forge tokens against another's tasks.
        let internal_workflow_id = self
            .owned_execution(&req.namespace, &req.workflow_id, &req.execution_id, caller)
            .await?
            .id;

        // Get task executions using the internal WorkflowId
        let workflow_id_typed = WorkflowId::from_uuid(internal_workflow_id.as_uuid());
        let tasks = self
            .task_execution_repository
            .find_by_workflow_id(workflow_id_typed)
            .await
            .map_err(|e| {
                error!(error = %e, "Failed to get task executions");
                Status::internal(format!("Failed to get task executions: {}", e))
            })?;

        // Convert to proto format
        let proto_tasks: Vec<TaskExecutionInfo> = tasks
            .into_iter()
            .map(|task| TaskExecutionInfo {
                id: task.id.to_string(),
                task_id: task.task_id,
                task_type: task.task_type,
                workflow_id: task.workflow_id.to_string(),
                execution_id: task.execution_id.to_string(),
                task_queue: task.task_queue,
                status: task.status,
                attempt: task.attempt_number,
                service_identity: task.service_identity.unwrap_or_default(),
                scheduled_at: Some(prost_types::Timestamp {
                    seconds: task.scheduled_at.timestamp(),
                    nanos: task.scheduled_at.timestamp_subsec_nanos() as i32,
                }),
                started_at: task.started_at.map(|t| prost_types::Timestamp {
                    seconds: t.timestamp(),
                    nanos: t.timestamp_subsec_nanos() as i32,
                }),
                completed_at: task.completed_at.map(|t| prost_types::Timestamp {
                    seconds: t.timestamp(),
                    nanos: t.timestamp_subsec_nanos() as i32,
                }),
                error_message: task.error_message.unwrap_or_default(),
                error_type: task.error_type.unwrap_or_default(),
                heartbeat_details: task
                    .heartbeat_details
                    .map(|h| String::from_utf8_lossy(&h).to_string())
                    .unwrap_or_default(),
            })
            .collect();

        let total_count = proto_tasks.len() as i64;

        info!(
            workflow_id = %internal_workflow_id,
            task_count = proto_tasks.len(),
            "GetTaskExecutions completed successfully"
        );

        Ok(Response::new(GetTaskExecutionsResponse {
            tasks: proto_tasks,
            next_page_token: vec![],
            total_count,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `TaskQueue = '…'` filters by task queue; it used to be
    /// taken for a label and match nothing.
    #[test]
    fn a_search_names_the_task_queue_as_a_filter() {
        let search = QueryService::parse_search_query(
            "TaskQueue = 'orders' AND WorkflowType = 'Checkout' AND Region = 'eu'",
        );
        assert_eq!(search.task_queue.as_deref(), Some("orders"));
        assert_eq!(search.workflow_name.as_deref(), Some("Checkout"));
        assert_eq!(search.labels, Some(serde_json::json!({"region": "eu"})));
    }
}
