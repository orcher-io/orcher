//! Workflow management gRPC service.

use crate::grpc::payload_limits::PayloadLimits;
use dashmap::DashMap;
use orcher_proto::orcher::v1::{
    workflow_service_server::{WorkflowService as WorkflowServiceTrait, WorkflowServiceServer},
    BatchCancelWorkflowRequest, BatchCancelWorkflowResponse, BatchOperationError,
    BatchOperationResult, BatchSendEventRequest, BatchSendEventResponse,
    BatchTerminateWorkflowRequest, BatchTerminateWorkflowResponse, CancelWorkflowRequest,
    CancelWorkflowResponse, GetWorkflowResultRequest, GetWorkflowResultResponse,
    GetWorkflowStatusRequest, GetWorkflowStatusResponse, QueryWorkflowRequest,
    QueryWorkflowResponse, ResetWorkflowRequest, ResetWorkflowResponse, SendEventRequest,
    SendEventResponse, SendEventWithStartWorkflowRequest, SendEventWithStartWorkflowResponse,
    StartWorkflowRequest, StartWorkflowResponse, TerminateWorkflowRequest,
    TerminateWorkflowResponse, UpdateWorkflowRequest, UpdateWorkflowResponse, WorkflowExecution,
    WorkflowStatus as ProtoWorkflowStatus,
};
use orcher_proto::tonic::{Request, Response, Status};
use orcher_storage::repositories::domains::workflow_execution::{
    is_start_request_held, is_workflow_id_held, WorkflowExecutionRepository,
};
use orcher_storage::schema::types::OverlapPolicy;
use orcher_storage::traits::domain::NamespaceRepositoryTrait;
use std::sync::Arc;
use tokio::sync::{watch, Notify};
use tracing::{debug, error, info, instrument, warn};

/// A run's final outcome, as handed to the clients waiting on its result.
///
/// Only a run that has ended for good has one: [`CompletionOutcome::of_run`]
/// is the one way to make it, and it asks [`run_is_final`] first. A failure
/// its retry policy is about to retry is not an outcome — the workflow goes on
/// in the retry — and handing it to a waiter answered the client with a
/// failure the workflow then recovered from. The completion paths used to
/// build the outcome themselves, each with its own idea of "finished"; one of
/// them answered every failure.
///
/// [`run_is_final`]: crate::workflows::execution::run_is_final
#[derive(Clone, Debug)]
pub struct CompletionOutcome {
    response: GetWorkflowResultResponse,
}

impl CompletionOutcome {
    /// `run`'s outcome, if it has ended for good; `None` while anything may
    /// still run it again.
    pub fn of_run(run: &orcher_storage::schema::types::WorkflowExecution) -> Option<Self> {
        final_result_response(run).map(|response| Self { response })
    }

    /// The answer to give a client waiting on the run's result.
    pub fn response(&self) -> &GetWorkflowResultResponse {
        &self.response
    }

    /// The run's status.
    pub fn status(&self) -> ProtoWorkflowStatus {
        ProtoWorkflowStatus::try_from(self.response.status)
            .unwrap_or(ProtoWorkflowStatus::Unspecified)
    }
}

/// Hands a workflow's outcome to everyone waiting on its result.
///
/// One watch channel per execution id, shared between the service that waits
/// (GetWorkflowResult) and the one that completes (ExecutionService).
///
/// The map is private so that no caller can hold one of its entries: a DashMap
/// entry is a synchronous lock on its shard, and one held across an `.await`
/// stays locked while the task is parked. Completing a workflow did exactly
/// that — it looked the entry up, then read the result from the database with
/// the entry still in hand — and every result waiter hashing to the same shard
/// then blocked its runtime thread, not just its task. Under load that took the
/// runtime's threads one by one until the engine answered nothing, SIGTERM
/// included. Every method here takes and drops the lock within one call.
#[derive(Clone, Default)]
pub struct WorkflowCompletionNotifier {
    waiters: Arc<DashMap<String, watch::Sender<Option<CompletionOutcome>>>>,
}

impl WorkflowCompletionNotifier {
    /// Wait on `execution_id`'s outcome, creating the channel if this is the
    /// first waiter.
    pub fn subscribe(&self, execution_id: &str) -> watch::Receiver<Option<CompletionOutcome>> {
        self.waiters
            .entry(execution_id.to_string())
            .or_insert_with(|| watch::channel(None).0)
            .subscribe()
    }

    /// Whether anyone is waiting on `execution_id`, so a completer can skip
    /// building an outcome nobody will read.
    pub fn has_waiters(&self, execution_id: &str) -> bool {
        self.waiters.contains_key(execution_id)
    }

    /// Give `execution_id`'s waiters its outcome. Nothing happens if there are
    /// none.
    pub fn notify(&self, execution_id: &str, outcome: CompletionOutcome) {
        if let Some(sender) = self.waiters.get(execution_id) {
            let _ = sender.send(Some(outcome));
        }
    }

    /// Forget `execution_id`'s channel once its waiters are done with it.
    pub fn remove(&self, execution_id: &str) {
        self.waiters.remove(execution_id);
    }
}

use crate::engine::dispatch::PollingManager;
use crate::engine::queues::ScheduleQueue;
use crate::grpc::runs::find_named_run;
use crate::grpc::services::query_dispatch::{PendingQuery, PendingQueryRegistry};
use crate::grpc::services::update_dispatch::{PendingUpdate, PendingUpdateRegistry};
use crate::utils::payload_handler::decode_input_payload;
use crate::workflows::execution::{run_was_replaced, string_map};
use crate::workflows::retry::convert_retry_policy;
use crate::workflows::{
    BatchStartOutcome, CancelOutcome, RunStart, RunTimeouts, StartOutcome,
    WorkflowExecutionService, WorkflowIdReusePolicy, WorkflowStart,
};
use orcher_common::types::time::Timestamptz;
use orcher_proto::orcher::v1::QueryRequest;
use orcher_storage::repositories::domains::child_workflow::ChildWorkflowRepository;
use orcher_storage::repositories::domains::journal::JournalRepository;
use std::sync::Mutex;

/// Default timeout for GetWorkflowResult if not specified (60 seconds)
const DEFAULT_RESULT_TIMEOUT_SECS: i64 = 60;

/// Default timeout for QueryWorkflow if not specified (10 seconds)
const DEFAULT_QUERY_TIMEOUT_SECS: u64 = 10;

/// Default timeout for UpdateWorkflow if not specified (30 seconds)
/// Longer than queries because updates may perform real work
const DEFAULT_UPDATE_TIMEOUT_SECS: u64 = 30;

/// WorkflowService gRPC implementation
///
/// Handles client requests for workflow management operations.
/// This is the primary interface for applications to interact with ORCHER.
pub struct WorkflowService {
    /// Workflow execution service for lifecycle operations
    workflow_execution_service: Arc<WorkflowExecutionService>,

    /// Repository for querying workflow state
    workflow_execution_repository: Arc<WorkflowExecutionRepository>,

    /// Schedule queue for upsert when schedule_config is provided
    schedule_queue: Option<Arc<ScheduleQueue>>,

    /// Shared completion channel map for get_workflow_result waiters.
    /// Keyed by execution_id. ExecutionService sends the outcome on completion,
    /// allowing waiters to return the result without querying the database.
    workflow_completion_notifier: WorkflowCompletionNotifier,

    /// Journal repository for appending event entries to workflow journals
    journal_repository: Option<Arc<JournalRepository>>,

    /// Event gate repository for opening gates when events arrive

    /// Polling manager for notifying workers of new events
    polling_manager: Option<Arc<PollingManager>>,

    /// Shared registry of pending queries awaiting worker responses.
    /// Shared with ExecutionService so it can attach queries to poll responses
    /// and deliver results from RespondQueryTask / CompleteWorkflowExecution.
    pending_queries: PendingQueryRegistry,

    /// Shared registry of pending updates awaiting worker responses.
    /// Like queries but updates can mutate workflow state and are journaled.
    pending_updates: PendingUpdateRegistry,

    /// Namespace repository for validating namespace existence before operations.
    /// When set, start_workflow and send_event_with_start_workflow will reject
    /// requests targeting non-existent or inactive namespaces.
    namespace_repository: Option<Arc<dyn NamespaceRepositoryTrait>>,

    /// Child tracking, so a reset child's successor run stays its parent's
    /// child. Unset, the child reconciliation sweep makes the same move later.
    child_repository: Option<Arc<ChildWorkflowRepository>>,

    /// How large a workflow input, event payload or update argument may be.
    payload_limits: PayloadLimits,
}

/// Refuse `what`, `size` bytes long, when it is over the payload limit: it is
/// refused here, where the caller hears why, rather than accepted and then
/// carried in every activation of the workflow after.
fn check_payload(limits: &PayloadLimits, what: &str, size: usize) -> Result<(), Status> {
    limits.check(what, size).map_err(Status::invalid_argument)
}

impl WorkflowService {
    /// Create a new WorkflowService
    ///
    /// # Arguments
    ///
    /// * `workflow_execution_service` - Service for workflow lifecycle operations
    /// * `workflow_execution_repository` - Repository for querying workflow state
    pub fn new(
        workflow_execution_service: Arc<WorkflowExecutionService>,
        workflow_execution_repository: Arc<WorkflowExecutionRepository>,
    ) -> Self {
        Self {
            workflow_execution_service,
            workflow_execution_repository,
            schedule_queue: None,
            workflow_completion_notifier: WorkflowCompletionNotifier::default(),
            journal_repository: None,
            polling_manager: None,
            pending_queries: crate::grpc::services::query_dispatch::new_pending_query_registry(),
            pending_updates: crate::grpc::services::update_dispatch::new_pending_update_registry(),
            namespace_repository: None,
            child_repository: None,
            payload_limits: PayloadLimits::default(),
        }
    }

    /// How large a workflow input, event payload or update argument may be.
    /// [`PayloadLimits::default`] unless set.
    pub fn with_payload_limits(mut self, limits: PayloadLimits) -> Self {
        self.payload_limits = limits;
        self
    }

    /// The refusal for a cancel or terminate that found the run already ended
    /// by the time it wrote: says how it ended, which is rarely "completed".
    async fn ended_meanwhile(&self, execution_id: &str, verb: &str) -> Status {
        let status = self
            .workflow_execution_repository
            .find_by_execution_id(execution_id)
            .await
            .ok()
            .flatten()
            .map(|e| e.status)
            .unwrap_or_else(|| "unknown".to_string());
        Status::failed_precondition(format!(
            "Workflow had already ended with status {status} before it could be {verb}"
        ))
    }

    /// Set the child tracking repository, so resetting a child keeps its
    /// successor run attached to the parent.
    pub fn with_child_repository(mut self, child_repository: Arc<ChildWorkflowRepository>) -> Self {
        self.child_repository = Some(child_repository);
        self
    }

    /// Set the schedule queue for handling schedule_config in StartWorkflow.
    pub fn with_schedule_queue(mut self, schedule_queue: Arc<ScheduleQueue>) -> Self {
        self.schedule_queue = Some(schedule_queue);
        self
    }

    /// Set event delivery dependencies: the journal events are recorded in,
    /// and the polling manager woken when a run has one to read.
    ///
    /// Required for `SendEvent`, `SendEventWithStart` and `BatchSendEvent`.
    pub fn with_event_support(
        mut self,
        journal_repository: Arc<JournalRepository>,
        polling_manager: Arc<PollingManager>,
    ) -> Self {
        self.journal_repository = Some(journal_repository);
        self.polling_manager = Some(polling_manager);
        self
    }

    /// How this service delivers events to runs; `UNAVAILABLE` if it was not
    /// given the journal to record them in.
    fn event_delivery(&self) -> Result<crate::workflows::event_delivery::EventDelivery, Status> {
        let journal = self
            .journal_repository
            .clone()
            .ok_or_else(|| Status::unavailable("Event delivery not configured on this server"))?;
        let delivery = crate::workflows::event_delivery::EventDelivery::new(
            journal,
            self.workflow_execution_repository.clone(),
        );
        Ok(match &self.polling_manager {
            Some(polling) => delivery.with_wake(polling.clone()),
            None => delivery,
        })
    }

    /// Set namespace repository for validating namespace existence.
    ///
    /// When configured, `start_workflow` and `send_event_with_start_workflow` will
    /// verify that the target namespace exists and is active before proceeding.
    pub fn with_namespace_repository(
        mut self,
        namespace_repository: Arc<dyn NamespaceRepositoryTrait>,
    ) -> Self {
        self.namespace_repository = Some(namespace_repository);
        self
    }

    /// Validate that a namespace exists and is active.
    ///
    /// Returns `Ok(())` if validation passes or no namespace repository is configured.
    /// Returns `Err(Status::not_found)` if the namespace doesn't exist or is inactive.
    async fn validate_namespace(
        &self,
        namespace: &str,
        organization_id: Option<&uuid::Uuid>,
    ) -> Result<(), Status> {
        if let Some(ref repo) = self.namespace_repository {
            // Scoped to the calling organisation when one was declared.
            //
            // This argument has existed on the repository since it was written
            // and every caller passed None, so a namespace resolved by name for
            // anybody. Where two organisations each hold a namespace of the
            // same name, it resolved to whichever row came back first.
            //
            // None keeps the previous behaviour, which is what single-tenant
            // deployments need: they have no organisations to scope by.
            let exists = repo.exists(namespace, organization_id).await.map_err(|e| {
                error!(namespace = %namespace, error = %e, "Failed to check namespace existence");
                Status::internal(format!("Failed to validate namespace: {}", e))
            })?;
            if !exists {
                return Err(Status::not_found(format!(
                    "Namespace '{}' does not exist or is not active. Create it first via the NamespaceService.",
                    namespace
                )));
            }
        }
        Ok(())
    }

    /// Get the shared workflow completion notifier for wiring to ExecutionService
    pub fn completion_notifier(&self) -> WorkflowCompletionNotifier {
        self.workflow_completion_notifier.clone()
    }

    /// Get the shared pending query registry for wiring to ExecutionService
    pub fn pending_queries(&self) -> PendingQueryRegistry {
        self.pending_queries.clone()
    }

    /// Get the shared pending update registry for wiring to ExecutionService
    pub fn pending_updates(&self) -> PendingUpdateRegistry {
        self.pending_updates.clone()
    }

    /// Create a gRPC server for this service
    pub fn into_server(self) -> WorkflowServiceServer<Self> {
        WorkflowServiceServer::new(self)
    }

    /// Convert database status string to proto WorkflowStatus enum
    ///
    /// Note: Database uses British spelling "cancelled", proto uses American "Canceled"
    fn status_to_proto(status: &str) -> ProtoWorkflowStatus {
        match status.to_lowercase().as_str() {
            "running" | "claimed" | "executing" | "queued" => ProtoWorkflowStatus::Running,
            "completed" | "success" => ProtoWorkflowStatus::Completed,
            "failed" | "error" => ProtoWorkflowStatus::Failed,
            "canceled" | "cancelled" => ProtoWorkflowStatus::Canceled,
            "terminated" => ProtoWorkflowStatus::Terminated,
            // A restarted-fresh predecessor is not itself a
            // terminal result — the logical workflow continues in a successor
            // execution. RestartedFresh is never counted as complete, so the
            // result path follows the chain rather than returning this row's
            // empty terminus.
            "restarted_fresh" => ProtoWorkflowStatus::RestartedFresh,
            "timed_out" | "timeout" => ProtoWorkflowStatus::TimedOut,
            "reset" => ProtoWorkflowStatus::Reset,
            "pending" => ProtoWorkflowStatus::Pending,
            "waiting" | "retrying" | "paused" => ProtoWorkflowStatus::Unspecified,
            _ => ProtoWorkflowStatus::Unspecified,
        }
    }
}

/// The answer to a client waiting on `execution`'s result, if the run has
/// ended for good; `None` while it may still run again.
///
/// The one place a run's result is read for a client: the waiter's own look at
/// the row and the completion paths' hand-over both come here, so they cannot
/// disagree about whether a run is finished or what it ended with.
fn final_result_response(
    execution: &orcher_storage::schema::types::WorkflowExecution,
) -> Option<GetWorkflowResultResponse> {
    // The engine's one definition of a finished run: a failed run only once
    // its retries are used up or settled, a timed-out run unless a retry is
    // scheduled for it.
    if !crate::workflows::execution::run_is_final(execution) {
        return None;
    }
    let status = WorkflowService::status_to_proto(&execution.status);

    let completed_at = execution.completed_at.map(|ts| prost_types::Timestamp {
        seconds: ts.timestamp(),
        nanos: ts.timestamp_subsec_nanos() as i32,
    });

    use orcher_proto::orcher::v1::get_workflow_result_response::Outcome;
    let outcome = match status {
        // A completion that carried no result leaves the column empty. That is
        // still the run's outcome: it used to read as "not written yet", so a
        // client that started waiting after such a run completed was never
        // answered and timed out.
        ProtoWorkflowStatus::Completed => execution.result.as_ref().map(|result| {
            Outcome::Result(serde_json::to_vec(result).unwrap_or_else(|_| b"null".to_vec()))
        }),
        ProtoWorkflowStatus::Failed => Some(Outcome::Error(
            execution
                .error
                .clone()
                .unwrap_or_else(|| "Unknown error".to_string()),
        )),
        ProtoWorkflowStatus::Canceled => Some(Outcome::Error("Workflow was canceled".to_string())),
        ProtoWorkflowStatus::Terminated => {
            Some(Outcome::Error("Workflow was terminated".to_string()))
        }
        ProtoWorkflowStatus::TimedOut => Some(Outcome::Error(
            execution
                .error
                .clone()
                .unwrap_or_else(|| "Workflow timed out".to_string()),
        )),
        _ => None,
    };

    Some(GetWorkflowResultResponse {
        execution: Some(WorkflowExecution {
            workflow_id: execution
                .workflow_id
                .clone()
                .unwrap_or_else(|| execution.execution_id.clone()),
            execution_id: execution.execution_id.clone(),
        }),
        status: status as i32,
        completed_at,
        outcome,
    })
}

/// A start request's retry policy in the form a run's row stores, or
/// `INVALID_ARGUMENT` for one that means nothing.
fn retry_policy_of(
    policy: Option<&orcher_proto::orcher::v1::RetryPolicy>,
) -> Result<(Option<i32>, Option<serde_json::Value>), Status> {
    convert_retry_policy(policy).map_err(|e| Status::invalid_argument(e.to_string()))
}

/// A start request as every start path reads it.
struct StartRequest {
    /// The run, and whether it may reuse its workflow id.
    start: WorkflowStart,
    /// The cron expression the request schedules the workflow on, in place
    /// of starting it now; `None` to start it now.
    cron_expression: Option<String>,
    /// The schedule's settings, if the request gave any.
    schedule_config: Option<orcher_proto::orcher::v1::ScheduleConfig>,
}

/// Read a start request: the one conversion StartWorkflow, BatchStartWorkflow
/// and SendEventWithStartWorkflow share.
///
/// Each used to read the request itself, and what one read another dropped:
/// a batch kept no time limits, labels or id reuse policy, and gave a run
/// without a workflow id none. The request is destructured whole, so a field
/// added to it is a compile error here until it is either read or stated as
/// not read, for every path at once.
fn read_start_request(
    req: StartWorkflowRequest,
    limits: &PayloadLimits,
) -> Result<StartRequest, Status> {
    let StartWorkflowRequest {
        workflow_id,
        workflow_type,
        task_queue,
        namespace,
        input,
        execution_timeout,
        run_timeout,
        task_timeout,
        retry_policy,
        cron_schedule,
        annotations,
        labels,
        request_id,
        start_delay,
        schedule_config,
        workflow_id_reuse_policy,
    } = req;

    if workflow_type.is_empty() {
        return Err(Status::invalid_argument("workflow_type is required"));
    }
    if task_queue.is_empty() {
        return Err(Status::invalid_argument("task_queue is required"));
    }
    check_payload(limits, "workflow input", input.len())?;

    let namespace = if namespace.is_empty() {
        "default".to_string()
    } else {
        namespace
    };

    // The input, with a Payload envelope unwrapped: a worker given the
    // envelope in place of the input fails to deserialize it.
    let input = if input.is_empty() {
        serde_json::Value::Null
    } else {
        let raw_input: serde_json::Value = serde_json::from_slice(&input).map_err(|e| {
            error!(error = %e, "Failed to parse input JSON");
            Status::invalid_argument(format!("Invalid input JSON: {}", e))
        })?;
        decode_input_payload(&raw_input).map_err(|e| {
            error!(error = %e, "Failed to decode input payload");
            Status::invalid_argument(format!("Failed to decode input payload: {}", e))
        })?
    };

    // The retry policy in the form a run's row stores it; refused here,
    // before anything is written, if it means nothing.
    let (max_retries, retry_policy) = retry_policy_of(retry_policy.as_ref())?;
    let reuse_policy = reuse_policy(workflow_id_reuse_policy)?;

    let request_id = (!request_id.is_empty()).then_some(request_id);
    if let Some(ref request_id) = request_id {
        if request_id.len() > MAX_REQUEST_ID_BYTES {
            return Err(Status::invalid_argument(format!(
                "request_id is {} bytes long; at most {MAX_REQUEST_ID_BYTES} are kept",
                request_id.len()
            )));
        }
    }

    // A workflow_id is part of a workflow's identity. If the client omits
    // it, generate one rather than storing NULL — otherwise the execution is
    // unaddressable by workflow_id and shows a blank id in listings. A start
    // with a request id is given one derived from it, so that a repeat of the
    // start names the same workflow, and its run the same database.
    let workflow_id = if !workflow_id.is_empty() {
        workflow_id
    } else if let Some(ref request_id) = request_id {
        workflow_id_of_request(&namespace, request_id)
    } else {
        uuid::Uuid::new_v4().to_string()
    };

    let start_delay = start_delay_of(start_delay.as_ref())?;

    let cron_expression = schedule_config
        .as_ref()
        .map(|sc| sc.cron_expression.clone())
        .filter(|s| !s.is_empty())
        .or_else(|| (!cron_schedule.is_empty()).then_some(cron_schedule));
    // Refused rather than ignored: a schedule fires at the times its cron
    // expression names, and a delay asked of it has nothing to delay.
    if cron_expression.is_some() && start_delay.is_some() {
        return Err(Status::invalid_argument(
            "start_delay cannot be given with a cron schedule; a schedule's runs start \
             when it fires",
        ));
    }

    Ok(StartRequest {
        start: WorkflowStart {
            run: RunStart {
                workflow_name: workflow_type,
                workflow_version: "1.0".to_string(),
                namespace,
                task_queue,
                input,
                workflow_id: Some(workflow_id),
                labels: string_map(&labels),
                annotations: string_map(&annotations),
                start_delay,
                request_id,
                max_retries,
                retry_policy,
                timeouts: RunTimeouts::from_durations(
                    execution_timeout.as_ref(),
                    run_timeout.as_ref(),
                    task_timeout.as_ref(),
                ),
            },
            reuse_policy,
        },
        cron_expression,
        schedule_config,
    })
}

/// The longest request id a start may give, in bytes. It is stored on the run
/// and indexed, so it is bounded; a request id is a client's token for one
/// call, which a UUID — 36 bytes — serves.
const MAX_REQUEST_ID_BYTES: usize = 255;

/// The longest start delay a start may ask for: a hundred years. A bound, so
/// that the time a run is due is always one the database can store.
const MAX_START_DELAY: std::time::Duration = std::time::Duration::from_secs(100 * 365 * 24 * 3600);

/// The workflow id a start that gives none is given when it gives a request
/// id: derived from the request id, so that a repeat of the start names the
/// same workflow. Without a request id each start is given a fresh one.
fn workflow_id_of_request(namespace: &str, request_id: &str) -> String {
    uuid::Uuid::new_v5(
        &uuid::Uuid::NAMESPACE_OID,
        format!("orcher:start-request:{namespace}:{request_id}").as_bytes(),
    )
    .to_string()
}

/// A start's delay, `None` for none (unset or zero), or `INVALID_ARGUMENT`
/// for a negative one or one past [`MAX_START_DELAY`].
fn start_delay_of(
    delay: Option<&prost_types::Duration>,
) -> Result<Option<std::time::Duration>, Status> {
    let Some(delay) = delay else {
        return Ok(None);
    };
    if delay.seconds < 0 || delay.nanos < 0 || delay.nanos >= 1_000_000_000 {
        return Err(Status::invalid_argument(format!(
            "start_delay must be a non-negative duration, got {}s {}ns",
            delay.seconds, delay.nanos
        )));
    }
    let delay = std::time::Duration::new(delay.seconds as u64, delay.nanos as u32);
    if delay > MAX_START_DELAY {
        return Err(Status::invalid_argument(format!(
            "start_delay is {}s; at most {}s is allowed",
            delay.as_secs(),
            MAX_START_DELAY.as_secs()
        )));
    }
    Ok((!delay.is_zero()).then_some(delay))
}

/// The start a SendEventWithStartWorkflow makes when no run of its workflow
/// id is open to take the event, as the StartWorkflowRequest it amounts to.
/// Both are destructured or built whole, so a field added to either is a
/// compile error here until it is carried.
fn start_request_of_event(req: &SendEventWithStartWorkflowRequest) -> StartWorkflowRequest {
    let SendEventWithStartWorkflowRequest {
        workflow_id,
        namespace,
        // The event, delivered once the run exists.
        event_name: _,
        event_payload: _,
        workflow_type,
        task_queue,
        workflow_input,
        execution_timeout,
        run_timeout,
        task_timeout,
        retry_policy,
        annotations,
        labels,
        request_id,
    } = req;
    StartWorkflowRequest {
        workflow_id: workflow_id.clone(),
        workflow_type: workflow_type.clone(),
        task_queue: task_queue.clone(),
        namespace: namespace.clone(),
        input: workflow_input.clone(),
        execution_timeout: *execution_timeout,
        run_timeout: *run_timeout,
        task_timeout: *task_timeout,
        retry_policy: retry_policy.clone(),
        cron_schedule: String::new(),
        annotations: annotations.clone(),
        labels: labels.clone(),
        request_id: request_id.clone(),
        start_delay: None,
        schedule_config: None,
        // An event-with-start starts the id only when no run of it is open,
        // and a closed one is always reused.
        workflow_id_reuse_policy: 0,
    }
}

/// The answer to a start that made, or had made, `execution`. `started_at` is
/// when the run starts: when it was submitted, or for a delayed start the
/// time it is due.
fn start_response(
    execution: &orcher_storage::schema::types::WorkflowExecution,
) -> StartWorkflowResponse {
    let started_at = execution.clocks_start_at();
    StartWorkflowResponse {
        workflow_id: execution
            .workflow_id
            .clone()
            .unwrap_or_else(|| execution.execution_id.clone()),
        execution_id: execution.execution_id.clone(),
        started_at: Some(prost_types::Timestamp {
            seconds: started_at.timestamp(),
            nanos: started_at.timestamp_subsec_nanos() as i32,
        }),
    }
}

/// The refusal of a start whose request id an earlier start of another
/// workflow gave: `INVALID_ARGUMENT`, for a request id is a client's token
/// for one start, and answering this one with that start's run would hand
/// the caller a workflow it did not ask for. Names the run, in trailing
/// metadata as well as the message, as an id in use is named.
fn request_id_in_use(execution: &orcher_storage::schema::types::WorkflowExecution) -> Status {
    let workflow_id = execution.workflow_id.as_deref().unwrap_or_default();
    let mut metadata = orcher_proto::tonic::metadata::MetadataMap::new();
    if let Ok(value) = workflow_id.parse() {
        metadata.insert(WORKFLOW_ID_METADATA_KEY, value);
    }
    if let Ok(value) = execution.execution_id.parse() {
        metadata.insert(EXECUTION_ID_METADATA_KEY, value);
    }
    Status::with_metadata(
        orcher_proto::tonic::Code::InvalidArgument,
        format!(
            "request_id was given to an earlier start of workflow {} ({}), execution {}; \
             a request id names one start",
            workflow_id, execution.workflow_type, execution.execution_id
        ),
        metadata,
    )
}

/// Trailing metadata on an ALREADY_EXISTS from a start, naming the workflow
/// whose id is in use and the execution holding it.
pub const WORKFLOW_ID_METADATA_KEY: &str = "x-orcher-workflow-id";
/// See [`WORKFLOW_ID_METADATA_KEY`].
pub const EXECUTION_ID_METADATA_KEY: &str = "x-orcher-execution-id";

/// The request's reuse policy. A value this server does not know is refused
/// rather than read as the default: a client asking for a policy it will not
/// get should hear so, not be started under another.
fn reuse_policy(value: i32) -> Result<WorkflowIdReusePolicy, Status> {
    use orcher_proto::orcher::v1::WorkflowIdReusePolicy as Proto;
    match Proto::try_from(value) {
        Ok(Proto::Unspecified | Proto::AllowDuplicate) => Ok(WorkflowIdReusePolicy::AllowDuplicate),
        Ok(Proto::AllowDuplicateFailedOnly) => Ok(WorkflowIdReusePolicy::AllowDuplicateFailedOnly),
        Ok(Proto::RejectDuplicate) => Ok(WorkflowIdReusePolicy::RejectDuplicate),
        Ok(Proto::TerminateIfRunning) => Ok(WorkflowIdReusePolicy::TerminateIfRunning),
        Err(_) => Err(Status::invalid_argument(format!(
            "unknown workflow_id_reuse_policy {value}"
        ))),
    }
}

/// The refusal of a start whose workflow id `execution` is using: still open,
/// or closed and not to be reused under the start's policy.
///
/// ALREADY_EXISTS, naming the execution in trailing metadata as well as in
/// the message, so a caller that meant to start the workflow once can go on
/// with the run that is already there rather than parse prose for it.
fn workflow_id_in_use(
    workflow_id: &str,
    execution: &orcher_storage::schema::types::WorkflowExecution,
    open: bool,
) -> Status {
    let message = if open {
        format!(
            "Workflow {} is already running as execution {}",
            workflow_id, execution.execution_id
        )
    } else {
        format!(
            "Workflow {} already ran as execution {}, which ended {}, and the start's \
             workflow_id_reuse_policy does not allow running it again",
            workflow_id, execution.execution_id, execution.status
        )
    };
    let mut metadata = orcher_proto::tonic::metadata::MetadataMap::new();
    // A workflow id is caller-chosen and need not be ASCII; one that cannot
    // travel as metadata is still in the message.
    if let Ok(value) = workflow_id.parse() {
        metadata.insert(WORKFLOW_ID_METADATA_KEY, value);
    }
    if let Ok(value) = execution.execution_id.parse() {
        metadata.insert(EXECUTION_ID_METADATA_KEY, value);
    }
    Status::with_metadata(orcher_proto::tonic::Code::AlreadyExists, message, metadata)
}

#[tonic::async_trait]
impl WorkflowServiceTrait for WorkflowService {
    /// Start a new workflow execution
    ///
    /// Creates a new workflow execution and triggers notification to waiting services.
    ///
    /// # Flow
    ///
    /// 1. Validate request (workflow_type and task_queue required)
    /// 2. Parse namespace (default to "default" if empty)
    /// 3. Deserialize input JSON from bytes
    /// 4. Call WorkflowExecutionService.start_workflow()
    /// 5. Return workflow_id, execution_id, and started_at timestamp
    #[instrument(skip(self, request), fields(
        workflow_type = %request.get_ref().workflow_type,
        task_queue = %request.get_ref().task_queue,
        namespace = %request.get_ref().namespace
    ))]
    async fn start_workflow(
        &self,
        request: Request<StartWorkflowRequest>,
    ) -> Result<Response<StartWorkflowResponse>, Status> {
        // The calling tenant, read before the request is consumed.
        let organization_id = crate::grpc::tenant::organization_from_metadata(request.metadata());
        let req = request.into_inner();

        debug!(
            workflow_type = %req.workflow_type,
            task_queue = %req.task_queue,
            namespace = %req.namespace,
            workflow_id = %req.workflow_id,
            "Received StartWorkflow request"
        );

        let StartRequest {
            start,
            cron_expression,
            schedule_config,
        } = read_start_request(req, &self.payload_limits)?;
        let namespace = start.run.namespace.clone();
        let workflow_type = start.run.workflow_name.clone();

        // Validate namespace exists and is active
        self.validate_namespace(&namespace, organization_id.as_ref())
            .await?;

        // If a cron expression is provided, upsert a schedule instead of starting immediately
        if let Some(cron_expr) = cron_expression {
            let schedule_queue = self.schedule_queue.as_ref().ok_or_else(|| {
                Status::failed_precondition(
                    "Schedule support not configured on this server instance",
                )
            })?;

            // Validate cron expression
            crate::engine::cron::validate_cron(&cron_expr)
                .map_err(|e| Status::invalid_argument(format!("Invalid cron expression: {}", e)))?;

            let sc = &schedule_config;
            let timezone = sc
                .as_ref()
                .map(|c| c.timezone.clone())
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "UTC".to_string());

            let overlap_policy = match sc
                .as_ref()
                .map(|c| c.overlap_policy.as_str())
                .unwrap_or("skip")
            {
                "parallel" => OverlapPolicy::Parallel,
                "replace" => OverlapPolicy::Replace,
                "queue" => OverlapPolicy::Queue,
                _ => OverlapPolicy::Skip,
            };

            let max_instances = sc.as_ref().map(|c| c.max_instances).unwrap_or(1);
            let max_runs = sc.as_ref().and_then(|c| {
                if c.max_runs > 0 {
                    Some(c.max_runs)
                } else {
                    None
                }
            });

            // The run each firing starts, as the schedule processor reads it
            // back (see ScheduleExecutionService).
            let task_template = crate::services::schedule_execution::schedule_template(&start.run);

            // Schedule name defaults to "{workflow_type}:{cron_expr}"
            let schedule_name = format!("{}:{}", workflow_type, cron_expr);

            let mut schedule = orcher_storage::schema::types::Schedule::new(
                schedule_name,
                cron_expr.clone(),
                timezone.clone(),
                task_template,
            );
            // Stamp the owner. The name is derived from the workflow type and
            // cron expression, so two tenants scheduling the same workflow
            // collide by construction; without an owner they collide into one
            // row and the later one silently rewrites the earlier.
            schedule.organization_id = organization_id;
            schedule.overlap_policy = overlap_policy;
            schedule.max_instances = max_instances;
            schedule.max_runs = max_runs;

            // Parse start_date / end_date from proto timestamps
            if let Some(sc) = sc {
                if let Some(ref ts) = sc.start_date {
                    schedule.start_date = Timestamptz::from_second(ts.seconds);
                }
                if let Some(ref ts) = sc.end_date {
                    schedule.end_date = Timestamptz::from_second(ts.seconds);
                }
            }

            // Compute first fire time
            let next_run =
                crate::engine::cron::next_fire_time(&cron_expr, &timezone, Timestamptz::now())
                    .map_err(|e| {
                        Status::internal(format!("Failed to compute next fire time: {}", e))
                    })?;
            schedule.next_run_at = next_run;

            // Upsert schedule
            let schedule = schedule_queue.upsert(schedule).await.map_err(|e| {
                error!(error = %e, "Failed to upsert schedule");
                Status::internal(format!("Failed to create schedule: {}", e))
            })?;

            info!(
                schedule_id = %schedule.id,
                schedule_name = %schedule.name,
                cron = %cron_expr,
                "Schedule upserted successfully"
            );

            let now = Timestamptz::now();
            return Ok(Response::new(StartWorkflowResponse {
                workflow_id: schedule.name.clone(),
                execution_id: schedule.id.to_string(),
                started_at: Some(prost_types::Timestamp {
                    seconds: now.timestamp(),
                    nanos: now.timestamp_subsec_nanos() as i32,
                }),
            }));
        }

        // No schedule — start the workflow immediately.
        let workflow_id = start.run.workflow_id.clone().unwrap_or_default();
        let outcome = self
            .workflow_execution_service
            .start_workflow_under_policy(
                start,
                // The tenant this request declared, stamped on the record.
                organization_id,
            )
            .await
            .map_err(|e| {
                error!(error = %e, "Failed to start workflow");
                Status::internal(format!("Failed to start workflow: {}", e))
            })?;
        let execution = match outcome {
            StartOutcome::Started(execution) => execution,
            // A repeat of a start already made: answered as it was, with the
            // run it made.
            StartOutcome::Repeated(execution) => {
                info!(
                    workflow_id = %workflow_id,
                    execution_id = %execution.execution_id,
                    "Answered a repeated start with the run its request id made"
                );
                return Ok(Response::new(start_response(&execution)));
            }
            StartOutcome::RequestIdInUse { execution } => {
                return Err(request_id_in_use(&execution));
            }
            StartOutcome::Refused { execution, open } => {
                info!(
                    workflow_id = %workflow_id,
                    execution_id = %execution.execution_id,
                    open,
                    "Refused a start of a workflow id already in use"
                );
                return Err(workflow_id_in_use(&workflow_id, &execution, open));
            }
        };

        info!(
            workflow_id = %execution.execution_id,
            execution_id = %execution.execution_id,
            workflow_type = %workflow_type,
            "Workflow started successfully"
        );

        // Workflows in flight are counted from the database at scrape time; an
        // in-memory gauge here read zero after every restart and was incremented
        // by only one of the ways an execution can be created.
        orcher_monitoring::metrics::lifecycle::task_submitted(&namespace, &workflow_type);

        Ok(Response::new(start_response(&execution)))
    }

    /// Get the current status of a workflow execution
    ///
    /// Queries the database for workflow execution status by execution_id.
    #[instrument(skip(self, request), fields(
        workflow_id = %request.get_ref().workflow_id,
        execution_id = %request.get_ref().execution_id,
        namespace = %request.get_ref().namespace
    ))]
    async fn get_workflow_status(
        &self,
        request: Request<GetWorkflowStatusRequest>,
    ) -> Result<Response<GetWorkflowStatusResponse>, Status> {
        let organization_id = crate::grpc::tenant::organization_from_metadata(request.metadata());
        let req = request.into_inner();

        debug!(
            workflow_id = %req.workflow_id,
            execution_id = %req.execution_id,
            namespace = %req.namespace,
            "Received GetWorkflowStatus request"
        );

        // Require an identifier.
        if req.execution_id.is_empty() && req.workflow_id.is_empty() {
            return Err(Status::invalid_argument(
                "Either workflow_id or execution_id is required",
            ));
        }

        // The run the request names, if the caller may see it.
        let execution = find_named_run(
            &self.workflow_execution_repository,
            organization_id,
            &req.namespace,
            &req.workflow_id,
            &req.execution_id,
        )
        .await?
        .ok_or_else(|| {
            let id = if req.execution_id.is_empty() {
                &req.workflow_id
            } else {
                &req.execution_id
            };
            warn!(id = %id, "Workflow execution not found");
            Status::not_found(format!("Workflow execution not found: {}", id))
        })?;

        info!(
            execution_id = %execution.execution_id,
            status = %execution.status,
            "Retrieved workflow status"
        );

        // Convert status
        let status = Self::status_to_proto(&execution.status);

        // Convert timestamps
        let started_at = execution.started_at.map(|ts| prost_types::Timestamp {
            seconds: ts.timestamp(),
            nanos: ts.timestamp_subsec_nanos() as i32,
        });

        let completed_at = execution.completed_at.map(|ts| prost_types::Timestamp {
            seconds: ts.timestamp(),
            nanos: ts.timestamp_subsec_nanos() as i32,
        });

        // Build response with outcome (result or error)
        let mut response = GetWorkflowStatusResponse {
            status: status as i32,
            execution: Some(WorkflowExecution {
                workflow_id: execution
                    .workflow_id
                    .clone()
                    .unwrap_or_else(|| execution.execution_id.clone()),
                execution_id: execution.execution_id.clone(),
            }),
            started_at,
            completed_at,
            outcome: None,
        };

        // Set outcome based on status
        if status == ProtoWorkflowStatus::Completed {
            // Only set outcome if result is available
            // If result is None, the workflow may still be writing its result (race condition)
            if let Some(result) = execution.result {
                let result_bytes = serde_json::to_vec(&result).unwrap_or_else(|_| b"null".to_vec());
                response.outcome = Some(
                    orcher_proto::orcher::v1::get_workflow_status_response::Outcome::Result(
                        result_bytes,
                    ),
                );
            }
            // Note: If result is None, outcome remains None - caller should retry or check status
        } else if status == ProtoWorkflowStatus::Failed {
            let error_message = execution
                .error
                .unwrap_or_else(|| "Unknown error".to_string());
            response.outcome = Some(
                orcher_proto::orcher::v1::get_workflow_status_response::Outcome::Error(
                    error_message,
                ),
            );
        }

        Ok(Response::new(response))
    }

    /// Get the result of a workflow execution, waiting for completion if necessary
    ///
    /// This method polls the database until the workflow completes or timeout is reached.
    #[instrument(skip(self, request), fields(
        workflow_id = %request.get_ref().workflow_id,
        execution_id = %request.get_ref().execution_id,
        namespace = %request.get_ref().namespace
    ))]
    async fn get_workflow_result(
        &self,
        request: Request<GetWorkflowResultRequest>,
    ) -> Result<Response<GetWorkflowResultResponse>, Status> {
        // Before `into_inner()`, which drops the metadata the tenant lives in.
        let caller = crate::grpc::tenant::organization_from_metadata(request.metadata());
        let req = request.into_inner();

        debug!(
            workflow_id = %req.workflow_id,
            execution_id = %req.execution_id,
            namespace = %req.namespace,
            "Received GetWorkflowResult request"
        );

        // Validate required fields - either workflow_id or execution_id must be provided
        if req.workflow_id.is_empty() && req.execution_id.is_empty() {
            return Err(Status::invalid_argument(
                "Either workflow_id or execution_id is required",
            ));
        }

        // Parse timeout (default to 60 seconds)
        let timeout_secs = if let Some(timeout) = &req.timeout {
            timeout.seconds as u64
        } else {
            DEFAULT_RESULT_TIMEOUT_SECS as u64
        };

        // Follow the named run's lineage to its current tail and return that
        // run's result. A restart-fresh, a retry and a reset each mark the run
        // replaced and start a successor (same workflow_id, parent_execution_id
        // -> predecessor); the result lives on the tail. We re-resolve on a
        // bounded interval so a run replaced *while* the client waits is still
        // followed — the completion notifier only fires for a run that has
        // ended for good, never on a replacement or a failure to be retried.
        let overall_deadline =
            tokio::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);
        const CHAIN_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(3);
        const MAX_CHAIN_HOPS: usize = 100_000;

        loop {
            // The run the client asked about, if it may see it: the exact run
            // when it names one, so a result is that run's lineage and never a
            // later workflow that reused the id. Another organisation's run
            // reads as missing — this RPC long-polls, so a caller could
            // otherwise wait on a foreign result and be handed it.
            let resolved = find_named_run(
                &self.workflow_execution_repository,
                caller,
                &req.namespace,
                &req.workflow_id,
                &req.execution_id,
            )
            .await?;

            let mut execution = resolved.ok_or_else(|| {
                let id = if !req.workflow_id.is_empty() {
                    &req.workflow_id
                } else {
                    &req.execution_id
                };
                Status::not_found(format!("Workflow execution not found: {}", id))
            })?;

            // Follow the lineage forward to the current tail.
            let mut hops = 0usize;
            while run_was_replaced(&execution) && hops < MAX_CHAIN_HOPS {
                let wf_id = execution.workflow_id.clone().unwrap_or_default();
                match self
                    .workflow_execution_repository
                    .find_latest_successor(&execution.execution_id, &wf_id)
                    .await
                    .map_err(|e| {
                        Status::internal(format!("Failed to follow the run's successors: {}", e))
                    })? {
                    Some(next) => {
                        execution = next;
                        hops += 1;
                    }
                    // Successor not created yet — wait and re-resolve.
                    None => break,
                }
            }

            let execution_id = execution.execution_id.clone();
            let workflow_id_display = execution
                .workflow_id
                .clone()
                .unwrap_or_else(|| execution_id.clone());

            // Has the tail ended for good?
            if let Some(response) = final_result_response(&execution) {
                self.workflow_completion_notifier.remove(&execution_id);
                return Ok(Response::new(response));
            }

            // Not terminal yet. Bound the wait so a restart-fresh that happens
            // mid-wait is re-resolved within CHAIN_POLL_INTERVAL.
            let now = tokio::time::Instant::now();
            if now >= overall_deadline {
                warn!(
                    workflow_id = %workflow_id_display,
                    execution_id = %execution_id,
                    timeout_secs = timeout_secs,
                    "Timeout waiting for workflow result"
                );
                return Err(Status::deadline_exceeded(format!(
                    "Workflow did not complete within {} seconds",
                    timeout_secs
                )));
            }
            let sub_timeout = (overall_deadline - now).min(CHAIN_POLL_INTERVAL);

            let mut rx = self.workflow_completion_notifier.subscribe(&execution_id);

            let outcome = tokio::time::timeout(sub_timeout, async {
                loop {
                    if rx.borrow().is_some() {
                        return rx.borrow().clone();
                    }
                    if rx.changed().await.is_err() {
                        return None;
                    }
                    if rx.borrow().is_some() {
                        return rx.borrow().clone();
                    }
                }
            })
            .await;

            self.workflow_completion_notifier.remove(&execution_id);

            if let Ok(Some(completion)) = outcome {
                // Only a run that has ended for good is ever handed over (see
                // `CompletionOutcome::of_run`), so this is the final result. A
                // failure with a retry pending is not: the wait times out on
                // this interval and the chain is followed to the retry.
                debug!(
                    workflow_id = %workflow_id_display,
                    execution_id = %execution_id,
                    status = ?completion.status(),
                    "Workflow result received via completion channel"
                );
                return Ok(Response::new(completion.response().clone()));
            }

            // Timed out on this interval, or the channel dropped: re-resolve the
            // chain and keep waiting until the overall deadline (checked above).
        }
    }

    /// Cancel a workflow execution gracefully
    ///
    /// Requests graceful cancellation by updating the workflow status to "canceled".
    /// The workflow execution will handle cancellation during its next step.
    #[instrument(skip(self, request), fields(
        workflow_id = %request.get_ref().workflow_id,
        execution_id = %request.get_ref().execution_id,
        namespace = %request.get_ref().namespace
    ))]
    async fn cancel_workflow(
        &self,
        request: Request<CancelWorkflowRequest>,
    ) -> Result<Response<CancelWorkflowResponse>, Status> {
        let organization_id = crate::grpc::tenant::organization_from_metadata(request.metadata());
        let req = request.into_inner();

        debug!(
            workflow_id = %req.workflow_id,
            execution_id = %req.execution_id,
            namespace = %req.namespace,
            reason = %req.reason,
            "Received CancelWorkflow request"
        );

        // Require an identifier.
        if req.execution_id.is_empty() && req.workflow_id.is_empty() {
            return Err(Status::invalid_argument(
                "Either workflow_id or execution_id is required",
            ));
        }

        // The run the request names, if the caller may see it.
        let execution = find_named_run(
            &self.workflow_execution_repository,
            organization_id,
            &req.namespace,
            &req.workflow_id,
            &req.execution_id,
        )
        .await?
        .ok_or_else(|| {
            let id = if req.execution_id.is_empty() {
                &req.workflow_id
            } else {
                &req.execution_id
            };
            warn!(id = %id, "Workflow execution not found");
            Status::not_found(format!("Workflow execution not found: {}", id))
        })?;

        // Check if the workflow has already ended
        if execution.has_ended() {
            warn!(
                execution_id = %execution.execution_id,
                current_status = %execution.status,
                "Workflow has already ended, cannot cancel"
            );
            return Err(Status::failed_precondition(format!(
                "Workflow has already ended with status: {}",
                execution.status
            )));
        }

        // Through the one path every cancellation takes: a workflow whose code
        // has run is asked, so it can clean up and end itself; one whose code
        // never ran is cancelled now. The status check above gives a good error
        // message, but the decision is made in the guarded writes, so a run that
        // ends meanwhile keeps its ending.
        let cleanup_deadline = cleanup_deadline(req.cleanup_timeout.as_ref())?;
        let reason = if req.reason.is_empty() {
            "Cancellation requested".to_string()
        } else {
            req.reason.clone()
        };
        let outcome = self
            .workflow_execution_service
            .cancel_execution(&execution.execution_id, &reason, cleanup_deadline)
            .await
            .map_err(|e| {
                error!(error = %e, execution_id = %execution.execution_id, "Failed to cancel workflow");
                Status::internal(format!("Failed to cancel workflow: {}", e))
            })?;
        if outcome == CancelOutcome::NotLive {
            // It reached a terminal state after the check above. Report that
            // rather than claiming a cancellation that did not happen.
            warn!(
                execution_id = %execution.execution_id,
                "Workflow reached a terminal state before the cancellation was applied"
            );
            return Err(self
                .ended_meanwhile(&execution.execution_id, "cancelled")
                .await);
        }
        Ok(Response::new(CancelWorkflowResponse { requested: true }))
    }

    /// Terminate a workflow execution forcefully
    ///
    /// Forcefully terminates a workflow by updating its status to "terminated".
    /// This is an immediate operation that does not allow cleanup.
    #[instrument(skip(self, request), fields(
        workflow_id = %request.get_ref().workflow_id,
        execution_id = %request.get_ref().execution_id,
        namespace = %request.get_ref().namespace
    ))]
    async fn terminate_workflow(
        &self,
        request: Request<TerminateWorkflowRequest>,
    ) -> Result<Response<TerminateWorkflowResponse>, Status> {
        let organization_id = crate::grpc::tenant::organization_from_metadata(request.metadata());
        let req = request.into_inner();

        debug!(
            workflow_id = %req.workflow_id,
            execution_id = %req.execution_id,
            namespace = %req.namespace,
            reason = %req.reason,
            "Received TerminateWorkflow request"
        );

        // Require an identifier.
        if req.execution_id.is_empty() && req.workflow_id.is_empty() {
            return Err(Status::invalid_argument(
                "Either workflow_id or execution_id is required",
            ));
        }

        // The run the request names, if the caller may see it.
        let execution = find_named_run(
            &self.workflow_execution_repository,
            organization_id,
            &req.namespace,
            &req.workflow_id,
            &req.execution_id,
        )
        .await?
        .ok_or_else(|| {
            let id = if req.execution_id.is_empty() {
                &req.workflow_id
            } else {
                &req.execution_id
            };
            warn!(id = %id, "Workflow execution not found");
            Status::not_found(format!("Workflow execution not found: {}", id))
        })?;

        // Check if the workflow has already ended
        if execution.has_ended() {
            warn!(
                execution_id = %execution.execution_id,
                current_status = %execution.status,
                "Workflow has already ended, cannot terminate"
            );
            return Err(Status::failed_precondition(format!(
                "Workflow has already ended with status: {}",
                execution.status
            )));
        }

        // Terminate through the guarded write, as cancel does: a run that
        // completes between the check above and this write keeps its result.
        // It is also the write the parent close policy uses, so a child
        // terminated for its parent ends exactly as one terminated here.
        let terminated = self
            .workflow_execution_repository
            .terminate(&execution.execution_id)
            .await
            .map_err(|e| {
                error!(error = %e, execution_id = %execution.execution_id, "Failed to update workflow status");
                Status::internal(format!("Failed to terminate workflow: {}", e))
            })?;

        if !terminated {
            warn!(
                execution_id = %execution.execution_id,
                "Workflow reached a terminal state before the termination was applied"
            );
            return Err(self
                .ended_meanwhile(&execution.execution_id, "terminated")
                .await);
        }

        info!(
            execution_id = %execution.execution_id,
            reason = %req.reason,
            "Workflow terminated"
        );

        Ok(Response::new(TerminateWorkflowResponse {
            terminated: true,
        }))
    }

    /// Reset a workflow execution to a specific journal point
    ///
    /// Creates a new execution that replays journal entries up to `target_event_id`,
    /// then re-executes from that point making fresh decisions. The old execution
    /// is marked with terminal status "reset".
    #[instrument(skip(self, request), fields(
        workflow_id = %request.get_ref().workflow_id,
        execution_id = %request.get_ref().execution_id,
        namespace = %request.get_ref().namespace,
        target_event_id = request.get_ref().target_event_id
    ))]
    async fn reset_workflow(
        &self,
        request: Request<ResetWorkflowRequest>,
    ) -> Result<Response<ResetWorkflowResponse>, Status> {
        let organization_id = crate::grpc::tenant::organization_from_metadata(request.metadata());
        let req = request.into_inner();

        info!(
            workflow_id = %req.workflow_id,
            target_event_id = req.target_event_id,
            reason = %req.reason,
            "Received ResetWorkflow request"
        );

        // Validate required fields
        if req.workflow_id.is_empty() && req.execution_id.is_empty() {
            return Err(Status::invalid_argument(
                "Either workflow_id or execution_id is required",
            ));
        }
        if req.target_event_id <= 0 {
            return Err(Status::invalid_argument(
                "target_event_id must be a positive integer",
            ));
        }

        // The run the request names, if the caller may see it.
        let execution = find_named_run(
            &self.workflow_execution_repository,
            organization_id,
            &req.namespace,
            &req.workflow_id,
            &req.execution_id,
        )
        .await?
        .ok_or_else(|| {
            let id = if req.execution_id.is_empty() {
                &req.workflow_id
            } else {
                &req.execution_id
            };
            Status::not_found(format!("Workflow execution not found: {}", id))
        })?;

        // Ensure journal repository is available
        let journal_repo = self.journal_repository.as_ref().ok_or_else(|| {
            Status::internal("Journal repository not configured — cannot reset workflow")
        })?;

        // The journal's `workflow_id` column holds the execution row's internal
        // UUID, not the user-supplied workflow id — the completion path parses
        // it with `Uuid::parse_str`, so a user id could never round-trip. Keying
        // the lookups below by `execution.workflow_id` matched no rows at all,
        // which reported a max event id of 0 and rejected every target.
        let journal_key = execution.id.as_uuid().to_string();

        // Validate target_event_id exists in the journal
        let max_event_id = journal_repo
            .get_max_event_id(&journal_key, &execution.execution_id)
            .await
            .map_err(|e| Status::internal(format!("Failed to query journal: {}", e)))?
            .unwrap_or(0);

        if req.target_event_id > max_event_id {
            return Err(Status::invalid_argument(format!(
                "target_event_id {} exceeds max journal event_id {} for execution {}",
                req.target_event_id, max_event_id, execution.execution_id
            )));
        }

        // A reset run takes over the workflow id's hold. Resetting a run that
        // is not the id's latest — an earlier one, closed, while a later one
        // is open — would make a second open run of the workflow; the hold
        // refuses that, and refusing it here, before the old run is marked,
        // leaves the old run as it was.
        if execution.holds_workflow_id {
            if let Some(ref wf_id) = execution.workflow_id {
                let holder = self
                    .workflow_execution_repository
                    .open_run_holding(execution.organization_id, &execution.namespace, wf_id)
                    .await
                    .map_err(|e| Status::internal(format!("Failed to query workflow: {}", e)))?;
                if let Some(holder) = holder.filter(|h| h.execution_id != execution.execution_id) {
                    return Err(Status::failed_precondition(format!(
                        "Workflow {} is running as execution {}; only that run can be reset \
                         while it is open",
                        wf_id, holder.execution_id
                    )));
                }
            }
        }

        // --- Perform the reset ---

        // 1. Mark old execution as "reset" (terminal state)
        self.workflow_execution_repository
            .update_status(
                &execution.execution_id,
                "reset",
                execution.started_at,
                Some(Timestamptz::now()),
            )
            .await
            .map_err(|e| {
                error!(error = %e, "Failed to mark execution as reset");
                Status::internal(format!("Failed to update execution status: {}", e))
            })?;

        // 2. Create new execution with same workflow identity
        let new_execution_id = uuid::Uuid::new_v4().to_string();

        let mut new_execution =
            orcher_storage::schema::types::execution::WorkflowExecution::builder()
                .execution_id(new_execution_id.clone())
                .workflow_name(execution.workflow_name.clone())
                .workflow_version(execution.workflow_version.clone())
                .namespace(execution.namespace.clone())
                .parameters(execution.parameters.clone())
                // Engine-created successor run, same as a fresh restart or a
                // workflow-level retry — `trigger_type` records the mechanism,
                // not the reason. That this run came from a reset is already
                // carried by parent_execution_id, the old run's 'reset' status,
                // and its WorkflowExecutionReset journal entry.
                .trigger_type("workflow".to_string())
                .build();

        new_execution.workflow_id = execution.workflow_id.clone();
        new_execution.workflow_type = execution.workflow_type.clone();
        new_execution.status = "running".to_string();
        new_execution.started_at = Some(Timestamptz::now());
        new_execution.root_execution_id = execution
            .root_execution_id
            .clone()
            .or(Some(execution.execution_id.clone()));
        new_execution.parent_execution_id = Some(execution.execution_id.clone());
        new_execution.task_queue = execution.task_queue.clone();
        new_execution.max_retries = execution.max_retries;
        new_execution.retry_policy = execution.retry_policy.clone();
        // The workflow's metadata goes on with it; the request id stays with
        // the run its start made.
        new_execution.labels = execution.labels.clone();
        new_execution.annotations = execution.annotations.clone();
        // Carry the release pin. The successor replays the journal copied below,
        // so it must run on the code that wrote that history — leaving it
        // unbound would let any release claim it and replay against code the
        // journal was not written by, which is exactly the divergence binding
        // exists to prevent.
        //
        // Note the contrast with restart-fresh and workflow-level retry: those
        // start an EMPTY journal, so they should adopt whatever release is
        // current rather than inherit a pin. Reset is the odd one out precisely
        // because it inherits history.
        new_execution.bound_version_id = execution.bound_version_id.clone();
        // And the step-id scheme and history lineage, for the same reason:
        // the copied history is keyed by the step ids it was written with,
        // and the lineage its child ids and random seed derive from goes on.
        // Whether a worker reporting no scheme activated it goes on too, as
        // its history may carry that worker's ids.
        new_execution.step_id_scheme = execution.step_id_scheme;
        new_execution.history_lineage = Some(execution.lineage());
        new_execution.activated_without_scheme = execution.activated_without_scheme;
        // The copied history holds the step results only if the old run's
        // did; a run whose activations rebuilt them goes on rebuilding them.
        new_execution.step_results_journaled = execution.step_results_journaled;
        // A new run, held to the same run and task timeouts; its run clock
        // starts with it.
        new_execution.run_timeout_secs = execution.run_timeout_secs;
        new_execution.task_timeout_secs = execution.task_timeout_secs;
        // A reset is an operator restarting the workflow, so it starts a new
        // execution deadline: the execution timeout the workflow was started
        // with, measured from the reset run's start. Resetting a workflow that
        // timed out is how an operator recovers it, and a reset run held to
        // the old, passed deadline could never run. The reset run's retries
        // and restart-fresh runs carry this new deadline, as a start's carry
        // its own.
        new_execution.timeout_secs = execution.timeout_secs;
        new_execution.execution_deadline = execution.timeout_secs.map(|secs| {
            new_execution.submitted_at + std::time::Duration::from_secs(secs.max(0) as u64)
        });
        // The reset run carries the workflow on: it belongs to the same
        // organization, and takes over the workflow id's hold — the old run
        // went terminal above, so nothing else holds it.
        new_execution.organization_id = execution.organization_id;
        new_execution.holds_workflow_id = execution.holds_workflow_id;
        if let (true, Some(wf_id)) = (execution.holds_workflow_id, &execution.workflow_id) {
            new_execution.id = orcher_common::types::identifiers::ExecutionId::from_uuid(
                self.workflow_execution_repository.id_beside_workflow_id(
                    execution.organization_id,
                    &execution.namespace,
                    wf_id,
                    |_| uuid::Uuid::new_v4(),
                ),
            );
        }

        // 3. Seed the new execution's journal BEFORE it becomes dispatchable.
        //
        // Creating the row with its outbox task publishes the run, and a worker
        // can claim it immediately. If the copy ran afterwards it would race the
        // replay: the worker starts from an empty journal and writes event 1,
        // then the copy tries to insert its own event 1 and trips
        // uq_journal_event_id. Seeding first means the run is only ever visible
        // with the history it is supposed to replay.
        //
        // Source and destination need *different* keys: the new row carries its
        // own internal id, so copying under the old one would leave the replay
        // with nothing to read.
        let new_journal_key = new_execution.id.as_uuid().to_string();
        let copied = journal_repo
            .copy_journal_up_to(
                &journal_key,
                &execution.execution_id,
                &new_journal_key,
                &new_execution_id,
                req.target_event_id,
            )
            .await
            .map_err(|e| {
                error!(error = %e, "Failed to copy journal entries for reset");
                Status::internal(format!("Failed to copy journal: {}", e))
            })?;

        // 4. Create the new execution with an outbox task for dispatch
        let outbox_payload = serde_json::json!({
            "execution_id": new_execution.execution_id,
            "workflow_name": new_execution.workflow_name,
            "workflow_version": new_execution.workflow_version,
            "namespace": new_execution.namespace,
            "task_queue": new_execution.task_queue,
            "parameters": new_execution.parameters,
        });
        let idempotency_key = format!("reset_{}", new_execution.execution_id);

        self.workflow_execution_repository
            .create_with_outbox_task(
                &new_execution,
                outbox_payload,
                idempotency_key,
                &new_execution.task_queue,
            )
            .await
            .map_err(|e| {
                error!(error = %e, "Failed to create reset execution");
                Status::internal(format!("Failed to create reset execution: {}", e))
            })?;

        // A reset child goes on in the new run; keep it its parent's child.
        // Not fatal: the new run exists either way, and the child
        // reconciliation sweep makes the same move when it finds the row
        // still naming the reset run.
        if let Some(ref children) = self.child_repository {
            if let Err(e) = children
                .repoint_child_execution(&execution.execution_id, &new_execution_id)
                .await
            {
                warn!(
                    error = %e,
                    old_execution_id = %execution.execution_id,
                    "Could not move child tracking to the reset run; the sweep will"
                );
            }
        }

        // 5. Append WorkflowExecutionReset entry to the OLD execution journal
        let reset_attrs = serde_json::json!({
            "new_execution_id": new_execution_id,
            "target_event_id": req.target_event_id,
            "reason": req.reason,
        });

        let reset_entry = orcher_storage::schema::types::journal::JournalEntry::new(
            journal_key.clone(),
            execution.execution_id.clone(),
            0,
            "WorkflowExecutionReset".to_string(),
            reset_attrs,
        );

        journal_repo.append_next(reset_entry).await.map_err(|e| {
            warn!(error = %e, "Failed to append reset journal entry (non-fatal)");
            Status::internal(format!("Failed to append reset journal entry: {}", e))
        })?;

        // 6. Notify pollers that work is available
        if let Some(ref polling_manager) = self.polling_manager {
            polling_manager
                .notify_work_available(&new_execution.task_queue, &new_execution.namespace)
                .await;
        }

        info!(
            old_execution_id = %execution.execution_id,
            new_execution_id = %new_execution_id,
            target_event_id = req.target_event_id,
            journal_entries_copied = copied,
            reason = %req.reason,
            "Workflow reset successfully"
        );

        Ok(Response::new(ResetWorkflowResponse {
            new_execution_id,
            old_execution_id: execution.execution_id,
        }))
    }

    /// Send an event to a running workflow
    ///
    /// The event is journaled in the run, once for the request id, and the run
    /// is handed out again to read it (see [`EventDelivery`]). An event sent
    /// before the run waits for it is read when it does.
    ///
    /// [`EventDelivery`]: crate::workflows::event_delivery::EventDelivery
    #[instrument(skip(self, request), fields(
        workflow_id = %request.get_ref().workflow_id,
        execution_id = %request.get_ref().execution_id,
        namespace = %request.get_ref().namespace,
        event_name = %request.get_ref().event_name
    ))]
    async fn send_event(
        &self,
        request: Request<SendEventRequest>,
    ) -> Result<Response<SendEventResponse>, Status> {
        let organization_id = crate::grpc::tenant::organization_from_metadata(request.metadata());
        let req = request.into_inner();

        debug!(
            workflow_id = %req.workflow_id,
            execution_id = %req.execution_id,
            namespace = %req.namespace,
            event_name = %req.event_name,
            "Received SendEvent request"
        );

        // Validate required fields
        if req.workflow_id.is_empty() {
            return Err(Status::invalid_argument("workflow_id is required"));
        }
        if req.event_name.is_empty() {
            return Err(Status::invalid_argument("event_name is required"));
        }
        check_payload(
            &self.payload_limits,
            &format!("the payload of event {:?}", req.event_name),
            req.payload.len(),
        )?;

        let delivery = self.event_delivery()?;

        // The target run, if the caller may see it.
        let execution = find_named_run(
            &self.workflow_execution_repository,
            organization_id,
            &req.namespace,
            &req.workflow_id,
            &req.execution_id,
        )
        .await?
        .ok_or_else(|| {
            warn!(workflow_id = %req.workflow_id, "Target workflow not found for event");
            Status::not_found(format!("Workflow not found: {}", req.workflow_id))
        })?;

        // Verify workflow is still running
        if execution.status != "running"
            && execution.status != "pending"
            && execution.status != "waiting"
        {
            warn!(
                execution_id = %execution.execution_id,
                status = %execution.status,
                "Cannot send event to non-running workflow"
            );
            return Err(Status::failed_precondition(format!(
                "Workflow is not running (status: {})",
                execution.status
            )));
        }

        let appended = delivery
            .deliver(&execution, &req.event_name, &req.payload, &req.request_id)
            .await
            .map_err(|e| {
                error!(error = %e, execution_id = %execution.execution_id, "Event delivery failed");
                Status::internal(format!("Failed to deliver event: {e}"))
            })?;

        info!(
            workflow_id = %req.workflow_id,
            execution_id = %execution.execution_id,
            event_name = %req.event_name,
            request_id = %req.request_id,
            repeat = !appended,
            "Event delivered"
        );

        Ok(Response::new(SendEventResponse { recorded: true }))
    }

    /// Send an event to a workflow, starting it if it doesn't exist
    ///
    /// Delivers the event to the workflow's open run, or, when it has none,
    /// starts one and delivers the event to it. A request id makes a repeat
    /// find the run the first request started and deliver nothing new.
    #[instrument(skip(self, request), fields(
        workflow_id = %request.get_ref().workflow_id,
        namespace = %request.get_ref().namespace,
        event_name = %request.get_ref().event_name,
        workflow_type = %request.get_ref().workflow_type
    ))]
    async fn send_event_with_start_workflow(
        &self,
        request: Request<SendEventWithStartWorkflowRequest>,
    ) -> Result<Response<SendEventWithStartWorkflowResponse>, Status> {
        // The calling tenant, read before the request is consumed.
        let organization_id = crate::grpc::tenant::organization_from_metadata(request.metadata());
        let req = request.into_inner();

        debug!(
            workflow_id = %req.workflow_id,
            namespace = %req.namespace,
            event_name = %req.event_name,
            workflow_type = %req.workflow_type,
            "Received SendEventWithStartWorkflow request"
        );

        // Validate required fields
        if req.workflow_id.is_empty() {
            return Err(Status::invalid_argument("workflow_id is required"));
        }
        if req.event_name.is_empty() {
            return Err(Status::invalid_argument("event_name is required"));
        }
        check_payload(
            &self.payload_limits,
            &format!("the payload of event {:?}", req.event_name),
            req.event_payload.len(),
        )?;
        check_payload(
            &self.payload_limits,
            "workflow input",
            req.workflow_input.len(),
        )?;

        // Validate namespace exists and is active
        let namespace = if req.namespace.is_empty() {
            "default"
        } else {
            &req.namespace
        };
        self.validate_namespace(namespace, organization_id.as_ref())
            .await?;

        let delivery = self.event_delivery()?;

        let is_active =
            |s: &str| matches!(s, "running" | "pending" | "waiting" | "queued" | "claimed");

        // Two event-with-starts of one id can both find nothing and both start
        // it; the workflow id's hold lets one through, and the other comes
        // round again to deliver its event to the run that won.
        const PASSES: usize = 3;
        let mut pass = 0;
        let (execution_id, workflow_id_out, workflow_started) = loop {
            pass += 1;

            // A request repeating one that started the workflow is answered
            // as that one was: with the run it started, its event seeded
            // into it once. The run need not be the one holding the id now;
            // it is the one this request made.
            if !req.request_id.is_empty() {
                let earlier = self
                    .workflow_execution_repository
                    .find_by_start_request(organization_id, namespace, &req.request_id)
                    .await
                    .map_err(|e| {
                        error!(error = %e, "Failed to look up the start request");
                        Status::internal(format!("Failed to query workflow: {}", e))
                    })?;
                if let Some(earlier) = earlier {
                    if earlier.workflow_id.as_deref() != Some(req.workflow_id.as_str()) {
                        return Err(request_id_in_use(&earlier));
                    }
                    // The first request may have stopped between starting the
                    // run and seeding its event. Seeded into a run that has
                    // ended, it would never be read.
                    if earlier.is_active() {
                        delivery
                            .deliver(&earlier, &req.event_name, &req.event_payload, &req.request_id)
                            .await
                            .map_err(|e| {
                                error!(error = %e, execution_id = %earlier.execution_id, "Event delivery failed");
                                Status::internal(format!("Failed to deliver event: {e}"))
                            })?;
                    }
                    break (earlier.execution_id.clone(), req.workflow_id.clone(), true);
                }
            }

            // Look for an existing active execution of this workflow_id — the
            // caller's own, in this namespace. Looked up by the id alone, this
            // found another organization's workflow of the same id and delivered
            // the event into its journal. The run holding the id is the one; a
            // run from before runs held their ids is found as the latest.
            let lookup_failed = |e: orcher_common::error::OrcherError| {
                error!(error = %e, "Failed to query workflow for event-with-start");
                Status::internal(format!("Failed to query workflow: {}", e))
            };
            // Not find_run: this is a start keyed on the workflow id, so the
            // run it signals is the one holding the id, as StartWorkflow's id
            // reuse decides; a run id in this field would otherwise be
            // signalled where the caller asked for a start under that id.
            let existing = match self
                .workflow_execution_repository
                .open_run_holding(organization_id, namespace, &req.workflow_id)
                .await
                .map_err(lookup_failed)?
            {
                Some(holder) => Some(holder),
                None => self
                    .workflow_execution_repository
                    .find_latest_owned(organization_id, namespace, &req.workflow_id)
                    .await
                    .map_err(lookup_failed)?,
            };

            if let Some(ref exec) = existing.as_ref().filter(|e| is_active(e.status.as_str())) {
                // --- Path A: workflow is alive — deliver the event, once for
                // the request id, as SendEvent does ---
                let appended = delivery
                    .deliver(exec, &req.event_name, &req.event_payload, &req.request_id)
                    .await
                    .map_err(|e| {
                        error!(error = %e, execution_id = %exec.execution_id, "Event delivery failed");
                        Status::internal(format!("Failed to deliver event: {e}"))
                    })?;

                info!(
                    workflow_id = %req.workflow_id,
                    execution_id = %exec.execution_id,
                    event_name = %req.event_name,
                    repeat = !appended,
                    "Event delivered to existing workflow"
                );

                break (exec.execution_id.clone(), req.workflow_id.clone(), false);
            } else {
                // --- Path B: no active workflow — start one, then pre-seed the event ---
                if req.workflow_type.is_empty() {
                    return Err(Status::invalid_argument(
                        "workflow_type is required when no active workflow exists",
                    ));
                }
                if req.task_queue.is_empty() {
                    return Err(Status::invalid_argument(
                        "task_queue is required when no active workflow exists",
                    ));
                }

                // The start, read as StartWorkflow reads one.
                let StartRequest { start, .. } =
                    read_start_request(start_request_of_event(&req), &self.payload_limits)?;

                let new_exec = self
                    .workflow_execution_service
                    .start_workflow(
                        start.run,
                        None,
                        // The tenant this request declared, stamped on the record.
                        organization_id,
                    )
                    .await;
                let new_exec = match new_exec {
                    Ok(new_exec) => new_exec,
                    // A repeat of this request got there first; the next
                    // pass answers with its run.
                    Err(e) if is_start_request_held(&e) && pass < PASSES => continue,
                    Err(e) if is_workflow_id_held(&e) => {
                        // Another run holds the id. Running, it is where the event
                        // goes, next pass; open but not running (a failure waiting
                        // for its retry, say), it has no place to take one, and the
                        // id cannot be started either.
                        if pass < PASSES {
                            continue;
                        }
                        let holder = self
                            .workflow_execution_repository
                            .open_run_holding(organization_id, namespace, &req.workflow_id)
                            .await
                            .map_err(lookup_failed)?;
                        return Err(match holder {
                            Some(holder) => workflow_id_in_use(&req.workflow_id, &holder, true),
                            None => Status::aborted(
                                "the workflow id changed hands while the event was being \
                             delivered; try again",
                            ),
                        });
                    }
                    Err(e) => {
                        error!(error = %e, "Failed to start workflow for event-with-start");
                        return Err(Status::internal(format!("Failed to start workflow: {}", e)));
                    }
                };

                // Deliver the event to the new run: once for the request,
                // when it gives an id, so a repeat of it finding the run does
                // not deliver it again. Delivered, not only journaled: a worker
                // may already have claimed the run, and its first activation
                // must be followed by one that reads the event.
                delivery
                    .deliver(&new_exec, &req.event_name, &req.event_payload, &req.request_id)
                    .await
                    .map_err(|e| {
                        error!(error = %e, execution_id = %new_exec.execution_id, "Event delivery failed");
                        Status::internal(format!("Failed to deliver event: {e}"))
                    })?;

                info!(
                    workflow_id = %req.workflow_id,
                    execution_id = %new_exec.execution_id,
                    workflow_type = %req.workflow_type,
                    event_name = %req.event_name,
                    "Workflow started and event pre-seeded"
                );

                break (new_exec.execution_id.clone(), req.workflow_id.clone(), true);
            }
        };

        let now = Timestamptz::now();
        Ok(Response::new(SendEventWithStartWorkflowResponse {
            workflow_id: workflow_id_out,
            execution_id,
            workflow_started,
            timestamp: Some(prost_types::Timestamp {
                seconds: now.timestamp(),
                nanos: now.timestamp_subsec_nanos() as i32,
            }),
        }))
    }

    /// Query a workflow's current state
    ///
    /// Queries the current state of a running workflow by invoking a query handler.
    ///
    /// NOTE: This is a placeholder implementation. Full query support requires:
    /// - Query handler registration in workflows
    /// - Query dispatch to running workflow execution
    /// - Synchronous response mechanism
    #[instrument(skip(self, request), fields(
        workflow_id = %request.get_ref().workflow_id,
        execution_id = %request.get_ref().execution_id,
        namespace = %request.get_ref().namespace,
        query_type = %request.get_ref().query_type
    ))]
    async fn query_workflow(
        &self,
        request: Request<QueryWorkflowRequest>,
    ) -> Result<Response<QueryWorkflowResponse>, Status> {
        // Before `into_inner()`, which drops the metadata the tenant lives in.
        let caller = crate::grpc::tenant::organization_from_metadata(request.metadata());
        let req = request.into_inner();

        debug!(
            workflow_id = %req.workflow_id,
            execution_id = %req.execution_id,
            namespace = %req.namespace,
            query_type = %req.query_type,
            "Received QueryWorkflow request"
        );

        // Validate required fields
        if req.workflow_id.is_empty() {
            return Err(Status::invalid_argument("workflow_id is required"));
        }
        if req.query_type.is_empty() {
            return Err(Status::invalid_argument("query_type is required"));
        }

        // The target run, if the caller may see it. A query runs a handler
        // inside a live workflow and perturbs its scheduling (the dispatch path
        // releases the claim to force a re-poll), so it must never reach
        // another organisation's run.
        let execution = find_named_run(
            &self.workflow_execution_repository,
            caller,
            &req.namespace,
            &req.workflow_id,
            &req.execution_id,
        )
        .await?
        .ok_or_else(|| {
            warn!(workflow_id = %req.workflow_id, "Target workflow not found for query");
            Status::not_found(format!("Workflow not found: {}", req.workflow_id))
        })?;

        // Verify workflow is in a queryable state (running, pending, or waiting)
        if execution.status != "running"
            && execution.status != "pending"
            && execution.status != "waiting"
        {
            return Err(Status::failed_precondition(format!(
                "Workflow is not running (status: {})",
                execution.status
            )));
        }

        // Generate unique query ID
        let query_id = uuid::Uuid::new_v4().to_string();

        // Create pending query entry
        let notify = Arc::new(Notify::new());
        let result: Arc<Mutex<Option<orcher_proto::orcher::v1::QueryResult>>> =
            Arc::new(Mutex::new(None));

        let pending = PendingQuery {
            request: QueryRequest {
                query_id: query_id.clone(),
                query_type: req.query_type.clone(),
                query_args: req.query_args.clone(),
            },
            execution_id: execution.execution_id.clone(),
            notify: notify.clone(),
            result: result.clone(),
        };

        // Register in shared pending query map
        self.pending_queries.insert(query_id.clone(), pending);

        debug!(
            query_id = %query_id,
            query_type = %req.query_type,
            execution_id = %execution.execution_id,
            "Query registered, triggering re-poll"
        );

        // Trigger a re-poll so the worker picks up the query
        // Release claim so the workflow can be polled again
        let _ = self
            .workflow_execution_repository
            .release_claim_to_resume(&execution.execution_id)
            .await;

        if let Some(ref pm) = self.polling_manager {
            pm.notify_work_available(&execution.task_queue, &execution.namespace)
                .await;
        }

        // Wait for the worker to respond (with timeout)
        let timeout = tokio::time::Duration::from_secs(DEFAULT_QUERY_TIMEOUT_SECS);
        let answered = tokio::time::timeout(timeout, notify.notified()).await;

        // Clean up the pending query entry regardless of outcome
        self.pending_queries.remove(&query_id);

        if answered.is_err() {
            warn!(
                query_id = %query_id,
                query_type = %req.query_type,
                "Query timed out waiting for worker response"
            );
            return Err(Status::deadline_exceeded(format!(
                "Query '{}' timed out after {}s — worker may not have a handler registered",
                req.query_type, DEFAULT_QUERY_TIMEOUT_SECS
            )));
        }

        // Extract the result
        let query_result = result
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| Status::internal("Query was notified but no result found"))?;

        // Check if the query handler returned an error
        match query_result.result {
            Some(orcher_proto::orcher::v1::query_result::Result::Answer(answer)) => {
                info!(
                    query_id = %query_id,
                    query_type = %req.query_type,
                    result_size = answer.len(),
                    "Query answered successfully"
                );
                Ok(Response::new(QueryWorkflowResponse { result: answer }))
            }
            Some(orcher_proto::orcher::v1::query_result::Result::ErrorMessage(msg)) => {
                warn!(
                    query_id = %query_id,
                    query_type = %req.query_type,
                    error = %msg,
                    "Query handler returned error"
                );
                Err(Status::internal(format!("Query handler error: {}", msg)))
            }
            None => Err(Status::internal("Query result was empty")),
        }
    }

    /// Update a running workflow (synchronous mutation)
    ///
    /// Sends an update to a running workflow that can read and modify state.
    /// Unlike queries (read-only), updates are journaled for replay durability.
    #[instrument(skip(self, request), fields(
        workflow_id = %request.get_ref().workflow_id,
        update_type = %request.get_ref().update_type
    ))]
    async fn update_workflow(
        &self,
        request: Request<UpdateWorkflowRequest>,
    ) -> Result<Response<UpdateWorkflowResponse>, Status> {
        let organization_id = crate::grpc::tenant::organization_from_metadata(request.metadata());
        let req = request.into_inner();

        debug!(
            workflow_id = %req.workflow_id,
            execution_id = %req.execution_id,
            namespace = %req.namespace,
            update_type = %req.update_type,
            "Received UpdateWorkflow request"
        );

        // Validate required fields
        if req.workflow_id.is_empty() {
            return Err(Status::invalid_argument("workflow_id is required"));
        }
        if req.update_type.is_empty() {
            return Err(Status::invalid_argument("update_type is required"));
        }
        check_payload(
            &self.payload_limits,
            &format!("the arguments of update {:?}", req.update_type),
            req.args.len(),
        )?;

        // The target run, if the caller may see it. An update mutates the
        // target's state through its journal.
        let execution = find_named_run(
            &self.workflow_execution_repository,
            organization_id,
            &req.namespace,
            &req.workflow_id,
            &req.execution_id,
        )
        .await?
        .ok_or_else(|| {
            warn!(workflow_id = %req.workflow_id, "Target workflow not found for update");
            Status::not_found(format!("Workflow not found: {}", req.workflow_id))
        })?;

        // Verify workflow is in a mutable state (running, pending, or waiting)
        if execution.status != "running"
            && execution.status != "pending"
            && execution.status != "waiting"
        {
            return Err(Status::failed_precondition(format!(
                "Workflow is not running (status: {})",
                execution.status
            )));
        }

        // Generate update_id (use client-provided for idempotency, or generate)
        let update_id = if req.update_id.is_empty() {
            uuid::Uuid::new_v4().to_string()
        } else {
            req.update_id.clone()
        };

        // Create pending update entry
        let notify = Arc::new(Notify::new());
        let result: Arc<Mutex<Option<orcher_proto::orcher::v1::UpdateResult>>> =
            Arc::new(Mutex::new(None));

        let pending = PendingUpdate {
            request: orcher_proto::orcher::v1::UpdateRequest {
                update_id: update_id.clone(),
                update_type: req.update_type.clone(),
                args: req.args.clone(),
            },
            execution_id: execution.execution_id.clone(),
            workflow_id: execution.id.to_string(),
            notify: notify.clone(),
            result: result.clone(),
        };

        // Register in shared pending update map
        self.pending_updates.insert(update_id.clone(), pending);

        debug!(
            update_id = %update_id,
            update_type = %req.update_type,
            execution_id = %execution.execution_id,
            "Update registered, triggering re-poll"
        );

        // Trigger a re-poll so the worker picks up the update
        let _ = self
            .workflow_execution_repository
            .release_claim_to_resume(&execution.execution_id)
            .await;

        if let Some(ref pm) = self.polling_manager {
            pm.notify_work_available(&execution.task_queue, &execution.namespace)
                .await;
        }

        // Wait for the worker to respond (with timeout)
        let timeout = tokio::time::Duration::from_secs(DEFAULT_UPDATE_TIMEOUT_SECS);
        let answered = tokio::time::timeout(timeout, notify.notified()).await;

        // Clean up the pending update entry regardless of outcome
        self.pending_updates.remove(&update_id);

        if answered.is_err() {
            warn!(
                update_id = %update_id,
                update_type = %req.update_type,
                "Update timed out waiting for worker response"
            );
            return Err(Status::deadline_exceeded(format!(
                "Update '{}' timed out after {}s — worker may not have an update handler registered",
                req.update_type, DEFAULT_UPDATE_TIMEOUT_SECS
            )));
        }

        // Extract the result
        let update_result = result
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| Status::internal("Update was notified but no result found"))?;

        // Map to response
        match update_result.result {
            Some(orcher_proto::orcher::v1::update_result::Result::Answer(answer)) => {
                info!(
                    update_id = %update_id,
                    update_type = %req.update_type,
                    result_size = answer.len(),
                    "Update completed successfully"
                );
                Ok(Response::new(UpdateWorkflowResponse {
                    update_id,
                    outcome: Some(
                        orcher_proto::orcher::v1::update_workflow_response::Outcome::Result(answer),
                    ),
                }))
            }
            Some(orcher_proto::orcher::v1::update_result::Result::ErrorMessage(msg)) => {
                warn!(
                    update_id = %update_id,
                    update_type = %req.update_type,
                    error = %msg,
                    "Update handler returned error"
                );
                Ok(Response::new(UpdateWorkflowResponse {
                    update_id,
                    outcome: Some(
                        orcher_proto::orcher::v1::update_workflow_response::Outcome::Failure(
                            orcher_proto::orcher::v1::Failure {
                                message: msg,
                                ..Default::default()
                            },
                        ),
                    ),
                }))
            }
            None => Err(Status::internal("Update result was empty")),
        }
    }

    async fn batch_cancel_workflow(
        &self,
        request: Request<BatchCancelWorkflowRequest>,
    ) -> Result<Response<BatchCancelWorkflowResponse>, Status> {
        // Read the caller before the metadata is dropped: a batch acts on a
        // namespace by name, and names are unique per organisation rather
        // than per deployment.
        let organization_id = crate::grpc::tenant::organization_from_metadata(request.metadata());
        let req = request.into_inner();
        let filter = req
            .filter
            .ok_or_else(|| Status::invalid_argument("filter is required"))?;

        if filter.namespace.is_empty() {
            return Err(Status::invalid_argument("filter.namespace is required"));
        }

        info!(
            namespace = %filter.namespace,
            reason = %req.reason,
            "Received BatchCancelWorkflow request"
        );

        let filter_status = if filter.status.is_empty() {
            None
        } else {
            Some(filter.status.as_str())
        };
        let workflow_name = if filter.workflow_type.is_empty() {
            None
        } else {
            Some(filter.workflow_type.as_str())
        };
        let search_attrs = if filter.labels.is_empty() {
            None
        } else {
            Some(serde_json::to_value(&filter.labels).unwrap_or_default())
        };
        let started_after = filter
            .started_after
            .map(|t| Timestamptz::from_second(t.seconds).unwrap_or_default());
        let started_before = filter
            .started_before
            .map(|t| Timestamptz::from_second(t.seconds).unwrap_or_default());
        let reason = if req.reason.is_empty() {
            "Cancellation requested".to_string()
        } else {
            req.reason.clone()
        };

        // Each match goes through the one path every cancellation takes, so a
        // workflow whose code has run is asked and may clean up, as it would be
        // if cancelled on its own. At most CANCEL_BATCH_CONCURRENCY at a time:
        // the requests wake their workflows, and a large batch must not wake
        // all of them at once.
        const CANCEL_BATCH_CONCURRENCY: usize = 16;
        use futures::StreamExt;
        let mut total_succeeded: i64 = 0;
        let mut total_skipped: i64 = 0;
        let mut errors: Vec<BatchOperationError> = Vec::new();
        loop {
            let execution_ids = self
                .workflow_execution_repository
                .select_for_batch_cancel(
                    &filter.namespace,
                    organization_id,
                    filter_status,
                    workflow_name,
                    search_attrs.as_ref(),
                    started_after,
                    started_before,
                    500,
                )
                .await
                .map_err(|e| {
                    error!(error = %e, "Failed to batch cancel workflows");
                    Status::internal(format!("Batch cancel failed: {}", e))
                })?;
            if execution_ids.is_empty() {
                break;
            }

            let service = &self.workflow_execution_service;
            let reason = reason.as_str();
            let outcomes: Vec<(String, orcher_common::OrcherResult<CancelOutcome>)> =
                futures::stream::iter(execution_ids)
                    .map(|execution_id| async move {
                        let outcome = service.cancel_execution(&execution_id, reason, None).await;
                        (execution_id, outcome)
                    })
                    .buffer_unordered(CANCEL_BATCH_CONCURRENCY)
                    .collect()
                    .await;

            let mut progressed = false;
            for (execution_id, outcome) in outcomes {
                match outcome {
                    Ok(CancelOutcome::Requested | CancelOutcome::Cancelled) => {
                        total_succeeded += 1;
                        progressed = true;
                    }
                    Ok(CancelOutcome::AlreadyRequested | CancelOutcome::NotLive) => {
                        total_skipped += 1;
                        progressed = true;
                    }
                    Err(e) => errors.push(BatchOperationError {
                        execution_id,
                        error: e.to_string(),
                    }),
                }
            }
            // A run that failed stays unasked and would be selected again; a
            // round that moved nothing would repeat forever.
            if !progressed {
                break;
            }
        }

        info!(
            namespace = %filter.namespace,
            total_succeeded = total_succeeded,
            total_failed = errors.len(),
            "Batch cancel complete"
        );

        Ok(Response::new(BatchCancelWorkflowResponse {
            result: Some(BatchOperationResult {
                total_matched: total_succeeded + total_skipped + errors.len() as i64,
                total_succeeded,
                total_failed: errors.len() as i64,
                total_skipped,
                errors,
            }),
        }))
    }

    async fn batch_terminate_workflow(
        &self,
        request: Request<BatchTerminateWorkflowRequest>,
    ) -> Result<Response<BatchTerminateWorkflowResponse>, Status> {
        // Read the caller before the metadata is dropped: a batch acts on a
        // namespace by name, and names are unique per organisation rather
        // than per deployment.
        let organization_id = crate::grpc::tenant::organization_from_metadata(request.metadata());
        let req = request.into_inner();
        let filter = req
            .filter
            .ok_or_else(|| Status::invalid_argument("filter is required"))?;

        if filter.namespace.is_empty() {
            return Err(Status::invalid_argument("filter.namespace is required"));
        }

        info!(
            namespace = %filter.namespace,
            reason = %req.reason,
            "Received BatchTerminateWorkflow request"
        );

        let filter_status = if filter.status.is_empty() {
            None
        } else {
            Some(filter.status.as_str())
        };
        let workflow_name = if filter.workflow_type.is_empty() {
            None
        } else {
            Some(filter.workflow_type.as_str())
        };
        let search_attrs = if filter.labels.is_empty() {
            None
        } else {
            Some(serde_json::to_value(&filter.labels).unwrap_or_default())
        };
        let started_after = filter
            .started_after
            .map(|t| Timestamptz::from_second(t.seconds).unwrap_or_default());
        let started_before = filter
            .started_before
            .map(|t| Timestamptz::from_second(t.seconds).unwrap_or_default());
        let now = Timestamptz::now();

        let mut total_succeeded: i64 = 0;
        loop {
            let updated = self
                .workflow_execution_repository
                .batch_update_status(
                    &filter.namespace,
                    organization_id,
                    filter_status,
                    workflow_name,
                    search_attrs.as_ref(),
                    started_after,
                    started_before,
                    "terminated",
                    Some(now),
                    500,
                )
                .await
                .map_err(|e| {
                    error!(error = %e, "Failed to batch terminate workflows");
                    Status::internal(format!("Batch terminate failed: {}", e))
                })?;

            let batch_count = updated.len() as i64;
            total_succeeded += batch_count;

            if batch_count == 0 {
                break;
            }
        }

        info!(
            namespace = %filter.namespace,
            total_succeeded = total_succeeded,
            "Batch terminate complete"
        );

        Ok(Response::new(BatchTerminateWorkflowResponse {
            result: Some(BatchOperationResult {
                total_matched: total_succeeded,
                total_succeeded,
                total_failed: 0,
                total_skipped: 0,
                errors: vec![],
            }),
        }))
    }

    async fn batch_send_event(
        &self,
        request: Request<BatchSendEventRequest>,
    ) -> Result<Response<BatchSendEventResponse>, Status> {
        // Read the caller before the metadata is dropped: a batch acts on a
        // namespace by name, and names are unique per organisation rather
        // than per deployment.
        let organization_id = crate::grpc::tenant::organization_from_metadata(request.metadata());
        let req = request.into_inner();
        let filter = req
            .filter
            .ok_or_else(|| Status::invalid_argument("filter is required"))?;

        if filter.namespace.is_empty() {
            return Err(Status::invalid_argument("filter.namespace is required"));
        }
        if req.event_name.is_empty() {
            return Err(Status::invalid_argument("event_name is required"));
        }
        check_payload(
            &self.payload_limits,
            &format!("the payload of event {:?}", req.event_name),
            req.payload.len(),
        )?;

        let delivery = self.event_delivery()?;

        info!(
            namespace = %filter.namespace,
            event_name = %req.event_name,
            "Received BatchSendEvent request"
        );

        let filter_status = if filter.status.is_empty() {
            None
        } else {
            Some(filter.status.as_str())
        };
        let workflow_name = if filter.workflow_type.is_empty() {
            None
        } else {
            Some(filter.workflow_type.as_str())
        };
        let search_attrs = if filter.labels.is_empty() {
            None
        } else {
            Some(serde_json::to_value(&filter.labels).unwrap_or_default())
        };
        let started_after = filter
            .started_after
            .map(|t| Timestamptz::from_second(t.seconds).unwrap_or_default());
        let started_before = filter
            .started_before
            .map(|t| Timestamptz::from_second(t.seconds).unwrap_or_default());

        let mut total_succeeded: i64 = 0;
        let mut total_failed: i64 = 0;
        let mut errors: Vec<BatchOperationError> = Vec::new();
        let mut offset: i64 = 0;
        let page_size: i64 = 100;

        loop {
            let executions = self
                .workflow_execution_repository
                .list_by_batch_filter(
                    &filter.namespace,
                    organization_id,
                    filter_status,
                    workflow_name,
                    search_attrs.as_ref(),
                    started_after,
                    started_before,
                    page_size,
                    offset,
                )
                .await
                .map_err(|e| {
                    error!(error = %e, "Failed to list workflows for batch event");
                    Status::internal(format!("Failed to list workflows: {}", e))
                })?;

            if executions.is_empty() {
                break;
            }

            for execution in &executions {
                // Delivered to each run as SendEvent delivers one: journaled,
                // released, and its own task queue woken. The request has no
                // request id, so a retried batch delivers again.
                match delivery
                    .deliver(execution, &req.event_name, &req.payload, "")
                    .await
                {
                    Ok(_) => total_succeeded += 1,
                    Err(e) => {
                        total_failed += 1;
                        if errors.len() < 100 {
                            errors.push(BatchOperationError {
                                execution_id: execution.execution_id.clone(),
                                error: e.to_string(),
                            });
                        }
                    }
                }
            }

            offset += page_size;
        }

        let total_matched = total_succeeded + total_failed;

        info!(
            namespace = %filter.namespace,
            event_name = %req.event_name,
            total_matched = total_matched,
            total_succeeded = total_succeeded,
            total_failed = total_failed,
            "Batch send event complete"
        );

        Ok(Response::new(BatchSendEventResponse {
            result: Some(BatchOperationResult {
                total_matched,
                total_succeeded,
                total_failed,
                total_skipped: 0,
                errors,
            }),
        }))
    }

    async fn batch_start_workflow(
        &self,
        request: Request<orcher_proto::orcher::v1::BatchStartWorkflowRequest>,
    ) -> Result<Response<orcher_proto::orcher::v1::BatchStartWorkflowResponse>, Status> {
        // The calling tenant, read before the request is consumed.
        let organization_id = crate::grpc::tenant::organization_from_metadata(request.metadata());
        let req = request.into_inner();

        if req.workflows.is_empty() {
            return Err(Status::invalid_argument("workflows list is empty"));
        }

        // Each workflow read as StartWorkflow reads one, so a batch-started
        // run is stored as a single start would store it.
        let mut starts = Vec::with_capacity(req.workflows.len());
        for (i, wf) in req.workflows.into_iter().enumerate() {
            let in_workflow =
                |e: Status| Status::new(e.code(), format!("workflow[{i}]: {}", e.message()));
            let StartRequest {
                start,
                cron_expression,
                schedule_config: _,
            } = read_start_request(wf, &self.payload_limits).map_err(in_workflow)?;
            // Refused rather than started now: a batch starts workflows, and
            // starting one asked to run on a schedule ignores what it asked.
            if cron_expression.is_some() {
                return Err(in_workflow(Status::invalid_argument(
                    "a batch cannot schedule a workflow; start it with StartWorkflow",
                )));
            }
            starts.push(start);
        }

        // Every namespace the batch starts in, each checked as a single
        // start's is. Only the first workflow's used to be.
        let mut namespaces: Vec<&str> = starts.iter().map(|s| s.run.namespace.as_str()).collect();
        namespaces.sort_unstable();
        namespaces.dedup();
        for namespace in &namespaces {
            self.validate_namespace(namespace, organization_id.as_ref())
                .await?;
        }

        info!(
            count = starts.len(),
            namespaces = ?namespaces,
            "BatchStartWorkflow request"
        );
        let workflow_ids: Vec<String> = starts
            .iter()
            .map(|s| s.run.workflow_id.clone().unwrap_or_default())
            .collect();

        let outcome = self
            .workflow_execution_service
            .batch_start_workflows(starts, organization_id)
            .await
            .map_err(|e| {
                if is_workflow_id_held(&e) {
                    // The batch is all or nothing, and one of its ids was
                    // taken by a start racing it; nothing was started.
                    return Status::already_exists(
                        "A workflow id in the batch is already running; \
                         no workflow in the batch was started",
                    );
                }
                error!(error = %e, "BatchStartWorkflow failed");
                Status::internal(format!("Failed to batch start workflows: {}", e))
            })?;
        let created = match outcome {
            BatchStartOutcome::Started(created) => created,
            // Refused as a single start of the same workflow would be, naming
            // the run in the way; nothing in the batch was started.
            BatchStartOutcome::Refused {
                index,
                execution,
                open,
            } => {
                let refused = workflow_id_in_use(&workflow_ids[index], &execution, open);
                return Err(Status::with_metadata(
                    refused.code(),
                    format!(
                        "workflow[{index}]: {}; no workflow in the batch was started",
                        refused.message()
                    ),
                    refused.metadata().clone(),
                ));
            }
            BatchStartOutcome::Duplicate { index, first } => {
                return Err(Status::already_exists(format!(
                    "workflow[{index}] names the workflow id {} that workflow[{first}] does; \
                     no workflow in the batch was started",
                    workflow_ids[index]
                )));
            }
            BatchStartOutcome::RequestIdInUse { index, execution } => {
                let refused = request_id_in_use(&execution);
                return Err(Status::with_metadata(
                    refused.code(),
                    format!(
                        "workflow[{index}]: {}; no workflow in the batch was started",
                        refused.message()
                    ),
                    refused.metadata().clone(),
                ));
            }
            BatchStartOutcome::DuplicateRequestId { index, first } => {
                return Err(Status::invalid_argument(format!(
                    "workflow[{index}] gives the request_id that workflow[{first}] does; \
                     no workflow in the batch was started"
                )));
            }
        };

        // Each run as a single start answers with it: a batch used to report
        // when the run was written, not when it starts.
        let executions = created.iter().map(start_response).collect();

        Ok(Response::new(
            orcher_proto::orcher::v1::BatchStartWorkflowResponse { executions },
        ))
    }
}

/// The time by which a cancelled workflow must have ended, from the cleanup
/// limit its caller gave; `None` when it gave none.
fn cleanup_deadline(
    timeout: Option<&prost_types::Duration>,
) -> Result<Option<Timestamptz>, Status> {
    let Some(timeout) = timeout else {
        return Ok(None);
    };
    if timeout.seconds < 0 || timeout.nanos < 0 {
        return Err(Status::invalid_argument(
            "cleanup_timeout must not be negative",
        ));
    }
    let limit = jiff::SignedDuration::new(timeout.seconds, timeout.nanos);
    Timestamptz::now()
        .checked_add(limit)
        .map(Some)
        .ok_or_else(|| Status::invalid_argument("cleanup_timeout is too large"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A timed-out workflow is finished: a client waiting on its result is
    /// answered at once, with the status and the reason.
    #[test]
    fn a_timed_out_workflow_answers_a_result_wait() {
        let mut run = orcher_storage::schema::types::WorkflowExecution::builder()
            .execution_id("run-1".to_string())
            .workflow_name("checkout".to_string())
            .workflow_version("1.0".to_string())
            .namespace("default".to_string())
            .build();
        run.status = "timed_out".to_string();
        run.error = Some("Workflow exceeded its run timeout".to_string());
        // A retry policy does not hold back a timeout.
        run.max_retries = 3;

        let response = final_result_response(&run).expect("a timed-out run is finished");
        assert_eq!(response.status, ProtoWorkflowStatus::TimedOut as i32);
        assert_eq!(
            response.outcome,
            Some(
                orcher_proto::orcher::v1::get_workflow_result_response::Outcome::Error(
                    "Workflow exceeded its run timeout".to_string()
                )
            )
        );
    }

    #[test]
    fn no_cleanup_limit_means_no_deadline() {
        assert!(cleanup_deadline(None).unwrap().is_none());
    }

    #[test]
    fn a_cleanup_limit_sets_a_deadline_after_now() {
        let before = Timestamptz::now();
        let limit = prost_types::Duration {
            seconds: 30,
            nanos: 0,
        };
        let deadline = cleanup_deadline(Some(&limit)).unwrap().unwrap();
        assert!(deadline > before);
    }

    #[test]
    fn a_negative_cleanup_limit_is_refused() {
        let limit = prost_types::Duration {
            seconds: -1,
            nanos: 0,
        };
        let refused = cleanup_deadline(Some(&limit)).unwrap_err();
        assert_eq!(refused.code(), orcher_proto::tonic::Code::InvalidArgument);
    }

    fn run(
        status: &str,
        retry_count: i32,
        max_retries: i32,
    ) -> orcher_storage::schema::types::WorkflowExecution {
        let mut run = orcher_storage::schema::types::WorkflowExecution::builder()
            .execution_id("exec-1".to_string())
            .workflow_name("checkout".to_string())
            .workflow_version("1.0".to_string())
            .namespace("default".to_string())
            .build();
        run.status = status.to_string();
        run.retry_count = retry_count;
        run.max_retries = max_retries;
        run
    }

    fn failed(error: &str) -> CompletionOutcome {
        let mut run = run("failed", 0, 0);
        run.error = Some(error.to_string());
        CompletionOutcome::of_run(&run).expect("a failure with no retries is final")
    }

    /// A failure its policy will retry is not an outcome: a client waiting on
    /// the workflow's result must not be handed it.
    #[test]
    fn a_failure_with_a_retry_pending_is_no_outcome() {
        let mut pending = run("failed", 0, 1);
        pending.next_retry_at = Some(Timestamptz::now());
        assert!(CompletionOutcome::of_run(&pending).is_none());
        assert!(final_result_response(&pending).is_none());

        let used_up = run("failed", 1, 1);
        assert!(CompletionOutcome::of_run(&used_up).is_some());
        let mut settled = run("failed", 0, 1);
        settled.retries_settled_at = Some(Timestamptz::now());
        assert!(CompletionOutcome::of_run(&settled).is_some());
    }

    /// A completion that carried no result is still the run's outcome.
    #[test]
    fn a_resultless_completion_is_an_outcome() {
        let response = final_result_response(&run("completed", 0, 0)).expect("completed is final");
        assert_eq!(response.status, ProtoWorkflowStatus::Completed as i32);
        assert!(response.outcome.is_none());
    }

    #[test]
    fn a_cancellation_is_reported_as_one() {
        let outcome = CompletionOutcome::of_run(&run("cancelled", 0, 0)).unwrap();
        assert_eq!(outcome.status(), ProtoWorkflowStatus::Canceled);
    }

    #[tokio::test]
    async fn a_waiter_is_handed_the_outcome() {
        let notifier = WorkflowCompletionNotifier::default();
        let mut waiter = notifier.subscribe("exec-1");
        let mut second = notifier.subscribe("exec-1");
        assert!(notifier.has_waiters("exec-1"));

        notifier.notify("exec-1", failed("boom"));

        waiter.changed().await.unwrap();
        assert_eq!(
            waiter.borrow().as_ref().unwrap().response().outcome,
            Some(
                orcher_proto::orcher::v1::get_workflow_result_response::Outcome::Error(
                    "boom".to_string()
                )
            )
        );
        second.changed().await.unwrap();
        assert!(second.borrow().is_some());
    }

    #[test]
    fn an_outcome_nobody_waits_for_is_dropped() {
        let notifier = WorkflowCompletionNotifier::default();
        assert!(!notifier.has_waiters("exec-1"));
        notifier.notify("exec-1", failed("boom"));
        assert!(!notifier.has_waiters("exec-1"));

        notifier.subscribe("exec-1");
        notifier.remove("exec-1");
        assert!(!notifier.has_waiters("exec-1"));
    }

    #[test]
    fn test_status_to_proto() {
        assert_eq!(
            WorkflowService::status_to_proto("running"),
            ProtoWorkflowStatus::Running
        );
        assert_eq!(
            WorkflowService::status_to_proto("RUNNING"),
            ProtoWorkflowStatus::Running
        );
        assert_eq!(
            WorkflowService::status_to_proto("completed"),
            ProtoWorkflowStatus::Completed
        );
        assert_eq!(
            WorkflowService::status_to_proto("success"),
            ProtoWorkflowStatus::Completed
        );
        assert_eq!(
            WorkflowService::status_to_proto("failed"),
            ProtoWorkflowStatus::Failed
        );
        assert_eq!(
            WorkflowService::status_to_proto("canceled"),
            ProtoWorkflowStatus::Canceled
        );
        assert_eq!(
            WorkflowService::status_to_proto("terminated"),
            ProtoWorkflowStatus::Terminated
        );
        assert_eq!(
            WorkflowService::status_to_proto("restarted_fresh"),
            ProtoWorkflowStatus::RestartedFresh
        );
        assert_eq!(
            WorkflowService::status_to_proto("unknown"),
            ProtoWorkflowStatus::Unspecified
        );
        assert_eq!(
            WorkflowService::status_to_proto("reset"),
            ProtoWorkflowStatus::Reset
        );
    }

    #[test]
    fn test_reset_status_is_terminal() {
        // "reset" should map to a distinct proto status (not Unspecified)
        let status = WorkflowService::status_to_proto("reset");
        assert_ne!(status, ProtoWorkflowStatus::Unspecified);
        assert_ne!(status, ProtoWorkflowStatus::Running);
        assert_eq!(status, ProtoWorkflowStatus::Reset);
    }

    #[test]
    fn test_reset_workflow_request_validation() {
        // Verify that ResetWorkflowRequest fields are structured correctly
        let req = ResetWorkflowRequest {
            workflow_id: "wf-123".to_string(),
            execution_id: "exec-456".to_string(),
            namespace: "production".to_string(),
            target_event_id: 5,
            reason: "Bad deployment caused failures".to_string(),
        };

        assert_eq!(req.workflow_id, "wf-123");
        assert_eq!(req.target_event_id, 5);
        assert!(!req.reason.is_empty());

        // target_event_id must be positive
        let invalid_req = ResetWorkflowRequest {
            target_event_id: 0,
            ..req.clone()
        };
        assert!(invalid_req.target_event_id <= 0);

        // Empty workflow_id and execution_id should be rejected
        let empty_req = ResetWorkflowRequest {
            workflow_id: "".to_string(),
            execution_id: "".to_string(),
            ..req
        };
        assert!(empty_req.workflow_id.is_empty() && empty_req.execution_id.is_empty());
    }

    #[test]
    fn test_reset_workflow_response_structure() {
        let resp = ResetWorkflowResponse {
            new_execution_id: "new-exec-789".to_string(),
            old_execution_id: "old-exec-456".to_string(),
        };

        assert_eq!(resp.new_execution_id, "new-exec-789");
        assert_eq!(resp.old_execution_id, "old-exec-456");
        assert_ne!(resp.new_execution_id, resp.old_execution_id);
    }
}

#[cfg(test)]
mod execution_timeout_tests {
    use crate::workflows::execution::limit_secs;

    fn d(seconds: i64, nanos: i32) -> prost_types::Duration {
        prost_types::Duration { seconds, nanos }
    }

    #[test]
    fn absent_means_no_deadline() {
        assert_eq!(limit_secs(None), None);
    }

    #[test]
    fn whole_seconds_pass_through() {
        assert_eq!(limit_secs(Some(&d(1800, 0))), Some(1800));
    }

    #[test]
    fn sub_second_rounds_up_rather_than_vanishing() {
        // Truncating would store 0, and a 0 deadline is indistinguishable from
        // "no deadline" — the caller asked for a limit, so give them one.
        assert_eq!(limit_secs(Some(&d(0, 500_000_000))), Some(1));
    }

    #[test]
    fn fractional_seconds_round_up() {
        assert_eq!(limit_secs(Some(&d(30, 1))), Some(31));
    }

    #[test]
    fn an_explicit_zero_is_no_deadline() {
        // A zero-valued Duration is what an unset proto field decodes to when a
        // caller sets it deliberately to "none"; treat it as absent.
        assert_eq!(limit_secs(Some(&d(0, 0))), None);
    }

    #[test]
    fn negative_is_rejected_not_stored() {
        assert_eq!(limit_secs(Some(&d(-5, 0))), None);
    }
}
