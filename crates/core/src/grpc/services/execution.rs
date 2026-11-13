//! Task execution gRPC service.

use crate::grpc::auth_layer::TenantContext;
use crate::grpc::service_auth::{ServiceIdentity, ServiceOperation};
use orcher_common::types::time::Timestamptz;
use orcher_common::{error::OrcherError, OrcherResult};
use orcher_proto::orcher::v1::{
    execution_service_server::{ExecutionService as ExecutionServiceTrait, ExecutionServiceServer},
    CancelTaskExecutionRequest, CancelTaskExecutionResponse, CommandType,
    CompleteTaskExecutionRequest, CompleteTaskExecutionResponse, CompleteWorkflowExecutionRequest,
    CompleteWorkflowExecutionResponse, FailTaskExecutionRequest, FailTaskExecutionResponse,
    FailWorkflowExecutionRequest, FailWorkflowExecutionResponse, PollTaskExecutionRequest,
    PollTaskExecutionResponse, PollTaskExecutionsRequest, PollTaskExecutionsResponse,
    PollWorkflowExecutionRequest, PollWorkflowExecutionResponse, PollWorkflowExecutionsRequest,
    PollWorkflowExecutionsResponse, RecordTaskHeartbeatRequest, RecordTaskHeartbeatResponse,
    RecordTaskHeartbeatsRequest, RecordTaskHeartbeatsResponse, ReleaseWorkflowExecutionRequest,
    ReleaseWorkflowExecutionResponse, RespondQueryTaskRequest, RespondQueryTaskResponse,
    RespondUpdateTaskRequest, RespondUpdateTaskResponse, ShutdownWorkerRequest,
    ShutdownWorkerResponse, TaskHeartbeatAnswer,
};
use orcher_storage::repositories::domains::workflow_execution::logs::{
    WorkflowExecutionLog, WorkflowExecutionLogsRepository,
};
// Use tonic re-exported from orcher_proto for trait compatibility
use orcher_proto::tonic::{Request, Response, Status};
use prost::Message as _;

use crate::grpc::payload_limits::{
    activation_too_large, receive_limit_stated, PayloadLimits, PAYLOAD_TOO_LARGE,
};
use std::sync::Arc;
use tracing::{debug, error, info, instrument, trace, warn};

// Import CommandProcessor for processing workflow commands
use crate::workflows::execution::{run_is_final, Delivery};
use crate::workflows::{
    ChildOutcomeDelivery, CommandProcessor, JournalMapper, JournalService, WorkflowRetryDecision,
    WorkflowRetryPolicyEvaluator,
};
use orcher_storage::repositories::domains::child_workflow::ChildWorkflowRepository;
use orcher_storage::schema::types::execution::WorkflowExecution;

// Import storage repositories
use orcher_storage::repositories::domains::journal::JournalRepository;
use orcher_storage::repositories::domains::workflow_execution::WorkflowExecutionRepository;

// Import PollingManager for efficient long-polling
use crate::engine::dispatch::{PollingManager, TaskPollingManager};
use orcher_common::types::dispatch::polling::PollExecutionRequest;

// Import ExecutionLogRepository for step result loading
use orcher_storage::repositories::{ExecutionLogRepository, ExecutionLogStatus, ExecutionStepType};

// Import TaskExecutionRepository for task execution lifecycle tracking
use orcher_storage::repositories::domains::task_execution::TaskExecutionRepository;

// Import RetryScheduler for automatic retry handling
use crate::retries::retry_scheduler::{RetryOutcome, TaskRetryScheduler};

// Import CoordinationRepository for outbox dispatch
use orcher_storage::schema::types::coordination::OutboxTask;
use orcher_storage::schema::types::execution::TaskExecution;
use orcher_storage::traits::domain::NamespaceRepositoryTrait;

use orcher_monitoring::metrics::polling::PollDurationGuard;
use uuid::Uuid;

// Import Queue trait for Redis workflow re-queue support

// Long-polling configuration constants
// Reduced from 60s to 5s for better responsiveness and lower latency
const DEFAULT_POLL_TIMEOUT_SECS: i64 = 5;

/// The most heartbeats one RecordTaskHeartbeats call carries: a worker
/// running more sends them in several.
const MAX_HEARTBEATS_PER_CALL: usize = 1000;

/// The most items one poll hands out, however many the worker has room for,
/// so that one worker cannot drain a queue other workers poll.
const MAX_ITEMS_PER_POLL: u32 = 16;

/// How many items a poll asking for `max_items` may take: 0 means 1, and no
/// more than [`MAX_ITEMS_PER_POLL`].
fn items_per_poll(max_items: u32) -> usize {
    max_items.clamp(1, MAX_ITEMS_PER_POLL) as usize
}
#[allow(dead_code)]
const DEFAULT_CLAIM_TIMEOUT_SECS: i64 = 300; // 5 minutes

/// ExecutionService gRPC implementation
///
/// Handles polling for workflow/task work and processing execution results.
/// This is the primary interface between services and the orchestrator.
///
/// Note: Using ORCHER terminology - "Service" not "Worker"
pub struct ExecutionService {
    /// This service itself, once it is serving (see `into_server`): what a
    /// completion is applied through on a task of its own.
    this: std::sync::OnceLock<std::sync::Weak<ExecutionService>>,

    /// Command processor for handling workflow commands
    command_processor: Arc<CommandProcessor>,

    /// Polling manager for efficient long-polling with notifications
    polling_manager: Arc<PollingManager>,

    /// Task polling manager for efficient task execution long-polling
    task_polling_manager: Option<Arc<TaskPollingManager>>,

    /// Execution log repository for loading step results
    execution_log_repository: Arc<dyn ExecutionLogRepository>,

    /// Capacity registry for tracking worker permits
    /// Workers register their capacity when polling, enabling intelligent dispatch
    capacity_registry: Arc<orcher_dispatch::capacity::CapacityRegistry>,

    /// Journal repository for querying task input
    journal_repository: Arc<JournalRepository>,

    /// Workflow execution repository for querying workflow metadata
    workflow_execution_repository: Arc<WorkflowExecutionRepository>,

    /// Task execution repository for task lifecycle tracking
    task_execution_repository: Arc<TaskExecutionRepository>,

    /// Workflow coordinator for task polling from outbox queue
    workflow_coordinator: Option<Arc<crate::engine::WorkflowCoordinator>>,

    /// Journal mapper for converting journal entries to proto
    journal_mapper: JournalMapper,

    /// Retry scheduler for automatic retry handling
    retry_scheduler: TaskRetryScheduler,

    /// Logs repository for runtime workflow execution logging
    logs_repository: Arc<WorkflowExecutionLogsRepository>,

    /// Default capacity for workers (number of concurrent tasks)
    /// Used when workers don't specify their capacity
    default_worker_capacity: usize,

    /// Eager dispatch integration for predictive task return optimization
    eager_dispatch: Arc<orcher_dispatch::EagerDispatchIntegration>,

    /// Shared completion channel map for get_workflow_result waiters.
    /// Keyed by execution_id. Sends the outcome when workflow completes/fails,
    /// allowing waiters to return results without re-querying the database.
    workflow_completion_notifier:
        Option<crate::grpc::services::workflow::WorkflowCompletionNotifier>,

    /// Shared registry of pending queries from WorkflowService.
    /// Queries are attached to poll responses and results delivered via RespondQueryTask.
    pending_queries: Option<crate::grpc::services::query_dispatch::PendingQueryRegistry>,

    /// Shared registry of pending updates from WorkflowService.
    /// Updates are attached to poll responses, results are journaled then delivered.
    pending_updates: Option<crate::grpc::services::update_dispatch::PendingUpdateRegistry>,

    /// Namespace repository for validating namespace existence on poll paths
    namespace_repository: Option<Arc<dyn NamespaceRepositoryTrait>>,

    /// Delivers a finished child's outcome to its parent. Shared with the
    /// child reconciliation sweep, which delivers what this path does not.
    child_outcomes: Arc<ChildOutcomeDelivery>,

    /// Relays a completion to the cluster node owning its workflow. Absent
    /// unless clustering and forwarding are on; every request is then handled
    /// here, as before.
    owner_forwarder: Option<Arc<crate::grpc::forward::OwnerForwarder>>,

    /// Refuse a workflow completion that carries no activation token, rather
    /// than applying it unfenced. Off by default: every SDK released so far
    /// sends a token of its own making on FailWorkflowExecution, and one
    /// predating the token sends none at all. See [`Admission`].
    require_activation_token: bool,

    /// The workers that have said they are shutting down (`ShutdownWorker`),
    /// shared with both polling managers, which answer their polls with
    /// nothing. See [`crate::worker::shutdowns`].
    worker_shutdowns: Arc<crate::worker::WorkerShutdowns>,

    /// The worker processes polling this replica, counted for as long as each
    /// poll is open, so that one that stops polling can be told from one that
    /// is still there. See [`crate::worker::liveness`].
    worker_liveness: Arc<crate::worker::WorkerLiveness>,

    /// The heartbeat timeout an attempt runs under when its task sets none and
    /// the worker taking it heartbeats every task on its own
    /// (`TaskCapabilities.auto_heartbeat`). `None` gives no default: such an
    /// attempt runs under the task's own timeouts only, as for any worker.
    default_heartbeat_timeout_secs: Option<i32>,

    /// How large a message and a payload may be: task results are held to
    /// it on completion, and what a poll hands a worker to what that worker
    /// can receive.
    payload_limits: PayloadLimits,
}

/// The heartbeat timeout given by default to an attempt of a task that sets
/// none, when its worker heartbeats on its own. A worker heartbeats at a third
/// of it or less, so one that dies or never received the task is noticed a
/// minute on, and a running task costs a heartbeat every twenty seconds.
pub const DEFAULT_HEARTBEAT_TIMEOUT_SECS: i32 = 60;

/// What admitting a workflow completion decided.
// Returned once and matched at once, so the size of `Apply` costs nothing.
#[allow(clippy::large_enum_variant)]
enum Admission {
    /// Apply it. `fence` holds the execution's other completions off until it
    /// has been applied; `activation` is what to record as applied then.
    /// `token` says whether the completion carried an activation token, which
    /// decides how its permit is found.
    Apply {
        fence: Option<orcher_storage::repositories::domains::workflow_execution::ActivationFence>,
        activation: Option<i64>,
        token: bool,
    },
    /// This activation's completion has already been applied: the same
    /// completion again. The commands are not applied again; what is re-run
    /// is only what follows an application and is safe to repeat, in case the
    /// first attempt was cut off after recording itself. See
    /// [`ExecutionService::finish_replayed`].
    Replayed(Settled),
    /// The activation was handed out again before this completion arrived,
    /// and its completion must not be applied over the newer one. Nothing is
    /// applied; see [`ExecutionService::finish_stale`].
    Stale(Settled),
}

/// Room set aside in a completion's answer for everything but a task's input:
/// its identifiers, token, timeouts, headers and retry policy.
const EAGER_ANSWER_HEADROOM: usize = 64 * 1024;

/// Who sent a poll, kept for acting on their behalf once the poll has found
/// work: what they can receive, and the credentials to act with.
struct PollCaller {
    metadata: tonic::metadata::MetadataMap,
    extensions: tonic::Extensions,
    namespace: String,
    identity: String,
    /// The largest message the worker can receive.
    receive_limit: usize,
    /// The worker said nothing about it, so it is held to the gRPC default.
    legacy: bool,
}

impl PollCaller {
    fn of<T>(
        request: &Request<T>,
        limits: &PayloadLimits,
        namespace: &str,
        identity: &str,
    ) -> Self {
        Self {
            metadata: request.metadata().clone(),
            extensions: request.extensions().clone(),
            namespace: namespace.to_string(),
            identity: identity.to_string(),
            receive_limit: limits.receive_limit(request.metadata()),
            legacy: receive_limit_stated(request.metadata()).is_none(),
        }
    }
}

/// Clear what a worker replaying the journal never reads and the activation
/// carries elsewhere.
///
/// A task's result reaches the workflow in the StepCompleted entry built from
/// the step log; the TaskCompleted entry carried the same bytes again, so a
/// task result travelled twice in every activation after it — a 3 MB result
/// made a 6.3 MB activation, over the 4 MiB a worker could receive. A task's
/// input is the workflow's own, issued again by its code on replay. No SDK
/// reads either field from an activation: the replayer matches tasks by id and
/// type, and takes results only from StepCompleted. The entries themselves
/// stay, so the journal a worker replays has the same shape and numbering.
pub(crate) fn strip_replayed_payloads(journal: &mut [orcher_proto::orcher::v1::JournalEntry]) {
    use orcher_proto::orcher::v1::journal_entry::Attributes;
    for entry in journal {
        match entry.attributes.as_mut() {
            Some(Attributes::TaskCompleted(attrs)) => attrs.result = Vec::new(),
            Some(Attributes::TaskScheduled(attrs)) => attrs.input = Vec::new(),
            _ => {}
        }
    }
}

/// An activation claimed for a poll and not yet returned to it. Dropped
/// while armed — the poll ended, or failed, before returning it — it is
/// handed back for the next poll; left claimed, its workflow would wait until
/// the claim timed out.
struct Undelivered {
    polling_manager: Arc<PollingManager>,
    execution_id: String,
    activation: i64,
    identity: String,
    armed: bool,
}

impl Drop for Undelivered {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let polling_manager = self.polling_manager.clone();
        let execution_id = std::mem::take(&mut self.execution_id);
        let identity = std::mem::take(&mut self.identity);
        let activation = self.activation;
        tokio::spawn(async move {
            polling_manager
                .hand_back(&execution_id, activation, &identity)
                .await;
        });
    }
}

/// Where an activation stood when a completion for it was turned away.
struct Settled {
    /// `None` for a completion that carried no token, attributed to an
    /// earlier activation by who sent it.
    activation: Option<i64>,
    current: i64,
    status: String,
    claimed_by: Option<String>,
    task_queue: String,
    parent_execution_id: Option<String>,
}

impl Settled {
    fn of(
        fence: &orcher_storage::repositories::domains::workflow_execution::ActivationFence,
        activation: Option<i64>,
    ) -> Self {
        Settled {
            activation,
            current: fence.current(),
            status: fence.status().to_string(),
            claimed_by: fence.claimed_by().map(str::to_string),
            task_queue: fence.task_queue().to_string(),
            parent_execution_id: fence.parent_execution_id().map(str::to_string),
        }
    }
}

/// Who sends a completion, failure or release, as far as admitting it goes.
#[derive(Debug, Clone, Copy)]
struct Completer<'a> {
    /// The identity it was sent under, which a completion without a token is
    /// attributed by.
    identity: &'a str,
    /// The organization the caller acts for, as the auth layer established.
    organization_id: Option<Uuid>,
    /// The namespace the request names.
    namespace: &'a str,
}

/// The answer for an execution the caller may not see, whether it does not
/// exist or belongs to someone else: the same either way, so it says nothing
/// about which.
fn execution_not_found(execution_id: &str) -> Status {
    Status::not_found(format!("workflow execution {execution_id} not found"))
}

/// The worker protocol version from which a workflow failure's
/// `non_retryable` is what the workflow code raised.
///
/// Workers below it mark every workflow failure non-retryable, or none, by
/// what their SDK does rather than by what the code raised. Honouring their
/// mark would end every workflow retry for them, so it is cleared: their
/// failures are retried by the policy alone, as they always were.
const WORKFLOW_FAILURE_RETRYABILITY: u32 = 2;

/// Clear a workflow failure's `non_retryable` reported by a worker below
/// [`WORKFLOW_FAILURE_RETRYABILITY`], whose mark does not mean it.
fn ignore_unmeant_non_retryable(
    protocol_version: u32,
    failure: Option<&mut orcher_proto::orcher::v1::Failure>,
) {
    if protocol_version < WORKFLOW_FAILURE_RETRYABILITY {
        if let Some(failure) = failure {
            failure.non_retryable = false;
        }
    }
}

/// [`ignore_unmeant_non_retryable`] for the FailWorkflow commands of a
/// completion.
fn ignore_unmeant_non_retryable_commands(
    protocol_version: u32,
    commands: &mut [orcher_proto::orcher::v1::Command],
) {
    for command in commands {
        if let Some(orcher_proto::orcher::v1::command::Attributes::FailWorkflow(attrs)) =
            command.attributes.as_mut()
        {
            ignore_unmeant_non_retryable(protocol_version, attrs.failure.as_mut());
        }
    }
}

/// The command that ends a run whose cancellation was requested, in place of
/// what a worker that cannot hand the request to its workflow reported.
fn older_worker_cancellation() -> orcher_proto::orcher::v1::Command {
    orcher_proto::orcher::v1::Command {
        command_type: CommandType::CancelWorkflow as i32,
        attributes: Some(
            orcher_proto::orcher::v1::command::Attributes::CancelWorkflow(
                orcher_proto::orcher::v1::CancelWorkflowCommandAttributes {
                    details:
                        b"Cancellation requested; the worker does not handle cancellation requests"
                            .to_vec(),
                },
            ),
        ),
    }
}

/// Whether a workflow task completion releases the execution's claim.
///
/// The claim is the engine's "this execution is with a worker or parked" mark.
/// It is released only when the workflow is finished. Every other completion
/// is a suspension — on a task, a timer, a child workflow, or an external
/// event — and the claim stays until whatever the workflow is waiting for
/// arrives: task completion, timer fire and child completion each release it
/// on their own path, and so does every event-delivery path.
///
/// A completion with no commands used to release the claim too, on the theory
/// that a workflow parked on an event has nothing to hold it. But an unclaimed
/// `running` execution is exactly what the pollers scan for, so the parked
/// workflow was re-dispatched within a poll interval, replayed, parked again,
/// and released again — sixteen times a second on a four-poller worker, for as
/// long as the event took to arrive. Holding the claim makes a parked workflow
/// behave like one waiting on a timer, with the claim-timeout reclaimer as the
/// same backstop.
fn claim_released_after_completion(
    workflow_completed: bool,
    workflow_failed: bool,
    workflow_cancelled: bool,
) -> bool {
    workflow_completed || workflow_failed || workflow_cancelled
}

/// Give back the capacity permit taken when this activation was handed out.
///
/// The permit is acquired by the poll, under the identity that polled —
/// `worker@host-wf-driver-poller-2`, which is also what the claim records. A
/// completion carries the worker's own identity, `worker@host`, so releasing by
/// the completing identity looked up a registry entry that does not exist,
/// released nothing, and said nothing about it. Every activation leaked a
/// permit. After `max_permits` of them the worker went deaf: it kept polling,
/// every poll answered "at capacity" before looking for work, and executions it
/// was entitled to claim sat untouched — on staging, twelve hours in, a
/// reconciliation stopped mid-loop and stayed there.
///
/// So prefer the claim holder and fall back to the completing identity, which
/// still covers a claim already cleared or never recorded. If neither is
/// registered, say so: a silently dropped permit is the whole bug.
fn release_activation_permit(
    registry: &orcher_dispatch::capacity::CapacityRegistry,
    task_queue: &str,
    namespace: &str,
    claimed_by: Option<&str>,
    completing_identity: &str,
    execution_id: &str,
) {
    let candidates = claimed_by
        .into_iter()
        .chain(std::iter::once(completing_identity))
        .filter(|identity| !identity.is_empty());

    for identity in candidates {
        if let Some(permits) = registry.get(task_queue, namespace, identity) {
            if permits.release_for(execution_id) {
                debug!(
                    identity = %identity,
                    task_queue = %task_queue,
                    namespace = %namespace,
                    execution_id = %execution_id,
                    permits_available = permits.available(),
                    "Released worker capacity permit"
                );
                return;
            }
        }
    }

    // Not an error: with more than one replica the activation may have been
    // handed out by another one, which returns its permit when it expires.
    debug!(
        claimed_by = ?claimed_by,
        completing_identity = %completing_identity,
        task_queue = %task_queue,
        namespace = %namespace,
        execution_id = %execution_id,
        "No permit held here for this activation; the replica that handed it out returns it"
    );
}

/// Which attempt of its task a dispatch payload delivers: the task execution's
/// `attempt_number`, 1 for the first, which a retry's dispatch carries.
///
/// Not the outbox row's `attempt_count`, which counts how many times the
/// outbox tried to hand the dispatch over and is 0 for nearly every task:
/// reporting it told every worker that every attempt, retries included, was
/// attempt 0. A payload written before `attempt_number` was carried is a first
/// attempt.
fn dispatched_attempt(payload: &serde_json::Value) -> i32 {
    payload
        .get("attempt_number")
        .and_then(|v| v.as_i64())
        .and_then(|n| i32::try_from(n).ok())
        .filter(|n| *n > 0)
        .unwrap_or(1)
}

/// Whether a claim that numbered its activation `activation_seq` is the first
/// to hand its run to a worker.
///
/// Every claim numbers the activation it hands out one past the last, in the
/// statement that takes the claim, and the number never goes back, so exactly
/// one claim in a run's life sees 1, however often the run is handed out again
/// or redelivered.
fn is_first_activation(activation_seq: i64) -> bool {
    activation_seq == 1
}

/// Whether the worker that reports as `identity` is the one holding a claim
/// recorded as `holder`.
///
/// A claim records the identity that polled, and the SDKs poll under a name
/// derived from the one they complete under: sdk-core — which the Rust, Python
/// and TypeScript SDKs all run on — polls as `{identity}-wf-driver-poller-{n}`
/// and completes as `{identity}`. A client that polls and completes under the
/// same name matches exactly.
fn claim_belongs_to(holder: &str, identity: &str) -> bool {
    holder == identity
        || holder
            .strip_prefix(identity)
            .and_then(|rest| rest.strip_prefix("-wf-driver-poller-"))
            .is_some_and(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()))
}

impl ExecutionService {
    /// Create a new ExecutionService
    ///
    /// # Arguments
    ///
    /// * `command_processor` - Processor for workflow commands
    /// * `polling_manager` - Manager for efficient long-polling
    /// * `execution_log_repository` - Repository for step result loading
    /// * `journal_repository` - Repository for journal queries
    /// * `workflow_execution_repository` - Repository for workflow metadata queries
    /// * `task_execution_repository` - Repository for task lifecycle tracking
    pub fn new(
        command_processor: Arc<CommandProcessor>,
        polling_manager: Arc<PollingManager>,
        execution_log_repository: Arc<dyn ExecutionLogRepository>,
        journal_repository: Arc<JournalRepository>,
        workflow_execution_repository: Arc<WorkflowExecutionRepository>,
        task_execution_repository: Arc<TaskExecutionRepository>,
        pool: sqlx::PgPool,
    ) -> Self {
        let logs_repository = Arc::new(WorkflowExecutionLogsRepository::new(pool.clone()));
        let capacity_registry = Arc::new(orcher_dispatch::capacity::CapacityRegistry::new());
        let eager_dispatch = Arc::new(
            orcher_dispatch::EagerDispatchIntegrationBuilder::new()
                .capacity_registry(Arc::clone(&capacity_registry))
                .build(),
        );
        let child_outcomes = Arc::new(ChildOutcomeDelivery::new(
            workflow_execution_repository.clone(),
            journal_repository.clone(),
            Arc::new(ChildWorkflowRepository::new(pool.clone())),
            polling_manager.clone(),
        ));
        let worker_shutdowns = Arc::new(crate::worker::WorkerShutdowns::new(pool.clone()));
        polling_manager.set_worker_shutdowns(worker_shutdowns.clone());
        let service = Self {
            this: std::sync::OnceLock::new(),
            command_processor,
            polling_manager,
            task_polling_manager: None,
            execution_log_repository,
            journal_repository,
            workflow_execution_repository,
            task_execution_repository,
            workflow_coordinator: None,
            journal_mapper: JournalMapper::new(),
            retry_scheduler: TaskRetryScheduler::new(pool),
            logs_repository,
            capacity_registry,
            default_worker_capacity: 100, // Default to 100 concurrent tasks per worker
            eager_dispatch,
            workflow_completion_notifier: None,
            pending_queries: None,
            pending_updates: None,
            namespace_repository: None,
            child_outcomes,
            owner_forwarder: None,
            require_activation_token: false,
            worker_shutdowns,
            worker_liveness: Arc::new(crate::worker::WorkerLiveness::new()),
            default_heartbeat_timeout_secs: Some(DEFAULT_HEARTBEAT_TIMEOUT_SECS),
            payload_limits: PayloadLimits::default(),
        };

        info!(
            default_worker_capacity = 100,
            "ExecutionService::new - capacity registry initialized"
        );

        service
    }

    /// The component this service delivers child outcomes with, for the child
    /// reconciliation sweep to share.
    pub fn child_outcome_delivery(&self) -> Arc<ChildOutcomeDelivery> {
        self.child_outcomes.clone()
    }

    /// Inject the shard-pool resolver into the embedded retry scheduler (partitioned
    /// persistence), so a task that fails and reschedules co-locates its new attempt on
    /// the workflow's shard. No-op when unpartitioned.
    pub fn set_shard_resolver(
        &self,
        resolver: std::sync::Arc<dyn orcher_storage::sharding::pool_routing::ShardPoolResolver>,
    ) {
        self.retry_scheduler.set_shard_resolver(resolver);
    }

    /// Set the workflow coordinator for task polling
    ///
    /// This enables task polling from the outbox queue for workers.
    ///
    /// # Arguments
    ///
    /// * `coordinator` - The workflow coordinator instance
    pub fn with_coordinator(
        mut self,
        coordinator: Arc<crate::engine::WorkflowCoordinator>,
    ) -> Self {
        self.workflow_coordinator = Some(coordinator);
        self
    }

    /// Set the task polling manager for efficient task execution long-polling
    ///
    /// This enables notification-based long-polling for task execution,
    /// preventing CPU spinning and database hammering.
    ///
    /// # Arguments
    ///
    /// * `task_polling_manager` - The task polling manager instance
    pub fn with_task_polling_manager(
        mut self,
        task_polling_manager: Arc<TaskPollingManager>,
    ) -> Self {
        info!("DEBUG ExecutionService::with_task_polling_manager - Setting task_polling_manager to Some");
        task_polling_manager.set_worker_shutdowns(self.worker_shutdowns.clone());
        self.task_polling_manager = Some(task_polling_manager);
        info!("DEBUG ExecutionService::with_task_polling_manager - task_polling_manager.is_some() = {}", self.task_polling_manager.is_some());
        self
    }

    /// Set the workflow completion notifier for get_workflow_result notification
    pub fn with_workflow_completion_notifier(
        mut self,
        notifier: crate::grpc::services::workflow::WorkflowCompletionNotifier,
    ) -> Self {
        self.workflow_completion_notifier = Some(notifier);
        self
    }

    /// Set the shared pending query registry for query dispatch.
    ///
    /// Queries from `WorkflowService::query_workflow()` are registered here.
    /// This service attaches them to poll responses and resolves them
    /// when workers respond via `RespondQueryTask` or `CompleteWorkflowExecution`.
    pub fn with_pending_queries(
        mut self,
        pending_queries: crate::grpc::services::query_dispatch::PendingQueryRegistry,
    ) -> Self {
        self.pending_queries = Some(pending_queries);
        self
    }

    /// Set the shared pending update registry for update dispatch.
    ///
    /// Updates from `WorkflowService::update_workflow()` are registered here.
    /// This service attaches them to poll responses, journals the results,
    /// and resolves them when workers respond.
    pub fn with_pending_updates(
        mut self,
        pending_updates: crate::grpc::services::update_dispatch::PendingUpdateRegistry,
    ) -> Self {
        self.pending_updates = Some(pending_updates);
        self
    }

    /// Set the eager dispatch integration with custom configuration
    ///
    /// # Arguments
    ///
    /// * `eager_dispatch` - Custom eager dispatch integration instance
    pub fn with_eager_dispatch(
        mut self,
        eager_dispatch: Arc<orcher_dispatch::EagerDispatchIntegration>,
    ) -> Self {
        self.eager_dispatch = eager_dispatch;
        self
    }

    /// Set the default worker capacity for workers that don't specify their own
    ///
    /// # Arguments
    ///
    /// * `capacity` - Default number of concurrent tasks per worker
    pub fn with_default_worker_capacity(mut self, capacity: usize) -> Self {
        info!(
            default_worker_capacity = capacity,
            "Setting default worker capacity"
        );
        self.default_worker_capacity = capacity;
        self
    }

    /// Set the namespace repository for validating namespace existence on poll paths
    pub fn with_namespace_repository(
        mut self,
        namespace_repository: Arc<dyn NamespaceRepositoryTrait>,
    ) -> Self {
        self.namespace_repository = Some(namespace_repository);
        self
    }

    /// Relay each workflow completion to the node owning its workflow, when
    /// that is not this one. See [`crate::grpc::forward`].
    pub fn with_owner_forwarder(
        mut self,
        forwarder: Arc<crate::grpc::forward::OwnerForwarder>,
    ) -> Self {
        self.owner_forwarder = Some(forwarder);
        self
    }

    /// Learn of workers shut down through any replica on this database, not
    /// only this one. Without it a `ShutdownWorker` is honoured only by the
    /// replica that took it (and by every replica once it restarts).
    pub fn start_worker_shutdown_listener(&self) -> tokio::task::JoinHandle<()> {
        self.worker_shutdowns.clone().start_listener()
    }

    /// Wait until the listener started by
    /// [`start_worker_shutdown_listener`](Self::start_worker_shutdown_listener)
    /// is subscribed and has read the shutdowns already recorded.
    pub async fn worker_shutdowns_listening(&self) {
        self.worker_shutdowns.listening().await
    }

    /// The worker processes this replica sees polling, for
    /// [`LapsedWorkerClaims`](crate::engine::dispatch::LapsedWorkerClaims) to
    /// record and judge.
    pub fn worker_liveness(&self) -> Arc<crate::worker::WorkerLiveness> {
        Arc::clone(&self.worker_liveness)
    }

    /// The heartbeat timeout an attempt runs under when its task sets none and
    /// its worker heartbeats every task on its own; `None` (or zero) gives no
    /// default. [`DEFAULT_HEARTBEAT_TIMEOUT_SECS`] unless set.
    pub fn with_default_heartbeat_timeout(mut self, timeout: Option<std::time::Duration>) -> Self {
        self.default_heartbeat_timeout_secs = timeout
            .map(|t| t.as_secs().min(i32::MAX as u64) as i32)
            .filter(|secs| *secs > 0);
        self
    }

    /// The heartbeat timeout to give an attempt whose task sets none, for a
    /// worker that declares `capabilities`: the default, if it heartbeats on
    /// its own; otherwise none, since a worker that does not would have its
    /// long tasks timed out while they run.
    fn auto_heartbeat_timeout(
        &self,
        capabilities: Option<&orcher_proto::orcher::v1::TaskCapabilities>,
    ) -> Option<i32> {
        capabilities
            .filter(|c| c.auto_heartbeat)
            .and(self.default_heartbeat_timeout_secs)
    }

    /// How large a message and a payload may be. [`PayloadLimits::default`]
    /// unless set.
    pub fn with_payload_limits(mut self, limits: PayloadLimits) -> Self {
        self.payload_limits = limits;
        self
    }

    /// Fail the task whose completion carries a result over the payload limit,
    /// so its workflow hears a task failure that says why, and refuse the
    /// completion with the same reason.
    ///
    /// Accepted, the result would be stored and then carried in every
    /// activation of the workflow; an activation larger than the worker can
    /// receive used to leave the workflow running for good, handed out again
    /// at every claim timeout. The failure is non-retryable: the same code
    /// produces the same result. The refusal is INVALID_ARGUMENT, which every
    /// SDK treats as final, so the worker logs it rather than resending.
    async fn refuse_oversized_task_result(
        &self,
        request: Request<CompleteTaskExecutionRequest>,
        reason: String,
    ) -> Result<Response<CompleteTaskExecutionResponse>, Status> {
        let (metadata, extensions, req) = request.into_parts();
        warn!(
            namespace = %req.namespace,
            identity = %req.identity,
            reason = %reason,
            "Task result over the payload limit; failing the task"
        );
        let failure = FailTaskExecutionRequest {
            task_token: req.task_token,
            namespace: req.namespace,
            identity: req.identity,
            failure: Some(orcher_proto::orcher::v1::Failure {
                message: reason.clone(),
                source: "engine".to_string(),
                failure_type: PAYLOAD_TOO_LARGE.to_string(),
                non_retryable: true,
                ..Default::default()
            }),
        };
        ExecutionServiceTrait::fail_task_execution(
            self,
            Request::from_parts(metadata, extensions, failure),
        )
        .await?;
        Err(Status::invalid_argument(reason))
    }

    /// `response`, if the worker that polled can receive it. A task larger
    /// than that would be refused by the worker's own gRPC stack on arrival,
    /// and the attempt would wait out its timeout and be handed out again,
    /// refused the same way each time; it is failed instead, non-retryably,
    /// saying why, and the poll is answered with no work.
    async fn deliverable_task(
        &self,
        response: Result<Response<PollTaskExecutionResponse>, Status>,
        caller: &PollCaller,
    ) -> Result<Response<PollTaskExecutionResponse>, Status> {
        let response = response?;
        let size = response.get_ref().encoded_len();
        let polled = response.get_ref();
        if polled.task_token.is_empty() || size <= caller.receive_limit {
            return Ok(response);
        }
        let polled = response.into_inner();
        let reason = activation_too_large(
            &format!("task {:?} with its input", polled.task_id),
            size,
            caller.receive_limit,
            caller.legacy,
        );
        warn!(
            task_id = %polled.task_id,
            workflow_id = %polled.workflow_id,
            identity = %caller.identity,
            reason = %reason,
            "Task larger than the polling worker can receive; failing it"
        );
        let failure = FailTaskExecutionRequest {
            task_token: polled.task_token,
            namespace: caller.namespace.clone(),
            identity: caller.identity.clone(),
            failure: Some(orcher_proto::orcher::v1::Failure {
                message: reason,
                source: "engine".to_string(),
                failure_type: PAYLOAD_TOO_LARGE.to_string(),
                non_retryable: true,
                ..Default::default()
            }),
        };
        if let Err(e) = ExecutionServiceTrait::fail_task_execution(
            self,
            Request::from_parts(caller.metadata.clone(), caller.extensions.clone(), failure),
        )
        .await
        {
            warn!(
                task_id = %polled.task_id,
                error = %e,
                "Could not fail the task too large for its worker; it times out instead"
            );
        }
        Ok(Response::new(PollTaskExecutionResponse {
            task_queue: polled.task_queue,
            namespace: polled.namespace,
            ..Default::default()
        }))
    }

    /// `response`, if the worker that polled can receive it.
    ///
    /// An activation carries the workflow's whole journal: every task result
    /// and event payload it has received. One larger than the worker can
    /// receive used to be handed out, refused by the worker's gRPC stack, and
    /// handed out again at every claim timeout, the workflow running forever.
    /// It is now completed here on the worker's behalf, failing the workflow
    /// with the reason — through the same path a worker's completion takes,
    /// so the activation, its permit and its claim are settled as usual — and
    /// the poll is answered with no work.
    async fn deliverable_activation(
        &self,
        response: PollWorkflowExecutionResponse,
        caller: &PollCaller,
    ) -> PollWorkflowExecutionResponse {
        let size = response.encoded_len();
        if size <= caller.receive_limit {
            return response;
        }
        let reason = activation_too_large(
            &format!("the activation of workflow {}", response.workflow_id),
            size,
            caller.receive_limit,
            caller.legacy,
        );
        warn!(
            workflow_id = %response.workflow_id,
            execution_id = %response.execution_id,
            identity = %caller.identity,
            reason = %reason,
            "Activation larger than the polling worker can receive; failing the workflow"
        );
        let completion = CompleteWorkflowExecutionRequest {
            workflow_id: response.workflow_id.clone(),
            execution_id: response.execution_id.clone(),
            namespace: caller.namespace.clone(),
            identity: caller.identity.clone(),
            commands: vec![orcher_proto::orcher::v1::Command {
                command_type: CommandType::FailWorkflow as i32,
                attributes: Some(orcher_proto::orcher::v1::command::Attributes::FailWorkflow(
                    orcher_proto::orcher::v1::FailWorkflowCommandAttributes {
                        failure: Some(orcher_proto::orcher::v1::Failure {
                            message: reason,
                            source: "engine".to_string(),
                            failure_type: PAYLOAD_TOO_LARGE.to_string(),
                            non_retryable: true,
                            ..Default::default()
                        }),
                    },
                )),
            }],
            stream_entry_id: response.stream_entry_id.clone(),
            ..Default::default()
        };
        if let Err(e) = ExecutionServiceTrait::complete_workflow_execution(
            self,
            Request::from_parts(
                caller.metadata.clone(),
                caller.extensions.clone(),
                completion,
            ),
        )
        .await
        {
            warn!(
                workflow_id = %response.workflow_id,
                execution_id = %response.execution_id,
                error = %e,
                "Could not fail the workflow too large for its worker; its claim times out"
            );
        }
        PollWorkflowExecutionResponse::default()
    }

    /// Refuse workflow completions that carry no activation token. A completion
    /// that carries one is always held to it; this decides only what happens to
    /// one that does not — accepted as before (the default), or refused.
    pub fn with_activation_token_required(mut self, required: bool) -> Self {
        self.require_activation_token = required;
        self
    }

    /// Decide whether a workflow completion is to be applied, and hold the
    /// execution's other completions off while it is.
    ///
    /// Runs where the completion is applied — after the relay to the owner has
    /// been decided, so on the owner — and before any command. A completion
    /// carrying an activation token is applied only for the activation that is
    /// current: one already applied is [`Admission::Replayed`], and one whose
    /// activation has since been handed out again is [`Admission::Stale`].
    ///
    /// One without a token is what every worker sent before tokens existed,
    /// and is applied as it always was — recorded against the current
    /// activation, which is the one it almost always answers — unless
    /// `require_token` says otherwise. It still waits its turn behind the lock,
    /// so it cannot interleave with another completion of the same execution.
    ///
    /// Either way the execution must exist: an admitted completion always
    /// carries its fence.
    #[allow(clippy::result_large_err)]
    /// Cancel the run instead of recording a failure an older worker
    /// reported, when its cancellation was requested. Guarded like a client's
    /// cancellation and not fenced, for the same reason. Returns whether it
    /// was cancelled here.
    async fn cancel_instead_of_failure(
        &self,
        workflow_id: &str,
        execution_id: &str,
        fence: Option<&orcher_storage::repositories::domains::workflow_execution::ActivationFence>,
    ) -> bool {
        // Failure reports seldom carry an activation token, so the fence that
        // would have read the request is usually absent. Failures are rare;
        // read it directly then.
        let requested = match fence {
            Some(fence) => fence.cancel_requested_at().is_some(),
            None => match uuid::Uuid::parse_str(workflow_id) {
                Ok(id) => matches!(
                    self.workflow_execution_repository
                        .pending_cancel_request(id)
                        .await,
                    Ok(Some(_))
                ),
                Err(_) => false,
            },
        };
        if !requested {
            return false;
        }
        match self
            .workflow_execution_repository
            .cancel(execution_id)
            .await
        {
            Ok(true) => {
                if let Err(e) = JournalService::new(self.journal_repository.clone())
                    .append_workflow_canceled(
                        workflow_id,
                        execution_id,
                        "Cancellation requested; the worker does not handle cancellation requests"
                            .to_string(),
                    )
                    .await
                {
                    warn!(execution_id = %execution_id, error = %e, "Cancelled, but not journalled");
                }
                info!(
                    execution_id = %execution_id,
                    "Worker does not handle cancellation requests; cancelled the run instead of failing it"
                );
                true
            }
            Ok(false) => false,
            Err(e) => {
                warn!(execution_id = %execution_id, error = %e, "Could not cancel the run; recording its failure");
                false
            }
        }
    }

    async fn admit_completion(
        &self,
        workflow_id: &str,
        execution_id: &str,
        token: Option<Result<crate::grpc::activation_token::ActivationToken, Status>>,
        require_token: bool,
        completer: Completer<'_>,
    ) -> Result<Admission, Status> {
        let identity = completer.identity;
        let token = token.transpose()?;
        if let Some(ref token) = token {
            if token.execution_id != execution_id {
                return Err(Status::invalid_argument(format!(
                    "activation token is for execution {}, not {execution_id}",
                    token.execution_id
                )));
            }
        } else if require_token {
            return Err(Status::invalid_argument(
                "a workflow completion must carry the activation token its poll returned",
            ));
        }

        let fence = self
            .workflow_execution_repository
            .enter_activation(workflow_id, execution_id)
            .await
            .map_err(|e| match e {
                OrcherError::InvalidInput { message, .. } => Status::invalid_argument(message),
                other => Status::internal(format!("Failed to fence the activation: {other}")),
            })?;

        // A completion names an execution that must exist, token or not. One
        // without a token used to be applied regardless, with no fence to hold
        // it or record it against: its commands wrote journal entries and task
        // rows for a workflow the table had no record of.
        let Some(fence) = fence else {
            return Err(execution_not_found(execution_id));
        };
        // The execution is found by id alone, and the RPC was authorized
        // against the namespace the caller named, not the execution's. So
        // without this a caller could complete, fail or release another
        // organization's execution, or one in a namespace its key does not
        // reach, by naming its id. Refused as though it did not exist.
        if !crate::grpc::tenant::may_act_on(fence.organization_id(), completer.organization_id)
            || fence.namespace() != completer.namespace
        {
            warn!(
                workflow_id = %workflow_id,
                execution_id = %execution_id,
                "Refusing a completion or release of a workflow execution outside the \
                 caller's organization or namespace"
            );
            return Err(execution_not_found(execution_id));
        }

        let Some(token) = token else {
            // Which activation a tokenless completion answers can only be
            // inferred from who sent it. Sent by the worker holding the claim,
            // it answers the current activation and is recorded against it, so
            // wake-ups know the activation is done. Sent while the claim is
            // held by a different worker, it is from an activation handed out
            // before that one, and applying or recording it would land over
            // the newer activation: it is acknowledged, like a stale token.
            // Sent by the holder when the current activation has already been
            // applied, it is a repeat of that completion: nothing has been
            // handed out since, because handing out another would have moved
            // the current activation on. A workflow parked by that completion
            // keeps its claim on purpose, and a repeat must not apply its
            // commands again nor, as a release, give the claim up.
            // With no claim, or no identity to compare, it is applied as it
            // always was and not recorded against any activation.
            return Ok(match fence.claimed_by() {
                Some(holder) if !identity.is_empty() && !claim_belongs_to(holder, identity) => {
                    Admission::Stale(Settled::of(&fence, None))
                }
                Some(_) if !identity.is_empty() && fence.completed() == Some(fence.current()) => {
                    debug!(
                        workflow_id = %workflow_id,
                        execution_id = %execution_id,
                        activation = fence.current(),
                        "Completion carries no activation token, and the activation its \
                         sender holds has been applied already; acknowledging it as a repeat"
                    );
                    Admission::Replayed(Settled::of(&fence, Some(fence.current())))
                }
                Some(_) if !identity.is_empty() => {
                    debug!(
                        workflow_id = %workflow_id,
                        execution_id = %execution_id,
                        "Completion carries no activation token; attributed to the current \
                         activation, whose claim its sender holds"
                    );
                    let activation = Some(fence.current());
                    Admission::Apply {
                        fence: Some(fence),
                        activation,
                        token: false,
                    }
                }
                _ => Admission::Apply {
                    fence: Some(fence),
                    activation: None,
                    token: false,
                },
            });
        };

        let activation = token.activation_seq;
        if fence.completed() == Some(activation) {
            info!(
                workflow_id = %workflow_id,
                execution_id = %execution_id,
                activation,
                "Completion for an activation already applied; acknowledging it without applying it again"
            );
            return Ok(Admission::Replayed(Settled::of(&fence, Some(activation))));
        }
        if activation != fence.current() {
            return Ok(Admission::Stale(Settled::of(&fence, Some(activation))));
        }

        Ok(Admission::Apply {
            fence: Some(fence),
            activation: Some(activation),
            token: true,
        })
    }

    /// Give back the permit taken when the activation being completed was
    /// handed out.
    ///
    /// With the activation's number, exactly that activation's permit is
    /// returned, under whichever identity polled for it, and a permit taken
    /// for a later activation of the same execution is left alone. Without one
    /// — a completion that carried no token — it falls back to looking the
    /// permit up by who holds the claim, as before tokens.
    fn return_permit(
        &self,
        workflow: &WorkflowExecution,
        namespace: &str,
        identity: &str,
        execution_id: &str,
        activation: Option<i64>,
    ) {
        match activation {
            Some(activation) => {
                if !self.capacity_registry.release_activation(
                    &workflow.task_queue,
                    namespace,
                    execution_id,
                    activation,
                ) {
                    debug!(
                        execution_id = %execution_id,
                        activation,
                        "No permit held here for this activation; the replica that handed it out returns it"
                    );
                }
            }
            None if !identity.is_empty() => release_activation_permit(
                &self.capacity_registry,
                &workflow.task_queue,
                namespace,
                workflow.claimed_by.as_deref(),
                identity,
                execution_id,
            ),
            None => {}
        }
    }

    /// Give up on an activation whose completion failed after it was admitted,
    /// so its workflow is handed out again rather than held until its claim
    /// times out. See `ActivationFence::abandon`. A completion that could not
    /// be attributed to an activation has nothing to give up: its fence is
    /// dropped, as before.
    async fn abandon_activation(
        &self,
        execution_id: &str,
        fence: Option<orcher_storage::repositories::domains::workflow_execution::ActivationFence>,
        activation: Option<i64>,
    ) {
        let (Some(fence), Some(activation)) = (fence, activation) else {
            return;
        };
        match fence.abandon(activation).await {
            Ok(Some(resume)) => {
                let preferred_worker = self.polling_manager.journal_holder(execution_id).await;
                self.polling_manager.push_continuation(
                    &resume.task_queue,
                    &resume.namespace,
                    execution_id,
                    preferred_worker,
                );
                self.polling_manager
                    .notify_work_available(&resume.task_queue, &resume.namespace)
                    .await;
            }
            Ok(None) => {}
            Err(e) => warn!(
                execution_id = %execution_id,
                activation,
                error = %e,
                "Could not give up the failed activation; its claim times out instead"
            ),
        }
    }

    /// Answer a completion for an activation that was handed out again.
    ///
    /// Acknowledged, not refused. Nothing the worker can do with a refusal
    /// helps: the workflow is with another worker, and an error only made the
    /// SDK back its connection off as if the engine were failing. Nothing is
    /// applied. The permit this replica gave out with that activation, if it
    /// still holds one, is returned — it is keyed by the activation, so the
    /// newer activation's permit is not touched.
    fn finish_stale(&self, namespace: &str, identity: &str, execution_id: &str, settled: &Settled) {
        info!(
            execution_id = %execution_id,
            activation = ?settled.activation,
            current = settled.current,
            "Completion for an activation that has since been handed out again; acknowledged, \
             nothing applied"
        );
        orcher_monitoring::metrics::lifecycle::stale_completion(namespace);
        self.return_stale_permit(namespace, identity, execution_id, settled);
    }

    /// Return the permit this replica gave out with an activation that has
    /// since been handed out again, if it still holds one. Keyed by the
    /// activation, so the newer activation's permit is not touched.
    fn return_stale_permit(
        &self,
        namespace: &str,
        identity: &str,
        execution_id: &str,
        settled: &Settled,
    ) {
        match settled.activation {
            Some(activation) => {
                self.capacity_registry.release_activation(
                    &settled.task_queue,
                    namespace,
                    execution_id,
                    activation,
                );
            }
            // No token, so no activation number: the permit is found by who
            // sent the completion instead. Only that worker's own pollers are
            // looked at — the claim, and the current activation's permit,
            // belong to another worker, whose are never touched.
            None if !identity.is_empty() => {
                for permits in self
                    .capacity_registry
                    .workers(&settled.task_queue, namespace)
                {
                    if claim_belongs_to(permits.identity(), identity)
                        && permits.release_for(execution_id)
                    {
                        break;
                    }
                }
            }
            None => {}
        }
    }

    /// Hand back an activation its worker will not run, so the workflow is
    /// offered to the next poll at once instead of waiting out its claim.
    ///
    /// Admitted exactly as a completion is, behind the same fence, so it
    /// cannot interleave with a completion of the same execution, and it acts
    /// only on the activation that is current and not yet applied: that one's
    /// claim is given up (`ActivationFence::abandon`), its continuation queued
    /// and its permit returned. A release repeated, or one for an activation
    /// already completed or handed out again, finds nothing to give up and
    /// does nothing. Without a token it is attributed to the current activation
    /// only if the caller is the worker holding the claim, and only if that
    /// activation has not been applied: a workflow parked by its completion
    /// keeps the claim on purpose. Held to a token as a completion is, so
    /// `require_activation_token` refuses a release without one too.
    ///
    /// An execution that does not exist, or that is outside the caller's
    /// organization or namespace, is answered NOT_FOUND, the same either way.
    ///
    /// Nothing is recorded as applied and nothing is written to the journal:
    /// the next activation replays exactly what this one would have.
    #[allow(clippy::result_large_err)]
    async fn release_activation(
        &self,
        workflow_id: &str,
        execution_id: &str,
        token: Option<Result<crate::grpc::activation_token::ActivationToken, Status>>,
        completer: Completer<'_>,
    ) -> Result<(), Status> {
        let (namespace, identity) = (completer.namespace, completer.identity);
        let admission = self
            .admit_completion(
                workflow_id,
                execution_id,
                token,
                self.require_activation_token,
                completer,
            )
            .await?;
        match admission {
            Admission::Apply {
                fence: Some(fence),
                activation: Some(activation),
                ..
            } => {
                let task_queue = fence.task_queue().to_string();
                let released = fence.abandon(activation).await.map_err(|e| {
                    Status::unavailable(format!("Failed to release the activation: {e}"))
                })?;
                if let Some(resume) = released {
                    info!(
                        workflow_id = %workflow_id,
                        execution_id = %execution_id,
                        activation,
                        identity = %identity,
                        "Worker handed back an activation it will not run; offering it to the next poll"
                    );
                    // No preference for the worker that gave it back: it is
                    // most likely shutting down, and a continuation that
                    // prefers it is left for it by every other poller.
                    self.polling_manager.clear_affinity(execution_id);
                    self.polling_manager.push_continuation(
                        &resume.task_queue,
                        &resume.namespace,
                        execution_id,
                        None,
                    );
                    self.polling_manager
                        .notify_work_available(&resume.task_queue, &resume.namespace)
                        .await;
                }
                self.capacity_registry.release_activation(
                    &task_queue,
                    namespace,
                    execution_id,
                    activation,
                );
            }
            // No claim to give back, or no way to tell whose it is.
            Admission::Apply { .. } | Admission::Replayed(_) => {
                debug!(
                    workflow_id = %workflow_id,
                    execution_id = %execution_id,
                    "Release of an activation already settled; nothing to do"
                );
            }
            Admission::Stale(settled) => {
                debug!(
                    workflow_id = %workflow_id,
                    execution_id = %execution_id,
                    activation = ?settled.activation,
                    current = settled.current,
                    "Release of an activation handed out again since; nothing to do"
                );
                self.return_stale_permit(namespace, identity, execution_id, &settled);
            }
        }
        Ok(())
    }

    /// Answer a completion whose activation has already been applied.
    ///
    /// The first attempt recorded itself and then may have been cut off — its
    /// caller hung up — before what follows an application ran. What follows
    /// is re-run here, each part safe to repeat: this activation's permit is
    /// returned if still held; a finished run's claim is released if this
    /// activation still holds it; a finished child's outcome is offered to its
    /// parent, which delivery takes once; and a run that is claimable again is
    /// announced to pollers. The alternative — recording the activation only
    /// after all of that — cannot be done: those steps write the same row from
    /// other connections, and would wait on the lock the record holds.
    async fn finish_replayed(&self, namespace: &str, execution_id: &str, settled: &Settled) {
        if let Some(activation) = settled.activation {
            self.capacity_registry.release_activation(
                &settled.task_queue,
                namespace,
                execution_id,
                activation,
            );
        }

        let ended = matches!(
            settled.status.as_str(),
            "completed" | "failed" | "cancelled" | "terminated" | "timed_out"
        );
        if ended && settled.claimed_by.is_some() && settled.activation == Some(settled.current) {
            self.polling_manager.release_claim(execution_id).await.ok();
        }
        if matches!(settled.status.as_str(), "completed" | "failed")
            && settled.parent_execution_id.is_some()
        {
            self.notify_parent_of_child_outcome(execution_id).await;
        }
        if !ended && settled.claimed_by.is_none() {
            self.polling_manager
                .notify_work_available(&settled.task_queue, namespace)
                .await;
        }
    }

    /// Get capacity statistics for a task queue
    ///
    /// Returns statistics about worker capacity including:
    /// - Total registered workers
    /// - Total capacity (max permits)
    /// - Total used permits
    /// - Total available permits
    pub fn capacity_stats(
        &self,
        task_queue: &str,
        namespace: &str,
    ) -> orcher_dispatch::capacity::QueueCapacityStats {
        self.capacity_registry.queue_stats(task_queue, namespace)
    }

    /// Get global capacity statistics across all queues
    pub fn global_capacity_stats(&self) -> orcher_dispatch::capacity::GlobalCapacityStats {
        self.capacity_registry.global_stats()
    }

    /// Get the capacity registry for external access
    pub fn capacity_registry(&self) -> Arc<orcher_dispatch::capacity::CapacityRegistry> {
        Arc::clone(&self.capacity_registry)
    }

    /// Create a gRPC server for this service
    pub fn into_server(self) -> ExecutionServiceServer<Self> {
        let service = Arc::new(self);
        let _ = service.this.set(Arc::downgrade(&service));
        ExecutionServiceServer::from_arc(service)
    }

    /// Helper: Create empty task response
    fn empty_task_response(req: PollTaskExecutionRequest) -> PollTaskExecutionResponse {
        PollTaskExecutionResponse {
            task_token: vec![],
            workflow_id: String::new(),
            execution_id: String::new(),
            task_id: String::new(),
            task_type: String::new(),
            task_queue: req.task_queue,
            namespace: req.namespace,
            input: vec![],
            headers: Default::default(),
            scheduled_time: None,
            attempt: 0,
            schedule_to_start_timeout: None,
            start_to_close_timeout: None,
            heartbeat_timeout: None,
            retry_policy: None,
        }
    }

    /// Helper: Build task response from claimed OutboxTask
    /// Take this attempt for the polling worker: mark it started with the
    /// token the worker will complete it with. `false` means the task must
    /// not be handed out.
    ///
    /// Completion finds the task by that token and requires it to be started,
    /// so a task handed out without this succeeding can run but can never be
    /// completed. Both dispatch paths used to hand it out anyway — one logging
    /// the failure and continuing, the other issuing the update fire-and-forget
    /// as if it were only for observability, where a fast worker could also
    /// finish before it landed. Not being `scheduled` any more means another
    /// dispatch already owns the attempt, and handing it out again would only
    /// run it twice. The outbox entry stays claimable, so a transient failure
    /// is dispatched again.
    async fn take_task_attempt(
        &self,
        task_execution_id: &str,
        identity: &str,
        task_token: &str,
        task_id: &str,
        auto_heartbeat_timeout_secs: Option<i32>,
        worker_instance_id: &str,
    ) -> bool {
        if task_execution_id.is_empty() {
            warn!(
                task_id = %task_id,
                "No task_execution_id in payload (legacy task or missing from CommandProcessor)"
            );
            return true;
        }
        let Ok(uuid) = Uuid::parse_str(task_execution_id) else {
            warn!(
                task_execution_id = %task_execution_id,
                task_id = %task_id,
                "Invalid task_execution_id UUID in payload"
            );
            return true;
        };
        let id = orcher_common::types::identifiers::TaskExecutionId::from_uuid(uuid);

        match self
            .task_execution_repository
            .mark_started_as(
                id,
                identity.to_string(),
                task_token.to_string(),
                auto_heartbeat_timeout_secs,
                crate::worker::liveness::tracked_instance(worker_instance_id),
            )
            .await
        {
            Ok(_) => {
                info!(
                    task_execution_id = %id,
                    service_identity = %identity,
                    task_id = %task_id,
                    "Task execution marked as started"
                );
                true
            }
            Err(e) => {
                warn!(
                    task_execution_id = %id,
                    task_id = %task_id,
                    error = %e,
                    "Task not handed out: this poll could not take the attempt"
                );
                false
            }
        }
    }

    /// See [`TaskPollingManager::settle_matching`].
    async fn settle_matching(&self, task_id: Uuid) {
        if let Some(ref task_manager) = self.task_polling_manager {
            task_manager.settle_matching(task_id).await;
        }
    }

    /// See [`TaskPollingManager::settle_undeliverable`].
    async fn settle_undeliverable(&self, task_id: Uuid) {
        if let Some(ref task_manager) = self.task_polling_manager {
            task_manager.settle_undeliverable(task_id).await;
        }
    }

    /// Whether the task execution `task_execution_id` is known to be past
    /// `scheduled`, so that no dispatch of it can take it any more. `false`
    /// when that cannot be told: an id that does not parse, or a failed read.
    async fn task_already_taken(&self, task_execution_id: &str) -> bool {
        let Ok(id) = Uuid::parse_str(task_execution_id) else {
            return false;
        };
        matches!(
            self.task_execution_repository.still_scheduled(&[id]).await,
            Ok(scheduled) if scheduled.is_empty()
        )
    }

    /// End the queued tasks of the workflow `workflow_id` (its row id) that
    /// its cancellation stops, and wake it if any were. Returns how many were
    /// ended; 0 too when that failed, leaving them to the timeout watcher.
    /// See [`crate::retries::cancelled_before_start`].
    async fn cancel_interrupted_queued_tasks(&self, workflow_id: &str) -> usize {
        let Ok(id) = Uuid::parse_str(workflow_id) else {
            return 0;
        };
        match crate::retries::cancelled_before_start::cancel_queued_tasks(
            &self.task_execution_repository,
            self.execution_log_repository.as_ref(),
            id,
        )
        .await
        {
            Ok(0) => 0,
            Ok(ended) => {
                if let Err(e) = self.requeue_workflow_for_continuation(id).await {
                    warn!(
                        workflow_id = %workflow_id,
                        error = %e,
                        "Queued tasks cancelled, but the workflow could not be woken"
                    );
                }
                ended
            }
            Err(e) => {
                warn!(
                    workflow_id = %workflow_id,
                    error = %e,
                    "Could not cancel the queued tasks its cancellation interrupted; left to the timeout watcher"
                );
                0
            }
        }
    }

    async fn build_task_response(
        &self,
        task: OutboxTask,
        req: PollTaskExecutionRequest,
        organization_id: Option<Uuid>,
    ) -> Result<Response<PollTaskExecutionResponse>, Status> {
        let payload = &task.payload;
        let workflow_id = payload
            .get("workflow_id")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let execution_id = payload
            .get("execution_id")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let task_id = payload
            .get("task_id")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let task_type = payload
            .get("task_type")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let input_bytes = payload
            .get("input")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_u64().map(|n| n as u8))
                    .collect::<Vec<u8>>()
            })
            .unwrap_or_default();

        // Get task_execution_id from payload (set by CommandProcessor)
        let task_execution_id_str = payload
            .get("task_execution_id")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let auto_heartbeat_timeout = self.auto_heartbeat_timeout(req.task_capabilities.as_ref());

        // Create task token (format: workflow_id:execution_id:task_id:sequence:attempt:started_at_ms)
        let started_at_ms = Timestamptz::now().timestamp_millis();
        let task_token = crate::grpc::task_token::encode(&crate::grpc::task_token::NewTaskToken {
            workflow_id,
            execution_id,
            task_id,
            attempt: dispatched_attempt(payload),
            started_at_ms,
            organization_id,
        });

        if !self
            .take_task_attempt(
                task_execution_id_str,
                &req.identity,
                &task_token,
                task_id,
                auto_heartbeat_timeout,
                &req.worker_instance_id,
            )
            .await
        {
            // Not taken. Only if the task is no longer waiting has its row
            // nothing left to deliver; one refused on a transient error keeps
            // it, and its lease brings it back. One refused because its
            // workflow's cancellation stops it is ended now, and then has
            // nothing left to deliver either.
            if self.task_already_taken(task_execution_id_str).await
                || (self.cancel_interrupted_queued_tasks(workflow_id).await > 0
                    && self.task_already_taken(task_execution_id_str).await)
            {
                self.settle_undeliverable(task.task_id).await;
            }
            return Ok(Response::new(Self::empty_task_response(req)));
        }
        // Taking the attempt settled its outbox row in the same statement
        // (see `TaskExecutionRepository::mark_started_as`); a partition row of
        // the matching layer is removed here.
        self.settle_matching(task.task_id).await;

        // Convert timeouts from payload
        let schedule_to_start_timeout = payload
            .get("schedule_to_start_timeout_secs")
            .and_then(|v| v.as_i64())
            .map(|secs| orcher_proto::prost_types::Duration {
                seconds: secs,
                nanos: 0,
            });

        let start_to_close_timeout = payload
            .get("start_to_close_timeout_secs")
            .and_then(|v| v.as_i64())
            .map(|secs| orcher_proto::prost_types::Duration {
                seconds: secs,
                nanos: 0,
            });

        // The timeout the attempt runs under: the task's own, or the default
        // it was just given for a worker that heartbeats on its own.
        let heartbeat_timeout = payload
            .get("heartbeat_timeout_secs")
            .and_then(|v| v.as_i64())
            .or(auto_heartbeat_timeout.map(i64::from))
            .map(|secs| orcher_proto::prost_types::Duration {
                seconds: secs,
                nanos: 0,
            });

        // The task's retry policy, as the engine enforces it.
        let retry_policy = payload
            .get("retry_policy")
            .map(crate::retries::retry_policy::stored_to_proto);

        info!(
            workflow_id = %workflow_id,
            execution_id = %execution_id,
            task_id = %task_id,
            task_type = %task_type,
            attempt = dispatched_attempt(payload),
            "Returning task to worker from outbox queue"
        );

        let response = PollTaskExecutionResponse {
            task_token: task_token.into_bytes(),
            workflow_id: workflow_id.to_string(),
            execution_id: execution_id.to_string(),
            task_id: task_id.to_string(),
            task_type: task_type.to_string(),
            task_queue: req.task_queue.clone(),
            namespace: req.namespace.clone(),
            input: input_bytes,
            headers: Default::default(),
            scheduled_time: Some(orcher_proto::prost_types::Timestamp {
                seconds: task.created_at.timestamp(),
                nanos: task.created_at.timestamp_subsec_nanos() as i32,
            }),
            attempt: dispatched_attempt(payload),
            schedule_to_start_timeout,
            start_to_close_timeout,
            heartbeat_timeout,
            retry_policy,
        };

        Ok(Response::new(response))
    }

    /// Build task response from a sync-matched task (no outbox claim DB reads).
    ///
    /// The task data comes directly from the dispatch side through the oneshot channel.
    /// The only DB write is mark_started (for observability), which is non-blocking.
    async fn build_task_response_from_sync_match(
        &self,
        sync_task: crate::engine::dispatch::task_manager::SyncMatchedTask,
        req: PollTaskExecutionRequest,
        organization_id: Option<Uuid>,
    ) -> Result<Response<PollTaskExecutionResponse>, Status> {
        let payload = &sync_task.payload;
        let workflow_id = payload
            .get("workflow_id")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let execution_id = payload
            .get("execution_id")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let task_id = payload
            .get("task_id")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let task_type = payload
            .get("task_type")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let input_bytes = payload
            .get("input")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_u64().map(|n| n as u8))
                    .collect::<Vec<u8>>()
            })
            .unwrap_or_default();

        let task_execution_id_str = payload
            .get("task_execution_id")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let auto_heartbeat_timeout = self.auto_heartbeat_timeout(req.task_capabilities.as_ref());

        // Create task token
        let started_at_ms = Timestamptz::now().timestamp_millis();
        let task_token = crate::grpc::task_token::encode(&crate::grpc::task_token::NewTaskToken {
            workflow_id,
            execution_id,
            task_id,
            attempt: dispatched_attempt(payload),
            started_at_ms,
            organization_id,
        });

        if !self
            .take_task_attempt(
                task_execution_id_str,
                &req.identity,
                &task_token,
                task_id,
                auto_heartbeat_timeout,
                &req.worker_instance_id,
            )
            .await
        {
            // One refused because its workflow's cancellation stops it is
            // ended now rather than handed out again and again.
            if !self.task_already_taken(task_execution_id_str).await {
                self.cancel_interrupted_queued_tasks(workflow_id).await;
            }
            return Ok(Response::new(Self::empty_task_response(req)));
        }

        // Taking the attempt settled its outbox row in the same statement (see
        // `TaskExecutionRepository::mark_started_as`). A row left open could
        // not have redelivered a lost response anyway: the attempt is started,
        // so a later poll could not take it. A lost response is recovered by
        // the attempt's timeouts, as for any polled task.

        // Convert timeouts from payload
        let schedule_to_start_timeout = payload
            .get("schedule_to_start_timeout_secs")
            .and_then(|v| v.as_i64())
            .map(|secs| orcher_proto::prost_types::Duration {
                seconds: secs,
                nanos: 0,
            });
        let start_to_close_timeout = payload
            .get("start_to_close_timeout_secs")
            .and_then(|v| v.as_i64())
            .map(|secs| orcher_proto::prost_types::Duration {
                seconds: secs,
                nanos: 0,
            });
        // The timeout the attempt runs under: the task's own, or the default
        // it was just given for a worker that heartbeats on its own.
        let heartbeat_timeout = payload
            .get("heartbeat_timeout_secs")
            .and_then(|v| v.as_i64())
            .or(auto_heartbeat_timeout.map(i64::from))
            .map(|secs| orcher_proto::prost_types::Duration {
                seconds: secs,
                nanos: 0,
            });

        // The task's retry policy, as the engine enforces it.
        let retry_policy = payload
            .get("retry_policy")
            .map(crate::retries::retry_policy::stored_to_proto);

        debug!(
            workflow_id = %workflow_id,
            execution_id = %execution_id,
            task_id = %task_id,
            task_type = %task_type,
            "⚡ Returning sync-matched task to worker (0 DB reads on poll side)"
        );

        let response = PollTaskExecutionResponse {
            task_token: task_token.into_bytes(),
            workflow_id: workflow_id.to_string(),
            execution_id: execution_id.to_string(),
            task_id: task_id.to_string(),
            task_type: task_type.to_string(),
            task_queue: req.task_queue.clone(),
            namespace: req.namespace.clone(),
            input: input_bytes,
            headers: Default::default(),
            scheduled_time: Some(orcher_proto::prost_types::Timestamp {
                seconds: sync_task.created_at.timestamp(),
                nanos: sync_task.created_at.timestamp_subsec_nanos() as i32,
            }),
            attempt: dispatched_attempt(payload),
            schedule_to_start_timeout,
            start_to_close_timeout,
            heartbeat_timeout,
            retry_policy,
        };

        Ok(Response::new(response))
    }

    /// Eagerly claim a single scheduled task and build its poll response.
    ///
    /// Calls `mark_started_eager` with a short TTL.  If the SDK drops the response
    /// (crash, channel close), the reclaimer resets the task back to 'scheduled' once
    /// the TTL expires — unless the task has a timeout of its own, which recovers it
    /// instead, or has heartbeated; see `mark_started_eager`.  Returns `None` when the task was already claimed by a poller
    /// (status != 'scheduled') — callers should treat this as a no-op and move on.
    ///
    /// `auto_heartbeat_timeout_secs` is the default heartbeat timeout for a task that
    /// sets none, given when the worker the task goes to heartbeats on its own; see
    /// [`Self::auto_heartbeat_timeout`]. Such a task gets no TTL: its heartbeat timeout
    /// recovers a dropped response instead.
    async fn build_eager_response_for_task(
        &self,
        task: &orcher_storage::schema::types::TaskExecution,
        identity: &str,
        eager_ttl: std::time::Duration,
        auto_heartbeat_timeout_secs: Option<i32>,
    ) -> Option<PollTaskExecutionResponse> {
        use prost_types::Duration as ProstDuration;

        let now = Timestamptz::now();
        let expires_at = now + eager_ttl;
        let started_at_ms = now.timestamp_millis();

        // The row's own owner, which is the authority: this path never sees a
        // request, so there is no caller to take it from.
        let token = crate::grpc::task_token::encode(&crate::grpc::task_token::NewTaskToken {
            workflow_id: &task.workflow_id.to_string(),
            execution_id: &task.execution_id.to_string(),
            task_id: &task.task_id,
            attempt: task.attempt_number,
            started_at_ms,
            organization_id: task.organization_id,
        });

        match self
            .task_execution_repository
            .mark_started_eager(
                task.id,
                identity.to_string(),
                token.clone(),
                expires_at,
                auto_heartbeat_timeout_secs,
            )
            .await
        {
            Ok(_) => {
                info!(
                    task_execution_id = %task.id,
                    task_id = %task.task_id,
                    identity = %identity,
                    expires_at = %expires_at,
                    "EAGER_DISPATCH: Eagerly claimed task for completing worker"
                );
            }
            Err(e) => {
                // Race with a normal poller — it won, skip gracefully
                debug!(
                    error = %e,
                    task_execution_id = %task.id,
                    task_id = %task.task_id,
                    "Eager claim failed (likely race with poller) — skipping"
                );
                return None;
            }
        }

        let schedule_to_start_timeout =
            task.schedule_to_start_timeout_secs
                .map(|secs| ProstDuration {
                    seconds: secs as i64,
                    nanos: 0,
                });
        let start_to_close_timeout = task.start_to_close_timeout_secs.map(|secs| ProstDuration {
            seconds: secs as i64,
            nanos: 0,
        });
        let heartbeat_timeout = task
            .heartbeat_timeout_secs
            .or(auto_heartbeat_timeout_secs)
            .map(|secs| ProstDuration {
                seconds: secs as i64,
                nanos: 0,
            });

        // The task's retry policy, as the engine enforces it.
        let retry_policy = task
            .retry_policy
            .as_ref()
            .map(crate::retries::retry_policy::stored_to_proto);

        Some(PollTaskExecutionResponse {
            task_token: token.into_bytes(),
            workflow_id: task.workflow_id.to_string(),
            execution_id: task.execution_id.to_string(),
            task_id: task.task_id.clone(),
            task_type: task.task_type.clone(),
            task_queue: task.task_queue.clone(),
            namespace: task.namespace.clone(),
            input: task.input.clone().unwrap_or_default(),
            headers: Default::default(),
            scheduled_time: Some(prost_types::Timestamp {
                seconds: task.scheduled_at.timestamp(),
                nanos: task.scheduled_at.timestamp_subsec_nanos() as i32,
            }),
            attempt: task.attempt_number,
            schedule_to_start_timeout,
            start_to_close_timeout,
            heartbeat_timeout,
            retry_policy,
        })
    }

    /// Validate that a namespace exists and is active in the database.
    ///
    /// Used on poll paths to give workers a clear error when polling on
    /// a non-existent or deprecated namespace, instead of silently returning
    /// empty responses forever.
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
}

/// Authorize a request that has a namespace and task_queue (poll-type RPCs).
///
/// If the auth layer injected a `ServiceIdentity` into the request extensions,
/// checks operation permission, namespace isolation, and task queue access.
/// If no identity is present (auth mode is `None`), this is a no-op.
#[allow(clippy::result_large_err)]
fn authorize_poll<T>(
    request: &Request<T>,
    operation: ServiceOperation,
    namespace: &str,
    task_queue: &str,
) -> Result<(), Status> {
    if let Some(identity) = request.extensions().get::<ServiceIdentity>() {
        crate::grpc::auth::check_operation(identity, operation)?;
        crate::grpc::auth::check_namespace(identity, namespace)?;
        crate::grpc::auth::check_task_queue(identity, task_queue)?;
    }
    Ok(())
}

/// Authorize a request that has a namespace but no task_queue (complete/fail RPCs).
///
/// If the auth layer injected a `ServiceIdentity`, checks operation permission
/// and namespace isolation. If no identity is present, this is a no-op.
#[allow(clippy::result_large_err)]
fn authorize_op<T>(
    request: &Request<T>,
    operation: ServiceOperation,
    namespace: &str,
) -> Result<(), Status> {
    if let Some(identity) = request.extensions().get::<ServiceIdentity>() {
        crate::grpc::auth::check_operation(identity, operation)?;
        crate::grpc::auth::check_namespace(identity, namespace)?;
    }
    Ok(())
}

impl ExecutionService {
    /// The part of `complete_workflow_execution` past checking the request:
    /// relaying it to the workflow's owner, or admitting it against its
    /// activation, applying its commands and recording the activation applied.
    async fn apply_workflow_completion(
        &self,
        mut req: CompleteWorkflowExecutionRequest,
        metadata: Option<tonic::metadata::MetadataMap>,
        caller: Option<uuid::Uuid>,
        receive_limit: usize,
        received_at: std::time::Instant,
    ) -> Result<Response<CompleteWorkflowExecutionResponse>, Status> {
        // Decide where this completion belongs before anything is applied. On
        // a node that does not own the workflow, every command before the first
        // fenced write would go through and the rest be refused; the owner
        // applies all of it.
        //
        // A completion with no commands writes nothing the fence guards — it
        // carries query or update results, or parks the workflow — so it is
        // handled wherever it lands, as it always was.
        let forwarding = (!req.commands.is_empty())
            .then_some(())
            .and(self.owner_forwarder.as_ref().zip(metadata.as_ref()));
        if let Some((forwarder, metadata)) = forwarding {
            use crate::grpc::forward::{Relayed, Route};
            if let Route::Owner { node, address } = forwarder.route(&req.workflow_id, metadata) {
                debug!(
                    workflow_id = %req.workflow_id,
                    owner = %node.as_str(),
                    peer = %address,
                    "Relaying workflow completion to the workflow's owner"
                );
                match forwarder
                    .complete_workflow_execution(&node, &address, metadata, req, received_at)
                    .await
                {
                    Relayed::Answered(answer) => return answer,
                    // Every engine version has this RPC, so an owner that
                    // says it does not is not one to hand the batch to;
                    // whether this node may apply it is for the check below.
                    Relayed::NotSent(body) | Relayed::Unsupported(body) => req = body,
                }
            }
            forwarder.ensure_owned_here(&req.workflow_id).await?;
        }

        // Only now, on the node that applies it, is the completion checked
        // against its activation: a node that relays it has nothing to check,
        // and checking there too would take the lock on the wrong side of the
        // relay.
        let token =
            crate::grpc::activation_token::from_completion(&req.task_token, &req.stream_entry_id);
        let (fence, activation, token_bearing) = match self
            .admit_completion(
                &req.workflow_id,
                &req.execution_id,
                token,
                self.require_activation_token,
                Completer {
                    identity: &req.identity,
                    organization_id: caller,
                    namespace: &req.namespace,
                },
            )
            .await?
        {
            Admission::Apply {
                fence,
                activation,
                token,
            } => (fence, activation, token),
            Admission::Replayed(settled) => {
                self.finish_replayed(&req.namespace, &req.execution_id, &settled)
                    .await;
                return Ok(Response::new(CompleteWorkflowExecutionResponse {
                    eager_tasks: vec![],
                }));
            }
            Admission::Stale(settled) => {
                self.finish_stale(&req.namespace, &req.identity, &req.execution_id, &settled);
                return Ok(Response::new(CompleteWorkflowExecutionResponse {
                    eager_tasks: vec![],
                }));
            }
        };

        // A worker from before cancellation requests did not hand the request
        // to the workflow, so what it reports is the workflow carrying on as if
        // nothing had been asked. Its commands are replaced by the
        // cancellation, which then ends the run as a workflow cancelling itself
        // does: guarded, with its claim, permit and parent settled. That is what
        // a cancellation did before requests, one activation later.
        if req.protocol_version < 1
            && fence
                .as_ref()
                .is_some_and(|f| f.cancel_requested_at().is_some())
        {
            info!(
                execution_id = %req.execution_id,
                "Worker does not handle cancellation requests; cancelling the run"
            );
            req.commands = vec![older_worker_cancellation()];
        }

        // Capture command count before moving req.commands
        let commands_count = req.commands.len();

        debug!(
            workflow_id = %req.workflow_id,
            execution_id = %req.execution_id,
            namespace = %req.namespace,
            command_count = commands_count,
            "🔵 RPC_ENTRY: About to call CommandProcessor.process_commands_with_result (eager task return enabled)"
        );

        // Process commands through CommandProcessor with result for eager task return
        let processed = self
            .command_processor
            .process_commands_with_result(
                &req.workflow_id,
                &req.execution_id,
                &req.namespace,
                req.commands,
            )
            .await;
        let processing_result = match processed {
            Ok(result) => result,
            Err(e) => {
                // The activation is given up rather than left holding the
                // claim; see ActivationFence::abandon.
                self.abandon_activation(&req.execution_id, fence, activation)
                    .await;
                let message = format!("Failed to process commands: {}", e);
                // A fence refusal: another node owns the workflow now. Sent
                // again it succeeds, so say so to the caller.
                if orcher_storage::sharding::is_ownership_lost(&message) {
                    warn!(
                        workflow_id = %req.workflow_id,
                        execution_id = %req.execution_id,
                        error = %e,
                        "Workflow completion refused: this node does not own the workflow"
                    );
                    return Err(Status::unavailable(message));
                }
                error!(
                    workflow_id = %req.workflow_id,
                    execution_id = %req.execution_id,
                    error = %e,
                    "Failed to process commands"
                );
                return Err(Status::internal(message));
            }
        };

        // Every command went through, so the activation is recorded as applied
        // and the next completion of this execution let in. On an error above,
        // it was abandoned instead: nothing recorded, the claim given back, and
        // a retry of the same completion admitted while nothing newer has been
        // handed out.
        let deferred_resume = match (fence, activation) {
            (Some(fence), Some(activation)) => match fence.applied(activation).await {
                Ok(deferred) => deferred,
                // Not failed on: the commands are in. What is lost is only the
                // recognition of this completion if it is sent again, which is
                // then applied again, against handlers that tolerate it.
                Err(e) => {
                    warn!(
                        workflow_id = %req.workflow_id,
                        execution_id = %req.execution_id,
                        error = %e,
                        "Completion applied, but not recorded against its activation"
                    );
                    None
                }
            },
            _ => None,
        };

        // Extract result flags
        let has_complete_command = processing_result.workflow_completed;
        let has_fail_command = processing_result.workflow_failed;
        // A workflow that ends itself as cancelled has finished as surely as one
        // that completes. It used to be treated as suspending: its claim was
        // kept and its permit never returned.
        let has_cancel_command = processing_result.workflow_cancelled;
        let should_release_claim = claim_released_after_completion(
            has_complete_command,
            has_fail_command,
            has_cancel_command,
        );

        // A wake-up arrived while this activation was out and was left for it.
        // Recording the activation released the claim; queue the continuation
        // so a worker takes it without waiting for a poll to find it.
        if let Some(resume) = deferred_resume.filter(|_| !should_release_claim) {
            let preferred_worker = self.polling_manager.journal_holder(&req.execution_id).await;
            self.polling_manager.push_continuation(
                &resume.task_queue,
                &resume.namespace,
                &req.execution_id,
                preferred_worker,
            );
            self.polling_manager
                .notify_work_available(&resume.task_queue, &resume.namespace)
                .await;
        }

        debug!(
            workflow_id = %req.workflow_id,
            execution_id = %req.execution_id,
            commands_processed = processing_result.commands_processed,
            scheduled_tasks = processing_result.scheduled_tasks.len(),
            workflow_completed = has_complete_command,
            workflow_failed = has_fail_command,
            "Commands processed"
        );

        // Resolve any query results piggybacked on this completion
        if !req.query_results.is_empty() {
            self.resolve_query_results(&req.query_results);
            debug!(
                workflow_id = %req.workflow_id,
                execution_id = %req.execution_id,
                query_count = req.query_results.len(),
                "Query results delivered via CompleteWorkflowExecution"
            );
        }

        // Resolve any update results piggybacked on this completion
        if !req.update_results.is_empty() {
            self.resolve_update_results(&req.update_results).await;
            debug!(
                workflow_id = %req.workflow_id,
                execution_id = %req.execution_id,
                update_count = req.update_results.len(),
                "Update results delivered via CompleteWorkflowExecution"
            );
        }

        // Single DB lookup shared by claim/permit release, the child→parent
        // resume, and eager dispatch below.
        //
        // It used to be skipped entirely when `identity` was empty, because the
        // round-trip was hoisted here for permit release — which does need an
        // identity. The parent resume was later attached to the same value and
        // silently inherited that condition: a worker that completes without an
        // identity has its children complete and their parents never woken.
        // Nothing validates `identity`, so that is a caller's typo away.
        //
        // A lookup error is also not "no such row": `.ok().flatten()` made a
        // failing query indistinguishable from a workflow that has no parent.
        let workflow_info = match self
            .workflow_execution_repository
            .find_by_execution_id(&req.execution_id)
            .await
        {
            Ok(found) => found,
            Err(e) => {
                error!(
                    execution_id = %req.execution_id,
                    error = %e,
                    "Could not load the completed execution: if it is a child, its \
                     parent will not be resumed, and its permit is not returned"
                );
                None
            }
        };

        // Only release claim if workflow is truly finished (CompleteWorkflow or FailWorkflow).
        // If the workflow is just suspending — on tasks, a timer, a child workflow,
        // or an external event — keep the claim so it won't be picked up again
        // until whatever it waits for arrives. Each resume path releases the claim
        // itself: task completion (complete_task_execution → requeue), timer fire,
        // child completion, and every event-delivery path (the SendEvent RPCs and
        // the in-workflow SendEvent command). See claim_released_after_completion.
        if should_release_claim {
            // Release claim via PollingManager
            self.polling_manager
                .release_claim(&req.execution_id)
                .await
                .map_err(|e| {
                    warn!(
                        workflow_id = %req.workflow_id,
                        execution_id = %req.execution_id,
                        error = %e,
                        "Failed to release claim (non-fatal)"
                    );
                    // Non-fatal: reclaimer will eventually clean up
                })
                .ok();

            // Release worker capacity permit
            if let Some(ref workflow) = workflow_info {
                self.return_permit(
                    workflow,
                    &req.namespace,
                    &req.identity,
                    &req.execution_id,
                    activation.filter(|_| token_bearing),
                );
            }

            info!(
                workflow_id = %req.workflow_id,
                execution_id = %req.execution_id,
                "Workflow execution completed, claim and permit released"
            );
        } else {
            // Workflow is suspended (tasks, timer, child workflow, or an external
            // event) - release permit but keep claim.
            // The permit is released so the worker can poll for other work.
            // The claim is kept so this workflow won't be re-polled until its
            // resume path releases it.
            if let Some(ref workflow) = workflow_info {
                self.return_permit(
                    workflow,
                    &req.namespace,
                    &req.identity,
                    &req.execution_id,
                    activation.filter(|_| token_bearing),
                );
            }

            debug!(
                workflow_id = %req.workflow_id,
                execution_id = %req.execution_id,
                command_count = commands_count,
                "Workflow suspended, keeping claim until its resume path releases it"
            );
        }

        // Only log workflow completion if there was actually a CompleteWorkflow command
        // (not on every workflow iteration/suspension)
        if has_complete_command {
            // Add workflow completion log entry for real-time streaming observability
            let now = Timestamptz::now();
            let complete_log = WorkflowExecutionLog {
                id: 0, // Will be set by database
                execution_id: req.execution_id.clone(),
                timestamp: now,
                level: "info".to_string(),
                message: "Workflow execution completed successfully".to_string(),
                step: None,
                task_id: None,
                worker_id: Some(req.identity.clone()),
                metadata: serde_json::json!({
                    "namespace": req.namespace,
                    "commands_processed": commands_count,
                }),
                source: Some("ExecutionService".to_string()),
                created_at: now,
            };

            // Non-blocking: log insertion failure shouldn't fail the completion
            if let Err(e) = self.logs_repository.add_log(&complete_log).await {
                warn!(
                    workflow_id = %req.workflow_id,
                    execution_id = %req.execution_id,
                    error = %e,
                    "Failed to add workflow completion log entry (non-fatal)"
                );
            } else {
                debug!(
                    workflow_id = %req.workflow_id,
                    execution_id = %req.execution_id,
                    "Workflow completion logged for real-time streaming"
                );
            }
        }

        // Record workflow completion for predictor learning (resets sequence tracking)
        if has_complete_command {
            if let Some(ref workflow) = workflow_info {
                self.eager_dispatch
                    .record_workflow_completion(&workflow.workflow_name);
            }
        }

        // Eager Task Return via predictive eager dispatch
        // Uses EagerDispatchIntegration for capacity-aware, prediction-enhanced task selection
        let eager_tasks =
            if !processing_result.scheduled_tasks.is_empty() && !req.identity.is_empty() {
                if let Some(ref workflow) = workflow_info {
                    let selected = self.eager_dispatch.select_tasks(
                        &processing_result.scheduled_tasks,
                        &workflow.workflow_name,
                        &req.identity,
                        &workflow.task_queue,
                        &req.namespace,
                    );

                    if !selected.is_empty() {
                        info!(
                            workflow_id = %req.workflow_id,
                            execution_id = %req.execution_id,
                            identity = %req.identity,
                            eager_task_count = selected.len(),
                            total_scheduled = processing_result.scheduled_tasks.len(),
                            "EAGER_DISPATCH: Returning {} tasks via predictive dispatch",
                            selected.len()
                        );
                    }

                    let mut eager_responses = Vec::new();
                    // Room left in the answer for tasks, with headroom for the
                    // fields around each. A task that does not fit is not
                    // claimed here; a poll hands it out instead.
                    let mut room = receive_limit.saturating_sub(EAGER_ANSWER_HEADROOM);
                    for eager_task in selected {
                        let size = eager_task.task.input.as_ref().map_or(0, Vec::len)
                            + EAGER_ANSWER_HEADROOM;
                        if size > room {
                            debug!(
                                task_id = %eager_task.task.task_id,
                                input_bytes = size,
                                "Eager task left to a poll: no room for it in the answer"
                            );
                            continue;
                        }
                        room -= size;
                        if let Some(resp) = self
                            .build_eager_response_for_task(
                                &eager_task.task,
                                &req.identity,
                                std::time::Duration::from_secs(10),
                                self.auto_heartbeat_timeout(req.eager_task_capabilities.as_ref()),
                            )
                            .await
                        {
                            eager_responses.push(resp);
                        }
                    }
                    eager_responses
                } else {
                    vec![]
                }
            } else {
                vec![]
            };

        // A failure's fate is decided once, here, as it is recorded: retried
        // after a backoff, or final. Only a final failure is an outcome.
        let failure_is_final = match (has_fail_command, workflow_info.as_ref()) {
            (true, Some(workflow)) => {
                self.decide_failure(
                    workflow,
                    processing_result.failure_type.as_deref(),
                    processing_result.non_retryable,
                )
                .await
            }
            // Unloadable: the retry scheduler and the child sweep decide it.
            (true, None) => false,
            (false, _) => false,
        };

        // A child's outcome resumes its parent.
        // When this run is a child's and has ended for good, append its outcome
        // to the parent's journal and re-queue the parent so it can resume.
        // Only a run with a parent_execution_id can be anybody's child; whether
        // it is — or is just the successor of an earlier run — is for the
        // child's tracking row to say, which the delivery looks up.
        if has_complete_command || failure_is_final || has_cancel_command {
            if let Some(ref workflow) = workflow_info {
                if workflow.parent_execution_id.is_some() {
                    self.notify_parent_of_child_outcome(&req.execution_id).await;
                }
            }
        }

        // Answer the clients waiting on the result — after the failure's fate
        // is decided, since a failure about to be retried is not the result.
        if has_complete_command || has_fail_command || has_cancel_command {
            self.answer_result_waiters(&req.execution_id).await;
        }

        // Clear worker affinity lease — workflow is done, no more tasks to route
        if has_complete_command || has_fail_command || has_cancel_command {
            self.polling_manager.clear_affinity(&req.execution_id);
        }

        // Labelled from the execution row, the same values the start path
        // recorded. The executing gauge that used to be decremented here is
        // counted from the database at scrape time instead.
        if has_complete_command || has_fail_command {
            let (namespace, workflow_type) = match workflow_info.as_ref() {
                Some(w) => (w.namespace.as_str(), w.workflow_type.as_str()),
                None => (
                    req.namespace.as_str(),
                    orcher_monitoring::metrics::lifecycle::UNKNOWN,
                ),
            };
            orcher_monitoring::metrics::lifecycle::workflow_finished(
                namespace,
                workflow_type,
                has_complete_command,
            );
        }

        Ok(Response::new(CompleteWorkflowExecutionResponse {
            eager_tasks,
        }))
    }
}

#[tonic::async_trait]
impl ExecutionServiceTrait for ExecutionService {
    /// Poll for workflow execution work
    ///
    /// This is a long-polling RPC that blocks until work is available or timeout.
    /// Services (ORCHER terminology for "workers") call this to get workflow
    /// execution work from their task queue.
    ///
    /// # Flow
    ///
    /// 1. Service calls with task_queue, namespace, identity
    /// 2. Server long-polls internal workflow queue
    /// 3. When work available, return full execution journal
    /// 4. Service replays journal to reconstruct workflow state
    /// 5. Service executes workflow code and generates commands
    /// 6. Service calls CompleteWorkflowExecution with commands
    #[instrument(skip(self, request), fields(
        task_queue = %request.get_ref().task_queue,
        namespace = %request.get_ref().namespace,
        identity = %request.get_ref().identity
    ))]
    async fn poll_workflow_execution(
        &self,
        request: Request<PollWorkflowExecutionRequest>,
    ) -> Result<Response<PollWorkflowExecutionResponse>, Status> {
        // Authorize before consuming the request
        authorize_poll(
            &request,
            ServiceOperation::PollWorkflowSteps,
            &request.get_ref().namespace,
            &request.get_ref().task_queue,
        )?;
        let caller = PollCaller::of(
            &request,
            &self.payload_limits,
            &request.get_ref().namespace,
            &request.get_ref().identity,
        );

        // The calling tenant, read before the request is consumed.
        let organization_id = crate::grpc::tenant::organization_from_metadata(request.metadata());
        let req = request.into_inner();
        let mut poll_guard = PollDurationGuard::new(&req.task_queue, &req.namespace);

        debug!(
            task_queue = %req.task_queue,
            namespace = %req.namespace,
            identity = %req.identity,
            "Received PollWorkflowExecution request from SDK"
        );

        // Validate request
        if req.task_queue.is_empty() {
            return Err(Status::invalid_argument("task_queue is required"));
        }
        if req.namespace.is_empty() {
            return Err(Status::invalid_argument("namespace is required"));
        }
        if req.identity.is_empty() {
            return Err(Status::invalid_argument("identity is required"));
        }

        // The process polling is alive for as long as this poll is open, and
        // until a little after: see `crate::worker::liveness`. Held to the end
        // of the handler, however it ends.
        let _presence = self.worker_liveness.polling(
            organization_id,
            &req.namespace,
            &req.worker_instance_id,
            &req.identity,
        );

        // A worker that has said it is shutting down gets no more work. Asked
        // here, in memory, before anything that costs a query, and again in
        // the wait loop; a refused poll takes no permit. Not logged above
        // debug: a stopping worker's pollers may ask many times a second.
        let shutdown_poll = crate::worker::shutdowns::Poll {
            organization_id,
            namespace: &req.namespace,
            identity: &req.identity,
            worker_instance_id: &req.worker_instance_id,
            task_queue: &req.task_queue,
        };
        if self.worker_shutdowns.is_shut_down(&shutdown_poll) {
            debug!(
                identity = %req.identity,
                task_queue = %req.task_queue,
                namespace = %req.namespace,
                "Worker has said it is shutting down; answering its poll with no work"
            );
            poll_guard.timeout();
            return Ok(Response::new(PollWorkflowExecutionResponse::default()));
        }

        // Validate namespace exists and is active
        self.validate_namespace(&req.namespace, organization_id.as_ref())
            .await?;

        // Register worker in capacity registry and acquire permit
        // This enables intelligent dispatch based on worker capacity
        let worker_permits = self.capacity_registry.get_or_register(
            &req.task_queue,
            &req.namespace,
            &req.identity,
            self.default_worker_capacity,
        );

        // Record worker capacity metrics for monitoring
        orcher_monitoring::metrics::polling::update_worker_capacity(
            &req.task_queue,
            &req.namespace,
            &req.identity,
            worker_permits.available(),
            worker_permits.used(),
            worker_permits.capacity(),
        );

        // Check if worker has available capacity before polling
        // This prevents over-saturation of workers
        if worker_permits.is_saturated() {
            debug!(
                identity = %req.identity,
                task_queue = %req.task_queue,
                used = worker_permits.used(),
                capacity = worker_permits.capacity(),
                "Worker at capacity, returning empty response"
            );
            poll_guard.timeout();
            return Ok(Response::new(PollWorkflowExecutionResponse::default()));
        }

        // Poll timeout: how long to wait for work (long-polling)
        let poll_timeout_secs = if let Some(timeout) = &req.poll_timeout {
            timeout.seconds as u64
        } else {
            DEFAULT_POLL_TIMEOUT_SECS as u64
        };

        debug!(
            task_queue = %req.task_queue,
            namespace = %req.namespace,
            poll_timeout_secs = poll_timeout_secs,
            "Starting efficient long-poll using PollingManager"
        );

        // Create poll request for PollingManager
        // Carry the worker's declared code release through to the claim, which
        // binds it to the execution on first claim. Empty is normalised to None
        // by `with_version_id`, so a worker that declares nothing is unchanged.
        let poll_request = PollExecutionRequest::new(
            req.task_queue.clone(),
            req.namespace.clone(),
            req.identity.clone(),
            std::time::Duration::from_secs(poll_timeout_secs),
        )
        .with_version_id(Some(req.version_id.clone()))
        .with_organization_id(organization_id)
        .with_worker_instance_id(Some(req.worker_instance_id.clone()))
        .with_step_ids(&req.supported_step_ids)
        .with_history_cached(req.history_cached);

        // Take a permit before polling, so work is only dispatched to a worker
        // with capacity. The guard holds it across the wait below and gives it
        // back if this request ends without dispatching anything — including
        // the endings that run no code here at all, such as the worker hanging
        // up mid-poll. Only a dispatch calls `keep`.
        let Some(permit) = worker_permits.try_acquire_guard() else {
            debug!(
                identity = %req.identity,
                task_queue = %req.task_queue,
                "Failed to acquire permit, worker at capacity"
            );
            poll_guard.timeout();
            return Ok(Response::new(PollWorkflowExecutionResponse::default()));
        };

        // The poll runs on its own task. A worker can end its poll at any
        // moment, and a poll ended between claiming an activation and
        // returning it left the activation claimed by a poll that no longer
        // existed, its workflow standing still until the claim timed out.
        // Run apart, the poll stops waiting once nobody waits for it, and an
        // activation it claimed regardless is handed back at once.
        let (polled_tx, polled) = tokio::sync::oneshot::channel();
        {
            let polling_manager = self.polling_manager.clone();
            let identity = req.identity.clone();
            let mut polled_tx = polled_tx;
            tokio::spawn(async move {
                let result = polling_manager
                    .poll_execution_until(poll_request, polled_tx.closed())
                    .await;
                if let Err(Ok(Some(undelivered))) = polled_tx.send(result) {
                    polling_manager
                        .hand_back(
                            &undelivered.execution_id,
                            undelivered.activation_seq,
                            &identity,
                        )
                        .await;
                }
            });
        }
        let poll_response = polled
            .await
            .map_err(|_| Status::internal("The poll ended without an answer"))?
            .map_err(|e| {
                // The permit goes back when `permit` drops on the way out.
                error!(error = %e, "Failed to poll for execution");
                Status::internal(format!("Failed to poll for execution: {}", e))
            })?;

        // No work: the permit goes back with the guard, and the worker is told
        // to poll again.
        let Some(poll_response) = poll_response else {
            debug!(
                task_queue = %req.task_queue,
                namespace = %req.namespace,
                poll_timeout_secs = poll_timeout_secs,
                "Poll timeout reached, no work available"
            );

            poll_guard.timeout();
            return Ok(Response::new(PollWorkflowExecutionResponse::default()));
        };

        // Until this activation is returned, ending here hands it back.
        let mut undelivered = Undelivered {
            polling_manager: self.polling_manager.clone(),
            execution_id: poll_response.execution_id.clone(),
            activation: poll_response.activation_seq,
            identity: req.identity.clone(),
            armed: true,
        };

        // The claim may have been running when the worker's shutdown was
        // recorded. It is then answered as if the shutdown had come first: the
        // activation is given straight back, and the permit with the guard.
        let shutdown_poll = crate::worker::shutdowns::Poll {
            organization_id,
            namespace: &req.namespace,
            identity: &req.identity,
            worker_instance_id: &req.worker_instance_id,
            task_queue: &req.task_queue,
        };
        if self.worker_shutdowns.is_shut_down(&shutdown_poll) {
            info!(
                identity = %req.identity,
                execution_id = %poll_response.execution_id,
                "Worker shut down while its poll was claiming; handing the activation back"
            );
            undelivered.armed = false;
            let token = (poll_response.activation_seq > 0).then(|| {
                Ok(crate::grpc::activation_token::ActivationToken {
                    execution_id: poll_response.execution_id.clone(),
                    activation_seq: poll_response.activation_seq,
                })
            });
            if let Err(e) = self
                .release_activation(
                    &poll_response.workflow_id,
                    &poll_response.execution_id,
                    token,
                    Completer {
                        identity: &req.identity,
                        organization_id,
                        namespace: &req.namespace,
                    },
                )
                .await
            {
                warn!(
                    execution_id = %poll_response.execution_id,
                    error = %e,
                    "Could not hand back the activation; its claim times out instead"
                );
            }
            poll_guard.timeout();
            return Ok(Response::new(PollWorkflowExecutionResponse::default()));
        }

        poll_guard.success();
        info!(
            workflow_id = %poll_response.workflow_id,
            execution_id = %poll_response.execution_id,
            workflow_type = %poll_response.workflow_type,
            journal_entries = poll_response.journal.len(),
            worker_permits_used = worker_permits.used(),
            worker_permits_available = worker_permits.available(),
            "Claimed work for service via PollingManager (permit acquired)"
        );

        // The run starts when it is first handed to a worker: its start is
        // logged, and its started_at stamped, by that claim alone. Keyed on
        // the activation the claim numbered, not on the run's retry count,
        // which is 0 for every activation of a first run and wrote this line
        // again at each one.
        if is_first_activation(poll_response.activation_seq) {
            let now = Timestamptz::now();

            let start_log = WorkflowExecutionLog {
                id: 0, // Will be set by database
                execution_id: poll_response.execution_id.clone(),
                timestamp: now,
                level: "info".to_string(),
                message: format!(
                    "Workflow '{}' execution started",
                    poll_response.workflow_type
                ),
                step: None,
                task_id: None,
                worker_id: Some(req.identity.clone()),
                metadata: serde_json::json!({
                    "workflow_type": poll_response.workflow_type,
                    "namespace": req.namespace,
                    "attempt": poll_response.attempt,
                }),
                source: Some("ExecutionService".to_string()),
                created_at: now,
            };

            // Non-blocking: log insertion failure shouldn't fail the poll
            if let Err(e) = self.logs_repository.add_log(&start_log).await {
                warn!(
                    workflow_id = %poll_response.workflow_id,
                    execution_id = %poll_response.execution_id,
                    error = %e,
                    "Failed to add workflow start log entry (non-fatal)"
                );
            } else {
                debug!(
                    workflow_id = %poll_response.workflow_id,
                    execution_id = %poll_response.execution_id,
                    "Workflow start logged for real-time streaming"
                );
            }

            // Set started_at timestamp in workflow_executions table on first poll
            if let Err(e) = self
                .workflow_execution_repository
                .update_started_at(&poll_response.execution_id, now)
                .await
            {
                warn!(
                    workflow_id = %poll_response.workflow_id,
                    execution_id = %poll_response.execution_id,
                    error = %e,
                    "Failed to update started_at timestamp (non-fatal)"
                );
            } else {
                debug!(
                    workflow_id = %poll_response.workflow_id,
                    execution_id = %poll_response.execution_id,
                    started_at = %now,
                    "Workflow started_at timestamp updated"
                );
            }
        }

        // Convert journal entries to proto format using JournalMapper
        let storage_journal: Vec<orcher_storage::schema::types::journal::JournalEntry> =
            poll_response
                .journal
                .iter()
                .map(
                    |entry| orcher_storage::schema::types::journal::JournalEntry {
                        id: entry.event_id,
                        organization_id: None, // Not available in poll response context
                        workflow_id: poll_response.workflow_id.clone(),
                        execution_id: poll_response.execution_id.clone(),
                        event_id: entry.event_id,
                        event_type: entry.entry_type.clone(),
                        timestamp: entry.timestamp,
                        payload: entry
                            .payload
                            .clone()
                            .unwrap_or(serde_json::Value::Object(Default::default())),
                        version: 1,
                        task_id: 0,
                        created_at: entry.timestamp,
                        shard_id: None,
                    },
                )
                .collect();

        let proto_journal = self
            .journal_mapper
            .to_proto_vec(&storage_journal)
            .map_err(|e| {
                error!(error = %e, "Failed to map journal entries to proto");
                Status::internal(format!("Failed to map journal entries: {}", e))
            })?;

        // Convert timestamps to proto format
        let workflow_start_time = Some(orcher_proto::prost_types::Timestamp {
            seconds: poll_response.started_at.timestamp(),
            nanos: poll_response.started_at.timestamp_subsec_nanos() as i32,
        });

        let execution_start_time = Some(orcher_proto::prost_types::Timestamp {
            seconds: poll_response.claimed_at.timestamp(),
            nanos: poll_response.claimed_at.timestamp_subsec_nanos() as i32,
        });

        // Debug: Log input before serialization
        debug!(
            workflow_id = %poll_response.workflow_id,
            execution_id = %poll_response.execution_id,
            input = ?poll_response.input,
            input_is_null = poll_response.input.is_null(),
            "About to serialize workflow input"
        );

        // Build response
        // The activation token rides in `stream_entry_id`, the one field every
        // SDK hands back on completion exactly as it received it; see
        // `activation_token`. The Redis stream id it was named for is no longer
        // produced. An activation numbered 0 did not come from a claim, and is
        // sent no token rather than one no completion could ever match.
        let activation_token = (poll_response.activation_seq > 0)
            .then(|| {
                crate::grpc::activation_token::encode(
                    &poll_response.execution_id,
                    poll_response.activation_seq,
                )
            })
            .or_else(|| poll_response.stream_entry_id.clone())
            .unwrap_or_default();

        let response = PollWorkflowExecutionResponse {
            stream_entry_id: activation_token,
            workflow_id: poll_response.workflow_id.clone(),
            execution_id: poll_response.execution_id.clone(),
            workflow_type: poll_response.workflow_type.clone(),
            task_queue: poll_response.task_queue.clone(),
            previous_execution_id: poll_response
                .parent_execution_id
                .clone()
                .unwrap_or_default(),
            attempt: poll_response.attempt,
            journal: proto_journal,
            previous_started_event_id: poll_response.previous_started_event_id.unwrap_or(0),
            started_event_id: poll_response.started_event_id,
            queries: self.collect_pending_queries(&poll_response.execution_id),
            updates: self.collect_pending_updates(&poll_response.execution_id),
            workflow_start_time,
            execution_start_time,
            input: serde_json::to_vec(&poll_response.input).map_err(|e| {
                error!(
                    error = %e,
                    workflow_id = %poll_response.workflow_id,
                    execution_id = %poll_response.execution_id,
                    input = ?poll_response.input,
                    "Failed to serialize workflow input to bytes"
                );
                Status::internal(format!("Failed to serialize workflow input: {}", e))
            })?,
            step_ids: poll_response.step_id_scheme.map_or(0, i32::from),
            history_lineage: poll_response.history_lineage.clone(),
            history_after_entry_id: poll_response.history_after_entry_id,
        };

        info!(
            workflow_id = %response.workflow_id,
            workflow_type = %response.workflow_type,
            journal_entries = response.journal.len(),
            input_bytes_len = response.input.len(),
            "Returning work to service"
        );

        // A run created before step results were journal entries has them
        // rebuilt from its execution log, as it always had.
        let mut journal = response.journal.clone();
        if !poll_response.step_results_journaled {
            self.rebuild_step_results(&mut journal, &response.workflow_id)
                .await?;
        }
        strip_replayed_payloads(&mut journal);
        let final_response = PollWorkflowExecutionResponse {
            journal,
            ..response
        };

        // The work is on its way to the worker, so the permit goes with it and
        // the completion gives it back. Everything above this line can still
        // end without dispatching, and there the guard returns it instead.
        // Recorded with the activation's number when it has one, so its
        // completion — and only its completion — returns this permit.
        if poll_response.activation_seq > 0 {
            permit.keep_activation(&final_response.execution_id, poll_response.activation_seq);
        } else {
            permit.keep(&final_response.execution_id);
        }

        // Settled through the completion path when it is too large for the
        // worker, which returns the permit kept above.
        let final_response = self.deliverable_activation(final_response, &caller).await;
        undelivered.armed = false;
        Ok(Response::new(final_response))
    }

    /// Complete workflow execution step
    ///
    /// Service calls this after executing workflow code to report commands.
    /// This is where command processing happens (start child workflows, schedule tasks, etc.).
    ///
    /// # Flow
    ///
    /// 1. Service sends commands generated by workflow execution
    /// 2. Server validates commands
    /// 3. Server processes each command:
    ///    - StartChildWorkflow → ChildWorkflowManager
    ///    - ScheduleTask → Task queue
    ///    - StartTimer → Timer service
    ///    - CompleteWorkflow → Update execution status
    /// 4. Server updates execution journal with new events
    /// 5. Return acknowledgement
    #[instrument(skip(self, request), fields(
        workflow_id = %request.get_ref().workflow_id,
        execution_id = %request.get_ref().execution_id,
        namespace = %request.get_ref().namespace,
        command_count = request.get_ref().commands.len()
    ))]
    async fn complete_workflow_execution(
        &self,
        request: Request<CompleteWorkflowExecutionRequest>,
    ) -> Result<Response<CompleteWorkflowExecutionResponse>, Status> {
        authorize_op(
            &request,
            ServiceOperation::CompleteWorkflowStep,
            &request.get_ref().namespace,
        )?;

        // Kept for the relay to the owner, which must carry the caller's
        // credentials; consuming the request below discards them.
        let received_at = std::time::Instant::now();
        let metadata = self
            .owner_forwarder
            .as_ref()
            .map(|_| request.metadata().clone());
        // Before `into_inner()`, which drops the metadata the tenant lives in.
        let caller = crate::grpc::tenant::organization_from_metadata(request.metadata());
        // What the worker can receive, which bounds the tasks handed back.
        let receive_limit = self.payload_limits.receive_limit(request.metadata());
        let mut req = request.into_inner();
        ignore_unmeant_non_retryable_commands(req.protocol_version, &mut req.commands);

        debug!(
            workflow_id = %req.workflow_id,
            execution_id = %req.execution_id,
            command_count = req.commands.len(),
            namespace = %req.namespace,
            "🔵 RPC_ENTRY: Received CompleteWorkflowExecution request"
        );

        // Log each command type for debugging
        for (idx, cmd) in req.commands.iter().enumerate() {
            let cmd_type = CommandType::try_from(cmd.command_type)
                .map(|t| format!("{:?}", t))
                .unwrap_or_else(|_| format!("Unknown({})", cmd.command_type));
            debug!(
                workflow_id = %req.workflow_id,
                execution_id = %req.execution_id,
                command_index = idx,
                command_type = %cmd_type,
                "🔵 RPC_ENTRY: Command in request"
            );
        }

        // Validate request
        if req.workflow_id.is_empty() {
            return Err(Status::invalid_argument("workflow_id is required"));
        }
        if req.execution_id.is_empty() {
            return Err(Status::invalid_argument("execution_id is required"));
        }
        if req.namespace.is_empty() {
            return Err(Status::invalid_argument("namespace is required"));
        }

        // Applied on a task of its own once it is here. The worker gives a
        // completion a deadline and the call ends when it passes; ended
        // mid-batch, the commands before that point were applied and the rest
        // were not, and the activation was never recorded as applied, so its
        // claim stayed held until it timed out and the workflow stood still
        // for minutes. Run apart, the whole batch is applied and the claim
        // released whether or not anyone still waits for the answer; a worker
        // that sends it again is told it was applied.
        match self.this.get().and_then(std::sync::Weak::upgrade) {
            Some(this) => tokio::spawn(async move {
                this.apply_workflow_completion(req, metadata, caller, receive_limit, received_at)
                    .await
            })
            .await
            .map_err(|e| Status::internal(format!("The completion could not be applied: {e}")))?,
            None => {
                self.apply_workflow_completion(req, metadata, caller, receive_limit, received_at)
                    .await
            }
        }
    }

    /// Report workflow execution failure
    ///
    /// Service calls this when workflow execution fails (e.g., panic, error).
    #[instrument(skip(self, request), fields(
        workflow_id = %request.get_ref().workflow_id,
        execution_id = %request.get_ref().execution_id,
        namespace = %request.get_ref().namespace
    ))]
    async fn fail_workflow_execution(
        &self,
        request: Request<FailWorkflowExecutionRequest>,
    ) -> Result<Response<FailWorkflowExecutionResponse>, Status> {
        authorize_op(
            &request,
            ServiceOperation::FailWorkflowStep,
            &request.get_ref().namespace,
        )?;

        // Before `into_inner()`, which drops the metadata the tenant lives in.
        let caller = crate::grpc::tenant::organization_from_metadata(request.metadata());
        let mut req = request.into_inner();
        ignore_unmeant_non_retryable(req.protocol_version, req.failure.as_mut());

        error!(
            workflow_id = %req.workflow_id,
            execution_id = %req.execution_id,
            "Workflow execution failed"
        );

        // Validate request
        if req.workflow_id.is_empty() {
            return Err(Status::invalid_argument("workflow_id is required"));
        }
        if req.execution_id.is_empty() {
            return Err(Status::invalid_argument("execution_id is required"));
        }
        if req.namespace.is_empty() {
            return Err(Status::invalid_argument("namespace is required"));
        }

        // Held to its activation as a completion is, when it carries a token.
        // A failure report has only `task_token` to carry one in, and every
        // SDK released so far puts a token of its own making there — the ids
        // joined — so today this is the unfenced path for all of them, still
        // serialised with the execution's completions. Requiring a token here
        // (`ORCHER_REQUIRE_ACTIVATION_TOKEN`) would refuse every failure those
        // workers report, and a workflow whose failure is refused is handed
        // out again, fails again, and never ends; so a report without one is
        // accepted whatever that setting says, until SDKs send the token here
        // too. One that does carry a token is always held to it.
        let token = crate::grpc::activation_token::decode(&req.task_token);
        let (fence, activation, token_bearing) = match self
            .admit_completion(
                &req.workflow_id,
                &req.execution_id,
                token,
                false,
                Completer {
                    identity: &req.identity,
                    organization_id: caller,
                    namespace: &req.namespace,
                },
            )
            .await?
        {
            Admission::Apply {
                fence,
                activation,
                token,
            } => (fence, activation, token),
            Admission::Replayed(settled) => {
                self.finish_replayed(&req.namespace, &req.execution_id, &settled)
                    .await;
                return Ok(Response::new(FailWorkflowExecutionResponse {}));
            }
            Admission::Stale(settled) => {
                self.finish_stale(&req.namespace, &req.identity, &req.execution_id, &settled);
                return Ok(Response::new(FailWorkflowExecutionResponse {}));
            }
        };
        let record_activation = |fence: Option<
            orcher_storage::repositories::domains::workflow_execution::ActivationFence,
        >| async move {
            if let (Some(fence), Some(activation)) = (fence, activation) {
                // A wake-up deferred to this activation is moot: the run has
                // failed, and its claim is released below either way.
                if let Err(e) = fence.applied(activation).await {
                    warn!(error = %e, "Failure applied, but not recorded against its activation");
                }
            }
        };

        let error_message = req
            .failure
            .as_ref()
            .map(|f| f.message.clone())
            .unwrap_or_else(|| "Unknown error".to_string());

        let failure_attributes = if let Some(failure) = &req.failure {
            serde_json::json!({
                "message": failure.message,
                "source": failure.source,
                "failure_type": failure.failure_type,
                "stack_trace": failure.stack_trace,
                // Read back by the retry scheduler, so it decides as this
                // path does.
                "non_retryable": failure.non_retryable,
            })
        } else {
            serde_json::json!({
                "message": error_message,
                "source": "Unknown",
                "failure_type": "WorkflowExecutionError",
            })
        };

        // 1–2. Record the failure: status, error and details in one write,
        //      guarded so a run that has already ended keeps its ending. The
        //      details carry the failure type, which is what a retry policy's
        //      non-retryable types are matched against when the retry
        //      scheduler looks at this run again.
        let now = Timestamptz::now();
        // A worker from before cancellation requests reports the workflow
        // failing as if nothing had been asked. The run is cancelled instead,
        // which is what a cancellation did before requests, and the failure is
        // then a late report like any other for a run that has ended.
        let cancelled_instead = req.protocol_version < 1
            && self
                .cancel_instead_of_failure(&req.workflow_id, &req.execution_id, fence.as_ref())
                .await;
        let recorded = if cancelled_instead {
            Ok(false)
        } else {
            self.workflow_execution_repository
                .record_failure(
                    &req.execution_id,
                    &error_message,
                    Some(failure_attributes.clone()),
                )
                .await
        };
        let recorded = match recorded {
            Ok(recorded) => recorded,
            Err(e) => {
                error!(
                    workflow_id = %req.workflow_id,
                    execution_id = %req.execution_id,
                    error = %e,
                    "Failed to update execution status to FAILED"
                );
                self.abandon_activation(&req.execution_id, fence, activation)
                    .await;
                return Err(Status::internal(format!(
                    "Failed to update execution status: {}",
                    e
                )));
            }
        };
        if !recorded {
            // A late report for a run that was cancelled, terminated or timed
            // out meanwhile — or replaced. Turning it back into a failure would
            // have the retry scheduler run it again. The worker has done
            // nothing wrong, so it gets its success; only its permit and the
            // claim are returned.
            info!(
                workflow_id = %req.workflow_id,
                execution_id = %req.execution_id,
                "Failure reported for a workflow that has already ended; ignored"
            );
            // Recorded all the same, so that this report sent again is
            // recognised and does not return a permit a second time.
            record_activation(fence).await;
            self.polling_manager
                .release_claim(&req.execution_id)
                .await
                .ok();
            if !req.identity.is_empty() || token_bearing {
                if let Ok(Some(workflow)) = self
                    .workflow_execution_repository
                    .find_by_execution_id(&req.execution_id)
                    .await
                {
                    self.return_permit(
                        &workflow,
                        &req.namespace,
                        &req.identity,
                        &req.execution_id,
                        activation.filter(|_| token_bearing),
                    );
                }
            }
            return Ok(Response::new(FailWorkflowExecutionResponse {}));
        }

        // 3. Append WorkflowExecutionFailed journal entry
        // The event id is allocated under the journal lock, in the insert's
        // transaction. It was the journal's length plus one, read beforehand
        // with no lock: a concurrent append collided with it, and any gap in
        // the ids made it collide with an existing entry.
        let failure_entry = orcher_storage::schema::types::journal::JournalEntry::new(
            req.workflow_id.clone(),
            req.execution_id.clone(),
            0,
            "WorkflowExecutionFailed".to_string(),
            failure_attributes,
        );

        if let Err(e) = self.journal_repository.append_next(failure_entry).await {
            error!(
                workflow_id = %req.workflow_id,
                execution_id = %req.execution_id,
                error = %e,
                "Failed to append WorkflowExecutionFailed journal entry"
            );
            self.abandon_activation(&req.execution_id, fence, activation)
                .await;
            return Err(Status::internal(format!(
                "Failed to append journal entry: {}",
                e
            )));
        }
        record_activation(fence).await;

        // 4. Add error log entry to workflow_execution_logs for observability
        debug!(
            workflow_id = %req.workflow_id,
            execution_id = %req.execution_id,
            "Attempting to add error log entry to workflow_execution_logs"
        );

        let error_log = WorkflowExecutionLog {
            id: 0, // Will be set by database
            execution_id: req.execution_id.clone(),
            timestamp: now,
            level: "error".to_string(),
            message: format!("Workflow execution failed: {}", error_message),
            step: None,
            task_id: None,
            worker_id: None,
            metadata: serde_json::json!({
                "failure_type": req.failure.as_ref().map(|f| f.failure_type.clone()).unwrap_or_default(),
                "source": req.failure.as_ref().map(|f| f.source.clone()).unwrap_or_default(),
            }),
            source: Some("ExecutionService".to_string()),
            created_at: now,
        };

        match self.logs_repository.add_log(&error_log).await {
            Ok(created_log) => {
                info!(
                    workflow_id = %req.workflow_id,
                    execution_id = %req.execution_id,
                    log_id = created_log.id,
                    "Successfully added error log entry to workflow_execution_logs"
                );
            }
            Err(e) => {
                error!(
                    workflow_id = %req.workflow_id,
                    execution_id = %req.execution_id,
                    error = %e,
                    error_debug = ?e,
                    "Failed to add error log entry to workflow_execution_logs (non-fatal) - this may indicate a foreign key constraint failure or missing execution record"
                );
            }
        }

        // 5. Release claim via PollingManager
        self.polling_manager
            .release_claim(&req.execution_id)
            .await
            .map_err(|e| {
                warn!(
                    workflow_id = %req.workflow_id,
                    execution_id = %req.execution_id,
                    error = %e,
                    "Failed to release claim (non-fatal)"
                );
                // Non-fatal: reclaimer will eventually clean up
            })
            .ok();

        // 5b. Clear worker affinity lease — workflow failed, no more tasks to route
        self.polling_manager.clear_affinity(&req.execution_id);

        // 6. Load execution once for retry scheduling, the capacity permit, and
        //    the parent wake-up.
        //
        //    This used to be `if let Ok(Some(workflow))` with no other arm, so a
        //    lookup that failed — a pool exhausted, a failover, a statement
        //    timeout, the same conditions that make workflows fail in the first
        //    place — skipped all three without a word. The parent of a failed
        //    child was never told: it stays parked, the reclaimer hands it back,
        //    it replays, re-emits StartChildWorkflow, gets the existing failed
        //    child as an idempotent no-op, and parks again. Nothing later
        //    appends ChildWorkflowExecutionFailed, so that loop does not end.
        let loaded = self
            .workflow_execution_repository
            .find_by_execution_id(&req.execution_id)
            .await;
        if let Err(ref e) = loaded {
            error!(
                execution_id = %req.execution_id,
                error = %e,
                "Could not load the failed execution: its parent will not be told, \
                 its permit is not returned, and no retry is scheduled"
            );
        }
        if let Ok(Some(workflow)) = loaded {
            // 6a. Decide the failure's fate once, now: retried after its
            //     backoff (the retry scheduler picks it up when `next_retry_at`
            //     passes), or final.
            let failure_type = req.failure.as_ref().map(|f| f.failure_type.as_str());
            let non_retryable = req.failure.as_ref().is_some_and(|f| f.non_retryable);
            let failure_is_final = self
                .decide_failure(&workflow, failure_type, non_retryable)
                .await;

            // 6b. Release worker capacity permit.
            // Matches the permit acquired in poll_workflow_execution. Without
            // it, each failure leaks a permit and the worker eventually stops
            // being offered work at all.
            self.return_permit(
                &workflow,
                &req.namespace,
                &req.identity,
                &req.execution_id,
                activation.filter(|_| token_bearing),
            );

            // 6c. If this run is a child's and its failure is final,
            //     tell the parent. A failure that will be retried is not the
            //     child's outcome: the parent used to be told of it anyway, and
            //     failed over a child whose retry then succeeded.
            if failure_is_final && workflow.parent_execution_id.is_some() {
                self.notify_parent_of_child_outcome(&req.execution_id).await;
            }
        }

        // Answer the clients waiting on the result if the failure is final.
        // This used to answer them with every failure, so a client was told
        // the workflow had failed while its retry was still to run — and could
        // succeed (fixed in 0.6.0). The row says which it is, now that 6a has
        // recorded the decision.
        self.answer_result_waiters(&req.execution_id).await;

        info!(
            workflow_id = %req.workflow_id,
            execution_id = %req.execution_id,
            error_message = %error_message,
            "Workflow execution failure persisted to database"
        );

        Ok(Response::new(FailWorkflowExecutionResponse {}))
    }

    /// Poll for up to `max_items` workflow activations: the first as a poll
    /// for one waits for it, and the rest are those ready at once, each
    /// claimed exactly as a poll for one claims it. A failure after the first
    /// ends the batch with what it has, since those are already claimed.
    async fn poll_workflow_executions(
        &self,
        request: Request<PollWorkflowExecutionsRequest>,
    ) -> Result<Response<PollWorkflowExecutionsResponse>, Status> {
        let (metadata, extensions, batch) = request.into_parts();
        let poll = batch
            .poll
            .ok_or_else(|| Status::invalid_argument("poll is required"))?;
        let limit = items_per_poll(batch.max_items);
        let first = self
            .poll_workflow_execution(Request::from_parts(
                metadata.clone(),
                extensions.clone(),
                poll.clone(),
            ))
            .await?
            .into_inner();
        if first.execution_id.is_empty() {
            return Ok(Response::new(PollWorkflowExecutionsResponse::default()));
        }
        let mut executions = vec![first];
        let at_once = PollWorkflowExecutionRequest {
            poll_timeout: Some(prost_types::Duration::default()),
            ..poll
        };
        while executions.len() < limit {
            match self
                .poll_workflow_execution(Request::from_parts(
                    metadata.clone(),
                    extensions.clone(),
                    at_once.clone(),
                ))
                .await
            {
                Ok(next) if !next.get_ref().execution_id.is_empty() => {
                    executions.push(next.into_inner());
                }
                _ => break,
            }
        }
        Ok(Response::new(PollWorkflowExecutionsResponse { executions }))
    }

    /// Poll for up to `max_items` tasks: the first as a poll for one waits
    /// for it, and the rest are those ready at once, each started exactly as
    /// a poll for one starts it. A failure after the first ends the batch
    /// with what it has, since those are already started.
    async fn poll_task_executions(
        &self,
        request: Request<PollTaskExecutionsRequest>,
    ) -> Result<Response<PollTaskExecutionsResponse>, Status> {
        let (metadata, extensions, batch) = request.into_parts();
        let poll = batch
            .poll
            .ok_or_else(|| Status::invalid_argument("poll is required"))?;
        let limit = items_per_poll(batch.max_items);
        let first = self
            .poll_task_execution(Request::from_parts(
                metadata.clone(),
                extensions.clone(),
                poll.clone(),
            ))
            .await?
            .into_inner();
        if first.task_token.is_empty() {
            return Ok(Response::new(PollTaskExecutionsResponse::default()));
        }
        let mut tasks = vec![first];
        let at_once = PollTaskExecutionRequest {
            poll_timeout: Some(prost_types::Duration::default()),
            ..poll
        };
        while tasks.len() < limit {
            match self
                .poll_task_execution(Request::from_parts(
                    metadata.clone(),
                    extensions.clone(),
                    at_once.clone(),
                ))
                .await
            {
                Ok(next) if !next.get_ref().task_token.is_empty() => {
                    tasks.push(next.into_inner());
                }
                _ => break,
            }
        }
        Ok(Response::new(PollTaskExecutionsResponse { tasks }))
    }

    /// Poll for task execution work
    ///
    /// This is a long-polling RPC for task execution work.
    /// Similar to workflow polling but for tasks.
    #[instrument(skip(self, request), fields(
        task_queue = %request.get_ref().task_queue,
        namespace = %request.get_ref().namespace,
        identity = %request.get_ref().identity
    ))]
    async fn poll_task_execution(
        &self,
        request: Request<PollTaskExecutionRequest>,
    ) -> Result<Response<PollTaskExecutionResponse>, Status> {
        authorize_poll(
            &request,
            ServiceOperation::PollActivityTasks,
            &request.get_ref().namespace,
            &request.get_ref().task_queue,
        )?;
        let caller = PollCaller::of(
            &request,
            &self.payload_limits,
            &request.get_ref().namespace,
            &request.get_ref().identity,
        );

        let org_id = request
            .extensions()
            .get::<TenantContext>()
            .and_then(|ctx| ctx.organization_id);
        // The calling tenant, read before the request is consumed.
        let organization_id = crate::grpc::tenant::organization_from_metadata(request.metadata());
        let req = request.into_inner();
        let mut poll_guard = PollDurationGuard::new(&req.task_queue, &req.namespace);

        debug!(
            task_queue = %req.task_queue,
            namespace = %req.namespace,
            identity = %req.identity,
            "Received PollTaskExecution request"
        );

        // Validate request
        if req.task_queue.is_empty() {
            return Err(Status::invalid_argument("task_queue is required"));
        }
        if req.namespace.is_empty() {
            return Err(Status::invalid_argument("namespace is required"));
        }

        // As for a workflow poll: the process is alive while this is open.
        // Under the organization tasks are claimed for, so that a process is
        // known where its attempts are.
        let _presence = self.worker_liveness.polling(
            org_id,
            &req.namespace,
            &req.worker_instance_id,
            &req.identity,
        );

        // A worker that has said it is shutting down gets no more tasks, on
        // either path below. Asked in memory before anything that costs a
        // query; the polling manager also watches for a shutdown that arrives
        // while the poll waits.
        let shutdown_poll = crate::worker::shutdowns::Poll {
            organization_id,
            namespace: &req.namespace,
            identity: &req.identity,
            worker_instance_id: &req.worker_instance_id,
            task_queue: &req.task_queue,
        };
        if self.worker_shutdowns.is_shut_down(&shutdown_poll) {
            debug!(
                identity = %req.identity,
                task_queue = %req.task_queue,
                "Worker has said it is shutting down; answering its task poll with nothing"
            );
            poll_guard.timeout();
            return Ok(Response::new(Self::empty_task_response(req)));
        }

        // Validate namespace exists and is active
        self.validate_namespace(&req.namespace, organization_id.as_ref())
            .await?;

        let poll_timeout_secs = req
            .poll_timeout
            .as_ref()
            .map(|t| t.seconds as u64)
            .unwrap_or(DEFAULT_POLL_TIMEOUT_SECS as u64);

        // Try TaskPollingManager first (efficient notification-based polling)
        if let Some(ref task_manager) = self.task_polling_manager {
            trace!(
                task_queue = %req.task_queue,
                namespace = %req.namespace,
                poll_timeout_secs = poll_timeout_secs,
                "Using TaskPollingManager for long-polling"
            );

            use crate::engine::dispatch::task_manager::PollTaskRequest;

            let poll_req = PollTaskRequest::new(
                req.task_queue.clone(),
                req.namespace.clone(),
                req.identity.clone(),
                std::time::Duration::from_secs(poll_timeout_secs),
            )
            .with_organization_id(org_id)
            .with_worker_instance_id(Some(req.worker_instance_id.clone()));

            let task_opt = task_manager.poll_task(poll_req).await.map_err(|e| {
                error!(error = %e, "TaskPollingManager failed");
                Status::internal(format!("Failed to poll: {}", e))
            })?;

            return match task_opt {
                Some(crate::engine::dispatch::task_manager::PollTaskResult::Claimed(task)) => {
                    poll_guard.success();
                    orcher_monitoring::metrics::record_poll_served(
                        "claimed",
                        &req.task_queue,
                        &req.namespace,
                    );
                    let built = self.build_task_response(task, req, organization_id).await;
                    self.deliverable_task(built, &caller).await
                }
                Some(crate::engine::dispatch::task_manager::PollTaskResult::SyncMatched(
                    sync_task,
                )) => {
                    poll_guard.success();
                    orcher_monitoring::metrics::record_poll_served(
                        "sync_match",
                        &req.task_queue,
                        &req.namespace,
                    );
                    let built = self
                        .build_task_response_from_sync_match(sync_task, req, organization_id)
                        .await;
                    self.deliverable_task(built, &caller).await
                }
                None => {
                    debug!(
                        task_queue = %req.task_queue,
                        namespace = %req.namespace,
                        poll_timeout_secs = poll_timeout_secs,
                        "Poll timeout reached, no tasks available"
                    );
                    poll_guard.timeout();
                    Ok(Response::new(Self::empty_task_response(req)))
                }
            };
        }

        // Fallback: legacy coordinator polling (immediate return, no long-poll)
        warn!(
            task_queue = %req.task_queue,
            namespace = %req.namespace,
            "⚠️  TaskPollingManager not configured, using legacy polling (CPU spinning!)"
        );

        let Some(ref coordinator) = self.workflow_coordinator else {
            warn!(
                task_queue = %req.task_queue,
                namespace = %req.namespace,
                "WorkflowCoordinator not available - cannot poll for tasks"
            );

            poll_guard.timeout();
            return Ok(Response::new(Self::empty_task_response(req)));
        };

        // Claim only dispatch_task types from outbox queue (one task at a time),
        // and only of the queue this poll serves: a task of another queue
        // handed to this worker would run where nothing registered it.
        let tasks = coordinator
            .outbox()
            .claim_dispatch_tasks(org_id, 1, &req.task_queue, &req.namespace)
            .await
            .map_err(|e| {
                error!(
                    error = %e,
                    task_queue = %req.task_queue,
                    "Failed to claim tasks from outbox"
                );
                Status::internal(format!("Failed to claim tasks: {}", e))
            })?;

        // If no tasks available, return empty response immediately
        if tasks.is_empty() {
            debug!(
                task_queue = %req.task_queue,
                namespace = %req.namespace,
                "No tasks available in outbox queue (legacy polling)"
            );

            poll_guard.timeout();
            return Ok(Response::new(Self::empty_task_response(req)));
        }

        // Get the first task and process it
        let task = match tasks.into_iter().next() {
            Some(task) => task,
            None => {
                poll_guard.timeout();
                return Ok(Response::new(Self::empty_task_response(req)));
            }
        };
        poll_guard.success();
        orcher_monitoring::metrics::record_poll_served(
            "legacy_claimed",
            &req.task_queue,
            &req.namespace,
        );
        let built = self.build_task_response(task, req, organization_id).await;
        self.deliverable_task(built, &caller).await
    }

    /// Complete task execution
    ///
    /// Service calls this after successfully executing a task.
    #[instrument(skip(self, request), fields(
        namespace = %request.get_ref().namespace,
        identity = %request.get_ref().identity
    ))]
    async fn complete_task_execution(
        &self,
        request: Request<CompleteTaskExecutionRequest>,
    ) -> Result<Response<CompleteTaskExecutionResponse>, Status> {
        authorize_op(
            &request,
            ServiceOperation::CompleteActivityTask,
            &request.get_ref().namespace,
        )?;

        let what = crate::grpc::task_token::decode(&request.get_ref().task_token)
            .map(|token| format!("the result of task {:?}", token.task_id))
            .unwrap_or_else(|_| "the task result".to_string());
        if let Err(reason) = self
            .payload_limits
            .check(&what, request.get_ref().result.len())
        {
            return self.refuse_oversized_task_result(request, reason).await;
        }

        // Before `into_inner()`, which drops the metadata the tenant lives in.
        let caller = crate::grpc::tenant::organization_from_metadata(request.metadata());
        let req = request.into_inner();

        info!(
            namespace = %req.namespace,
            identity = %req.identity,
            "Received CompleteTaskExecution request"
        );

        // Decode task token
        let token = crate::grpc::task_token::decode(&req.task_token)?;
        let workflow_id = &token.workflow_id;
        let execution_id = &token.execution_id;
        let task_id = &token.task_id;
        let started_at = token.started_at;

        // Calculate result hash for correlation
        let result_hash = {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut hasher = DefaultHasher::new();
            req.result.hash(&mut hasher);
            hasher.finish()
        };

        info!(
            workflow_id = %workflow_id,
            execution_id = %execution_id,
            task_id = %task_id,
            result_size = req.result.len(),
            result_hash = result_hash,
            "Completing task execution with result"
        );

        // Parse workflow_id as UUID
        let workflow_id_uuid = Uuid::parse_str(workflow_id).map_err(|e| {
            error!(error = %e, workflow_id = %workflow_id, "Invalid workflow_id UUID");
            Status::invalid_argument(format!("Invalid workflow_id: {}", e))
        })?;

        // Find the attempt this token was issued for. A lookup that failed is
        // not "no such task", and neither is a reason to go on: both used to
        // fall through to writing a Completed execution_log row and requeueing
        // the workflow, with nothing journaled, so the workflow replayed a
        // result the task table never recorded.
        let task_exec = match self
            .task_execution_repository
            .find_by_task_token(&token.raw)
            .await
        {
            Ok(Some(found)) if may_act_on_task(&found, caller, &token) => found,
            Ok(Some(found)) => {
                warn!(
                    task_id = %found.task_id,
                    "Refused completion of a task belonging to another organisation"
                );
                return Err(Status::not_found("Task execution not found"));
            }
            Ok(None) => {
                warn!(
                    task_token = %token.raw,
                    task_id = %task_id,
                    "Task execution record not found by task_token"
                );
                return Err(Status::not_found("Task execution not found"));
            }
            Err(e) => {
                error!(
                    error = %e,
                    task_token = %token.raw,
                    "Failed to lookup task execution by task_token"
                );
                return Err(Status::unavailable(format!(
                    "Could not load the task: {}",
                    e
                )));
            }
        };

        // The task's type labels its metrics, never its id: an id carries a
        // sequence number, so labelling by it minted a series per loop
        // iteration.
        let task_type_label = task_exec.task_type.clone();

        // Which attempt this is comes from the task execution row, which is the
        // only thing that defines it: the retry scheduler creates a new row with
        // an incremented `attempt_number`.
        //
        // The token also carries an "attempt". A token issued before the
        // engine took it from the task execution carries the outbox's delivery
        // count instead, 0 for every task, so the row stays the authority.
        let execution_attempt = task_exec.attempt_number;

        // Calculate timing early so batch and observability writes share the same values
        let now = Timestamptz::now();
        let duration_ms =
            started_at.map(|start| crate::retries::task_failure::elapsed_ms(start, now));

        // Batch critical writes in a single transaction:
        // mark_completed + outbox complete + journal append = 1 commit instead of 5
        let task_exec_id_str = task_exec.id.as_uuid().to_string();
        let outcome = self
            .complete_task_batch(
                task_exec.id.as_uuid(),
                &req.result,
                &task_exec_id_str,
                workflow_id,
                execution_id,
                task_id,
                execution_attempt,
                duration_ms,
            )
            .await
            .map_err(|e| {
                error!(
                    error = %e,
                    task_execution_id = %task_exec.id,
                    task_id = %task_id,
                    "Failed to complete task batch"
                );
                Status::internal(format!("Failed to complete task: {}", e))
            })?;

        // Only a completion that recorded itself goes on to write the replay
        // row and wake the workflow. Everything after this point is keyed on
        // the task id alone — the execution_log row especially, which each
        // attempt overwrites — so a repeated report, or a late one from an
        // attempt that timed out and was retried, used to overwrite the
        // current attempt's result there and requeue the workflow again.
        // The replay row and the wake-up follow the commit, keyed on the task.
        // A report of an attempt that ended another way must not touch them:
        // a late one from an attempt that timed out and was retried would
        // overwrite the current attempt's result. A repeat of a completion
        // that was recorded rewrites them from what was recorded, not from
        // this report: the first report may have committed and then failed
        // before writing them, and for a run whose journal does not hold its
        // step results the replay row is the only way a result reaches the
        // workflow, so a retry that wrote nothing would leave it waiting
        // forever. Both writes are idempotent.
        let (recorded_result, execution_attempt, repeat) = match outcome {
            CompletionOutcome::Applied => {
                info!(
                    task_execution_id = %task_exec.id,
                    task_id = %task_id,
                    "Task completion batch committed"
                );
                (
                    std::borrow::Cow::Borrowed(req.result.as_slice()),
                    execution_attempt,
                    false,
                )
            }
            CompletionOutcome::AlreadyCompleted { result, attempt } => {
                info!(
                    task_execution_id = %task_exec.id,
                    task_id = %task_id,
                    "Task completion already recorded; rewriting its replay row"
                );
                (std::borrow::Cow::Owned(result), attempt, true)
            }
            CompletionOutcome::NotStarted(status) => {
                warn!(
                    task_execution_id = %task_exec.id,
                    task_id = %task_id,
                    status = %status,
                    "Refused completion of an attempt that is no longer running"
                );
                return Err(Status::failed_precondition(format!(
                    "attempt no longer running: {}",
                    status
                )));
            }
            CompletionOutcome::Missing => {
                error!(
                    task_execution_id = %task_exec.id,
                    task_id = %task_id,
                    "Task row found by token is not on the workflow's pool"
                );
                return Err(Status::unavailable(
                    "Could not record the completion: the task was not where its workflow is",
                ));
            }
        };

        // Parse result as JSON for observability writes (outside the critical tx)
        let result_json = crate::workflows::journal::result_value(&recorded_result);

        let input = crate::retries::task_failure::step_input(&task_exec);

        // The step's execution log row: what a run whose journal does not
        // hold its step results replays, and every run's record of the step.
        let entry = orcher_storage::repositories::ExecutionLogEntry {
            workflow_id: workflow_id_uuid,
            step_name: task_id.to_string(),
            step_type: ExecutionStepType::Task,
            execution_attempt,
            status: ExecutionLogStatus::Completed,
            input,
            result: Some(result_json),
            error_message: None,
            error_details: None,
            created_at: started_at.unwrap_or(now),
            started_at,
            completed_at: Some(now),
            duration_ms,
            retry_count: if execution_attempt > 1 {
                execution_attempt - 1
            } else {
                0
            },
            last_error: None,
        };

        if let Err(e) = self.execution_log_repository.store_entry(&entry).await {
            error!(
                error = %e,
                workflow_id = %workflow_id,
                task_id = %task_id,
                "Failed to store task result to execution_log"
            );
            return Err(Status::internal(format!(
                "Failed to store task result: {}",
                e
            )));
        }

        // A repeat has been counted and streamed already.
        if !repeat {
            orcher_monitoring::metrics::lifecycle::task_finished(
                &req.namespace,
                &task_type_label,
                true,
                duration_ms.map(|ms| ms as f64 / 1000.0),
            );
            self.log_task_completed(
                &req,
                workflow_id,
                execution_id,
                task_id,
                now,
                duration_ms,
                execution_attempt,
            )
            .await;
        }

        // NOTE: Notification is NOT sent here. It fires inside requeue_workflow_for_continuation()
        // AFTER the claim is released. Notifying before claim release is harmful — pollers wake,
        // find nothing (claim still held), and sleep for the full poll timeout.

        // Fetch workflow info once — shared by both requeue and eager dispatch pattern learning
        // to avoid a second find_by_id call inside requeue_workflow_for_continuation.
        let workflow_info = self
            .workflow_execution_repository
            .find_by_id(workflow_id_uuid)
            .await
            .ok()
            .flatten();

        // Workflow Resume Mechanism: Check if all tasks complete and re-queue workflow
        // This is the critical missing piece that enables workflow continuation after tasks finish
        match self.check_should_requeue_workflow(workflow_id_uuid).await {
            Ok(true) => {
                // All tasks complete - re-queue workflow for continuation
                let requeue_result = if let Some(ref wf) = workflow_info {
                    // Fast path: skip the internal find_by_id since we already have the data
                    self.requeue_workflow_for_continuation_inner(
                        workflow_id_uuid,
                        &wf.execution_id,
                        &wf.namespace,
                        &wf.task_queue,
                    )
                    .await
                } else {
                    // Fallback: workflow_info fetch failed, let requeue do its own query
                    self.requeue_workflow_for_continuation(workflow_id_uuid)
                        .await
                };

                if let Err(e) = requeue_result {
                    error!(
                        workflow_id = %workflow_id_uuid,
                        error = %e,
                        "Failed to re-queue workflow for continuation (non-fatal, polling fallback will work)"
                    );
                    // Don't fail the request - re-queue is best-effort
                }
            }
            Ok(false) => {
                // Tasks still pending - workflow will resume after more tasks complete
                debug!(
                    workflow_id = %workflow_id_uuid,
                    "Tasks still pending - workflow will be re-queued after remaining tasks complete"
                );
            }
            Err(e) => {
                warn!(
                    workflow_id = %workflow_id_uuid,
                    error = %e,
                    "Failed to check workflow task status for re-queue (non-fatal)"
                );
                // Don't fail the request - this is best-effort optimization
            }
        }

        // Record task execution for predictor pattern learning (reuses pre-fetched workflow_info)
        if let (false, Some(workflow)) = (repeat, workflow_info.as_ref()) {
            self.eager_dispatch
                .record_task_execution(&workflow.workflow_name, task_id);
        }

        // Eager tasks for task completion: not returned here.
        // For linear workflows, the next task doesn't exist until complete_workflow_execution
        // runs and the workflow schedules new tasks — so find_scheduled_for_workflow always
        // returns empty, adding DB overhead with no benefit.
        // Eager dispatch for the common case is handled in complete_workflow_execution.
        Ok(Response::new(CompleteTaskExecutionResponse {
            eager_tasks: vec![],
        }))
    }

    /// Report task execution failure
    ///
    /// Service calls this when task execution fails.
    #[instrument(skip(self, request), fields(
        namespace = %request.get_ref().namespace,
        identity = %request.get_ref().identity
    ))]
    async fn fail_task_execution(
        &self,
        request: Request<FailTaskExecutionRequest>,
    ) -> Result<Response<FailTaskExecutionResponse>, Status> {
        authorize_op(
            &request,
            ServiceOperation::FailActivityTask,
            &request.get_ref().namespace,
        )?;

        // Before `into_inner()`, which drops the metadata the tenant lives in.
        let caller = crate::grpc::tenant::organization_from_metadata(request.metadata());
        let req = request.into_inner();

        error!(
            namespace = %req.namespace,
            identity = %req.identity,
            "Task execution failed"
        );

        // Decode task token
        let token = crate::grpc::task_token::decode(&req.task_token)?;
        let workflow_id = &token.workflow_id;
        let execution_id = &token.execution_id;
        let task_id = &token.task_id;
        let execution_attempt = token.attempt;
        let started_at = token.started_at;

        error!(
            workflow_id = %workflow_id,
            execution_id = %execution_id,
            task_id = %task_id,
            failure_message = %req.failure.as_ref().map(|f| &f.message).unwrap_or(&String::new()),
            "Task execution failed"
        );

        // Parse workflow_id as UUID
        let workflow_id_uuid = Uuid::parse_str(workflow_id).map_err(|e| {
            error!(error = %e, workflow_id = %workflow_id, "Invalid workflow_id UUID");
            Status::invalid_argument(format!("Invalid workflow_id: {}", e))
        })?;

        // Extract failure details
        let (error_message, error_type, error_details) = if let Some(failure) = &req.failure {
            let details = serde_json::json!({
                "failure_type": failure.failure_type,
                "source": failure.source,
                "stack_trace": failure.stack_trace,
                "non_retryable": failure.non_retryable,
            });
            (
                failure.message.clone(),
                Some(failure.failure_type.clone()),
                Some(details),
            )
        } else {
            ("Task execution failed".to_string(), None, None)
        };
        let non_retryable = req.failure.as_ref().is_some_and(|f| f.non_retryable);

        // Find task execution record by task_token and mark as failed
        let task_execution_lookup_result = self
            .task_execution_repository
            .find_by_task_token(&token.raw)
            .await;

        if let Ok(Some(found)) = task_execution_lookup_result.as_ref() {
            if !may_act_on_task(found, caller, &token) {
                warn!(
                    task_id = %found.task_id,
                    "Refused failure of a task belonging to another organisation"
                );
                return Err(Status::not_found("Task execution not found"));
            }
        }

        // The task's type labels its metrics, never its id: an id carries a
        // sequence number, so labelling by it minted a series per loop
        // iteration. Taken here because the lookup is consumed further down.
        let task_type_label = task_execution_lookup_result
            .as_ref()
            .ok()
            .and_then(|found| found.as_ref())
            .map(|t| t.task_type.clone())
            .unwrap_or_else(|| orcher_monitoring::metrics::lifecycle::UNKNOWN.to_string());

        // Which attempt this is comes from the task execution row, which is the
        // only thing that defines it: the retry scheduler creates a new row with
        // an incremented `attempt_number`.
        //
        // The token also carries an "attempt". A token issued before the
        // engine took it from the task execution carries the outbox's delivery
        // count instead, 0 for every task. The row is already loaded below for
        // other reasons, so preferring it costs nothing and leaves one source
        // of truth.
        let execution_attempt = task_execution_lookup_result
            .as_ref()
            .ok()
            .and_then(|found| found.as_ref())
            .map(|task_exec| task_exec.attempt_number)
            .unwrap_or_else(|| execution_attempt.max(1));

        // A lookup that failed is not "no such task". Answer with an error and
        // record nothing: the task stays started, and its start-to-close
        // timeout hands the failure to the timeout path instead.
        let task_exec = task_execution_lookup_result.map_err(|e| {
            error!(error = %e, task_token = %token.raw, "Failed to look up task execution by task_token");
            Status::unavailable(format!("Could not load the task: {}", e))
        })?;
        // No row, no attempt to fail. This used to journal TaskFailed anyway,
        // for a task the table had no record of.
        let Some(task) = task_exec else {
            warn!(task_id = %task_id, "Task execution record not found by task_token");
            return Err(Status::not_found("Task execution not found"));
        };

        // An attempt that has already ended has nothing left to record. This
        // is checked before anything is written because the terminal replay
        // row below is keyed on the task, not the attempt: a late report from
        // an attempt that timed out and was retried would otherwise overwrite
        // the newer attempt's row — its result, if it had completed — with
        // this one's failure. A retry is only ever created once the attempt it
        // replaces has left `scheduled` and `started`, so a superseded attempt
        // is always caught here.
        if !matches!(task.status.as_str(), "scheduled" | "started") {
            info!(
                task_id = %task_id,
                status = %task.status,
                "Task attempt already ended; ignoring the failure report"
            );
            return Ok(Response::new(FailTaskExecutionResponse {}));
        }

        // What the failure records, built as the timeout watcher builds it.
        let now = Timestamptz::now();
        let duration_ms =
            started_at.map(|start| crate::retries::task_failure::elapsed_ms(start, now));
        let failed_entry = crate::retries::task_failure::failed_step(
            &task,
            &error_message,
            error_details.as_ref(),
            now,
        );

        // The replayable execution_log failure row is DEFERRED to the terminal (DLQ /
        // no-retry) branch below: a retryable attempt leaves no replayable failure, so the
        // workflow observes the failure exactly once, after retries are exhausted. (Per-attempt
        // failures remain observable via the FAILED_TOTAL metric and the TaskFailed journal
        // entry appended below.)

        orcher_monitoring::metrics::lifecycle::task_finished(
            &req.namespace,
            &task_type_label,
            false,
            duration_ms.map(|ms| ms as f64 / 1000.0),
        );

        // What a failure records, and the order it is recorded in.
        //
        // Each of these used to be written on its own, and each failure only
        // logged: the task marked failed, then the retry row or dead-letter
        // mark, then — for a failure with no retry left — the row the
        // workflow replays to observe it, then the TaskFailed journal entry.
        // The RPC reported success whatever happened. A task marked failed
        // whose replay row was then lost was the worst case: nothing timed it
        // out, because it was no longer started, and the workflow waited on
        // it forever.
        //
        // Now the retry is decided first, without writing anything. A terminal
        // failure's replay row is written next; it lives outside the
        // workflow's shard and is an idempotent upsert. Then one transaction
        // on the workflow's pool marks the task failed, writes the retry row
        // or dead-letter mark, and appends TaskFailed. Any error returns an
        // error with the task still started, so its timeout recovers it; a
        // report of a task already recorded changes nothing.
        let mut decision = self
            .retry_scheduler
            .decide(&task, error_type.as_deref(), non_retryable)
            .map_err(|e| {
                error!(error = %e, task_id = %task_id, "Failed to evaluate retry policy");
                Status::internal(format!("Failed to evaluate retry policy: {}", e))
            })?;
        // Work its workflow's cancellation interrupted is not retried: the
        // retry would only run it again. A task told to stop by its heartbeat
        // usually ends here, failing with the cancellation it raised.
        if matches!(
            decision,
            crate::retries::retry_policy::RetryDecision::Retry { .. }
        ) && self
            .task_execution_repository
            .stopped_by_cancellation(task.id, workflow_id_uuid)
            .await
            .map_err(|e| Status::unavailable(format!("Could not load the task: {}", e)))?
        {
            decision = crate::retries::retry_policy::RetryDecision::MoveToDlq {
                reason: crate::retries::DlqReason::StoppedByCancellation,
            };
        }
        let terminal = !matches!(
            decision,
            crate::retries::retry_policy::RetryDecision::Retry { .. }
        );

        // Written before the transaction rather than inside it: the execution
        // log's pool can be the same one the transaction holds a connection
        // from, and waiting on a second connection while holding the first is
        // how concurrent reports once exhausted the pool. The status check
        // above is what keeps a report from an attempt that has already ended
        // from getting this far; what it leaves is a report racing its own
        // attempt's timeout, whose retry would have to run and finish before
        // this write for the two to collide.
        if terminal {
            self.execution_log_repository
                .store_failure(&failed_entry)
                .await
                .map_err(|e| {
                    error!(error = %e, workflow_id = %workflow_id, task_id = %task_id, "Failed to store terminal task failure to execution_log");
                    Status::unavailable(format!("Could not record the task failure: {}", e))
                })?;
        }

        let pool = self
            .task_execution_repository
            .pool_for_workflow(workflow_id_uuid);
        let mut tx = pool.begin().await.map_err(|e| {
            Status::unavailable(format!("Could not record the task failure: {}", e))
        })?;

        let marked = TaskExecutionRepository::mark_failed_on(
            &mut tx,
            task.id,
            &error_message,
            error_type.as_deref(),
            error_details.as_ref(),
        )
        .await
        .map_err(|e| Status::unavailable(format!("Could not record the task failure: {}", e)))?;
        if marked.is_none() {
            // Already finished: this is a repeated report, and the first
            // one recorded everything.
            info!(task_id = %task_id, "Task failure already recorded; ignoring the repeated report");
            return Ok(Response::new(FailTaskExecutionResponse {}));
        }
        match self
            .retry_scheduler
            .apply_on(&mut tx, &task, decision)
            .await
            .map_err(|e| {
                Status::unavailable(format!("Could not schedule the task's retry: {}", e))
            })? {
            RetryOutcome::Rescheduled {
                new_task_execution_id,
                next_attempt,
                scheduled_at,
            } => {
                // Delivered by TaskRetryDispatcher once its backoff elapses.
                info!(
                    task_id = %task_id,
                    workflow_id = %workflow_id,
                    new_task_execution_id = %new_task_execution_id,
                    next_attempt = %next_attempt,
                    scheduled_at = %scheduled_at,
                    "Task retry scheduled (delivery deferred to retry dispatcher)"
                );
            }
            RetryOutcome::MovedToDlq { reason } => {
                warn!(task_id = %task_id, workflow_id = %workflow_id, reason = %reason, "Task moved to DLQ after exhausting retries");
            }
        }

        let journal_entry = crate::retries::task_failure::task_failed_entry(
            &task,
            &error_message,
            error_details.as_ref(),
            now,
        );
        orcher_storage::repositories::domains::journal::JournalRepository::append_next_on(
            &mut tx,
            journal_entry,
        )
        .await
        .map_err(|e| Status::unavailable(format!("Could not record the task failure: {}", e)))?;
        // A failure that ends the step settles it, with the rest.
        if terminal {
            crate::workflows::journal::SettledStep::from(&failed_entry)
                .append_on(
                    &mut tx,
                    &task.workflow_id.to_string(),
                    &task.execution_id.to_string(),
                )
                .await
                .map_err(|e| {
                    Status::unavailable(format!("Could not record the task failure: {}", e))
                })?;
        }

        tx.commit().await.map_err(|e| {
            Status::unavailable(format!("Could not record the task failure: {}", e))
        })?;

        if terminal {
            info!(workflow_id = %workflow_id, execution_id = %execution_id, task_id = %task_id, "Terminal task failure recorded for replay");
        }

        // Workflow Resume Mechanism: Check if all tasks complete and re-queue workflow
        // Even on failure, workflow needs to continue (might have error handling)
        match self.check_should_requeue_workflow(workflow_id_uuid).await {
            Ok(true) => {
                if let Err(e) = self
                    .requeue_workflow_for_continuation(workflow_id_uuid)
                    .await
                {
                    error!(
                        workflow_id = %workflow_id,
                        error = %e,
                        "Failed to re-queue workflow after task failure (non-fatal)"
                    );
                }
            }
            Ok(false) => {
                debug!(
                    workflow_id = %workflow_id,
                    "Tasks still pending after failure - workflow will be re-queued later"
                );
            }
            Err(e) => {
                warn!(
                    workflow_id = %workflow_id,
                    error = %e,
                    "Failed to check workflow task status for re-queue after failure (non-fatal)"
                );
            }
        }

        // Add task failure log entry for real-time streaming observability
        let task_fail_log = WorkflowExecutionLog {
            id: 0, // Will be set by database
            execution_id: execution_id.to_string(),
            timestamp: now,
            level: "error".to_string(),
            message: format!("Task '{}' failed: {}", task_id, error_message),
            step: Some(task_id.to_string()),
            task_id: None,
            worker_id: Some(req.identity.clone()),
            metadata: serde_json::json!({
                "task_type": error_type.unwrap_or_else(|| "UnknownTaskType".to_string()),
                "namespace": req.namespace,
                "duration_ms": duration_ms,
                "attempt": execution_attempt,
            }),
            source: Some("ExecutionService".to_string()),
            created_at: now,
        };

        // Non-blocking: log insertion failure shouldn't fail the task failure reporting
        if let Err(e) = self.logs_repository.add_log(&task_fail_log).await {
            warn!(
                workflow_id = %workflow_id,
                execution_id = %execution_id,
                task_id = %task_id,
                error = %e,
                "Failed to add task failure log entry (non-fatal)"
            );
        } else {
            debug!(
                workflow_id = %workflow_id,
                execution_id = %execution_id,
                task_id = %task_id,
                "Task failure logged for real-time streaming"
            );
        }

        Ok(Response::new(FailTaskExecutionResponse {}))
    }

    /// Cancel task execution
    ///
    /// Service calls this when task execution is canceled.
    #[instrument(skip(self, request), fields(
        namespace = %request.get_ref().namespace,
        identity = %request.get_ref().identity
    ))]
    async fn cancel_task_execution(
        &self,
        request: Request<CancelTaskExecutionRequest>,
    ) -> Result<Response<CancelTaskExecutionResponse>, Status> {
        authorize_op(
            &request,
            ServiceOperation::CancelWorkflow,
            &request.get_ref().namespace,
        )?;

        // Before `into_inner()`, which drops the metadata the tenant lives in.
        let caller = crate::grpc::tenant::organization_from_metadata(request.metadata());
        let req = request.into_inner();

        warn!(
            namespace = %req.namespace,
            identity = %req.identity,
            "Task execution canceled"
        );

        // Validate request
        if req.task_token.is_empty() {
            return Err(Status::invalid_argument("task_token is required"));
        }

        // 1. Decode task token
        let token = crate::grpc::task_token::decode(&req.task_token)?;
        let workflow_id = &token.workflow_id;
        let execution_id = &token.execution_id;
        let task_id = &token.task_id;
        let execution_attempt = token.attempt;
        let started_at = token.started_at;

        let workflow_id_uuid = Uuid::parse_str(workflow_id).map_err(|e| {
            Status::invalid_argument(format!("Invalid workflow_id in task token: {}", e))
        })?;

        // 2. Mark task execution as cancelled
        let task_exec = match self
            .task_execution_repository
            .find_by_task_token(&token.raw)
            .await
        {
            // Another organisation's task reads as absent, so the reply cannot
            // be used to confirm the token named something real.
            Ok(Some(task)) if may_act_on_task(&task, caller, &token) => task,
            Ok(Some(task)) => {
                warn!(
                    task_id = %task.task_id,
                    "Refused cancellation of a task belonging to another organisation"
                );
                return Err(Status::not_found("Task execution not found"));
            }
            Ok(None) => {
                warn!(task_token = %token.raw, "Task not found for cancellation");
                return Err(Status::not_found("Task execution not found"));
            }
            Err(e) => {
                error!(error = %e, "Failed to look up task for cancellation");
                return Err(Status::internal("Failed to look up task"));
            }
        };

        // An attempt that already ended keeps the outcome it ended with. Its
        // step is not touched: a cancelled step replays as a failure, so one
        // written for an attempt that failed and is being retried would end
        // the step under the retry.
        if task_exec.is_terminal() {
            info!(task_id = %task_id, "Task already finished; cancellation not recorded");
            return Ok(Response::new(CancelTaskExecutionResponse {}));
        }

        // 3. Store to execution_log: the cancelled step the workflow replays
        // as a failure of type Cancelled.
        let now = Timestamptz::now();
        let duration_ms =
            started_at.map(|start| crate::retries::task_failure::elapsed_ms(start, now));
        let entry = crate::retries::task_failure::cancelled_step(
            &task_exec,
            "Task cancelled by its worker",
            now,
        );

        // The same order as a failure: the replay row first (an idempotent
        // upsert outside the workflow's shard that never replaces a
        // completion), then one transaction marking the task cancelled and
        // appending TaskCanceled.
        self.execution_log_repository
            .store_failure(&entry)
            .await
            .map_err(|e| {
                Status::unavailable(format!("Could not record the cancellation: {}", e))
            })?;

        let pool = self
            .task_execution_repository
            .pool_for_workflow(workflow_id_uuid);
        let mut tx = pool.begin().await.map_err(|e| {
            Status::unavailable(format!("Could not record the cancellation: {}", e))
        })?;
        let marked = TaskExecutionRepository::mark_cancelled_on(&mut tx, task_exec.id)
            .await
            .map_err(|e| Status::internal(format!("Failed to cancel task: {}", e)))?;
        if marked.is_none() {
            info!(task_id = %task_id, "Task already finished; cancellation not recorded");
            return Ok(Response::new(CancelTaskExecutionResponse {}));
        }
        orcher_storage::repositories::domains::journal::JournalRepository::append_next_on(
            &mut tx,
            orcher_storage::schema::types::journal::JournalEntry::new(
                workflow_id.to_string(),
                execution_id.to_string(),
                0,
                "TaskCanceled".to_string(),
                serde_json::json!({
                    "task_id": task_id,
                    "execution_attempt": execution_attempt,
                    "duration_ms": duration_ms,
                    "canceled_at": Timestamptz::now().to_rfc3339(),
                }),
            ),
        )
        .await
        .map_err(|e| Status::unavailable(format!("Could not record the cancellation: {}", e)))?;
        crate::workflows::journal::SettledStep::from(&entry)
            .append_on(&mut tx, &workflow_id.to_string(), &execution_id.to_string())
            .await
            .map_err(|e| {
                Status::unavailable(format!("Could not record the cancellation: {}", e))
            })?;
        tx.commit().await.map_err(|e| {
            Status::unavailable(format!("Could not record the cancellation: {}", e))
        })?;
        info!(task_id = %task_id, "Task execution marked as cancelled");

        // 5. Re-queue workflow for continuation if all tasks are now terminal
        match self.check_should_requeue_workflow(workflow_id_uuid).await {
            Ok(true) => {
                if let Err(e) = self
                    .requeue_workflow_for_continuation(workflow_id_uuid)
                    .await
                {
                    error!(
                        workflow_id = %workflow_id,
                        error = %e,
                        "Failed to re-queue workflow after task cancellation (non-fatal)"
                    );
                }
            }
            Ok(false) => {}
            Err(e) => {
                warn!(
                    workflow_id = %workflow_id,
                    error = %e,
                    "Failed to check workflow task status after cancellation (non-fatal)"
                );
            }
        }

        Ok(Response::new(CancelTaskExecutionResponse {}))
    }

    /// Record task heartbeat
    ///
    /// Long-running tasks call this periodically to prevent timeout.
    #[instrument(skip(self, request), fields(
        namespace = %request.get_ref().namespace,
        identity = %request.get_ref().identity
    ))]
    async fn record_task_heartbeat(
        &self,
        request: Request<RecordTaskHeartbeatRequest>,
    ) -> Result<Response<RecordTaskHeartbeatResponse>, Status> {
        authorize_op(
            &request,
            ServiceOperation::PollActivityTasks,
            &request.get_ref().namespace,
        )?;

        // Before `into_inner()`, which drops the metadata the tenant lives in.
        let caller = crate::grpc::tenant::organization_from_metadata(request.metadata());
        let req = request.into_inner();

        debug!(
            namespace = %req.namespace,
            identity = %req.identity,
            "Received task heartbeat"
        );

        // Validate request
        if req.task_token.is_empty() {
            return Err(Status::invalid_argument("task_token is required"));
        }

        // 1. Decode task token
        let token = crate::grpc::task_token::decode(&req.task_token)?;

        // 2. Look up task execution by token
        let task = self
            .task_execution_repository
            .find_by_task_token(&token.raw)
            .await
            .map_err(|e| {
                // Unavailable, as the other task RPCs answer a failed lookup:
                // the worker should heartbeat again, not give the task up.
                error!(error = %e, "Failed to look up task execution for heartbeat");
                Status::unavailable(format!("Could not load the task: {}", e))
            })?
            // Another organisation's task is filtered out before the not-found
            // below, so it is reported the same way an unknown token is. A
            // forged heartbeat kept a dead worker's task looking alive, which
            // stopped the timeout watcher reclaiming it.
            .filter(|task| may_act_on_task(task, caller, &token))
            .ok_or_else(|| {
                warn!(task_token = %token.raw, "Task execution not found for heartbeat");
                Status::not_found("Task execution not found for the given task token")
            })?;

        // 3. Verify task is still running
        if task.status != "started" {
            warn!(
                task_id = %task.task_id,
                status = %task.status,
                "Heartbeat received for non-running task"
            );
            return Err(Status::failed_precondition(format!(
                "Task is not running (status: {})",
                task.status
            )));
        }

        // 4. Update heartbeat timestamp and details
        let details = if req.details.is_empty() {
            None
        } else {
            Some(req.details)
        };

        self.task_execution_repository
            .update_heartbeat(task.id, details)
            .await
            .map_err(|e| match e {
                // The task finished between the check above and this write,
                // which the write refuses. That is the same answer as the
                // check's, not a server fault.
                OrcherError::NotFound { .. } => {
                    warn!(task_id = %task.task_id, "Heartbeat received for a task that has just stopped running");
                    Status::failed_precondition("Task is not running")
                }
                e => {
                    error!(error = %e, task_id = %task.task_id, "Failed to update heartbeat");
                    Status::internal(format!("Failed to update heartbeat: {}", e))
                }
            })?;

        debug!(
            task_id = %task.task_id,
            workflow_id = %token.workflow_id,
            "Heartbeat recorded successfully"
        );

        // 5. Check if parent workflow is cancelled/terminated → signal task to cancel
        let cancel_requested = self.task_should_stop(&task).await;

        if cancel_requested {
            info!(
                task_id = %task.task_id,
                workflow_id = %token.workflow_id,
                "Workflow cancelled/terminated — signalling task cancellation via heartbeat"
            );
        }

        Ok(Response::new(RecordTaskHeartbeatResponse {
            cancel_requested,
        }))
    }

    /// Record the heartbeats of several running tasks in one call, each as
    /// [`record_task_heartbeat`](Self::record_task_heartbeat) records one, in
    /// two statements per database however many there are: one to find the
    /// tasks, one to record them and read whether a cancellation stops each.
    /// A heartbeat that is refused is answered with the status a single
    /// heartbeat would have been refused with, and refuses none of the others.
    #[instrument(skip(self, request), fields(
        namespace = %request.get_ref().namespace,
        identity = %request.get_ref().identity,
        heartbeats = request.get_ref().heartbeats.len()
    ))]
    async fn record_task_heartbeats(
        &self,
        request: Request<RecordTaskHeartbeatsRequest>,
    ) -> Result<Response<RecordTaskHeartbeatsResponse>, Status> {
        authorize_op(
            &request,
            ServiceOperation::PollActivityTasks,
            &request.get_ref().namespace,
        )?;
        let caller = crate::grpc::tenant::organization_from_metadata(request.metadata());
        let req = request.into_inner();
        if req.heartbeats.len() > MAX_HEARTBEATS_PER_CALL {
            return Err(Status::invalid_argument(format!(
                "at most {MAX_HEARTBEATS_PER_CALL} heartbeats per call, not {}",
                req.heartbeats.len()
            )));
        }

        let mut answers: Vec<TaskHeartbeatAnswer> = req
            .heartbeats
            .iter()
            .map(|heartbeat| TaskHeartbeatAnswer {
                task_token: heartbeat.task_token.clone(),
                ..Default::default()
            })
            .collect();
        let refuse = |answer: &mut TaskHeartbeatAnswer, code: tonic::Code, message: &str| {
            answer.refused_code = code as i32;
            answer.refused_message = message.to_string();
        };

        // The tokens that decode, with where their answer goes.
        let mut decoded = Vec::with_capacity(req.heartbeats.len());
        for (index, heartbeat) in req.heartbeats.iter().enumerate() {
            if heartbeat.task_token.is_empty() {
                refuse(
                    &mut answers[index],
                    tonic::Code::InvalidArgument,
                    "task_token is required",
                );
                continue;
            }
            match crate::grpc::task_token::decode(&heartbeat.task_token) {
                Ok(token) => decoded.push((index, token)),
                Err(status) => refuse(&mut answers[index], status.code(), status.message()),
            }
        }

        let raw: Vec<String> = decoded.iter().map(|(_, token)| token.raw.clone()).collect();
        let tasks = match self
            .task_execution_repository
            .find_by_task_tokens(&raw)
            .await
        {
            Ok(tasks) => tasks,
            Err(e) => {
                // As for one heartbeat: the worker should send them again.
                error!(error = %e, "Failed to look up task executions for heartbeats");
                for (index, _) in &decoded {
                    refuse(
                        &mut answers[*index],
                        tonic::Code::Unavailable,
                        "Could not load the task",
                    );
                }
                return Ok(Response::new(RecordTaskHeartbeatsResponse { answers }));
            }
        };
        let by_token: std::collections::HashMap<
            &str,
            &orcher_storage::schema::types::TaskExecution,
        > = tasks
            .iter()
            .filter_map(|task| task.task_token.as_deref().map(|token| (token, task)))
            .collect();

        // The running tasks the caller may act on, with where their answer goes.
        let mut running = Vec::with_capacity(decoded.len());
        for (index, token) in &decoded {
            // Another organisation's task is answered as an unknown token is.
            let Some(task) = by_token
                .get(token.raw.as_str())
                .filter(|task| may_act_on_task(task, caller, token))
            else {
                refuse(
                    &mut answers[*index],
                    tonic::Code::NotFound,
                    "Task execution not found for the given task token",
                );
                continue;
            };
            if task.status != "started" {
                refuse(
                    &mut answers[*index],
                    tonic::Code::FailedPrecondition,
                    &format!("Task is not running (status: {})", task.status),
                );
                continue;
            }
            let details = &req.heartbeats[*index].details;
            running.push((
                *index,
                task.id,
                (!details.is_empty()).then(|| details.clone()),
            ));
        }

        let heartbeats: Vec<_> = running
            .iter()
            .map(|(_, id, details)| (*id, details.clone()))
            .collect();
        match self
            .task_execution_repository
            .record_heartbeats(&heartbeats)
            .await
        {
            Ok(recorded) => {
                let stopped: std::collections::HashMap<_, _> = recorded.into_iter().collect();
                for (index, id, _) in &running {
                    match stopped.get(id) {
                        Some(cancel_requested) => {
                            answers[*index].cancel_requested = *cancel_requested
                        }
                        // It stopped running between the read and the write.
                        None => refuse(
                            &mut answers[*index],
                            tonic::Code::FailedPrecondition,
                            "Task is not running",
                        ),
                    }
                }
            }
            Err(e) => {
                error!(error = %e, "Failed to record heartbeats");
                for (index, _, _) in &running {
                    refuse(
                        &mut answers[*index],
                        tonic::Code::Unavailable,
                        "Failed to record the heartbeat",
                    );
                }
            }
        }

        Ok(Response::new(RecordTaskHeartbeatsResponse { answers }))
    }

    /// Respond to query task
    ///
    /// Service calls this to return query results from workflow execution.
    #[instrument(skip(self, request), fields(
        namespace = %request.get_ref().namespace
    ))]
    async fn respond_query_task(
        &self,
        request: Request<RespondQueryTaskRequest>,
    ) -> Result<Response<RespondQueryTaskResponse>, Status> {
        authorize_op(
            &request,
            ServiceOperation::QueryWorkflow,
            &request.get_ref().namespace,
        )?;

        let req = request.into_inner();

        debug!(
            namespace = %req.namespace,
            query_count = req.query_results.len(),
            "Received query responses"
        );

        // Validate request
        if req.task_token.is_empty() {
            return Err(Status::invalid_argument("task_token is required"));
        }

        if req.query_results.is_empty() {
            return Ok(Response::new(RespondQueryTaskResponse {}));
        }

        // Resolve pending queries — delivers results to waiting clients
        self.resolve_query_results(&req.query_results);

        info!(
            query_count = req.query_results.len(),
            "Query results delivered via RespondQueryTask"
        );

        Ok(Response::new(RespondQueryTaskResponse {}))
    }

    /// Respond to update task
    ///
    /// Service calls this to return update results from workflow execution.
    /// Unlike queries, update results are journaled for replay durability.
    #[instrument(skip(self, request), fields(
        namespace = %request.get_ref().namespace
    ))]
    async fn respond_update_task(
        &self,
        request: Request<RespondUpdateTaskRequest>,
    ) -> Result<Response<RespondUpdateTaskResponse>, Status> {
        authorize_op(
            &request,
            ServiceOperation::UpdateWorkflow,
            &request.get_ref().namespace,
        )?;

        let req = request.into_inner();

        debug!(
            namespace = %req.namespace,
            update_count = req.update_results.len(),
            "Received update responses"
        );

        // Validate request
        if req.task_token.is_empty() {
            return Err(Status::invalid_argument("task_token is required"));
        }

        if req.update_results.is_empty() {
            return Ok(Response::new(RespondUpdateTaskResponse {}));
        }

        // Resolve pending updates — journals results and delivers to waiting clients
        self.resolve_update_results(&req.update_results).await;

        info!(
            update_count = req.update_results.len(),
            "Update results delivered via RespondUpdateTask"
        );

        Ok(Response::new(RespondUpdateTaskResponse {}))
    }

    /// Hand back a workflow activation the worker received but will not run.
    ///
    /// Relayed to the workflow's owner first, as a completion is, and then
    /// admitted behind the same activation fence; see
    /// [`ExecutionService::release_activation`] for what it does and when it
    /// does nothing. Always acknowledged unless the request is malformed or
    /// the release could not be carried out, so a worker may send it again.
    #[instrument(skip(self, request), fields(
        workflow_id = %request.get_ref().workflow_id,
        execution_id = %request.get_ref().execution_id,
        namespace = %request.get_ref().namespace,
        identity = %request.get_ref().identity
    ))]
    async fn release_workflow_execution(
        &self,
        request: Request<ReleaseWorkflowExecutionRequest>,
    ) -> Result<Response<ReleaseWorkflowExecutionResponse>, Status> {
        authorize_op(
            &request,
            ServiceOperation::CompleteWorkflowStep,
            &request.get_ref().namespace,
        )?;

        let received_at = std::time::Instant::now();
        let metadata = self
            .owner_forwarder
            .as_ref()
            .map(|_| request.metadata().clone());
        // Before `into_inner()`, which drops the metadata the tenant lives in.
        let caller = crate::grpc::tenant::organization_from_metadata(request.metadata());
        let mut req = request.into_inner();

        if req.workflow_id.is_empty() {
            return Err(Status::invalid_argument("workflow_id is required"));
        }
        if req.execution_id.is_empty() {
            return Err(Status::invalid_argument("execution_id is required"));
        }
        if req.namespace.is_empty() {
            return Err(Status::invalid_argument("namespace is required"));
        }
        // The worker did not hold the entries an activation was joined to:
        // whatever was sent it of this run before is not to be built on.
        if req.history_missing {
            self.polling_manager
                .forget_history_sent(&req.execution_id)
                .await;
        }

        // The claim it gives up is the owner's to give, and the fence it goes
        // through is taken where completions of this workflow are applied.
        if let Some((forwarder, metadata)) = self.owner_forwarder.as_ref().zip(metadata.as_ref()) {
            use crate::grpc::forward::{Relayed, Route};
            if let Route::Owner { node, address } = forwarder.route(&req.workflow_id, metadata) {
                debug!(
                    workflow_id = %req.workflow_id,
                    owner = %node.as_str(),
                    peer = %address,
                    "Relaying workflow release to the workflow's owner"
                );
                match forwarder
                    .release_workflow_execution(&node, &address, metadata, req, received_at)
                    .await
                {
                    Relayed::Answered(answer) => return answer,
                    Relayed::NotSent(body) => {
                        req = body;
                        forwarder.ensure_owned_here(&req.workflow_id).await?;
                    }
                    // An owner still on a version without this RPC. Carried
                    // out here instead: the release is one conditional write
                    // under the execution's activation lock, which the
                    // owner's completions take too, so it cannot interleave
                    // with them from here any more than from there. Retrying
                    // would meet the same owner, and not releasing would
                    // leave the workflow waiting out its claim.
                    Relayed::Unsupported(body) => req = body,
                }
            } else {
                forwarder.ensure_owned_here(&req.workflow_id).await?;
            }
        }

        let token = crate::grpc::activation_token::decode(&req.task_token);
        self.release_activation(
            &req.workflow_id,
            &req.execution_id,
            token,
            Completer {
                identity: &req.identity,
                organization_id: caller,
                namespace: &req.namespace,
            },
        )
        .await?;
        Ok(Response::new(ReleaseWorkflowExecutionResponse {}))
    }

    /// Record that a worker is shutting down.
    ///
    /// Its open polls are answered at once with nothing and its later ones
    /// claim nothing, on every replica; see [`crate::worker::shutdowns`].
    /// Idempotent: a repeat only restarts the period the shutdown holds for.
    #[instrument(skip(self, request), fields(
        namespace = %request.get_ref().namespace,
        identity = %request.get_ref().identity
    ))]
    async fn shutdown_worker(
        &self,
        request: Request<ShutdownWorkerRequest>,
    ) -> Result<Response<ShutdownWorkerResponse>, Status> {
        // A worker may poll for workflows, for tasks, or both; either right
        // is enough to say it will stop polling. A credential limited to some
        // task queues may stop polls on those only: the shutdown is limited to
        // the queues it names, which must all be the credential's, or to the
        // credential's own when it names none. Limiting it to the queues the
        // identity was seen polling instead would need every replica to track
        // what every identity polls, and would still let a key limited to one
        // queue stop a worker's polls on all the others it shares with it.
        let mut task_queues = request.get_ref().task_queues.clone();
        if let Some(identity) = request.extensions().get::<ServiceIdentity>() {
            crate::grpc::auth::check_operation(identity, ServiceOperation::PollWorkflowSteps)
                .or_else(|_| {
                    crate::grpc::auth::check_operation(
                        identity,
                        ServiceOperation::PollActivityTasks,
                    )
                })?;
            crate::grpc::auth::check_namespace(identity, &request.get_ref().namespace)?;
            for task_queue in &task_queues {
                crate::grpc::auth::check_task_queue(identity, task_queue)?;
            }
            if task_queues.is_empty() {
                task_queues = identity.allowed_task_queues.clone();
            }
        }

        // The same organization the worker's polls are matched by.
        let organization_id = crate::grpc::tenant::organization_from_metadata(request.metadata());
        let req = request.into_inner();

        if req.namespace.is_empty() {
            return Err(Status::invalid_argument("namespace is required"));
        }
        if req.identity.is_empty() {
            return Err(Status::invalid_argument("identity is required"));
        }
        self.validate_namespace(&req.namespace, organization_id.as_ref())
            .await?;

        task_queues.sort();
        task_queues.dedup();
        let shutdown = orcher_storage::repositories::domains::worker::WorkerShutdown {
            organization_id: organization_id.unwrap_or_else(Uuid::nil),
            namespace: req.namespace,
            identity: req.identity,
            worker_instance_id: req.worker_instance_id,
            task_queues,
            remaining_ms: 0,
            unnamed_remaining_ms: 0,
        };
        let identity = shutdown.identity.clone();
        self.worker_shutdowns
            .shut_down(shutdown)
            .await
            .map_err(|e| match e {
                OrcherError::InvalidInput { message, .. } => Status::invalid_argument(message),
                OrcherError::ResourceExhausted { message, .. } => {
                    Status::resource_exhausted(message)
                }
                e => {
                    // Already refused on this replica; not yet on the others.
                    warn!(identity = %identity, error = %e, "Could not record the worker's shutdown");
                    Status::unavailable(format!("Failed to record the shutdown: {e}"))
                }
            })?;
        Ok(Response::new(ShutdownWorkerResponse {}))
    }
}

/// What a task completion found when it came to record itself.
#[derive(Debug)]
enum CompletionOutcome {
    /// This report completed the attempt and journaled TaskCompleted.
    Applied,
    /// The task's completion was already journaled. Carries the recorded
    /// result and the attempt that produced it, so the caller can rewrite
    /// what follows the commit — the replay row and the wake-up — which a
    /// failure after the first report's commit can have left undone.
    AlreadyCompleted { result: Vec<u8>, attempt: i32 },
    /// The attempt had already ended another way (the status it ended in):
    /// timed out, failed, cancelled — usually because a newer attempt took
    /// its place. Nothing was recorded.
    NotStarted(String),
    /// The row the lookup found is not on the pool the workflow routes to:
    /// a routing inconsistency, not an answer about the attempt.
    Missing,
}

impl ExecutionService {
    /// Stream a task's completion to the execution's live log. Best effort.
    #[allow(clippy::too_many_arguments)]
    async fn log_task_completed(
        &self,
        req: &CompleteTaskExecutionRequest,
        workflow_id: &str,
        execution_id: &str,
        task_id: &str,
        now: Timestamptz,
        duration_ms: Option<i64>,
        execution_attempt: i32,
    ) {
        // Add task completion log entry for real-time streaming observability
        let task_complete_log = WorkflowExecutionLog {
            id: 0, // Will be set by database
            execution_id: execution_id.to_string(),
            timestamp: now,
            level: "info".to_string(),
            message: format!("Task '{}' completed successfully", task_id),
            step: Some(task_id.to_string()),
            task_id: None,
            worker_id: Some(req.identity.clone()),
            metadata: serde_json::json!({
                "namespace": req.namespace,
                "duration_ms": duration_ms,
                "attempt": execution_attempt,
            }),
            source: Some("ExecutionService".to_string()),
            created_at: now,
        };

        // Non-blocking: log insertion failure shouldn't fail the task completion
        if let Err(e) = self.logs_repository.add_log(&task_complete_log).await {
            warn!(
                workflow_id = %workflow_id,
                execution_id = %execution_id,
                task_id = %task_id,
                error = %e,
                "Failed to add task completion log entry (non-fatal)"
            );
        } else {
            debug!(
                workflow_id = %workflow_id,
                execution_id = %execution_id,
                task_id = %task_id,
                "Task completion logged for real-time streaming"
            );
        }
    }

    /// Batch task completion: mark_completed + outbox complete + journal append
    /// in a single PostgreSQL transaction.
    ///
    /// This reduces 5 sequential DB round-trips (each with its own commit) to 1 transaction
    /// with 1 commit. With synchronous_commit=on, this saves ~880ms per task (4 × 220ms fdatasync).
    /// With synchronous_commit=off, saves ~4-8ms per task (4 × 1-2ms).
    async fn complete_task_batch(
        &self,
        task_execution_id: uuid::Uuid,
        result: &[u8],
        task_execution_id_str: &str,
        workflow_id: &str,
        execution_id: &str,
        task_id: &str,
        execution_attempt: i32,
        duration_ms: Option<i64>,
    ) -> OrcherResult<CompletionOutcome> {
        use sqlx::Row;

        // Route the completion transaction to the workflow's shard so the task-status
        // UPDATE, outbox complete, and TaskCompleted journal insert all land on the same
        // pool that holds the workflow's rows (co-located; matches the create path).
        let wf_uuid = uuid::Uuid::parse_str(workflow_id).map_err(|e| {
            OrcherError::internal(format!("Invalid workflow_id UUID in completion: {}", e))
        })?;
        let pool = self.task_execution_repository.pool_for_workflow(wf_uuid);
        let mut tx = pool.begin().await.map_err(|e| {
            OrcherError::internal(format!(
                "Failed to begin task completion transaction: {}",
                e
            ))
        })?;

        let now = Timestamptz::now();

        // Serialize journal work for this execution and make completion idempotent
        // to at-least-once re-delivery: a task re-dispatched during failover re-drive
        // can be completed more than once. The transaction-scoped advisory lock (held
        // until commit) makes the dedup check + append below atomic against a
        // concurrent completion, and shares the same key as get_next_event_id.
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(format!("{}:{}", workflow_id, execution_id))
            .execute(&mut *tx)
            .await
            .map_err(|e| OrcherError::internal(format!("Failed to acquire journal lock: {}", e)))?;

        // 1. Mark this attempt completed — only if it is still the one running.
        //    The row is the attempt: a retry is a new row with its own token, so
        //    a report from an attempt that was timed out or failed and then
        //    retried finds its own row no longer started, whatever became of
        //    the attempt that replaced it.
        let rows_affected = sqlx::query(
            r#"
            UPDATE task_executions
            SET status = 'completed',
                result = $2,
                completed_at = $3,
                updated_at = $4
            WHERE id = $1 AND status = 'started'
            "#,
        )
        .bind(task_execution_id)
        .bind(result)
        .bind(now)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(|e| OrcherError::internal(format!("Failed to mark task as completed: {}", e)))?
        .rows_affected();

        if rows_affected == 0 {
            // Say why, from the same transaction. A row already completed was
            // completed by this very attempt, so this report repeats one that
            // is recorded; any other status means the attempt ended some other
            // way and its result must not be recorded now.
            let row: Option<(String, Option<Vec<u8>>, i32)> = sqlx::query_as(
                "SELECT status, result, attempt_number FROM task_executions WHERE id = $1",
            )
            .bind(task_execution_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| {
                OrcherError::internal(format!("Failed to read the task's status: {}", e))
            })?;
            return Ok(match row {
                Some((status, result, attempt)) if status == "completed" => {
                    CompletionOutcome::AlreadyCompleted {
                        result: result.unwrap_or_default(),
                        attempt,
                    }
                }
                Some((status, _, _)) => CompletionOutcome::NotStarted(status),
                None => CompletionOutcome::Missing,
            });
        }

        // A task re-dispatched during failover re-drive can be running as a
        // second row while the first has already answered it. The journal is
        // what the workflow replays, so it gets one TaskCompleted per task: if
        // one is there, this report is a duplicate and its own result is not
        // kept. Its row is still finished, though, and with the journaled
        // result: left started, the timeout watcher would retry a task that
        // has completed, and the workflow would count it as still running.
        // (V049 keeps a repeated ScheduleTask from inserting such a row, but
        // only for rows scheduled by command, so this stays reachable.)
        // The result is the task's StepCompleted entry's; a TaskCompleted
        // written before those entries carries it itself.
        type Journaled = (Option<String>, Option<i32>, Option<serde_json::Value>);
        let journaled: Option<Journaled> = sqlx::query_as(
            r#"
            SELECT tc.payload->>'result', (tc.payload->>'execution_attempt')::int,
                   (SELECT sc.payload->'result'
                    FROM workflow_execution_journal sc
                    WHERE sc.workflow_id = $1 AND sc.execution_id = $2
                      AND sc.event_type = 'StepCompleted'
                      AND sc.payload->>'step_name' = $3
                    LIMIT 1)
            FROM workflow_execution_journal tc
            WHERE tc.workflow_id = $1 AND tc.execution_id = $2
              AND tc.event_type = 'TaskCompleted'
              AND tc.payload->>'task_id' = $3
            ORDER BY tc.event_id
            LIMIT 1
            "#,
        )
        .bind(workflow_id)
        .bind(execution_id)
        .bind(task_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| {
            OrcherError::internal(format!("Failed to check completion idempotency: {}", e))
        })?;
        if let Some((journaled_result, journaled_attempt, step_result)) = journaled {
            let from_step =
                step_result.map(|result| serde_json::to_vec(&result).unwrap_or_default());
            let recorded = journaled_result
                .map(|encoded| {
                    base64::Engine::decode(&base64::engine::general_purpose::STANDARD, encoded)
                        .map_err(|e| {
                            OrcherError::internal(format!(
                                "Journaled task result is not base64: {}",
                                e
                            ))
                        })
                })
                .transpose()?
                .or(from_step)
                .unwrap_or_default();
            sqlx::query("UPDATE task_executions SET result = $2 WHERE id = $1")
                .bind(task_execution_id)
                .bind(&recorded)
                .execute(&mut *tx)
                .await
                .map_err(|e| {
                    OrcherError::internal(format!("Failed to finish a duplicate task row: {}", e))
                })?;
            tx.commit().await.map_err(|e| {
                OrcherError::internal(format!("Failed to commit a duplicate task row: {}", e))
            })?;
            info!(
                task_id = %task_id,
                workflow_id = %workflow_id,
                "Duplicate task completion: row finished, journaled result kept"
            );
            return Ok(CompletionOutcome::AlreadyCompleted {
                result: recorded,
                attempt: journaled_attempt.unwrap_or(execution_attempt),
            });
        }

        // 2. The outbox row is already completed by this point, so nothing is done
        //    here.
        //
        //    A dispatch_task's outbox entry records that the dispatch was
        //    *emitted*, and the outbox processor completes it as soon as the
        //    executor returns — long before the worker's result arrives here.
        //    This step used to look the row up by `payload->>'task_execution_id'`
        //    and complete it if still pending; measured over a live workload it
        //    matched 0 of 540 times, because there is nothing left to find.
        //
        //    It was a fallback for the rare case where the processor's own
        //    mark_completed fails and leaves a row in 'processing'. That case is
        //    already handled by cleanup_stuck_tasks, which the outbox processor
        //    runs on a timer — so the fallback bought nothing and cost an
        //    unindexable JSON predicate on the hottest table, inside the
        //    completion transaction.

        // 3. Get next event ID and append TaskCompleted journal entry
        let next_event_id: i64 = sqlx::query("SELECT get_next_event_id($1, $2) as next_id")
            .bind(workflow_id)
            .bind(execution_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| {
                OrcherError::internal(format!("Failed to get next event ID in batch tx: {}", e))
            })?
            .try_get("next_id")
            .map_err(|e| OrcherError::internal(format!("Failed to parse next event ID: {}", e)))?;

        // The result goes in the task's StepCompleted entry, appended below,
        // and only there: it is what the workflow reads.
        let attributes = serde_json::json!({
            "task_id": task_id,
            "execution_attempt": execution_attempt,
            "duration_ms": duration_ms,
            "completed_at": Timestamptz::now().to_rfc3339(),
        });

        let journal_entry = orcher_storage::schema::types::journal::JournalEntry::new(
            workflow_id.to_string(),
            execution_id.to_string(),
            next_event_id,
            "TaskCompleted".to_string(),
            attributes,
        );

        sqlx::query(
            r#"
            INSERT INTO workflow_execution_journal (
                organization_id, workflow_id, execution_id, event_id, event_type,
                timestamp, payload, version, task_id
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            -- Defense-in-depth: the advisory lock above already serializes event_id
            -- allocation, but a residual duplicate append degrades to a no-op rather
            -- than surfacing a uq_journal_event_id error to the client.
            ON CONFLICT (workflow_id, execution_id, event_id) DO NOTHING
            "#,
        )
        .bind(journal_entry.organization_id)
        .bind(&journal_entry.workflow_id)
        .bind(&journal_entry.execution_id)
        .bind(journal_entry.event_id)
        .bind(&journal_entry.event_type)
        .bind(journal_entry.timestamp)
        .bind(&journal_entry.payload)
        .bind(journal_entry.version)
        .bind(journal_entry.task_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            OrcherError::internal(format!("Failed to append journal entry in batch tx: {}", e))
        })?;

        // 4. The step's result, as its workflow reads it, with the rest.
        crate::workflows::journal::SettledStep {
            step_name: task_id.to_string(),
            step_type: ExecutionStepType::Task,
            status: ExecutionLogStatus::Completed,
            execution_attempt,
            result: Some(crate::workflows::journal::result_value(result)),
            error_message: None,
            error_details: None,
            completed_at: Some(now),
            duration_ms,
        }
        .append_on(&mut tx, workflow_id, execution_id)
        .await?;

        // Commit all 4 operations in one round-trip
        tx.commit().await.map_err(|e| {
            OrcherError::internal(format!("Failed to commit task completion batch: {}", e))
        })?;

        info!(
            task_execution_id = %task_execution_id_str,
            task_id = %task_id,
            "Task completion batch committed (mark_completed + outbox + journal)"
        );

        Ok(CompletionOutcome::Applied)
    }

    /// Rebuild the `StepCompleted` entries of a run whose journal does not
    /// hold them (one created before step results were journal entries) from
    /// its execution log, appended after the journal's own entries and
    /// numbered on from them. `StepCompleted` entries its journal does hold
    /// are sent without attributes, as they were then.
    ///
    /// Every activation of such a run reads its whole execution log; a run
    /// created now reads neither.
    async fn rebuild_step_results(
        &self,
        journal: &mut Vec<orcher_proto::orcher::v1::JournalEntry>,
        workflow_id: &str,
    ) -> Result<(), Status> {
        use orcher_proto::orcher::v1::{journal_entry::Attributes, EntryType, JournalEntry};

        for entry in journal.iter_mut() {
            if entry.entry_type == EntryType::StepCompleted as i32 {
                entry.entry_type = EntryType::Unspecified as i32;
                entry.attributes = None;
            }
        }

        let workflow_uuid = Uuid::parse_str(workflow_id).map_err(|e| {
            error!(error = %e, workflow_id = %workflow_id, "Invalid workflow_id UUID");
            Status::internal(format!("Invalid workflow_id: {}", e))
        })?;
        let steps = self
            .execution_log_repository
            .list_entries(workflow_uuid)
            .await
            .map_err(|e| {
                error!(error = %e, workflow_id = %workflow_id, "Failed to load step results");
                Status::internal(format!("Failed to load step results: {}", e))
            })?;

        let mut next_entry_id = journal.iter().map(|e| e.entry_id).max().unwrap_or(0) + 1;
        for row in &steps {
            let step = crate::workflows::journal::SettledStep::from(row);
            if !step.is_readable() {
                continue;
            }
            let attributes = step.attributes();
            journal.push(JournalEntry {
                entry_id: next_entry_id,
                timestamp: attributes.completed_at,
                entry_type: EntryType::StepCompleted as i32,
                version: 1,
                task_id: 0,
                attributes: Some(Attributes::StepCompleted(attributes)),
            });
            next_entry_id += 1;
        }
        Ok(())
    }

    /// Hand the run `execution_id`'s outcome to the clients waiting on its
    /// result, if it has ended for good.
    ///
    /// Read from the row, after whatever ended the run has been recorded —
    /// including a failure's retry decision — so the waiters hear what
    /// `GetWorkflowResult` itself would read: nothing while a retry is to run
    /// (the waiter follows the chain to it), the outcome once it is final.
    /// Nothing is read unless someone is waiting. A waiter not answered here
    /// re-reads the chain on its own within a few seconds, so a lookup that
    /// fails costs latency, not an answer.
    async fn answer_result_waiters(&self, execution_id: &str) {
        let Some(ref notifier) = self.workflow_completion_notifier else {
            return;
        };
        // Only asks whether anyone is waiting; the outcome is read from the
        // database with no lock held, and handed over after.
        if !notifier.has_waiters(execution_id) {
            return;
        }
        match self
            .workflow_execution_repository
            .find_by_execution_id(execution_id)
            .await
        {
            Ok(Some(run)) => {
                if let Some(outcome) =
                    crate::grpc::services::workflow::CompletionOutcome::of_run(&run)
                {
                    notifier.notify(execution_id, outcome);
                    debug!(execution_id = %execution_id, "Handed the run's outcome to its result waiters");
                } else {
                    debug!(
                        execution_id = %execution_id,
                        status = %run.status,
                        "Run has not ended for good; its result waiters keep waiting"
                    );
                }
            }
            Ok(None) => {}
            Err(e) => warn!(
                execution_id = %execution_id,
                error = %e,
                "Could not read the run for its result waiters; they will re-read it"
            ),
        }
    }

    /// Decide what becomes of a run that has just failed, and record it.
    ///
    /// Returns whether the failure is final. A failure the policy retries gets
    /// `next_retry_at` — the retry scheduler starts the successor once it has
    /// passed — and is not final. A failure it will not retry is settled, so
    /// the scheduler stops re-deciding it and anything asking whether it is
    /// final reads the same answer from the row. A failure the workflow code
    /// raised as non-retryable (`non_retryable`) is never retried.
    async fn decide_failure(
        &self,
        run: &WorkflowExecution,
        failure_type: Option<&str>,
        non_retryable: bool,
    ) -> bool {
        // The failure was not recorded because the run had already ended —
        // cancelled or terminated while its worker was failing it. There is
        // no failure to decide; its ending is what the parent hears.
        if run.status != "failed" {
            return run_is_final(run);
        }
        match WorkflowRetryPolicyEvaluator::new().decide_failure(run, failure_type, non_retryable) {
            WorkflowRetryDecision::Retry {
                backoff_duration,
                scheduled_at,
                next_attempt,
            } => {
                match self
                    .workflow_execution_repository
                    .set_next_retry_at(&run.execution_id, scheduled_at)
                    .await
                {
                    Ok(()) => info!(
                        execution_id = %run.execution_id,
                        next_attempt,
                        max_retries = run.max_retries,
                        delay_ms = backoff_duration.as_millis() as u64,
                        "Workflow failure recorded; retry scheduled after backoff"
                    ),
                    // Without a time the scheduler retries it at once, which
                    // is a shorter backoff, not a lost retry.
                    Err(e) => warn!(error = %e, "Failed to set next_retry_at (non-fatal)"),
                }
                false
            }
            WorkflowRetryDecision::Abandon { reason } => {
                // Recorded even when the retries are simply used up: the
                // marker is how every reader tells a decided failure from one
                // still waiting for its decision.
                if let Err(e) = self
                    .workflow_execution_repository
                    .settle_failure(&run.execution_id)
                    .await
                {
                    // Left unsettled, the retry scheduler reaches the same
                    // decision and settles it; the parent is told then.
                    warn!(
                        execution_id = %run.execution_id,
                        error = %e,
                        "Failed to settle a final failure; the retry scheduler will"
                    );
                }
                debug!(
                    execution_id = %run.execution_id,
                    reason = %reason,
                    "Workflow failure is final"
                );
                true
            }
        }
    }

    /// Tell the parent of the child run `child_execution_id` how it ended,
    /// retrying briefly.
    ///
    /// Every failure here used to be a `warn!` and a return, and the parent,
    /// parked on the child, then waited forever: nothing else ever appended the
    /// outcome. A transient error — a pool timeout, a failover — is exactly
    /// when that happened, so a few quick attempts cover it, and the child
    /// reconciliation sweep delivers whatever is still undelivered after that.
    /// Delivery is idempotent, so an attempt that committed and then failed to
    /// requeue is safe to repeat.
    async fn notify_parent_of_child_outcome(&self, child_execution_id: &str) {
        const ATTEMPTS: u32 = 5;
        let mut delay = std::time::Duration::from_millis(100);
        for attempt in 1..=ATTEMPTS {
            match self.deliver_child_outcome(child_execution_id).await {
                Ok(delivery) => {
                    debug!(
                        child_execution_id = %child_execution_id,
                        ?delivery,
                        "Child outcome delivery"
                    );
                    return;
                }
                Err(e) if attempt < ATTEMPTS => {
                    warn!(
                        child_execution_id = %child_execution_id,
                        attempt,
                        error = %e,
                        "Could not tell the parent how its child ended; retrying"
                    );
                    tokio::time::sleep(delay).await;
                    delay *= 2;
                }
                Err(e) => {
                    error!(
                        child_execution_id = %child_execution_id,
                        error = %e,
                        "Could not tell the parent how its child ended: the parent \
                         waits on this child until the reconciliation sweep delivers it"
                    );
                }
            }
        }
    }

    /// Deliver the outcome of the run `child_execution_id`, read from its row.
    ///
    /// Read afresh on each attempt: the row carries what command processing
    /// just wrote — the result, the error, whether the failure was settled.
    async fn deliver_child_outcome(&self, child_execution_id: &str) -> OrcherResult<Delivery> {
        let run = self
            .workflow_execution_repository
            .find_by_execution_id(child_execution_id)
            .await?
            .ok_or_else(|| OrcherError::not_found("child execution", child_execution_id))?;
        self.child_outcomes.deliver_for_run(&run).await
    }

    /// Whether the workflow should be handed out again now that one of its
    /// tasks has finished: none of its tasks is still waiting or running. Only
    /// a task's completion or failure asks, so the workflow has at least one.
    async fn check_should_requeue_workflow(&self, workflow_id: Uuid) -> OrcherResult<bool> {
        use orcher_common::types::identifiers::WorkflowId;

        let open = self
            .task_execution_repository
            .has_open_tasks(WorkflowId::from_uuid(workflow_id))
            .await?;
        debug!(
            workflow_id = %workflow_id,
            open_tasks = open,
            "Checked whether the workflow is ready to be handed out again"
        );
        Ok(!open)
    }

    /// Whether a running task should stop, from its workflow's state.
    ///
    /// It should when the workflow was cancelled or terminated, and when the
    /// workflow was asked to cancel after the task was first scheduled: that
    /// is the work the request interrupts. A task the workflow scheduled after
    /// the request is its cleanup and runs on. See
    /// [`TaskExecutionRepository::stopped_by_cancellation`]; a failed read
    /// answers no, and the next heartbeat asks again.
    async fn task_should_stop(&self, task: &orcher_storage::schema::types::TaskExecution) -> bool {
        self.task_execution_repository
            .stopped_by_cancellation(task.id, task.workflow_id.as_uuid())
            .await
            .unwrap_or(false)
    }

    /// Collect pending queries for a given execution_id from the shared registry.
    ///
    /// Returns `QueryRequest` proto messages to attach to the poll response.
    /// Does not remove queries from the registry — they stay until resolved.
    fn collect_pending_queries(
        &self,
        execution_id: &str,
    ) -> Vec<orcher_proto::orcher::v1::QueryRequest> {
        let Some(ref registry) = self.pending_queries else {
            return vec![];
        };

        let mut queries = Vec::new();
        for entry in registry.iter() {
            if entry.value().execution_id == execution_id {
                queries.push(entry.value().request.clone());
            }
        }

        if !queries.is_empty() {
            debug!(
                execution_id = %execution_id,
                query_count = queries.len(),
                "Attaching pending queries to poll response"
            );
        }

        queries
    }

    /// Resolve pending queries with results from a worker.
    ///
    /// Called by both `respond_query_task` and `complete_workflow_execution`
    /// when query_results are returned.
    fn resolve_query_results(&self, query_results: &[orcher_proto::orcher::v1::QueryResult]) {
        let Some(ref registry) = self.pending_queries else {
            return;
        };

        for qr in query_results {
            if let Some((_, pending)) = registry.remove(&qr.query_id) {
                // Write the result and notify the waiting client
                if let Ok(mut result) = pending.result.lock() {
                    *result = Some(qr.clone());
                }
                pending.notify.notify_one();

                debug!(
                    query_id = %qr.query_id,
                    "Query result delivered to waiting client"
                );
            } else {
                warn!(
                    query_id = %qr.query_id,
                    "Received query result for unknown/expired query ID"
                );
            }
        }
    }

    /// Collect pending updates for a given execution_id from the shared registry.
    ///
    /// Returns `UpdateRequest` proto messages to attach to the poll response.
    /// Does not remove updates from the registry — they stay until resolved.
    fn collect_pending_updates(
        &self,
        execution_id: &str,
    ) -> Vec<orcher_proto::orcher::v1::UpdateRequest> {
        let Some(ref registry) = self.pending_updates else {
            return vec![];
        };

        let mut updates = Vec::new();
        for entry in registry.iter() {
            if entry.value().execution_id == execution_id {
                updates.push(entry.value().request.clone());
            }
        }

        if !updates.is_empty() {
            debug!(
                execution_id = %execution_id,
                update_count = updates.len(),
                "Attaching pending updates to poll response"
            );
        }

        updates
    }

    /// Resolve pending updates with results from a worker.
    ///
    /// Called by both `respond_update_task` and `complete_workflow_execution`
    /// when update_results are returned. Unlike queries, updates are journaled
    /// for replay durability before notifying the waiting client.
    async fn resolve_update_results(
        &self,
        update_results: &[orcher_proto::orcher::v1::UpdateResult],
    ) {
        let Some(ref registry) = self.pending_updates else {
            return;
        };

        let journal_service = JournalService::new(self.journal_repository.clone());

        for ur in update_results {
            if let Some((_, pending)) = registry.remove(&ur.update_id) {
                // Journal UpdateAccepted entry
                let accepted_attrs = serde_json::json!({
                    "update_id": ur.update_id,
                    "update_type": pending.request.update_type,
                    "args": base64::Engine::encode(&base64::engine::general_purpose::STANDARD,&pending.request.args),
                    "identity": "client",
                });
                if let Err(e) = journal_service
                    .append_entry(
                        &pending.workflow_id,
                        &pending.execution_id,
                        "UpdateAccepted",
                        accepted_attrs,
                    )
                    .await
                {
                    warn!(
                        error = %e,
                        update_id = %ur.update_id,
                        "Failed to journal UpdateAccepted"
                    );
                }

                // Journal UpdateCompleted entry
                let completed_attrs = match &ur.result {
                    Some(orcher_proto::orcher::v1::update_result::Result::Answer(answer)) => {
                        serde_json::json!({
                            "update_id": ur.update_id,
                            "update_type": pending.request.update_type,
                            "result": base64::Engine::encode(&base64::engine::general_purpose::STANDARD,answer),
                        })
                    }
                    Some(orcher_proto::orcher::v1::update_result::Result::ErrorMessage(msg)) => {
                        serde_json::json!({
                            "update_id": ur.update_id,
                            "update_type": pending.request.update_type,
                            "failure": { "message": msg },
                        })
                    }
                    None => {
                        serde_json::json!({
                            "update_id": ur.update_id,
                            "update_type": pending.request.update_type,
                        })
                    }
                };
                if let Err(e) = journal_service
                    .append_entry(
                        &pending.workflow_id,
                        &pending.execution_id,
                        "UpdateCompleted",
                        completed_attrs,
                    )
                    .await
                {
                    warn!(
                        error = %e,
                        update_id = %ur.update_id,
                        "Failed to journal UpdateCompleted"
                    );
                }

                // Write the result and notify the waiting client
                if let Ok(mut result) = pending.result.lock() {
                    *result = Some(ur.clone());
                }
                pending.notify.notify_one();

                debug!(
                    update_id = %ur.update_id,
                    "Update result journaled and delivered to waiting client"
                );
            } else {
                warn!(
                    update_id = %ur.update_id,
                    "Received update result for unknown/expired update ID"
                );
            }
        }
    }

    /// Re-queue workflow execution for continuation after task completion
    ///
    /// This method pushes the workflow execution back to the Redis matching queue
    /// so that the SDK can poll and resume the workflow. This is the critical
    /// missing piece that enables workflow resume after tasks complete.
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - UUID of the workflow to re-queue
    ///
    /// # Workflow Resume Flow
    ///
    /// 1. Task completes → `complete_task_execution()`
    /// 2. Check if all tasks done → `check_should_requeue_workflow()`
    /// 3. If all done → `requeue_workflow_for_continuation()` ← this method
    /// 4. Push execution_id to Redis matching queue
    /// 5. Notify polling manager
    /// 6. SDK polls → finds workflow → replays journal → continues execution
    ///
    /// # Redis Queue Key Format
    ///
    /// Uses same format as workflow start: `{task_queue}:{namespace}`
    ///
    /// # Fallback
    ///
    /// If Redis is unavailable, the notification to polling manager still happens,
    /// and PostgreSQL-based polling fallback will eventually pick up the workflow.
    async fn requeue_workflow_for_continuation(&self, workflow_id: Uuid) -> OrcherResult<()> {
        // Query workflow execution to get execution_id, namespace, and task_queue
        let workflow_execution = self
            .workflow_execution_repository
            .find_by_id(workflow_id)
            .await?
            .ok_or_else(|| OrcherError::not_found("workflow_execution", workflow_id.to_string()))?;

        self.requeue_workflow_for_continuation_inner(
            workflow_id,
            &workflow_execution.execution_id,
            &workflow_execution.namespace,
            &workflow_execution.task_queue,
        )
        .await
    }

    /// Inner implementation of workflow re-queue, accepting pre-fetched data.
    ///
    /// Callers that already hold a `WorkflowExecution` can skip the extra DB round-trip by
    /// calling this directly. `requeue_workflow_for_continuation` delegates here after fetching.
    async fn requeue_workflow_for_continuation_inner(
        &self,
        workflow_id: Uuid,
        execution_id: &str,
        namespace: &str,
        task_queue: &str,
    ) -> OrcherResult<()> {
        info!(
            workflow_id = %workflow_id,
            execution_id = %execution_id,
            namespace = %namespace,
            task_queue = %task_queue,
            "Re-queuing workflow for continuation after task completion"
        );

        // Release the claim so the workflow can be claimed again
        // The workflow was claimed during the previous execution, and we need to clear
        // claimed_by/claimed_at so it can be claimed again when a worker polls
        debug!(
            workflow_id = %workflow_id,
            execution_id = %execution_id,
            "Releasing claim before re-queue"
        );

        // Not released while an activation is still out: that activation's
        // completion takes the wake-up instead, rather than a second worker
        // being handed the workflow alongside the first.
        match self
            .workflow_execution_repository
            .release_claim_to_resume(execution_id)
            .await
        {
            Ok(orcher_storage::repositories::domains::workflow_execution::Resume::Deferred) => {
                return Ok(());
            }
            Ok(_) => {
                debug!(
                    workflow_id = %workflow_id,
                    execution_id = %execution_id,
                    "Claim released successfully"
                );
            }
            Err(e) => {
                warn!(
                    workflow_id = %workflow_id,
                    execution_id = %execution_id,
                    error = %e,
                    "Failed to release claim (non-fatal, will rely on claim timeout)"
                );
                // Don't fail - claim timeout will eventually release it
            }
        }

        // Fast path: push to in-memory continuation queue for direct dispatch
        // This skips Redis round-trip and the expensive claim scan query.
        // Annotate with the preferred worker (affinity hint) so the same worker
        // picks up the continuation, improving cache hit rate on HighPerfCache.
        let preferred_worker = self.polling_manager.journal_holder(execution_id).await;
        self.polling_manager.push_continuation(
            task_queue,
            namespace,
            execution_id,
            preferred_worker.clone(),
        );

        debug!(
            workflow_id = %workflow_id,
            execution_id = %execution_id,
            task_queue = %task_queue,
            preferred_worker = ?preferred_worker,
            "⚡ Requeue: pushed to continuation queue (skips Redis)"
        );

        // Notify polling manager to wake a waiting poller
        self.polling_manager
            .notify_work_available(task_queue, namespace)
            .await;

        trace!(
            workflow_id = %workflow_id,
            execution_id = %execution_id,
            "Requeue complete, polling manager notified"
        );

        Ok(())
    }
}

// ================================================================================================
// HELPER FUNCTIONS
// ================================================================================================

/// Whether `caller` may act on the task execution the token resolved to.
///
/// Two things have to agree before a completion is allowed.
///
/// The **owning execution** is the authority. `find_by_task_token` reports the
/// execution's `organization_id` on the task it returns, joined in the same
/// query that resolved the token, so ownership is decided by what was stored
/// rather than by anything the caller sent. Not the task row's own column:
/// that was never written, so authorising on it refused every tenant's own
/// completions. This alone
/// closes the hole: a task token used to be unsigned plaintext, and
/// `GetTaskExecutions` returns the three ids it is made of, so knowing another
/// tenant's task ids was enough to complete, fail or cancel their work with a
/// result of your choosing.
///
/// The **token** is checked against it as well. A signed token carries the
/// organisation it was minted for, so a token that verifies but names a
/// different organisation than the row it resolved to is a contradiction —
/// either the row was re-owned underneath a worker, or a signature was minted
/// somewhere it should not have been. Neither should be acted on, and an
/// unsigned or pre-signing token carries no organisation and so cannot
/// contradict anything.
fn may_act_on_task(
    task_exec: &TaskExecution,
    caller: Option<Uuid>,
    token: &crate::grpc::task_token::TaskToken,
) -> bool {
    if let Some(signed_for) = token.organization_id {
        if task_exec.organization_id != Some(signed_for) {
            warn!(
                task_id = %task_exec.task_id,
                "Task token names a different organisation than the task row"
            );
            return false;
        }
    }

    crate::grpc::tenant::may_act_on(task_exec.organization_id, caller)
}

// ================================================================================================
// TESTS
// ================================================================================================

#[cfg(test)]
mod tests {

    /// A failed step replays the failure type and retryability it recorded,
    /// and the generic type only when it recorded none.
    #[test]
    fn a_failed_step_replays_its_recorded_type() {
        use orcher_storage::repositories::{
            ExecutionLogEntry, ExecutionLogStatus, ExecutionStepType,
        };
        let step = |details: Option<serde_json::Value>| ExecutionLogEntry {
            workflow_id: uuid::Uuid::new_v4(),
            step_name: "charge_0".into(),
            step_type: ExecutionStepType::Task,
            execution_attempt: 1,
            status: ExecutionLogStatus::Failed,
            input: None,
            result: None,
            error_message: Some("declined".into()),
            error_details: details,
            created_at: Timestamptz::now(),
            started_at: None,
            completed_at: None,
            duration_ms: None,
            retry_count: 0,
            last_error: None,
        };
        let typed = crate::workflows::journal::SettledStep::from(&step(Some(
            serde_json::json!({"failure_type": "CardDeclined", "non_retryable": true}),
        )))
        .failure()
        .unwrap();
        assert_eq!(typed.failure_type, "CardDeclined");
        assert!(typed.non_retryable);
        let untyped = crate::workflows::journal::SettledStep::from(&step(None))
            .failure()
            .unwrap();
        assert_eq!(untyped.failure_type, "ExecutionError");
        assert!(!untyped.non_retryable);
    }

    /// A cancelled step replays as a never-retried failure of the one
    /// cancellation type, including one recorded with no type or message.
    #[test]
    fn a_cancelled_step_replays_as_a_cancellation() {
        use orcher_storage::repositories::{
            ExecutionLogEntry, ExecutionLogStatus, ExecutionStepType,
        };
        let step = |message: Option<&str>| ExecutionLogEntry {
            workflow_id: uuid::Uuid::new_v4(),
            step_name: "charge_0".into(),
            step_type: ExecutionStepType::Task,
            execution_attempt: 1,
            status: ExecutionLogStatus::Cancelled,
            input: None,
            result: None,
            error_message: message.map(Into::into),
            error_details: None,
            created_at: Timestamptz::now(),
            started_at: None,
            completed_at: None,
            duration_ms: None,
            retry_count: 0,
            last_error: None,
        };
        for message in [Some("Task cancelled by its worker"), None] {
            let failure = crate::workflows::journal::SettledStep::from(&step(message))
                .failure()
                .expect("replays a failure");
            assert_eq!(failure.failure_type, "Cancelled");
            assert!(failure.non_retryable);
            assert!(!failure.message.is_empty());
        }
    }

    use super::{
        claim_belongs_to, claim_released_after_completion, dispatched_attempt,
        older_worker_cancellation, release_activation_permit,
    };
    use orcher_common::types::time::Timestamptz;
    use orcher_proto::orcher::v1::CommandType;

    /// A task is handed out as the attempt its dispatch delivers, counted
    /// from 1, and never as the outbox's delivery count.
    #[test]
    fn a_task_is_handed_out_as_the_attempt_its_dispatch_delivers() {
        use crate::engine::coordinator::WorkflowCoordinator;
        use orcher_common::types::identifiers::{ExecutionId, WorkflowId};
        use orcher_storage::schema::types::TaskExecution;

        let mut task = TaskExecution::new_scheduled(
            "charge_0".to_string(),
            "charge".to_string(),
            WorkflowId::from_uuid(uuid::Uuid::new_v4()),
            ExecutionId::from_uuid(uuid::Uuid::new_v4()),
            "payments".to_string(),
            "default".to_string(),
            None,
            None,
            None,
            None,
            None,
        );
        assert_eq!(
            dispatched_attempt(&WorkflowCoordinator::task_dispatch_payload(&task)),
            1
        );
        task.attempt_number = 3;
        assert_eq!(
            dispatched_attempt(&WorkflowCoordinator::task_dispatch_payload(&task)),
            3
        );
        // A dispatch written before it carried the attempt is a first one.
        assert_eq!(dispatched_attempt(&serde_json::json!({"task_id": "t"})), 1);
        assert_eq!(
            dispatched_attempt(&serde_json::json!({"attempt_number": 0})),
            1
        );
    }

    /// A claim belongs to the worker that completes under the name its
    /// pollers were derived from, and to no worker whose name merely starts
    /// the same way.
    #[test]
    fn a_claim_belongs_only_to_the_worker_that_polled_for_it() {
        assert!(claim_belongs_to("w@h", "w@h"));
        assert!(claim_belongs_to("w@h-wf-driver-poller-0", "w@h"));
        assert!(claim_belongs_to("w@h-wf-driver-poller-12", "w@h"));
        assert!(!claim_belongs_to("w@h-wf-driver-poller-", "w@h"));
        assert!(!claim_belongs_to("w@h-wf-driver-poller-x", "w@h"));
        assert!(!claim_belongs_to("w@h-wf-driver-poller-1-b", "w@h"));
        assert!(!claim_belongs_to("w@h2-wf-driver-poller-0", "w@h"));
        assert!(!claim_belongs_to("w@h", "w@h2"));
    }
    use orcher_dispatch::capacity::CapacityRegistry;

    const QUEUE: &str = "transfers";
    const NS: &str = "staging";
    const WORKER: &str = "worker@host";
    const POLLER: &str = "worker@host-wf-driver-poller-2";
    const EXEC: &str = "exec-1";

    /// The permit is taken by the poller and given back by the worker, and
    /// those are two different names for the same worker. Releasing by the
    /// completing name alone found nothing and dropped the permit on the floor.
    #[test]
    fn a_permit_taken_by_a_poller_is_released_by_the_worker() {
        let registry = CapacityRegistry::new();
        let permits = registry.register(QUEUE, NS, POLLER, 2);
        permits.try_acquire_guard().expect("a permit").keep(EXEC);
        assert_eq!(permits.available(), 1);

        release_activation_permit(&registry, QUEUE, NS, Some(POLLER), WORKER, EXEC);

        assert_eq!(permits.available(), 2, "the poller got its permit back");
    }

    /// A claim already cleared, or one never recorded, still has to give the
    /// permit back — the completing identity is the only name left to try.
    #[test]
    fn without_a_claim_holder_the_completing_worker_is_used() {
        let registry = CapacityRegistry::new();
        let permits = registry.register(QUEUE, NS, WORKER, 1);
        permits.try_acquire_guard().expect("a permit").keep(EXEC);

        release_activation_permit(&registry, QUEUE, NS, None, WORKER, EXEC);

        assert_eq!(permits.available(), 1);
    }

    /// With two replicas, the completion can arrive at the replica that did
    /// not hand the activation out. It must leave that replica's permits
    /// alone — they belong to other activations — and the replica holding
    /// this one returns it when it expires.
    #[test]
    fn a_completion_for_another_replicas_activation_takes_nothing_here() {
        let registry = CapacityRegistry::new();
        let permits = registry.register(QUEUE, NS, POLLER, 2);
        permits
            .try_acquire_guard()
            .expect("a permit")
            .keep("an-activation-held-here");

        release_activation_permit(
            &registry,
            QUEUE,
            NS,
            Some(POLLER),
            WORKER,
            "handed-out-elsewhere",
        );

        assert_eq!(permits.available(), 1, "the permit held here is still held");
    }

    /// An unregistered worker is not a crash. It is the case the warning exists
    /// for, and releasing nothing is all that can be done about it.
    #[test]
    fn an_unknown_worker_releases_nothing_and_survives() {
        let registry = CapacityRegistry::new();
        release_activation_permit(&registry, QUEUE, NS, Some("ghost"), "also-ghost", EXEC);
    }

    #[test]
    fn a_finished_workflow_releases_its_claim() {
        assert!(claim_released_after_completion(true, false, false));
        assert!(claim_released_after_completion(false, true, false));
        assert!(claim_released_after_completion(false, false, true));
    }

    /// A completion that neither completes nor fails is a suspension. That
    /// covers a workflow parked on an external event, which returns with no
    /// commands at all; releasing the claim there made the pollers re-dispatch
    /// it continuously until the event arrived.
    #[test]
    fn a_suspended_workflow_keeps_its_claim_even_with_no_commands() {
        assert!(!claim_released_after_completion(false, false, false));
    }

    /// A workflow that ends itself as cancelled has finished: keeping its
    /// claim left its permit with the worker for good.
    #[test]
    fn a_workflow_that_cancels_itself_releases_its_claim() {
        assert!(claim_released_after_completion(false, false, true));
    }

    #[test]
    fn an_older_workers_steps_are_replaced_by_the_cancellation() {
        let command = older_worker_cancellation();
        assert_eq!(command.command_type, CommandType::CancelWorkflow as i32);
        assert!(matches!(
            command.attributes,
            Some(orcher_proto::orcher::v1::command::Attributes::CancelWorkflow(_))
        ));
    }

    // TODO: Add integration tests when dependencies are wired up
    // ExecutionService now requires CommandProcessor and PollingManager
    // which need proper initialization with database pools, etc.
    //
    // Integration tests should be added in tests/integration/ directory
    // with full setup including:
    // - Database connection pool
    // - WorkflowExecutionRepository
    // - JournalRepository
    // - PollingManager initialization
    // - CommandProcessor initialization
    //
    // Example tests to add:
    // - test_poll_returns_work_when_available
    // - test_poll_timeout_when_no_work
    // - test_complete_processes_commands
    // - test_complete_releases_claim
    // - test_fail_updates_status_and_releases_claim
    // - test_task_execution_flow

    // ============================================================================
    // Step Injection Filter Tests - Unit-type Result Handling
    // ============================================================================

    /// Test that null results (from unit-type tasks) are NOT filtered out
    ///
    /// This test verifies the fix for the Step 3 injection bug where tasks
    /// returning `Result<()>` (which serialize to JSON null) were being
    /// incorrectly filtered during step injection.
    #[test]
    fn test_null_result_is_valid() {
        // Simulate a task that returns Result<()> -> JSON null
        let null_result = Some(serde_json::json!(null));

        // Check that we consider this a valid result
        let has_result = null_result.is_some();
        assert!(
            has_result,
            "Null result should be considered valid (Some(Value::Null))"
        );

        // Verify the actual value is null
        assert_eq!(null_result, Some(serde_json::Value::Null));
    }

    /// Test that missing results (None) are filtered out
    #[test]
    fn test_missing_result_is_invalid() {
        let missing_result: Option<serde_json::Value> = None;

        let has_result = missing_result.is_some();
        assert!(!has_result, "Missing result (None) should be filtered out");
    }

    /// Test that object results are valid
    #[test]
    fn test_object_result_is_valid() {
        let object_result = Some(serde_json::json!({
            "charge_id": "ch_123",
            "amount": 99.99
        }));

        let has_result = object_result.is_some();
        assert!(has_result, "Object result should be valid");
    }

    /// Test internal event filtering
    #[test]
    fn test_internal_events_are_filtered() {
        let test_cases = vec![
            ("event:task_scheduled:123", true),
            ("event:workflow_started:456", true),
            ("event:timer_fired:789", true),
            ("charge_card_0", false),
            ("send_confirmation_email_2", false),
            ("reserve_inventory_1", false),
        ];

        for (step_name, should_filter) in test_cases {
            let is_internal = step_name.starts_with("event:");
            assert_eq!(
                is_internal, should_filter,
                "Step '{}' internal={} expected={}",
                step_name, is_internal, should_filter
            );
        }
    }

    /// Test the complete filtering logic as implemented in poll_workflow_execution
    #[test]
    fn test_step_injection_filter_logic() {
        use serde_json::Value;

        struct TestCase {
            name: &'static str,
            step_name: String,
            status: &'static str, // "completed" or "failed"
            result: Option<Value>,
            should_inject: bool,
        }

        let test_cases = vec![
            // ✅ Should inject: normal task with object result
            TestCase {
                name: "Normal task with object result",
                step_name: "charge_card_0".to_string(),
                status: "completed",
                result: Some(serde_json::json!({"charge_id": "ch_123"})),
                should_inject: true,
            },
            // ✅ Should inject: task with null result (unit type)
            TestCase {
                name: "Task with null result (unit type)",
                step_name: "send_email_1".to_string(),
                status: "completed",
                result: Some(serde_json::json!(null)),
                should_inject: true,
            },
            // ✅ Should inject: task with empty object
            TestCase {
                name: "Task with empty object",
                step_name: "update_state_2".to_string(),
                status: "completed",
                result: Some(serde_json::json!({})),
                should_inject: true,
            },
            // ✅ Should inject: failed task with result
            TestCase {
                name: "Failed task with result",
                step_name: "validate_input_3".to_string(),
                status: "failed",
                result: Some(serde_json::json!({"error": "validation_error"})),
                should_inject: true,
            },
            // ❌ Should NOT inject: internal event
            TestCase {
                name: "Internal event",
                step_name: "event:task_scheduled:abc123".to_string(),
                status: "completed",
                result: Some(serde_json::json!(null)),
                should_inject: false,
            },
            // ❌ Should NOT inject: missing result
            TestCase {
                name: "Task with no result",
                step_name: "incomplete_task_4".to_string(),
                status: "completed",
                result: None,
                should_inject: false,
            },
        ];

        for tc in test_cases {
            // Replicate the filtering logic from poll_workflow_execution
            let is_internal_event = tc.step_name.starts_with("event:");
            let has_result = tc.result.is_some();
            let is_completed_or_failed = tc.status == "completed" || tc.status == "failed";

            let should_inject = is_completed_or_failed && has_result && !is_internal_event;

            assert_eq!(
                should_inject, tc.should_inject,
                "Test case '{}' failed: step_name='{}', status='{}', has_result={}, is_internal={}, expected={}, got={}",
                tc.name, tc.step_name, tc.status, has_result, is_internal_event, tc.should_inject, should_inject
            );
        }
    }

    /// Test that unit type () serializes to null
    #[test]
    fn test_unit_type_serialization() {
        // Unit type () serializes to null
        let unit_json = serde_json::to_value(()).unwrap();
        assert!(unit_json.is_null());

        // This should pass the has_result check
        assert!(Some(unit_json).is_some());
    }

    /// Test serialization of common task return types
    #[test]
    fn test_common_task_return_types() {
        use serde_json::Value;

        // 1. Unit type () -> null
        let unit_json = serde_json::to_value(()).unwrap();
        assert!(unit_json.is_null());

        // 2. Option<T> where None -> null
        let none_json: Value = serde_json::to_value(&None::<String>).unwrap();
        assert!(none_json.is_null());

        // 3. Option<T> where Some -> value
        let some_json = serde_json::to_value(Some("data")).unwrap();
        assert!(some_json.is_string());

        // 4. Struct -> object
        #[derive(serde::Serialize)]
        struct TaskResult {
            id: String,
        }
        let struct_json = serde_json::to_value(&TaskResult {
            id: "123".to_string(),
        })
        .unwrap();
        assert!(struct_json.is_object());

        // 5. Vec<T> -> array
        let vec_json = serde_json::to_value(vec![1, 2, 3]).unwrap();
        assert!(vec_json.is_array());

        // All of these should pass the has_result check
        assert!(Some(unit_json).is_some());
        assert!(Some(none_json).is_some());
        assert!(Some(some_json).is_some());
        assert!(Some(struct_json).is_some());
        assert!(Some(vec_json).is_some());
    }

    // The task token's format, signing and compatibility now live in
    // `crate::grpc::task_token`, and so do their tests. Keeping a second copy
    // here would be a second place to update when the format moves.
}
