//! Workflow execution creation and management.

use orcher_common::types::time::Timestamptz;
use orcher_common::{error::OrcherError, OrcherResult};
use orcher_proto::prost_types;
use orcher_storage::repositories::domains::workflow_execution::logs::{
    WorkflowExecutionLog, WorkflowExecutionLogsRepository,
};
use orcher_storage::repositories::domains::workflow_execution::{
    is_start_request_held, is_workflow_id_held, start_dispatch_payload, WorkflowExecutionRepository,
};
use orcher_storage::schema::types::execution::WorkflowExecution;
use serde_json::Value;
use std::sync::Arc;
use tracing::{debug, error, info, instrument, warn};
use uuid::Uuid;

use crate::engine::queues::outbox::OutboxQueue;
use crate::events::{EventBus, ExecutionEvent};
use crate::services::task_dispatch::TaskDispatchService;
use crate::workflows::JournalService;

/// Whether a start may use a workflow id an earlier run already carried.
///
/// An open run always refuses a start, except under `TerminateIfRunning`; the
/// policies differ in what they allow once it has closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WorkflowIdReusePolicy {
    /// Start once the previous run has closed, however it closed.
    #[default]
    AllowDuplicate,
    /// Start once the previous run has closed without completing.
    AllowDuplicateFailedOnly,
    /// Never start an id a run has carried before.
    RejectDuplicate,
    /// Terminate the open run, if there is one, and start.
    TerminateIfRunning,
}

impl WorkflowIdReusePolicy {
    /// Whether a new run may start after `previous`, the latest run of the id,
    /// which is closed.
    ///
    /// A run left `restarted_fresh` or `reset` is not an outcome — its
    /// workflow went on in a successor — and it is only ever the latest when
    /// that successor was written before runs held their ids. Nothing says
    /// how the workflow ended, so a policy that depends on it refuses.
    pub fn allows_reuse_after(self, previous: &WorkflowExecution) -> bool {
        match self {
            Self::AllowDuplicate | Self::TerminateIfRunning => true,
            Self::RejectDuplicate => false,
            Self::AllowDuplicateFailedOnly => matches!(
                previous.status.as_str(),
                "failed" | "cancelled" | "terminated" | "timed_out"
            ),
        }
    }
}

/// The time limits a run is started under, in whole seconds. `None` asks for
/// no limit of that kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RunTimeouts {
    /// The execution timeout, covering the workflow across all its runs,
    /// measured from when the first run's clocks start — its `submitted_at`,
    /// or for a delayed start the time it is due, so that a delay is not
    /// spent from it (`WorkflowExecution::clocks_start_at`). A run past it ends timed
    /// out and is never retried, and no retry or restart-fresh starts after
    /// it; only a reset, an operator restarting the workflow, starts a new
    /// deadline (`timeout_secs`, `execution_deadline`).
    pub execution_secs: Option<i32>,
    /// The run timeout: how long one run may last, measured from when its
    /// own clocks start, as the execution timeout is. A run past it ends timed out, and its workflow-level
    /// retry policy decides whether it is retried, as for a failure. A
    /// successor run — a retry, a restart-fresh, a reset — carries the limit
    /// and starts its own clock (`run_timeout_secs`).
    pub run_secs: Option<i32>,
    /// The task timeout: how long one activation of the run may stay claimed
    /// by a worker without an answer before the claim lapses and the
    /// activation is handed to another worker. In place of the engine's claim
    /// timeout, for this run only; carried by successors as the run timeout
    /// is (`task_timeout_secs`).
    pub task_secs: Option<i32>,
}

impl RunTimeouts {
    /// The limits a start asked for, as durations on the wire.
    pub fn from_durations(
        execution: Option<&prost_types::Duration>,
        run: Option<&prost_types::Duration>,
        task: Option<&prost_types::Duration>,
    ) -> Self {
        Self {
            execution_secs: limit_secs(execution),
            run_secs: limit_secs(run),
            task_secs: limit_secs(task),
        }
    }

    /// Put these limits on `execution`, which is about to be created.
    pub fn apply_to(self, execution: &mut WorkflowExecution) {
        // An unset limit and a dropped one look identical to the watchers that
        // enforce them — no deadline at all — so every one is stored here.
        execution.timeout_secs = self.execution_secs;
        // The execution timeout covers the workflow across all its runs: one
        // deadline, fixed now, that every successor carries.
        // A delayed start's clocks start when it is due: the delay is not
        // spent from its execution timeout. Applied after the delay is.
        let clocks_start_at = execution.clocks_start_at();
        execution.execution_deadline = self
            .execution_secs
            .map(|secs| clocks_start_at + std::time::Duration::from_secs(secs.max(0) as u64));
        execution.run_timeout_secs = self.run_secs;
        execution.task_timeout_secs = self.task_secs;
    }
}

/// A map of strings — a start's labels or annotations — as a run's column
/// stores it: a JSON object, `None` for an empty map.
pub fn string_map(map: &std::collections::HashMap<String, String>) -> Option<Value> {
    (!map.is_empty()).then(|| serde_json::to_value(map).unwrap_or_default())
}

/// A time limit in whole seconds, as the columns store them.
///
/// `None` means no limit. A duration that is set but shorter than a second
/// rounds *up* to one rather than truncating to zero: zero would be stored as a
/// limit of nothing, and the caller asked for a limit, not for none.
pub fn limit_secs(timeout: Option<&prost_types::Duration>) -> Option<i32> {
    let d = timeout?;
    if d.seconds <= 0 && d.nanos <= 0 {
        return None;
    }
    let secs = if d.nanos > 0 {
        d.seconds + 1
    } else {
        d.seconds
    };
    Some(secs.clamp(1, i32::MAX as i64) as i32)
}

/// What a new run is started with: everything a start asks of the run itself.
///
/// Every way a workflow begins describes its run with one of these —
/// StartWorkflow, BatchStartWorkflow, SendEventWithStartWorkflow, a
/// schedule's run, a child — and the run's row is made from it in one place,
/// [`WorkflowExecutionService::new_run`]. Each path used to build the row
/// itself, and a setting one path stored another dropped: a batch kept no
/// time limits, labels or id reuse policy, a child no time limits. There is no
/// default to fall back on, so a setting added here is named by every path
/// that makes one before it compiles.
#[derive(Debug, Clone, PartialEq)]
pub struct RunStart {
    /// The workflow type to run.
    pub workflow_name: String,
    /// The workflow version.
    pub workflow_version: String,
    /// The namespace to run it in.
    pub namespace: String,
    /// The queue its activations are handed out on.
    pub task_queue: String,
    /// The workflow's input.
    pub input: Value,
    /// The workflow id: held by a top-level run while it is open, a child's
    /// name for itself under its parent. `None` for none.
    pub workflow_id: Option<String>,
    /// Labels for the run, as a JSON object; `None` for none.
    pub labels: Option<Value>,
    /// The run's retry cap, converted from the start's `maximum_attempts`;
    /// `None` is no retry.
    pub max_retries: Option<i32>,
    /// The retry policy JSON a run's row stores.
    pub retry_policy: Option<Value>,
    /// The run's time limits. A run's execution deadline is fixed from its
    /// own start, so a child starts a chain of its own rather than sharing
    /// its parent's.
    pub timeouts: RunTimeouts,
    /// Annotations for the run, as a JSON object of strings; `None` for none.
    /// Carried to the workflow's later runs as its labels are.
    pub annotations: Option<Value>,
    /// How long after its submission the run's first activation is due;
    /// `None` to hand it out at once. Its clocks start when it is due.
    pub start_delay: Option<std::time::Duration>,
    /// The start's request id, which makes it idempotent: a start repeating
    /// it in the organization's namespace is answered with the run the first
    /// made. `None` for a start that gave none, and for a child or a
    /// schedule's run, which are started by the engine, not by a request.
    pub request_id: Option<String>,
}

/// A top-level start: the run, and whether it may take a workflow id an
/// earlier run carried.
#[derive(Debug, Clone)]
pub struct WorkflowStart {
    /// The run to start.
    pub run: RunStart,
    /// Whether it may reuse its workflow id; see [`WorkflowIdReusePolicy`].
    pub reuse_policy: WorkflowIdReusePolicy,
}

/// What came of a [`WorkflowExecutionService::batch_start_workflows`]. A batch
/// is all or nothing: refused, none of it was started.
#[derive(Debug)]
pub enum BatchStartOutcome {
    /// Every run, in the order asked for: started now, or, for a start
    /// repeating an earlier one's request id, the run that one made.
    Started(Vec<WorkflowExecution>),
    /// The start at `index` gives a request id an earlier start of another
    /// workflow gave: `execution` is the run that one made.
    RequestIdInUse {
        /// The refused start's place in the batch.
        index: usize,
        /// The run the request id was first used for.
        execution: WorkflowExecution,
    },
    /// The start at `index` gives the request id the start at `first` does.
    DuplicateRequestId {
        /// The later start.
        index: usize,
        /// The earlier start giving the same request id.
        first: usize,
    },
    /// The start at `index` is refused: its workflow id is in use by
    /// `execution`, open or closed and not to be reused under its policy.
    Refused {
        /// The refused start's place in the batch.
        index: usize,
        /// The run in the way.
        execution: WorkflowExecution,
        /// Whether it is still open.
        open: bool,
    },
    /// The start at `index` names the workflow id the start at `first` does.
    Duplicate {
        /// The later start.
        index: usize,
        /// The earlier start naming the same id.
        first: usize,
    },
}

/// What a workflow id allows a start under a policy, as it stands.
enum IdCheck {
    /// Nothing is in the way.
    Free,
    /// `execution` is in the way, and the start is refused.
    Refused {
        execution: WorkflowExecution,
        open: bool,
    },
    /// `execution` is open and the start terminates it first.
    TerminateFirst(WorkflowExecution),
}

/// What a start's request id says of it, as it stands.
enum RequestCheck {
    /// No run carries it: the start is a new one.
    New,
    /// It repeats the start that made `0`.
    Repeat(WorkflowExecution),
    /// The run `0` carries it, and is not a run of the workflow this start
    /// names.
    InUse(WorkflowExecution),
}

/// What came of a start under a [`WorkflowIdReusePolicy`].
#[derive(Debug)]
pub enum StartOutcome {
    /// The run was started.
    Started(WorkflowExecution),
    /// The start repeats an earlier one's request id: nothing was started,
    /// and this is the run the earlier one made, whatever has become of it.
    /// Its workflow id and reuse policy were not looked at — the start is
    /// the earlier one, answered again.
    Repeated(WorkflowExecution),
    /// The start's request id was given to a start of another workflow:
    /// `execution` is the run that one made.
    RequestIdInUse {
        /// The run the request id was first used for.
        execution: WorkflowExecution,
    },
    /// The id is in use, by `execution`: open, or closed and not to be reused
    /// under the policy.
    Refused {
        /// The run in the way.
        execution: WorkflowExecution,
        /// Whether it is still open, rather than closed and not to be reused.
        open: bool,
    },
}

/// What a cancellation did; see [`WorkflowExecutionService::cancel_execution`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelOutcome {
    /// The run was asked to cancel, and woken to see the request.
    Requested,
    /// The run had been asked already; nothing more was done.
    AlreadyRequested,
    /// The run was cancelled at once: its code never ran, or it was a failure
    /// waiting for its retry.
    Cancelled,
    /// There was no such run, or it had already ended.
    NotLive,
}

/// Service for managing workflow execution lifecycle
///
/// Handles creation, queuing, and lifecycle event publishing for workflow executions.
pub struct WorkflowExecutionService {
    /// Repository for workflow execution persistence
    execution_repository: Arc<WorkflowExecutionRepository>,

    /// Journal service for execution history
    #[allow(dead_code)]
    journal_service: Arc<JournalService>,

    /// Outbox queue for atomic dispatch
    #[allow(dead_code)]
    outbox_queue: Arc<OutboxQueue>,

    /// Event bus for publishing lifecycle events
    event_bus: Arc<EventBus>,

    /// Polling manager for notifying waiting services
    polling_manager: Option<Arc<crate::engine::dispatch::PollingManager>>,

    /// Message queue reference (optional, retained for API compatibility)
    ///
    /// NOTE: This field is no longer used for direct publishing in start_workflow.
    /// The transactional outbox pattern (OutboxProcessor → TaskDispatchService)
    /// now handles all workflow dispatch to ensure exactly-once semantics.
    /// See docs/bugs/MULTI_WORKER_SCALING_BUG.md for details.
    ///
    /// This field is kept for backward compatibility with the public API.
    #[allow(dead_code)]

    /// Workflow execution logs repository for user-facing logs
    logs_repository: Arc<WorkflowExecutionLogsRepository>,

    /// Optional direct dispatch service for sync-match fast path.
    /// When set, start_workflow calls dispatch_with_sync_match immediately
    /// after the outbox write, bypassing the outbox processor poll delay.
    /// The outbox entry is marked completed after successful sync-match.
    /// Uses std::sync::OnceLock for safe one-time initialization after Arc wrapping.
    task_dispatch: std::sync::OnceLock<Arc<TaskDispatchService>>,

    /// Ends the queued tasks a cancellation request interrupts as the request
    /// is made. Unset, they are ended when a poll is refused one or by the
    /// timeout watcher's sweep, which take whatever this misses anyway.
    queued_task_cancellation:
        std::sync::OnceLock<crate::retries::cancelled_before_start::QueuedTaskCancellation>,

    /// Notified when a delayed run is started here, so that the engine's
    /// `DelayedStarts` looks again at when the next one is due.
    delayed_start_wake: Arc<tokio::sync::Notify>,
}

impl WorkflowExecutionService {
    /// Create a new workflow execution service
    ///
    /// # Arguments
    ///
    /// * `execution_repository` - Repository for workflow execution persistence
    /// * `journal_service` - Service for execution journal management
    /// * `outbox_queue` - Queue for atomic task dispatch
    /// * `event_bus` - Event bus for publishing lifecycle events
    /// * `queue` - Optional message queue for fast workflow matching (broker-agnostic)
    /// * `pool` - Database connection pool for logs repository
    pub fn new(
        execution_repository: Arc<WorkflowExecutionRepository>,
        journal_service: Arc<JournalService>,
        outbox_queue: Arc<OutboxQueue>,
        event_bus: Arc<EventBus>,
        pool: sqlx::PgPool,
    ) -> Self {
        let logs_repository = Arc::new(WorkflowExecutionLogsRepository::new(pool));
        Self {
            execution_repository,
            journal_service,
            outbox_queue,
            event_bus,
            polling_manager: None,
            logs_repository,
            task_dispatch: std::sync::OnceLock::new(),
            queued_task_cancellation: std::sync::OnceLock::new(),
            delayed_start_wake: Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// What a delayed start made here notifies: give it to the engine's
    /// `DelayedStarts`, so that a run due sooner than the one it is sleeping
    /// towards is handed out when it is due.
    pub fn delayed_start_wake(&self) -> Arc<tokio::sync::Notify> {
        self.delayed_start_wake.clone()
    }

    /// Set what ends the queued tasks a cancellation request interrupts, so
    /// they are ended as the request is made. Idempotent; a second call is
    /// ignored.
    pub fn set_queued_task_cancellation(
        &self,
        cancellation: crate::retries::cancelled_before_start::QueuedTaskCancellation,
    ) {
        let _ = self.queued_task_cancellation.set(cancellation);
    }

    /// Set the task dispatch service for sync-match fast path.
    /// When set, workflow starts will attempt direct dispatch to waiting workers,
    /// bypassing the outbox processor poll delay.
    pub fn set_task_dispatch(&self, task_dispatch: Arc<TaskDispatchService>) {
        let _ = self.task_dispatch.set(task_dispatch);
    }

    /// Set the polling manager for notifying waiting services
    ///
    /// This should be called after construction to wire up notifications.
    /// When a workflow is created, the polling manager will notify any
    /// services waiting on the task queue.
    ///
    /// # Arguments
    ///
    /// * `polling_manager` - The polling manager to use for notifications
    pub fn with_polling_manager(
        mut self,
        polling_manager: Arc<crate::engine::dispatch::PollingManager>,
    ) -> Self {
        self.polling_manager = Some(polling_manager);
        self
    }

    /// Start a workflow execution.
    ///
    /// Creates the run's row atomically with its first dispatch and journal
    /// entry, wakes the polls waiting on its queue, and publishes its start.
    /// A run with a parent is that parent's child.
    ///
    /// `organization_id` is passed explicitly rather than picked up from
    /// ambient state. This is a write, and a row stamped with the wrong tenant
    /// — or none — stays wrong: a later query filtering by organisation simply
    /// will not find it, and nothing reports that as an error. An explicit
    /// argument makes every caller answer the question, including callers that
    /// do not exist yet.
    ///
    /// `None` is legitimate. Single-tenant deployments have no organisations,
    /// and internal callers inherit whatever the original execution recorded.
    #[instrument(skip(self, start), fields(
        workflow_name = %start.workflow_name,
        namespace = %start.namespace,
        task_queue = %start.task_queue
    ))]
    pub async fn start_workflow(
        &self,
        start: RunStart,
        parent_execution_id: Option<String>,
        organization_id: Option<Uuid>,
    ) -> OrcherResult<WorkflowExecution> {
        self.start_workflow_with_id(
            Uuid::new_v4().to_string(),
            start,
            parent_execution_id,
            organization_id,
        )
        .await
    }

    /// The row of a new run of `start`, not yet written: the one place a run
    /// is made from what its start asked for.
    ///
    /// A top-level run holds its workflow id: while it is open, no other run
    /// in the organization's namespace may start under the same id, and a
    /// start that races another is refused by the insert itself (V059). A
    /// child's id is its parent's name for it, unique per parent only, so a
    /// child holds nothing.
    fn new_run(
        &self,
        start: &RunStart,
        execution_id: String,
        parent_execution_id: Option<String>,
        root_execution_id: Option<String>,
        organization_id: Option<Uuid>,
    ) -> WorkflowExecution {
        // Destructured whole, so a setting added to RunStart is a compile
        // error here until it is stored.
        let RunStart {
            workflow_name,
            workflow_version,
            namespace,
            task_queue,
            input,
            workflow_id,
            labels,
            max_retries,
            retry_policy,
            timeouts,
            annotations,
            start_delay,
            request_id,
        } = start;

        let holds_workflow_id = parent_execution_id.is_none() && workflow_id.is_some();

        let mut builder = WorkflowExecution::builder()
            .execution_id(execution_id.clone())
            .workflow_name(workflow_name.clone())
            .workflow_type(workflow_name.clone())
            .workflow_version(workflow_version.clone())
            .namespace(namespace.clone())
            .task_queue(task_queue.clone())
            .parameters(input.clone())
            // A child workflow is triggered by another workflow. The value must
            // be one of the workflow_exec_trigger_check set (manual, api,
            // scheduled, event, webhook, workflow, cli) — "child_workflow" is
            // not, and violated the constraint, blocking every child start.
            .trigger_type(if parent_execution_id.is_some() {
                "workflow".to_string()
            } else {
                "api".to_string()
            })
            // Workflow-level retry is opt-in: a start without a retry policy
            // is not retried. An explicit policy's `maximum_attempts` has been
            // converted to the retry count by `convert_retry_policy`.
            .max_retries(max_retries.unwrap_or(0));
        if let Some(wf_id) = workflow_id {
            builder = builder.workflow_id(wf_id.clone());
        }
        if let Some(ref parent_id) = parent_execution_id {
            builder = builder.parent_execution_id(parent_id.clone());
        }
        if let Some(attrs) = labels {
            builder = builder.labels(attrs.clone());
        }
        if let Some(policy) = retry_policy {
            builder = builder.retry_policy(policy.clone());
        }
        if let Some(org) = organization_id {
            builder = builder.organization_id(org);
        }

        let mut execution = builder.build();
        if let Some(annotations) = annotations {
            execution.annotations = annotations.clone();
        }
        execution.start_request_id = request_id.clone();
        execution.holds_workflow_id = holds_workflow_id;
        if let (Some(wf_id), true) = (workflow_id, holds_workflow_id) {
            execution.id = orcher_common::types::identifiers::ExecutionId::from_uuid(
                self.execution_repository.id_beside_workflow_id(
                    organization_id,
                    namespace,
                    wf_id,
                    |_| Uuid::new_v4(),
                ),
            );
        }
        // Lowercase status for consistent SQL querying.
        execution.status = "running".to_string();
        execution.root_execution_id = root_execution_id;
        execution.started_at = Some(Timestamptz::now());
        // A delayed run is written now and handed out once it is due: no
        // claim takes it while it awaits its start, and the delayed-start
        // release lets it go then (`DelayedStarts`). It starts when it is
        // due, and its clocks with it.
        if let Some(delay) = start_delay.filter(|delay| !delay.is_zero()) {
            let due = execution.submitted_at + delay;
            execution.start_after = Some(due);
            execution.awaiting_start = true;
            execution.started_at = Some(due);
        }
        // The limits the caller asked for. `None` means no limit, which is
        // what the watchers treat as "never expires" — so an unset limit and a
        // dropped one look identical.
        timeouts.apply_to(&mut execution);
        execution
    }

    /// [`start_workflow`](Self::start_workflow) under an execution id the
    /// caller has already chosen.
    ///
    /// For a start that must be recorded somewhere before the execution
    /// exists — a child, whose tracking row names its run — so that both
    /// carry the same id. The id must be fresh: it is the execution's primary
    /// identifier.
    pub async fn start_workflow_with_id(
        &self,
        execution_id: String,
        start: RunStart,
        parent_execution_id: Option<String>,
        organization_id: Option<Uuid>,
    ) -> OrcherResult<WorkflowExecution> {
        info!(
            workflow_name = %start.workflow_name,
            execution_id = %execution_id,
            namespace = %start.namespace,
            parent_execution_id = ?parent_execution_id,
            "Starting workflow execution"
        );

        // A child is in its parent's tree; a top-level run is its own root.
        let root_execution_id = if let Some(ref parent_id) = parent_execution_id {
            self.get_root_execution_id(parent_id).await?
        } else {
            Some(execution_id.clone())
        };

        let execution = self.new_run(
            &start,
            execution_id.clone(),
            parent_execution_id.clone(),
            root_execution_id,
            organization_id,
        );
        let task_queue = start.task_queue.as_str();
        let namespace = start.namespace.as_str();
        let workflow_name = start.workflow_name.as_str();

        let outbox_payload = start_dispatch_payload(&execution, task_queue);

        // Generate idempotency key for the dispatch task
        let idempotency_key = format!("workflow_start_{}", execution.execution_id);

        // The execution, its dispatch and its first journal entry, in one
        // transaction.
        let (created, task_id) = self
            .execution_repository
            .create_with_outbox_task(&execution, outbox_payload, idempotency_key, task_queue)
            .await
            .map_err(|e| {
                error!(
                    execution_id = %execution_id,
                    error = %e,
                    "Failed to atomically create workflow execution and outbox task"
                );
                e
            })?;

        debug!(
            execution_id = %created.execution_id,
            task_id = %task_id,
            "Workflow execution and outbox task created atomically"
        );

        self.announce_started(&created, parent_execution_id.as_deref())
            .await;

        info!(
            workflow_name = %workflow_name,
            execution_id = %execution_id,
            namespace = %namespace,
            task_id = %task_id,
            "Workflow execution started successfully with atomic dispatch"
        );

        // Write the user-facing log line for the submission. The run's start
        // is logged once, by the claim that first hands it to a worker; this
        // used to say "started" too, so every run read as started twice over.
        let start_log = WorkflowExecutionLog::info(
            execution_id.clone(),
            format!("Workflow '{}' execution submitted", workflow_name),
        )
        .with_source("WorkflowExecutionService".to_string())
        .with_metadata(serde_json::json!({
            "workflow_name": workflow_name,
            "workflow_version": start.workflow_version,
            "namespace": namespace,
            "task_queue": task_queue,
        }));

        // Non-blocking: log insertion failure shouldn't fail workflow start
        if let Err(e) = self.logs_repository.add_log(&start_log).await {
            warn!(
                execution_id = %execution_id,
                workflow_name = %workflow_name,
                error = %e,
                "Failed to add workflow start log entry (non-fatal)"
            );
        }

        Ok(created)
    }

    /// Wake what a new run concerns: the outbox processor that dispatches it,
    /// the workflow polls waiting on its queue, and the event bus's
    /// subscribers. Every start does this, a batch's runs included; a batch
    /// used to wake no workflow poll, so its runs waited for a poll's next
    /// periodic look.
    async fn announce_started(
        &self,
        created: &WorkflowExecution,
        parent_execution_id: Option<&str>,
    ) {
        // A delayed run is handed out when it is due, not now: the release
        // looks again at when that is, and wakes the run's queue then.
        if created.awaiting_start {
            self.delayed_start_wake.notify_one();
        } else {
            // Wake the outbox processor so it picks up the new task without
            // waiting for its next poll interval.
            self.outbox_queue.notify_new_tasks();
            // The outbox wake reaches task pollers, not the workflow polls.
            if let Some(ref polling_manager) = self.polling_manager {
                polling_manager
                    .notify_work_available(&created.task_queue, &created.namespace)
                    .await;
            }
        }
        // Best-effort: a lost event is not a lost start.
        if let Err(e) = self
            .publish_workflow_started_event(created, parent_execution_id)
            .await
        {
            warn!(
                execution_id = %created.execution_id,
                error = %e,
                "Failed to publish workflow started event (best-effort, continuing)"
            );
        }
    }

    /// What `run`'s request id, if it gives one, says of its start.
    ///
    /// A request id is remembered on the run its start made, in the
    /// organization's namespace (`start_request_id`, V069), for as long as
    /// that run is kept. A start repeating it is the same start only if it
    /// names the same workflow — type and id — as the run carrying it; a
    /// start without a workflow id is given one derived from its request id,
    /// so a repeat of it names the same one. Anything else is a request id
    /// reused for another start, which is refused rather than answered with
    /// a run the caller did not ask for.
    async fn check_start_request(
        &self,
        run: &RunStart,
        organization_id: Option<Uuid>,
    ) -> OrcherResult<RequestCheck> {
        let Some(request_id) = run.request_id.as_deref() else {
            return Ok(RequestCheck::New);
        };
        let Some(earlier) = self
            .execution_repository
            .find_by_start_request(organization_id, &run.namespace, request_id)
            .await?
        else {
            return Ok(RequestCheck::New);
        };
        Ok(
            if earlier.workflow_type == run.workflow_name && earlier.workflow_id == run.workflow_id
            {
                RequestCheck::Repeat(earlier)
            } else {
                RequestCheck::InUse(earlier)
            },
        )
    }

    /// Whether `execution` is the run `run` itself made: the same request id,
    /// for the same workflow. The request id is looked up before the
    /// workflow id, so a racing repeat can find nothing by its request id and
    /// then, once the start it repeats has committed, find that start's run
    /// holding the workflow id. That run is this start's answer, not a run in
    /// its way: refusing the repeat, or terminating the run under
    /// TerminateIfRunning, would treat one start as two.
    fn made_by(run: &RunStart, execution: &WorkflowExecution) -> bool {
        run.request_id.is_some()
            && execution.start_request_id == run.request_id
            && execution.workflow_type == run.workflow_name
            && execution.workflow_id == run.workflow_id
    }

    /// What `workflow_id` allows a start under `policy`, as it stands.
    ///
    /// The checks give a refusal its reason; they do not make it safe. Two
    /// starts can both find the id free and both insert — the unique index
    /// lets one of them through.
    async fn check_workflow_id(
        &self,
        policy: WorkflowIdReusePolicy,
        organization_id: Option<Uuid>,
        namespace: &str,
        workflow_id: &str,
    ) -> OrcherResult<IdCheck> {
        if let Some(open) = self
            .execution_repository
            .open_run_holding(organization_id, namespace, workflow_id)
            .await?
        {
            return Ok(if policy == WorkflowIdReusePolicy::TerminateIfRunning {
                IdCheck::TerminateFirst(open)
            } else {
                IdCheck::Refused {
                    execution: open,
                    open: true,
                }
            });
        }
        if let Some(previous) = self
            .execution_repository
            .latest_run_named(organization_id, namespace, workflow_id)
            .await?
        {
            if !policy.allows_reuse_after(&previous) {
                return Ok(IdCheck::Refused {
                    execution: previous,
                    open: false,
                });
            }
        }
        Ok(IdCheck::Free)
    }

    /// Terminate `open`, a run a TerminateIfRunning start replaces, through
    /// the same guarded write TerminateWorkflow uses. Whether it ended: a run
    /// that closes meanwhile is simply no longer in the way, and one it
    /// cannot terminate — a failure not yet decided — is looked at again.
    async fn terminate_in_the_way(&self, open: &WorkflowExecution) -> OrcherResult<bool> {
        let terminated = self
            .execution_repository
            .terminate(&open.execution_id)
            .await?;
        info!(
            workflow_id = ?open.workflow_id,
            execution_id = %open.execution_id,
            terminated,
            "Terminating the open run of a workflow id to start it again"
        );
        Ok(terminated)
    }

    /// Start a top-level workflow under its workflow id, as its policy allows.
    ///
    /// A workflow id names at most one open run in an organization's
    /// namespace. An open run in the way refuses the start under every policy
    /// but [`WorkflowIdReusePolicy::TerminateIfRunning`], which terminates it
    /// first; a closed one refuses it only if the policy says so. Either
    /// refusal names the run in the way, so the caller can reach it. A start
    /// without a workflow id is started as it is.
    ///
    /// A start giving a request id is looked up by it first: one repeating an
    /// earlier start is answered with the run that start made, open or
    /// closed, under any policy — it is not a second start, so neither the
    /// id's hold nor its reuse policy applies, and a TerminateIfRunning
    /// repeat does not end the run it started. Two repeats racing each other
    /// are decided by the request id's unique index (V069), and the loser is
    /// answered with the winner's run.
    pub async fn start_workflow_under_policy(
        &self,
        start: WorkflowStart,
        organization_id: Option<Uuid>,
    ) -> OrcherResult<StartOutcome> {
        let WorkflowStart { run, reuse_policy } = start;
        // Each pass either starts the run, is refused, or loses a race to
        // another start and looks again. Losing more than a few times in a row
        // means the id is being fought over; say so rather than spin.
        const PASSES: usize = 5;
        for _ in 0..PASSES {
            match self.check_start_request(&run, organization_id).await? {
                RequestCheck::New => {}
                RequestCheck::Repeat(execution) => return Ok(StartOutcome::Repeated(execution)),
                RequestCheck::InUse(execution) => {
                    return Ok(StartOutcome::RequestIdInUse { execution })
                }
            }
            let Some(workflow_id) = run.workflow_id.clone() else {
                match self
                    .start_workflow(run.clone(), None, organization_id)
                    .await
                {
                    Ok(execution) => return Ok(StartOutcome::Started(execution)),
                    Err(e) if is_start_request_held(&e) => continue,
                    Err(e) => return Err(e),
                }
            };
            match self
                .check_workflow_id(reuse_policy, organization_id, &run.namespace, &workflow_id)
                .await?
            {
                IdCheck::Refused { execution, .. } | IdCheck::TerminateFirst(execution)
                    if Self::made_by(&run, &execution) =>
                {
                    return Ok(StartOutcome::Repeated(execution));
                }
                IdCheck::Refused { execution, open } => {
                    return Ok(StartOutcome::Refused { execution, open })
                }
                IdCheck::TerminateFirst(open) => {
                    if !self.terminate_in_the_way(&open).await? {
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    }
                    continue;
                }
                IdCheck::Free => {}
            }

            match self
                .start_workflow(run.clone(), None, organization_id)
                .await
            {
                Ok(execution) => return Ok(StartOutcome::Started(execution)),
                Err(e) if is_workflow_id_held(&e) || is_start_request_held(&e) => {
                    debug!(
                        workflow_id = %workflow_id,
                        "Another start of this workflow id got there first"
                    );
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
        Err(OrcherError::internal(format!(
            "workflow id {} changed hands on each of {} attempts to start it; try again",
            run.workflow_id.as_deref().unwrap_or_default(),
            PASSES
        )))
    }

    /// Start several top-level workflows at once, each as a single start of
    /// it would: its run made by [`new_run`](Self::new_run) and its workflow
    /// id taken as its own reuse policy allows.
    ///
    /// All or nothing. Every refusal is decided before anything changes, so a
    /// refused batch starts nothing and terminates nothing; once none is
    /// refused, the runs a TerminateIfRunning start replaces are terminated,
    /// and the runs, their dispatches and journal entries are committed
    /// together (one transaction per database). A start racing the batch for
    /// one of its ids is refused by the unique index, and the batch looks
    /// again.
    ///
    /// `organization_id` is the caller's: it scopes the ids, and is stamped on
    /// every run.
    ///
    /// A start giving a request id an earlier start gave is that start
    /// repeated, as for a single start: its place in the result is the run
    /// the earlier start made, and nothing is started or checked for it. A
    /// batch retried after its answer was lost is so answered with the runs
    /// it started, and one whose starts were only partly committed — its
    /// databases commit apart — starts the rest.
    pub async fn batch_start_workflows(
        &self,
        starts: Vec<WorkflowStart>,
        organization_id: Option<Uuid>,
    ) -> OrcherResult<BatchStartOutcome> {
        if starts.is_empty() {
            return Ok(BatchStartOutcome::Started(vec![]));
        }

        // One request id given twice would be one start asked for twice over,
        // with two different answers.
        let mut requested = std::collections::HashMap::new();
        for (index, start) in starts.iter().enumerate() {
            if let Some(ref request_id) = start.run.request_id {
                if let Some(&first) =
                    requested.get(&(start.run.namespace.as_str(), request_id.as_str()))
                {
                    return Ok(BatchStartOutcome::DuplicateRequestId { index, first });
                }
                requested.insert((start.run.namespace.as_str(), request_id.as_str()), index);
            }
        }

        // One id named twice could never start: the second would be in the
        // first's way.
        let mut named = std::collections::HashMap::new();
        for (index, start) in starts.iter().enumerate() {
            if let Some(ref wf_id) = start.run.workflow_id {
                if let Some(&first) = named.get(&(start.run.namespace.as_str(), wf_id.as_str())) {
                    return Ok(BatchStartOutcome::Duplicate { index, first });
                }
                named.insert((start.run.namespace.as_str(), wf_id.as_str()), index);
            }
        }

        const PASSES: usize = 5;
        // Under the default policy an open run is refused by the unique index
        // whatever is checked here, and a closed run is always reusable, so a
        // default-policy start is looked at only once the index has refused
        // the batch — to name the run in its way.
        let mut check_every_id = false;
        for pass in 0..PASSES {
            // The starts that repeat earlier ones, answered with the runs
            // those made; looked at again each pass, as a racing repeat may
            // have committed meanwhile.
            let mut repeated: Vec<Option<WorkflowExecution>> = Vec::with_capacity(starts.len());
            for (index, start) in starts.iter().enumerate() {
                match self
                    .check_start_request(&start.run, organization_id)
                    .await?
                {
                    RequestCheck::New => repeated.push(None),
                    RequestCheck::Repeat(execution) => repeated.push(Some(execution)),
                    RequestCheck::InUse(execution) => {
                        return Ok(BatchStartOutcome::RequestIdInUse { index, execution })
                    }
                }
            }

            let mut in_the_way = Vec::new();
            for (index, start) in starts.iter().enumerate() {
                if repeated[index].is_some() {
                    continue;
                }
                let Some(ref wf_id) = start.run.workflow_id else {
                    continue;
                };
                if start.reuse_policy == WorkflowIdReusePolicy::AllowDuplicate && !check_every_id {
                    continue;
                }
                match self
                    .check_workflow_id(
                        start.reuse_policy,
                        organization_id,
                        &start.run.namespace,
                        wf_id,
                    )
                    .await?
                {
                    IdCheck::Free => {}
                    IdCheck::Refused { execution, .. } | IdCheck::TerminateFirst(execution)
                        if Self::made_by(&start.run, &execution) =>
                    {
                        repeated[index] = Some(execution);
                    }
                    IdCheck::Refused { execution, open } => {
                        return Ok(BatchStartOutcome::Refused {
                            index,
                            execution,
                            open,
                        })
                    }
                    IdCheck::TerminateFirst(open) => in_the_way.push(open),
                }
            }
            if !in_the_way.is_empty() {
                let mut all_ended = true;
                for open in &in_the_way {
                    all_ended &= self.terminate_in_the_way(open).await?;
                }
                if !all_ended {
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
                continue;
            }

            let executions: Vec<WorkflowExecution> = starts
                .iter()
                .zip(&repeated)
                .filter(|(_, repeat)| repeat.is_none())
                .map(|(start, _)| {
                    let execution_id = Uuid::new_v4().to_string();
                    self.new_run(
                        &start.run,
                        execution_id.clone(),
                        None,
                        Some(execution_id),
                        organization_id,
                    )
                })
                .collect();

            match self
                .execution_repository
                .batch_create_with_outbox(&executions)
                .await
            {
                Ok(results) => {
                    let mut created = results.into_iter().map(|(exec, _)| exec);
                    let mut runs = Vec::with_capacity(repeated.len());
                    for repeat in repeated {
                        match repeat {
                            Some(execution) => runs.push(execution),
                            None => {
                                let execution =
                                    created.next().expect("a run created for every new start");
                                self.announce_started(&execution, None).await;
                                runs.push(execution);
                            }
                        }
                    }
                    info!(count = runs.len(), "Batch workflow start completed");
                    return Ok(BatchStartOutcome::Started(runs));
                }
                Err(e)
                    if (is_workflow_id_held(&e) || is_start_request_held(&e))
                        && pass + 1 < PASSES =>
                {
                    debug!("A workflow id in the batch was taken meanwhile; looking again");
                    check_every_id = true;
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
        Err(OrcherError::internal(format!(
            "the batch's workflow ids changed hands on each of {PASSES} attempts to start it; \
             try again"
        )))
    }

    /// Queue workflow execution for service pickup
    ///
    /// ⚠️ DEPRECATED: This method is no longer used.
    ///
    /// Use `create_with_outbox_task` in the repository for atomic execution + outbox task creation.
    /// This ensures atomicity and prevents orphan executions or lost dispatches.
    ///
    /// # Task #5: Atomic Outbox Transaction
    ///
    /// This method has been replaced by the atomic transaction approach in `start_workflow`.
    /// Keeping this for backward compatibility but it should not be called.
    #[deprecated(
        since = "0.1.0",
        note = "Use create_with_outbox_task for atomic transaction"
    )]
    #[allow(dead_code)]
    async fn queue_workflow_execution(
        &self,
        execution: &WorkflowExecution,
        _task_queue: &str,
    ) -> OrcherResult<()> {
        warn!(
            execution_id = %execution.execution_id,
            "queue_workflow_execution called but is deprecated - use atomic transaction instead"
        );

        // This method is deprecated and should not be used
        // The atomic approach in start_workflow is now the standard
        Ok(())
    }

    /// Publish workflow started event to EventBus
    ///
    /// Notifies subscribers that a workflow has started execution.
    async fn publish_workflow_started_event(
        &self,
        execution: &WorkflowExecution,
        _parent_execution_id: Option<&str>,
    ) -> OrcherResult<()> {
        debug!(
            execution_id = %execution.execution_id,
            "Publishing workflow started event"
        );

        // Publish ExecutionEvent::Started
        if let Err(e) = self
            .event_bus
            .publish_execution_event(ExecutionEvent::Started {
                execution_id: execution.execution_id.clone(),
                started_at: execution.started_at.unwrap_or_else(Timestamptz::now),
            })
            .await
        {
            warn!(
                error = %e,
                execution_id = %execution.execution_id,
                "Failed to publish execution started event"
            );
        } else {
            debug!(
                execution_id = %execution.execution_id,
                "Published workflow started event"
            );
        }

        Ok(())
    }

    /// Get root execution ID from parent execution
    ///
    /// Traverses the parent chain to find the root execution ID.
    async fn get_root_execution_id(
        &self,
        parent_execution_id: &str,
    ) -> OrcherResult<Option<String>> {
        debug!(
            parent_execution_id = %parent_execution_id,
            "Getting root execution ID from parent"
        );

        // Look up parent execution
        let parent = self
            .execution_repository
            .find_by_execution_id(parent_execution_id)
            .await?
            .ok_or_else(|| OrcherError::NotFound {
                resource_type: "WorkflowExecution".to_string(),
                id: parent_execution_id.to_string(),
            })?;

        // Return parent's root (or parent ID if it's the root)
        let root_id = parent.root_execution_id.or(Some(parent.execution_id));

        debug!(
            parent_execution_id = %parent_execution_id,
            root_execution_id = ?root_id,
            "Retrieved root execution ID"
        );

        Ok(root_id)
    }

    /// Cancel the run `execution_id`: the one path every cancellation takes,
    /// whoever asks for it.
    ///
    /// A run whose code has run is asked to cancel, so it can clean up: the
    /// request is recorded with its reason and optional cleanup deadline,
    /// journaled once for the workflow to observe, and the run woken. A run
    /// whose code never ran, or a failure waiting for its retry, has nothing to
    /// clean up and is cancelled at once. Asking again is harmless and does not
    /// wake the run a second time.
    pub async fn cancel_execution(
        &self,
        execution_id: &str,
        reason: &str,
        cleanup_deadline: Option<Timestamptz>,
    ) -> OrcherResult<CancelOutcome> {
        if let Some(target) = self
            .execution_repository
            .request_cancel(execution_id, reason, cleanup_deadline)
            .await?
        {
            // Once only, so a request retried after a failure here, or two
            // racing, leave a single entry for the workflow to observe; and
            // appended again if an earlier attempt failed before journaling.
            let journaled = self
                .journal_service
                .append_workflow_cancel_requested(
                    &target.workflow_id.to_string(),
                    execution_id,
                    reason,
                )
                .await?;
            if !(target.newly_requested || journaled) {
                return Ok(CancelOutcome::AlreadyRequested);
            }
            // The tasks the run had queued are the work the request
            // interrupts: ended now, after the request is journaled, so the
            // wake-up below tells the run of both. One that fails is left to
            // the timeout watcher's sweep; the start refuses them meanwhile.
            if let Some(cancellation) = self.queued_task_cancellation.get() {
                if let Err(e) = cancellation.cancel_queued_tasks(target.workflow_id).await {
                    warn!(
                        execution_id = %execution_id,
                        error = %e,
                        "Cancellation requested, but its queued tasks could not be cancelled yet"
                    );
                }
            }
            // Woken as an event wakes it: a parked run is claimable again, and
            // one whose activation is out is handed another when it reports.
            if let Err(e) = self
                .execution_repository
                .release_claim_to_resume(execution_id)
                .await
            {
                warn!(
                    execution_id = %execution_id,
                    error = %e,
                    "Cancellation requested, but the run could not be woken; it sees the request on its next activation"
                );
            }
            if let Some(ref polling_manager) = self.polling_manager {
                polling_manager
                    .notify_work_available(&target.task_queue, &target.namespace)
                    .await;
            }
            info!(execution_id = %execution_id, reason = %reason, "Workflow asked to cancel");
            return Ok(CancelOutcome::Requested);
        }

        if !self.execution_repository.cancel(execution_id).await? {
            debug!(
                execution_id = %execution_id,
                "Missing or already ended, nothing to cancel"
            );
            return Ok(CancelOutcome::NotLive);
        }
        self.event_bus
            .publish_execution_event(ExecutionEvent::Cancelled {
                execution_id: execution_id.to_string(),
                reason: reason.to_string(),
                cancelled_at: Timestamptz::now(),
            })
            .await
            .ok();
        info!(execution_id = %execution_id, "Workflow cancelled");
        Ok(CancelOutcome::Cancelled)
    }

    /// Cancel a child workflow, as an API cancellation would: asked if its
    /// code has run, ended otherwise. Returns whether it was asked or ended.
    pub async fn cancel_child_workflow(
        &self,
        execution_id: &str,
        reason: &str,
    ) -> OrcherResult<bool> {
        Ok(self.cancel_execution(execution_id, reason, None).await? != CancelOutcome::NotLive)
    }

    /// The execution `execution_id`, if it exists.
    pub async fn find_execution(
        &self,
        execution_id: &str,
    ) -> OrcherResult<Option<WorkflowExecution>> {
        self.execution_repository
            .find_by_execution_id(execution_id)
            .await
    }

    /// The execution `parent_execution_id` started as `child_workflow_id`, if
    /// one exists.
    pub async fn find_child(
        &self,
        parent_execution_id: &str,
        child_workflow_id: &str,
    ) -> OrcherResult<Option<WorkflowExecution>> {
        self.execution_repository
            .find_child(parent_execution_id, child_workflow_id)
            .await
    }

    /// Start a child workflow execution
    ///
    /// Convenience method for starting child workflows with parent context.
    /// This is called by ChildWorkflowManager after creating the child workflow record.
    ///
    /// # Arguments
    ///
    /// * `execution_id` - The id the child's tracking record already holds
    /// * `child_workflow_id` - The child workflow identifier
    /// * `child_workflow_type` - Type/name of the child workflow
    /// * `workflow_version` - Version of the workflow
    /// * `namespace` - Namespace for isolation
    /// * `task_queue` - Task queue for service polling
    /// * `input` - Child workflow input parameters
    /// * `parent_execution_id` - Parent execution identifier
    /// * `settings` - Retry cap and policy, labels and time limits the parent asked for
    #[instrument(skip(self, input, settings), fields(
        child_workflow_id = %child_workflow_id,
        child_workflow_type = %child_workflow_type,
        parent_execution_id = %parent_execution_id
    ))]
    #[allow(clippy::too_many_arguments)]
    pub async fn start_child_workflow(
        &self,
        execution_id: &str,
        child_workflow_id: &str,
        child_workflow_type: &str,
        workflow_version: &str,
        namespace: &str,
        task_queue: &str,
        input: Value,
        parent_execution_id: &str,
        settings: crate::workflows::ChildRunSettings,
    ) -> OrcherResult<WorkflowExecution> {
        info!(
            child_workflow_id = %child_workflow_id,
            child_workflow_type = %child_workflow_type,
            parent_execution_id = %parent_execution_id,
            "Starting child workflow execution"
        );

        // Start workflow with parent context
        // A child belongs to whoever owns its parent. Derived rather than
        // taken as an argument: it is not a caller's decision, and letting a
        // caller supply it is an invitation to supply the wrong one.
        //
        // A parent that cannot be found is an error, not an unowned child: the
        // parent is the one issuing this command, so its row not being there
        // is transient or a bug, and either way the child must not be created
        // belonging to no one.
        let organization_id = self
            .execution_repository
            .find_by_execution_id(parent_execution_id)
            .await?
            .ok_or_else(|| OrcherError::not_found("parent execution", parent_execution_id))?
            .organization_id;

        // Destructured whole, so a setting added to ChildRunSettings is a
        // compile error here until it reaches the run.
        let crate::workflows::ChildRunSettings {
            max_retries,
            retry_policy,
            labels,
            annotations,
            timeouts,
        } = settings;
        let execution = self
            .start_workflow_with_id(
                execution_id.to_string(),
                RunStart {
                    workflow_name: child_workflow_type.to_string(),
                    workflow_version: workflow_version.to_string(),
                    namespace: namespace.to_string(),
                    task_queue: task_queue.to_string(),
                    input,
                    // The child carries the parent-assigned correlation id
                    // (e.g. "child_0") as its workflow_id. On completion this
                    // id is echoed back in the ChildWorkflowExecutionCompleted
                    // event so the parent can match it to its child call on
                    // replay. workflow_id is not unique (only execution_id
                    // is), so per-parent ids like "child_0" are safe.
                    workflow_id: Some(child_workflow_id.to_string()),
                    labels,
                    // The child's own retry policy, stored the way a top-level
                    // start stores one, so the retry scheduler treats both alike.
                    max_retries,
                    retry_policy,
                    // The child's own time limits, from its start command. Its
                    // execution deadline runs from its own start: a child
                    // starts a chain of its own and does not inherit its
                    // parent's limits.
                    timeouts,
                    annotations,
                    // A child's start command asks for no delay, and is
                    // made once by its parent's command, not by a request.
                    start_delay: None,
                    request_id: None,
                },
                Some(parent_execution_id.to_string()),
                organization_id,
            )
            .await?;

        info!(
            child_workflow_id = %child_workflow_id,
            execution_id = %execution.execution_id,
            parent_execution_id = %parent_execution_id,
            "Child workflow execution started"
        );

        Ok(execution)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_service_construction() {
        // This is a compile-time test to ensure the service can be constructed
        // Actual tests require database and would be integration tests
    }
}
