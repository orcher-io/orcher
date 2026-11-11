//! Task and workflow execution storage types.

use orcher_common::types::identifiers::{ExecutionId, TaskExecutionId, WorkflowId};
use orcher_common::types::time::Timestamptz;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskExecution {
    pub id: TaskExecutionId,
    pub organization_id: Option<Uuid>,
    pub task_id: String,
    pub task_type: String,
    pub workflow_id: WorkflowId,
    pub execution_id: ExecutionId,
    pub task_queue: String,
    pub namespace: String,
    /// Self-reported by the claiming service
    pub service_identity: Option<String>,
    /// 1 for first attempt, 2+ for retries
    pub attempt_number: i32,
    /// scheduled | started | completed | failed | timed_out | cancelled
    pub status: String,
    pub scheduled_at: Timestamptz,
    pub started_at: Option<Timestamptz>,
    pub completed_at: Option<Timestamptz>,
    /// Format: `workflow_id:execution_id:task_id:sequence:attempt:started_at_ms`
    pub task_token: Option<String>,
    pub schedule_to_start_timeout_secs: Option<i32>,
    pub start_to_close_timeout_secs: Option<i32>,
    pub heartbeat_timeout_secs: Option<i32>,
    pub input: Option<Vec<u8>>,
    pub result: Option<Vec<u8>>,
    pub error_message: Option<String>,
    pub error_type: Option<String>,
    pub error_details: Option<serde_json::Value>,
    pub retry_policy: Option<serde_json::Value>,
    pub headers: serde_json::Value,
    pub metadata: serde_json::Value,
    pub last_heartbeat_at: Option<Timestamptz>,
    pub heartbeat_details: Option<Vec<u8>>,
    pub created_at: Timestamptz,
    pub updated_at: Timestamptz,
}

impl orcher_common::types::dispatch::EagerDispatchable for TaskExecution {
    fn task_type(&self) -> &str {
        &self.task_type
    }

    fn task_queue(&self) -> &str {
        &self.task_queue
    }

    fn namespace(&self) -> &str {
        &self.namespace
    }

    fn workflow_type(&self) -> Option<&str> {
        None
    }
}

/// The failure type a run records when it ends past its run timeout. The
/// workflow's retry policy decides on it as on any failure type, so listing it
/// among the non-retryable types turns retrying a run timeout off.
pub const RUN_TIMED_OUT_FAILURE_TYPE: &str = "WorkflowRunTimedOut";

/// The failure type a run records when it ends past the workflow's execution
/// timeout. Never retried: the execution timeout bounds every run of the
/// workflow.
pub const EXECUTION_TIMED_OUT_FAILURE_TYPE: &str = "WorkflowExecutionTimedOut";

/// `max_retries` for a run with no limit on its retries: a workflow started
/// with `maximum_attempts = 0`.
///
/// Stored as the largest count rather than a separate marker so that every
/// reader of the column — the retry eligibility queries, the
/// `retry_count <= max_retries` check, [`WorkflowExecution::can_retry`] —
/// treats it correctly as it stands: `retry_count` cannot reach it.
pub const UNLIMITED_RETRIES: i32 = i32::MAX;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowExecution {
    pub id: ExecutionId,
    /// External-facing execution ID (string format for API)
    pub execution_id: String,
    /// User-provided workflow ID (for client lookups)
    pub workflow_id: Option<String>,
    /// FK to workflow_registry.id (NULL if workflow deleted)
    pub registry_id: Option<Uuid>,
    pub workflow_name: String,
    /// The workflow class/type (e.g. "OrderWorkflow"). Populated from the client's workflow_type
    /// request field. Distinct from workflow_id (user-provided run identifier).
    #[serde(default)]
    pub workflow_type: String,
    pub workflow_version: String,
    /// The code release this execution is pinned to, set on first claim and
    /// never changed (V034). `None` means unbound — claimable by any release.
    ///
    /// Read-only for most callers: the claim statement owns writing it. The one
    /// exception is reset, which must carry it onto a successor that replays
    /// this execution's journal.
    pub bound_version_id: Option<String>,
    pub namespace: String,
    /// pending | queued | running | paused | completed | failed | cancelled | timed_out | retrying | waiting
    pub status: String,
    pub parameters: serde_json::Value,
    pub result: Option<serde_json::Value>,
    pub error: Option<String>,
    pub error_details: Option<serde_json::Value>,
    pub context: serde_json::Value,
    pub environment: serde_json::Value,
    pub progress: Option<serde_json::Value>,
    pub current_step: Option<String>,
    pub statistics: Option<serde_json::Value>,
    pub total_steps: i32,
    pub completed_steps: i32,
    pub failed_steps: i32,
    pub retried_steps: i32,
    pub duration_ms: Option<i64>,
    pub cpu_time_ms: Option<i64>,
    pub memory_peak_bytes: Option<i64>,
    pub average_step_duration_ms: Option<i64>,
    pub idempotency_key: Option<String>,
    /// 0-100, higher = higher priority
    pub priority: i32,
    /// The execution timeout the workflow was started with, in seconds. It
    /// bounds the whole chain of runs, and is enforced through
    /// [`execution_deadline`](Self::execution_deadline). `None` = no deadline.
    pub timeout_secs: Option<i32>,
    /// When the execution timeout runs out (V067): `timeout_secs` from when
    /// the first run's clocks start ([`clocks_start_at`](Self::clocks_start_at):
    /// its `submitted_at`, or for a delayed start the time it is due), carried
    /// unchanged to every retry and restart-fresh, so that it bounds the chain
    /// rather than each run. A delay is not spent from the execution timeout.
    /// A reset, an operator restarting the workflow, starts a new one from
    /// its own `submitted_at`. `None` on a run with no execution
    /// timeout, and on runs written before V067; read it through
    /// [`execution_expires_at`](Self::execution_expires_at).
    #[serde(default)]
    pub execution_deadline: Option<Timestamptz>,
    /// The run timeout (V065): how long this run may last, from when its
    /// clocks start ([`clocks_start_at`](Self::clocks_start_at)). A successor
    /// run carries the limit and starts its own clock. `None` = no limit.
    #[serde(default)]
    pub run_timeout_secs: Option<i32>,
    /// The task timeout (V065): how long one activation of this run may stay
    /// claimed by a worker without an answer before the claim lapses and the
    /// activation is handed to another. `None` = the engine's claim timeout.
    #[serde(default)]
    pub task_timeout_secs: Option<i32>,
    pub retry_count: i32,
    /// How many times a failed run may be retried, the first run not counted:
    /// a retry count, compared against `retry_count`. A start's
    /// `maximum_attempts` (which counts the first run) is converted to it at
    /// the API boundary. [`UNLIMITED_RETRIES`] is no limit; 0 is no retry,
    /// and is what a run started without a retry policy gets.
    pub max_retries: i32,
    pub tags: serde_json::Value,
    /// Custom indexed metadata for visibility queries
    pub labels: serde_json::Value,
    /// manual | api | scheduled | event | webhook | workflow | cli
    pub trigger_type: String,
    pub triggered_by: Option<Uuid>,
    pub parent_execution_id: Option<String>,
    /// Root of the execution tree (for tracing chains)
    pub root_execution_id: Option<String>,
    pub user_id: Option<Uuid>,
    pub organization_id: Option<Uuid>,
    pub submitted_at: Timestamptz,
    pub started_at: Option<Timestamptz>,
    pub completed_at: Option<Timestamptz>,
    pub cancelled_at: Option<Timestamptz>,
    pub last_retry_at: Option<Timestamptz>,
    pub next_retry_at: Option<Timestamptz>,
    /// When the retry policy decided this run's failure or timeout is final.
    /// Set only on a failed or timed-out run that will not be retried; `None`
    /// otherwise, including on a failed run whose retries are simply used up.
    pub retries_settled_at: Option<Timestamptz>,
    pub created_at: Timestamptz,
    pub updated_at: Timestamptz,
    pub claimed_by: Option<String>,
    pub claimed_at: Option<Timestamptz>,
    /// Expires to detect crashed services
    pub claim_timeout_at: Option<Timestamptz>,
    pub task_queue: String,
    /// hash of workflow_id % total_shards
    #[serde(default)]
    pub shard_id: Option<i32>,
    /// Set when a task completes during an in-progress CompleteWorkflowExecution RPC.
    /// Prevents the poll-fail-to-claim-re-push latency loop.
    pub pending_requeue: bool,
    pub pending_requeue_at: Option<Timestamptz>,
    /// How many times a claim has handed this execution to a worker (V048).
    /// The activation token carries the value its claim set, which is how a
    /// completion is matched to the activation it answers. Written only by the
    /// claim statements; 0 for an execution never claimed.
    #[serde(default)]
    pub activation_seq: i64,
    /// Per-workflow retry policy (backoff settings). NULL = use server defaults.
    #[serde(default)]
    pub retry_policy: Option<serde_json::Value>,
    /// This run holds its `workflow_id` as a name in its namespace (V059): a
    /// start's run, or a successor carrying one on. While it is open no other
    /// run in the organization's namespace may hold the same id — a unique
    /// index decides that. `false` for a child, whose id is its parent's
    /// correlation id and unique only per parent.
    #[serde(default)]
    pub holds_workflow_id: bool,
    /// Annotations (V069): metadata the start attached to the workflow, as a
    /// JSON object of strings. Not indexed and not searched, unlike
    /// [`labels`](Self::labels); reported by Describe and carried to every
    /// run of the workflow. `{}` for none.
    #[serde(default = "empty_object")]
    pub annotations: serde_json::Value,
    /// The request id of the start that created this run (V069), unique in
    /// the organization's namespace: a start repeating it is answered with
    /// this run rather than starting another. Only the run a start created
    /// carries it, not its retries or successors, so the request is
    /// remembered for as long as this row is kept. `None` for a start that
    /// gave none, and for every run not made by a start.
    #[serde(default)]
    pub start_request_id: Option<String>,
    /// When a delayed start's run is due (V069): its submission plus the
    /// start's delay. The run's clocks — its run timeout and the workflow's
    /// execution deadline — start here, not at submission. `None` for a run
    /// started at once. Kept once the run is due, as the record of when it
    /// was to start.
    #[serde(default)]
    pub start_after: Option<Timestamptz>,
    /// Whether the run is still waiting out its start delay (V069). No claim
    /// hands out a run that is; the delayed-start release clears it once
    /// [`start_after`](Self::start_after) has passed and wakes the run's
    /// queue. `false` for a run started at once.
    #[serde(default)]
    pub awaiting_start: bool,
    /// The step-id scheme the run is bound to, as `orcher.v1.StepIdScheme`
    /// numbers it (V070). Bound at the run's first activation by a worker
    /// that reports its schemes, never changed after, and copied by a reset.
    /// `None` until then.
    #[serde(default)]
    pub step_id_scheme: Option<i16>,
    /// The run's history lineage (V070), bound with the scheme and copied by
    /// a reset. `None` reads as the run's own id: see
    /// [`lineage`](Self::lineage).
    #[serde(default)]
    pub history_lineage: Option<String>,
    /// Whether a worker reporting no step-id schemes has activated the run
    /// (V070), so that its history may carry its SDK's legacy ids.
    #[serde(default)]
    pub activated_without_scheme: bool,
    /// Whether the run's step results are entries of its journal (V078):
    /// every step that settles appends a `StepCompleted` with its result or
    /// failure, and an activation sends the journal as it is. `false` for a
    /// run an earlier engine started, whose activations still rebuild those
    /// entries from the execution log. Set when the run is created, carried
    /// by a reset, and never changed.
    #[serde(default)]
    pub step_results_journaled: bool,
}

/// `{}`, a run's annotations when it has none.
fn empty_object() -> serde_json::Value {
    serde_json::json!({})
}

impl WorkflowExecution {
    /// The run's history lineage: the bound one, or the run's own id while
    /// none is bound, which is what the first activation binds.
    pub fn lineage(&self) -> String {
        self.history_lineage
            .clone()
            .unwrap_or_else(|| self.id.as_uuid().to_string())
    }

    pub fn new(
        workflow_name: String,
        workflow_version: String,
        namespace: String,
        parameters: serde_json::Value,
    ) -> Self {
        let now = Timestamptz::now();
        let execution_id = Uuid::new_v4();

        Self {
            id: ExecutionId::from_uuid(Uuid::new_v4()),
            // Unbound until a worker claims it and stamps its release.
            bound_version_id: None,
            execution_id: execution_id.to_string(),
            workflow_id: None,
            registry_id: None,
            workflow_type: workflow_name.clone(),
            workflow_name,
            workflow_version,
            namespace,
            status: "pending".to_string(),
            parameters,
            result: None,
            error: None,
            error_details: None,
            context: serde_json::json!({}),
            environment: serde_json::json!({}),
            progress: None,
            current_step: None,
            statistics: None,
            total_steps: 0,
            completed_steps: 0,
            failed_steps: 0,
            retried_steps: 0,
            duration_ms: None,
            cpu_time_ms: None,
            memory_peak_bytes: None,
            average_step_duration_ms: None,
            idempotency_key: None,
            priority: 0,
            timeout_secs: None,
            execution_deadline: None,
            run_timeout_secs: None,
            task_timeout_secs: None,
            retry_count: 0,
            // Workflow retry is opt-in: no policy, no retry.
            max_retries: 0,
            tags: serde_json::json!({}),
            labels: serde_json::json!({}),
            trigger_type: "manual".to_string(),
            triggered_by: None,
            parent_execution_id: None,
            root_execution_id: None,
            user_id: None,
            organization_id: None,
            submitted_at: now,
            started_at: None,
            completed_at: None,
            cancelled_at: None,
            last_retry_at: None,
            next_retry_at: None,
            retries_settled_at: None,
            created_at: now,
            updated_at: now,
            claimed_by: None,
            claimed_at: None,
            claim_timeout_at: None,
            task_queue: "default".to_string(),
            shard_id: None,
            pending_requeue: false,
            pending_requeue_at: None,
            activation_seq: 0,
            retry_policy: None,
            holds_workflow_id: false,
            annotations: empty_object(),
            start_request_id: None,
            start_after: None,
            awaiting_start: false,
            step_id_scheme: None,
            history_lineage: None,
            activated_without_scheme: false,
            step_results_journaled: true,
        }
    }

    pub fn builder() -> WorkflowExecutionBuilder {
        WorkflowExecutionBuilder::default()
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self.status.as_str(),
            "completed" | "failed" | "cancelled" | "timed_out"
        )
    }

    /// Whether this run has ended, so nothing can act on it any more. A
    /// failure, or a run timeout, is still live while a retry is scheduled for
    /// it. The in-memory form of the predicate the guarded status writes use,
    /// kept here so every caller agrees on which statuses are final.
    pub fn has_ended(&self) -> bool {
        match self.status.as_str() {
            "completed" | "cancelled" | "terminated" | "restarted_fresh" | "reset" => true,
            "failed" | "timed_out" => !(self.can_retry() && self.next_retry_at.is_some()),
            _ => false,
        }
    }

    pub fn is_active(&self) -> bool {
        matches!(
            self.status.as_str(),
            "pending" | "queued" | "running" | "retrying" | "waiting"
        )
    }

    /// Whether this run's retry is still to come: a failure the retry policy
    /// has not given up on, or a run that timed out on its run timeout with a
    /// retry scheduled for it.
    ///
    /// A timed-out run is decided as it times out — the write that ends it
    /// schedules its retry or settles it — so one with no retry scheduled is
    /// final. A failure is decided after it is recorded, and one not yet
    /// decided may still be retried.
    pub fn can_retry(&self) -> bool {
        let retryable = match self.status.as_str() {
            "failed" => true,
            "timed_out" => self.next_retry_at.is_some(),
            _ => false,
        };
        retryable && self.retry_count < self.max_retries && self.retries_settled_at.is_none()
    }

    /// When this run's clocks start: when a delayed start is due
    /// ([`start_after`](Self::start_after)), and otherwise when the run was
    /// submitted. The run timeout is measured from here, and a start's
    /// execution deadline is fixed from here.
    pub fn clocks_start_at(&self) -> Timestamptz {
        self.start_after.unwrap_or(self.submitted_at)
    }

    /// When the workflow's execution timeout runs out, if it has one: the
    /// deadline its chain of runs was started with. A run written before V067
    /// carries no deadline of its own and is measured from its own
    /// `submitted_at`, as it always was.
    pub fn execution_expires_at(&self) -> Option<Timestamptz> {
        self.execution_deadline.or_else(|| {
            self.timeout_secs
                .map(|secs| self.submitted_at + std::time::Duration::from_secs(secs.max(0) as u64))
        })
    }

    /// Whether the workflow's execution timeout has run out by `at`: no run of
    /// the workflow may start from then on.
    pub fn execution_expired_by(&self, at: Timestamptz) -> bool {
        self.execution_expires_at()
            .is_some_and(|deadline| deadline <= at)
    }

    pub fn progress_percent(&self) -> f64 {
        if self.total_steps == 0 {
            0.0
        } else {
            (self.completed_steps as f64 / self.total_steps as f64) * 100.0
        }
    }

    pub fn running_duration_ms(&self) -> Option<i64> {
        self.started_at.map(|start| {
            let end = self.completed_at.unwrap_or_else(Timestamptz::now);
            (end - start).as_millis() as i64
        })
    }
}

#[derive(Debug, Default)]
pub struct WorkflowExecutionBuilder {
    execution_id: Option<String>,
    workflow_id: Option<String>,
    registry_id: Option<Uuid>,
    workflow_name: Option<String>,
    workflow_type: Option<String>,
    workflow_version: Option<String>,
    namespace: Option<String>,
    parameters: Option<serde_json::Value>,
    idempotency_key: Option<String>,
    priority: Option<i32>,
    timeout_secs: Option<i32>,
    max_retries: Option<i32>,
    retry_policy: Option<serde_json::Value>,
    tags: Option<serde_json::Value>,
    labels: Option<serde_json::Value>,
    trigger_type: Option<String>,
    triggered_by: Option<Uuid>,
    user_id: Option<Uuid>,
    organization_id: Option<Uuid>,
    parent_execution_id: Option<String>,
    task_queue: Option<String>,
}

impl WorkflowExecutionBuilder {
    pub fn execution_id(mut self, id: String) -> Self {
        self.execution_id = Some(id);
        self
    }

    pub fn workflow_id(mut self, id: String) -> Self {
        self.workflow_id = Some(id);
        self
    }

    pub fn registry_id(mut self, id: Uuid) -> Self {
        self.registry_id = Some(id);
        self
    }

    pub fn workflow_name(mut self, name: String) -> Self {
        self.workflow_name = Some(name);
        self
    }

    pub fn workflow_type(mut self, t: String) -> Self {
        self.workflow_type = Some(t);
        self
    }

    pub fn workflow_version(mut self, version: String) -> Self {
        self.workflow_version = Some(version);
        self
    }

    pub fn namespace(mut self, namespace: String) -> Self {
        self.namespace = Some(namespace);
        self
    }

    pub fn parameters(mut self, params: serde_json::Value) -> Self {
        self.parameters = Some(params);
        self
    }

    pub fn idempotency_key(mut self, key: String) -> Self {
        self.idempotency_key = Some(key);
        self
    }

    pub fn priority(mut self, priority: i32) -> Self {
        self.priority = Some(priority);
        self
    }

    pub fn timeout_secs(mut self, timeout: i32) -> Self {
        self.timeout_secs = Some(timeout);
        self
    }

    pub fn max_retries(mut self, retries: i32) -> Self {
        self.max_retries = Some(retries);
        self
    }

    pub fn retry_policy(mut self, policy: serde_json::Value) -> Self {
        self.retry_policy = Some(policy);
        self
    }

    pub fn tags(mut self, tags: serde_json::Value) -> Self {
        self.tags = Some(tags);
        self
    }

    pub fn labels(mut self, attrs: serde_json::Value) -> Self {
        self.labels = Some(attrs);
        self
    }

    pub fn trigger_type(mut self, trigger: String) -> Self {
        self.trigger_type = Some(trigger);
        self
    }

    pub fn triggered_by(mut self, user: Uuid) -> Self {
        self.triggered_by = Some(user);
        self
    }

    pub fn user_id(mut self, user: Uuid) -> Self {
        self.user_id = Some(user);
        self
    }

    pub fn organization_id(mut self, org: Uuid) -> Self {
        self.organization_id = Some(org);
        self
    }

    pub fn parent_execution_id(mut self, parent: String) -> Self {
        self.parent_execution_id = Some(parent);
        self
    }

    pub fn task_queue(mut self, queue: String) -> Self {
        self.task_queue = Some(queue);
        self
    }

    pub fn build(self) -> WorkflowExecution {
        let now = Timestamptz::now();
        let execution_id = self
            .execution_id
            .unwrap_or_else(|| Uuid::new_v4().to_string());

        WorkflowExecution {
            id: ExecutionId::from_uuid(Uuid::new_v4()),
            // Unbound until a worker claims it and stamps its release.
            bound_version_id: None,
            execution_id,
            workflow_id: self.workflow_id,
            registry_id: self.registry_id,
            workflow_name: self
                .workflow_name
                .clone()
                .expect("workflow_name is required"),
            workflow_type: self.workflow_type.unwrap_or_else(|| {
                self.workflow_name
                    .clone()
                    .expect("workflow_name is required")
            }),
            workflow_version: self.workflow_version.expect("workflow_version is required"),
            namespace: self.namespace.unwrap_or_else(|| "default".to_string()),
            status: "pending".to_string(),
            parameters: self.parameters.unwrap_or_else(|| serde_json::json!({})),
            result: None,
            error: None,
            error_details: None,
            context: serde_json::json!({}),
            environment: serde_json::json!({}),
            progress: None,
            current_step: None,
            statistics: None,
            total_steps: 0,
            completed_steps: 0,
            failed_steps: 0,
            retried_steps: 0,
            duration_ms: None,
            cpu_time_ms: None,
            memory_peak_bytes: None,
            average_step_duration_ms: None,
            idempotency_key: self.idempotency_key,
            priority: self.priority.unwrap_or(0),
            timeout_secs: self.timeout_secs,
            execution_deadline: None,
            run_timeout_secs: None,
            task_timeout_secs: None,
            retry_count: 0,
            // Workflow retry is opt-in. Defaulting to 3 here gave every run
            // built without an explicit cap — a batch start's, a
            // restarted-fresh successor's — three retries nobody asked for.
            max_retries: self.max_retries.unwrap_or(0),
            tags: self.tags.unwrap_or_else(|| serde_json::json!({})),
            labels: self.labels.unwrap_or_else(|| serde_json::json!({})),
            trigger_type: self.trigger_type.unwrap_or_else(|| "manual".to_string()),
            triggered_by: self.triggered_by,
            parent_execution_id: self.parent_execution_id.clone(),
            root_execution_id: self.parent_execution_id,
            user_id: self.user_id,
            organization_id: self.organization_id,
            submitted_at: now,
            started_at: None,
            completed_at: None,
            cancelled_at: None,
            last_retry_at: None,
            next_retry_at: None,
            retries_settled_at: None,
            created_at: now,
            updated_at: now,
            claimed_by: None,
            claimed_at: None,
            claim_timeout_at: None,
            task_queue: self.task_queue.unwrap_or_else(|| "default".to_string()),
            shard_id: None,
            pending_requeue: false,
            pending_requeue_at: None,
            activation_seq: 0,
            retry_policy: self.retry_policy,
            holds_workflow_id: false,
            annotations: empty_object(),
            start_request_id: None,
            start_after: None,
            awaiting_start: false,
            step_id_scheme: None,
            history_lineage: None,
            activated_without_scheme: false,
            step_results_journaled: true,
        }
    }
}

/// How the execution was initiated.
///
/// When `trigger_type = Workflow`, check `parent_execution_id` to distinguish
/// sub-workflows (Some) from chained/sequential workflows (None).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionTrigger {
    Manual,
    Scheduled,
    Event,
    Webhook,
    Api,
    Workflow,
}

impl Default for ExecutionTrigger {
    fn default() -> Self {
        Self::Manual
    }
}

impl std::fmt::Display for ExecutionTrigger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecutionTrigger::Manual => write!(f, "manual"),
            ExecutionTrigger::Scheduled => write!(f, "scheduled"),
            ExecutionTrigger::Event => write!(f, "event"),
            ExecutionTrigger::Webhook => write!(f, "webhook"),
            ExecutionTrigger::Api => write!(f, "api"),
            ExecutionTrigger::Workflow => write!(f, "workflow"),
        }
    }
}

impl std::str::FromStr for ExecutionTrigger {
    type Err = orcher_common::error::OrcherError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "manual" => Ok(ExecutionTrigger::Manual),
            "scheduled" => Ok(ExecutionTrigger::Scheduled),
            "event" => Ok(ExecutionTrigger::Event),
            "webhook" => Ok(ExecutionTrigger::Webhook),
            "api" => Ok(ExecutionTrigger::Api),
            "workflow" => Ok(ExecutionTrigger::Workflow),
            _ => Err(orcher_common::error::OrcherError::InvalidInput {
                message: format!("Invalid execution trigger type: {}", s),
                field: Some("trigger_type".to_string()),
            }),
        }
    }
}

/// The failure type a task attempt timed out by missed heartbeats is recorded
/// under. Retryable like every timeout; a retry policy stops it by listing
/// either this or [`TIMEOUT_FAILURE_TYPE`].
pub const HEARTBEAT_TIMEOUT_FAILURE_TYPE: &str = "HeartbeatTimeout";

/// The failure type of every other timeout, and the one a retry policy lists
/// to stop retrying timeouts of every kind.
pub const TIMEOUT_FAILURE_TYPE: &str = "TimeoutError";

/// The failure type of a task step that ended cancelled, however it came to:
/// ended before it started because its workflow's cancellation interrupted
/// it, or stopped by its worker while it ran. The step's message says which.
/// One type, so a runtime has one value to map to its cancellation error.
/// Never retried.
pub const CANCELLED_FAILURE_TYPE: &str = "Cancelled";

/// The failure type a timeout of `timeout_type` (`"Heartbeat"`,
/// `"StartToClose"`, ...) is recorded under.
///
/// A heartbeat timeout has a type of its own: it is the one that says the
/// worker stopped answering for the attempt — it died, or never received it —
/// rather than that the task ran too long.
pub fn timeout_failure_type(timeout_type: &str) -> &'static str {
    if timeout_type.eq_ignore_ascii_case("heartbeat") {
        HEARTBEAT_TIMEOUT_FAILURE_TYPE
    } else {
        TIMEOUT_FAILURE_TYPE
    }
}

impl TaskExecution {
    #[allow(clippy::too_many_arguments)]
    pub fn new_scheduled(
        task_id: String,
        task_type: String,
        workflow_id: WorkflowId,
        execution_id: ExecutionId,
        task_queue: String,
        namespace: String,
        input: Option<Vec<u8>>,
        retry_policy: Option<serde_json::Value>,
        schedule_to_start_timeout_secs: Option<i32>,
        start_to_close_timeout_secs: Option<i32>,
        heartbeat_timeout_secs: Option<i32>,
    ) -> Self {
        let now = Timestamptz::now();
        Self {
            id: TaskExecutionId::new_v4(),
            organization_id: None,
            task_id,
            task_type,
            workflow_id,
            execution_id,
            task_queue,
            namespace,
            service_identity: None,
            attempt_number: 1,
            status: "scheduled".to_string(),
            scheduled_at: now,
            started_at: None,
            completed_at: None,
            task_token: None,
            schedule_to_start_timeout_secs,
            start_to_close_timeout_secs,
            heartbeat_timeout_secs,
            input,
            result: None,
            error_message: None,
            error_type: None,
            error_details: None,
            retry_policy,
            headers: serde_json::json!({}),
            metadata: serde_json::json!({}),
            last_heartbeat_at: None,
            heartbeat_details: None,
            created_at: now,
            updated_at: now,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_scheduled_with_org(
        organization_id: Uuid,
        task_id: String,
        task_type: String,
        workflow_id: WorkflowId,
        execution_id: ExecutionId,
        task_queue: String,
        namespace: String,
        input: Option<Vec<u8>>,
        retry_policy: Option<serde_json::Value>,
        schedule_to_start_timeout_secs: Option<i32>,
        start_to_close_timeout_secs: Option<i32>,
        heartbeat_timeout_secs: Option<i32>,
    ) -> Self {
        let now = Timestamptz::now();
        Self {
            id: TaskExecutionId::new_v4(),
            organization_id: Some(organization_id),
            task_id,
            task_type,
            workflow_id,
            execution_id,
            task_queue,
            namespace,
            service_identity: None,
            attempt_number: 1,
            status: "scheduled".to_string(),
            scheduled_at: now,
            started_at: None,
            completed_at: None,
            task_token: None,
            schedule_to_start_timeout_secs,
            start_to_close_timeout_secs,
            heartbeat_timeout_secs,
            input,
            result: None,
            error_message: None,
            error_type: None,
            error_details: None,
            retry_policy,
            headers: serde_json::json!({}),
            metadata: serde_json::json!({}),
            last_heartbeat_at: None,
            heartbeat_details: None,
            created_at: now,
            updated_at: now,
        }
    }

    pub fn mark_started(&mut self, service_identity: String, task_token: String) -> &mut Self {
        self.status = "started".to_string();
        self.service_identity = Some(service_identity);
        self.task_token = Some(task_token);
        self.started_at = Some(Timestamptz::now());
        self.updated_at = Timestamptz::now();
        self
    }

    pub fn mark_completed(&mut self, result: Vec<u8>) -> &mut Self {
        self.status = "completed".to_string();
        self.result = Some(result);
        self.completed_at = Some(Timestamptz::now());
        self.updated_at = Timestamptz::now();
        self
    }

    pub fn mark_failed(
        &mut self,
        error_message: String,
        error_type: Option<String>,
        error_details: Option<serde_json::Value>,
    ) -> &mut Self {
        self.status = "failed".to_string();
        self.error_message = Some(error_message);
        self.error_type = error_type;
        self.error_details = error_details;
        self.completed_at = Some(Timestamptz::now());
        self.updated_at = Timestamptz::now();
        self
    }

    pub fn mark_timed_out(&mut self, timeout_type: String) -> &mut Self {
        self.status = "timed_out".to_string();
        self.error_message = Some(format!("Task timed out: {}", timeout_type));
        self.error_type = Some(timeout_failure_type(&timeout_type).to_string());
        self.completed_at = Some(Timestamptz::now());
        self.updated_at = Timestamptz::now();
        self
    }

    pub fn mark_cancelled(&mut self, reason: Option<String>) -> &mut Self {
        self.status = "cancelled".to_string();
        if let Some(reason) = reason {
            self.error_message = Some(reason);
        }
        self.completed_at = Some(Timestamptz::now());
        self.updated_at = Timestamptz::now();
        self
    }

    pub fn update_heartbeat(&mut self, details: Option<Vec<u8>>) -> &mut Self {
        self.last_heartbeat_at = Some(Timestamptz::now());
        self.heartbeat_details = details;
        self.updated_at = Timestamptz::now();
        self
    }

    pub fn duration_secs(&self) -> Option<i64> {
        if let (Some(started), Some(completed)) = (self.started_at, self.completed_at) {
            Some((completed - started).as_secs())
        } else {
            None
        }
    }

    pub fn is_scheduled(&self) -> bool {
        self.status == "scheduled"
    }

    pub fn is_started(&self) -> bool {
        self.status == "started"
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self.status.as_str(),
            "completed" | "failed" | "cancelled" | "timed_out"
        )
    }

    pub fn is_successful(&self) -> bool {
        self.status == "completed"
    }

    pub fn is_failed(&self) -> bool {
        matches!(self.status.as_str(), "failed" | "timed_out")
    }
}

#[cfg(test)]
mod has_ended_tests {
    use super::*;

    fn run(status: &str) -> WorkflowExecution {
        let mut run = WorkflowExecution::new(
            "w".to_string(),
            "1".to_string(),
            "default".to_string(),
            serde_json::json!({}),
        );
        run.status = status.to_string();
        run
    }

    #[test]
    fn every_final_status_has_ended() {
        for status in [
            "completed",
            "cancelled",
            "terminated",
            "timed_out",
            "restarted_fresh",
            "reset",
        ] {
            assert!(run(status).has_ended(), "{status} should have ended");
        }
    }

    #[test]
    fn live_statuses_have_not_ended() {
        for status in ["pending", "running", "waiting"] {
            assert!(!run(status).has_ended(), "{status} should be live");
        }
    }

    #[test]
    fn a_failure_with_a_retry_scheduled_is_still_live() {
        let mut failed = run("failed");
        failed.retry_count = 0;
        failed.max_retries = 3;
        failed.next_retry_at = Some(Timestamptz::now());
        assert!(!failed.has_ended());
    }

    #[test]
    fn a_failure_with_no_retry_left_has_ended() {
        let mut exhausted = run("failed");
        exhausted.retry_count = 3;
        exhausted.max_retries = 3;
        exhausted.next_retry_at = Some(Timestamptz::now());
        assert!(exhausted.has_ended());

        let mut unscheduled = run("failed");
        unscheduled.next_retry_at = None;
        assert!(unscheduled.has_ended());
    }
}
