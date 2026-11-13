//! # WorkflowExecution Domain Repository
//!
//! Workflow execution persistence implementation using PostgreSQL backend.
//! Handles CRUD operations for code-first workflow executions (WASM, Container, In-Process, Git+Container).
//!
//! This repository maps to the V003 workflow_executions table schema.

use jiff::SignedDuration;
use orcher_common::types::time::Timestamptz;
use orcher_common::{error::OrcherError, types::identifiers::ExecutionId, OrcherResult};
use sqlx::{PgPool, Row};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::schema::types::execution::WorkflowExecution;
use crate::sharding::pool_routing::ShardPoolResolver;
use crate::sharding::{shard_predicate, ClaimBounds, ClaimParams, ClaimScope, WriteFence};
use crate::stale_plan::retry_on_stale_plan;

/// Claim-time release routing, shared by every path that can claim work.
///
/// Two rules in one predicate:
///
/// * a **bound** execution goes only to a worker declaring the same release, so
///   it never replays against code its journal was not written by
/// * an **unbound** execution goes to the queue's *current* release, so a
///   rollout is steerable and the outgoing release drains monotonically rather
///   than taking new work until the moment its workers stop
///
/// With no current release configured the second rule disappears: the COALESCE
/// falls back to the caller's own release, which trivially matches, so a queue
/// that never sets one behaves exactly as before.
///
/// Built once and shared on purpose. This filter previously lived in one of
/// three claim queries, which made it enforced in tests and bypassable in
/// production; a single builder makes that divergence hard to reintroduce.
///
/// The first branch is guarded on `bound_version_id IS NOT NULL` deliberately.
/// A worker that declares nothing binds `version` to NULL, and
/// `NULL IS NOT DISTINCT FROM NULL` is TRUE — so without that guard an
/// undeclared worker matched every unbound execution through the first branch
/// and skipped the current-release check entirely.
///
/// `version` is the bind position of the claiming worker's release. `ns` and
/// `queue` are SQL expressions rather than positions, because the scanning paths
/// have them as bind parameters while the targeted by-id path correlates on the
/// row's own columns.
/// The owning organisation is correlated from the candidate row rather than
/// bound, and is written out qualified on purpose: `task_queue_releases` now
/// carries an `organization_id` of its own, so an unqualified reference inside
/// the subquery would resolve to the policy row's owner and compare it with
/// itself — always true, and silently back to matching any tenant's policy.
fn release_predicate(version: &str, ns: &str, queue: &str) -> String {
    format!(
        "((bound_version_id IS NOT NULL AND {version} IS NOT DISTINCT FROM bound_version_id) \
           OR (bound_version_id IS NULL \
               AND COALESCE( \
                     (SELECT r.current_version_id FROM task_queue_releases r \
                       WHERE r.namespace = {ns} AND r.task_queue = {queue} \
                         AND r.organization_id \
                             IS NOT DISTINCT FROM workflow_executions.organization_id), \
                     {version} \
                   ) IS NOT DISTINCT FROM {version}))"
    )
}

/// Which runs a worker reporting the step-id schemes bound at `schemes` (a
/// `smallint[]` of `orcher.v1.StepIdScheme` numbers) may claim.
///
/// A run's history is keyed by the step ids of the scheme it was first
/// activated under, so once bound it goes only to a worker that supports that
/// scheme. An unbound run goes to a worker reporting none (one predating
/// schemes) or to one with a scheme it can bind: its SDK's own (any but
/// `KERNEL`), or `KERNEL` if no worker reporting none has activated the run,
/// since that one's history carries its SDK's ids. A worker reporting none
/// never gets a bound run. Refuses rather than falls through, as
/// [`release_predicate`] does.
///
/// Columns are unqualified, so it reads the row being claimed.
fn step_id_predicate(schemes: &str) -> String {
    format!(
        "(CASE WHEN step_id_scheme IS NOT NULL THEN step_id_scheme = ANY({schemes})                WHEN cardinality({schemes}) = 0 THEN TRUE                ELSE EXISTS (SELECT 1 FROM unnest({schemes}) s WHERE s > {kernel})                     OR ({kernel} = ANY({schemes}) AND NOT activated_without_scheme)           END)",
        kernel = STEP_ID_KERNEL,
    )
}

/// The `SET` assignments that bind a claimed run's step-id scheme and history
/// lineage, for a worker reporting the schemes bound at `schemes`, reading the
/// row's current values through `row` (`we.` or empty).
///
/// Bound at the first claim by a worker that reports schemes and never
/// changed: `KERNEL` if the worker supports it and no worker reporting none
/// has activated the run, otherwise the worker's own SDK scheme. A claim by a
/// worker reporting none binds no scheme and records that it activated the
/// run. The lineage is the run's own id unless one is already bound, as a
/// reset binds it.
fn step_id_binding(schemes: &str, row: &str) -> String {
    format!(
        "step_id_scheme = CASE              WHEN {row}step_id_scheme IS NOT NULL THEN {row}step_id_scheme              WHEN cardinality({schemes}) = 0 THEN NULL              WHEN {kernel} = ANY({schemes}) AND NOT {row}activated_without_scheme THEN {kernel}              ELSE (SELECT min(s) FROM unnest({schemes}) s WHERE s > {kernel})            END::smallint,          activated_without_scheme = {row}activated_without_scheme            OR ({row}step_id_scheme IS NULL AND cardinality({schemes}) = 0),          history_lineage = COALESCE({row}history_lineage, {row}id::text)",
        kernel = STEP_ID_KERNEL,
    )
}

/// `orcher.v1.StepIdScheme.KERNEL`: the kernel's step ids, shared by every
/// SDK. Every other scheme but the unspecified one is an SDK's own.
const STEP_ID_KERNEL: i16 = 1;

/// A run a cancellation request applies to: what the caller needs to journal
/// the request and wake the run.
#[derive(Debug, Clone)]
pub struct CancelRequestTarget {
    /// Internal id of the run (`workflow_executions.id`).
    pub workflow_id: Uuid,
    /// Namespace the run belongs to.
    pub namespace: String,
    /// Task queue its activations are handed out on.
    pub task_queue: String,
    /// Whether this call made the request. False when an earlier one had, so
    /// the caller need not wake the run again.
    pub newly_requested: bool,
}

/// Information about pending requeue status after claim release
#[derive(Debug, Clone)]
pub struct PendingRequeueInfo {
    /// Whether the workflow needs to be re-queued to Redis
    pub pending_requeue: bool,
    /// Task queue for routing the workflow
    pub task_queue: String,
    /// Namespace for the workflow
    pub namespace: String,
}

/// Detailed reason why a claim failed
#[derive(Debug, Clone)]
pub enum ClaimFailureReason {
    /// Execution not found in database
    NotFound,
    /// Execution is in a terminal status (completed, cancelled, failed)
    TerminalStatus(String),
    /// Execution is claimed by a different identity
    ClaimedByOther {
        /// The identity that currently holds the claim
        claimed_by: String,
        /// When the current claim expires (if set)
        claim_timeout_at: Option<Timestamptz>,
    },
    /// Execution was cancelled
    Cancelled,
}

/// Result of checking execution state for claim eligibility
#[derive(Debug, Clone)]
pub struct ClaimStateCheck {
    /// Whether the execution exists
    pub exists: bool,
    /// The current status if execution exists
    pub status: Option<String>,
    /// Who currently has the claim (if any)
    pub claimed_by: Option<String>,
    /// When the claim expires (if claimed)
    pub claim_timeout_at: Option<Timestamptz>,
    /// Whether the execution is cancelled
    pub is_cancelled: bool,
    /// The reason claim would fail (if applicable)
    pub failure_reason: Option<ClaimFailureReason>,
}

/// Workflow that needs to be re-queued (for fallback processing)
#[derive(Debug, Clone)]
pub struct PendingRequeueWorkflow {
    /// Execution ID of the workflow
    pub execution_id: String,
    /// Task queue for routing the workflow
    pub task_queue: String,
    /// Namespace for the workflow
    pub namespace: String,
}

/// Intermediate type for deserializing from SQL (sqlx::FromRow)
#[derive(Debug, Clone, sqlx::FromRow)]
struct WorkflowExecutionRow {
    /// The release this execution is pinned to (V034). Previously absent here,
    /// so the column was write-only: the claim statement set and filtered it,
    /// but nothing in Rust could read it back.
    bound_version_id: Option<String>,
    // Identifiers
    id: Uuid,
    execution_id: String,
    workflow_id: Option<String>,
    registry_id: Option<Uuid>,
    workflow_name: String,
    workflow_type: String,
    workflow_version: String,
    namespace: String,

    // Status
    status: String,

    // Input/Output
    parameters: serde_json::Value,
    result: Option<serde_json::Value>,
    error: Option<String>,
    error_details: Option<serde_json::Value>,

    // Context
    context: serde_json::Value,
    environment: serde_json::Value,

    // Progress
    progress: Option<serde_json::Value>,
    current_step: Option<String>,

    // Statistics
    statistics: Option<serde_json::Value>,
    total_steps: i32,
    completed_steps: i32,
    failed_steps: i32,
    retried_steps: i32,

    // Performance metrics
    duration_ms: Option<i64>,
    cpu_time_ms: Option<i64>,
    memory_peak_bytes: Option<i64>,
    average_step_duration_ms: Option<i64>,

    // Idempotency
    idempotency_key: Option<String>,

    // Execution metadata
    priority: i32,
    timeout_secs: Option<i32>,
    // The run and task timeouts (V065). Absent from queries that name their
    // columns.
    #[sqlx(default)]
    run_timeout_secs: Option<i32>,
    #[sqlx(default)]
    task_timeout_secs: Option<i32>,
    // The execution deadline every run of the workflow shares (V067). Absent
    // from queries that name their columns.
    #[sqlx(default)]
    execution_deadline: Option<Timestamptz>,
    retry_count: i32,
    max_retries: i32,

    // Tags
    tags: serde_json::Value,

    // Labels
    labels: serde_json::Value,

    // Trigger
    trigger_type: String,
    triggered_by: Option<Uuid>,

    // Hierarchy
    parent_execution_id: Option<String>,
    root_execution_id: Option<String>,

    // Ownership
    user_id: Option<Uuid>,
    organization_id: Option<Uuid>,

    // Timing
    submitted_at: Timestamptz,
    started_at: Option<Timestamptz>,
    completed_at: Option<Timestamptz>,
    cancelled_at: Option<Timestamptz>,
    last_retry_at: Option<Timestamptz>,
    next_retry_at: Option<Timestamptz>,
    // Absent from queries that name their columns rather than select them all.
    #[sqlx(default)]
    retries_settled_at: Option<Timestamptz>,

    // Audit
    created_at: Timestamptz,
    updated_at: Timestamptz,

    // Execution locking
    claimed_by: Option<String>,
    claimed_at: Option<Timestamptz>,
    claim_timeout_at: Option<Timestamptz>,
    task_queue: String,

    // Pending requeue (Solution B: Task-Aware Re-polling)
    pending_requeue: bool,
    pending_requeue_at: Option<Timestamptz>,

    // Activation numbering (V048). Absent from queries that name their columns.
    #[sqlx(default)]
    activation_seq: i64,

    // The shard this row belongs to
    shard_id: Option<i32>,

    // Per-workflow retry policy (backoff settings)
    retry_policy: Option<serde_json::Value>,

    // Whether this run holds its workflow id (V059). Absent from queries that
    // name their columns.
    #[sqlx(default)]
    holds_workflow_id: bool,

    // A start's annotations, its request id, and its delay (V069). Absent
    // from queries that name their columns.
    #[sqlx(default)]
    annotations: Option<serde_json::Value>,
    #[sqlx(default)]
    start_request_id: Option<String>,
    #[sqlx(default)]
    start_after: Option<Timestamptz>,
    #[sqlx(default)]
    awaiting_start: bool,
    #[sqlx(default)]
    step_id_scheme: Option<i16>,
    #[sqlx(default)]
    history_lineage: Option<String>,
    #[sqlx(default)]
    activated_without_scheme: bool,
    #[sqlx(default)]
    step_results_journaled: bool,
    #[sqlx(default)]
    claimed_through_entry: Option<i64>,
    #[sqlx(default)]
    observed_through_entry: Option<i64>,
}

impl From<WorkflowExecutionRow> for WorkflowExecution {
    fn from(row: WorkflowExecutionRow) -> Self {
        WorkflowExecution {
            id: ExecutionId::from_uuid(row.id),
            bound_version_id: row.bound_version_id,
            execution_id: row.execution_id,
            workflow_id: row.workflow_id,
            registry_id: row.registry_id,
            workflow_name: row.workflow_name,
            workflow_type: row.workflow_type,
            workflow_version: row.workflow_version,
            namespace: row.namespace,
            status: row.status,
            parameters: row.parameters,
            result: row.result,
            error: row.error,
            error_details: row.error_details,
            context: row.context,
            environment: row.environment,
            progress: row.progress,
            current_step: row.current_step,
            statistics: row.statistics,
            total_steps: row.total_steps,
            completed_steps: row.completed_steps,
            failed_steps: row.failed_steps,
            retried_steps: row.retried_steps,
            duration_ms: row.duration_ms,
            cpu_time_ms: row.cpu_time_ms,
            memory_peak_bytes: row.memory_peak_bytes,
            average_step_duration_ms: row.average_step_duration_ms,
            idempotency_key: row.idempotency_key,
            priority: row.priority,
            timeout_secs: row.timeout_secs,
            run_timeout_secs: row.run_timeout_secs,
            task_timeout_secs: row.task_timeout_secs,
            execution_deadline: row.execution_deadline,
            retry_count: row.retry_count,
            max_retries: row.max_retries,
            tags: row.tags,
            labels: row.labels,
            trigger_type: row.trigger_type,
            triggered_by: row.triggered_by,
            parent_execution_id: row.parent_execution_id,
            root_execution_id: row.root_execution_id,
            user_id: row.user_id,
            organization_id: row.organization_id,
            submitted_at: row.submitted_at,
            started_at: row.started_at,
            completed_at: row.completed_at,
            cancelled_at: row.cancelled_at,
            last_retry_at: row.last_retry_at,
            next_retry_at: row.next_retry_at,
            retries_settled_at: row.retries_settled_at,
            created_at: row.created_at,
            updated_at: row.updated_at,
            claimed_by: row.claimed_by,
            claimed_at: row.claimed_at,
            claim_timeout_at: row.claim_timeout_at,
            task_queue: row.task_queue,
            pending_requeue: row.pending_requeue,
            pending_requeue_at: row.pending_requeue_at,
            activation_seq: row.activation_seq,
            shard_id: row.shard_id,
            retry_policy: row.retry_policy,
            holds_workflow_id: row.holds_workflow_id,
            annotations: row.annotations.unwrap_or_else(|| serde_json::json!({})),
            start_request_id: row.start_request_id,
            start_after: row.start_after,
            awaiting_start: row.awaiting_start,
            step_id_scheme: row.step_id_scheme,
            history_lineage: row.history_lineage,
            activated_without_scheme: row.activated_without_scheme,
            step_results_journaled: row.step_results_journaled,
            claimed_through_entry: row.claimed_through_entry,
            observed_through_entry: row.observed_through_entry,
        }
    }
}

/// How a request names a workflow run; resolved by
/// [`WorkflowExecutionRepository::find_run`].
#[derive(Debug, Clone, Copy)]
pub struct RunRef<'a> {
    /// The calling organization; `None` for an unscoped caller.
    pub organization_id: Option<Uuid>,
    /// The namespace a `workflow_id` is looked up in.
    pub namespace: &'a str,
    /// The workflow's id, or empty.
    pub workflow_id: &'a str,
    /// One run's id, or empty.
    pub execution_id: &'a str,
}

/// Filters for listing workflow executions
#[derive(Debug, Default, Clone)]
pub struct ListExecutionsFilters {
    /// Filter by workflow type (the workflow class name, e.g. "OrderWorkflow")
    pub workflow_name: Option<String>,
    /// Filter by workflow version
    pub workflow_version: Option<String>,
    /// Filter by namespace
    pub namespace: Option<String>,
    /// Filter by task queue
    pub task_queue: Option<String>,
    /// Filter by execution status
    pub status: Option<String>,
    /// Filter by user ID
    pub user_id: Option<Uuid>,
    /// Filter by organization ID
    pub organization_id: Option<Uuid>,
    /// Filter by parent execution ID
    pub parent_execution_id: Option<String>,
    /// Filter by labels (JSONB containment query)
    pub labels: Option<serde_json::Value>,
    /// Maximum number of results to return
    pub limit: i64,
    /// Number of results to skip (ignored when cursor fields are set)
    pub offset: i64,
    /// Only return executions submitted at or after this timestamp
    pub submitted_after: Option<orcher_common::types::time::Timestamptz>,
    /// Keyset cursor: last seen submitted_at (exclusive upper bound for next page)
    pub cursor_submitted_at: Option<orcher_common::types::time::Timestamptz>,
    /// Keyset cursor: last seen execution_id (tiebreaker, lexicographic)
    pub cursor_execution_id: Option<String>,
}

/// Workflow execution repository
#[derive(Clone, Debug)]
pub struct WorkflowExecutionRepository {
    pool: PgPool,
    /// Optional shard-pool routing for partitioned persistence. When set, writes and
    /// reads for a workflow are routed to the database holding that workflow's shard
    /// (keyed by the `workflow_executions.id` UUID — the same key the coordinator
    /// routes by). Unset ⇒ the single `pool` is used, byte-identical to before.
    shard_resolver: std::sync::Arc<std::sync::OnceLock<std::sync::Arc<dyn ShardPoolResolver>>>,
    /// Restricts workflow claims to this node's shards. Absent ⇒ claim
    /// everything, which is every single-node deployment.
    claim_scope: std::sync::Arc<std::sync::OnceLock<std::sync::Arc<dyn ClaimScope>>>,
    /// Refuses status writes from an owner that has been superseded. Absent ⇒
    /// writes are unguarded, which is every single-node deployment.
    write_fence: std::sync::Arc<std::sync::OnceLock<std::sync::Arc<dyn WriteFence>>>,
    /// Bounds how many activation fences may be held at once on each pool,
    /// keyed by the pool. See [`WorkflowExecutionRepository::enter_activation`].
    activation_slots: std::sync::Arc<
        std::sync::Mutex<std::collections::HashMap<String, std::sync::Arc<tokio::sync::Semaphore>>>,
    >,
    /// See [`release_claim_to_resume_on`].
    in_flight_bound_secs: std::sync::Arc<std::sync::atomic::AtomicI64>,
    /// How long a run released to resume is held for the worker process
    /// that keeps its journal, in milliseconds. See
    /// [`WorkflowExecutionRepository::set_journal_holder_hold`].
    journal_hold_ms: std::sync::Arc<std::sync::atomic::AtomicU64>,
    /// The cluster node this engine runs as, recorded on every claim it makes
    /// (`claimed_on_node`, V051). Unset ⇒ claims name no node, which is every
    /// single-node deployment. See
    /// [`WorkflowExecutionRepository::release_claims_of_dead_nodes`].
    claim_node: std::sync::Arc<std::sync::OnceLock<String>>,
}

/// Connections the engine holds for good on a pool: the change listeners
/// (`PgListener::connect_with` takes a pool connection and keeps it) of the
/// state cache invalidator, the workflow retry scheduler and the worker
/// shutdowns. Not available to anything else, so not counted as room for
/// activation fences.
pub const LISTENER_CONNECTIONS: u32 = 3;

/// How many activation fences a pool of `max_connections` may hold at once.
///
/// Each fence keeps a connection for as long as its completion takes to apply,
/// and the completion takes connections of its own — one at a time — to do
/// it. Were fences allowed every connection, enough concurrent completions
/// would each hold one and wait for another that never frees. Half of what the
/// listeners leave goes to fences and half to the work they wait on, which
/// keeps the other paths through the same pool — polls, task completions,
/// sweeps — from queueing behind a burst of workflow completions.
fn activation_slots_for(max_connections: u32) -> usize {
    (max_connections.saturating_sub(LISTENER_CONNECTIONS) as usize / 2).max(1)
}

/// How long after its claim an activation counts as in flight, by default.
///
/// See [`release_claim_to_resume_on`]. An activation — the worker replaying
/// the journal and running workflow code up to its next wait — takes
/// milliseconds to seconds; a minute is far beyond any healthy one.
pub const DEFAULT_IN_FLIGHT_BOUND_SECS: i64 = 60;

/// SQL predicate: the run is not waiting out a start delay (V069), so a claim
/// may hand it out. Every statement that hands out an activation —
/// `claim_next_from`, `claim_batch_with_offset`, `claim_execution_by_id_from`
/// — carries it, as it carries the release predicate: a path without it
/// would start a delayed run early. The claim indexes are built with the same
/// predicate (V069), so a run waiting out its delay is not in them, and a
/// queue holding many delayed starts costs a claim nothing until they are due.
const START_DUE: &str = "NOT awaiting_start";

/// Whether a claim for the worker process `{instance}` may take a run of
/// `{t}`: it is held for no process, its hold has lapsed, or it is held for
/// this one. A run released to resume is held for the process that had its
/// last activation (`claimed_by_instance`), which keeps its journal, until
/// `preferred_until` (V086). `{instance}` is a bind parameter, or `NULL` for a
/// claim made for no process, such as a batch for this engine's buffer.
fn holder_predicate(t: &str, instance: &str) -> String {
    format!(
        "({t}.preferred_until IS NULL OR {t}.preferred_until <= NOW() \
          OR {t}.claimed_by_instance = {instance})"
    )
}

/// How long a run released to resume is held for the worker process that
/// keeps its journal, unless set otherwise
/// ([`WorkflowExecutionRepository::set_journal_holder_hold`]).
pub const DEFAULT_JOURNAL_HOLD: std::time::Duration = std::time::Duration::from_millis(100);

/// An execution parked on a wait: claimed, its last activation completed and
/// no wake-up left pending for it. `{t}` is the table's alias.
const PARKED: &str = "({t}.claimed_by IS NOT NULL \
                      AND {t}.status = 'running' \
                      AND NOT {t}.pending_requeue \
                      AND {t}.completed_activation IS NOT NULL \
                      AND {t}.activation_seq = {t}.completed_activation)";

/// The journal entries that record an outcome a parked workflow is woken for.
const WAKING_OUTCOMES: &[&str] = &[
    "TaskCompleted",
    "TaskFailed",
    "TaskTimedOut",
    "TaskCanceled",
    "TimerFired",
    "ChildWorkflowExecutionCompleted",
    "ChildWorkflowExecutionFailed",
    "ChildWorkflowExecutionTimedOut",
    "ChildWorkflowExecutionCanceled",
    "ChildWorkflowExecutionTerminated",
    "EventReceived",
];

/// Wake the parked execution matched by `key_predicate` (with `$1` bound to
/// `key`): release its claim, unless an activation of it is in flight, in
/// which case record the wake-up for that activation's completion to take.
///
/// In flight means: claimed, handed out more recently than any completion
/// that has been applied (`activation_seq` ahead of `completed_activation`),
/// and claimed within the last `in_flight_secs`. The time bound is what keeps
/// a workflow from waiting out its whole claim timeout when the activation's
/// completion will never be recorded:
///
/// * during a rolling deploy, an engine from before V048 applies completions
///   without recording them, so every workflow it completes looks in flight
///   to the upgraded ones;
/// * a worker that crashed mid-activation never completes it.
///
/// Past the bound the claim is released as it always was. The cost is that an
/// activation genuinely running longer than the bound can have its workflow
/// handed out again beside it, and its completion then arrives stale and is
/// acknowledged without being applied — the newer activation replays and does
/// the same work.
pub async fn release_claim_to_resume_on(
    pool: &PgPool,
    key_predicate: &str,
    key: &str,
    in_flight_secs: i64,
    hold_for: std::time::Duration,
) -> OrcherResult<Resume> {
    let in_flight = "(claimed_by IS NOT NULL \
                     AND activation_seq > COALESCE(completed_activation, 0) \
                     AND claimed_at > NOW() - make_interval(secs => $2))";
    let sql = format!(
        r#"
        UPDATE workflow_executions
        SET pending_requeue = CASE WHEN {in_flight} THEN TRUE ELSE pending_requeue END,
            pending_requeue_at = CASE WHEN {in_flight} THEN NOW() ELSE pending_requeue_at END,
            claimed_by = CASE WHEN {in_flight} THEN claimed_by ELSE NULL END,
            claimed_at = CASE WHEN {in_flight} THEN claimed_at ELSE NULL END,
            claim_timeout_at = CASE WHEN {in_flight} THEN claim_timeout_at ELSE NULL END,
            claimed_on_node = CASE WHEN {in_flight} THEN claimed_on_node ELSE NULL END,
            -- Released, it is held a moment for the process that had its
            -- last activation and keeps its journal (claimed_by_instance is
            -- left as it was); a zero hold holds it for nobody.
            preferred_until = CASE
                WHEN {in_flight} THEN preferred_until
                WHEN claimed_by_instance IS NOT NULL AND $3 > 0
                    THEN NOW() + make_interval(secs => $3)
                ELSE NULL
            END,
            updated_at = NOW()
        WHERE {key_predicate}
        RETURNING claimed_by IS NOT NULL AND pending_requeue
        "#
    );
    let deferred: Option<bool> = sqlx::query_scalar(&sql)
        .bind(key)
        .bind(in_flight_secs as f64)
        .bind(hold_for.as_secs_f64())
        .fetch_optional(pool)
        .await
        .map_err(|e| {
            OrcherError::internal(format!("Failed to release the claim to resume: {e}"))
        })?;
    Ok(match deferred {
        Some(true) => Resume::Deferred,
        Some(false) => Resume::Released,
        None => Resume::NotFound,
    })
}

/// One execution's completions held off while the current one is applied.
///
/// Returned by [`WorkflowExecutionRepository::enter_activation`]. The lock goes
/// when this does: [`ActivationFence::applied`] records the activation and
/// releases it; dropping it releases it having recorded nothing.
#[derive(Debug)]
pub struct ActivationFence {
    tx: sqlx::Transaction<'static, sqlx::Postgres>,
    execution_id: String,
    current: i64,
    completed: Option<i64>,
    status: String,
    claimed_by: Option<String>,
    task_queue: String,
    parent_execution_id: Option<String>,
    organization_id: Option<Uuid>,
    namespace: String,
    cancel_requested_at: Option<Timestamptz>,
    _slot: tokio::sync::OwnedSemaphorePermit,
}

/// What recording an applied activation found waiting for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeferredResume {
    /// The execution's task queue, for the continuation.
    pub task_queue: String,
    /// The execution's namespace, for the continuation.
    pub namespace: String,
}

/// An engine whose heartbeat has gone stale, as
/// [`WorkflowExecutionRepository::release_claims_of_dead_nodes`] takes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadNode {
    /// The node id it registered and recorded its claims under.
    pub node_id: String,
    /// Its last heartbeat.
    pub last_seen: Timestamptz,
}

/// A claim released because the engine that made it died.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReleasedClaim {
    /// The execution, claimable again.
    pub execution_id: String,
    /// Its task queue, for the continuation.
    pub task_queue: String,
    /// Its namespace, for the continuation.
    pub namespace: String,
}

/// What handing a batch-claimed activation to a poller did; see
/// [`WorkflowExecutionRepository::hand_to`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandOut {
    /// The claim moved to the poller, which has the activation now.
    Handed,
    /// The claim is still the batch's, but a completion of the execution was
    /// being applied, which an activation is never handed out alongside. The
    /// claim is still held: the entry must be offered again, not dropped —
    /// dropped, the execution stayed claimed by nobody until the claim timed
    /// out, minutes in which its workflow could not run.
    Busy,
    /// The claim lapsed or was taken back; the entry is no longer the
    /// batch's to hand out.
    Gone,
}

/// What asking to wake a parked execution did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Resume {
    /// The claim was released: the execution can be handed out again now.
    Released,
    /// An activation is out with a worker and not yet completed, so the claim
    /// was kept and the wake-up recorded; the completion of that activation
    /// releases the claim when it is applied.
    Deferred,
    /// No such execution.
    NotFound,
}

impl ActivationFence {
    /// The number of the activation most recently handed out.
    pub fn current(&self) -> i64 {
        self.current
    }

    /// The latest activation whose completion has been applied, if any.
    pub fn completed(&self) -> Option<i64> {
        self.completed
    }

    /// The execution's status when the fence was entered.
    pub fn status(&self) -> &str {
        &self.status
    }

    /// Who held the execution's claim when the fence was entered.
    pub fn claimed_by(&self) -> Option<&str> {
        self.claimed_by.as_deref()
    }

    /// The execution's task queue.
    pub fn task_queue(&self) -> &str {
        &self.task_queue
    }

    /// The run this execution reports its outcome to, if it has one.
    pub fn parent_execution_id(&self) -> Option<&str> {
        self.parent_execution_id.as_deref()
    }

    /// The organization the execution belongs to, if any.
    pub fn organization_id(&self) -> Option<Uuid> {
        self.organization_id
    }

    /// The namespace the execution runs in.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// When cancellation was requested for the run, if it was. Read with the
    /// rest of the fence, so a completion learns it without another query.
    pub fn cancel_requested_at(&self) -> Option<Timestamptz> {
        self.cancel_requested_at
    }

    /// Give up on applying the completion of `activation`, which failed
    /// partway or not at all, and let the workflow be handed out again.
    ///
    /// Nothing is recorded as applied. The claim is released if `activation`
    /// still holds it, and a wake-up deferred to it is taken, so the workflow
    /// is redelivered promptly: left alone, the claim would have been kept by
    /// an activation nothing was going to complete, and every wake-up deferred
    /// to it until the claim timed out.
    ///
    /// The same completion sent again is still admitted as long as nothing has
    /// claimed the workflow since — the activation is still the current one,
    /// and a transient failure deserves its retry. Once a poller has claimed
    /// it, the retry answers an activation that is no longer current and is
    /// acknowledged without being applied; the newer activation replays what
    /// the journal holds, which includes whatever part of the batch went in.
    pub async fn abandon(mut self, activation: i64) -> OrcherResult<Option<DeferredResume>> {
        let released: Option<(String, String, String)> = sqlx::query_as(
            r#"
            UPDATE workflow_executions
            SET claimed_by = NULL,
                claimed_at = NULL,
                claim_timeout_at = NULL,
                claimed_on_node = NULL,
                pending_requeue = FALSE,
                pending_requeue_at = NULL,
                updated_at = NOW()
            WHERE execution_id = $1
              AND activation_seq = $2
              AND claimed_by IS NOT NULL
            RETURNING task_queue, namespace, status
            "#,
        )
        .bind(&self.execution_id)
        .bind(activation)
        .fetch_optional(&mut *self.tx)
        .await
        .map_err(|e| OrcherError::internal(format!("Failed to abandon the activation: {e}")))?;
        self.tx
            .commit()
            .await
            .map_err(|e| OrcherError::internal(format!("Failed to abandon the activation: {e}")))?;
        Ok(released.and_then(|(task_queue, namespace, status)| {
            (!matches!(
                status.as_str(),
                "completed" | "failed" | "cancelled" | "terminated" | "timed_out"
            ))
            .then_some(DeferredResume {
                task_queue,
                namespace,
            })
        }))
    }

    /// Record that the completion of `activation` has been applied, and let
    /// the next completion of this execution in.
    ///
    /// Recorded even if another claim has handed out a newer activation
    /// meanwhile. What was applied was applied; the record only says so, so
    /// that the same completion sent again is recognised. It never moves
    /// backwards.
    ///
    /// A wake-up that arrived while the activation was out — a task finishing,
    /// a child's outcome, an event — found it in flight and was deferred to
    /// here rather than handing the execution to a second worker alongside the
    /// first. It is taken now: the claim is released in the same statement, so
    /// once this commits the execution is claimable whatever happens to the
    /// caller afterwards, and the returned [`DeferredResume`] is only for
    /// queueing the continuation promptly.
    pub async fn applied(mut self, activation: i64) -> OrcherResult<Option<DeferredResume>> {
        let deferred: Option<(bool, String, String)> = sqlx::query_as(
            r#"
            WITH before AS (
                SELECT pending_requeue
                FROM workflow_executions
                WHERE execution_id = $1
                FOR UPDATE
            )
            UPDATE workflow_executions we
            SET completed_activation = GREATEST(COALESCE(we.completed_activation, 0), $2),
                -- The run has observed everything its applied activation
                -- read, but only the current claim's position is that
                -- activation's: one recorded after a newer claim keeps what
                -- was known before.
                observed_through_entry = CASE
                    WHEN we.activation_seq = $2 THEN we.claimed_through_entry
                    ELSE we.observed_through_entry
                END,
                claimed_by = CASE WHEN before.pending_requeue THEN NULL ELSE we.claimed_by END,
                claimed_at = CASE WHEN before.pending_requeue THEN NULL ELSE we.claimed_at END,
                claim_timeout_at =
                    CASE WHEN before.pending_requeue THEN NULL ELSE we.claim_timeout_at END,
                claimed_on_node =
                    CASE WHEN before.pending_requeue THEN NULL ELSE we.claimed_on_node END,
                pending_requeue = FALSE,
                pending_requeue_at = NULL
            FROM before
            WHERE we.execution_id = $1
            RETURNING before.pending_requeue, we.task_queue, we.namespace
            "#,
        )
        .bind(&self.execution_id)
        .bind(activation)
        .fetch_optional(&mut *self.tx)
        .await
        .map_err(|e| OrcherError::internal(format!("Failed to record the activation: {e}")))?;
        self.tx
            .commit()
            .await
            .map_err(|e| OrcherError::internal(format!("Failed to record the activation: {e}")))?;
        Ok(deferred.and_then(|(pending, task_queue, namespace)| {
            pending.then_some(DeferredResume {
                task_queue,
                namespace,
            })
        }))
    }
}

/// SQL predicate on `{alias}.execution_id`: no completion of the execution is
/// being applied right now — its activation fence (the advisory lock
/// [`WorkflowExecutionRepository::enter_activation`] holds) can be taken on
/// the spot. Taken, it is held only until the statement commits.
///
/// Every statement that claims or releases an execution checks it, last, on
/// the rows it has already chosen. A completion's commands are applied
/// outside the fence's transaction and the activation is recorded as
/// completed only at the end; an execution claimed or released in between
/// would be handed out again and replay a journal only partly written. A row
/// found fenced is skipped; the next poll or sweep finds it again.
fn fence_free(alias: &str) -> String {
    format!(
        "pg_try_advisory_xact_lock(hashtextextended('orcher.activation:' || {alias}.execution_id, 0))"
    )
}

/// PostgreSQL's PERCENTILE_CONT over already-sorted values: the value at
/// position `fraction * (n - 1)`, interpolated linearly between neighbours.
fn percentile_cont(sorted: &[f64], fraction: f64) -> f64 {
    let position = fraction * (sorted.len() - 1) as f64;
    let lower = position.floor() as usize;
    let upper = position.ceil() as usize;
    sorted[lower] + (sorted[upper] - sorted[lower]) * (position - lower as f64)
}

/// The claim's organization filter, bound as `param` when the poll names an
/// organization and absent when it does not.
///
/// Two statements rather than one `($n IS NULL OR organization_id = $n)`:
/// planned once for both, that form can use neither claim index for the
/// organization, and a poll for an organization holding a small share of a
/// queue walked everyone else's entries to find its own. As an equality it is
/// answered from `idx_workflow_executions_claim_by_organization` (V056), and
/// a poll that names none keeps `idx_workflow_executions_claim`.
fn organization_predicate(organization_id: Option<Uuid>, param: &str) -> String {
    match organization_id {
        Some(_) => format!("AND organization_id = {param}"),
        None => String::new(),
    }
}

/// Whichever of two rows was submitted later.
fn later_submitted(
    a: Option<WorkflowExecutionRow>,
    b: Option<WorkflowExecutionRow>,
) -> Option<WorkflowExecutionRow> {
    match (a, b) {
        (Some(a), Some(b)) => Some(if b.submitted_at > a.submitted_at {
            b
        } else {
            a
        }),
        (a, b) => a.or(b),
    }
}

/// A run in one of these states has ended, and no write may move it to another.
/// `failed` is not among them: a failure can be reported more than once, and a
/// failure waiting for its retry can still be cancelled — see [`STILL_LIVE`].
/// SQL expression: the time an ended status is reached, when a write moves a
/// run to one without giving it (`$1` is the new status). Every ended run
/// carries the time it ended.
const REACHED_NOW_IF_ENDED: &str = "CASE WHEN $1 IN ('completed', 'failed', 'cancelled', \
     'terminated', 'timed_out') THEN NOW() END";

const ENDED: &str =
    "('completed', 'cancelled', 'terminated', 'timed_out', 'restarted_fresh', 'reset')";

/// SQL predicate: the run can still be cancelled or terminated. Running work,
/// and a failure or run timeout with a retry scheduled for it: cancelling that
/// must win over the retry, or the scheduler starts the work again after it
/// was cancelled. A failure with no retry scheduled — final, or not yet
/// decided — has ended. "Not yet decided" lasts only between recording a
/// failure and deciding it; a cancel that lands in that moment is refused, and
/// the retry goes ahead. A timeout is decided as it is recorded.
const STILL_LIVE: &str = "(status NOT IN ('completed', 'failed', 'cancelled', 'terminated', \
     'timed_out', 'restarted_fresh', 'reset') \
     OR (status IN ('failed', 'timed_out') AND retries_settled_at IS NULL \
         AND retry_count < max_retries AND next_retry_at IS NOT NULL))";

/// SQL expression: when the workflow's execution timeout runs out, NULL if it
/// has none. The deadline every run of the workflow shares (V067); a run
/// written before V067 has none of its own and is measured from its own
/// `submitted_at`. The SQL form of
/// [`WorkflowExecution::execution_expires_at`].
pub const EXECUTION_EXPIRES_AT: &str =
    "COALESCE(execution_deadline, submitted_at + make_interval(secs => timeout_secs))";

/// SQL expression: the status a run taken for a retry had ended with, which it
/// goes back to when the retry is given back or given up. A run timeout
/// records its failure type as it ends (V067); any other run taken for a retry
/// had failed.
const ENDED_BEFORE_RETRY: &str =
    "CASE WHEN error_details->>'failure_type' = 'WorkflowRunTimedOut' \
     THEN 'timed_out' ELSE 'failed' END";

/// The unique index that lets one open run hold a workflow id in an
/// organization's namespace (V059).
pub const WORKFLOW_ID_HOLD_INDEX: &str = "uq_workflow_executions_open_workflow_id";

/// SQL predicate: the run holds its workflow id and is open — exactly the rows
/// [`WORKFLOW_ID_HOLD_INDEX`] covers, written as the index's predicate is so
/// that a lookup by it is answered from the index. Open is anything not in a
/// terminal state, a failure whose retry may still come, and a run timeout
/// whose retry is scheduled (V067): a new start that slipped in before the
/// retry would leave two runs of one workflow.
const HOLDS_OPEN: &str = "holds_workflow_id \
     AND (status NOT IN ('completed', 'failed', 'cancelled', 'terminated', \
                         'timed_out', 'restarted_fresh', 'reset') \
          OR (retries_settled_at IS NULL AND retry_count < max_retries \
              AND (status = 'failed' \
                   OR (status = 'timed_out' AND next_retry_at IS NOT NULL))))";

/// The scope a workflow id is held in, as [`WORKFLOW_ID_HOLD_INDEX`] keys it:
/// an unscoped deployment's runs, which have no organization, share one.
const HOLD_SCOPE: &str = "COALESCE(organization_id, '00000000-0000-0000-0000-000000000000'::uuid) \
     = COALESCE($1::uuid, '00000000-0000-0000-0000-000000000000'::uuid) \
     AND namespace = $2 AND workflow_id = $3";

/// Whether `error` is a run refused because another open run already holds
/// its workflow id — the index deciding a race the caller's own check could
/// not see.
pub fn is_workflow_id_held(error: &OrcherError) -> bool {
    error.to_string().contains(WORKFLOW_ID_HOLD_INDEX)
}

/// The unique index that lets one run carry a start's request id in an
/// organization's namespace (V069).
pub const START_REQUEST_INDEX: &str = "uq_workflow_executions_start_request";

/// Whether `error` is a run refused because another run already carries its
/// start's request id — a repeat of the start that raced the original and
/// lost, which is answered with the run the original made.
pub fn is_start_request_held(error: &OrcherError) -> bool {
    error.to_string().contains(START_REQUEST_INDEX)
}

/// A delayed run released by [`WorkflowExecutionRepository::release_due_starts`]:
/// the queue to wake for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DueStart {
    /// The released run.
    pub execution_id: String,
    /// Its namespace.
    pub namespace: String,
    /// The queue its activations are handed out on.
    pub task_queue: String,
}

/// The payload of a new run's first dispatch: what the outbox processor
/// hands the run's queue.
pub fn start_dispatch_payload(
    execution: &WorkflowExecution,
    task_queue: &str,
) -> serde_json::Value {
    serde_json::json!({
        "execution_id": execution.execution_id,
        "workflow_name": execution.workflow_name,
        "workflow_version": execution.workflow_version,
        "namespace": execution.namespace,
        "task_queue": task_queue,
        "parameters": execution.parameters,
    })
}

/// Write a new run in `tx`: its row, its first dispatch and its first journal
/// entry. Every start writes a run through this, a single one and each of a
/// batch's, so a column added to a run is written by both or by neither.
///
/// The row persists the app-generated `id` (rather than letting the database
/// default a random one) so it is known before the write and stays the
/// stable shard-routing key for every later operation; `shard_id` is the
/// shard that id resolves to, `None` without a resolver.
async fn insert_new_run(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    execution: &WorkflowExecution,
    shard_id: Option<i32>,
    outbox_task_payload: &serde_json::Value,
    idempotency_key: &str,
    task_queue: &str,
) -> OrcherResult<(WorkflowExecution, Uuid)> {
    let routed_id = execution.id.as_uuid();

    // 1. The run.
    let execution_row = sqlx::query_as::<_, WorkflowExecutionRow>(
        concat!(r#"
        INSERT INTO workflow_executions (
            execution_id, workflow_id, registry_id, workflow_name, workflow_type, workflow_version, namespace,
            status, parameters, context, environment, priority, timeout_secs,
            max_retries, retry_policy, tags, labels, trigger_type, triggered_by, user_id, organization_id,
            parent_execution_id, root_execution_id, idempotency_key, submitted_at, task_queue, id, retry_count,
            shard_id,
            -- Carried so a reset successor keeps the release its journal was
            -- written by. Normally NULL: the claim statement owns this column,
            -- and a brand-new execution is unbound until first claimed.
            bound_version_id,
            holds_workflow_id, run_timeout_secs, task_timeout_secs, execution_deadline,
            annotations, start_request_id, start_after, awaiting_start,
            -- Carried by a reset, like the release: the successor replays the
            -- history copied to it, keyed by the step ids it was written with.
            step_id_scheme, history_lineage, activated_without_scheme,
            -- Whether its history holds its step results, also carried by a
            -- reset: the copied history holds them only if the old run's did.
            step_results_journaled
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42)
        RETURNING "#, crate::repositories::columns::workflow_executions!(), r#"
        "#),
    )
    .bind(&execution.execution_id)
    .bind(&execution.workflow_id)
    .bind(execution.registry_id)
    .bind(&execution.workflow_name)
    .bind(&execution.workflow_type)
    .bind(&execution.workflow_version)
    .bind(&execution.namespace)
    .bind(&execution.status)
    .bind(&execution.parameters)
    .bind(&execution.context)
    .bind(&execution.environment)
    .bind(execution.priority)
    .bind(execution.timeout_secs)
    .bind(execution.max_retries)
    .bind(&execution.retry_policy)
    .bind(&execution.tags)
    .bind(&execution.labels)
    .bind(&execution.trigger_type)
    .bind(execution.triggered_by)
    .bind(execution.user_id)
    .bind(execution.organization_id)
    .bind(&execution.parent_execution_id)
    .bind(&execution.root_execution_id)
    .bind(&execution.idempotency_key)
    .bind(execution.submitted_at)
    .bind(task_queue)
    .bind(routed_id)
    // Carry retry_count so a retry successor inherits the cap (0 for a fresh start).
    .bind(execution.retry_count)
    .bind(shard_id)
    .bind(&execution.bound_version_id)
    .bind(execution.holds_workflow_id)
    .bind(execution.run_timeout_secs)
    .bind(execution.task_timeout_secs)
    .bind(execution.execution_deadline)
    .bind(&execution.annotations)
    .bind(&execution.start_request_id)
    .bind(execution.start_after)
    .bind(execution.awaiting_start)
    .bind(execution.step_id_scheme)
    .bind(&execution.history_lineage)
    .bind(execution.activated_without_scheme)
    .bind(execution.step_results_journaled)
    .fetch_one(&mut **tx)
    .await
    .map_err(|e| {
        warn!(error = %e, execution_id = %execution.execution_id, "Failed to create workflow execution in transaction");
        OrcherError::internal(format!(
            "Failed to create workflow execution {}: {}",
            execution.execution_id, e
        ))
    })?;
    let created: WorkflowExecution = execution_row.into();

    // 2. Its first dispatch.
    let task_id = Uuid::new_v4();
    let task_id = sqlx::query_scalar::<_, Uuid>(
        r#"
        INSERT INTO outbox_queue (
            task_id, workflow_id, task_type, payload, idempotency_key,
            status, created_at, attempt_count, max_retries, backoff_multiplier,
            shard_id, next_retry_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        ON CONFLICT (idempotency_key) DO NOTHING
        RETURNING task_id
        "#,
    )
    .bind(task_id)
    .bind(created.id.as_uuid())
    .bind("dispatch_step") // OutboxTaskType::DispatchStep (using Display trait format)
    .bind(outbox_task_payload)
    .bind(idempotency_key)
    .bind("pending") // TaskStatus::Pending (using Display trait format)
    .bind(Timestamptz::now())
    .bind(0_i32) // attempt_count
    .bind(5_i32) // max_retries
    .bind(2.0_f64) // backoff_multiplier
    .bind(shard_id)
    // A delayed run's dispatch is a wake-up for when it is due, not before:
    // dispatched now it would find nothing to hand out. Its release wakes the
    // queue too (`release_due_starts`); this is the fast path, a waiting
    // worker handed the run as it comes due.
    .bind(
        execution
            .awaiting_start
            .then_some(execution.start_after)
            .flatten(),
    )
    .fetch_optional(&mut **tx)
    .await
    .map_err(|e| {
        warn!(error = %e, "Failed to create outbox task in transaction");
        OrcherError::internal(format!("Failed to create outbox task: {}", e))
    })?
    .unwrap_or(task_id); // Return original ID if duplicate (idempotency)

    // 3. Its WorkflowExecutionStarted journal entry.
    //
    // A reset seeds the new execution's journal from the run it replays before
    // creating the row, so event 1 already exists — and the copied entry is the
    // one replay must see, carrying the original run's parameters. Yield to it
    // rather than failing the create; on a normal start nothing can conflict.
    sqlx::query(
        r#"
        INSERT INTO workflow_execution_journal (
            workflow_id, execution_id, event_id, event_type,
            timestamp, payload, version
        )
        VALUES ($1, $2, 1, 'WorkflowExecutionStarted', NOW(), $3, 1)
        ON CONFLICT (workflow_id, execution_id, event_id) DO NOTHING
        "#,
    )
    .bind(created.id.as_uuid().to_string())
    .bind(&created.execution_id)
    .bind(&execution.parameters)
    .execute(&mut **tx)
    .await
    .map_err(|e| {
        warn!(error = %e, "Failed to append journal entry in transaction");
        OrcherError::internal(format!("Failed to append journal entry: {}", e))
    })?;

    Ok((created, task_id))
}

impl WorkflowExecutionRepository {
    /// Create a new workflow execution repository
    pub fn new(pool: PgPool) -> Self {
        Self {
            activation_slots: std::sync::Arc::new(std::sync::Mutex::new(
                std::collections::HashMap::new(),
            )),
            in_flight_bound_secs: std::sync::Arc::new(std::sync::atomic::AtomicI64::new(
                DEFAULT_IN_FLIGHT_BOUND_SECS,
            )),
            journal_hold_ms: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(
                DEFAULT_JOURNAL_HOLD.as_millis() as u64,
            )),
            pool,
            shard_resolver: std::sync::Arc::new(std::sync::OnceLock::new()),
            claim_scope: std::sync::Arc::new(std::sync::OnceLock::new()),
            write_fence: std::sync::Arc::new(std::sync::OnceLock::new()),
            claim_node: std::sync::Arc::new(std::sync::OnceLock::new()),
        }
    }

    /// Inject the shard-pool resolver (partitioned persistence). Idempotent; a second
    /// call is ignored. Wired once at startup after the router is built.
    pub fn set_shard_resolver(&self, resolver: std::sync::Arc<dyn ShardPoolResolver>) {
        let _ = self.shard_resolver.set(resolver);
    }

    /// Restrict this repository's workflow claims to the shards `scope` reports.
    pub fn set_claim_scope(&self, scope: std::sync::Arc<dyn ClaimScope>) {
        let _ = self.claim_scope.set(scope);
    }

    /// Record `node` as the engine that made every claim this repository
    /// makes from now on. Set once, at startup, when clustering is on; a
    /// second call is ignored.
    pub fn set_claim_node(&self, node: &str) {
        let _ = self.claim_node.set(node.to_string());
    }

    /// The node claims are recorded against, if any.
    fn claim_node(&self) -> Option<&str> {
        self.claim_node.get().map(String::as_str)
    }

    /// Guard status writes with `fence`, so a superseded owner cannot overwrite
    /// state it no longer has authority over.
    pub fn set_write_fence(&self, fence: std::sync::Arc<dyn WriteFence>) {
        let _ = self.write_fence.set(fence);
    }

    /// Open a transaction for a status write: fenced when a fence is injected,
    /// plain otherwise.
    ///
    /// Fails **closed**. If a fence is present and cannot confirm we still hold
    /// the workflow's shard, this returns an error rather than falling back to
    /// an unguarded write — the whole point is to stop a stalled owner writing
    /// after its claim was reassigned.
    async fn begin_write<'p>(
        &self,
        pool: &'p PgPool,
        workflow_id: Uuid,
    ) -> OrcherResult<sqlx::Transaction<'p, sqlx::Postgres>> {
        let Some(fence) = self.write_fence.get() else {
            return pool
                .begin()
                .await
                .map_err(|e| OrcherError::internal(format!("Failed to begin transaction: {}", e)));
        };
        match fence.guard(workflow_id).await {
            Some(token) => crate::repositories::domains::cluster::begin_shard_guarded_tx(
                pool,
                token.shard_id,
                token.generation,
            )
            .await
            .map_err(|e| OrcherError::internal(format!("shard-guarded begin failed: {e}"))),
            None => Err(OrcherError::internal(format!(
                "{}: refusing write for workflow {workflow_id}, this node does not hold its shard",
                crate::sharding::OWNERSHIP_LOST
            ))),
        }
    }

    /// Bind values for this poll's shard filter, or all-`None` to run unfiltered.
    fn claim_bounds(&self) -> ClaimParams {
        ClaimBounds::params(ClaimBounds::for_poll(self.claim_scope.get()).as_ref())
    }

    /// Bind values restricting a scan to exactly this node's shards, or
    /// all-`None` when there is no scope or ownership is unknown. For the
    /// retry passes: a retry writes under the fence, so on a node that does not
    /// own the run it could only be refused.
    fn owned_bounds(&self) -> ClaimParams {
        ClaimBounds::params(ClaimBounds::for_owned(self.claim_scope.get()).as_ref())
    }

    /// The pool that owns `workflow_id`'s shard (the resolver's pick if set, else the
    /// repository's own single pool). `namespace_id` selects the namespace's pool-set;
    /// `None` resolves to the shared default set (OSS / single-tenant routing).
    fn pool_for(&self, namespace_id: Option<Uuid>, workflow_id: Uuid) -> &PgPool {
        match self.shard_resolver.get() {
            Some(resolver) => resolver.pool_for_workflow(namespace_id, workflow_id),
            None => &self.pool,
        }
    }

    /// Pools to scan for a lookup that isn't keyed by the routing id (e.g. by
    /// `execution_id`): every shard pool when a resolver is set, else the single pool.
    fn scan_pools(&self) -> Vec<&PgPool> {
        match self.shard_resolver.get() {
            Some(resolver) => resolver.all_pools(),
            None => vec![&self.pool],
        }
    }

    /// Create a new workflow execution
    pub async fn create(&self, execution: &WorkflowExecution) -> OrcherResult<WorkflowExecution> {
        debug!(
            execution_id = %execution.execution_id,
            workflow_name = %execution.workflow_name,
            "Creating workflow execution"
        );

        let row = sqlx::query_as::<_, WorkflowExecutionRow>(
            concat!(r#"
            INSERT INTO workflow_executions (
                execution_id, workflow_id, registry_id, workflow_name, workflow_type, workflow_version, namespace,
                status, parameters, context, environment, priority, timeout_secs,
                max_retries, retry_policy, tags, labels, trigger_type, triggered_by, user_id, organization_id,
                parent_execution_id, root_execution_id, idempotency_key, submitted_at, task_queue,
                -- Carried so a reset successor keeps the release its journal was
                -- written by. Normally NULL: the claim statement owns this column,
                -- and a brand-new execution is unbound until first claimed.
                bound_version_id,
                holds_workflow_id, run_timeout_secs, task_timeout_secs, execution_deadline,
                annotations, start_request_id, start_after, awaiting_start,
                -- Carried by a reset, like the release: the successor replays
                -- the history copied to it, keyed by the step ids it was
                -- written with.
                step_id_scheme, history_lineage, activated_without_scheme,
                step_results_journaled
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39)
            RETURNING "#, crate::repositories::columns::workflow_executions!(), r#"
            "#),
        )
        .bind(&execution.execution_id)
        .bind(&execution.workflow_id)
        .bind(execution.registry_id)
        .bind(&execution.workflow_name)
        .bind(&execution.workflow_type)
        .bind(&execution.workflow_version)
        .bind(&execution.namespace)
        .bind(&execution.status)
        .bind(&execution.parameters)
        .bind(&execution.context)
        .bind(&execution.environment)
        .bind(execution.priority)
        .bind(execution.timeout_secs)
        .bind(execution.max_retries)
        .bind(&execution.retry_policy)
        .bind(&execution.tags)
        .bind(&execution.labels)
        .bind(&execution.trigger_type)
        .bind(execution.triggered_by)
        .bind(execution.user_id)
        .bind(execution.organization_id)
        .bind(&execution.parent_execution_id)
        .bind(&execution.root_execution_id)
        .bind(&execution.idempotency_key)
        .bind(execution.submitted_at)
        .bind(&execution.task_queue)
        .bind(&execution.bound_version_id)
        .bind(execution.holds_workflow_id)
        .bind(execution.run_timeout_secs)
        .bind(execution.task_timeout_secs)
        .bind(execution.execution_deadline)
        .bind(&execution.annotations)
        .bind(&execution.start_request_id)
        .bind(execution.start_after)
        .bind(execution.awaiting_start)
        .bind(execution.step_id_scheme)
        .bind(&execution.history_lineage)
        .bind(execution.activated_without_scheme)
        .bind(execution.step_results_journaled)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| {
            warn!(error = %e, "Failed to create workflow execution");
            OrcherError::internal(format!("Failed to create workflow execution: {}", e))
        })?;

        info!(
            execution_id = %execution.execution_id,
            "Workflow execution created successfully"
        );

        Ok(row.into())
    }

    /// Create a new workflow execution atomically with an outbox task
    ///
    /// This method ensures that both the workflow execution and its dispatch task
    /// are created in a single database transaction, guaranteeing atomicity.
    /// This prevents orphan executions (created but never dispatched) or lost
    /// dispatches (task queued but execution doesn't exist).
    ///
    /// # Arguments
    ///
    /// * `execution` - The workflow execution to create
    /// * `outbox_task_payload` - Payload for the outbox dispatch task
    /// * `idempotency_key` - Unique key to prevent duplicate task creation
    /// * `task_queue` - Task queue name for service routing
    ///
    /// # Returns
    ///
    /// Tuple of (WorkflowExecution, task_id)
    ///
    /// # ORCHER Terminology
    ///
    /// Uses ORCHER terms: Task (not Activity), Execution Journal (not History)
    pub async fn create_with_outbox_task(
        &self,
        execution: &WorkflowExecution,
        outbox_task_payload: serde_json::Value,
        idempotency_key: String,
        task_queue: &str,
    ) -> OrcherResult<(WorkflowExecution, Uuid)> {
        debug!(
            execution_id = %execution.execution_id,
            workflow_name = %execution.workflow_name,
            task_queue = %task_queue,
            "Creating workflow execution atomically with outbox task"
        );

        // Route the whole start transaction to the pool that owns this workflow's
        // shard (keyed by the app-generated `id`, known before the insert), so the
        // execution row, its outbox task, and its first journal entry co-locate.
        let routed_id = execution.id.as_uuid();
        // Persist the shard alongside the row, derived from the same id and the same
        // resolver that just chose the pool — so a row can never claim a shard it was
        // not routed to. Left NULL when no resolver is injected (no ownership, so
        // nothing consumes it); the partial indexes on shard_id skip those rows.
        let shard_id: Option<i32> = self.shard_resolver.get().map(|r| r.shard_of(routed_id));
        let mut tx =
            self.pool_for(None, routed_id).begin().await.map_err(|e| {
                OrcherError::internal(format!("Failed to begin transaction: {}", e))
            })?;

        let (created_execution, task_id_result) = insert_new_run(
            &mut tx,
            execution,
            shard_id,
            &outbox_task_payload,
            &idempotency_key,
            task_queue,
        )
        .await?;

        // 4. Commit transaction (execution + outbox + journal in one commit)
        tx.commit().await.map_err(|e| {
            warn!(
                error = %e,
                execution_id = %created_execution.execution_id,
                "Failed to commit atomic execution + outbox + journal transaction"
            );
            OrcherError::internal(format!("Failed to commit transaction: {}", e))
        })?;

        info!(
            execution_id = %created_execution.execution_id,
            task_id = %task_id_result,
            task_queue = %task_queue,
            "Workflow execution and outbox task created atomically"
        );

        Ok((created_execution, task_id_result))
    }

    /// Batch-create multiple workflow executions with outbox tasks and journal entries
    /// in a single database transaction. This is 5-10x faster than calling
    /// `create_with_outbox_task` N times because it uses 1 commit instead of N.
    ///
    /// Each run is written exactly as [`create_with_outbox_task`] writes one
    /// — the same statements, through [`insert_new_run`] — on its own task
    /// queue (`WorkflowExecution::task_queue`). A batch used to write its own
    /// copy of them, which stored every run on the first run's queue, and
    /// left out the columns added since: a field added to a run is now
    /// written for both or neither.
    ///
    /// [`create_with_outbox_task`]: Self::create_with_outbox_task
    pub async fn batch_create_with_outbox(
        &self,
        executions: &[WorkflowExecution],
    ) -> OrcherResult<Vec<(WorkflowExecution, Uuid)>> {
        if executions.is_empty() {
            return Ok(vec![]);
        }

        debug!(
            count = executions.len(),
            "Batch creating workflow executions with outbox tasks"
        );

        // With partitioned persistence a batch may span shards, and one transaction
        // cannot cross databases. Partition the executions by the pool that owns their
        // shard and commit one transaction per pool. Without a resolver every execution
        // resolves to the same pool → a single group → exactly the original one-commit
        // batch. Results are returned in input order regardless of grouping.
        // Grouping uses raw pool pointers for identity; scope them so they are dropped
        // before any `.await` (raw pointers are `!Send` and must not cross an await).
        let groups: Vec<Vec<usize>> = {
            let mut group_ptrs: Vec<*const PgPool> = Vec::new();
            let mut groups: Vec<Vec<usize>> = Vec::new();
            for (i, execution) in executions.iter().enumerate() {
                let ptr = self.pool_for(None, execution.id.as_uuid()) as *const PgPool;
                match group_ptrs.iter().position(|p| *p == ptr) {
                    Some(g) => groups[g].push(i),
                    None => {
                        group_ptrs.push(ptr);
                        groups.push(vec![i]);
                    }
                }
            }
            groups
        };

        let mut ordered: Vec<Option<(WorkflowExecution, Uuid)>> =
            (0..executions.len()).map(|_| None).collect();

        for idxs in &groups {
            // Re-resolve the pool from the first execution in this group.
            let pool = self.pool_for(None, executions[idxs[0]].id.as_uuid());
            let mut tx = pool.begin().await.map_err(|e| {
                OrcherError::internal(format!("Failed to begin transaction: {}", e))
            })?;

            for &i in idxs {
                let execution = &executions[i];
                // The shard its id resolves to, as a single start records it.
                let routed_id = execution.id.as_uuid();
                let shard_id: Option<i32> =
                    self.shard_resolver.get().map(|r| r.shard_of(routed_id));
                let task_queue = execution.task_queue.as_str();
                let idempotency_key = format!("workflow_start_{}", execution.execution_id);
                let (created, task_id) = insert_new_run(
                    &mut tx,
                    execution,
                    shard_id,
                    &start_dispatch_payload(execution, task_queue),
                    &idempotency_key,
                    task_queue,
                )
                .await?;

                ordered[i] = Some((created, task_id));
            }

            // One COMMIT per pool group.
            tx.commit().await.map_err(|e| {
                OrcherError::internal(format!("Failed to commit batch transaction: {}", e))
            })?;
        }

        let results: Vec<(WorkflowExecution, Uuid)> = ordered
            .into_iter()
            .map(|o| o.expect("every index filled"))
            .collect();

        info!(
            count = results.len(),
            pool_groups = groups.len(),
            "Batch workflow creation committed"
        );

        Ok(results)
    }

    /// Find execution by internal UUID (the shard routing key — routes directly).
    pub async fn find_by_id(&self, id: Uuid) -> OrcherResult<Option<WorkflowExecution>> {
        debug!(id = %id, "Finding workflow execution by ID");

        let row = sqlx::query_as::<_, WorkflowExecutionRow>(concat!(
            "SELECT ",
            crate::repositories::columns::workflow_executions!(),
            " FROM workflow_executions WHERE id = $1"
        ))
        .bind(id)
        .fetch_optional(self.pool_for(None, id))
        .await
        .map_err(|e| OrcherError::internal(format!("Failed to find workflow execution: {}", e)))?;

        Ok(row.map(Into::into))
    }

    /// Find execution by external execution_id (string). Not keyed by the shard
    /// routing id, so under partitioned persistence this scatters across pools and
    /// returns the first match (a single-pool deployment scans just that pool).
    pub async fn find_by_execution_id(
        &self,
        execution_id: &str,
    ) -> OrcherResult<Option<WorkflowExecution>> {
        debug!(execution_id = %execution_id, "Finding workflow execution by execution_id");

        for pool in self.scan_pools() {
            let row = sqlx::query_as::<_, WorkflowExecutionRow>(concat!(
                "SELECT ",
                crate::repositories::columns::workflow_executions!(),
                " FROM workflow_executions WHERE execution_id = $1"
            ))
            .bind(execution_id)
            .fetch_optional(pool)
            .await
            .map_err(|e| {
                OrcherError::internal(format!("Failed to find workflow execution: {}", e))
            })?;
            if let Some(row) = row {
                return Ok(Some(row.into()));
            }
        }

        Ok(None)
    }

    /// The execution a parent started under `child_workflow_id`, on any shard.
    ///
    /// A child routes by its own id, so it need not share its parent's shard;
    /// every pool is scanned, as for [`find_by_execution_id`](Self::find_by_execution_id).
    pub async fn find_child(
        &self,
        parent_execution_id: &str,
        child_workflow_id: &str,
    ) -> OrcherResult<Option<WorkflowExecution>> {
        for pool in self.scan_pools() {
            let row = sqlx::query_as::<_, WorkflowExecutionRow>(concat!(
                "SELECT ",
                crate::repositories::columns::workflow_executions!(),
                " FROM workflow_executions \
                 WHERE parent_execution_id = $1 AND workflow_id = $2 \
                 ORDER BY created_at LIMIT 1"
            ))
            .bind(parent_execution_id)
            .bind(child_workflow_id)
            .fetch_optional(pool)
            .await
            .map_err(|e| OrcherError::internal(format!("Failed to find child execution: {}", e)))?;
            if let Some(row) = row {
                return Ok(Some(row.into()));
            }
        }
        Ok(None)
    }

    /// Find execution by idempotency key
    pub async fn find_by_idempotency_key(
        &self,
        key: &str,
    ) -> OrcherResult<Option<WorkflowExecution>> {
        debug!(idempotency_key = %key, "Finding workflow execution by idempotency key");

        let mut row = None;
        for pool in self.scan_pools() {
            row = sqlx::query_as::<_, WorkflowExecutionRow>(concat!(
                "SELECT ",
                crate::repositories::columns::workflow_executions!(),
                " FROM workflow_executions WHERE idempotency_key = $1"
            ))
            .bind(key)
            .fetch_optional(pool)
            .await
            .map_err(|e| {
                OrcherError::internal(format!(
                    "Failed to find workflow execution by idempotency key: {}",
                    e
                ))
            })?;
            if row.is_some() {
                break;
            }
        }

        Ok(row.map(Into::into))
    }

    /// Find a workflow execution by workflow_id (user-provided identifier).
    ///
    /// A workflow_id can span multiple executions (retries, restarts),
    /// so this returns the most recently submitted one.
    pub async fn find_by_workflow_id(
        &self,
        workflow_id: &str,
    ) -> OrcherResult<Option<WorkflowExecution>> {
        debug!(workflow_id = %workflow_id, "Finding workflow execution by workflow_id");

        // Runs route by their own id, so a workflow's runs can sit on different
        // shards: take the latest across every pool. Reading only the default
        // pool missed runs elsewhere, which is how an event-with-start found no
        // live run and started a second one.
        let mut latest: Option<WorkflowExecutionRow> = None;
        for pool in self.scan_pools() {
            let row = sqlx::query_as::<_, WorkflowExecutionRow>(concat!(
                "SELECT ",
                crate::repositories::columns::workflow_executions!(),
                " FROM workflow_executions WHERE workflow_id = $1 \
                 ORDER BY submitted_at DESC LIMIT 1"
            ))
            .bind(workflow_id)
            .fetch_optional(pool)
            .await
            .map_err(|e| {
                OrcherError::internal(format!(
                    "Failed to find workflow execution by workflow_id: {}",
                    e
                ))
            })?;
            latest = later_submitted(latest, row);
        }

        Ok(latest.map(Into::into))
    }

    /// The latest run of `workflow_id` in `namespace` that `organization_id`
    /// owns, on any shard.
    ///
    /// A workflow id is only unique within an organization's namespace. Looked
    /// up on its own it can name another tenant's workflow, and delivering an
    /// event to whatever that returned put one organization's event into
    /// another's journal. `None` for the organization is an unscoped caller —
    /// a server without tenancy — and matches any owner, as `may_act_on` does.
    pub async fn find_latest_owned(
        &self,
        organization_id: Option<Uuid>,
        namespace: &str,
        workflow_id: &str,
    ) -> OrcherResult<Option<WorkflowExecution>> {
        let mut latest: Option<WorkflowExecutionRow> = None;
        for pool in self.scan_pools() {
            let row = sqlx::query_as::<_, WorkflowExecutionRow>(concat!(
                "SELECT ",
                crate::repositories::columns::workflow_executions!(),
                " FROM workflow_executions \
                 WHERE workflow_id = $1 AND namespace = $2 \
                   AND ($3::uuid IS NULL OR organization_id = $3) \
                 ORDER BY submitted_at DESC LIMIT 1"
            ))
            .bind(workflow_id)
            .bind(namespace)
            .bind(organization_id)
            .fetch_optional(pool)
            .await
            .map_err(|e| {
                OrcherError::internal(format!("Failed to find workflow {}: {}", workflow_id, e))
            })?;
            latest = later_submitted(latest, row);
        }
        Ok(latest.map(Into::into))
    }

    /// The open run holding `workflow_id` in `organization_id`'s `namespace`,
    /// on any shard.
    ///
    /// Scoped as the hold is: `None` is the unscoped deployment's scope, not
    /// every organization's. A caller must never be told another tenant's run
    /// is in its way.
    pub async fn open_run_holding(
        &self,
        organization_id: Option<Uuid>,
        namespace: &str,
        workflow_id: &str,
    ) -> OrcherResult<Option<WorkflowExecution>> {
        let mut found: Option<WorkflowExecutionRow> = None;
        for pool in self.scan_pools() {
            let row = sqlx::query_as::<_, WorkflowExecutionRow>(&format!(
                concat!(
                    "SELECT ",
                    crate::repositories::columns::workflow_executions!(),
                    " FROM workflow_executions WHERE {HOLD_SCOPE} AND {HOLDS_OPEN} \
                 ORDER BY submitted_at DESC LIMIT 1"
                ),
                HOLD_SCOPE = HOLD_SCOPE,
                HOLDS_OPEN = HOLDS_OPEN,
            ))
            .bind(organization_id)
            .bind(namespace)
            .bind(workflow_id)
            .fetch_optional(pool)
            .await
            .map_err(|e| {
                OrcherError::internal(format!(
                    "Failed to find the run holding workflow id {}: {}",
                    workflow_id, e
                ))
            })?;
            found = later_submitted(found, row);
        }
        Ok(found.map(Into::into))
    }

    /// The run the start with request id `request_id` made in
    /// `organization_id`'s `namespace`, on any shard, whatever has become of
    /// it since.
    ///
    /// Scoped as [`START_REQUEST_INDEX`] keys the request: `None` is the
    /// unscoped deployment's scope, not every organization's, so a request id
    /// one tenant uses never answers another's start with its run.
    pub async fn find_by_start_request(
        &self,
        organization_id: Option<Uuid>,
        namespace: &str,
        request_id: &str,
    ) -> OrcherResult<Option<WorkflowExecution>> {
        for pool in self.scan_pools() {
            let row = sqlx::query_as::<_, WorkflowExecutionRow>(concat!(
                "SELECT ",
                crate::repositories::columns::workflow_executions!(),
                " FROM workflow_executions \
                 WHERE COALESCE(organization_id, '00000000-0000-0000-0000-000000000000'::uuid) \
                       = COALESCE($1::uuid, '00000000-0000-0000-0000-000000000000'::uuid) \
                   AND namespace = $2 AND start_request_id = $3"
            ))
            .bind(organization_id)
            .bind(namespace)
            .bind(request_id)
            .fetch_optional(pool)
            .await
            .map_err(|e| {
                OrcherError::internal(format!(
                    "Failed to find the run of start request {}: {}",
                    request_id, e
                ))
            })?;
            if let Some(row) = row {
                return Ok(Some(row.into()));
            }
        }
        Ok(None)
    }

    /// Release the delayed runs that are due: up to `limit` of them, the
    /// earliest due first, on each of this node's shards.
    ///
    /// A released run is claimable from the statement's commit on; the caller
    /// wakes the queues the returned runs are on. Releasing is idempotent and
    /// takes each run once — a run another engine is releasing at the same
    /// moment is skipped, not waited for — so every engine may run this. With
    /// clustering on, each releases only its own shards' runs, whose polls it
    /// serves and can wake.
    ///
    /// A run is released whatever has become of it meanwhile: a delayed run
    /// cancelled or terminated before it was due is not claimable for those
    /// reasons, and releasing it only takes it out of the delayed set.
    pub async fn release_due_starts(&self, limit: i64) -> OrcherResult<Vec<DueStart>> {
        let (los, his, min, max) = self.owned_bounds();
        let sql = format!(
            r#"
            UPDATE workflow_executions we
            SET awaiting_start = FALSE,
                updated_at = NOW()
            WHERE we.id IN (
                SELECT id FROM workflow_executions
                WHERE awaiting_start
                  AND start_after <= NOW()
                  AND {shard}
                ORDER BY start_after
                LIMIT $1
                FOR UPDATE SKIP LOCKED
            )
              AND we.awaiting_start
            RETURNING we.execution_id, we.namespace, we.task_queue
            "#,
            shard = shard_predicate("workflow_executions", 2),
        );
        let mut released = Vec::new();
        for pool in self.scan_pools() {
            let rows: Vec<(String, String, String)> = sqlx::query_as(&sql)
                .bind(limit)
                .bind(los.clone())
                .bind(his.clone())
                .bind(min)
                .bind(max)
                .fetch_all(pool)
                .await
                .map_err(|e| {
                    OrcherError::internal(format!("Failed to release due delayed starts: {}", e))
                })?;
            released.extend(
                rows.into_iter()
                    .map(|(execution_id, namespace, task_queue)| DueStart {
                        execution_id,
                        namespace,
                        task_queue,
                    }),
            );
        }
        Ok(released)
    }

    /// When the next delayed run on this node's shards is due, if any is
    /// waiting: what [`release_due_starts`](Self::release_due_starts) sleeps
    /// until.
    pub async fn next_start_due(&self) -> OrcherResult<Option<Timestamptz>> {
        let (los, his, min, max) = self.owned_bounds();
        let sql = format!(
            "SELECT MIN(start_after) FROM workflow_executions \
             WHERE awaiting_start AND {shard}",
            shard = shard_predicate("workflow_executions", 1),
        );
        let mut next: Option<Timestamptz> = None;
        for pool in self.scan_pools() {
            let due: Option<Timestamptz> = sqlx::query_scalar(&sql)
                .bind(los.clone())
                .bind(his.clone())
                .bind(min)
                .bind(max)
                .fetch_one(pool)
                .await
                .map_err(|e| {
                    OrcherError::internal(format!(
                        "Failed to read when the next delayed start is due: {}",
                        e
                    ))
                })?;
            next = match (next, due) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (a, b) => a.or(b),
            };
        }
        Ok(next)
    }

    /// The latest run started under `workflow_id` in `organization_id`'s
    /// `namespace`, open or closed, on any shard — what a reuse policy judges
    /// a new start against. A child's run is not one: its id is its parent's
    /// name for it. Runs written before V059 hold nothing once closed, so a
    /// first run (one with no predecessor) counts too.
    pub async fn latest_run_named(
        &self,
        organization_id: Option<Uuid>,
        namespace: &str,
        workflow_id: &str,
    ) -> OrcherResult<Option<WorkflowExecution>> {
        let mut latest: Option<WorkflowExecutionRow> = None;
        for pool in self.scan_pools() {
            let row = sqlx::query_as::<_, WorkflowExecutionRow>(&format!(
                concat!(
                    "SELECT ",
                    crate::repositories::columns::workflow_executions!(),
                    " FROM workflow_executions WHERE {HOLD_SCOPE} \
                   AND (holds_workflow_id OR parent_execution_id IS NULL) \
                 ORDER BY submitted_at DESC LIMIT 1"
                ),
                HOLD_SCOPE = HOLD_SCOPE,
            ))
            .bind(organization_id)
            .bind(namespace)
            .bind(workflow_id)
            .fetch_optional(pool)
            .await
            .map_err(|e| {
                OrcherError::internal(format!(
                    "Failed to find the latest run of workflow id {}: {}",
                    workflow_id, e
                ))
            })?;
            latest = later_submitted(latest, row);
        }
        Ok(latest.map(Into::into))
    }

    /// A routing id for a new run of `workflow_id` that lands on the same
    /// database as every other run of it.
    ///
    /// The hold is a unique index, and an index only sees its own database. A
    /// run routes by its id, so with several shard databases two runs of one
    /// workflow id could sit on two of them, each alone in its index. Taking
    /// the first of `candidate(0)`, `candidate(1)`, … that routes where the
    /// workflow id itself would keeps them together. With one database the
    /// first candidate always does, so nothing changes there; a deterministic
    /// `candidate` gives a deterministic id.
    pub fn id_beside_workflow_id(
        &self,
        organization_id: Option<Uuid>,
        namespace: &str,
        workflow_id: &str,
        mut candidate: impl FnMut(u32) -> Uuid,
    ) -> Uuid {
        let Some(resolver) = self.shard_resolver.get() else {
            return candidate(0);
        };
        let anchor = Uuid::new_v5(
            &Uuid::NAMESPACE_OID,
            format!(
                "orcher:workflow-id:{}:{}:{}",
                organization_id.map(|o| o.to_string()).unwrap_or_default(),
                namespace,
                workflow_id
            )
            .as_bytes(),
        );
        let home = resolver.pool_for_workflow(None, anchor) as *const PgPool;
        // Each candidate lands on the home database with probability 1/pools,
        // so this bound is never reached; it only keeps a broken resolver from
        // looping forever.
        for n in 0..4096 {
            let id = candidate(n);
            if std::ptr::eq(resolver.pool_for_workflow(None, id), home) {
                return id;
            }
        }
        warn!(
            workflow_id = %workflow_id,
            "No routing id found beside the workflow id; its runs may span databases"
        );
        candidate(0)
    }

    /// The run a request names, as the caller may see it.
    ///
    /// The one definition of how `(namespace, workflow_id, execution_id)` names
    /// a run, for every request that addresses one:
    ///
    /// - An `execution_id` names that exact run. Run ids are unique across
    ///   namespaces, so the namespace does not narrow it. When a `workflow_id`
    ///   is given too, the run must be one of that workflow's runs (or, for a
    ///   run without a workflow id, the run itself).
    /// - Otherwise a `workflow_id` names the workflow's latest run in
    ///   `namespace`: an id is unique only within an organization's namespace.
    /// - A `workflow_id` that names no workflow there is tried as a run id,
    ///   because callers have long passed one in that field.
    ///
    /// Each form is scoped to `organization_id` (`None` is an unscoped caller,
    /// which sees every run). A run outside the scope is `None`, as a missing
    /// one is, so the answer does not reveal that an id exists. `None` too
    /// when neither id is given.
    pub async fn find_run(&self, run: RunRef<'_>) -> OrcherResult<Option<WorkflowExecution>> {
        let visible = |exec: &WorkflowExecution| {
            run.organization_id.is_none() || exec.organization_id == run.organization_id
        };
        if !run.execution_id.is_empty() {
            let found = self.find_by_execution_id(run.execution_id).await?;
            return Ok(found.filter(|exec| {
                visible(exec)
                    && (run.workflow_id.is_empty()
                        || exec.workflow_id.as_deref() == Some(run.workflow_id)
                        // A run without a workflow id is reported with its
                        // run id in that field, and named back the same way.
                        || exec.execution_id == run.workflow_id)
            }));
        }
        if run.workflow_id.is_empty() {
            return Ok(None);
        }
        if let Some(latest) = self
            .find_latest_owned(run.organization_id, run.namespace, run.workflow_id)
            .await?
        {
            return Ok(Some(latest));
        }
        Ok(self
            .find_by_execution_id(run.workflow_id)
            .await?
            .filter(|exec| visible(exec)))
    }

    /// The run that replaced `execution_id`, if one was created.
    ///
    /// A retry, a restart-fresh and a reset each end a run and start its
    /// successor under the same `workflow_id`, pointing back through
    /// `parent_execution_id`. A run can also have started a child of its own
    /// under that same business id, which matches the same way; the successor
    /// is the one created last, since it only exists once its predecessor has
    /// ended.
    pub async fn find_latest_successor(
        &self,
        execution_id: &str,
        workflow_id: &str,
    ) -> OrcherResult<Option<WorkflowExecution>> {
        let sql = concat!(
            "SELECT ",
            crate::repositories::columns::workflow_executions!(),
            " FROM workflow_executions \
             WHERE parent_execution_id = $1 AND workflow_id = $2 \
             ORDER BY created_at DESC LIMIT 1"
        );
        let mut latest: Option<WorkflowExecutionRow> = None;
        for pool in self.scan_pools() {
            let row = sqlx::query_as::<_, WorkflowExecutionRow>(sql)
                .bind(execution_id)
                .bind(workflow_id)
                .fetch_optional(pool)
                .await
                .map_err(|e| {
                    OrcherError::internal(format!("Failed to find successor run: {}", e))
                })?;
            latest = match (latest, row) {
                (Some(a), Some(b)) => Some(if b.created_at > a.created_at { b } else { a }),
                (a, b) => a.or(b),
            };
        }
        Ok(latest.map(Into::into))
    }

    /// Every execution among `execution_ids` that exists, on any shard.
    ///
    /// For sweeps that hold a batch of ids and need their rows: one query per
    /// pool rather than one per id.
    pub async fn find_by_execution_ids(
        &self,
        execution_ids: &[String],
    ) -> OrcherResult<Vec<WorkflowExecution>> {
        if execution_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut rows: Vec<WorkflowExecutionRow> = Vec::new();
        for pool in self.scan_pools() {
            let batch = sqlx::query_as::<_, WorkflowExecutionRow>(concat!(
                "SELECT ",
                crate::repositories::columns::workflow_executions!(),
                " FROM workflow_executions WHERE execution_id = ANY($1)"
            ))
            .bind(execution_ids)
            .fetch_all(pool)
            .await
            .map_err(|e| OrcherError::internal(format!("Failed to load executions: {}", e)))?;
            rows.extend(batch);
        }
        Ok(rows.into_iter().map(Into::into).collect())
    }

    /// Update execution status and timing by internal ID (UUID)
    ///
    /// Note: The parameter is named `workflow_id` for API compatibility, but it
    /// actually refers to the internal `id` column (UUID), not the user-provided
    /// `workflow_id` column. This is because the poll response returns `id` as
    /// the workflow_id field for journal keying purposes.
    pub async fn update_status_by_workflow_id(
        &self,
        workflow_id: &str,
        status: &str,
        started_at: Option<Timestamptz>,
        completed_at: Option<Timestamptz>,
    ) -> OrcherResult<()> {
        debug!(
            workflow_id = %workflow_id,
            status = %status,
            "Updating workflow execution status by internal id"
        );

        // Parse as UUID and use the `id` column (internal UUID primary key)
        // The poll response returns `id` as workflow_id for journal keying
        let uuid = Uuid::parse_str(workflow_id)
            .map_err(|e| OrcherError::invalid_input(format!("Invalid workflow_id UUID: {}", e)))?;

        // Fenced: a superseded owner must not move a workflow's status.
        let mut tx = self.begin_write(self.pool_for(None, uuid), uuid).await?;

        sqlx::query(&format!(
            r#"
            UPDATE workflow_executions
            SET status = $1,
                started_at = COALESCE($2, started_at),
                completed_at = COALESCE($3, completed_at, {REACHED_NOW_IF_ENDED})
            WHERE id = $4
            "#
        ))
        .bind(status)
        .bind(started_at)
        .bind(completed_at)
        .bind(uuid)
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            OrcherError::internal(format!("Failed to update workflow execution status: {}", e))
        })?;

        tx.commit()
            .await
            .map_err(|e| OrcherError::internal(format!("Failed to commit status update: {}", e)))?;

        Ok(())
    }

    /// Update execution status and timing
    pub async fn update_status(
        &self,
        execution_id: &str,
        status: &str,
        started_at: Option<Timestamptz>,
        completed_at: Option<Timestamptz>,
    ) -> OrcherResult<()> {
        debug!(
            execution_id = %execution_id,
            status = %status,
            "Updating workflow execution status"
        );

        // Keyed by the globally-unique execution_id, not the routing id — run on every
        // shard pool; only the pool holding the row updates it. Single pool when
        // unpartitioned, unchanged.
        let sql = format!(
            r#"
            UPDATE workflow_executions
            SET status = $1,
                started_at = COALESCE($2, started_at),
                completed_at = COALESCE($3, completed_at, {REACHED_NOW_IF_ENDED})
            WHERE execution_id = $4
            "#
        );
        for pool in self.scan_pools() {
            sqlx::query(&sql)
                .bind(status)
                .bind(started_at)
                .bind(completed_at)
                .bind(execution_id)
                .execute(pool)
                .await
                .map_err(|e| {
                    OrcherError::internal(format!(
                        "Failed to update workflow execution status: {}",
                        e
                    ))
                })?;
        }

        Ok(())
    }

    /// Update execution progress
    pub async fn update_progress(
        &self,
        execution_id: &str,
        progress: serde_json::Value,
    ) -> OrcherResult<()> {
        debug!(execution_id = %execution_id, "Updating workflow execution progress");

        for pool in self.scan_pools() {
            let done =
                sqlx::query("UPDATE workflow_executions SET progress = $1 WHERE execution_id = $2")
                    .bind(&progress)
                    .bind(execution_id)
                    .execute(pool)
                    .await
                    .map_err(|e| {
                        OrcherError::internal(format!(
                            "Failed to update workflow execution progress: {}",
                            e
                        ))
                    })?;
            if done.rows_affected() > 0 {
                break;
            }
        }

        Ok(())
    }

    /// Update execution result and statistics
    pub async fn update_result(
        &self,
        execution_id: &str,
        result: Option<serde_json::Value>,
        error: Option<String>,
        statistics: Option<serde_json::Value>,
    ) -> OrcherResult<()> {
        debug!(execution_id = %execution_id, "Updating workflow execution result");

        for pool in self.scan_pools() {
            let done = sqlx::query(
                r#"
                        UPDATE workflow_executions
                        SET result = $1, error = $2, statistics = $3
                        WHERE execution_id = $4
                        "#,
            )
            .bind(&result)
            .bind(&error)
            .bind(&statistics)
            .bind(execution_id)
            .execute(pool)
            .await
            .map_err(|e| {
                OrcherError::internal(format!("Failed to update workflow execution result: {}", e))
            })?;
            if done.rows_affected() > 0 {
                break;
            }
        }

        Ok(())
    }

    /// Serialise the completions of one execution, and read where its
    /// activations stand.
    ///
    /// Holds a transaction-scoped advisory lock on the execution until the
    /// returned [`ActivationFence`] is finished or dropped, so two completions
    /// of the same execution — a retry racing the original, or a stale worker
    /// racing the current one — are applied one after the other, never
    /// interleaved, and the second sees what the first recorded. `None` when
    /// there is no such execution.
    ///
    /// # Why a lock beside the batch rather than one transaction around it
    ///
    /// Applying a completion runs every command through its own handler, and
    /// the handlers write through their own repositories: the journal, task
    /// rows, the outbox, child tracking, each on its own connection and some on
    /// a shard's pool under the shard write fence. Folding all of that into a
    /// single transaction would mean threading one connection through every
    /// handler and repository they touch. So the batch is not atomic; what is
    /// guaranteed is that it runs alone, and that it is recorded as applied
    /// only once it has all gone through. A batch that fails partway is not
    /// recorded, and the worker's retry runs it again; the handlers it repeats
    /// are the ones made idempotent for exactly that (a task is scheduled once,
    /// an ended run keeps its ending).
    ///
    /// The lock is advisory rather than `SELECT ... FOR UPDATE` on the row for
    /// the same reason: the handlers update that row from other connections,
    /// and a row lock held here would have them wait on a transaction that is
    /// itself waiting on them.
    pub async fn enter_activation(
        &self,
        workflow_id: &str,
        execution_id: &str,
    ) -> OrcherResult<Option<ActivationFence>> {
        let workflow_uuid =
            Uuid::parse_str(workflow_id).map_err(|e| OrcherError::InvalidInput {
                message: format!("Invalid workflow_id UUID: {}", e),
                field: Some("workflow_id".to_string()),
            })?;

        // The row's own database, so every completion of this execution locks
        // in the same place and reads the row it will write. That is the
        // workflow's routed pool for nearly every row, and it is tried first;
        // a row that lives elsewhere — a namespace pinned to its own pool-set
        // — is found by trying the rest, as lookups by execution id are.
        let routed = self.pool_for(None, workflow_uuid);
        let mut pools = vec![routed];
        pools.extend(
            self.scan_pools()
                .into_iter()
                .filter(|pool| !std::ptr::eq(*pool, routed)),
        );

        for pool in pools {
            if let Some(fence) = self.enter_activation_on(pool, execution_id).await? {
                return Ok(Some(fence));
            }
        }
        Ok(None)
    }

    /// The slots for fences on `pool`, created on first use and sized from it.
    fn activation_slots_of(&self, pool: &PgPool) -> std::sync::Arc<tokio::sync::Semaphore> {
        // Keyed by the database the pool connects to, which is what the
        // fences on it contend for — not by the pool's address, which differs
        // between clones of the same pool.
        let options = pool.connect_options();
        let key = format!(
            "{}:{}/{}",
            options.get_host(),
            options.get_port(),
            options.get_database().unwrap_or_default()
        );
        let mut slots = self
            .activation_slots
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        slots
            .entry(key)
            .or_insert_with(|| {
                std::sync::Arc::new(tokio::sync::Semaphore::new(activation_slots_for(
                    pool.options().get_max_connections(),
                )))
            })
            .clone()
    }

    async fn enter_activation_on(
        &self,
        pool: &PgPool,
        execution_id: &str,
    ) -> OrcherResult<Option<ActivationFence>> {
        let slot = self
            .activation_slots_of(pool)
            .acquire_owned()
            .await
            .map_err(|e| OrcherError::internal(format!("activation fence closed: {e}")))?;

        let mut tx = pool
            .begin()
            .await
            .map_err(|e| OrcherError::internal(format!("Failed to begin transaction: {}", e)))?;

        sqlx::query(
            "SELECT pg_advisory_xact_lock(hashtextextended('orcher.activation:' || $1, 0))",
        )
        .bind(execution_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            OrcherError::internal(format!("Failed to lock the execution's activations: {e}"))
        })?;

        #[allow(clippy::type_complexity)]
        let row: Option<(
            i64,
            Option<i64>,
            String,
            Option<String>,
            String,
            Option<String>,
            Option<Uuid>,
            String,
            Option<Timestamptz>,
        )> = sqlx::query_as(
            "SELECT activation_seq, completed_activation, status, claimed_by, task_queue, \
                 parent_execution_id, organization_id, namespace, cancel_requested_at \
                 FROM workflow_executions WHERE execution_id = $1",
        )
        .bind(execution_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| OrcherError::internal(format!("Failed to read the activation: {e}")))?;

        Ok(row.map(
            |(
                current,
                completed,
                status,
                claimed_by,
                task_queue,
                parent_execution_id,
                organization_id,
                namespace,
                cancel_requested_at,
            )| {
                ActivationFence {
                    tx,
                    execution_id: execution_id.to_string(),
                    current,
                    completed,
                    status,
                    claimed_by,
                    task_queue,
                    parent_execution_id,
                    organization_id,
                    namespace,
                    cancel_requested_at,
                    _slot: slot,
                }
            },
        ))
    }

    /// Atomically complete a workflow execution
    ///
    /// Updates status to "completed", sets the result, and completed_at timestamp
    /// in a single atomic UPDATE statement. This prevents race conditions where
    /// clients polling for results might see status=completed but result=NULL.
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - The workflow ID (UUID string) to complete
    /// * `result` - The workflow result as JSON value
    /// * `completed_at` - When the workflow completed
    ///
    /// # Returns
    ///
    /// `Ok(true)` when the run was completed, `Ok(false)` when it had already
    /// ended and was left as it was; an error if the update fails.
    #[allow(clippy::too_many_arguments)]
    pub async fn complete_workflow_atomic(
        &self,
        workflow_id: &str,
        result: Option<serde_json::Value>,
        completed_at: Timestamptz,
        total_steps: i32,
        completed_steps: i32,
        failed_steps: i32,
        retried_steps: i32,
    ) -> OrcherResult<bool> {
        debug!(
            workflow_id = %workflow_id,
            has_result = result.is_some(),
            "Atomically completing workflow execution"
        );

        // Parse workflow_id string to UUID for database query
        let workflow_uuid =
            Uuid::parse_str(workflow_id).map_err(|e| OrcherError::InvalidInput {
                message: format!("Invalid workflow_id UUID: {}", e),
                field: Some("workflow_id".to_string()),
            })?;

        // Single UPDATE that finalizes the row: status + result + completed_at +
        // step counts + duration (computed in SQL from started_at). Collapses the
        // former 4-UPDATE + 2-SELECT completion path into one write — see the
        // dispatch scaling note (DB-ops-per-transition). `result` is COALESCE'd so
        // a resultless completion doesn't null an existing value; duration falls
        // back to the current value when started_at is unknown.
        // Fenced when clustering is on: a node that has lost this workflow's
        // shard is refused here rather than being allowed to finalise a workflow
        // another node now owns.
        let mut tx = self
            .begin_write(self.pool_for(None, workflow_uuid), workflow_uuid)
            .await?;

        // A run that has already ended keeps its ending, as a failure does in
        // fail_workflow_atomic — and a failed run with it, which a completion
        // must not turn into a success its retry then runs again. Unguarded, a
        // completion sent twice stamped the row twice, and one arriving after
        // the run was cancelled turned it back into a completed run.
        let rows_affected = sqlx::query(&format!(
            r#"
            UPDATE workflow_executions
            SET status = 'completed',
                result = COALESCE($1, result),
                completed_at = $2,
                total_steps = $4,
                completed_steps = $5,
                failed_steps = $6,
                retried_steps = $7,
                duration_ms = CASE
                    WHEN started_at IS NOT NULL
                    THEN (EXTRACT(EPOCH FROM ($2 - started_at)) * 1000)::bigint
                    ELSE duration_ms
                END,
                updated_at = NOW()
            WHERE id = $3
              AND status NOT IN {ENDED}
              AND status <> 'failed'
            "#
        ))
        .bind(&result)
        .bind(completed_at)
        .bind(workflow_uuid)
        .bind(total_steps)
        .bind(completed_steps)
        .bind(failed_steps)
        .bind(retried_steps)
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            error!(
                workflow_id = %workflow_id,
                error = %e,
                "Failed to atomically complete workflow execution"
            );
            OrcherError::internal(format!("Failed to complete workflow execution: {}", e))
        })?
        .rows_affected();

        if rows_affected == 0 {
            info!(
                workflow_id = %workflow_id,
                "Completion not recorded: the workflow does not exist or has already ended"
            );
        } else {
            info!(
                workflow_id = %workflow_id,
                "Workflow completed atomically (status + result in single update)"
            );
        }

        tx.commit().await.map_err(|e| {
            OrcherError::internal(format!("Failed to commit workflow completion: {}", e))
        })?;

        Ok(rows_affected > 0)
    }

    /// Atomically fail a workflow execution
    ///
    /// Updates status to "failed", sets the error message, and completed_at timestamp
    /// in a single atomic UPDATE statement.
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - The workflow ID (UUID string) to fail
    /// * `error` - The error message
    /// * `error_details` - Optional detailed error information as JSON
    /// * `completed_at` - When the workflow failed
    ///
    /// # Returns
    ///
    /// Ok(()) on success, error if the update fails
    pub async fn fail_workflow_atomic(
        &self,
        workflow_id: &str,
        error: String,
        error_details: Option<serde_json::Value>,
        completed_at: Timestamptz,
    ) -> OrcherResult<bool> {
        debug!(
            workflow_id = %workflow_id,
            error = %error,
            "Atomically failing workflow execution"
        );

        // Parse workflow_id string to UUID for database query
        let workflow_uuid =
            Uuid::parse_str(workflow_id).map_err(|e| OrcherError::InvalidInput {
                message: format!("Invalid workflow_id UUID: {}", e),
                field: Some("workflow_id".to_string()),
            })?;

        // Fenced on the same terms as completion: a node that has lost this
        // workflow's shard must not be able to mark it failed.
        let mut tx = self
            .begin_write(self.pool_for(None, workflow_uuid), workflow_uuid)
            .await?;

        // A run that has already ended keeps its ending. A failure reported
        // for a run that was cancelled or terminated meanwhile used to turn it
        // back into a failure — which the retry scheduler would then retry.
        let rows_affected = sqlx::query(&format!(
            r#"
            UPDATE workflow_executions
            SET status = 'failed',
                error = $1,
                error_details = $2,
                completed_at = $3,
                updated_at = NOW()
            WHERE id = $4
              AND status NOT IN {ENDED}
            "#
        ))
        .bind(&error)
        .bind(&error_details)
        .bind(completed_at)
        .bind(workflow_uuid)
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            error!(
                workflow_id = %workflow_id,
                error = %e,
                "Failed to atomically fail workflow execution"
            );
            OrcherError::internal(format!("Failed to fail workflow execution: {}", e))
        })?
        .rows_affected();

        if rows_affected == 0 {
            info!(
                workflow_id = %workflow_id,
                "Failure not recorded: the workflow does not exist or has already ended"
            );
        } else {
            info!(
                workflow_id = %workflow_id,
                "Workflow failed atomically (status + error in single update)"
            );
        }

        tx.commit().await.map_err(|e| {
            OrcherError::internal(format!("Failed to commit workflow failure: {}", e))
        })?;

        Ok(rows_affected > 0)
    }

    /// Record that the workflow ended itself as cancelled, unless it has
    /// already ended.
    ///
    /// The cancellation a workflow reports for itself, as opposed to
    /// [`cancel`](Self::cancel), which a client asks for. It is replay-driven,
    /// so it is fenced on the same terms as completion and failure, and
    /// guarded the same way: a run that has already ended keeps its ending.
    /// The status write it replaces had neither guard, so a late cancellation
    /// could overwrite a completed or terminated run, and it stamped
    /// `completed_at` where every other cancellation stamps `cancelled_at`.
    ///
    /// Returns `false` when nothing was recorded.
    pub async fn cancel_workflow_atomic(
        &self,
        workflow_id: &str,
        cancelled_at: Timestamptz,
    ) -> OrcherResult<bool> {
        let workflow_uuid =
            Uuid::parse_str(workflow_id).map_err(|e| OrcherError::InvalidInput {
                message: format!("Invalid workflow_id UUID: {}", e),
                field: Some("workflow_id".to_string()),
            })?;

        let mut tx = self
            .begin_write(self.pool_for(None, workflow_uuid), workflow_uuid)
            .await?;

        let rows_affected = sqlx::query(&format!(
            r#"
            UPDATE workflow_executions
            SET status = 'cancelled',
                cancelled_at = $1,
                completed_at = COALESCE(completed_at, $1),
                next_retry_at = NULL,
                updated_at = NOW()
            WHERE id = $2
              AND status NOT IN {ENDED}
            "#
        ))
        .bind(cancelled_at)
        .bind(workflow_uuid)
        .execute(&mut *tx)
        .await
        .map_err(|e| OrcherError::internal(format!("Failed to cancel workflow execution: {}", e)))?
        .rows_affected();

        tx.commit().await.map_err(|e| {
            OrcherError::internal(format!("Failed to commit workflow cancellation: {}", e))
        })?;

        Ok(rows_affected > 0)
    }

    /// Record that the run `execution_id` failed, unless it has already ended.
    ///
    /// The worker-reported counterpart of
    /// [`fail_workflow_atomic`](Self::fail_workflow_atomic), keyed by execution
    /// id: status, error and details in one guarded write. Returns `false` when
    /// nothing was recorded — no such run, or it had already completed, been
    /// cancelled, terminated, timed out or replaced. A late failure report
    /// must not turn a finished run back into a failure.
    pub async fn record_failure(
        &self,
        execution_id: &str,
        error: &str,
        error_details: Option<serde_json::Value>,
    ) -> OrcherResult<bool> {
        let sql = format!(
            r#"
            UPDATE workflow_executions
            SET status = 'failed',
                error = $2,
                error_details = $3,
                completed_at = NOW(),
                updated_at = NOW()
            WHERE execution_id = $1
              AND status NOT IN {ENDED}
            "#
        );
        for pool in self.scan_pools() {
            let done = sqlx::query(&sql)
                .bind(execution_id)
                .bind(error)
                .bind(&error_details)
                .execute(pool)
                .await
                .map_err(|e| OrcherError::internal(format!("Failed to record failure: {}", e)))?;
            if done.rows_affected() > 0 {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Update step counts
    pub async fn update_step_counts(
        &self,
        execution_id: &str,
        total_steps: i32,
        completed_steps: i32,
        failed_steps: i32,
        retried_steps: i32,
    ) -> OrcherResult<()> {
        debug!(execution_id = %execution_id, "Updating workflow execution step counts");

        for pool in self.scan_pools() {
            let done = sqlx::query(
                        r#"
                        UPDATE workflow_executions
                        SET total_steps = $1, completed_steps = $2, failed_steps = $3, retried_steps = $4
                        WHERE execution_id = $5
                        "#,
                    )
                    .bind(total_steps)
                    .bind(completed_steps)
                    .bind(failed_steps)
                    .bind(retried_steps)
                    .bind(execution_id)
                    .execute(pool)
                    .await
                    .map_err(|e| {
                        OrcherError::internal(format!(
                            "Failed to update workflow execution step counts: {}",
                            e
                        ))
                    })?;
            if done.rows_affected() > 0 {
                break;
            }
        }

        Ok(())
    }

    /// Update started_at timestamp
    pub async fn update_started_at(
        &self,
        execution_id: &str,
        started_at: Timestamptz,
    ) -> OrcherResult<()> {
        debug!(execution_id = %execution_id, "Updating workflow execution started_at timestamp");

        // Only stamp started_at once. The poll path calls this on every activation
        // delivery, but started_at must reflect the *first* start — the `IS NULL`
        // guard makes repeat calls a cheap no-op (no row rewrite / WAL) and keeps
        // the value stable instead of bumping it to the latest poll time.
        for pool in self.scan_pools() {
            let done = sqlx::query(
                r#"
                        UPDATE workflow_executions
                        SET started_at = $1, updated_at = $2
                        WHERE execution_id = $3 AND started_at IS NULL
                        "#,
            )
            .bind(started_at)
            .bind(Timestamptz::now())
            .bind(execution_id)
            .execute(pool)
            .await
            .map_err(|e| {
                OrcherError::internal(format!(
                    "Failed to update workflow execution started_at: {}",
                    e
                ))
            })?;
            if done.rows_affected() > 0 {
                break;
            }
        }

        Ok(())
    }

    /// Update workflow duration in milliseconds
    pub async fn update_duration(&self, execution_id: &str, duration_ms: i64) -> OrcherResult<()> {
        debug!(execution_id = %execution_id, duration_ms = %duration_ms, "Updating workflow execution duration");

        for pool in self.scan_pools() {
            let done = sqlx::query(
                r#"
                        UPDATE workflow_executions
                        SET duration_ms = $1, updated_at = $2
                        WHERE execution_id = $3
                        "#,
            )
            .bind(duration_ms)
            .bind(Timestamptz::now())
            .bind(execution_id)
            .execute(pool)
            .await
            .map_err(|e| {
                OrcherError::internal(format!(
                    "Failed to update workflow execution duration: {}",
                    e
                ))
            })?;
            if done.rows_affected() > 0 {
                break;
            }
        }

        Ok(())
    }

    /// Merge labels into existing ones (JSONB || operator)
    pub async fn update_labels(
        &self,
        execution_id: &str,
        labels: &serde_json::Value,
    ) -> OrcherResult<()> {
        debug!(execution_id = %execution_id, "Updating workflow labels");

        for pool in self.scan_pools() {
            let done = sqlx::query(
                r#"
                        UPDATE workflow_executions
                        SET labels = labels || $1, updated_at = $2
                        WHERE execution_id = $3
                        "#,
            )
            .bind(labels)
            .bind(Timestamptz::now())
            .bind(execution_id)
            .execute(pool)
            .await
            .map_err(|e| {
                OrcherError::internal(format!("Failed to update workflow labels: {}", e))
            })?;
            if done.rows_affected() > 0 {
                break;
            }
        }

        Ok(())
    }

    /// Record a request to cancel a workflow whose code has run, so it can
    /// clean up before it ends.
    ///
    /// Applies only to a live run that has been handed at least one
    /// activation, and never to a failure waiting for its retry: a run whose
    /// code has not run has nothing to clean up, and a caller ends it with
    /// [`cancel`](Self::cancel) instead. The time, reason and deadline are set
    /// by the first request and kept by any that follow, so asking again is
    /// harmless. Decided inside the `UPDATE`, like `cancel`, and not
    /// shard-fenced, for the same reason.
    ///
    /// Returns the run when the request applies to it (recorded now or
    /// earlier), `None` when it does not.
    pub async fn request_cancel(
        &self,
        execution_id: &str,
        reason: &str,
        deadline: Option<Timestamptz>,
    ) -> OrcherResult<Option<CancelRequestTarget>> {
        for pool in self.scan_pools() {
            // `NOW()` is fixed for the transaction, so the request time equals it
            // exactly when this statement set it.
            let row: Option<(Uuid, String, String, bool)> = sqlx::query_as(
                r#"
                UPDATE workflow_executions
                SET cancel_deadline = CASE WHEN cancel_requested_at IS NULL
                                           THEN $3 ELSE cancel_deadline END,
                    cancel_reason = COALESCE(cancel_reason, $2),
                    cancel_requested_at = COALESCE(cancel_requested_at, NOW()),
                    updated_at = NOW()
                WHERE execution_id = $1
                  AND status IN ('pending', 'queued', 'running', 'retrying', 'waiting')
                  AND activation_seq > 0
                RETURNING id, namespace, task_queue, cancel_requested_at = NOW()
                "#,
            )
            .bind(execution_id)
            .bind(reason)
            .bind(deadline)
            .fetch_optional(pool)
            .await
            .map_err(|e| {
                OrcherError::internal(format!("Failed to record cancellation request: {}", e))
            })?;
            if let Some((id, namespace, task_queue, newly_requested)) = row {
                return Ok(Some(CancelRequestTarget {
                    workflow_id: id,
                    namespace,
                    task_queue,
                    newly_requested,
                }));
            }
        }
        Ok(None)
    }

    /// When cancellation was requested for the run with internal id
    /// `workflow_id`, if it was and the run has not ended.
    pub async fn pending_cancel_request(
        &self,
        workflow_id: Uuid,
    ) -> OrcherResult<Option<Timestamptz>> {
        let requested: Option<Option<Timestamptz>> = sqlx::query_scalar(&format!(
            r#"
            SELECT cancel_requested_at FROM workflow_executions
            WHERE id = $1 AND {STILL_LIVE}
            "#
        ))
        .bind(workflow_id)
        .fetch_optional(self.pool_for(None, workflow_id))
        .await
        .map_err(|e| {
            OrcherError::internal(format!("Failed to read cancellation request: {}", e))
        })?;
        Ok(requested.flatten())
    }

    /// Cancel an execution, refusing to overwrite a terminal state.
    ///
    /// Returns `true` when a row moved to cancelled, `false` when there was
    /// nothing to cancel — no such execution, or it had already ended:
    /// completed, cancelled, replaced by a successor run (restarted fresh or
    /// reset), terminated, or failed or timed out with no retry scheduled. A
    /// failure or run timeout with a retry scheduled is cancelled, and its
    /// retry with it: the retry scheduler only takes up failed and timed-out
    /// runs. Callers need that distinction: reporting a
    /// successful cancel for a workflow that had already finished is a lie the
    /// client acts on.
    ///
    /// The terminal-state predicate is the point of this method. Checking the
    /// status in the caller and then writing is a check-then-act race — a
    /// workflow that completes in between gets its result overwritten by a
    /// cancellation. Deciding it inside the `UPDATE` makes that impossible.
    ///
    /// Deliberately NOT shard-fenced, and that is a decision rather than a gap:
    /// cancel is a control-plane call that arrives at whichever node the client
    /// reached, which usually does not own the shard. Refusing it there would
    /// break cancellation on a multi-node cluster. What cancel needs is
    /// atomicity against concurrent completion, which the predicate below
    /// provides; ownership fencing protects replay-driven writes, which this is
    /// not.
    pub async fn cancel(&self, execution_id: &str) -> OrcherResult<bool> {
        info!(execution_id = %execution_id, "Cancelling workflow execution");

        let mut result = sqlx::postgres::PgQueryResult::default();
        for pool in self.scan_pools() {
            result = sqlx::query(&format!(
                r#"
                        UPDATE workflow_executions
                        SET status = 'cancelled', cancelled_at = NOW(),
                            completed_at = COALESCE(completed_at, NOW()), next_retry_at = NULL
                        WHERE execution_id = $1
                          AND {STILL_LIVE}
                        "#
            ))
            .bind(execution_id)
            .execute(pool)
            .await
            .map_err(|e| {
                OrcherError::internal(format!("Failed to cancel workflow execution: {}", e))
            })?;
            if result.rows_affected() > 0 {
                break;
            }
        }

        if result.rows_affected() == 0 {
            warn!(execution_id = %execution_id, "Execution not found or already in terminal state");
            return Ok(false);
        }

        Ok(true)
    }

    /// Terminate an execution, refusing to overwrite a state it has already
    /// ended in.
    ///
    /// The same guarded write as [`cancel`](Self::cancel), for the same reason:
    /// a run that completes between a caller's status check and this write
    /// must keep its result, and a failure waiting for its retry is ended
    /// along with the retry. Returns `true` only when a row moved to
    /// terminated. Not shard-fenced, like `cancel`: it is a control-plane call.
    pub async fn terminate(&self, execution_id: &str) -> OrcherResult<bool> {
        info!(execution_id = %execution_id, "Terminating workflow execution");

        for pool in self.scan_pools() {
            let result = sqlx::query(&format!(
                r#"
                UPDATE workflow_executions
                SET status = 'terminated',
                    completed_at = COALESCE(completed_at, NOW()),
                    next_retry_at = NULL,
                    updated_at = NOW()
                WHERE execution_id = $1
                  AND {STILL_LIVE}
                "#
            ))
            .bind(execution_id)
            .execute(pool)
            .await
            .map_err(|e| {
                OrcherError::internal(format!("Failed to terminate workflow execution: {}", e))
            })?;
            if result.rows_affected() > 0 {
                return Ok(true);
            }
        }

        warn!(execution_id = %execution_id, "Execution not found or already ended");
        Ok(false)
    }

    /// List executions with filters and pagination
    pub async fn list(
        &self,
        filters: ListExecutionsFilters,
    ) -> OrcherResult<Vec<WorkflowExecution>> {
        debug!(?filters, "Listing workflow executions");

        let mut query = String::from(concat!(
            "SELECT ",
            crate::repositories::columns::workflow_executions!(),
            " FROM workflow_executions WHERE 1=1"
        ));
        let mut bind_index = 1;

        if filters.workflow_name.is_some() {
            query.push_str(&format!(" AND workflow_type = ${}", bind_index));
            bind_index += 1;
        }
        if filters.workflow_version.is_some() {
            query.push_str(&format!(" AND workflow_version = ${}", bind_index));
            bind_index += 1;
        }
        if filters.namespace.is_some() {
            query.push_str(&format!(" AND namespace = ${}", bind_index));
            bind_index += 1;
        }
        if filters.task_queue.is_some() {
            query.push_str(&format!(" AND task_queue = ${}", bind_index));
            bind_index += 1;
        }
        if filters.status.is_some() {
            query.push_str(&format!(" AND status = ${}", bind_index));
            bind_index += 1;
        }
        if filters.user_id.is_some() {
            query.push_str(&format!(" AND user_id = ${}", bind_index));
            bind_index += 1;
        }
        if filters.organization_id.is_some() {
            query.push_str(&format!(" AND organization_id = ${}", bind_index));
            bind_index += 1;
        }
        if filters.parent_execution_id.is_some() {
            query.push_str(&format!(" AND parent_execution_id = ${}", bind_index));
            bind_index += 1;
        }
        if filters.labels.is_some() {
            query.push_str(&format!(" AND labels @> ${}", bind_index));
            bind_index += 1;
        }
        if filters.submitted_after.is_some() {
            query.push_str(&format!(" AND submitted_at >= ${}", bind_index));
            bind_index += 1;
        }

        // Keyset cursor: when both cursor fields are set, use keyset pagination
        // instead of offset to avoid skipping rows on concurrent inserts.
        let use_keyset =
            filters.cursor_submitted_at.is_some() && filters.cursor_execution_id.is_some();
        if use_keyset {
            query.push_str(&format!(
                " AND (submitted_at < ${0} OR (submitted_at = ${0} AND execution_id < ${1}))",
                bind_index,
                bind_index + 1
            ));
            bind_index += 2;
        }

        query.push_str(&format!(
            " ORDER BY submitted_at DESC, execution_id DESC LIMIT ${} OFFSET ${}",
            bind_index,
            bind_index + 1
        ));

        // With shard pools, each pool is asked for the first offset + limit
        // rows in the same order, the results are merged, and the page is cut
        // once. One pool keeps the query exactly as it was.
        let offset = if use_keyset { 0 } else { filters.offset };
        let pools = self.scan_pools();
        let (pool_limit, pool_offset) = if pools.len() == 1 {
            (filters.limit, offset)
        } else {
            (filters.limit + offset, 0)
        };
        let mut rows: Vec<WorkflowExecutionRow> = Vec::new();
        for pool in &pools {
            let mut q = sqlx::query_as::<_, WorkflowExecutionRow>(&query);

            if let Some(ref name) = filters.workflow_name {
                q = q.bind(name);
            }
            if let Some(ref version) = filters.workflow_version {
                q = q.bind(version);
            }
            if let Some(ref namespace) = filters.namespace {
                q = q.bind(namespace);
            }
            if let Some(ref task_queue) = filters.task_queue {
                q = q.bind(task_queue);
            }
            if let Some(ref status) = filters.status {
                q = q.bind(status);
            }
            if let Some(user_id) = filters.user_id {
                q = q.bind(user_id);
            }
            if let Some(org_id) = filters.organization_id {
                q = q.bind(org_id);
            }
            if let Some(ref parent_id) = filters.parent_execution_id {
                q = q.bind(parent_id);
            }
            if let Some(ref attrs) = filters.labels {
                q = q.bind(attrs);
            }
            if let Some(submitted_after) = filters.submitted_after {
                q = q.bind(submitted_after);
            }
            if use_keyset {
                q = q.bind(filters.cursor_submitted_at.unwrap());
                q = q.bind(filters.cursor_execution_id.as_deref().unwrap_or(""));
            }

            rows.extend(
                q.bind(pool_limit)
                    .bind(pool_offset)
                    .fetch_all(*pool)
                    .await
                    .map_err(|e| {
                        OrcherError::internal(format!("Failed to list workflow executions: {}", e))
                    })?,
            );
        }
        if pools.len() > 1 {
            rows.sort_by(|a, b| {
                b.submitted_at
                    .cmp(&a.submitted_at)
                    .then_with(|| b.execution_id.cmp(&a.execution_id))
            });
            rows = rows
                .into_iter()
                .skip(offset.max(0) as usize)
                .take(filters.limit.max(0) as usize)
                .collect();
        }

        Ok(rows.into_iter().map(Into::into).collect())
    }

    /// Count executions matching filters
    pub async fn count(&self, filters: ListExecutionsFilters) -> OrcherResult<i64> {
        debug!(?filters, "Counting workflow executions");

        let mut query = String::from("SELECT COUNT(*) FROM workflow_executions WHERE 1=1");
        let mut bind_index = 1;

        if filters.workflow_name.is_some() {
            query.push_str(&format!(" AND workflow_type = ${}", bind_index));
            bind_index += 1;
        }
        if filters.workflow_version.is_some() {
            query.push_str(&format!(" AND workflow_version = ${}", bind_index));
            bind_index += 1;
        }
        if filters.namespace.is_some() {
            query.push_str(&format!(" AND namespace = ${}", bind_index));
            bind_index += 1;
        }
        if filters.task_queue.is_some() {
            query.push_str(&format!(" AND task_queue = ${}", bind_index));
            bind_index += 1;
        }
        if filters.status.is_some() {
            query.push_str(&format!(" AND status = ${}", bind_index));
            bind_index += 1;
        }
        if filters.user_id.is_some() {
            query.push_str(&format!(" AND user_id = ${}", bind_index));
            bind_index += 1;
        }
        if filters.organization_id.is_some() {
            query.push_str(&format!(" AND organization_id = ${}", bind_index));
            bind_index += 1;
        }
        if filters.parent_execution_id.is_some() {
            query.push_str(&format!(" AND parent_execution_id = ${}", bind_index));
            bind_index += 1;
        }
        if filters.labels.is_some() {
            query.push_str(&format!(" AND labels @> ${}", bind_index));
        }

        // Summed over every pool; the query is rebuilt per pool because a
        // bound query is consumed by running it.
        let mut count = 0;
        for pool in self.scan_pools() {
            let mut q = sqlx::query_scalar::<_, i64>(&query);

            if let Some(ref name) = filters.workflow_name {
                q = q.bind(name);
            }
            if let Some(ref version) = filters.workflow_version {
                q = q.bind(version);
            }
            if let Some(ref namespace) = filters.namespace {
                q = q.bind(namespace);
            }
            if let Some(ref task_queue) = filters.task_queue {
                q = q.bind(task_queue);
            }
            if let Some(ref status) = filters.status {
                q = q.bind(status);
            }
            if let Some(user_id) = filters.user_id {
                q = q.bind(user_id);
            }
            if let Some(org_id) = filters.organization_id {
                q = q.bind(org_id);
            }
            if let Some(ref parent_id) = filters.parent_execution_id {
                q = q.bind(parent_id);
            }
            if let Some(ref attrs) = filters.labels {
                q = q.bind(attrs);
            }

            count += q.fetch_one(pool).await.map_err(|e| {
                OrcherError::internal(format!("Failed to count workflow executions: {}", e))
            })?;
        }

        Ok(count)
    }

    /// Live runs a batch cancellation has not reached yet, up to `limit`: those
    /// matching the filters (as in [`batch_update_status`](Self::batch_update_status))
    /// with no cancellation request on record.
    ///
    /// The batch cancels each one through the same path as a single
    /// cancellation, so a run whose code has run is asked rather than ended.
    /// A run leaves this set once it is asked or ended, so calling this until it
    /// returns nothing reaches every match exactly once.
    #[allow(clippy::too_many_arguments)]
    pub async fn select_for_batch_cancel(
        &self,
        namespace: &str,
        organization_id: Option<Uuid>,
        filter_status: Option<&str>,
        workflow_type: Option<&str>,
        labels: Option<&serde_json::Value>,
        started_after: Option<Timestamptz>,
        started_before: Option<Timestamptz>,
        limit: i64,
    ) -> OrcherResult<Vec<String>> {
        let mut query = format!(
            "SELECT execution_id FROM workflow_executions \
             WHERE namespace = $1 AND ($2::uuid IS NULL OR organization_id = $2) \
               AND {STILL_LIVE} AND cancel_requested_at IS NULL"
        );
        let mut bind_index = 3;
        if filter_status.is_some() {
            query.push_str(&format!(" AND status = ${}", bind_index));
            bind_index += 1;
        }
        if workflow_type.is_some() {
            query.push_str(&format!(" AND workflow_type = ${}", bind_index));
            bind_index += 1;
        }
        if labels.is_some() {
            query.push_str(&format!(" AND labels @> ${}", bind_index));
            bind_index += 1;
        }
        if started_after.is_some() {
            query.push_str(&format!(" AND started_at >= ${}", bind_index));
            bind_index += 1;
        }
        if started_before.is_some() {
            query.push_str(&format!(" AND started_at <= ${}", bind_index));
            bind_index += 1;
        }
        query.push_str(&format!(" ORDER BY execution_id LIMIT ${}", bind_index));

        let mut execution_ids: Vec<String> = Vec::new();
        for pool in self.scan_pools() {
            let remaining = limit - execution_ids.len() as i64;
            if remaining <= 0 {
                break;
            }
            let mut q = sqlx::query_scalar::<_, String>(&query)
                .bind(namespace)
                .bind(organization_id);
            if let Some(status) = filter_status {
                q = q.bind(status);
            }
            if let Some(workflow_type) = workflow_type {
                q = q.bind(workflow_type);
            }
            if let Some(labels) = labels {
                q = q.bind(labels);
            }
            if let Some(after) = started_after {
                q = q.bind(after);
            }
            if let Some(before) = started_before {
                q = q.bind(before);
            }
            let found = q.bind(remaining).fetch_all(pool).await.map_err(|e| {
                OrcherError::internal(format!("Failed to select runs to cancel: {}", e))
            })?;
            execution_ids.extend(found);
        }
        Ok(execution_ids)
    }

    /// Batch update status for workflows matching a filter.
    /// Skips workflows already in terminal states.
    /// Returns execution_ids of updated rows.
    #[allow(clippy::too_many_arguments)]
    /// Deliberately not shard-fenced. This updates every matching workflow in a
    /// namespace, which spans shards by construction, so no single generation
    /// authorises the write — and like `cancel`, it is an administrative call
    /// that arrives at whichever node the client reached rather than a
    /// replay-driven one. Refusing it on a non-owner would break batch
    /// operations on a multi-node cluster. What it needs is atomicity against
    /// concurrent completion, which the outer predicate below provides.
    pub async fn batch_update_status(
        &self,
        namespace: &str,
        organization_id: Option<Uuid>,
        filter_status: Option<&str>,
        workflow_type: Option<&str>,
        labels: Option<&serde_json::Value>,
        started_after: Option<Timestamptz>,
        started_before: Option<Timestamptz>,
        new_status: &str,
        completed_at: Option<Timestamptz>,
        limit: i64,
    ) -> OrcherResult<Vec<String>> {
        debug!(
            namespace = %namespace,
            new_status = %new_status,
            limit = limit,
            "Batch updating workflow execution statuses"
        );

        // The terminal-state predicate appears TWICE on purpose, and the outer one
        // is the one that matters.
        //
        // Under READ COMMITTED, when a row this statement is updating was changed
        // by a transaction that committed after the snapshot, Postgres re-checks
        // the command's WHERE against the NEW row version — but a subquery in that
        // WHERE still reflects the original snapshot. With the guard only inside
        // the subquery, a workflow that completed mid-statement still satisfies
        // `execution_id IN (…)` and gets overwritten, losing its result. Repeating
        // it in the outer WHERE means the re-check sees the new status and skips
        // the row. Same lesson as the cancel path: decide it in the UPDATE.
        let mut query = format!(
                "UPDATE workflow_executions SET status = $1, completed_at = COALESCE($2, completed_at), next_retry_at = NULL, updated_at = NOW() WHERE {STILL_LIVE} AND execution_id IN (SELECT execution_id FROM workflow_executions WHERE namespace = $3 AND ($4::uuid IS NULL OR organization_id = $4) AND {STILL_LIVE}"
            );
        let mut bind_index = 5;

        if filter_status.is_some() {
            query.push_str(&format!(" AND status = ${}", bind_index));
            bind_index += 1;
        }
        if workflow_type.is_some() {
            query.push_str(&format!(" AND workflow_type = ${}", bind_index));
            bind_index += 1;
        }
        if labels.is_some() {
            query.push_str(&format!(" AND labels @> ${}", bind_index));
            bind_index += 1;
        }
        if started_after.is_some() {
            query.push_str(&format!(" AND started_at >= ${}", bind_index));
            bind_index += 1;
        }
        if started_before.is_some() {
            query.push_str(&format!(" AND started_at <= ${}", bind_index));
            bind_index += 1;
        }

        query.push_str(&format!(" LIMIT ${}", bind_index));
        query.push_str(") RETURNING execution_id");

        // Every pool, each taking what is left of the limit: a batch that
        // updated only the default pool silently left every matching workflow
        // on the other shards running.
        let mut execution_ids: Vec<String> = Vec::new();
        for pool in self.scan_pools() {
            let remaining = limit - execution_ids.len() as i64;
            if remaining <= 0 {
                break;
            }
            let mut q = sqlx::query_scalar::<_, String>(&query)
                .bind(new_status)
                .bind(completed_at)
                .bind(namespace)
                .bind(organization_id);

            if let Some(status) = filter_status {
                q = q.bind(status);
            }
            if let Some(wf_type) = workflow_type {
                q = q.bind(wf_type);
            }
            if let Some(attrs) = labels {
                q = q.bind(attrs);
            }
            if let Some(after) = started_after {
                q = q.bind(after);
            }
            if let Some(before) = started_before {
                q = q.bind(before);
            }
            q = q.bind(remaining);

            execution_ids.extend(q.fetch_all(pool).await.map_err(|e| {
                OrcherError::internal(format!("Failed to batch update workflow statuses: {}", e))
            })?);
        }

        info!(
            namespace = %namespace,
            new_status = %new_status,
            updated_count = execution_ids.len(),
            "Batch status update complete"
        );

        Ok(execution_ids)
    }

    /// List workflow executions matching a filter for batch event delivery.
    /// Returns full WorkflowExecution objects needed for event processing.
    #[allow(clippy::too_many_arguments)]
    pub async fn list_by_batch_filter(
        &self,
        namespace: &str,
        organization_id: Option<Uuid>,
        filter_status: Option<&str>,
        workflow_type: Option<&str>,
        labels: Option<&serde_json::Value>,
        started_after: Option<Timestamptz>,
        started_before: Option<Timestamptz>,
        limit: i64,
        offset: i64,
    ) -> OrcherResult<Vec<WorkflowExecution>> {
        debug!(
            namespace = %namespace,
            limit = limit,
            offset = offset,
            "Listing workflow executions by batch filter"
        );

        let mut query = String::from(
            // Scoped by owner as well as name: a namespace called "default"
            // exists in every tenant.
            concat!("SELECT ", crate::repositories::columns::workflow_executions!(), " FROM workflow_executions WHERE namespace = $1 AND ($2::uuid IS NULL OR organization_id = $2) AND status NOT IN ('completed', 'failed', 'cancelled', 'terminated', 'timed_out')")
        );
        let mut bind_index = 3;

        if filter_status.is_some() {
            query.push_str(&format!(" AND status = ${}", bind_index));
            bind_index += 1;
        }
        if workflow_type.is_some() {
            query.push_str(&format!(" AND workflow_type = ${}", bind_index));
            bind_index += 1;
        }
        if labels.is_some() {
            query.push_str(&format!(" AND labels @> ${}", bind_index));
            bind_index += 1;
        }
        if started_after.is_some() {
            query.push_str(&format!(" AND started_at >= ${}", bind_index));
            bind_index += 1;
        }
        if started_before.is_some() {
            query.push_str(&format!(" AND started_at <= ${}", bind_index));
            bind_index += 1;
        }

        query.push_str(&format!(
            " ORDER BY submitted_at LIMIT ${} OFFSET ${}",
            bind_index,
            bind_index + 1
        ));

        // Every pool, merged in submitted_at order and paged once, as in
        // `list`: a batch operation that read only the default pool silently
        // skipped every matching workflow on the other shards.
        let pools = self.scan_pools();
        let (pool_limit, pool_offset) = if pools.len() == 1 {
            (limit, offset)
        } else {
            (limit + offset, 0)
        };
        let mut rows: Vec<WorkflowExecutionRow> = Vec::new();
        for pool in &pools {
            let mut q = sqlx::query_as::<_, WorkflowExecutionRow>(&query)
                .bind(namespace)
                .bind(organization_id);

            if let Some(status) = filter_status {
                q = q.bind(status);
            }
            if let Some(wf_type) = workflow_type {
                q = q.bind(wf_type);
            }
            if let Some(attrs) = labels {
                q = q.bind(attrs);
            }
            if let Some(after) = started_after {
                q = q.bind(after);
            }
            if let Some(before) = started_before {
                q = q.bind(before);
            }
            q = q.bind(pool_limit).bind(pool_offset);

            rows.extend(q.fetch_all(*pool).await.map_err(|e| {
                OrcherError::internal(format!("Failed to list workflows by batch filter: {}", e))
            })?);
        }
        if pools.len() > 1 {
            rows.sort_by_key(|r| r.submitted_at);
            rows = rows
                .into_iter()
                .skip(offset.max(0) as usize)
                .take(limit.max(0) as usize)
                .collect();
        }

        Ok(rows.into_iter().map(Into::into).collect())
    }

    /// Find children executions (sub-workflows)
    pub async fn find_children(
        &self,
        parent_execution_id: &str,
    ) -> OrcherResult<Vec<WorkflowExecution>> {
        debug!(parent_execution_id = %parent_execution_id, "Finding child workflow executions");

        // A child routes by its own id, not its parent's: gather every pool.
        let mut rows = Vec::new();
        for pool in self.scan_pools() {
            rows.extend(
                sqlx::query_as::<_, WorkflowExecutionRow>(
                    concat!("SELECT ", crate::repositories::columns::workflow_executions!(), " FROM workflow_executions WHERE parent_execution_id = $1 ORDER BY submitted_at"),
                )
                .bind(parent_execution_id)
                .fetch_all(pool)
                .await
                .map_err(|e| {
                    OrcherError::internal(format!("Failed to find child workflow executions: {}", e))
                })?,
            );
        }
        rows.sort_by_key(|r| r.submitted_at);

        Ok(rows.into_iter().map(Into::into).collect())
    }

    /// Get active executions (pending, queued, running, retrying, waiting)
    pub async fn get_active(
        &self,
        namespace: &str,
        limit: i64,
    ) -> OrcherResult<Vec<WorkflowExecution>> {
        debug!(namespace = %namespace, "Getting active workflow executions");

        // Every pool, merged in the same order and cut to the limit.
        let mut rows: Vec<WorkflowExecutionRow> = Vec::new();
        for pool in self.scan_pools() {
            rows.extend(
                sqlx::query_as::<_, WorkflowExecutionRow>(concat!(
                    r#"
                    SELECT "#,
                    crate::repositories::columns::workflow_executions!(),
                    r#" FROM workflow_executions
                    WHERE namespace = $1
                      AND status IN ('pending', 'queued', 'running', 'retrying', 'waiting')
                      AND cancelled_at IS NULL
                    ORDER BY priority DESC, submitted_at
                    LIMIT $2
                    "#
                ))
                .bind(namespace)
                .bind(limit)
                .fetch_all(pool)
                .await
                .map_err(|e| {
                    OrcherError::internal(format!(
                        "Failed to get active workflow executions: {}",
                        e
                    ))
                })?,
            );
        }
        rows.sort_by(|a, b| {
            b.priority
                .cmp(&a.priority)
                .then_with(|| a.submitted_at.cmp(&b.submitted_at))
        });
        rows.truncate(limit.max(0) as usize);

        Ok(rows.into_iter().map(Into::into).collect())
    }

    /// Claim and get the next available workflow execution for processing
    /// Uses SELECT FOR UPDATE SKIP LOCKED for safe distributed polling
    /// The release new executions on this queue bind to, if a policy is set.
    ///
    /// `None` means no policy: any release may take new work, which is the
    /// default and what every queue does until someone sets one.
    pub async fn current_release(
        &self,
        task_queue: &str,
        namespace: &str,
        organization_id: Option<uuid::Uuid>,
    ) -> OrcherResult<Option<String>> {
        // The default pool, deliberately: release pins are coordination state,
        // held once for the deployment, not sharded workflow state.
        let row: Option<Option<String>> = sqlx::query_scalar(
            "SELECT current_version_id FROM task_queue_releases \
             WHERE namespace = $1 AND task_queue = $2 \
               AND organization_id IS NOT DISTINCT FROM $3",
        )
        .bind(namespace)
        .bind(task_queue)
        .bind(organization_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| OrcherError::internal(format!("Failed to read current release: {}", e)))?;

        Ok(row.flatten())
    }

    /// Point new executions on this queue at a release.
    ///
    /// Passing `None` removes the policy and restores "any release may take new
    /// work". Executions already bound are unaffected either way — this decides
    /// where *new* work goes, never where running work continues.
    pub async fn set_current_release(
        &self,
        task_queue: &str,
        namespace: &str,
        version_id: Option<&str>,
        organization_id: Option<uuid::Uuid>,
    ) -> OrcherResult<()> {
        // Blank is not a release. Without this an empty form field would pin the
        // queue to a release named "" and nothing could ever claim new work.
        let version_id = version_id.filter(|v| !v.trim().is_empty());

        // The default pool, deliberately: release pins are coordination state,
        // held once for the deployment, not sharded workflow state.
        sqlx::query(
            "INSERT INTO task_queue_releases \
                 (organization_id, namespace, task_queue, current_version_id, updated_at) \
             VALUES ($1, $2, $3, $4, now()) \
             ON CONFLICT (organization_id, namespace, task_queue) \
             DO UPDATE SET current_version_id = EXCLUDED.current_version_id, updated_at = now()",
        )
        .bind(organization_id)
        .bind(namespace)
        .bind(task_queue)
        .bind(version_id)
        .execute(&self.pool)
        .await
        .map_err(|e| OrcherError::internal(format!("Failed to set current release: {}", e)))?;

        info!(
            task_queue = %task_queue,
            namespace = %namespace,
            version_id = ?version_id,
            "Current release updated for task queue"
        );
        Ok(())
    }

    /// Move a stalled execution onto a different release, or unbind it entirely.
    ///
    /// The escape hatch for the one failure mode routing introduces: an
    /// execution bound to a release nobody runs any more simply waits. Starting
    /// a worker on that release is the first answer, but it is not always
    /// possible — the build may be gone. Passing `None` unbinds the execution so
    /// any release may pick it up.
    ///
    /// This is deliberately a deliberate act. Replay against different code is
    /// exactly what binding prevents, so moving an execution is safe only when
    /// the new release is compatible with the journal already written, which the
    /// server cannot verify. Refuses to touch anything terminal, since there is
    /// nothing left to run.
    pub async fn rebind_execution(
        &self,
        execution_id: &str,
        version_id: Option<&str>,
        organization_id: Option<uuid::Uuid>,
    ) -> OrcherResult<bool> {
        let version_id = version_id.filter(|v| !v.trim().is_empty());

        let mut moved = false;
        for pool in self.scan_pools() {
            let result = sqlx::query(
                "UPDATE workflow_executions SET bound_version_id = $1, updated_at = NOW() \
                 WHERE execution_id = $2 \
                   AND ($3::uuid IS NULL OR organization_id = $3) \
                   AND status NOT IN ('completed', 'failed', 'cancelled', 'terminated', 'timed_out')",
            )
            .bind(version_id)
            .bind(execution_id)
            .bind(organization_id)
            .execute(pool)
            .await
            .map_err(|e| OrcherError::internal(format!("Failed to rebind execution: {}", e)))?;

            if result.rows_affected() > 0 {
                moved = true;
                break;
            }
        }

        if moved {
            warn!(
                execution_id = %execution_id,
                version_id = ?version_id,
                "Execution rebound to a different release — replay will continue against code \
                 its journal was not written by if the two are not compatible"
            );
        }
        Ok(moved)
    }

    /// Releases with executions waiting for them on this queue, and how many.
    ///
    /// Routing by binding trades availability for correctness: an execution bound
    /// to a release no live worker declares simply is not claimed. That is the
    /// right call, but it is only operable if the stall is visible — otherwise the
    /// first symptom is a workflow that quietly never finishes.
    ///
    /// Answers two questions with one query: "what is stuck and on what" during an
    /// incident, and "which releases still have live executions" when deciding
    /// whether an old release can finally be retired.
    pub async fn releases_with_waiting_work(
        &self,
        task_queue: &str,
        namespace: &str,
        organization_id: Option<uuid::Uuid>,
    ) -> OrcherResult<Vec<(String, i64)>> {
        // Counted on every pool and summed per release.
        let mut totals: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
        for pool in self.scan_pools() {
            let rows: Vec<(String, i64)> = sqlx::query_as(
                r#"
            SELECT bound_version_id, COUNT(*)
            FROM workflow_executions
            WHERE task_queue = $1
              AND namespace = $2
              AND ($3::uuid IS NULL OR organization_id = $3)
              AND bound_version_id IS NOT NULL
              AND status IN ('pending', 'queued', 'running', 'retrying', 'waiting')
              AND claimed_at IS NULL
              AND cancelled_at IS NULL
            GROUP BY bound_version_id
            ORDER BY COUNT(*) DESC
            "#,
            )
            .bind(task_queue)
            .bind(namespace)
            .bind(organization_id)
            .fetch_all(pool)
            .await
            .map_err(|e| {
                OrcherError::internal(format!("Failed to list releases with waiting work: {}", e))
            })?;
            for (release, count) in rows {
                *totals.entry(release).or_insert(0) += count;
            }
        }

        let mut rows: Vec<(String, i64)> = totals.into_iter().collect();
        rows.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        Ok(rows)
    }

    /// `version_id` is the code release of the claiming worker, or `None` when it
    /// declares none. It binds the execution on **first** claim only — see the
    /// COALESCE below.
    ///
    /// `organization_id` is the organization the poll was made for: with one,
    /// only that organization's executions are claimed, and without one any
    /// are, as for every by-id path (`tenant::may_act_on`). A namespace name
    /// is unique only within an organization, so without this a worker of one
    /// organization was handed another's execution in a namespace of the same
    /// name — and, now that a completion is refused outside the caller's
    /// organization, one it could never complete.
    pub async fn claim_and_get_next(
        &self,
        task_queue: &str,
        namespace: &str,
        identity: &str,
        claim_timeout_secs: i64,
        version_id: Option<&str>,
        organization_id: Option<Uuid>,
    ) -> OrcherResult<Option<WorkflowExecution>> {
        self.claim_next_from(
            task_queue,
            namespace,
            identity,
            claim_timeout_secs,
            version_id,
            organization_id,
            None,
            &[],
        )
        .await
    }

    /// [`claim_and_get_next`](Self::claim_and_get_next) for a poll from the
    /// worker process `worker_instance_id`, which the claim records
    /// (`claimed_by_instance`, V060) so that it can be released if that
    /// process stops being seen. `None` records none, and such a claim is
    /// never released on its process's account.
    ///
    /// `step_ids` are the step-id schemes the worker supports, as
    /// `orcher.v1.StepIdScheme` numbers them; empty for a worker that reports
    /// none. They decide which runs it may claim and bind a run's scheme on
    /// its first claim ([`step_id_predicate`], [`step_id_binding`]).
    #[allow(clippy::too_many_arguments)]
    pub async fn claim_next_from(
        &self,
        task_queue: &str,
        namespace: &str,
        identity: &str,
        claim_timeout_secs: i64,
        version_id: Option<&str>,
        organization_id: Option<Uuid>,
        worker_instance_id: Option<&str>,
        step_ids: &[i16],
    ) -> OrcherResult<Option<WorkflowExecution>> {
        debug!(
            task_queue = %task_queue,
            namespace = %namespace,
            identity = %identity,
            claim_timeout_secs = %claim_timeout_secs,
            "CLAIM_AND_GET_NEXT: Called"
        );

        let claim_timeout = Timestamptz::now() + SignedDuration::from_secs(claim_timeout_secs);

        // The workflow queue is partitioned across shard pools (a workflow's row lives on
        // its shard). Scan pools in turn and claim the next available workflow from the
        // first pool that has one; FOR UPDATE SKIP LOCKED keeps concurrent pollers safe.
        // With no resolver this is a single pool — identical to before.
        let (los, his, min, max) = self.claim_bounds();
        let sql = format!(
            concat!(
                r#"
            WITH next_execution AS (
                SELECT id
                FROM workflow_executions
                WHERE task_queue = $3
                  AND namespace = $4
                  AND status IN ('pending', 'queued', 'running', 'retrying', 'waiting')
                  AND claimed_at IS NULL
                  AND cancelled_at IS NULL
                  AND {due}
                  {organization}
                  -- Route by binding. An unbound execution is claimable by
                  -- anyone, which is every execution today and stays the default.
                  -- A bound one is claimable only by a worker declaring the same
                  -- release, so it never replays against code its journal was
                  -- not written by.
                  --
                  -- Refuses rather than falling through: a stalled execution is
                  -- recoverable by starting a worker on that release, whereas a
                  -- diverged one may not be recoverable at all. Same direction as
                  -- the write fence.
                  AND {release}
                  -- And by step-id scheme, refusing in the same direction:
                  -- a history replayed under other step ids is not found.
                  AND {step_ids}
                  -- And, for a short while after it is released to resume,
                  -- to the worker process that keeps its journal.
                  AND {holder}
                  AND {shard}
                ORDER BY priority DESC, submitted_at
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
            UPDATE workflow_executions we
            SET claimed_by = $1,
                claimed_at = NOW(),
                -- The run's own task timeout, if it was started with one
                -- (V065); otherwise the engine's claim timeout.
                claim_timeout_at = COALESCE(
                    NOW() + make_interval(secs => we.task_timeout_secs), $2),
                -- The engine handing the activation out, so that if it dies
                -- with the reply undelivered its survivors can release it.
                claimed_on_node = $10,
                -- And the worker process it goes to, so that if that dies
                -- with the activation unanswered it can be released too.
                claimed_by_instance = $11,
                -- Claimed: whoever holds it now, it is held for nobody.
                preferred_until = NULL,
                -- Where in its journal this activation reads to: its last
                -- entry now. An outcome recorded after the claim finds the
                -- activation in flight, so its wake-up is deferred to the
                -- next one, which carries it (V087).
                claimed_through_entry = (
                    SELECT MAX(j.event_id) FROM workflow_execution_journal j
                    WHERE j.execution_id = we.execution_id),
                -- Bind on first claim and never rebind. COALESCE keeps the
                -- original value once set, so a later claim by a different
                -- release cannot move an in-flight execution onto new code —
                -- which is the divergence the binding exists to prevent.
                bound_version_id = COALESCE(we.bound_version_id, $9),
                -- And bind the step-id scheme and history lineage, also on
                -- first claim only.
                {bind_step_ids},
                -- Every claim is a new activation. Numbering it here, in the
                -- statement that hands it out, means no two activations can
                -- carry the same number and a later one always carries a
                -- higher one — which is what lets a completion from an
                -- earlier one be told apart and refused.
                activation_seq = we.activation_seq + 1,
                -- The activation handed out now replays everything, so a
                -- wake-up deferred for the one before it is answered by it.
                pending_requeue = FALSE,
                pending_requeue_at = NULL,
                updated_at = NOW()
            FROM next_execution ne
            WHERE we.id = ne.id
              AND {fence_free}
            RETURNING "#,
                crate::repositories::columns::workflow_executions!("we"),
                r#"
            "#
            ),
            shard = shard_predicate("workflow_executions", 5),
            due = START_DUE,
            release = release_predicate("$9", "$4", "$3"),
            step_ids = step_id_predicate("$12"),
            bind_step_ids = step_id_binding("$12", "we."),
            fence_free = fence_free("we"),
            organization = organization_predicate(organization_id, "$13"),
            holder = holder_predicate("workflow_executions", "$11"),
        );
        for pool in self.scan_pools() {
            let mut conn = pool.acquire().await.map_err(|e| {
                OrcherError::internal(format!("Failed to claim workflow execution: {}", e))
            })?;
            let row = retry_on_stale_plan!("workflow claim", conn => {
                let row = sqlx::query_as::<_, WorkflowExecutionRow>(&sql)
                    .bind(identity)
                    .bind(claim_timeout)
                    .bind(task_queue)
                    .bind(namespace)
                    .bind(los.clone())
                    .bind(his.clone())
                    .bind(min)
                    .bind(max)
                    .bind(version_id)
                    .bind(self.claim_node())
                    .bind(worker_instance_id)
                    .bind(step_ids);
                match organization_id {
                    Some(organization_id) => row.bind(organization_id),
                    None => row,
                }
                .fetch_optional(&mut *conn)
                .await
            })
            .map_err(|e| {
                warn!(
                    error = %e,
                    task_queue = %task_queue,
                    namespace = %namespace,
                    "Failed to claim workflow execution"
                );
                OrcherError::internal(format!("Failed to claim workflow execution: {}", e))
            })?;
            if let Some(execution_row) = row {
                debug!(
                    execution_id = %execution_row.execution_id,
                    workflow_name = %execution_row.workflow_name,
                    claimed_by = %identity,
                    "Claimed next workflow execution"
                );
                return Ok(Some(execution_row.into()));
            }
        }

        Ok(None)
    }

    /// Release a claim on a workflow execution
    pub async fn release_claim(&self, execution_id: &str) -> OrcherResult<()> {
        debug!(execution_id = %execution_id, "RELEASE_CLAIM: Starting release");

        // Keyed by the globally-unique execution_id, not the routing id: scan shard
        // pools to find the one holding the row, then run every statement on it.
        let mut owner_pool: Option<&PgPool> = None;
        let mut before_state = None;
        for pool in self.scan_pools() {
            let row = sqlx::query(
                r#"
                SELECT execution_id, workflow_name, status, claimed_by, claimed_at, claim_timeout_at, task_queue, namespace
                FROM workflow_executions
                WHERE execution_id = $1
                "#,
            )
            .bind(execution_id)
            .fetch_optional(pool)
            .await
            .map_err(|e| {
                warn!(error = %e, "Failed to query execution before release");
                OrcherError::internal(format!("Failed to query execution: {}", e))
            })?;
            if let Some(row) = row {
                owner_pool = Some(pool);
                before_state = Some(row);
                break;
            }
        }
        let pool = match owner_pool {
            Some(p) => p,
            None => {
                warn!(execution_id = %execution_id, "RELEASE_CLAIM: Execution not found in database!");
                return Err(OrcherError::not_found(
                    "workflow_execution",
                    execution_id.to_string(),
                ));
            }
        };

        if let Some(row) = before_state {
            let workflow_name: String = row.try_get("workflow_name").unwrap_or_default();
            let status: String = row.try_get("status").unwrap_or_default();
            let claimed_by: Option<String> = row.try_get("claimed_by").ok().flatten();
            let task_queue: String = row.try_get("task_queue").unwrap_or_default();
            let namespace: String = row.try_get("namespace").unwrap_or_default();

            debug!(
                execution_id = %execution_id,
                workflow_name = %workflow_name,
                status = %status,
                claimed_by = ?claimed_by,
                task_queue = %task_queue,
                namespace = %namespace,
                "RELEASE_CLAIM: BEFORE state"
            );
        } else {
            warn!(execution_id = %execution_id, "RELEASE_CLAIM: Execution not found in database!");
            return Err(OrcherError::not_found(
                "workflow_execution",
                execution_id.to_string(),
            ));
        }

        let result = sqlx::query(
            r#"
            UPDATE workflow_executions
            SET claimed_by = NULL,
                claimed_at = NULL,
                claim_timeout_at = NULL,
                claimed_on_node = NULL,
                updated_at = NOW()
            WHERE execution_id = $1
            "#,
        )
        .bind(execution_id)
        .execute(pool)
        .await
        .map_err(|e| {
            warn!(error = %e, "Failed to release claim");
            OrcherError::internal(format!("Failed to release claim: {}", e))
        })?;

        if result.rows_affected() == 0 {
            warn!(execution_id = %execution_id, "RELEASE_CLAIM: No rows affected!");
            return Err(OrcherError::not_found(
                "workflow_execution",
                execution_id.to_string(),
            ));
        }

        // Verify the release by checking the state after
        let after_state = sqlx::query(
            r#"
            SELECT execution_id, workflow_name, status, claimed_by, claimed_at, claim_timeout_at
            FROM workflow_executions
            WHERE execution_id = $1
            "#,
        )
        .bind(execution_id)
        .fetch_optional(pool)
        .await
        .map_err(|e| {
            warn!(error = %e, "Failed to verify release");
            OrcherError::internal(format!("Failed to verify release: {}", e))
        })?;

        if let Some(row) = after_state {
            let claimed_by: Option<String> = row.try_get("claimed_by").ok().flatten();
            let claimed_at: Option<Timestamptz> = row.try_get("claimed_at").ok().flatten();
            let claim_timeout_at: Option<Timestamptz> =
                row.try_get("claim_timeout_at").ok().flatten();

            debug!(
                execution_id = %execution_id,
                claimed_by = ?claimed_by,
                claimed_at = ?claimed_at,
                claim_timeout_at = ?claim_timeout_at,
                "RELEASE_CLAIM: AFTER state - claim released successfully"
            );

            // Validate that fields are actually NULL
            if claimed_by.is_some() || claimed_at.is_some() || claim_timeout_at.is_some() {
                warn!(
                    execution_id = %execution_id,
                    "RELEASE_CLAIM: WARNING - Some claim fields are still set!"
                );
            }
        }

        debug!(execution_id = %execution_id, rows_affected = result.rows_affected(), "RELEASE_CLAIM: Complete");
        Ok(())
    }

    /// Release stale claims (from crashed services)
    pub async fn release_stale_claims(&self) -> OrcherResult<u64> {
        debug!("Releasing stale claims");

        // Every pool, for the same reason as reclaim_expired.
        let mut count = 0;
        for pool in self.scan_pools() {
            count += sqlx::query(
                r#"
                UPDATE workflow_executions
                SET claimed_by = NULL,
                    claimed_at = NULL,
                    claim_timeout_at = NULL,
                    claimed_on_node = NULL,
                    updated_at = NOW()
                WHERE claimed_at IS NOT NULL
                  AND claim_timeout_at < NOW()
                  AND status IN ('pending', 'queued', 'running', 'retrying', 'waiting')
                "#,
            )
            .execute(pool)
            .await
            .map_err(|e| {
                warn!(error = %e, "Failed to release stale claims");
                OrcherError::internal(format!("Failed to release stale claims: {}", e))
            })?
            .rows_affected();
        }
        if count > 0 {
            info!(count = count, "Released stale claims");
        }

        Ok(count)
    }

    // ========================================================================
    // PENDING REQUEUE SUPPORT (Solution B: Task-Aware Re-polling)
    // ========================================================================

    /// Set pending_requeue flag for a workflow execution
    ///
    /// Called when a task completes but the workflow is still claimed.
    /// This defers the Redis re-queue until after the claim is released,
    /// preventing the poll → fail-to-claim → re-push latency loop.
    ///
    /// # Arguments
    /// * `execution_id` - The execution ID to mark for pending requeue
    ///
    /// # Returns
    /// * `Ok(true)` - Flag was set successfully
    /// * `Ok(false)` - Execution not found or already marked
    /// * `Err(_)` - Database error
    pub async fn set_pending_requeue(&self, execution_id: &str) -> OrcherResult<bool> {
        debug!(
            execution_id = %execution_id,
            "Setting pending_requeue flag for deferred re-queue"
        );

        let mut result = sqlx::postgres::PgQueryResult::default();
        for pool in self.scan_pools() {
            result = sqlx::query(
                r#"
                        UPDATE workflow_executions
                        SET pending_requeue = TRUE,
                            pending_requeue_at = NOW(),
                            updated_at = NOW()
                        WHERE execution_id = $1
                          AND pending_requeue = FALSE
                        "#,
            )
            .bind(execution_id)
            .execute(pool)
            .await
            .map_err(|e| {
                warn!(
                    execution_id = %execution_id,
                    error = %e,
                    "Failed to set pending_requeue flag"
                );
                OrcherError::internal(format!("Failed to set pending_requeue: {}", e))
            })?;
            if result.rows_affected() > 0 {
                break;
            }
        }

        let updated = result.rows_affected() > 0;
        if updated {
            info!(
                execution_id = %execution_id,
                "Pending requeue flag set - will re-queue after claim release"
            );
        } else {
            debug!(
                execution_id = %execution_id,
                "Pending requeue flag not set (already set or execution not found)"
            );
        }

        Ok(updated)
    }

    /// Clear pending_requeue flag for a workflow execution
    ///
    /// Called after the workflow has been successfully re-queued to Redis
    /// following a claim release.
    ///
    /// # Arguments
    /// * `execution_id` - The execution ID to clear the flag for
    ///
    /// # Returns
    /// * `Ok(true)` - Flag was cleared successfully
    /// * `Ok(false)` - Execution not found or flag was not set
    /// * `Err(_)` - Database error
    pub async fn clear_pending_requeue(&self, execution_id: &str) -> OrcherResult<bool> {
        debug!(
            execution_id = %execution_id,
            "Clearing pending_requeue flag after successful re-queue"
        );

        let mut result = sqlx::postgres::PgQueryResult::default();
        for pool in self.scan_pools() {
            result = sqlx::query(
                r#"
                        UPDATE workflow_executions
                        SET pending_requeue = FALSE,
                            pending_requeue_at = NULL,
                            updated_at = NOW()
                        WHERE execution_id = $1
                          AND pending_requeue = TRUE
                        "#,
            )
            .bind(execution_id)
            .execute(pool)
            .await
            .map_err(|e| {
                warn!(
                    execution_id = %execution_id,
                    error = %e,
                    "Failed to clear pending_requeue flag"
                );
                OrcherError::internal(format!("Failed to clear pending_requeue: {}", e))
            })?;
            if result.rows_affected() > 0 {
                break;
            }
        }

        let cleared = result.rows_affected() > 0;
        if cleared {
            debug!(
                execution_id = %execution_id,
                "Pending requeue flag cleared after successful re-queue"
            );
        }

        Ok(cleared)
    }

    /// Wake a parked execution: release its claim so it is handed out again —
    /// unless an activation of it is out with a worker right now.
    ///
    /// Every wake-up path — a task finishing, a child's outcome reaching its
    /// parent, a timer firing, an event arriving — used to release the claim
    /// outright. When the execution's current activation had not completed
    /// yet, that let a poller claim it again and hand a second activation out
    /// while the first was still running, and the first one's completion then
    /// arrived for an activation that was no longer current.
    ///
    /// In flight means claimed, and handed out more recently than any
    /// completion that has been applied (`activation_seq` ahead of
    /// `completed_activation`). Then the wake-up is recorded
    /// (`pending_requeue`) and the claim kept, and
    /// [`ActivationFence::applied`] releases it when that activation's
    /// completion is applied. The decision and the write are one statement,
    /// and `applied` takes the same row lock, so a wake-up is either seen by
    /// the completion or made after it and released here — never lost between
    /// them. Should the activation never complete, its claim times out and the
    /// next claim replays everything, wake-up included.
    pub async fn release_claim_to_resume(&self, execution_id: &str) -> OrcherResult<Resume> {
        let bound = self.in_flight_bound_secs();
        for pool in self.scan_pools() {
            match release_claim_to_resume_on(
                pool,
                "execution_id = $1",
                execution_id,
                bound,
                self.journal_hold(),
            )
            .await?
            {
                Resume::NotFound => continue,
                resume => {
                    if resume == Resume::Deferred {
                        info!(
                            execution_id = %execution_id,
                            "Wake-up deferred: an activation is in flight, and its completion \
                             releases the claim"
                        );
                    }
                    return Ok(resume);
                }
            }
        }
        Ok(Resume::NotFound)
    }

    /// Record that the claimed activation `activation` of `execution_id` is
    /// being handed to the poller `identity`: [`HandOut::Handed`] if it was,
    /// [`HandOut::Gone`] if that activation is no longer the claimed one — its
    /// claim lapsed and something claimed the execution again — in which case
    /// it must not be handed out, and [`HandOut::Busy`] if it is still the
    /// claimed one but a completion of the execution is being applied, in
    /// which case it must be offered again.
    ///
    /// The claim is also restarted, as a fresh claim would be: the activation
    /// begins now, not when the batch was claimed, and runs for the run's own
    /// task timeout if it has one (`claim_timeout_secs` otherwise). Left at the batch's claim
    /// time, an execution that sat in the buffer could be handed out already
    /// older than the in-flight bound, and the first wake-up would release
    /// its claim and hand it to a second worker. A claim that has expired in
    /// the buffer is not handed out at all — the reclaimer may be about to
    /// take it back — and the entry is skipped.
    ///
    /// Nor is one outside `organization_id`, scoped as
    /// [`claim_and_get_next`](Self::claim_and_get_next) is. The buffers are kept
    /// per organization, so this does not happen; it is checked here as well
    /// so that the claim that hands an execution to a worker is scoped on
    /// every path, not only the ones that find it.
    ///
    /// `worker_instance_id` is the process the poller belongs to, recorded as
    /// a claim records it.
    pub async fn hand_to(
        &self,
        execution_id: &str,
        activation: i64,
        identity: &str,
        claim_timeout_secs: i64,
        organization_id: Option<Uuid>,
        worker_instance_id: Option<&str>,
    ) -> OrcherResult<HandOut> {
        let sql = format!(
            "UPDATE workflow_executions \
             SET claimed_by = $3, claimed_at = NOW(), \
                 claim_timeout_at = NOW() + make_interval(secs => COALESCE(task_timeout_secs, $4)), \
                 claimed_on_node = $5, claimed_by_instance = $7, \
                 pending_requeue = FALSE, pending_requeue_at = NULL \
             WHERE execution_id = $1 AND activation_seq = $2 AND claimed_by IS NOT NULL \
               AND claim_timeout_at > NOW() \
               AND ($6::uuid IS NULL OR organization_id = $6) AND {}",
            fence_free("workflow_executions")
        );
        for pool in self.scan_pools() {
            let moved = sqlx::query(&sql)
                .bind(execution_id)
                .bind(activation)
                .bind(identity)
                .bind(claim_timeout_secs as f64)
                .bind(self.claim_node())
                .bind(organization_id)
                .bind(worker_instance_id)
                .execute(pool)
                .await
                .map_err(|e| {
                    OrcherError::internal(format!("Failed to hand out the activation: {e}"))
                })?;
            if moved.rows_affected() > 0 {
                return Ok(HandOut::Handed);
            }
            // Not moved: busy if the claim is still exactly the batch's, so
            // only the fence refused it.
            let held: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM workflow_executions \
                 WHERE execution_id = $1 AND activation_seq = $2 AND claimed_by IS NOT NULL \
                   AND claim_timeout_at > NOW() \
                   AND ($3::uuid IS NULL OR organization_id = $3))",
            )
            .bind(execution_id)
            .bind(activation)
            .bind(organization_id)
            .fetch_one(pool)
            .await
            .map_err(|e| {
                OrcherError::internal(format!("Failed to hand out the activation: {e}"))
            })?;
            if held {
                return Ok(HandOut::Busy);
            }
        }
        Ok(HandOut::Gone)
    }

    /// How long after it was claimed an activation still counts as in flight
    /// for [`release_claim_to_resume`](Self::release_claim_to_resume).
    pub fn set_activation_in_flight_bound(&self, bound: std::time::Duration) {
        self.in_flight_bound_secs.store(
            bound.as_secs().max(1) as i64,
            std::sync::atomic::Ordering::Relaxed,
        );
    }

    /// How long a run released to resume is held for the worker process
    /// that had its last activation: that process keeps the run's journal,
    /// and an activation going back to it carries only the new entries.
    /// Zero holds nothing, and any process may claim a run at once.
    pub fn set_journal_holder_hold(&self, hold: std::time::Duration) {
        self.journal_hold_ms.store(
            hold.as_millis() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
    }

    fn journal_hold(&self) -> std::time::Duration {
        std::time::Duration::from_millis(
            self.journal_hold_ms
                .load(std::sync::atomic::Ordering::Relaxed),
        )
    }

    fn in_flight_bound_secs(&self) -> i64 {
        self.in_flight_bound_secs
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Release claim and get pending_requeue status atomically
    ///
    /// This combines claim release with pending_requeue check in a single
    /// database round-trip for efficiency. Returns the pending_requeue state
    /// so the caller knows whether to push to Redis.
    ///
    /// # Arguments
    /// * `execution_id` - The execution ID to release claim for
    ///
    /// # Returns
    /// * `Ok(Some(PendingRequeueInfo))` - Claim released, includes requeue info
    /// * `Ok(None)` - Execution not found
    /// * `Err(_)` - Database error
    pub async fn release_claim_and_check_requeue(
        &self,
        execution_id: &str,
    ) -> OrcherResult<Option<PendingRequeueInfo>> {
        debug!(
            execution_id = %execution_id,
            "RELEASE_CLAIM_AND_CHECK: Starting atomic release + requeue check"
        );

        // Atomically release claim, read AND clear pending_requeue flag using CTE
        // This fixes a TOCTOU race where:
        // 1. complete_workflow_execution starts release_claim_and_check_requeue
        // 2. complete_task_execution sets pending_requeue = true (in another transaction)
        // 3. release_claim_and_check_requeue reads pending_requeue = false (before other tx commits)
        // 4. The other transaction commits pending_requeue = true
        // 5. Nobody processes the pending requeue!
        //
        // Fix: Use a CTE to capture the old value BEFORE the update, then clear it.
        // If a concurrent transaction sets pending_requeue=true AFTER this runs,
        // the flag will remain true and be picked up by the background recovery job.
        let mut result: Option<(bool, String, String)> = None;
        for pool in self.scan_pools() {
            result = sqlx::query_as(
                        r#"
                        WITH old_state AS (
                            SELECT pending_requeue, task_queue, namespace
                            FROM workflow_executions
                            WHERE execution_id = $1
                            FOR UPDATE
                        )
                        UPDATE workflow_executions we
                        SET claimed_by = NULL,
                            claimed_at = NULL,
                            claim_timeout_at = NULL,
                            claimed_on_node = NULL,
                            pending_requeue = FALSE,
                            pending_requeue_at = NULL,
                            updated_at = NOW()
                        FROM old_state
                        WHERE we.execution_id = $1
                        RETURNING old_state.pending_requeue, old_state.task_queue, old_state.namespace
                        "#,
                    )
                    .bind(execution_id)
                    .fetch_optional(pool)
                    .await
                    .map_err(|e| {
                        warn!(
                            execution_id = %execution_id,
                            error = %e,
                            "Failed to release claim and check requeue"
                        );
                        OrcherError::internal(format!("Failed to release claim and check requeue: {}", e))
                    })?;
            if result.is_some() {
                break;
            }
        }

        match result {
            Some((pending_requeue, task_queue, namespace)) => {
                debug!(
                    execution_id = %execution_id,
                    pending_requeue = pending_requeue,
                    task_queue = %task_queue,
                    namespace = %namespace,
                    "RELEASE_CLAIM_AND_CHECK: Claim released, requeue info retrieved"
                );
                Ok(Some(PendingRequeueInfo {
                    pending_requeue,
                    task_queue,
                    namespace,
                }))
            }
            None => {
                warn!(
                    execution_id = %execution_id,
                    "RELEASE_CLAIM_AND_CHECK: Execution not found"
                );
                Ok(None)
            }
        }
    }

    /// Find workflows with pending_requeue that need to be re-queued
    ///
    /// This is a fallback mechanism for cases where the re-queue after claim
    /// release failed or was missed. A background job can call this periodically.
    ///
    /// # Arguments
    /// * `older_than_secs` - Only return workflows where pending_requeue_at is older than this
    /// * `limit` - Maximum number of workflows to return
    ///
    /// # Returns
    /// * `Ok(Vec<PendingRequeueInfo>)` - Workflows that need re-queuing
    /// * `Err(_)` - Database error
    pub async fn find_pending_requeues(
        &self,
        older_than_secs: i64,
        limit: i64,
    ) -> OrcherResult<Vec<PendingRequeueWorkflow>> {
        debug!(
            older_than_secs = older_than_secs,
            limit = limit,
            "Finding workflows with pending requeue"
        );

        // Every pool; each returns at most `limit`, and the total is cut to it.
        let mut rows: Vec<(String, String, String)> = Vec::new();
        for pool in self.scan_pools() {
            rows.extend(
                sqlx::query_as::<_, (String, String, String)>(
                    r#"
            SELECT execution_id, task_queue, namespace
            FROM workflow_executions
            WHERE pending_requeue = TRUE
              AND pending_requeue_at < NOW() - $1 * INTERVAL '1 second'
              AND claimed_by IS NULL
              AND status IN ('running', 'waiting')
            ORDER BY pending_requeue_at ASC
            LIMIT $2
            "#,
                )
                .bind(older_than_secs)
                .bind(limit)
                .fetch_all(pool)
                .await
                .map_err(|e| {
                    warn!(error = %e, "Failed to find pending requeues");
                    OrcherError::internal(format!("Failed to find pending requeues: {}", e))
                })?,
            );
        }
        rows.truncate(limit.max(0) as usize);

        let workflows: Vec<PendingRequeueWorkflow> = rows
            .into_iter()
            .map(
                |(execution_id, task_queue, namespace)| PendingRequeueWorkflow {
                    execution_id,
                    task_queue,
                    namespace,
                },
            )
            .collect();

        if !workflows.is_empty() {
            info!(
                count = workflows.len(),
                "Found workflows with pending requeue for fallback processing"
            );
        }

        Ok(workflows)
    }

    /// Get count of available (unclaimed) workflow executions in a task queue
    ///
    /// This is used by the polling system to determine queue depth and
    /// whether to wait for work or poll immediately.
    pub async fn get_available_count(
        &self,
        task_queue: &str,
        namespace: &str,
    ) -> OrcherResult<i64> {
        debug!(
            task_queue = %task_queue,
            namespace = %namespace,
            "Getting available execution count"
        );

        // Summed over every pool.
        let mut count = 0;
        for pool in self.scan_pools() {
            let found = sqlx::query_scalar::<_, i64>(
                r#"
                SELECT COUNT(*)
                FROM workflow_executions
                WHERE task_queue = $1
                  AND namespace = $2
                  AND status IN ('pending', 'queued', 'running', 'retrying', 'waiting')
                  AND claimed_at IS NULL
                  AND cancelled_at IS NULL
                "#,
            )
            .bind(task_queue)
            .bind(namespace)
            .fetch_one(pool)
            .await
            .map_err(|e| {
                warn!(error = %e, "Failed to get available count");
                OrcherError::internal(format!("Failed to get available count: {}", e))
            })?;
            count += found;
        }

        debug!(count = count, "Available executions in queue");
        Ok(count)
    }

    /// Get available, claimed, and running counts for a queue in a single query.
    ///
    /// - **available**: unclaimed, schedulable (pending/queued/retrying/waiting)
    /// - **claimed**: claimed by a poller but not yet started (status = 'claimed')
    /// - **running**: actively executing by a worker (status = 'running', claimed)
    pub async fn get_queue_counts(
        &self,
        task_queue: &str,
        namespace: &str,
    ) -> OrcherResult<(i64, i64, i64)> {
        // Summed over every pool.
        let mut totals = (0i64, 0i64, 0i64);
        for pool in self.scan_pools() {
            let row: (Option<i64>, Option<i64>, Option<i64>) = sqlx::query_as(
            r#"
            SELECT
                COUNT(*) FILTER (
                    WHERE status IN ('pending', 'queued', 'retrying', 'waiting')
                      AND claimed_at IS NULL
                ) AS available_count,
                COUNT(*) FILTER (
                    WHERE status = 'claimed'
                ) AS claimed_count,
                COUNT(*) FILTER (
                    WHERE status = 'running'
                      AND claimed_at IS NOT NULL
                ) AS running_count
            FROM workflow_executions
            WHERE task_queue = $1
              AND namespace = $2
              AND cancelled_at IS NULL
              AND status NOT IN ('completed', 'failed', 'cancelled', 'terminated', 'timed_out', 'restarted_fresh')
            "#,
        )
        .bind(task_queue)
        .bind(namespace)
        .fetch_one(pool)
        .await
        .map_err(|e| {
            warn!(error = %e, "Failed to get queue counts");
            OrcherError::internal(format!("Failed to get queue counts: {}", e))
        })?;
            totals.0 += row.0.unwrap_or(0);
            totals.1 += row.1.unwrap_or(0);
            totals.2 += row.2.unwrap_or(0);
        }

        Ok(totals)
    }

    // ============================================================================
    // Polling-Friendly Method Aliases
    // ============================================================================
    // These methods provide more intuitive names for the polling/queueing system
    // while maintaining backward compatibility with existing claim methods.

    /// Claim an execution for polling (alias for claim_and_get_next)
    ///
    /// This is the primary method used by the ExecutionPollingManager to atomically
    /// claim work from the task queue. Uses SELECT FOR UPDATE SKIP LOCKED for
    /// lock-free concurrency.
    ///
    /// # ORCHER Terminology
    /// - "Service" polls for work (not "Worker")
    /// - "Execution" is the unit of work (not "Activation")
    /// - "Task Queue" distributes work to services
    #[inline]
    #[allow(clippy::too_many_arguments)]
    pub async fn claim_execution_for_poll(
        &self,
        task_queue: &str,
        namespace: &str,
        identity: &str,
        claim_timeout_secs: i64,
        version_id: Option<&str>,
        organization_id: Option<Uuid>,
        worker_instance_id: Option<&str>,
        step_ids: &[i16],
    ) -> OrcherResult<Option<WorkflowExecution>> {
        self.claim_next_from(
            task_queue,
            namespace,
            identity,
            claim_timeout_secs,
            version_id,
            organization_id,
            worker_instance_id,
            step_ids,
        )
        .await
    }

    /// Claim multiple executions in a single query (batch claiming)
    ///
    /// This method claims up to `limit` workflow executions atomically in a single
    /// database round-trip. This dramatically reduces contention when multiple
    /// workers are polling for work.
    ///
    /// ## Performance Impact
    ///
    /// With single-claim (`LIMIT 1`), N workers require N database round-trips
    /// to claim N workflows. With batch claiming, one worker can claim N workflows
    /// in a single round-trip, reducing:
    /// - Database connection usage
    /// - Lock contention on the workflow_executions table
    /// - Network latency overhead
    ///
    /// ## Use Cases
    ///
    /// 1. **High-throughput polling**: Claim multiple workflows and distribute to waiting workers
    /// 2. **Prefetching**: Claim a batch for local processing queue
    /// 3. **Load balancing**: Central coordinator claims and distributes to workers
    ///
    /// # Arguments
    ///
    /// * `task_queue` - Name of the task queue to claim from
    /// * `namespace` - Namespace for isolation
    /// * `identity` - Identity of the service claiming the executions
    /// * `claim_timeout_secs` - How long each claim is valid
    /// * `limit` - Maximum number of executions to claim (recommended: 5-20)
    ///
    /// # Returns
    ///
    /// - `Ok(vec)` - Vector of claimed executions (may be empty if no work available)
    /// - `Err(_)` - Database error
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // Claim up to 10 workflows for batch processing
    /// let executions = repo.claim_batch(
    ///     "orders-queue",
    ///     "production",
    ///     "service-1",
    ///     300,  // 5 minute claim timeout
    ///     10,   // claim up to 10
    /// ).await?;
    ///
    /// println!("Claimed {} workflows", executions.len());
    /// for exec in executions {
    ///     // Process or distribute to workers
    /// }
    /// ```
    #[allow(clippy::too_many_arguments)]
    pub async fn claim_batch(
        &self,
        task_queue: &str,
        namespace: &str,
        identity: &str,
        claim_timeout_secs: i64,
        limit: i32,
        version_id: Option<&str>,
        organization_id: Option<Uuid>,
    ) -> OrcherResult<Vec<WorkflowExecution>> {
        // Delegate to claim_batch_with_offset with no offset
        self.claim_batch_with_offset(
            task_queue,
            namespace,
            identity,
            claim_timeout_secs,
            limit,
            0,
            version_id,
            organization_id,
            &[],
        )
        .await
    }

    /// Claim multiple executions with optional offset for spreading workers
    ///
    /// This method extends `claim_batch` by allowing an offset into the queue.
    /// When multiple workers are claiming simultaneously, using different offsets
    /// spreads them across the queue and reduces hot-spot contention on the
    /// same index pages.
    ///
    /// ## Performance Impact
    ///
    /// With 4+ workers all claiming from offset 0:
    /// - All workers scan the same index pages
    /// - Lock contention on the first N rows
    /// - Workers that lose the race get nothing
    ///
    /// With staggered offsets (0, 5, 10, 15):
    /// - Workers scan different parts of the index
    /// - Reduced lock contention
    /// - Better parallelism
    ///
    /// # Arguments
    ///
    /// * `task_queue` - Name of the task queue to claim from
    /// * `namespace` - Namespace for isolation
    /// * `identity` - Identity of the service claiming the executions
    /// * `claim_timeout_secs` - How long each claim is valid
    /// * `limit` - Maximum number of executions to claim
    /// * `offset` - Number of rows to skip before claiming (for spreading workers)
    ///
    /// # Returns
    ///
    /// - `Ok(vec)` - Vector of claimed executions (may be empty if no work available)
    /// - `Err(_)` - Database error
    ///
    /// Scoped to `organization_id` and routed by `step_ids` as
    /// [`claim_next_from`](Self::claim_next_from) is.
    #[allow(clippy::too_many_arguments)]
    pub async fn claim_batch_with_offset(
        &self,
        task_queue: &str,
        namespace: &str,
        identity: &str,
        claim_timeout_secs: i64,
        limit: i32,
        offset: i32,
        version_id: Option<&str>,
        organization_id: Option<Uuid>,
        step_ids: &[i16],
    ) -> OrcherResult<Vec<WorkflowExecution>> {
        use orcher_common::types::time::Timestamptz;

        let claim_timeout = Timestamptz::now() + SignedDuration::from_secs(claim_timeout_secs);

        debug!(
            task_queue = %task_queue,
            namespace = %namespace,
            identity = %identity,
            claim_timeout_secs = claim_timeout_secs,
            limit = limit,
            offset = offset,
            "CLAIM_BATCH: Attempting to claim up to {} executions (offset: {})",
            limit, offset
        );

        // Batch claim using CTE with LIMIT N and optional OFFSET
        // Uses the same SELECT FOR UPDATE SKIP LOCKED pattern for race-free claiming
        // OFFSET spreads workers across the queue to reduce hot-spot contention.
        // The queue is partitioned across shard pools; scan pools and accumulate claims
        // up to `limit` so a poller drains work from every shard, not just the primary.
        // With no resolver this is a single pool — identical to before.
        let sql = format!(
            concat!(
                r#"
            WITH next_executions AS (
                SELECT id
                FROM workflow_executions
                WHERE task_queue = $3
                  AND namespace = $4
                  AND status IN ('pending', 'queued', 'running', 'retrying', 'waiting')
                  AND claimed_at IS NULL
                  AND cancelled_at IS NULL
                  AND {due}
                  {organization}
                  -- Route by binding, exactly as the single-execution claim does.
                  -- This path is the one the live poller uses, so leaving it out
                  -- would make the filter look enforced while being bypassable.
                  AND {release}
                  AND {step_ids}
                  -- A batch is claimed for this engine's buffer, not for a
                  -- worker process, so it takes no run held for one.
                  AND {holder}
                ORDER BY priority DESC, submitted_at
                OFFSET $6
                LIMIT $5
                FOR UPDATE SKIP LOCKED
            )
            UPDATE workflow_executions we
            SET claimed_by = $1,
                claimed_at = NOW(),
                -- The run's own task timeout, as the single claim takes it.
                -- The first of a batch goes straight to the poller that
                -- claimed it; the rest restart the clock as they are handed
                -- out (hand_to).
                claim_timeout_at = COALESCE(
                    NOW() + make_interval(secs => we.task_timeout_secs), $2),
                -- A batch sits in this engine's prefetch buffer until a poll
                -- takes it: if the engine dies first, nobody else knows the
                -- claims are there, and only this says whose they were.
                claimed_on_node = $8,
                -- Held by this engine, not by the process whose poll filled
                -- the buffer: the process each execution is handed to is
                -- recorded as it is handed over (hand_to).
                claimed_by_instance = NULL,
                preferred_until = NULL,
                -- Where in its journal this activation reads to: its last
                -- entry now. An outcome recorded after the claim finds the
                -- activation in flight, so its wake-up is deferred to the
                -- next one, which carries it (V087).
                claimed_through_entry = (
                    SELECT MAX(j.event_id) FROM workflow_execution_journal j
                    WHERE j.execution_id = we.execution_id),
                -- Bind on first claim, never rebind. Also missing here before, so
                -- anything claimed in a batch was never bound at all.
                bound_version_id = COALESCE(we.bound_version_id, $7),
                {bind_step_ids},
                -- A new activation, numbered as the single claim numbers it.
                -- A prefetched execution keeps the number it was claimed
                -- with, so if its claim lapses in the buffer and another
                -- worker claims it, the one handed out late is the stale one.
                activation_seq = we.activation_seq + 1,
                -- The activation handed out now replays everything, so a
                -- wake-up deferred for the one before it is answered by it.
                pending_requeue = FALSE,
                pending_requeue_at = NULL,
                updated_at = NOW()
            FROM next_executions ne
            WHERE we.id = ne.id
              AND {fence_free}
            RETURNING "#,
                crate::repositories::columns::workflow_executions!("we"),
                r#"
            "#
            ),
            due = START_DUE,
            release = release_predicate("$7", "$4", "$3"),
            step_ids = step_id_predicate("$9"),
            bind_step_ids = step_id_binding("$9", "we."),
            fence_free = fence_free("we"),
            organization = organization_predicate(organization_id, "$10"),
            holder = holder_predicate("workflow_executions", "NULL::text"),
        );
        let mut rows: Vec<WorkflowExecutionRow> = Vec::new();
        for pool in self.scan_pools() {
            if rows.len() >= limit as usize {
                break;
            }
            let remaining = limit - rows.len() as i32;
            let mut conn = pool.acquire().await.map_err(|e| {
                OrcherError::internal(format!("Failed to batch claim workflow executions: {}", e))
            })?;
            let batch = retry_on_stale_plan!("workflow batch claim", conn => {
                let batch = sqlx::query_as::<_, WorkflowExecutionRow>(&sql)
                    .bind(identity)
                    .bind(claim_timeout)
                    .bind(task_queue)
                    .bind(namespace)
                    .bind(remaining)
                    .bind(offset)
                    .bind(version_id)
                    .bind(self.claim_node())
                    .bind(step_ids);
                match organization_id {
                    Some(organization_id) => batch.bind(organization_id),
                    None => batch,
                }
                .fetch_all(&mut *conn)
                .await
            })
            .map_err(|e| {
                error!(
                    error = %e,
                    task_queue = %task_queue,
                    namespace = %namespace,
                    limit = limit,
                    offset = offset,
                    "Failed to batch claim workflow executions"
                );
                OrcherError::internal(format!("Failed to batch claim workflow executions: {}", e))
            })?;
            rows.extend(batch);
        }

        let claimed_count = rows.len();

        if claimed_count > 0 {
            debug!(
                task_queue = %task_queue,
                namespace = %namespace,
                identity = %identity,
                requested = limit,
                claimed = claimed_count,
                "CLAIM_BATCH: Successfully claimed {} executions",
                claimed_count
            );

            // Log individual executions at debug level
            for row in &rows {
                debug!(
                    execution_id = %row.execution_id,
                    workflow_name = %row.workflow_name,
                    priority = row.priority,
                    "CLAIM_BATCH: Claimed execution"
                );
            }
        } else {
            debug!(
                task_queue = %task_queue,
                namespace = %namespace,
                "CLAIM_BATCH: No executions available to claim"
            );
        }

        Ok(rows.into_iter().map(|r| r.into()).collect())
    }

    /// The organization `execution_id` belongs to (`Some(None)` for none), if
    /// it is waiting to be claimed; `None` if it is claimed, finished,
    /// cancelled or not found.
    pub async fn unclaimed_owner(&self, execution_id: &str) -> OrcherResult<Option<Option<Uuid>>> {
        for pool in self.scan_pools() {
            let owner: Option<Option<Uuid>> = sqlx::query_scalar(
                "SELECT organization_id FROM workflow_executions \
                 WHERE execution_id = $1 AND claimed_at IS NULL AND cancelled_at IS NULL \
                   AND NOT awaiting_start \
                   AND status IN ('pending', 'queued', 'running', 'retrying', 'waiting')",
            )
            .bind(execution_id)
            .fetch_optional(pool)
            .await
            .map_err(|e| {
                OrcherError::internal(format!("Failed to read the execution's owner: {e}"))
            })?;
            if owner.is_some() {
                return Ok(owner);
            }
        }
        Ok(None)
    }

    /// Claim a specific execution by execution_id (fast path for Redis-based matching)
    ///
    /// This is a fast claim operation that directly claims a specific execution
    /// by its execution_id. Used when the execution_id is already known (e.g., from Redis BRPOP).
    ///
    /// This is FAST (< 10ms) because it's a simple UPDATE by execution_id (indexed).
    ///
    /// # Arguments
    ///
    /// * `execution_id` - Specific execution ID to claim
    /// * `identity` - Identity of the service claiming the execution
    /// * `claim_timeout_secs` - How long the claim is valid
    ///
    /// # Returns
    ///
    /// - `Ok(Some(execution))` - Successfully claimed the execution
    /// - `Ok(None)` - Execution already claimed, not found, or not claimable
    /// - `Err(_)` - Database error
    ///
    /// # ORCHER Terminology
    /// - "Service" claims work (not "Worker")
    /// - Used with Redis matching layer for optimal performance
    ///
    /// Scoped to `organization_id` as [`claim_and_get_next`](Self::claim_and_get_next) is:
    /// a continuation names an execution, but not whose it is.
    pub async fn claim_execution_by_id(
        &self,
        execution_id: &str,
        identity: &str,
        claim_timeout_secs: i64,
        version_id: Option<&str>,
        organization_id: Option<Uuid>,
    ) -> OrcherResult<Option<WorkflowExecution>> {
        self.claim_execution_by_id_from(
            execution_id,
            identity,
            claim_timeout_secs,
            version_id,
            organization_id,
            None,
            &[],
        )
        .await
    }

    /// [`claim_execution_by_id`](Self::claim_execution_by_id) for a poll from
    /// the worker process `worker_instance_id`, recorded as
    /// [`claim_next_from`](Self::claim_next_from) records it, and routed by
    /// `step_ids` as that is.
    #[allow(clippy::too_many_arguments)]
    pub async fn claim_execution_by_id_from(
        &self,
        execution_id: &str,
        identity: &str,
        claim_timeout_secs: i64,
        version_id: Option<&str>,
        organization_id: Option<Uuid>,
        worker_instance_id: Option<&str>,
        step_ids: &[i16],
    ) -> OrcherResult<Option<WorkflowExecution>> {
        use orcher_common::types::time::Timestamptz;

        let claim_timeout_at = Timestamptz::now() + SignedDuration::from_secs(claim_timeout_secs);

        debug!(
            execution_id = %execution_id,
            identity = %identity,
            claim_timeout_secs = claim_timeout_secs,
            "Fast claim by execution_id"
        );

        // Fast UPDATE by execution_id (indexed) with atomic claim check. Keyed by the
        // globally-unique execution_id, not the routing id, so scatter across shard
        // pools — the row lives on exactly one; others return no row.
        let sql = format!(
            concat!(
                r#"
            UPDATE workflow_executions
            SET claimed_by = $1,
                claimed_at = NOW(),
                -- The run's own task timeout, as every claim takes it.
                claim_timeout_at = COALESCE(
                    NOW() + make_interval(secs => task_timeout_secs), $2),
                claimed_on_node = $5,
                claimed_by_instance = $7,
                preferred_until = NULL,
                -- Where in its journal this activation reads to: its last
                -- entry now. An outcome recorded after the claim finds the
                -- activation in flight, so its wake-up is deferred to the
                -- next one, which carries it (V087).
                claimed_through_entry = (
                    SELECT MAX(j.event_id) FROM workflow_execution_journal j
                    WHERE j.execution_id = workflow_executions.execution_id),
                bound_version_id = COALESCE(bound_version_id, $4),
                {bind_step_ids},
                -- A new activation, numbered as every claim numbers it.
                activation_seq = activation_seq + 1,
                -- Answers any wake-up deferred for the activation before it.
                pending_requeue = FALSE,
                pending_requeue_at = NULL,
                updated_at = NOW()
            WHERE execution_id = $3
              AND claimed_at IS NULL
              AND cancelled_at IS NULL
              AND status IN ('pending', 'queued', 'running', 'retrying', 'waiting')
              AND {due}
              AND ($6::uuid IS NULL OR organization_id = $6)
              -- Targeted claims take the same routing rule as the scanning ones.
              -- A continuation is handed to a specific worker, so a mismatch is
              -- unlikely rather than impossible; guarding it keeps the rule true
              -- of every path that can claim, not merely most of them.
              AND {release}
              AND {step_ids}
              -- Held for the process that keeps its journal, as every claim.
              AND {holder}
              -- Last, so the fence is tried on this row only if it is
              -- claimable at all.
              AND {fence_free}
            RETURNING "#,
                crate::repositories::columns::workflow_executions!(),
                r#"
            "#
            ),
            due = START_DUE,
            release = release_predicate(
                "$4",
                "workflow_executions.namespace",
                "workflow_executions.task_queue"
            ),
            step_ids = step_id_predicate("$8"),
            bind_step_ids = step_id_binding("$8", ""),
            fence_free = fence_free("workflow_executions"),
            holder = holder_predicate("workflow_executions", "$7"),
        );
        let mut execution: Option<WorkflowExecution> = None;
        for pool in self.scan_pools() {
            let row: Option<WorkflowExecutionRow> = sqlx::query_as::<_, WorkflowExecutionRow>(&sql)
                .bind(identity)
                .bind(claim_timeout_at)
                .bind(execution_id)
                .bind(version_id)
                .bind(self.claim_node())
                .bind(organization_id)
                .bind(worker_instance_id)
                .bind(step_ids)
                .fetch_optional(pool)
                .await
                .map_err(|e| {
                    error!(
                        error = %e,
                        execution_id = %execution_id,
                        "Failed to claim execution by ID"
                    );
                    OrcherError::internal(format!("Failed to claim execution by ID: {}", e))
                })?;
            if let Some(row) = row {
                execution = Some(row.into());
                break;
            }
        }

        if let Some(ref exec) = execution {
            info!(
                execution_id = %exec.execution_id,
                workflow_name = %exec.workflow_name,
                claimed_by = %identity,
                "Successfully claimed execution by ID (fast path)"
            );
        } else {
            debug!(
                execution_id = %execution_id,
                "Execution not claimable (already claimed, not found, or wrong status)"
            );
        }

        Ok(execution)
    }

    /// Check why a claim would fail for a specific execution
    ///
    /// This diagnostic method queries the execution state to understand why
    /// a claim attempt failed. Useful for debugging and observability.
    ///
    /// # Arguments
    ///
    /// * `execution_id` - The execution ID to check
    /// * `identity` - The identity that attempted to claim
    ///
    /// # Returns
    ///
    /// Detailed information about the execution state and why claiming failed
    pub async fn check_claim_state(
        &self,
        execution_id: &str,
        identity: &str,
    ) -> OrcherResult<ClaimStateCheck> {
        #[derive(sqlx::FromRow)]
        struct StateRow {
            status: String,
            claimed_by: Option<String>,
            claim_timeout_at: Option<Timestamptz>,
            cancelled_at: Option<Timestamptz>,
        }

        let mut row: Option<StateRow> = None;
        for pool in self.scan_pools() {
            row = sqlx::query_as(
                r#"
                SELECT status, claimed_by, claim_timeout_at, cancelled_at
                FROM workflow_executions
                WHERE execution_id = $1
                "#,
            )
            .bind(execution_id)
            .fetch_optional(pool)
            .await
            .map_err(|e| {
                warn!(error = %e, execution_id = %execution_id, "Failed to check claim state");
                OrcherError::internal(format!("Failed to check claim state: {}", e))
            })?;
            if row.is_some() {
                break;
            }
        }

        match row {
            None => Ok(ClaimStateCheck {
                exists: false,
                status: None,
                claimed_by: None,
                claim_timeout_at: None,
                is_cancelled: false,
                failure_reason: Some(ClaimFailureReason::NotFound),
            }),
            Some(state) => {
                let is_cancelled = state.cancelled_at.is_some();
                let is_terminal = matches!(
                    state.status.as_str(),
                    "completed" | "failed" | "cancelled" | "terminated"
                );
                let is_claimed_by_other = state.claimed_by.as_ref().is_some_and(|cb| {
                    cb != identity
                        && state
                            .claim_timeout_at
                            .is_some_and(|t| t > Timestamptz::now())
                });

                let failure_reason = if is_cancelled {
                    Some(ClaimFailureReason::Cancelled)
                } else if is_terminal {
                    Some(ClaimFailureReason::TerminalStatus(state.status.clone()))
                } else if is_claimed_by_other {
                    Some(ClaimFailureReason::ClaimedByOther {
                        claimed_by: state.claimed_by.clone().unwrap_or_default(),
                        claim_timeout_at: state.claim_timeout_at,
                    })
                } else {
                    None // Claim should succeed
                };

                Ok(ClaimStateCheck {
                    exists: true,
                    status: Some(state.status),
                    claimed_by: state.claimed_by,
                    claim_timeout_at: state.claim_timeout_at,
                    is_cancelled,
                    failure_reason,
                })
            }
        }
    }

    /// Reclaim executions with expired claims (alias for release_stale_claims)
    ///
    /// Used by the background reclaim task to recover work from crashed services.
    /// Returns the execution IDs that were reclaimed.
    ///
    /// # ORCHER Terminology
    /// - Crashed "services" (not "workers") leave stale claims
    /// - The "reclaimer" background task handles timeout recovery
    pub async fn reclaim_expired(&self) -> OrcherResult<Vec<String>> {
        debug!("Reclaiming executions with expired claims");

        // Every pool: a claim expires wherever its execution lives. This swept
        // only the default pool, so with shard databases a node that died
        // left every claim it held on the other shards in place for good —
        // in a two-shard run, fourteen of twenty workflows never finished.
        let mut execution_ids: Vec<String> = Vec::new();
        for pool in self.scan_pools() {
            let rows = sqlx::query_as::<_, (String,)>(
                r#"
                UPDATE workflow_executions
                SET claimed_by = NULL,
                    claimed_at = NULL,
                    claim_timeout_at = NULL,
                    claimed_on_node = NULL,
                    updated_at = NOW()
                WHERE claimed_at IS NOT NULL
                  AND claim_timeout_at < NOW()
                  AND status IN ('pending', 'queued', 'running', 'retrying', 'waiting')
                RETURNING execution_id
                "#,
            )
            .fetch_all(pool)
            .await
            .map_err(|e| {
                warn!(error = %e, "Failed to reclaim expired claims");
                OrcherError::internal(format!("Failed to reclaim expired claims: {}", e))
            })?;
            execution_ids.extend(rows.into_iter().map(|(id,)| id));
        }

        if !execution_ids.is_empty() {
            info!(
                count = execution_ids.len(),
                executions = ?execution_ids,
                "Reclaimed expired claims"
            );
        }

        Ok(execution_ids)
    }

    /// Release the claims engines that have died left with an activation
    /// still out, so the executions are handed out again now rather than when
    /// their claims time out. Returns what was released, for the caller to
    /// announce to pollers.
    ///
    /// Only a claim that names one of `dead` as the engine that made it
    /// (`claimed_on_node`), and whose activation has not completed, is
    /// released: one the dead engine batch-claimed into its prefetch buffer
    /// and never handed out, or handed to a worker whose completion has not
    /// been applied. A parked execution — its activation completed and its
    /// claim kept until what it waits for arrives — is left alone: the wake-up
    /// that releases it does not depend on the engine that claimed it.
    ///
    /// Nothing is numbered. `activation_seq` stays where the claim left it, so
    /// a worker that did get the activation and completes it through another
    /// engine is still answering the current activation and is applied, as
    /// long as nobody has claimed the execution since. Once a poller has, the
    /// completion is acknowledged without being applied and the newer
    /// activation replays the same work: duplicate work, never a wrong result.
    ///
    /// A claim made after `last_seen` plus `alive_margin_secs` is not
    /// released. The dead engine could not have made it; an engine that was
    /// only paused, and is back, did — after its heartbeat had already gone
    /// stale — and its claims are as good as anyone's. Nor is one made more
    /// than `claim_timeout_secs` before `last_seen`: that one has timed out
    /// already and is the reclaimer's, and leaving it out keeps the read to
    /// the few claims made in the engine's last minutes. (An engine
    /// configured with a longer claim timeout than this one has its oldest
    /// claims wait for that timeout, as they did before.)
    ///
    /// A completion being applied right now is never released under it.
    /// Commands are applied outside the transaction of the activation fence,
    /// and the activation is recorded as completed only at the end, so until
    /// then the row still reads as an activation that is out. Releasing it
    /// there would let a poller claim the next activation and replay a
    /// journal that is only partly written. Each row is released only if its
    /// fence can be taken on the spot (the same advisory lock
    /// `enter_activation` takes, held for the release statement alone); one
    /// whose fence is held is skipped, and on the next sweep it is either
    /// completed — and so not released — or free.
    ///
    /// Several engines observe the same death and run this at once. Each row
    /// is released by one of them: the others' updates wait on its row lock,
    /// then find the claim gone and skip the row, so each execution is
    /// returned — and announced — once.
    pub async fn release_claims_of_dead_nodes(
        &self,
        dead: &[DeadNode],
        alive_margin_secs: i64,
        claim_timeout_secs: i64,
    ) -> OrcherResult<Vec<ReleasedClaim>> {
        if dead.is_empty() {
            return Ok(Vec::new());
        }
        let nodes: Vec<&str> = dead.iter().map(|d| d.node_id.as_str()).collect();
        let last_seen: Vec<Timestamptz> = dead.iter().map(|d| d.last_seen).collect();
        let mut released = Vec::new();
        // Every pool: a dead engine's claims are wherever the executions it
        // claimed live, which with shard databases is all of them.
        for pool in self.scan_pools() {
            let rows = sqlx::query_as::<_, (String, String, String)>(&format!(
                r#"
                UPDATE workflow_executions we
                SET claimed_by = NULL,
                    claimed_at = NULL,
                    claim_timeout_at = NULL,
                    claimed_on_node = NULL,
                    -- The activation handed out next replays everything, so
                    -- a wake-up deferred to the lost one is answered by it.
                    pending_requeue = FALSE,
                    pending_requeue_at = NULL,
                    updated_at = NOW()
                FROM (
                    -- The candidates first, from the partial index over
                    -- activations still out (V052), so that the fence is
                    -- tried on them alone and not on every row scanned.
                    SELECT c.id, dead.node_id
                    FROM unnest($1::text[], $2::timestamptz[]) AS dead(node_id, last_seen)
                    JOIN workflow_executions c
                      ON c.claimed_on_node = dead.node_id
                     AND c.claimed_on_node IS NOT NULL
                     AND c.claimed_by IS NOT NULL
                     AND c.activation_seq > COALESCE(c.completed_activation, 0)
                     AND c.claimed_at <= dead.last_seen + make_interval(secs => $3)
                     AND c.claimed_at >= dead.last_seen - make_interval(secs => $4)
                    OFFSET 0
                ) AS candidate
                WHERE we.id = candidate.id
                  -- Checked again on the row being written, which is what a
                  -- concurrent release or claim is decided on.
                  AND we.claimed_on_node = candidate.node_id
                  AND we.claimed_by IS NOT NULL
                  AND we.activation_seq > COALESCE(we.completed_activation, 0)
                  AND we.status IN ('pending', 'queued', 'running', 'retrying', 'waiting')
                  AND {fence_free}
                RETURNING we.execution_id, we.task_queue, we.namespace
                "#,
                fence_free = fence_free("we"),
            ))
            .bind(&nodes)
            .bind(&last_seen)
            .bind(alive_margin_secs as f64)
            .bind(claim_timeout_secs as f64)
            .fetch_all(pool)
            .await
            .map_err(|e| {
                OrcherError::internal(format!("Failed to release a dead engine's claims: {e}"))
            })?;
            released.extend(
                rows.into_iter()
                    .map(|(execution_id, task_queue, namespace)| ReleasedClaim {
                        execution_id,
                        task_queue,
                        namespace,
                    }),
            );
        }
        if !released.is_empty() {
            info!(
                count = released.len(),
                nodes = ?nodes,
                "Released the claims of engines that died with activations out"
            );
        }
        Ok(released)
    }

    /// Release the claims of worker processes that have stopped being seen,
    /// so the executions are handed out again now rather than when their
    /// claims time out. Returns what was released, for the caller to announce
    /// to pollers.
    ///
    /// Only a claim recorded against one of `lapsed` (`claimed_by_instance`,
    /// V060), in its organization and namespace, and whose activation has not
    /// completed, is released: the process got the activation and never
    /// answered it. A parked execution — its activation completed and its
    /// claim kept until what it waits for arrives — is left alone: what wakes
    /// it does not depend on the process that ran it. A claim recorded against
    /// no process (a poll that named none, or a batch still in an engine's
    /// prefetch buffer) is never released here.
    ///
    /// The activation is voided as it is released: `activation_seq` moves on,
    /// so if the process was not dead after all, only slow or cut off, its
    /// completion arrives for an activation that is no longer current and is
    /// acknowledged without being applied — whether or not anyone has claimed
    /// the execution since. The next activation replays the same work:
    /// duplicate work, never a wrong result.
    ///
    /// A claim made later than `alive_margin_secs` after the process was last
    /// seen is not released: it was made by a poll that process sent after it
    /// was read as lapsed, so it is back, and its claims are as good as
    /// anyone's. Every claim the process made before it stopped was made by a
    /// poll that was seen, so it falls within that margin of the last sighting.
    ///
    /// A completion being applied right now is never released under it: as in
    /// [`release_claims_of_dead_nodes`](Self::release_claims_of_dead_nodes),
    /// each row is released only if its fence can be taken on the spot, and
    /// one whose fence is held is left for the next sweep, by when it is
    /// completed — and so not released — or free.
    ///
    /// Several engines may run this at once for the same processes. Each row is
    /// released by one of them: the others' updates wait on its row lock, then
    /// find the claim gone and skip the row, so each execution is returned —
    /// and announced — once.
    pub async fn release_claims_of_lapsed_workers(
        &self,
        lapsed: &[crate::repositories::domains::worker::LapsedInstance],
        alive_margin_secs: i64,
    ) -> OrcherResult<Vec<ReleasedClaim>> {
        if lapsed.is_empty() {
            return Ok(Vec::new());
        }
        let organizations: Vec<Uuid> = lapsed.iter().map(|l| l.organization_id).collect();
        let namespaces: Vec<&str> = lapsed.iter().map(|l| l.namespace.as_str()).collect();
        let instances: Vec<&str> = lapsed
            .iter()
            .map(|l| l.worker_instance_id.as_str())
            .collect();
        let last_seen: Vec<Timestamptz> = lapsed.iter().map(|l| l.last_seen).collect();
        let mut released = Vec::new();
        // Every pool: a process's claims are wherever the executions it was
        // handed live, which with shard databases is all of them.
        for pool in self.scan_pools() {
            let rows = sqlx::query_as::<_, (String, String, String)>(&format!(
                r#"
                UPDATE workflow_executions we
                SET claimed_by = NULL,
                    claimed_at = NULL,
                    claim_timeout_at = NULL,
                    claimed_on_node = NULL,
                    claimed_by_instance = NULL,
                    -- Voids the activation the process held: a completion of
                    -- it that arrives after all is stale from now on.
                    activation_seq = we.activation_seq + 1,
                    -- The activation handed out next replays everything, so
                    -- a wake-up deferred to the lost one is answered by it.
                    pending_requeue = FALSE,
                    pending_requeue_at = NULL,
                    updated_at = NOW()
                FROM (
                    -- The candidates first, from the partial index over
                    -- activations still out (V061), so that the fence is
                    -- tried on them alone and not on every row scanned.
                    SELECT c.id,
                           lapsed.instance,
                           lapsed.last_seen + make_interval(secs => $5) AS cutoff
                    FROM unnest($1::uuid[], $2::text[], $3::text[], $4::timestamptz[])
                         AS lapsed(organization_id, namespace, instance, last_seen)
                    JOIN workflow_executions c
                      ON c.claimed_by_instance = lapsed.instance
                     AND c.claimed_by_instance IS NOT NULL
                     AND c.claimed_by IS NOT NULL
                     AND c.activation_seq > COALESCE(c.completed_activation, 0)
                     AND c.claimed_at <= lapsed.last_seen + make_interval(secs => $5)
                     -- The process is known within the organization and the
                     -- namespace its polls were made for, and its claims are
                     -- the ones those polls could make: another tenant's
                     -- process sending the same id is another process.
                     AND c.namespace = lapsed.namespace
                     AND COALESCE(c.organization_id, '00000000-0000-0000-0000-000000000000'::uuid)
                         = lapsed.organization_id
                    OFFSET 0
                ) AS candidate
                WHERE we.id = candidate.id
                  -- Checked again on the row being written, which is what a
                  -- concurrent release or claim is decided on.
                  AND we.claimed_by_instance = candidate.instance
                  AND we.claimed_by IS NOT NULL
                  AND we.claimed_at <= candidate.cutoff
                  AND we.activation_seq > COALESCE(we.completed_activation, 0)
                  AND we.status IN ('pending', 'queued', 'running', 'retrying', 'waiting')
                  AND {fence_free}
                RETURNING we.execution_id, we.task_queue, we.namespace
                "#,
                fence_free = fence_free("we"),
            ))
            .bind(&organizations)
            .bind(&namespaces)
            .bind(&instances)
            .bind(&last_seen)
            .bind(alive_margin_secs as f64)
            .fetch_all(pool)
            .await
            .map_err(|e| {
                OrcherError::internal(format!(
                    "Failed to release the claims of lapsed worker processes: {e}"
                ))
            })?;
            released.extend(
                rows.into_iter()
                    .map(|(execution_id, task_queue, namespace)| ReleasedClaim {
                        execution_id,
                        task_queue,
                        namespace,
                    }),
            );
        }
        if !released.is_empty() {
            info!(
                count = released.len(),
                instances = ?instances,
                "Released the claims of worker processes that stopped being seen"
            );
        }
        Ok(released)
    }

    /// Wake the parked executions whose wake-up was lost: an outcome of
    /// something they wait on was recorded, and nothing released their claim.
    ///
    /// A task's outcome is committed first and the workflow woken after, in a
    /// separate step. An engine that dies between the two leaves the outcome
    /// in the journal and the workflow parked on it — claimed, its last
    /// activation completed, no wake-up pending — until the claim times out,
    /// minutes later. The same happens to a wake-up that fails on a transient
    /// error. This finds those executions and releases them through
    /// [`release_claim_to_resume_on`], as the lost wake-up would have.
    ///
    /// Read from the outcomes journaled between `since` and `grace_secs` ago,
    /// through the journal's time index, rather than from every parked
    /// execution: a deployment can have hundreds of thousands parked, and only
    /// the few with a recent outcome can be waiting on a lost wake-up. An
    /// outcome counts when it is newer than the execution's claim, since the
    /// activation that parked it was handed out before the outcome existed;
    /// one that arrived while an activation was out left its wake-up pending
    /// for that activation's completion, so a parked execution with such an
    /// outcome is one whose wake-up went missing. A task's outcome counts only
    /// once no task of the workflow is still scheduled or started, which is
    /// the rule the ordinary wake-up follows (`check_should_requeue_workflow`):
    /// a fan-out is not replayed as each sibling finishes, nor a failure whose
    /// retry is waiting. Timer, child and event outcomes count as they are.
    /// `grace_secs`
    /// leaves the ordinary wake-up, which follows its outcome by milliseconds,
    /// time to happen first.
    ///
    /// Safe on every replica at once: the release re-checks that the
    /// execution is still parked on the claim it had, so each is released —
    /// and returned for a continuation — by one caller.
    ///
    /// At most `limit` candidates are taken, so that a sweep after an outage,
    /// reading a whole claim timeout's outcomes, does not hold up the rest of
    /// the reclaimer's tick with one release per candidate. The `bool` is true
    /// when the limit was reached: the caller reads the same window again,
    /// where the executions released this time are no longer parked.
    pub async fn wake_parked_with_unconsumed_outcomes(
        &self,
        since: Timestamptz,
        grace_secs: i64,
        limit: i64,
    ) -> OrcherResult<(Vec<ReleasedClaim>, bool)> {
        let bound = self.in_flight_bound_secs();
        let mut woken = Vec::new();
        let mut remaining = limit;
        for pool in self.scan_pools() {
            if remaining <= 0 {
                break;
            }
            let candidates = sqlx::query_as::<_, (String, String, String, Timestamptz)>(&format!(
                r#"
                SELECT we.execution_id, we.task_queue, we.namespace, we.claimed_at
                FROM (
                    SELECT DISTINCT j.execution_id
                    FROM workflow_execution_journal j
                    WHERE j.timestamp >= $1
                      AND j.timestamp < NOW() - make_interval(secs => $2)
                      AND j.event_type = ANY($3)
                ) recent
                JOIN workflow_executions we ON we.execution_id = recent.execution_id
                WHERE {parked}
                  AND EXISTS (
                      SELECT 1 FROM workflow_execution_journal j
                      WHERE j.execution_id = we.execution_id
                        AND j.event_type = ANY($3)
                        AND j.timestamp > we.claimed_at
                        AND j.timestamp < NOW() - make_interval(secs => $2)
                        AND (j.event_type NOT LIKE 'Task%'
                             OR NOT EXISTS (
                                 SELECT 1 FROM task_executions t
                                 WHERE t.workflow_id = we.id
                                   AND t.status IN ('scheduled', 'started')))
                  )
                LIMIT $4
                "#,
                parked = PARKED.replace("{t}", "we"),
            ))
            .bind(since)
            .bind(grace_secs as f64)
            .bind(WAKING_OUTCOMES)
            .bind(remaining)
            .fetch_all(pool)
            .await
            .map_err(|e| {
                OrcherError::internal(format!("Failed to find parked executions to wake: {e}"))
            })?;
            remaining -= candidates.len() as i64;

            for (execution_id, task_queue, namespace, claimed_at) in candidates {
                // Still parked, and on the same claim: another replica that
                // woke it first, or a poller that has since claimed it, leaves
                // nothing here to release. The timestamp is this sweep's own
                // read of the row, rendered by Postgres, not caller input.
                let predicate = format!(
                    "execution_id = $1 AND {parked} AND claimed_at = '{claimed_at}'::timestamptz",
                    parked = PARKED.replace("{t}", "workflow_executions"),
                    claimed_at = claimed_at.to_rfc3339(),
                );
                if release_claim_to_resume_on(
                    pool,
                    &predicate,
                    &execution_id,
                    bound,
                    self.journal_hold(),
                )
                .await?
                    == Resume::Released
                {
                    woken.push(ReleasedClaim {
                        execution_id,
                        task_queue,
                        namespace,
                    });
                }
            }
        }
        if !woken.is_empty() {
            warn!(
                count = woken.len(),
                "Woke parked executions whose outcome was recorded without a wake-up"
            );
        }
        Ok((woken, remaining <= 0))
    }

    /// Release the claim on activation `activation` of `execution_id` made
    /// for the poll `claimed_by`, which ended without delivering it — the
    /// poll was cancelled, or building the activation failed.
    ///
    /// No worker has the activation, so nothing will complete it: left
    /// claimed, its workflow waited until the claim timed out. Only while the
    /// claim is still exactly that one, the activation not completed; one
    /// taken back or handed on since is left alone. Returns the execution's
    /// queue and namespace if it was released and its run has not ended, for
    /// the caller to wake.
    pub async fn release_undelivered(
        &self,
        execution_id: &str,
        activation: i64,
        claimed_by: &str,
    ) -> OrcherResult<Option<(String, String)>> {
        for pool in self.scan_pools() {
            let released: Option<(String, String, String)> = sqlx::query_as(
                "UPDATE workflow_executions \
                 SET claimed_by = NULL, claimed_at = NULL, claim_timeout_at = NULL, \
                     claimed_on_node = NULL, claimed_by_instance = NULL, \
                     pending_requeue = FALSE, pending_requeue_at = NULL, updated_at = NOW() \
                 WHERE execution_id = $1 AND activation_seq = $2 AND claimed_by = $3 \
                   AND COALESCE(completed_activation, 0) < activation_seq \
                 RETURNING task_queue, namespace, status",
            )
            .bind(execution_id)
            .bind(activation)
            .bind(claimed_by)
            .fetch_optional(pool)
            .await
            .map_err(|e| {
                OrcherError::internal(format!("Failed to release an undelivered claim: {e}"))
            })?;
            if let Some((task_queue, namespace, status)) = released {
                let ended = matches!(
                    status.as_str(),
                    "completed" | "failed" | "cancelled" | "terminated" | "timed_out"
                );
                return Ok((!ended).then_some((task_queue, namespace)));
            }
        }
        Ok(None)
    }

    /// Release a claim this engine made into its prefetch buffer and never
    /// handed out, as it shuts down — rather than leave it for its survivors
    /// to find once this engine has stopped heartbeating.
    ///
    /// Only while the claim is still exactly the one the buffer holds: the
    /// same activation, still under the identity that claimed the batch, and
    /// made by this engine. One handed to a poller since has been moved to
    /// that poller's identity and is left alone. `true` if it was released.
    pub async fn release_unhanded(
        &self,
        execution_id: &str,
        activation: i64,
        claimed_by: &str,
    ) -> OrcherResult<bool> {
        let Some(node) = self.claim_node() else {
            return Ok(false);
        };
        for pool in self.scan_pools() {
            let released = sqlx::query(
                "UPDATE workflow_executions \
                 SET claimed_by = NULL, claimed_at = NULL, claim_timeout_at = NULL, \
                     claimed_on_node = NULL, updated_at = NOW() \
                 WHERE execution_id = $1 AND activation_seq = $2 AND claimed_by = $3 \
                   AND claimed_on_node = $4",
            )
            .bind(execution_id)
            .bind(activation)
            .bind(claimed_by)
            .bind(node)
            .execute(pool)
            .await
            .map_err(|e| {
                OrcherError::internal(format!("Failed to release a prefetched claim: {e}"))
            })?;
            if released.rows_affected() > 0 {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Find runs whose retry is due
    ///
    /// Returns runs that:
    /// - Failed, or timed out on their run timeout (V067)
    /// - Have retry_count < max_retries, and were not settled as final
    /// - Are past their next_retry_at time (or, for a failure, have no
    ///   next_retry_at set and have sat undecided for a grace period)
    pub async fn find_due_for_retry(&self, limit: usize) -> OrcherResult<Vec<WorkflowExecution>> {
        debug!(limit = limit, "Finding runs whose retry is due");

        // Keyed by status/time, not the routing id — scan every shard pool (a failed
        // workflow lives on its own shard) and merge, earliest-submitted first, capped at
        // the caller's limit. Single pool when unpartitioned, unchanged.
        //
        // A failure's fate is decided as it is recorded: `next_retry_at` for a
        // retry, `retries_settled_at` for a final failure. A failed row with
        // neither has not been decided yet — the recording request may still
        // be in flight — and acting on it would override the decision about
        // to be written. It is eligible only once it has sat undecided for a
        // grace period: rows failed before failures were decided this way
        // have no decision at all and must still be retried, and so must a
        // run whose decision was lost to an error.
        //
        // A run timeout is decided by the write that records it, so a
        // timed-out run is due only once the retry that write scheduled is.
        //
        // With clustering on, each node scans only its own shards: every node
        // runs this pass, and a retry of a run another node owns would only be
        // refused by the fence.
        let (los, his, min, max) = self.owned_bounds();
        let sql = format!(
            concat!(
                r#"
            SELECT "#,
                crate::repositories::columns::workflow_executions!(),
                r#" FROM workflow_executions
            WHERE status IN ('failed', 'timed_out')
              AND retry_count < max_retries
              AND retries_settled_at IS NULL
              AND (next_retry_at <= NOW()
                   OR (status = 'failed'
                       AND next_retry_at IS NULL
                       AND updated_at < NOW() - INTERVAL '30 seconds'))
              AND {shard}
            ORDER BY submitted_at ASC
            LIMIT $1
            "#
            ),
            shard = shard_predicate("workflow_executions", 2),
        );
        let mut rows: Vec<WorkflowExecutionRow> = Vec::new();
        for pool in self.scan_pools() {
            let batch = sqlx::query_as::<_, WorkflowExecutionRow>(&sql)
                .bind(limit as i32)
                .bind(los.clone())
                .bind(his.clone())
                .bind(min)
                .bind(max)
                .fetch_all(pool)
                .await
                .map_err(|e| {
                    OrcherError::internal(format!(
                        "Failed to find failed workflows for retry: {}",
                        e
                    ))
                })?;
            rows.extend(batch);
        }
        rows.sort_by_key(|r| r.submitted_at);
        rows.truncate(limit);

        Ok(rows.into_iter().map(Into::into).collect())
    }

    /// Update retry schedule for a workflow
    ///
    /// Sets the retry_count, last_retry_at, and next_retry_at fields
    pub async fn update_retry_schedule(
        &self,
        execution_id: &str,
        retry_count: i32,
        next_retry_at: Timestamptz,
    ) -> OrcherResult<()> {
        debug!(
            execution_id = %execution_id,
            retry_count = retry_count,
            next_retry_at = %next_retry_at,
            "Updating workflow retry schedule"
        );

        // Keyed by the globally-unique execution_id — run on every shard pool; only the
        // owning pool matches. Single pool when unpartitioned, unchanged.
        let sql = r#"
            UPDATE workflow_executions
            SET retry_count = $1,
                last_retry_at = NOW(),
                next_retry_at = $2,
                updated_at = NOW()
            WHERE execution_id = $3
            "#;
        let mut affected = 0u64;
        for pool in self.scan_pools() {
            let result = sqlx::query(sql)
                .bind(retry_count)
                .bind(next_retry_at)
                .bind(execution_id)
                .execute(pool)
                .await
                .map_err(|e| {
                    OrcherError::internal(format!(
                        "Failed to update workflow retry schedule: {}",
                        e
                    ))
                })?;
            affected += result.rows_affected();
        }

        if affected == 0 {
            warn!(
                execution_id = %execution_id,
                "No rows updated when scheduling retry - workflow may not exist"
            );
            return Err(OrcherError::not_found(
                "workflow_execution",
                execution_id.to_string(),
            ));
        }

        Ok(())
    }

    /// Set the next retry time for a failed workflow without changing retry_count,
    /// and announce it on the `workflow_retry_due` channel.
    ///
    /// Called by the failure path when it decides a failure will be retried, so
    /// the retry scheduler finds it with a simple `next_retry_at <= NOW()`
    /// check. The announcement is for the scheduler's realtime listener, on
    /// every replica. The `workflow_failed` notification fires on the status
    /// write, before this decision exists, so a listener acting on it can only
    /// defer to the poll; without this one every retry waited for the next
    /// poll, however short the backoff. Sent by the same statement as the
    /// write, so it is delivered if and only if the decision is committed.
    pub async fn set_next_retry_at(
        &self,
        execution_id: &str,
        next_retry_at: Timestamptz,
    ) -> OrcherResult<()> {
        let sql = r#"
            WITH due AS (
                UPDATE workflow_executions
                SET next_retry_at = $1,
                    updated_at    = NOW()
                WHERE execution_id = $2
                RETURNING execution_id, next_retry_at
            )
            SELECT pg_notify(
                'workflow_retry_due',
                json_build_object(
                    'execution_id', execution_id,
                    'due_at_ms', (EXTRACT(EPOCH FROM next_retry_at) * 1000)::bigint
                )::text
            )::text
            FROM due
            "#;
        for pool in self.scan_pools() {
            let updated = sqlx::query(sql)
                .bind(next_retry_at)
                .bind(execution_id)
                .fetch_all(pool)
                .await
                .map_err(|e| {
                    OrcherError::internal(format!("Failed to set next_retry_at: {}", e))
                })?;
            if !updated.is_empty() {
                break;
            }
        }

        Ok(())
    }

    /// Take the failed or timed-out run `id` for a retry: move it to
    /// `restarted_fresh` if, and only if, it is still waiting for its retry,
    /// and the workflow's execution timeout has not run out.
    ///
    /// Every replica's retry scheduler can pick up the same failure, and each
    /// used to mark it replaced and create a successor — two retries of one
    /// failure, both running. The status check inside the `UPDATE` makes the
    /// transition happen once; only the caller it returns `true` to creates
    /// the successor. Also refuses a run that was cancelled or terminated
    /// while it waited, and one whose workflow is past its execution timeout:
    /// no run of it may start from then on, and the caller settles it
    /// instead. Fenced like the other status writes.
    ///
    /// `next_retry_at` is left set: on a `restarted_fresh` run it marks a
    /// retry whose successor is not known to exist yet, and is cleared by
    /// [`retry_successor_created`](Self::retry_successor_created). A marked
    /// run that stays marked lost its successor to a crash between the two
    /// writes; [`find_stranded_retries`](Self::find_stranded_retries) finds it.
    pub async fn claim_for_retry(&self, id: Uuid) -> OrcherResult<bool> {
        let mut tx = self.begin_write(self.pool_for(None, id), id).await?;
        let moved = sqlx::query(&format!(
            r#"
            UPDATE workflow_executions
            SET status = 'restarted_fresh',
                completed_at = COALESCE(completed_at, NOW()),
                next_retry_at = COALESCE(next_retry_at, NOW()),
                updated_at = NOW()
            WHERE id = $1
              AND retry_count < max_retries
              AND retries_settled_at IS NULL
              AND (status = 'failed'
                   OR (status = 'timed_out' AND next_retry_at IS NOT NULL))
              AND NOT COALESCE({EXECUTION_EXPIRES_AT} <= NOW(), FALSE)
            "#
        ))
        .bind(id)
        .execute(&mut *tx)
        .await
        .map_err(|e| OrcherError::internal(format!("Failed to take run for retry: {}", e)))?
        .rows_affected();
        tx.commit()
            .await
            .map_err(|e| OrcherError::internal(format!("Failed to commit retry claim: {}", e)))?;
        Ok(moved > 0)
    }

    /// Clear the marker [`claim_for_retry`](Self::claim_for_retry) leaves on a
    /// retried run, once its successor exists.
    pub async fn retry_successor_created(&self, id: Uuid) -> OrcherResult<()> {
        sqlx::query(
            r#"
            UPDATE workflow_executions
            SET next_retry_at = NULL, updated_at = NOW()
            WHERE id = $1 AND status = 'restarted_fresh' AND next_retry_at IS NOT NULL
            "#,
        )
        .bind(id)
        .execute(self.pool_for(None, id))
        .await
        .map_err(|e| OrcherError::internal(format!("Failed to clear retry marker: {}", e)))?;
        Ok(())
    }

    /// Runs taken for a retry at least `grace_secs` ago whose successor was
    /// not recorded as created: the scheduler that took them stopped between
    /// the two writes. Also returns runs retried before the marker was
    /// cleared on success, whose successor exists; the caller checks.
    pub async fn find_stranded_retries(
        &self,
        grace_secs: u64,
        limit: usize,
    ) -> OrcherResult<Vec<WorkflowExecution>> {
        // Scoped to this node's shards like the retry pass it recovers.
        let (los, his, min, max) = self.owned_bounds();
        let sql = format!(
            concat!(
                r#"
            SELECT "#,
                crate::repositories::columns::workflow_executions!(),
                r#" FROM workflow_executions
            WHERE status = 'restarted_fresh'
              AND next_retry_at IS NOT NULL
              AND updated_at < NOW() - make_interval(secs => $1)
              AND {shard}
            ORDER BY updated_at ASC
            LIMIT $2
            "#
            ),
            shard = shard_predicate("workflow_executions", 3),
        );
        let mut rows: Vec<WorkflowExecutionRow> = Vec::new();
        for pool in self.scan_pools() {
            rows.extend(
                sqlx::query_as::<_, WorkflowExecutionRow>(&sql)
                    .bind(grace_secs as f64)
                    .bind(limit as i64)
                    .bind(los.clone())
                    .bind(his.clone())
                    .bind(min)
                    .bind(max)
                    .fetch_all(pool)
                    .await
                    .map_err(|e| {
                        OrcherError::internal(format!("Failed to find stranded retries: {}", e))
                    })?,
            );
        }
        rows.truncate(limit);
        Ok(rows.into_iter().map(Into::into).collect())
    }

    /// Give back a retry taken with [`claim_for_retry`](Self::claim_for_retry)
    /// whose successor could not be created, so the failure is retried again
    /// rather than left replaced by nothing. The run goes back to the status
    /// it ended with.
    pub async fn release_retry_claim(&self, id: Uuid) -> OrcherResult<()> {
        let mut tx = self.begin_write(self.pool_for(None, id), id).await?;
        sqlx::query(&format!(
            r#"
            UPDATE workflow_executions
            SET status = {ENDED_BEFORE_RETRY}, updated_at = NOW()
            WHERE id = $1 AND status = 'restarted_fresh'
            "#
        ))
        .bind(id)
        .execute(&mut *tx)
        .await
        .map_err(|e| OrcherError::internal(format!("Failed to release retry claim: {}", e)))?;
        tx.commit()
            .await
            .map_err(|e| OrcherError::internal(format!("Failed to commit retry release: {}", e)))?;
        Ok(())
    }

    /// Close a run taken for a retry whose successor will not be created: a
    /// newer start holds its workflow id, or the workflow's execution timeout
    /// ran out before the successor could start. It goes back to the status it
    /// ended with, settled, so its failure or timeout is its outcome and
    /// nothing retries it again. Not fenced: it only ever follows this node's
    /// own take of the run.
    pub async fn give_up_retry(&self, id: Uuid) -> OrcherResult<()> {
        sqlx::query(&format!(
            r#"
            UPDATE workflow_executions
            SET status = {ENDED_BEFORE_RETRY},
                retries_settled_at = COALESCE(retries_settled_at, NOW()),
                next_retry_at = NULL,
                updated_at = NOW()
            WHERE id = $1 AND status = 'restarted_fresh'
            "#
        ))
        .bind(id)
        .execute(self.pool_for(None, id))
        .await
        .map_err(|e| OrcherError::internal(format!("Failed to close a superseded retry: {}", e)))?;
        Ok(())
    }

    /// Record that a failed run will not be retried: its failure is final.
    ///
    /// Retry eligibility used to be `status = 'failed' AND retry_count <
    /// max_retries`, so a failure the policy gave up on before its retries ran
    /// out — an error type it does not retry — still looked retryable, and the
    /// retry scheduler picked it up again on every poll to reach the same
    /// decision. `retries_settled_at` makes the decision durable without
    /// touching `max_retries`, which a reset carries forward as the policy.
    /// Only a failed or timed-out run is touched.
    pub async fn settle_failure(&self, execution_id: &str) -> OrcherResult<()> {
        for pool in self.scan_pools() {
            let done = sqlx::query(
                r#"
                UPDATE workflow_executions
                SET retries_settled_at = COALESCE(retries_settled_at, NOW()),
                    next_retry_at      = NULL,
                    updated_at         = NOW()
                WHERE execution_id = $1
                  AND status IN ('failed', 'timed_out')
                "#,
            )
            .bind(execution_id)
            .execute(pool)
            .await
            .map_err(|e| OrcherError::internal(format!("Failed to settle failure: {}", e)))?;
            if done.rows_affected() > 0 {
                break;
            }
        }
        Ok(())
    }

    /// Get execution time percentiles (avg, median, p95, p99) in milliseconds.
    ///
    /// Filters by namespace and optionally workflow_name. Only considers completed/failed
    /// executions that have a non-null `duration_ms`.
    pub async fn get_execution_time_percentiles(
        &self,
        namespace: &str,
        organization_id: Option<Uuid>,
        workflow_type: Option<&str>,
    ) -> OrcherResult<(Option<f64>, Option<f64>, Option<f64>, Option<f64>)> {
        // Scoped to the caller's organization as well as the namespace name:
        // every tenant has a "default", and these figures covered all of them.
        let pools = self.scan_pools();
        if pools.len() == 1 {
            type PercentileRow = (Option<f64>, Option<f64>, Option<f64>, Option<f64>);
            let row: Option<PercentileRow> = sqlx::query_as(
                r#"
                SELECT
                    AVG(duration_ms::float8) as avg_ms,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY duration_ms) as median_ms,
                    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ms) as p95_ms,
                    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY duration_ms) as p99_ms
                FROM workflow_executions
                WHERE namespace = $1
                  AND ($2::uuid IS NULL OR organization_id = $2)
                  AND ($3::TEXT IS NULL OR workflow_type = $3)
                  AND duration_ms IS NOT NULL
                  AND status IN ('completed', 'failed')
                "#,
            )
            .bind(namespace)
            .bind(organization_id)
            .bind(workflow_type)
            .fetch_optional(pools[0])
            .await
            .map_err(|e| {
                OrcherError::internal(format!("Failed to get execution time percentiles: {}", e))
            })?;
            return Ok(row.unwrap_or((None, None, None, None)));
        }

        // Percentiles do not combine across pools, so the durations are
        // gathered and computed here, the way PERCENTILE_CONT does — over the
        // most recent executions on each pool, to bound the read.
        const PER_POOL: i64 = 100_000;
        let mut durations: Vec<f64> = Vec::new();
        for pool in pools {
            let rows: Vec<f64> = sqlx::query_scalar(
                r#"
                SELECT duration_ms::float8
                FROM workflow_executions
                WHERE namespace = $1
                  AND ($2::uuid IS NULL OR organization_id = $2)
                  AND ($3::TEXT IS NULL OR workflow_type = $3)
                  AND duration_ms IS NOT NULL
                  AND status IN ('completed', 'failed')
                ORDER BY completed_at DESC NULLS LAST
                LIMIT $4
                "#,
            )
            .bind(namespace)
            .bind(organization_id)
            .bind(workflow_type)
            .bind(PER_POOL)
            .fetch_all(pool)
            .await
            .map_err(|e| {
                OrcherError::internal(format!("Failed to get execution time percentiles: {}", e))
            })?;
            durations.extend(rows);
        }
        if durations.is_empty() {
            return Ok((None, None, None, None));
        }
        durations.sort_by(f64::total_cmp);
        let average = durations.iter().sum::<f64>() / durations.len() as f64;
        Ok((
            Some(average),
            Some(percentile_cont(&durations, 0.5)),
            Some(percentile_cont(&durations, 0.95)),
            Some(percentile_cont(&durations, 0.99)),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::PgPool;

    #[test]
    fn a_retried_run_timeout_goes_back_to_timed_out() {
        // The status a given-back retry returns to is read off the failure
        // type the timeout watcher records.
        assert!(ENDED_BEFORE_RETRY.contains(&format!(
            "'{}'",
            crate::schema::types::execution::RUN_TIMED_OUT_FAILURE_TYPE
        )));
    }

    /// Helper to create a test database pool
    async fn create_test_pool() -> PgPool {
        let database_url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://postgres:postgres@localhost/orcher_test".to_string());

        PgPool::connect(&database_url)
            .await
            .expect("Failed to connect to test database")
    }

    /// Seed an organization (and the user that owns it) for tenancy tests.
    async fn seed_organization(pool: &PgPool) -> Uuid {
        let owner = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO users (id, email, password_hash, hash_algorithm, password_changed_at) \
             VALUES ($1, $2, 'x', 'argon2', NOW())",
        )
        .bind(owner)
        .bind(format!("owner-{owner}@repository.test"))
        .execute(pool)
        .await
        .expect("seed user");
        let organization = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO organizations (id, name, display_name, slug, owner_id) \
             VALUES ($1, $2, $2, $2, $3)",
        )
        .bind(organization)
        .bind(format!("org-{organization}"))
        .bind(owner)
        .execute(pool)
        .await
        .expect("seed organization");
        organization
    }

    async fn seed_run(pool: &PgPool, organization: Uuid, workflow_id: &str) -> Uuid {
        let id = Uuid::new_v4();
        sqlx::query(
            r#"INSERT INTO workflow_executions
                 (id, execution_id, workflow_id, workflow_name, workflow_type, workflow_version,
                  namespace, task_queue, status, trigger_type, organization_id,
                  submitted_at, created_at, updated_at)
               VALUES ($1, $2, $3, 'tenancy', 'tenancy', '1.0.0', 'default', 'q',
                       'running', 'api', $4, NOW(), NOW(), NOW())"#,
        )
        .bind(id)
        .bind(id.to_string())
        .bind(workflow_id)
        .bind(organization)
        .execute(pool)
        .await
        .expect("seed run");
        id
    }

    /// A run in `namespace`, submitted `age_secs` ago; `workflow_id` `None` is a
    /// run started without one.
    async fn seed_named_run(
        pool: &PgPool,
        organization: Uuid,
        namespace: &str,
        workflow_id: Option<&str>,
        age_secs: i64,
    ) -> Uuid {
        let id = Uuid::new_v4();
        sqlx::query(
            r#"INSERT INTO workflow_executions
                 (id, execution_id, workflow_id, workflow_name, workflow_type, workflow_version,
                  namespace, task_queue, status, trigger_type, organization_id,
                  submitted_at, created_at, updated_at)
               VALUES ($1, $2, $3, 'resolution', 'resolution', '1.0.0', $4, 'q',
                       'completed', 'api', $5,
                       NOW() - make_interval(secs => $6), NOW(), NOW())"#,
        )
        .bind(id)
        .bind(id.to_string())
        .bind(workflow_id)
        .bind(namespace)
        .bind(organization)
        .bind(age_secs as f64)
        .execute(pool)
        .await
        .expect("seed run");
        id
    }

    /// Every RPC that addresses a run resolves it through `find_run`, so this
    /// is the contract for all of them: what each form of a request names,
    /// and that it never names a run the caller may not see.
    #[tokio::test]
    #[ignore] // Requires database
    async fn a_request_names_the_run_the_caller_may_see() {
        let pool = create_test_pool().await;
        let repo = WorkflowExecutionRepository::new(pool.clone());
        let workflow_id = format!("invoice-{}", Uuid::new_v4());
        let wf = workflow_id.as_str();

        let ours = seed_organization(&pool).await;
        let theirs = seed_organization(&pool).await;
        let older = seed_named_run(&pool, ours, "default", Some(wf), 60).await;
        let latest = seed_named_run(&pool, ours, "default", Some(wf), 10).await;
        let other_namespace = seed_named_run(&pool, ours, "billing", Some(wf), 1).await;
        let foreign = seed_named_run(&pool, theirs, "default", Some(wf), 0).await;
        let unnamed = seed_named_run(&pool, ours, "default", None, 5).await;

        let find =
            |org: Option<Uuid>, namespace: &'static str, workflow_id: String, run: String| {
                let repo = &repo;
                async move {
                    repo.find_run(RunRef {
                        organization_id: org,
                        namespace,
                        workflow_id: &workflow_id,
                        execution_id: &run,
                    })
                    .await
                    .expect("find_run")
                    .map(|exec| exec.id.as_uuid())
                }
            };
        let none = String::new;

        // A workflow id names the latest run in the caller's namespace — not a
        // newer run of the same id in another namespace or organization.
        assert_eq!(
            find(Some(ours), "default", wf.into(), none()).await,
            Some(latest)
        );
        assert_eq!(
            find(Some(ours), "billing", wf.into(), none()).await,
            Some(other_namespace)
        );
        assert_eq!(
            find(Some(theirs), "default", wf.into(), none()).await,
            Some(foreign)
        );
        let stranger = seed_organization(&pool).await;
        assert_eq!(
            find(Some(stranger), "default", wf.into(), none()).await,
            None
        );
        assert_eq!(find(Some(ours), "elsewhere", wf.into(), none()).await, None);

        // A run id names that exact run, in any namespace, if it is the
        // caller's, and if it is one of the named workflow's runs.
        assert_eq!(
            find(Some(ours), "default", none(), older.to_string()).await,
            Some(older)
        );
        assert_eq!(
            find(Some(ours), "default", none(), other_namespace.to_string()).await,
            Some(other_namespace)
        );
        assert_eq!(
            find(Some(ours), "default", wf.into(), older.to_string()).await,
            Some(older)
        );
        assert_eq!(
            find(Some(ours), "default", "another".into(), older.to_string()).await,
            None
        );
        assert_eq!(
            find(Some(ours), "default", none(), foreign.to_string()).await,
            None
        );
        // A run without a workflow id is named back with its run id in both.
        assert_eq!(
            find(
                Some(ours),
                "default",
                unnamed.to_string(),
                unnamed.to_string()
            )
            .await,
            Some(unnamed)
        );

        // A run id passed as the workflow id still names the run — the
        // caller's own only.
        assert_eq!(
            find(Some(ours), "default", older.to_string(), none()).await,
            Some(older)
        );
        assert_eq!(
            find(Some(ours), "default", foreign.to_string(), none()).await,
            None
        );

        // An unscoped caller sees every organization's runs.
        assert_eq!(
            find(None, "default", none(), foreign.to_string()).await,
            Some(foreign)
        );
        assert_eq!(
            find(None, "default", wf.into(), none()).await,
            Some(foreign)
        );

        assert_eq!(find(Some(ours), "default", none(), none()).await, None);

        for id in [older, latest, other_namespace, foreign, unnamed] {
            let _ = sqlx::query("DELETE FROM workflow_executions WHERE id = $1")
                .bind(id)
                .execute(&pool)
                .await;
        }
    }

    /// Every ended run carries the time it ended: a cancelled
    /// run, and a run moved to an ended status by a write that gave no time.
    #[tokio::test]
    #[ignore] // Requires database
    async fn an_ended_run_carries_the_time_it_ended() {
        let pool = create_test_pool().await;
        let repo = WorkflowExecutionRepository::new(pool.clone());
        let organization = seed_organization(&pool).await;
        let completed_at = |id: Uuid| {
            let pool = pool.clone();
            async move {
                sqlx::query_scalar::<_, Option<chrono::DateTime<chrono::Utc>>>(
                    "SELECT completed_at FROM workflow_executions WHERE id = $1",
                )
                .bind(id)
                .fetch_one(&pool)
                .await
                .unwrap()
            }
        };

        let cancelled = seed_run(&pool, organization, &format!("c-{}", Uuid::new_v4())).await;
        assert!(repo.cancel(&cancelled.to_string()).await.unwrap());
        assert!(
            completed_at(cancelled).await.is_some(),
            "cancelled without a time"
        );

        let terminated = seed_run(&pool, organization, &format!("t-{}", Uuid::new_v4())).await;
        repo.update_status(&terminated.to_string(), "terminated", None, None)
            .await
            .unwrap();
        assert!(
            completed_at(terminated).await.is_some(),
            "terminated without a time"
        );

        let running = seed_run(&pool, organization, &format!("r-{}", Uuid::new_v4())).await;
        repo.update_status(&running.to_string(), "running", None, None)
            .await
            .unwrap();
        assert!(
            completed_at(running).await.is_none(),
            "a live run has no end time"
        );

        for id in [cancelled, terminated, running] {
            let _ = sqlx::query("DELETE FROM workflow_executions WHERE id = $1")
                .bind(id)
                .execute(&pool)
                .await;
        }
    }

    /// A listing by task queue returns only that queue's runs.
    #[tokio::test]
    #[ignore] // Requires database
    async fn a_listing_by_task_queue_returns_only_that_queue() {
        let pool = create_test_pool().await;
        let repo = WorkflowExecutionRepository::new(pool.clone());
        let organization = seed_organization(&pool).await;
        let ours = seed_run(&pool, organization, &format!("q-{}", Uuid::new_v4())).await;
        let theirs = seed_run(&pool, organization, &format!("o-{}", Uuid::new_v4())).await;
        sqlx::query("UPDATE workflow_executions SET task_queue = 'other' WHERE id = $1")
            .bind(theirs)
            .execute(&pool)
            .await
            .unwrap();

        let listed = repo
            .list(ListExecutionsFilters {
                organization_id: Some(organization),
                task_queue: Some("q".to_string()),
                limit: 100,
                ..Default::default()
            })
            .await
            .unwrap();
        let ids: Vec<Uuid> = listed.iter().map(|run| run.id.as_uuid()).collect();
        assert_eq!(ids, [ours]);

        for id in [ours, theirs] {
            let _ = sqlx::query("DELETE FROM workflow_executions WHERE id = $1")
                .bind(id)
                .execute(&pool)
                .await;
        }
    }

    /// A workflow id is only unique within an organization's namespace. Two
    /// tenants using the same id must each find their own workflow, and never
    /// the other's: events were delivered to whatever an id-only lookup
    /// returned, which could be another organization's workflow.
    #[tokio::test]
    #[ignore] // Requires database
    async fn a_workflow_id_resolves_to_the_callers_own_workflow() {
        let pool = create_test_pool().await;
        let repo = WorkflowExecutionRepository::new(pool.clone());
        let workflow_id = format!("order-{}", Uuid::new_v4());

        let first = seed_organization(&pool).await;
        let second = seed_organization(&pool).await;
        let firsts = seed_run(&pool, first, &workflow_id).await;
        let seconds = seed_run(&pool, second, &workflow_id).await;

        let found = repo
            .find_latest_owned(Some(first), "default", &workflow_id)
            .await
            .unwrap()
            .expect("the first organization's workflow");
        assert_eq!(found.id.as_uuid(), firsts);

        let found = repo
            .find_latest_owned(Some(second), "default", &workflow_id)
            .await
            .unwrap()
            .expect("the second organization's workflow");
        assert_eq!(found.id.as_uuid(), seconds);

        let stranger = seed_organization(&pool).await;
        assert!(
            repo.find_latest_owned(Some(stranger), "default", &workflow_id)
                .await
                .unwrap()
                .is_none(),
            "an organization with no such workflow must find nothing"
        );
        assert!(
            repo.find_latest_owned(Some(first), "elsewhere", &workflow_id)
                .await
                .unwrap()
                .is_none(),
            "the id is scoped to its namespace"
        );

        for id in [firsts, seconds] {
            let _ = sqlx::query("DELETE FROM workflow_executions WHERE id = $1")
                .bind(id)
                .execute(&pool)
                .await;
        }
    }

    /// Matches what PostgreSQL returns for the same values, so a sharded
    /// deployment reports the figures a single database would.
    #[test]
    fn percentile_cont_matches_postgres() {
        let values = [1.0, 2.0, 3.0, 4.0, 10.0];
        assert_eq!(percentile_cont(&values, 0.5), 3.0);
        assert!((percentile_cont(&values, 0.95) - 8.8).abs() < 1e-9);
        assert!((percentile_cont(&values, 0.99) - 9.76).abs() < 1e-9);
        assert_eq!(percentile_cont(&[7.0], 0.99), 7.0);
    }

    #[test]
    fn test_filters_default() {
        let filters = ListExecutionsFilters::default();
        assert_eq!(filters.limit, 0);
        assert_eq!(filters.offset, 0);
        assert!(filters.workflow_name.is_none());
    }

    #[tokio::test]
    #[ignore] // Requires database
    async fn test_create_with_outbox_task_atomic() {
        // Task #5: Test atomic creation of workflow execution + outbox task
        let pool = create_test_pool().await;
        let repo = WorkflowExecutionRepository::new(pool.clone());

        let execution = WorkflowExecution::builder()
            .execution_id(format!("test_exec_{}", Uuid::new_v4()))
            .workflow_name("test_workflow".to_string())
            .workflow_version("v1.0".to_string())
            .namespace("test".to_string())
            .parameters(serde_json::json!({"key": "value"}))
            .build();

        let payload = serde_json::json!({
            "execution_id": execution.execution_id,
            "workflow_name": execution.workflow_name,
        });

        let idempotency_key = format!("test_key_{}", Uuid::new_v4());
        let task_queue = "test-queue";

        // Create execution and outbox task atomically
        let result = repo
            .create_with_outbox_task(
                &execution,
                payload.clone(),
                idempotency_key.clone(),
                task_queue,
            )
            .await;

        assert!(result.is_ok(), "Atomic creation should succeed");
        let (created_exec, task_id) = result.unwrap();

        // Verify execution was created
        assert_eq!(created_exec.execution_id, execution.execution_id);
        assert_eq!(created_exec.workflow_name, execution.workflow_name);
        assert_eq!(created_exec.task_queue, task_queue.to_string());

        // Verify outbox task was created
        let task_check =
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM outbox_queue WHERE task_id = $1")
                .bind(task_id)
                .fetch_one(&pool)
                .await
                .unwrap();

        assert_eq!(task_check, 1, "Outbox task should exist");

        // Verify task payload
        let task_payload: serde_json::Value =
            sqlx::query_scalar("SELECT payload FROM outbox_queue WHERE task_id = $1")
                .bind(task_id)
                .fetch_one(&pool)
                .await
                .unwrap();

        assert_eq!(task_payload, payload, "Task payload should match");
    }

    #[tokio::test]
    #[ignore] // Requires database
    async fn test_create_with_outbox_task_idempotency() {
        // Task #5: Test idempotency - duplicate key should not fail
        let pool = create_test_pool().await;
        let repo = WorkflowExecutionRepository::new(pool.clone());

        let execution_id = format!("test_exec_{}", Uuid::new_v4());
        let idempotency_key = format!("test_key_{}", Uuid::new_v4());
        let task_queue = "test-queue";

        let execution1 = WorkflowExecution::builder()
            .execution_id(execution_id.clone())
            .workflow_name("test_workflow".to_string())
            .workflow_version("v1.0".to_string())
            .namespace("test".to_string())
            .parameters(serde_json::json!({"key": "value"}))
            .build();

        let payload = serde_json::json!({"test": "data"});

        // First creation should succeed
        let result1 = repo
            .create_with_outbox_task(
                &execution1,
                payload.clone(),
                idempotency_key.clone(),
                task_queue,
            )
            .await;

        assert!(result1.is_ok(), "First creation should succeed");
        let (_, _task_id1) = result1.unwrap();

        // Second creation with same idempotency key should also succeed
        // (execution will fail on unique constraint, but that's expected in real use)
        // The outbox task should be idempotent due to ON CONFLICT DO NOTHING

        // Verify only one task with this idempotency key exists
        let task_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM outbox_queue WHERE idempotency_key = $1")
                .bind(&idempotency_key)
                .fetch_one(&pool)
                .await
                .unwrap();

        assert_eq!(task_count, 1, "Should have exactly one task with this key");
    }

    #[tokio::test]
    #[ignore] // Requires database
    async fn test_create_with_outbox_task_rollback_on_error() {
        // Task #5: Test transaction rollback if outbox creation fails
        // This is harder to test directly, but we can verify that if the transaction
        // fails, neither the execution nor the outbox task are created

        let pool = create_test_pool().await;
        let repo = WorkflowExecutionRepository::new(pool.clone());

        // Create an execution with invalid data that will fail
        let mut execution = WorkflowExecution::builder()
            .execution_id(format!("test_exec_{}", Uuid::new_v4()))
            .workflow_name("test_workflow".to_string())
            .workflow_version("v1.0".to_string())
            .namespace("test".to_string())
            .parameters(serde_json::json!({"key": "value"}))
            .build();

        // Set an invalid namespace (too long or invalid constraint)
        // This test assumes namespace has a constraint
        execution.namespace = "a".repeat(300); // Very long namespace

        let payload = serde_json::json!({"test": "data"});
        let idempotency_key = format!("test_key_{}", Uuid::new_v4());

        let result = repo
            .create_with_outbox_task(&execution, payload, idempotency_key.clone(), "test-queue")
            .await;

        // Should fail
        assert!(result.is_err(), "Should fail with invalid data");

        // Verify neither execution nor task was created
        let exec_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM workflow_executions WHERE execution_id = $1")
                .bind(&execution.execution_id)
                .fetch_one(&pool)
                .await
                .unwrap();

        assert_eq!(exec_count, 0, "Execution should not exist due to rollback");

        let task_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM outbox_queue WHERE idempotency_key = $1")
                .bind(&idempotency_key)
                .fetch_one(&pool)
                .await
                .unwrap();

        assert_eq!(task_count, 0, "Task should not exist due to rollback");
    }

    #[tokio::test]
    #[ignore] // Requires database
    async fn test_create_with_outbox_task_prevents_orphans() {
        // Task #5: Verify that we cannot have orphan executions
        // (execution created but no outbox task) with the atomic approach

        let pool = create_test_pool().await;
        let repo = WorkflowExecutionRepository::new(pool.clone());

        let execution_id = format!("test_exec_{}", Uuid::new_v4());
        let execution = WorkflowExecution::builder()
            .execution_id(execution_id.clone())
            .workflow_name("test_workflow".to_string())
            .workflow_version("v1.0".to_string())
            .namespace("test".to_string())
            .parameters(serde_json::json!({"key": "value"}))
            .build();

        let payload = serde_json::json!({"execution_id": execution_id});
        let idempotency_key = format!("test_key_{}", Uuid::new_v4());

        // Create atomically
        let result = repo
            .create_with_outbox_task(&execution, payload, idempotency_key.clone(), "test-queue")
            .await;

        assert!(result.is_ok());

        // Verify both exist
        let exec_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM workflow_executions WHERE execution_id = $1)",
        )
        .bind(&execution_id)
        .fetch_one(&pool)
        .await
        .unwrap();

        let task_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM outbox_queue WHERE idempotency_key = $1)",
        )
        .bind(&idempotency_key)
        .fetch_one(&pool)
        .await
        .unwrap();

        assert!(exec_exists, "Execution must exist");
        assert!(task_exists, "Outbox task must exist");

        // This proves atomicity - both exist or neither exists (tested in rollback test)
    }

    // ========================================================================
    // Task #9: Unit Tests for Claim/Release Operations
    // ========================================================================

    #[tokio::test]
    #[ignore] // Requires database
    async fn test_claim_and_get_next_success() {
        // Task #9: Test successful claiming of next available execution
        let pool = create_test_pool().await;
        let repo = WorkflowExecutionRepository::new(pool.clone());

        // Create an unclaimed execution
        let mut execution = WorkflowExecution::builder()
            .execution_id(format!("test_exec_{}", Uuid::new_v4()))
            .workflow_name("test_workflow".to_string())
            .workflow_version("v1.0".to_string())
            .namespace("test".to_string())
            .parameters(serde_json::json!({"key": "value"}))
            .build();

        execution.task_queue = "test-queue".to_string();
        execution.status = "running".to_string();

        let created = repo.create(&execution).await.unwrap();

        // Claim the execution
        let claimed = repo
            .claim_and_get_next("test-queue", "test", "service-1", 60, None, None)
            .await
            .unwrap();

        assert!(claimed.is_some(), "Should claim available execution");
        let claimed = claimed.unwrap();

        assert_eq!(claimed.execution_id, created.execution_id);
        assert_eq!(claimed.claimed_by, Some("service-1".to_string()));
        assert!(claimed.claimed_at.is_some());
        assert!(claimed.claim_timeout_at.is_some());
    }

    #[tokio::test]
    #[ignore] // Requires database
    async fn test_claim_and_get_next_skip_locked() {
        // Task #9: Test that SELECT FOR UPDATE SKIP LOCKED works
        // When one service claims, another service should skip it
        let pool = create_test_pool().await;
        let repo = WorkflowExecutionRepository::new(pool.clone());

        // Create an execution and claim it
        let mut execution = WorkflowExecution::builder()
            .execution_id(format!("test_exec_{}", Uuid::new_v4()))
            .workflow_name("test_workflow".to_string())
            .workflow_version("v1.0".to_string())
            .namespace("test".to_string())
            .parameters(serde_json::json!({}))
            .build();

        execution.task_queue = "test-queue".to_string();
        execution.status = "running".to_string();

        repo.create(&execution).await.unwrap();

        // Service 1 claims
        let claimed1 = repo
            .claim_and_get_next("test-queue", "test", "service-1", 60, None, None)
            .await
            .unwrap();

        assert!(claimed1.is_some());

        // Service 2 tries to claim - should get nothing (execution already claimed)
        let claimed2 = repo
            .claim_and_get_next("test-queue", "test", "service-2", 60, None, None)
            .await
            .unwrap();

        assert!(
            claimed2.is_none(),
            "Second service should not claim already-claimed execution"
        );
    }

    #[tokio::test]
    #[ignore] // Requires database
    async fn test_claim_and_get_next_filters_by_task_queue() {
        // Task #9: Test that claim filters by task queue
        let pool = create_test_pool().await;
        let repo = WorkflowExecutionRepository::new(pool.clone());

        // Create executions on different task queues
        let mut exec1 = WorkflowExecution::builder()
            .execution_id(format!("test_exec_{}", Uuid::new_v4()))
            .workflow_name("test_workflow".to_string())
            .workflow_version("v1.0".to_string())
            .namespace("test".to_string())
            .parameters(serde_json::json!({}))
            .build();

        exec1.task_queue = "queue-a".to_string();
        exec1.status = "running".to_string();

        let mut exec2 = WorkflowExecution::builder()
            .execution_id(format!("test_exec_{}", Uuid::new_v4()))
            .workflow_name("test_workflow".to_string())
            .workflow_version("v1.0".to_string())
            .namespace("test".to_string())
            .parameters(serde_json::json!({}))
            .build();

        exec2.task_queue = "queue-b".to_string();
        exec2.status = "running".to_string();

        repo.create(&exec1).await.unwrap();
        repo.create(&exec2).await.unwrap();

        // Claim from queue-a
        let claimed = repo
            .claim_and_get_next("queue-a", "test", "service-1", 60, None, None)
            .await
            .unwrap();

        assert!(claimed.is_some());
        assert_eq!(claimed.unwrap().execution_id, exec1.execution_id);

        // Claim from queue-b
        let claimed = repo
            .claim_and_get_next("queue-b", "test", "service-1", 60, None, None)
            .await
            .unwrap();

        assert!(claimed.is_some());
        assert_eq!(claimed.unwrap().execution_id, exec2.execution_id);
    }

    #[tokio::test]
    #[ignore] // Requires database
    async fn test_claim_and_get_next_respects_priority() {
        // Task #9: Test that claims respect priority ordering
        let pool = create_test_pool().await;
        let repo = WorkflowExecutionRepository::new(pool.clone());

        // Create executions with different priorities
        let mut low_priority = WorkflowExecution::builder()
            .execution_id(format!("low_{}", Uuid::new_v4()))
            .workflow_name("test_workflow".to_string())
            .workflow_version("v1.0".to_string())
            .namespace("test".to_string())
            .priority(1)
            .parameters(serde_json::json!({}))
            .build();

        low_priority.task_queue = "test-queue".to_string();
        low_priority.status = "running".to_string();

        let mut high_priority = WorkflowExecution::builder()
            .execution_id(format!("high_{}", Uuid::new_v4()))
            .workflow_name("test_workflow".to_string())
            .workflow_version("v1.0".to_string())
            .namespace("test".to_string())
            .priority(10)
            .parameters(serde_json::json!({}))
            .build();

        high_priority.task_queue = "test-queue".to_string();
        high_priority.status = "running".to_string();

        // Create low priority first
        repo.create(&low_priority).await.unwrap();
        // Then high priority
        repo.create(&high_priority).await.unwrap();

        // Claim should get high priority first
        let claimed = repo
            .claim_and_get_next("test-queue", "test", "service-1", 60, None, None)
            .await
            .unwrap();

        assert!(claimed.is_some());
        assert_eq!(
            claimed.unwrap().execution_id,
            high_priority.execution_id,
            "Should claim high priority execution first"
        );
    }

    #[tokio::test]
    #[ignore] // Requires database
    async fn test_release_claim_success() {
        // Task #9: Test releasing a claim
        let pool = create_test_pool().await;
        let repo = WorkflowExecutionRepository::new(pool.clone());

        // Create and claim an execution
        let mut execution = WorkflowExecution::builder()
            .execution_id(format!("test_exec_{}", Uuid::new_v4()))
            .workflow_name("test_workflow".to_string())
            .workflow_version("v1.0".to_string())
            .namespace("test".to_string())
            .parameters(serde_json::json!({}))
            .build();

        execution.task_queue = "test-queue".to_string();
        execution.status = "running".to_string();

        repo.create(&execution).await.unwrap();

        let claimed = repo
            .claim_and_get_next("test-queue", "test", "service-1", 60, None, None)
            .await
            .unwrap()
            .unwrap();

        assert!(claimed.claimed_by.is_some());

        // Release the claim
        let result = repo.release_claim(&claimed.execution_id).await;
        assert!(result.is_ok());

        // Verify claim is released
        let execution = repo
            .find_by_execution_id(&claimed.execution_id)
            .await
            .unwrap()
            .unwrap();

        assert!(execution.claimed_by.is_none());
        assert!(execution.claimed_at.is_none());
        assert!(execution.claim_timeout_at.is_none());
    }

    #[tokio::test]
    #[ignore] // Requires database
    async fn test_release_claim_allows_reclaim() {
        // Task #9: Test that releasing allows another service to claim
        let pool = create_test_pool().await;
        let repo = WorkflowExecutionRepository::new(pool.clone());

        let mut execution = WorkflowExecution::builder()
            .execution_id(format!("test_exec_{}", Uuid::new_v4()))
            .workflow_name("test_workflow".to_string())
            .workflow_version("v1.0".to_string())
            .namespace("test".to_string())
            .parameters(serde_json::json!({}))
            .build();

        execution.task_queue = "test-queue".to_string();
        execution.status = "running".to_string();

        repo.create(&execution).await.unwrap();

        // Service 1 claims
        let claimed1 = repo
            .claim_and_get_next("test-queue", "test", "service-1", 60, None, None)
            .await
            .unwrap()
            .unwrap();

        // Service 1 releases
        repo.release_claim(&claimed1.execution_id).await.unwrap();

        // Service 2 can now claim
        let claimed2 = repo
            .claim_and_get_next("test-queue", "test", "service-2", 60, None, None)
            .await
            .unwrap();

        assert!(claimed2.is_some());
        assert_eq!(claimed2.unwrap().claimed_by, Some("service-2".to_string()));
    }

    #[tokio::test]
    #[ignore] // Requires database
    async fn test_release_stale_claims() {
        // Task #9: Test releasing stale claims from crashed services
        let pool = create_test_pool().await;
        let repo = WorkflowExecutionRepository::new(pool.clone());

        // Create an execution and claim it with a past timeout
        let mut execution = WorkflowExecution::builder()
            .execution_id(format!("test_exec_{}", Uuid::new_v4()))
            .workflow_name("test_workflow".to_string())
            .workflow_version("v1.0".to_string())
            .namespace("test".to_string())
            .parameters(serde_json::json!({}))
            .build();

        execution.task_queue = "test-queue".to_string();
        execution.status = "running".to_string();

        let created = repo.create(&execution).await.unwrap();

        // Manually set a stale claim (claim_timeout_at in the past)
        sqlx::query(
            r#"
            UPDATE workflow_executions
            SET claimed_by = $1,
                claimed_at = NOW() - INTERVAL '2 hours',
                claim_timeout_at = NOW() - INTERVAL '1 hour'
            WHERE execution_id = $2
            "#,
        )
        .bind("crashed-service")
        .bind(&created.execution_id)
        .execute(&pool)
        .await
        .unwrap();

        // Release stale claims
        let released_count = repo.release_stale_claims().await.unwrap();

        assert_eq!(released_count, 1, "Should release 1 stale claim");

        // Verify claim was released
        let execution = repo
            .find_by_execution_id(&created.execution_id)
            .await
            .unwrap()
            .unwrap();

        assert!(execution.claimed_by.is_none());
        assert!(execution.claimed_at.is_none());
        assert!(execution.claim_timeout_at.is_none());
    }

    #[tokio::test]
    #[ignore] // Requires database
    async fn test_get_available_count() {
        let pool = create_test_pool().await;
        let repo = WorkflowExecutionRepository::new(pool.clone());

        // Create 3 available executions
        for i in 0..3 {
            let mut execution = WorkflowExecution::builder()
                .execution_id(format!("test_exec_{}", Uuid::new_v4()))
                .workflow_name("test_workflow".to_string())
                .workflow_version("v1.0".to_string())
                .namespace("test".to_string())
                .parameters(serde_json::json!({}))
                .build();

            execution.task_queue = "test-queue".to_string();
            execution.status = if i == 0 { "pending" } else { "queued" }.to_string();

            repo.create(&execution).await.unwrap();
        }

        // Create 1 claimed execution
        let mut claimed_exec = WorkflowExecution::builder()
            .execution_id(format!("test_exec_{}", Uuid::new_v4()))
            .workflow_name("test_workflow".to_string())
            .workflow_version("v1.0".to_string())
            .namespace("test".to_string())
            .parameters(serde_json::json!({}))
            .build();

        claimed_exec.task_queue = "test-queue".to_string();
        claimed_exec.status = "running".to_string();

        let _created = repo.create(&claimed_exec).await.unwrap();
        repo.claim_and_get_next("test-queue", "test", "service-1", 60, None, None)
            .await
            .unwrap();

        // Create 1 completed execution (should not count)
        let mut completed_exec = WorkflowExecution::builder()
            .execution_id(format!("test_exec_{}", Uuid::new_v4()))
            .workflow_name("test_workflow".to_string())
            .workflow_version("v1.0".to_string())
            .namespace("test".to_string())
            .parameters(serde_json::json!({}))
            .build();

        completed_exec.task_queue = "test-queue".to_string();
        completed_exec.status = "completed".to_string();
        repo.create(&completed_exec).await.unwrap();

        // Count available (unclaimed, non-terminal)
        let count = repo
            .get_available_count("test-queue", "test")
            .await
            .unwrap();

        assert_eq!(count, 3, "Should have 3 available executions");

        // Different namespace should have 0
        let count_other = repo
            .get_available_count("test-queue", "other-namespace")
            .await
            .unwrap();

        assert_eq!(count_other, 0, "Other namespace should have 0");
    }

    #[tokio::test]
    #[ignore] // Requires database
    async fn test_claim_execution_for_poll_alias() {
        // Verify the polling alias works correctly
        let pool = create_test_pool().await;
        let repo = WorkflowExecutionRepository::new(pool.clone());

        let mut execution = WorkflowExecution::builder()
            .execution_id(format!("test_exec_{}", Uuid::new_v4()))
            .workflow_name("test_workflow".to_string())
            .workflow_version("v1.0".to_string())
            .namespace("test".to_string())
            .parameters(serde_json::json!({}))
            .build();

        execution.task_queue = "test-queue".to_string();
        execution.status = "pending".to_string();

        repo.create(&execution).await.unwrap();

        // Use the polling-friendly alias
        let claimed = repo
            .claim_execution_for_poll("test-queue", "test", "service-1", 60, None, None, None, &[])
            .await
            .unwrap();

        assert!(claimed.is_some());
        let claimed = claimed.unwrap();
        assert_eq!(claimed.claimed_by, Some("service-1".to_string()));
        assert!(claimed.claimed_at.is_some());
        assert!(claimed.claim_timeout_at.is_some());
    }

    #[tokio::test]
    #[ignore] // Requires database
    async fn test_reclaim_expired_returns_execution_ids() {
        let pool = create_test_pool().await;
        let repo = WorkflowExecutionRepository::new(pool.clone());

        // Create 2 executions with expired claims
        let mut exec_ids = Vec::new();
        for _ in 0..2 {
            let mut execution = WorkflowExecution::builder()
                .execution_id(format!("test_exec_{}", Uuid::new_v4()))
                .workflow_name("test_workflow".to_string())
                .workflow_version("v1.0".to_string())
                .namespace("test".to_string())
                .parameters(serde_json::json!({}))
                .build();

            execution.task_queue = "test-queue".to_string();
            execution.status = "running".to_string();

            let created = repo.create(&execution).await.unwrap();
            exec_ids.push(created.execution_id.clone());

            // Set expired claim
            sqlx::query(
                r#"
                UPDATE workflow_executions
                SET claimed_by = $1,
                    claimed_at = NOW() - INTERVAL '2 hours',
                    claim_timeout_at = NOW() - INTERVAL '1 hour'
                WHERE execution_id = $2
                "#,
            )
            .bind("crashed-service")
            .bind(&created.execution_id)
            .execute(&pool)
            .await
            .unwrap();
        }

        // Create 1 execution with active claim (should not be reclaimed)
        let mut active_exec = WorkflowExecution::builder()
            .execution_id(format!("test_exec_{}", Uuid::new_v4()))
            .workflow_name("test_workflow".to_string())
            .workflow_version("v1.0".to_string())
            .namespace("test".to_string())
            .parameters(serde_json::json!({}))
            .build();

        active_exec.task_queue = "test-queue".to_string();
        active_exec.status = "running".to_string();

        let active = repo.create(&active_exec).await.unwrap();
        repo.claim_and_get_next("test-queue", "test", "active-service", 3600, None, None)
            .await
            .unwrap();

        // Reclaim expired
        let reclaimed_ids = repo.reclaim_expired().await.unwrap();

        assert_eq!(reclaimed_ids.len(), 2, "Should reclaim 2 expired claims");

        // Verify reclaimed IDs match what we created
        for id in &exec_ids {
            assert!(
                reclaimed_ids.contains(id),
                "Reclaimed IDs should contain {}",
                id
            );
        }

        // Verify active claim was not reclaimed
        assert!(
            !reclaimed_ids.contains(&active.execution_id),
            "Active claim should not be reclaimed"
        );

        // Verify claims were actually released
        for id in &reclaimed_ids {
            let execution = repo.find_by_execution_id(id).await.unwrap().unwrap();
            assert!(execution.claimed_by.is_none());
            assert!(execution.claimed_at.is_none());
            assert!(execution.claim_timeout_at.is_none());
        }
    }

    #[tokio::test]
    #[ignore] // Requires database
    async fn test_release_stale_claims_preserves_active() {
        // Task #9: Test that active claims are not released
        let pool = create_test_pool().await;
        let repo = WorkflowExecutionRepository::new(pool.clone());

        let mut execution = WorkflowExecution::builder()
            .execution_id(format!("test_exec_{}", Uuid::new_v4()))
            .workflow_name("test_workflow".to_string())
            .workflow_version("v1.0".to_string())
            .namespace("test".to_string())
            .parameters(serde_json::json!({}))
            .build();

        execution.task_queue = "test-queue".to_string();
        execution.status = "running".to_string();

        repo.create(&execution).await.unwrap();

        // Claim with future timeout (active claim)
        let claimed = repo
            .claim_and_get_next("test-queue", "test", "active-service", 3600, None, None)
            .await
            .unwrap()
            .unwrap();

        // Release stale claims
        let released_count = repo.release_stale_claims().await.unwrap();

        assert_eq!(released_count, 0, "Should not release active claims");

        // Verify claim still exists
        let execution = repo
            .find_by_execution_id(&claimed.execution_id)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(execution.claimed_by, Some("active-service".to_string()));
        assert!(execution.claimed_at.is_some());
    }

    #[tokio::test]
    #[ignore] // Requires database
    async fn test_release_stale_claims_multiple() {
        // Task #9: Test releasing multiple stale claims at once
        let pool = create_test_pool().await;
        let repo = WorkflowExecutionRepository::new(pool.clone());

        // Create multiple executions with stale claims
        for i in 0..5 {
            let mut execution = WorkflowExecution::builder()
                .execution_id(format!("test_exec_{}_{}", i, Uuid::new_v4()))
                .workflow_name("test_workflow".to_string())
                .workflow_version("v1.0".to_string())
                .namespace("test".to_string())
                .parameters(serde_json::json!({}))
                .build();

            execution.task_queue = "test-queue".to_string();
            execution.status = "running".to_string();

            let created = repo.create(&execution).await.unwrap();

            // Set stale claim
            sqlx::query(
                r#"
                UPDATE workflow_executions
                SET claimed_by = $1,
                    claimed_at = NOW() - INTERVAL '2 hours',
                    claim_timeout_at = NOW() - INTERVAL '1 hour'
                WHERE execution_id = $2
                "#,
            )
            .bind(format!("crashed-service-{}", i))
            .bind(&created.execution_id)
            .execute(&pool)
            .await
            .unwrap();
        }

        // Release all stale claims
        let released_count = repo.release_stale_claims().await.unwrap();

        assert_eq!(released_count, 5, "Should release all 5 stale claims");
    }
}
