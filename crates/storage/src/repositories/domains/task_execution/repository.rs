//! # Task Execution Repository
//!
//! Repository for managing task execution lifecycle records in the Orcher service-pull model.
//!
//! This repository handles:
//! - Creating scheduled task execution records
//! - Marking tasks as started (when services claim them)
//! - Marking tasks as completed or failed
//! - Tracking heartbeats for long-running tasks
//! - Querying execution history by various criteria
//!
//! ## Orcher Service-Pull Model
//!
//! - Services poll for tasks via PollTaskExecution
//! - Services self-identify (no server-side assignment)
//! - Task executions are scoped to workflow executions
//! - Multiple attempts tracked separately (retries)
//! - Task lifecycle: scheduled → started → completed/failed

use async_trait::async_trait;
use orcher_common::error::{OrcherError, OrcherResult};
use orcher_common::types::identifiers::{ExecutionId, TaskExecutionId, WorkflowId};
use orcher_common::types::time::Timestamptz;
use serde_json::Value as JsonValue;
use sqlx::{FromRow, PgPool};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::repositories::CrudOperations;
use crate::schema::types::execution::TaskExecution;
use crate::sharding::pool_routing::ShardPoolResolver;

// ================================================================================================
// ROW MAPPING
// ================================================================================================

/// Database row representation for task_executions table
#[derive(Debug, FromRow)]
struct TaskExecutionRow {
    id: Uuid,
    organization_id: Option<Uuid>,
    task_id: String,
    task_type: String,
    workflow_id: Uuid,
    execution_id: Uuid,
    task_queue: String,
    namespace: String,
    service_identity: Option<String>,
    attempt_number: i32,
    status: String,
    scheduled_at: Timestamptz,
    started_at: Option<Timestamptz>,
    completed_at: Option<Timestamptz>,
    task_token: Option<String>,
    schedule_to_start_timeout_secs: Option<i32>,
    start_to_close_timeout_secs: Option<i32>,
    heartbeat_timeout_secs: Option<i32>,
    input: Option<Vec<u8>>,
    result: Option<Vec<u8>>,
    error_message: Option<String>,
    error_type: Option<String>,
    error_details: Option<JsonValue>,
    retry_policy: Option<JsonValue>,
    headers: JsonValue,
    metadata: JsonValue,
    last_heartbeat_at: Option<Timestamptz>,
    heartbeat_details: Option<Vec<u8>>,
    created_at: Timestamptz,
    updated_at: Timestamptz,
}

impl From<TaskExecutionRow> for TaskExecution {
    fn from(row: TaskExecutionRow) -> Self {
        Self {
            id: TaskExecutionId::from_uuid(row.id),
            organization_id: row.organization_id,
            task_id: row.task_id,
            task_type: row.task_type,
            workflow_id: WorkflowId::from_uuid(row.workflow_id),
            execution_id: ExecutionId::from_uuid(row.execution_id),
            task_queue: row.task_queue,
            namespace: row.namespace,
            service_identity: row.service_identity,
            attempt_number: row.attempt_number,
            status: row.status,
            scheduled_at: row.scheduled_at,
            started_at: row.started_at,
            completed_at: row.completed_at,
            task_token: row.task_token,
            schedule_to_start_timeout_secs: row.schedule_to_start_timeout_secs,
            start_to_close_timeout_secs: row.start_to_close_timeout_secs,
            heartbeat_timeout_secs: row.heartbeat_timeout_secs,
            input: row.input,
            result: row.result,
            error_message: row.error_message,
            error_type: row.error_type,
            error_details: row.error_details,
            retry_policy: row.retry_policy,
            headers: row.headers,
            metadata: row.metadata,
            last_heartbeat_at: row.last_heartbeat_at,
            heartbeat_details: row.heartbeat_details,
            created_at: row.created_at,
            updated_at: row.updated_at,
        }
    }
}

// ================================================================================================
// REPOSITORY
// ================================================================================================

/// Whether the task row aliased `t` is work its workflow's cancellation
/// stops: the workflow was cancelled or terminated, or was asked to cancel
/// and the task is not cleanup the workflow scheduled in answer.
///
/// A task is judged by its first attempt, so a retry written after the
/// request is still the work the request interrupted. That attempt records
/// where in its journal the activation that scheduled it read to (V088). The
/// activation carried the journal up to there and no further, so it had seen
/// the request exactly when the request's journal entry is at or before that
/// point: its tasks are cleanup, run as the workflow's cancellation handler
/// meant them to. An activation that had not seen it was carrying on as if
/// nothing had been asked, and its tasks are stopped — including one handed
/// out before the request and answered after it, whose tasks are scheduled
/// after the request but are not cleanup. A request recorded but not yet
/// journaled has been seen by no activation.
///
/// A first attempt with no recorded position (written before V088, or by an
/// activation whose claim recorded none) is judged by time instead, as every
/// task was before: it is stopped when the request came after it was first
/// scheduled, the earliest `scheduled_at` over its attempts. Both times are
/// the database's: the request is stamped with `NOW()` and so is a task's
/// `scheduled_at` when it is inserted.
///
/// One rule for every place that asks: the heartbeat that tells a running
/// task to stop, the start that refuses a queued one, the retry that is not
/// scheduled, and the sweep that ends a queued one. A task's rows live on its
/// workflow's shard, so the workflow row and its journal are on the same
/// database.
const STOPPED_BY_CANCELLATION: &str = "EXISTS (\
    SELECT 1 FROM workflow_executions w \
    WHERE w.id = t.workflow_id \
      AND (w.status IN ('cancelled', 'terminated') \
           OR (w.cancel_requested_at IS NOT NULL AND COALESCE( \
                (SELECT NOT EXISTS ( \
                        SELECT 1 FROM workflow_execution_journal j \
                        WHERE j.execution_id = w.execution_id \
                          AND j.event_type = 'WorkflowExecutionCancelRequested' \
                          AND j.event_id <= f.scheduled_through_entry) \
                 FROM task_executions f \
                 WHERE f.workflow_id = t.workflow_id AND f.task_id = t.task_id \
                   AND f.scheduled_through_entry IS NOT NULL \
                 ORDER BY f.attempt_number LIMIT 1), \
                w.cancel_requested_at > ( \
                    SELECT MIN(f.scheduled_at) FROM task_executions f \
                    WHERE f.workflow_id = t.workflow_id AND f.task_id = t.task_id)))))";

/// The outbox row that delivers attempt `s` of a task to a worker, found by
/// the key the coordinator gives it (`WorkflowCoordinator::task_dispatch_key`)
/// over its unique index. The row is written in the transaction that
/// schedules the attempt, so it is on the same database as the task row.
///
/// The row is open, `pending` or `processing`, while the attempt is
/// `scheduled` and closed once it is not: taking the attempt, by a poll or
/// eagerly, settles it in the same statement, and the eager reclaimer, which
/// puts an attempt back to `scheduled`, opens it again. An open row of an
/// attempt already taken could deliver nothing, and was claimed again and
/// again; a closed row of an attempt still waiting would leave it waiting.
const ATTEMPT_DISPATCH: &str =
    "o.idempotency_key = s.workflow_id::text || ':' || s.execution_id::text \
     || ':dispatch_task:' || s.task_id || ':' || s.attempt_number::text";

/// The columns of a task row the start statements return, from their CTE
/// `started` as from the row.
const STARTED_COLUMNS: &str =
    "id, organization_id, task_id, task_type, workflow_id, execution_id, \
     task_queue, namespace, service_identity, attempt_number, status, \
     scheduled_at, started_at, completed_at, task_token, \
     schedule_to_start_timeout_secs, start_to_close_timeout_secs, \
     heartbeat_timeout_secs, input, result, error_message, error_type, \
     error_details, retry_policy, headers, metadata, \
     last_heartbeat_at, heartbeat_details, created_at, updated_at";

/// Settle the dispatch row of the attempt in the CTE `started`; see
/// [`ATTEMPT_DISPATCH`].
fn settle_dispatch_cte() -> String {
    format!(
        "settled AS ( \
            UPDATE outbox_queue o \
            SET status = 'completed', completed_at = NOW(), \
                lease_expires_at = NULL, lease_owner = NULL \
            FROM started s \
            WHERE {ATTEMPT_DISPATCH} \
              AND o.status IN ('pending', 'processing'))"
    )
}

/// Minimal info returned by the eager-claim reclaimer for queue notification.
#[derive(Debug, Clone, FromRow)]
pub struct ReclaimedEagerTask {
    /// Internal workflow UUID.
    pub workflow_id: Uuid,
    /// Task queue the task belongs to.
    pub task_queue: String,
    /// Namespace the task belongs to.
    pub namespace: String,
}

/// Summary of task execution states for a workflow
#[derive(Debug, Clone)]
pub struct WorkflowTaskSummary {
    /// Total number of tasks in the workflow
    pub total_tasks: i64,
    /// Number of tasks that have completed successfully
    pub completed_tasks: i64,
    /// Number of tasks that have failed
    pub failed_tasks: i64,
    /// Number of tasks that were cancelled
    pub canceled_tasks: i64,
    /// Number of tasks that timed out (terminal; retries create separate rows)
    pub timed_out_tasks: i64,
    /// Number of tasks currently running
    pub running_tasks: i64,
    /// Number of tasks that are scheduled but not yet running
    pub scheduled_tasks: i64,
}

/// Repository for task execution operations
#[derive(Debug, Clone)]
pub struct TaskExecutionRepository {
    pool: PgPool,
    /// Optional shard-pool routing for partitioned persistence. A task's rows live on
    /// the same shard as its workflow (keyed by `workflow_executions.id`). Ops that
    /// know the workflow id route directly; ops keyed only by the globally-unique
    /// task/execution id scatter across pools (the row is on exactly one). Unset ⇒ the
    /// single `pool` is used, byte-identical to before.
    shard_resolver: std::sync::Arc<std::sync::OnceLock<std::sync::Arc<dyn ShardPoolResolver>>>,
}

impl TaskExecutionRepository {
    /// Create a new TaskExecutionRepository instance
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            shard_resolver: std::sync::Arc::new(std::sync::OnceLock::new()),
        }
    }

    /// Inject the shard-pool resolver (partitioned persistence). Idempotent; a second
    /// call is ignored. Wired once at startup after the router is built.
    pub fn set_shard_resolver(&self, resolver: std::sync::Arc<dyn ShardPoolResolver>) {
        let _ = self.shard_resolver.set(resolver);
    }

    /// The pool that owns `workflow_id`'s shard (resolver's pick if set, else the single
    /// pool). `None` namespace ⇒ shared default set (OSS / single-tenant routing).
    /// The shard `workflow_id` belongs to, from the same resolver that picks its
    /// pool — a task row therefore records the shard it was actually routed to.
    /// `None` when no resolver is injected; the partial index skips such rows.
    fn shard_for(&self, workflow_id: Uuid) -> Option<i32> {
        self.shard_resolver.get().map(|r| r.shard_of(workflow_id))
    }

    fn pool_for(&self, workflow_id: Uuid) -> &PgPool {
        match self.shard_resolver.get() {
            Some(resolver) => resolver.pool_for_workflow(None, workflow_id),
            None => &self.pool,
        }
    }

    /// The pool owning `workflow_id`'s shard, for callers outside this repo that run
    /// their own transaction over a workflow's task rows (e.g. the completion batch).
    pub fn pool_for_workflow(&self, workflow_id: Uuid) -> &PgPool {
        self.pool_for(workflow_id)
    }

    /// Pools to scan for an op keyed only by the globally-unique task/execution id (not
    /// the routing workflow id): every shard pool when a resolver is set, else the one
    /// pool. The target row exists on exactly one pool.
    fn scan_pools(&self) -> Vec<&PgPool> {
        match self.shard_resolver.get() {
            Some(resolver) => resolver.all_pools(),
            None => vec![&self.pool],
        }
    }

    /// Which of `ids` are task executions still scheduled — not yet taken by
    /// any poll — on whichever database holds them.
    pub async fn still_scheduled(&self, ids: &[Uuid]) -> OrcherResult<Vec<Uuid>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut scheduled = Vec::new();
        for pool in self.scan_pools() {
            let found: Vec<Uuid> = sqlx::query_scalar(
                "SELECT id FROM task_executions WHERE id = ANY($1) AND status = 'scheduled'",
            )
            .bind(ids)
            .fetch_all(pool)
            .await
            .map_err(|e| {
                OrcherError::internal(format!("Failed to read task execution status: {e}"))
            })?;
            scheduled.extend(found);
        }
        Ok(scheduled)
    }

    /// Get a reference to the connection pool
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    // ========== LIFECYCLE OPERATIONS ==========

    /// Create a new scheduled task execution
    #[allow(clippy::too_many_arguments)]
    pub async fn create_scheduled(
        &self,
        task_id: String,
        task_type: String,
        workflow_id: WorkflowId,
        execution_id: ExecutionId,
        task_queue: String,
        namespace: String,
        attempt_number: i32,
        input: Option<Vec<u8>>,
        retry_policy: Option<JsonValue>,
        schedule_to_start_timeout_secs: Option<i32>,
        start_to_close_timeout_secs: Option<i32>,
        heartbeat_timeout_secs: Option<i32>,
        headers: Option<JsonValue>,
        metadata: Option<JsonValue>,
    ) -> OrcherResult<TaskExecution> {
        self.insert_scheduled(
            self.pool_for(workflow_id.as_uuid()),
            false,
            task_id,
            task_type,
            workflow_id,
            execution_id,
            task_queue,
            namespace,
            attempt_number,
            input,
            retry_policy,
            schedule_to_start_timeout_secs,
            start_to_close_timeout_secs,
            heartbeat_timeout_secs,
            headers,
            metadata,
        )
        .await?
        .ok_or_else(|| OrcherError::internal("task execution insert returned no row"))
    }

    /// Record a task a workflow has scheduled, unless it already has been.
    ///
    /// For the ScheduleTask command. The row is unique per execution, task and
    /// attempt (V049), so the same command applied a second time — a
    /// completion retried after its reply was lost, or re-emitted by a replay
    /// after the task had finished — inserts nothing and returns `None`, and
    /// the caller schedules nothing. Before, the second insert went in beside
    /// the first and the task ran twice.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_scheduled_once(
        &self,
        task_id: String,
        task_type: String,
        workflow_id: WorkflowId,
        execution_id: ExecutionId,
        task_queue: String,
        namespace: String,
        attempt_number: i32,
        input: Option<Vec<u8>>,
        retry_policy: Option<JsonValue>,
        schedule_to_start_timeout_secs: Option<i32>,
        start_to_close_timeout_secs: Option<i32>,
        heartbeat_timeout_secs: Option<i32>,
        headers: Option<JsonValue>,
        metadata: Option<JsonValue>,
    ) -> OrcherResult<Option<TaskExecution>> {
        self.insert_scheduled(
            self.pool_for(workflow_id.as_uuid()),
            true,
            task_id,
            task_type,
            workflow_id,
            execution_id,
            task_queue,
            namespace,
            attempt_number,
            input,
            retry_policy,
            schedule_to_start_timeout_secs,
            start_to_close_timeout_secs,
            heartbeat_timeout_secs,
            headers,
            metadata,
        )
        .await
    }

    /// [`create_scheduled_once`](Self::create_scheduled_once) inside the
    /// caller's transaction, so the row commits together with its outbox
    /// dispatch or not at all. The transaction must be on the pool that owns
    /// `workflow_id`'s shard.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_scheduled_once_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        task_id: String,
        task_type: String,
        workflow_id: WorkflowId,
        execution_id: ExecutionId,
        task_queue: String,
        namespace: String,
        attempt_number: i32,
        input: Option<Vec<u8>>,
        retry_policy: Option<JsonValue>,
        schedule_to_start_timeout_secs: Option<i32>,
        start_to_close_timeout_secs: Option<i32>,
        heartbeat_timeout_secs: Option<i32>,
        headers: Option<JsonValue>,
        metadata: Option<JsonValue>,
    ) -> OrcherResult<Option<TaskExecution>> {
        self.insert_scheduled(
            &mut **tx,
            true,
            task_id,
            task_type,
            workflow_id,
            execution_id,
            task_queue,
            namespace,
            attempt_number,
            input,
            retry_policy,
            schedule_to_start_timeout_secs,
            start_to_close_timeout_secs,
            heartbeat_timeout_secs,
            headers,
            metadata,
        )
        .await
    }

    /// Take the right to hand a first attempt to the outbox, once.
    ///
    /// True for exactly one caller per row: the one whose transaction marks
    /// `last_dispatched_at` on a first attempt that is still scheduled and has
    /// never been dispatched. A second caller — another engine replaying the
    /// same ScheduleTask, or the sweep on another node — waits on the row lock
    /// and then finds it marked. The caller inserts the outbox row in the same
    /// transaction, so the mark and the dispatch commit together.
    ///
    /// Retries (attempt > 1) are left alone: the retry dispatcher delivers
    /// them once their backoff has elapsed, and dispatching one here would cut
    /// its backoff short.
    ///
    /// Nor is a row scheduled before `scheduled_since` claimed. A first attempt
    /// written before rows were marked has only its outbox row to show it was
    /// dispatched, and data retention deletes completed outbox rows; past that
    /// point an unmarked row that was dispatched cannot be told from one that
    /// never was, and dispatching it would run the task twice. The caller
    /// passes a bound inside the outbox retention period.
    pub async fn claim_first_dispatch(
        conn: &mut sqlx::PgConnection,
        id: TaskExecutionId,
        scheduled_since: Timestamptz,
    ) -> OrcherResult<bool> {
        let claimed = sqlx::query(
            r#"
            UPDATE task_executions
            SET last_dispatched_at = NOW()
            WHERE id = $1
              AND status = 'scheduled'
              AND attempt_number = 1
              AND last_dispatched_at IS NULL
              AND scheduled_at >= $2
            "#,
        )
        .bind(id.as_uuid())
        .bind(scheduled_since)
        .execute(conn)
        .await
        .map_err(|e| {
            error!("Failed to claim task dispatch: {}", e);
            OrcherError::internal(format!("Failed to claim task dispatch: {}", e))
        })?
        .rows_affected();
        Ok(claimed == 1)
    }

    #[allow(clippy::too_many_arguments)]
    async fn insert_scheduled<'e, E: sqlx::PgExecutor<'e>>(
        &self,
        executor: E,
        once: bool,
        task_id: String,
        task_type: String,
        workflow_id: WorkflowId,
        execution_id: ExecutionId,
        task_queue: String,
        namespace: String,
        attempt_number: i32,
        input: Option<Vec<u8>>,
        retry_policy: Option<JsonValue>,
        schedule_to_start_timeout_secs: Option<i32>,
        start_to_close_timeout_secs: Option<i32>,
        heartbeat_timeout_secs: Option<i32>,
        headers: Option<JsonValue>,
        metadata: Option<JsonValue>,
    ) -> OrcherResult<Option<TaskExecution>> {
        debug!(
            "Creating scheduled task execution: task_id={}, workflow_id={}, attempt={}",
            task_id, workflow_id, attempt_number
        );

        let id = Uuid::new_v4();
        let now = Timestamptz::now();
        let headers = headers.unwrap_or_else(|| serde_json::json!({}));
        let metadata = metadata.unwrap_or_else(|| serde_json::json!({}));

        // `scheduled_by_command` is what puts the row under V049's unique
        // index; NULL leaves it outside, as every insert was before. The
        // conflict target names the index's predicate so PostgreSQL can infer
        // it; without the mark there is nothing to conflict with.
        let row: Option<TaskExecutionRow> = sqlx::query_as(
            r#"
            INSERT INTO task_executions (
                id, task_id, task_type, workflow_id, execution_id,
                task_queue, namespace, attempt_number, status, scheduled_at,
                schedule_to_start_timeout_secs, start_to_close_timeout_secs,
                heartbeat_timeout_secs, input, retry_policy, headers, metadata,
                created_at, updated_at, shard_id, scheduled_by_command, organization_id,
                scheduled_through_entry
            )
            -- A task belongs to its workflow's organization. Recorded here, it
            -- is what the task's dispatch is stamped with, so the claims that
            -- hand the task to a worker can be scoped by it, and what the
            -- task's completion and heartbeats are checked against.
            -- Scheduled by the database's clock, the one a cancellation request
            -- is timed by: whether a task is work the request interrupted is
            -- decided by comparing the two (`STOPPED_BY_CANCELLATION`), and an
            -- engine clock behind the database's made the workflow's cleanup,
            -- scheduled after the request, look scheduled before it.
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW(), $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
                    (SELECT organization_id FROM workflow_executions WHERE execution_id = $5::text),
                    -- Scheduled by a command: where in its journal the
                    -- activation that sent it read to (V088), which is what
                    -- the run's claim records while its completion is applied
                    -- under the activation fence, so no newer claim can have
                    -- moved it. Any other insert has no activation behind it.
                    CASE WHEN $20 THEN
                        (SELECT claimed_through_entry FROM workflow_executions
                         WHERE execution_id = $5::text)
                    END)
            ON CONFLICT (execution_id, task_id, attempt_number) WHERE scheduled_by_command
            DO NOTHING
            RETURNING id, organization_id, task_id, task_type, workflow_id, execution_id,
                      task_queue, namespace, service_identity, attempt_number, status,
                      scheduled_at, started_at, completed_at, task_token,
                      schedule_to_start_timeout_secs, start_to_close_timeout_secs,
                      heartbeat_timeout_secs, input, result, error_message, error_type,
                      error_details, retry_policy, headers, metadata,
                      last_heartbeat_at, heartbeat_details, created_at, updated_at
            "#,
        )
        .bind(id)
        .bind(&task_id)
        .bind(&task_type)
        .bind(workflow_id.as_uuid())
        .bind(execution_id.as_uuid())
        .bind(&task_queue)
        .bind(&namespace)
        .bind(attempt_number)
        .bind("scheduled")
        .bind(schedule_to_start_timeout_secs)
        .bind(start_to_close_timeout_secs)
        .bind(heartbeat_timeout_secs)
        .bind(input)
        .bind(retry_policy)
        .bind(&headers)
        .bind(&metadata)
        .bind(now)
        .bind(now)
        .bind(self.shard_for(workflow_id.as_uuid()))
        .bind(once.then_some(true))
        // The caller routes: the pool that owns the workflow's shard, or a
        // transaction on it.
        .fetch_optional(executor)
        .await
        .map_err(|e| {
            error!("Failed to create scheduled task execution: {}", e);
            OrcherError::internal(format!("Failed to create task execution: {}", e))
        })?;

        match &row {
            Some(row) => info!(
                "Created scheduled task execution: id={}, task_id={}, attempt={}",
                row.id, task_id, attempt_number
            ),
            None => info!(
                "Task already scheduled, not scheduled again: task_id={}, execution_id={}, attempt={}",
                task_id, execution_id, attempt_number
            ),
        }

        Ok(row.map(Into::into))
    }

    /// Mark task execution as started (service claimed), by a worker that does
    /// not heartbeat on its own; see [`mark_started_as`](Self::mark_started_as).
    pub async fn mark_started(
        &self,
        id: TaskExecutionId,
        service_identity: String,
        task_token: String,
    ) -> OrcherResult<TaskExecution> {
        self.mark_started_as(id, service_identity, task_token, None, None)
            .await
    }

    /// Mark task execution as started (service claimed).
    ///
    /// `auto_heartbeat_timeout_secs` is the heartbeat timeout this attempt runs
    /// under if the task sets none, given when the worker taking it heartbeats
    /// every task on its own; `None` for any other worker. It is recorded only
    /// when the task has no heartbeat timeout of its own, so the timeout watcher
    /// times the attempt out once that long passes without a heartbeat.
    ///
    /// `worker_instance_id` is the worker process the attempt was handed to,
    /// as its poll named it (V060), so that the attempt can be timed out early
    /// if that process stops being seen; `None` when the poll named none.
    ///
    /// A task its workflow's cancellation stops is not started (see
    /// [`STOPPED_BY_CANCELLATION`]): it was queued when the request came, and
    /// starting it would run interrupted work to the end. It stays scheduled
    /// for [`cancel_before_start_on`](Self::cancel_before_start_on) to end.
    pub async fn mark_started_as(
        &self,
        id: TaskExecutionId,
        service_identity: String,
        task_token: String,
        auto_heartbeat_timeout_secs: Option<i32>,
        worker_instance_id: Option<&str>,
    ) -> OrcherResult<TaskExecution> {
        debug!(
            "Marking task execution as started: id={}, service={}",
            id, service_identity
        );

        let now = Timestamptz::now();

        // Keyed by the globally-unique task id; the row lives on exactly one shard, so
        // scatter the UPDATE across pools and take the pool that returns the row.
        // The attempt's dispatch row is settled with it; see [`ATTEMPT_DISPATCH`].
        let sql = format!(
            r#"
            WITH started AS (
            UPDATE task_executions t
            SET status = 'started',
                service_identity = $2,
                task_token = $3,
                started_at = $4,
                auto_heartbeat_timeout_secs = CASE
                    WHEN heartbeat_timeout_secs IS NULL THEN $6::INTEGER
                END,
                worker_instance_id = $7,
                updated_at = $5
            WHERE t.id = $1 AND t.status = 'scheduled'
              AND NOT {STOPPED_BY_CANCELLATION}
            RETURNING {STARTED_COLUMNS}
            ),
            {settled}
            SELECT {STARTED_COLUMNS} FROM started
            "#,
            settled = settle_dispatch_cte(),
        );
        let mut last_err: Option<sqlx::Error> = None;
        for pool in self.scan_pools() {
            match sqlx::query_as::<_, TaskExecutionRow>(&sql)
                .bind(id.as_uuid())
                .bind(&service_identity)
                .bind(&task_token)
                .bind(now)
                .bind(now)
                .bind(auto_heartbeat_timeout_secs)
                .bind(worker_instance_id)
                .fetch_optional(pool)
                .await
            {
                Ok(Some(row)) => {
                    info!(
                        "Marked task execution as started: id={}, service={}",
                        id, service_identity
                    );
                    return Ok(row.into());
                }
                Ok(None) => continue,
                Err(e) => last_err = Some(e),
            }
        }
        if let Some(e) = last_err {
            error!("Failed to mark task execution as started: {}", e);
            return Err(OrcherError::internal(format!(
                "Failed to mark task as started: {}",
                e
            )));
        }
        Err(OrcherError::internal(
            "Failed to mark task as started: task not found, not in 'scheduled' status, \
             or stopped by its workflow's cancellation",
        ))
    }

    /// Mark task execution as started via eager return, with a short-lived claim TTL.
    ///
    /// Identical to `mark_started` but also sets `eager_claim_expires_at`, for a task
    /// with no timeout of its own: if the SDK drops the response without executing it,
    /// the reclaimer resets it back to `'scheduled'` once the TTL expires.
    ///
    /// A task with a start-to-close or heartbeat timeout gets no TTL. The engine is
    /// never told that a worker has begun an eager task, so the TTL cannot tell a
    /// dropped response from a task that is simply running longer than it, and a reset
    /// of a running task hands it out a second time and orphans the first worker's
    /// completion. Such a task is covered by its own timeouts instead, exactly as a
    /// polled task whose response is lost.
    ///
    /// Nor does a task handed to a worker that heartbeats on its own
    /// (`auto_heartbeat_timeout_secs` given; see [`mark_started_as`](Self::mark_started_as)):
    /// it runs under a heartbeat timeout whatever it declares, which recovers a dropped
    /// response the same way.
    ///
    /// A task its workflow's cancellation stops is not started, as in
    /// [`mark_started_as`](Self::mark_started_as).
    pub async fn mark_started_eager(
        &self,
        id: TaskExecutionId,
        service_identity: String,
        task_token: String,
        eager_claim_expires_at: Timestamptz,
        auto_heartbeat_timeout_secs: Option<i32>,
    ) -> OrcherResult<TaskExecution> {
        debug!(
            "Marking task execution as eagerly started: id={}, service={}, expires_at={}",
            id, service_identity, eager_claim_expires_at
        );

        let now = Timestamptz::now();

        // Keyed by the globally-unique task id; scatter across shards (row on exactly one).
        // The attempt's dispatch row is settled with it, so no poll claims a
        // dispatch of a task already handed out; the reclaimer opens it again
        // if it puts the task back. See [`ATTEMPT_DISPATCH`].
        let sql = format!(
            r#"
            WITH started AS (
            UPDATE task_executions t
            SET status = 'started',
                service_identity = $2,
                task_token = $3,
                started_at = $4,
                eager_claim_expires_at = CASE
                    WHEN start_to_close_timeout_secs IS NULL AND heartbeat_timeout_secs IS NULL
                         AND $6::INTEGER IS NULL
                    THEN $5
                END,
                auto_heartbeat_timeout_secs = CASE
                    WHEN heartbeat_timeout_secs IS NULL THEN $6::INTEGER
                END,
                updated_at = $4
            WHERE t.id = $1 AND t.status = 'scheduled'
              AND NOT {STOPPED_BY_CANCELLATION}
            RETURNING {STARTED_COLUMNS}
            ),
            {settled}
            SELECT {STARTED_COLUMNS} FROM started
            "#,
            settled = settle_dispatch_cte(),
        );
        let mut last_err: Option<sqlx::Error> = None;
        for pool in self.scan_pools() {
            match sqlx::query_as::<_, TaskExecutionRow>(&sql)
                .bind(id.as_uuid())
                .bind(&service_identity)
                .bind(&task_token)
                .bind(now)
                .bind(eager_claim_expires_at)
                .bind(auto_heartbeat_timeout_secs)
                .fetch_optional(pool)
                .await
            {
                Ok(Some(row)) => {
                    info!(
                        "Marked task execution as eagerly started: id={}, expires_at={}",
                        id, eager_claim_expires_at
                    );
                    return Ok(row.into());
                }
                Ok(None) => continue,
                Err(e) => last_err = Some(e),
            }
        }
        if let Some(e) = last_err {
            error!("Failed to mark task execution as eagerly started: {}", e);
            return Err(OrcherError::internal(format!(
                "Failed to mark task as eagerly started: {}",
                e
            )));
        }
        Err(OrcherError::internal(
            "Failed to mark task as eagerly started: task not found, not in 'scheduled' status, \
             or stopped by its workflow's cancellation",
        ))
    }

    /// Reset task executions whose eager claim TTL has expired back to `'scheduled'`.
    ///
    /// Called by the reclaimer every 5 seconds.  Returns the minimal info needed to
    /// notify the affected task queues so pollers wake up promptly.
    ///
    /// Only a task nothing else would recover is reset: one that has not heartbeated
    /// (a heartbeat clears the claim, but a row written by an older engine may still
    /// carry it) and runs under no timeout — none of its own, and no default heartbeat
    /// timeout given for a worker that heartbeats on its own. The others — rows claimed
    /// before `mark_started_eager` stopped giving them a TTL — have the claim cleared and
    /// are left to their timeouts, since resetting a task that is running runs it twice.
    pub async fn reclaim_expired_eager_tasks(&self) -> OrcherResult<Vec<ReclaimedEagerTask>> {
        // Every pool: an eager claim expires wherever its task lives.
        let mut reclaimed: Vec<ReclaimedEagerTask> = Vec::new();
        for pool in self.scan_pools() {
            sqlx::query(
                r#"
            UPDATE task_executions
            SET eager_claim_expires_at = NULL
            WHERE status = 'started'
              AND eager_claim_expires_at IS NOT NULL
              AND eager_claim_expires_at < NOW()
              AND (last_heartbeat_at IS NOT NULL
                   OR start_to_close_timeout_secs IS NOT NULL
                   OR heartbeat_timeout_secs IS NOT NULL
                   OR auto_heartbeat_timeout_secs IS NOT NULL)
            "#,
            )
            .execute(pool)
            .await
            .map_err(|e| {
                error!("Failed to clear covered eager task claims: {}", e);
                OrcherError::internal(format!("Failed to reclaim eager tasks: {}", e))
            })?;
            // The task's dispatch row, settled when it was handed out, is
            // opened again with it, in the same statement, or nothing would
            // deliver it a second time. Its claim count starts over: no claim
            // of it so far failed to deliver it. In the matching layer the
            // reopened row is moved into its partition again, where a row of
            // the same id still there keeps it from being there twice.
            let sql = format!(
                r#"
            WITH started AS (
            UPDATE task_executions
            SET status = 'scheduled',
                service_identity = NULL,
                task_token = NULL,
                started_at = NULL,
                eager_claim_expires_at = NULL,
                updated_at = NOW()
            WHERE status = 'started'
              AND eager_claim_expires_at IS NOT NULL
              AND eager_claim_expires_at < NOW()
              AND last_heartbeat_at IS NULL
              AND start_to_close_timeout_secs IS NULL
              AND heartbeat_timeout_secs IS NULL
              AND auto_heartbeat_timeout_secs IS NULL
            RETURNING workflow_id, execution_id, task_id, attempt_number, task_queue, namespace
            ),
            reopened AS (
            UPDATE outbox_queue o
            SET status = 'pending',
                completed_at = NULL,
                processing_started_at = NULL,
                next_retry_at = NULL,
                lease_expires_at = NULL,
                lease_owner = NULL,
                attempt_count = 0
            FROM started s
            WHERE {ATTEMPT_DISPATCH}
              AND o.status <> 'pending'
            )
            SELECT workflow_id, task_queue, namespace FROM started
            "#
            );
            let rows: Vec<ReclaimedEagerTask> =
                sqlx::query_as(&sql).fetch_all(pool).await.map_err(|e| {
                    error!("Failed to reclaim expired eager task claims: {}", e);
                    OrcherError::internal(format!("Failed to reclaim eager tasks: {}", e))
                })?;
            reclaimed.extend(rows);
        }

        if !reclaimed.is_empty() {
            info!(
                count = reclaimed.len(),
                "Reclaimed expired eager task claims"
            );
        }

        Ok(reclaimed)
    }

    /// Find scheduled tasks for a workflow in a specific task queue, ordered by schedule time.
    ///
    /// Used by the eager dispatch path to find the next candidates to return to the completing
    /// worker.  Limits to 3 to avoid over-eagering on high-fanout workflows.
    pub async fn find_scheduled_for_workflow(
        &self,
        workflow_id: WorkflowId,
        task_queue: &str,
        namespace: &str,
    ) -> OrcherResult<Vec<TaskExecution>> {
        let rows: Vec<TaskExecutionRow> = sqlx::query_as(
            r#"
            SELECT id, organization_id, task_id, task_type, workflow_id, execution_id,
                   task_queue, namespace, service_identity, attempt_number, status,
                   scheduled_at, started_at, completed_at, task_token,
                   schedule_to_start_timeout_secs, start_to_close_timeout_secs,
                   heartbeat_timeout_secs, input, result, error_message, error_type,
                   error_details, retry_policy, headers, metadata,
                   last_heartbeat_at, heartbeat_details, created_at, updated_at
            FROM task_executions
            WHERE workflow_id = $1
              AND status = 'scheduled'
              AND task_queue = $2
              AND namespace = $3
            ORDER BY scheduled_at ASC
            LIMIT 3
            "#,
        )
        .bind(workflow_id.as_uuid())
        .bind(task_queue)
        .bind(namespace)
        .fetch_all(self.pool_for(workflow_id.as_uuid()))
        .await
        .map_err(|e| {
            error!("Failed to find scheduled tasks for workflow: {}", e);
            OrcherError::internal(format!("Failed to find scheduled tasks: {}", e))
        })?;

        Ok(rows.into_iter().map(Into::into).collect())
    }

    /// Mark task execution as completed successfully
    pub async fn mark_completed(
        &self,
        id: TaskExecutionId,
        result: Vec<u8>,
    ) -> OrcherResult<TaskExecution> {
        debug!("Marking task execution as completed: id={}", id);

        let now = Timestamptz::now();

        // The row lives on one pool; the update succeeds on that one.
        let mut completed: Option<TaskExecutionRow> = None;
        for pool in self.scan_pools() {
            completed = sqlx::query_as(
                r#"
            UPDATE task_executions
            SET status = 'completed',
                result = $2,
                completed_at = $3,
                updated_at = $4
            WHERE id = $1 AND status = 'started'
            RETURNING id, organization_id, task_id, task_type, workflow_id, execution_id,
                      task_queue, namespace, service_identity, attempt_number, status,
                      scheduled_at, started_at, completed_at, task_token,
                      schedule_to_start_timeout_secs, start_to_close_timeout_secs,
                      heartbeat_timeout_secs, input, result, error_message, error_type,
                      error_details, retry_policy, headers, metadata,
                      last_heartbeat_at, heartbeat_details, created_at, updated_at
            "#,
            )
            .bind(id.as_uuid())
            .bind(&result)
            .bind(now)
            .bind(now)
            .fetch_optional(pool)
            .await
            .map_err(|e| {
                error!("Failed to mark task execution as completed: {}", e);
                OrcherError::internal(format!("Failed to mark task as completed: {}", e))
            })?;
            if completed.is_some() {
                break;
            }
        }
        let row = completed.ok_or_else(|| {
            OrcherError::internal(
                "Failed to mark task as completed: task not found or not in 'started' status",
            )
        })?;

        info!("Marked task execution as completed: id={}", id);

        Ok(row.into())
    }

    /// Mark task execution as failed
    pub async fn mark_failed(
        &self,
        id: TaskExecutionId,
        error_message: String,
        error_type: Option<String>,
        error_details: Option<JsonValue>,
    ) -> OrcherResult<TaskExecution> {
        debug!("Marking task execution as failed: id={}", id);

        let now = Timestamptz::now();

        // Keyed by the globally-unique task id — the row lives on one shard, so scatter
        // the UPDATE and take the pool that returns it. Single pool when unpartitioned.
        let sql = r#"
            UPDATE task_executions
            SET status = 'failed',
                error_message = $2,
                error_type = $3,
                error_details = $4,
                -- A start time, as every status past 'scheduled' needs
                -- (task_executions_started_requires_time).
                started_at = COALESCE(started_at, $5),
                completed_at = $5,
                updated_at = $6
            WHERE id = $1 AND status IN ('scheduled', 'started')
            RETURNING id, organization_id, task_id, task_type, workflow_id, execution_id,
                      task_queue, namespace, service_identity, attempt_number, status,
                      scheduled_at, started_at, completed_at, task_token,
                      schedule_to_start_timeout_secs, start_to_close_timeout_secs,
                      heartbeat_timeout_secs, input, result, error_message, error_type,
                      error_details, retry_policy, headers, metadata,
                      last_heartbeat_at, heartbeat_details, created_at, updated_at
            "#;
        let mut last_err: Option<sqlx::Error> = None;
        for pool in self.scan_pools() {
            match sqlx::query_as::<_, TaskExecutionRow>(sql)
                .bind(id.as_uuid())
                .bind(&error_message)
                .bind(&error_type)
                .bind(&error_details)
                .bind(now)
                .bind(now)
                .fetch_optional(pool)
                .await
            {
                Ok(Some(row)) => {
                    info!("Marked task execution as failed: id={}", id);
                    return Ok(row.into());
                }
                Ok(None) => continue,
                Err(e) => last_err = Some(e),
            }
        }
        if let Some(e) = last_err {
            error!("Failed to mark task execution as failed: {}", e);
            return Err(OrcherError::internal(format!(
                "Failed to mark task as failed: {}",
                e
            )));
        }
        Err(OrcherError::internal(
            "Failed to mark task as failed: task not found or not in a failable status",
        ))
    }

    /// Mark a task failed on `conn`, so it commits with the rest of what a
    /// failure records. `None` when the task is no longer scheduled or
    /// started — another report already recorded its outcome.
    pub async fn mark_failed_on(
        conn: &mut sqlx::PgConnection,
        id: TaskExecutionId,
        error_message: &str,
        error_type: Option<&str>,
        error_details: Option<&JsonValue>,
    ) -> OrcherResult<Option<TaskExecution>> {
        let now = Timestamptz::now();
        let row = sqlx::query_as::<_, TaskExecutionRow>(
            r#"
            UPDATE task_executions
            SET status = 'failed',
                error_message = $2,
                error_type = $3,
                error_details = $4,
                -- A start time, as every status past 'scheduled' needs
                -- (task_executions_started_requires_time).
                started_at = COALESCE(started_at, $5),
                completed_at = $5,
                updated_at = $5
            WHERE id = $1 AND status IN ('scheduled', 'started')
            RETURNING id, organization_id, task_id, task_type, workflow_id, execution_id,
                      task_queue, namespace, service_identity, attempt_number, status,
                      scheduled_at, started_at, completed_at, task_token,
                      schedule_to_start_timeout_secs, start_to_close_timeout_secs,
                      heartbeat_timeout_secs, input, result, error_message, error_type,
                      error_details, retry_policy, headers, metadata,
                      last_heartbeat_at, heartbeat_details, created_at, updated_at
            "#,
        )
        .bind(id.as_uuid())
        .bind(error_message)
        .bind(error_type)
        .bind(error_details)
        .bind(now)
        .fetch_optional(&mut *conn)
        .await
        .map_err(|e| OrcherError::internal(format!("Failed to mark task as failed: {}", e)))?;
        Ok(row.map(Into::into))
    }

    /// Mark a task cancelled on `conn`; `None` when it had already finished.
    pub async fn mark_cancelled_on(
        conn: &mut sqlx::PgConnection,
        id: TaskExecutionId,
    ) -> OrcherResult<Option<TaskExecution>> {
        let now = Timestamptz::now();
        let row = sqlx::query_as::<_, TaskExecutionRow>(
            r#"
            UPDATE task_executions
            SET status = 'cancelled',
                -- Every status past 'scheduled' carries a start time
                -- (task_executions_started_requires_time): a task cancelled
                -- before it started takes the moment it ended.
                started_at = COALESCE(started_at, $2),
                completed_at = $2,
                updated_at = $2
            WHERE id = $1 AND status IN ('scheduled', 'started')
            RETURNING id, organization_id, task_id, task_type, workflow_id, execution_id,
                      task_queue, namespace, service_identity, attempt_number, status,
                      scheduled_at, started_at, completed_at, task_token,
                      schedule_to_start_timeout_secs, start_to_close_timeout_secs,
                      heartbeat_timeout_secs, input, result, error_message, error_type,
                      error_details, retry_policy, headers, metadata,
                      last_heartbeat_at, heartbeat_details, created_at, updated_at
            "#,
        )
        .bind(id.as_uuid())
        .bind(now)
        .fetch_optional(&mut *conn)
        .await
        .map_err(|e| OrcherError::internal(format!("Failed to mark task as cancelled: {}", e)))?;
        Ok(row.map(Into::into))
    }

    /// Whether the task execution `id`, of the workflow with row id
    /// `workflow_id`, is work its workflow's cancellation stops (see
    /// [`STOPPED_BY_CANCELLATION`]). `false` for a task that does not exist.
    pub async fn stopped_by_cancellation(
        &self,
        id: TaskExecutionId,
        workflow_id: Uuid,
    ) -> OrcherResult<bool> {
        Self::stopped_by_cancellation_on(self.pool_for(workflow_id), id).await
    }

    /// [`stopped_by_cancellation`](Self::stopped_by_cancellation) on the given
    /// connection, which must be on the task's shard.
    pub async fn stopped_by_cancellation_on<'e, E: sqlx::PgExecutor<'e>>(
        executor: E,
        id: TaskExecutionId,
    ) -> OrcherResult<bool> {
        let stopped: Option<bool> = sqlx::query_scalar(&format!(
            "SELECT {STOPPED_BY_CANCELLATION} FROM task_executions t WHERE t.id = $1"
        ))
        .bind(id.as_uuid())
        .fetch_optional(executor)
        .await
        .map_err(|e| {
            OrcherError::internal(format!("Failed to read whether the task is stopped: {}", e))
        })?;
        Ok(stopped.unwrap_or(false))
    }

    /// End, as cancelled before they started, up to `limit` task executions
    /// still scheduled whose run has ended, on the shard `executor` is on.
    /// Returns how many were ended.
    ///
    /// A run that has ended reads no more results, so nothing will ever want
    /// such a task: a lost session's closing task, sent to a queue only the
    /// dead worker polled, waited there for good. Its run is over, so nothing
    /// is recorded for the run to read; only the task row ends, saying why. A
    /// task already running is left to finish or time out.
    pub async fn cancel_scheduled_of_ended_runs_on<'e, E: sqlx::PgExecutor<'e>>(
        executor: E,
        limit: i64,
    ) -> OrcherResult<u64> {
        let ended = sqlx::query(
            r#"
            UPDATE task_executions t
            SET status = 'cancelled',
                error_message = 'Task cancelled before it started: its run had already ended',
                error_type = 'Cancelled',
                started_at = COALESCE(t.started_at, NOW()),
                completed_at = NOW(),
                updated_at = NOW()
            WHERE t.id IN (
                SELECT o.id FROM task_executions o
                JOIN workflow_executions w ON w.id = o.workflow_id
                WHERE o.status = 'scheduled'
                  AND w.status IN ('completed', 'failed', 'cancelled', 'terminated',
                                   'timed_out', 'restarted_fresh', 'reset')
                ORDER BY o.scheduled_at
                LIMIT $1
                FOR UPDATE OF o SKIP LOCKED
            )
              AND t.status = 'scheduled'
            "#,
        )
        .bind(limit)
        .execute(executor)
        .await
        .map_err(|e| {
            OrcherError::internal(format!(
                "Failed to end the scheduled tasks of ended runs: {}",
                e
            ))
        })?;
        Ok(ended.rows_affected())
    }

    /// Task executions still scheduled that their workflow's cancellation
    /// stops, oldest first, at most `limit`: of the workflow with row id
    /// `workflow_id` when given, of any workflow otherwise. `executor` must be
    /// on the shard holding them.
    pub async fn scheduled_stopped_by_cancellation_on<'e, E: sqlx::PgExecutor<'e>>(
        executor: E,
        workflow_id: Option<Uuid>,
        limit: i64,
    ) -> OrcherResult<Vec<TaskExecution>> {
        let scope = if workflow_id.is_some() {
            "AND t.workflow_id = $2"
        } else {
            ""
        };
        let sql = format!(
            r#"
            SELECT id, organization_id, task_id, task_type, workflow_id, execution_id,
                   task_queue, namespace, service_identity, attempt_number, status,
                   scheduled_at, started_at, completed_at, task_token,
                   schedule_to_start_timeout_secs, start_to_close_timeout_secs,
                   heartbeat_timeout_secs, input, result, error_message, error_type,
                   error_details, retry_policy, headers, metadata,
                   last_heartbeat_at, heartbeat_details, created_at, updated_at
            FROM task_executions t
            WHERE t.status = 'scheduled'
              {scope}
              AND {STOPPED_BY_CANCELLATION}
            ORDER BY t.scheduled_at
            LIMIT $1
            "#
        );
        let mut query = sqlx::query_as::<_, TaskExecutionRow>(&sql).bind(limit);
        if let Some(workflow_id) = workflow_id {
            query = query.bind(workflow_id);
        }
        let rows = query.fetch_all(executor).await.map_err(|e| {
            OrcherError::internal(format!(
                "Failed to read the queued tasks a cancellation stops: {}",
                e
            ))
        })?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    /// End the scheduled task execution `id` as cancelled before it started,
    /// recording why, provided it is still scheduled and its workflow's
    /// cancellation still stops it. `None` when it is not: another engine
    /// ended it first, or it was never one to end.
    ///
    /// Run inside the caller's transaction, so the ending commits together
    /// with the outcome the workflow is told of.
    pub async fn cancel_before_start_on(
        conn: &mut sqlx::PgConnection,
        id: TaskExecutionId,
        error_message: &str,
        error_type: &str,
        error_details: &JsonValue,
    ) -> OrcherResult<Option<TaskExecution>> {
        let now = Timestamptz::now();
        let row = sqlx::query_as::<_, TaskExecutionRow>(&format!(
            r#"
            UPDATE task_executions t
            SET status = 'cancelled',
                error_message = $2,
                error_type = $3,
                error_details = $4,
                -- Every status past 'scheduled' carries a start time, as a
                -- schedule-to-start timeout's does: the moment it ended.
                started_at = COALESCE(started_at, $5),
                completed_at = $5,
                updated_at = $5
            WHERE t.id = $1 AND t.status = 'scheduled'
              AND {STOPPED_BY_CANCELLATION}
            RETURNING id, organization_id, task_id, task_type, workflow_id, execution_id,
                      task_queue, namespace, service_identity, attempt_number, status,
                      scheduled_at, started_at, completed_at, task_token,
                      schedule_to_start_timeout_secs, start_to_close_timeout_secs,
                      heartbeat_timeout_secs, input, result, error_message, error_type,
                      error_details, retry_policy, headers, metadata,
                      last_heartbeat_at, heartbeat_details, created_at, updated_at
            "#
        ))
        .bind(id.as_uuid())
        .bind(error_message)
        .bind(error_type)
        .bind(error_details)
        .bind(now)
        .fetch_optional(&mut *conn)
        .await
        .map_err(|e| {
            OrcherError::internal(format!(
                "Failed to cancel the task before it started: {}",
                e
            ))
        })?;
        Ok(row.map(Into::into))
    }

    /// Mark task execution as cancelled.
    ///
    /// Transitions a task from `scheduled` or `started` to `cancelled`.
    /// Both states are valid: a scheduled task may be cancelled before a worker
    /// picks it up, and a started task may be cancelled mid-execution (the
    /// worker calls this after receiving `cancel_requested` via heartbeat).
    pub async fn mark_cancelled(&self, id: TaskExecutionId) -> OrcherResult<TaskExecution> {
        debug!("Marking task execution as cancelled: id={}", id);

        let now = Timestamptz::now();

        // Keyed by the globally-unique task id — the row lives on one shard, so scatter
        // and take the pool that returns it. Single pool when unpartitioned, unchanged.
        let sql = r#"
            UPDATE task_executions
            -- 'cancelled': the only spelling task_executions_status_check
            -- allows. This wrote 'canceled', so every task cancellation was
            -- refused by the database.
            SET status = 'cancelled',
                -- Every status past 'scheduled' carries a start time
                -- (task_executions_started_requires_time): a task cancelled
                -- before it started takes the moment it ended. Without it,
                -- cancelling a scheduled task was refused by the database.
                started_at = COALESCE(started_at, $2),
                completed_at = $2,
                updated_at = $3
            WHERE id = $1 AND status IN ('scheduled', 'started')
            RETURNING id, organization_id, task_id, task_type, workflow_id, execution_id,
                      task_queue, namespace, service_identity, attempt_number, status,
                      scheduled_at, started_at, completed_at, task_token,
                      schedule_to_start_timeout_secs, start_to_close_timeout_secs,
                      heartbeat_timeout_secs, input, result, error_message, error_type,
                      error_details, retry_policy, headers, metadata,
                      last_heartbeat_at, heartbeat_details, created_at, updated_at
            "#;
        let mut last_err: Option<sqlx::Error> = None;
        for pool in self.scan_pools() {
            match sqlx::query_as::<_, TaskExecutionRow>(sql)
                .bind(id.as_uuid())
                .bind(now)
                .bind(now)
                .fetch_optional(pool)
                .await
            {
                Ok(Some(row)) => {
                    info!("Marked task execution as cancelled: id={}", id);
                    return Ok(row.into());
                }
                Ok(None) => continue,
                Err(e) => last_err = Some(e),
            }
        }
        if let Some(e) = last_err {
            error!("Failed to mark task execution as cancelled: {}", e);
            return Err(OrcherError::internal(format!(
                "Failed to mark task as cancelled: {}",
                e
            )));
        }
        Err(OrcherError::internal(
            "Failed to mark task as cancelled: task not found or not in a cancellable status",
        ))
    }

    /// Mark task execution as timed out
    pub async fn mark_timed_out(
        &self,
        id: TaskExecutionId,
        timeout_type: String,
    ) -> OrcherResult<TaskExecution> {
        debug!("Marking task execution as timed out: id={}", id);

        let now = Timestamptz::now();
        let error_message = format!("Task timed out: {}", timeout_type);

        // Keyed by the globally-unique task id — scatter across shards (row on one pool).
        let sql = r#"
            UPDATE task_executions
            SET status = 'timed_out',
                error_message = $2,
                error_type = $3,
                started_at = COALESCE(started_at, $4),
                completed_at = $5,
                updated_at = $6
            WHERE id = $1 AND status IN ('scheduled', 'started')
            RETURNING id, organization_id, task_id, task_type, workflow_id, execution_id,
                      task_queue, namespace, service_identity, attempt_number, status,
                      scheduled_at, started_at, completed_at, task_token,
                      schedule_to_start_timeout_secs, start_to_close_timeout_secs,
                      heartbeat_timeout_secs, input, result, error_message, error_type,
                      error_details, retry_policy, headers, metadata,
                      last_heartbeat_at, heartbeat_details, created_at, updated_at
            "#;
        let mut last_err: Option<sqlx::Error> = None;
        for pool in self.scan_pools() {
            match sqlx::query_as::<_, TaskExecutionRow>(sql)
                .bind(id.as_uuid())
                .bind(&error_message)
                .bind(&timeout_type)
                .bind(now)
                .bind(now)
                .bind(now)
                .fetch_optional(pool)
                .await
            {
                Ok(Some(row)) => {
                    info!("Marked task execution as timed out: id={}", id);
                    return Ok(row.into());
                }
                Ok(None) => continue,
                Err(e) => last_err = Some(e),
            }
        }
        if let Some(e) = last_err {
            error!("Failed to mark task execution as timed out: {}", e);
            return Err(OrcherError::internal(format!(
                "Failed to mark task as timed out: {}",
                e
            )));
        }
        Err(OrcherError::internal(
            "Failed to mark task as timed out: task not found or not in a timeout-eligible status",
        ))
    }

    /// Update heartbeat for a running task
    pub async fn update_heartbeat(
        &self,
        id: TaskExecutionId,
        heartbeat_details: Option<Vec<u8>>,
    ) -> OrcherResult<TaskExecution> {
        debug!("Updating heartbeat for task execution: id={}", id);

        let now = Timestamptz::now();

        // Keyed by the globally-unique task id — scatter across shards (row on one pool).
        let sql = r#"
            UPDATE task_executions
            SET last_heartbeat_at = $2,
                heartbeat_details = $3,
                -- A heartbeat is the worker saying it has the task, which is
                -- what an eager claim's TTL was waiting to learn: from here on
                -- the task is not reset under it.
                eager_claim_expires_at = NULL,
                updated_at = $4
            WHERE id = $1 AND status = 'started'
            RETURNING id, organization_id, task_id, task_type, workflow_id, execution_id,
                      task_queue, namespace, service_identity, attempt_number, status,
                      scheduled_at, started_at, completed_at, task_token,
                      schedule_to_start_timeout_secs, start_to_close_timeout_secs,
                      heartbeat_timeout_secs, input, result, error_message, error_type,
                      error_details, retry_policy, headers, metadata,
                      last_heartbeat_at, heartbeat_details, created_at, updated_at
            "#;
        let mut last_err: Option<sqlx::Error> = None;
        for pool in self.scan_pools() {
            match sqlx::query_as::<_, TaskExecutionRow>(sql)
                .bind(id.as_uuid())
                .bind(now)
                .bind(&heartbeat_details)
                .bind(now)
                .fetch_optional(pool)
                .await
            {
                Ok(Some(row)) => return Ok(row.into()),
                Ok(None) => continue,
                Err(e) => last_err = Some(e),
            }
        }
        if let Some(e) = last_err {
            error!("Failed to update heartbeat: {}", e);
            return Err(OrcherError::internal(format!(
                "Failed to update heartbeat: {}",
                e
            )));
        }
        // Not an internal error: the task exists or existed, and is simply no
        // longer running. Callers answer that differently from a failed write.
        Err(OrcherError::not_found(
            "started task execution",
            id.as_uuid().to_string(),
        ))
    }

    /// Record a heartbeat for each task in `heartbeats`, its row id and the
    /// details it carries, in one statement per database, as
    /// [`update_heartbeat`](Self::update_heartbeat) records one. Answers the
    /// tasks it recorded, those still running, each with whether a
    /// cancellation stops it (see [`stopped_by_cancellation`]
    /// (Self::stopped_by_cancellation)). A task the same call names twice is
    /// recorded once.
    pub async fn record_heartbeats(
        &self,
        heartbeats: &[(TaskExecutionId, Option<Vec<u8>>)],
    ) -> OrcherResult<Vec<(TaskExecutionId, bool)>> {
        if heartbeats.is_empty() {
            return Ok(Vec::new());
        }
        let ids: Vec<Uuid> = heartbeats.iter().map(|(id, _)| id.as_uuid()).collect();
        let details: Vec<Option<Vec<u8>>> = heartbeats.iter().map(|(_, d)| d.clone()).collect();
        let now = Timestamptz::now();
        let sql = format!(
            r#"
            UPDATE task_executions t
            SET last_heartbeat_at = $3,
                heartbeat_details = h.details,
                -- As for one heartbeat: the worker has the task, so an eager
                -- claim's TTL no longer resets it.
                eager_claim_expires_at = NULL,
                updated_at = $3
            FROM unnest($1::uuid[], $2::bytea[]) AS h(id, details)
            WHERE t.id = h.id AND t.status = 'started'
            RETURNING t.id, {STOPPED_BY_CANCELLATION}
            "#
        );
        let mut recorded = Vec::new();
        // Each task row lives on one database; the others match none of it.
        for pool in self.scan_pools() {
            let rows: Vec<(Uuid, bool)> = sqlx::query_as(&sql)
                .bind(&ids)
                .bind(&details)
                .bind(now)
                .fetch_all(pool)
                .await
                .map_err(|e| {
                    error!("Failed to record heartbeats: {}", e);
                    OrcherError::internal(format!("Failed to record heartbeats: {}", e))
                })?;
            recorded.extend(
                rows.into_iter()
                    .map(|(id, stopped)| (TaskExecutionId::from_uuid(id), stopped)),
            );
        }
        Ok(recorded)
    }

    // ========== QUERY OPERATIONS ==========

    /// Find task execution by ID
    pub async fn find_by_id(&self, id: TaskExecutionId) -> OrcherResult<Option<TaskExecution>> {
        debug!("Finding task execution by id: {}", id);

        // Keyed by the globally-unique task id — scan shard pools (row on exactly one).
        let sql = r#"
            SELECT id, organization_id, task_id, task_type, workflow_id, execution_id,
                   task_queue, namespace, service_identity, attempt_number, status,
                   scheduled_at, started_at, completed_at, task_token,
                   schedule_to_start_timeout_secs, start_to_close_timeout_secs,
                   heartbeat_timeout_secs, input, result, error_message, error_type,
                   error_details, retry_policy, headers, metadata,
                   last_heartbeat_at, heartbeat_details, created_at, updated_at
            FROM task_executions
            WHERE id = $1
            "#;
        for pool in self.scan_pools() {
            let row: Option<TaskExecutionRow> = sqlx::query_as(sql)
                .bind(id.as_uuid())
                .fetch_optional(pool)
                .await
                .map_err(|e| {
                    error!("Failed to find task execution by id: {}", e);
                    OrcherError::internal(format!("Failed to find task execution: {}", e))
                })?;
            if let Some(row) = row {
                return Ok(Some(row.into()));
            }
        }
        Ok(None)
    }

    /// Task executions by task token, as [`find_by_task_token`]
    /// (Self::find_by_task_token) finds each, in one query per database. A
    /// token that names no task has nothing in the answer.
    pub async fn find_by_task_tokens(
        &self,
        task_tokens: &[String],
    ) -> OrcherResult<Vec<TaskExecution>> {
        if task_tokens.is_empty() {
            return Ok(Vec::new());
        }
        // As for one token: the owner is the execution's organisation.
        let sql = r#"
            SELECT t.id, COALESCE(w.organization_id, t.organization_id) AS organization_id,
                   t.task_id, t.task_type, t.workflow_id, t.execution_id,
                   t.task_queue, t.namespace, t.service_identity, t.attempt_number, t.status,
                   t.scheduled_at, t.started_at, t.completed_at, t.task_token,
                   t.schedule_to_start_timeout_secs, t.start_to_close_timeout_secs,
                   t.heartbeat_timeout_secs, t.input, t.result, t.error_message, t.error_type,
                   t.error_details, t.retry_policy, t.headers, t.metadata,
                   t.last_heartbeat_at, t.heartbeat_details, t.created_at, t.updated_at
            FROM task_executions t
            LEFT JOIN workflow_executions w ON w.id = t.workflow_id
            WHERE t.task_token = ANY($1)
            "#;
        let mut found = Vec::new();
        for pool in self.scan_pools() {
            let rows: Vec<TaskExecutionRow> = sqlx::query_as(sql)
                .bind(task_tokens)
                .fetch_all(pool)
                .await
                .map_err(|e| {
                    error!("Failed to find task executions by task_token: {}", e);
                    OrcherError::internal(format!("Failed to find task executions: {}", e))
                })?;
            found.extend(rows.into_iter().map(TaskExecution::from));
        }
        Ok(found)
    }

    /// Find task execution by task token
    pub async fn find_by_task_token(
        &self,
        task_token: &str,
    ) -> OrcherResult<Option<TaskExecution>> {
        debug!("Finding task execution by task_token: {}", task_token);

        // Keyed by the globally-unique task_token, not the routing id — scan shard pools
        // (the row lives on exactly one). Single pool ⇒ unchanged.
        //
        // `organization_id` is the owning *execution's*, not the task row's own
        // column. The completion RPCs authorise on this value, and the task row's
        // copy has never been written: every task row in a real deployment was
        // found unowned while every execution was owned, so authorising on the
        // row refused every tenant's own completions. The execution is the one
        // source of truth for who owns a task; a copy on each child row is a
        // second source that can disagree with it, and did. The row's own column
        // is the fallback only for a task whose execution is gone.
        //
        // Joined in this query rather than looked up after it, because this is
        // the completion and heartbeat path and a second round trip per task
        // would be paid on every one. A task and its execution share a database,
        // as the retry sweep's correlated check on workflow_executions already
        // relies on.
        let sql = r#"
            SELECT t.id, COALESCE(w.organization_id, t.organization_id) AS organization_id,
                   t.task_id, t.task_type, t.workflow_id, t.execution_id,
                   t.task_queue, t.namespace, t.service_identity, t.attempt_number, t.status,
                   t.scheduled_at, t.started_at, t.completed_at, t.task_token,
                   t.schedule_to_start_timeout_secs, t.start_to_close_timeout_secs,
                   t.heartbeat_timeout_secs, t.input, t.result, t.error_message, t.error_type,
                   t.error_details, t.retry_policy, t.headers, t.metadata,
                   t.last_heartbeat_at, t.heartbeat_details, t.created_at, t.updated_at
            FROM task_executions t
            LEFT JOIN workflow_executions w ON w.id = t.workflow_id
            WHERE t.task_token = $1
            "#;
        for pool in self.scan_pools() {
            let row: Option<TaskExecutionRow> = sqlx::query_as(sql)
                .bind(task_token)
                .fetch_optional(pool)
                .await
                .map_err(|e| {
                    error!("Failed to find task execution by task_token: {}", e);
                    OrcherError::internal(format!("Failed to find task execution: {}", e))
                })?;
            if let Some(row) = row {
                return Ok(Some(row.into()));
            }
        }
        Ok(None)
    }

    /// Find all task executions for a workflow
    pub async fn find_by_workflow_id(
        &self,
        workflow_id: WorkflowId,
    ) -> OrcherResult<Vec<TaskExecution>> {
        debug!("Finding task executions by workflow_id: {}", workflow_id);

        let rows: Vec<TaskExecutionRow> = sqlx::query_as(
            r#"
            SELECT id, organization_id, task_id, task_type, workflow_id, execution_id,
                   task_queue, namespace, service_identity, attempt_number, status,
                   scheduled_at, started_at, completed_at, task_token,
                   schedule_to_start_timeout_secs, start_to_close_timeout_secs,
                   heartbeat_timeout_secs, input, result, error_message, error_type,
                   error_details, retry_policy, headers, metadata,
                   last_heartbeat_at, heartbeat_details, created_at, updated_at
            FROM task_executions
            WHERE workflow_id = $1
            ORDER BY scheduled_at ASC
            "#,
        )
        .bind(workflow_id.as_uuid())
        .fetch_all(self.pool_for(workflow_id.as_uuid()))
        .await
        .map_err(|e| {
            error!("Failed to find task executions by workflow_id: {}", e);
            OrcherError::internal(format!("Failed to find task executions: {}", e))
        })?;

        Ok(rows.into_iter().map(Into::into).collect())
    }

    /// Find all task executions for a specific execution
    pub async fn find_by_execution_id(
        &self,
        execution_id: ExecutionId,
    ) -> OrcherResult<Vec<TaskExecution>> {
        debug!("Finding task executions by execution_id: {}", execution_id);

        // Keyed by execution, not by the routing workflow id: gather every pool.
        let mut rows: Vec<TaskExecutionRow> = Vec::new();
        for pool in self.scan_pools() {
            let found: Vec<TaskExecutionRow> = sqlx::query_as(
                r#"
            SELECT id, organization_id, task_id, task_type, workflow_id, execution_id,
                   task_queue, namespace, service_identity, attempt_number, status,
                   scheduled_at, started_at, completed_at, task_token,
                   schedule_to_start_timeout_secs, start_to_close_timeout_secs,
                   heartbeat_timeout_secs, input, result, error_message, error_type,
                   error_details, retry_policy, headers, metadata,
                   last_heartbeat_at, heartbeat_details, created_at, updated_at
            FROM task_executions
            WHERE execution_id = $1
            ORDER BY scheduled_at ASC
            "#,
            )
            .bind(execution_id.as_uuid())
            .fetch_all(pool)
            .await
            .map_err(|e| {
                error!("Failed to find task executions by execution_id: {}", e);
                OrcherError::internal(format!("Failed to find task executions: {}", e))
            })?;
            rows.extend(found);
        }
        rows.sort_by_key(|r| r.scheduled_at);

        Ok(rows.into_iter().map(Into::into).collect())
    }

    /// Find task executions by status
    pub async fn find_by_status(&self, status: &str) -> OrcherResult<Vec<TaskExecution>> {
        debug!("Finding task executions by status: {}", status);

        // Not keyed by the routing workflow id: gather every pool.
        let mut rows: Vec<TaskExecutionRow> = Vec::new();
        for pool in self.scan_pools() {
            let found: Vec<TaskExecutionRow> = sqlx::query_as(
                r#"
            SELECT id, organization_id, task_id, task_type, workflow_id, execution_id,
                   task_queue, namespace, service_identity, attempt_number, status,
                   scheduled_at, started_at, completed_at, task_token,
                   schedule_to_start_timeout_secs, start_to_close_timeout_secs,
                   heartbeat_timeout_secs, input, result, error_message, error_type,
                   error_details, retry_policy, headers, metadata,
                   last_heartbeat_at, heartbeat_details, created_at, updated_at
            FROM task_executions
            WHERE status = $1
            ORDER BY scheduled_at DESC
            "#,
            )
            .bind(status)
            .fetch_all(pool)
            .await
            .map_err(|e| {
                error!("Failed to find task executions by status: {}", e);
                OrcherError::internal(format!("Failed to find task executions: {}", e))
            })?;
            rows.extend(found);
        }
        rows.sort_by_key(|r| std::cmp::Reverse(r.scheduled_at));

        Ok(rows.into_iter().map(Into::into).collect())
    }

    /// Find task executions by service identity
    pub async fn find_by_service_identity(
        &self,
        service_identity: &str,
    ) -> OrcherResult<Vec<TaskExecution>> {
        debug!(
            "Finding task executions by service_identity: {}",
            service_identity
        );

        // Not keyed by the routing workflow id: gather every pool.
        let mut rows: Vec<TaskExecutionRow> = Vec::new();
        for pool in self.scan_pools() {
            let found: Vec<TaskExecutionRow> = sqlx::query_as(
                r#"
            SELECT id, organization_id, task_id, task_type, workflow_id, execution_id,
                   task_queue, namespace, service_identity, attempt_number, status,
                   scheduled_at, started_at, completed_at, task_token,
                   schedule_to_start_timeout_secs, start_to_close_timeout_secs,
                   heartbeat_timeout_secs, input, result, error_message, error_type,
                   error_details, retry_policy, headers, metadata,
                   last_heartbeat_at, heartbeat_details, created_at, updated_at
            FROM task_executions
            WHERE service_identity = $1
            ORDER BY started_at DESC
            "#,
            )
            .bind(service_identity)
            .fetch_all(pool)
            .await
            .map_err(|e| {
                error!("Failed to find task executions by service_identity: {}", e);
                OrcherError::internal(format!("Failed to find task executions: {}", e))
            })?;
            rows.extend(found);
        }
        rows.sort_by_key(|r| std::cmp::Reverse(r.started_at));

        Ok(rows.into_iter().map(Into::into).collect())
    }

    /// Find task executions by task_id (across all workflows and attempts)
    pub async fn find_by_task_id(&self, task_id: &str) -> OrcherResult<Vec<TaskExecution>> {
        debug!("Finding task executions by task_id: {}", task_id);

        // Not keyed by the routing workflow id: gather every pool.
        let mut rows: Vec<TaskExecutionRow> = Vec::new();
        for pool in self.scan_pools() {
            let found: Vec<TaskExecutionRow> = sqlx::query_as(
                r#"
            SELECT id, organization_id, task_id, task_type, workflow_id, execution_id,
                   task_queue, namespace, service_identity, attempt_number, status,
                   scheduled_at, started_at, completed_at, task_token,
                   schedule_to_start_timeout_secs, start_to_close_timeout_secs,
                   heartbeat_timeout_secs, input, result, error_message, error_type,
                   error_details, retry_policy, headers, metadata,
                   last_heartbeat_at, heartbeat_details, created_at, updated_at
            FROM task_executions
            WHERE task_id = $1
            ORDER BY attempt_number ASC, scheduled_at ASC
            "#,
            )
            .bind(task_id)
            .fetch_all(pool)
            .await
            .map_err(|e| {
                error!("Failed to find task executions by task_id: {}", e);
                OrcherError::internal(format!("Failed to find task executions: {}", e))
            })?;
            rows.extend(found);
        }
        rows.sort_by(|a, b| {
            a.attempt_number
                .cmp(&b.attempt_number)
                .then_with(|| a.scheduled_at.cmp(&b.scheduled_at))
        });

        Ok(rows.into_iter().map(Into::into).collect())
    }

    /// Find task executions for a specific task within a workflow
    /// Find task execution by idempotency key (workflow_id, execution_id, task_id)
    ///
    /// Used to prevent duplicate task scheduling during workflow replay.
    /// Returns the most recent task execution for the given combination.
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - The workflow ID
    /// * `execution_id` - The execution ID (run ID)
    /// * `task_id` - The task ID (step name)
    ///
    /// # Returns
    ///
    /// The task execution if found, None otherwise
    pub async fn find_by_idempotency_key(
        &self,
        workflow_id: WorkflowId,
        execution_id: ExecutionId,
        task_id: &str,
    ) -> OrcherResult<Option<TaskExecution>> {
        debug!(
            "Finding task execution by idempotency key: workflow_id={}, execution_id={}, task_id={}",
            workflow_id, execution_id, task_id
        );

        let row: Option<TaskExecutionRow> = sqlx::query_as(
            r#"
            SELECT id, organization_id, task_id, task_type, workflow_id, execution_id,
                   task_queue, namespace, service_identity, attempt_number, status,
                   scheduled_at, started_at, completed_at, task_token,
                   schedule_to_start_timeout_secs, start_to_close_timeout_secs,
                   heartbeat_timeout_secs, input, result, error_message, error_type,
                   error_details, retry_policy, headers, metadata,
                   last_heartbeat_at, heartbeat_details, created_at, updated_at
            FROM task_executions
            WHERE workflow_id = $1 AND execution_id = $2 AND task_id = $3
            ORDER BY attempt_number DESC
            LIMIT 1
            "#,
        )
        .bind(workflow_id.as_uuid())
        .bind(execution_id.as_uuid())
        .bind(task_id)
        .fetch_optional(self.pool_for(workflow_id.as_uuid()))
        .await
        .map_err(|e| {
            error!("Failed to find task execution by idempotency key: {}", e);
            OrcherError::internal(format!("Failed to find task execution: {}", e))
        })?;

        Ok(row.map(Into::into))
    }

    /// Find task executions by workflow ID and task ID
    pub async fn find_by_workflow_and_task(
        &self,
        workflow_id: WorkflowId,
        task_id: &str,
    ) -> OrcherResult<Vec<TaskExecution>> {
        debug!(
            "Finding task executions by workflow_id={} and task_id={}",
            workflow_id, task_id
        );

        let rows: Vec<TaskExecutionRow> = sqlx::query_as(
            r#"
            SELECT id, organization_id, task_id, task_type, workflow_id, execution_id,
                   task_queue, namespace, service_identity, attempt_number, status,
                   scheduled_at, started_at, completed_at, task_token,
                   schedule_to_start_timeout_secs, start_to_close_timeout_secs,
                   heartbeat_timeout_secs, input, result, error_message, error_type,
                   error_details, retry_policy, headers, metadata,
                   last_heartbeat_at, heartbeat_details, created_at, updated_at
            FROM task_executions
            WHERE workflow_id = $1 AND task_id = $2
            ORDER BY attempt_number ASC
            "#,
        )
        .bind(workflow_id.as_uuid())
        .bind(task_id)
        .fetch_all(self.pool_for(workflow_id.as_uuid()))
        .await
        .map_err(|e| {
            error!("Failed to find task executions by workflow and task: {}", e);
            OrcherError::internal(format!("Failed to find task executions: {}", e))
        })?;

        Ok(rows.into_iter().map(Into::into).collect())
    }

    /// Find failed task executions within a time window
    pub async fn find_recent_failures(
        &self,
        since: Timestamptz,
        limit: i64,
    ) -> OrcherResult<Vec<TaskExecution>> {
        debug!("Finding recent failed task executions since: {}", since);

        // Not keyed by the routing workflow id: gather every pool.
        let mut rows: Vec<TaskExecutionRow> = Vec::new();
        for pool in self.scan_pools() {
            let found: Vec<TaskExecutionRow> = sqlx::query_as(
                r#"
            SELECT id, organization_id, task_id, task_type, workflow_id, execution_id,
                   task_queue, namespace, service_identity, attempt_number, status,
                   scheduled_at, started_at, completed_at, task_token,
                   schedule_to_start_timeout_secs, start_to_close_timeout_secs,
                   heartbeat_timeout_secs, input, result, error_message, error_type,
                   error_details, retry_policy, headers, metadata,
                   last_heartbeat_at, heartbeat_details, created_at, updated_at
            FROM task_executions
            WHERE status IN ('failed', 'timed_out')
              AND completed_at >= $1
            ORDER BY completed_at DESC
            LIMIT $2
            "#,
            )
            .bind(since)
            .bind(limit)
            .fetch_all(pool)
            .await
            .map_err(|e| {
                error!("Failed to find recent failed task executions: {}", e);
                OrcherError::internal(format!("Failed to find task executions: {}", e))
            })?;
            rows.extend(found);
        }
        rows.sort_by_key(|r| std::cmp::Reverse(r.completed_at));
        rows.truncate(limit.max(0) as usize);

        Ok(rows.into_iter().map(Into::into).collect())
    }

    // ========== ANALYTICS / STATISTICS ==========

    /// Count task executions by status
    pub async fn count_by_status(&self, status: &str) -> OrcherResult<i64> {
        debug!("Counting task executions by status: {}", status);

        // Summed over every pool.
        let mut total = 0;
        for pool in self.scan_pools() {
            let count: (i64,) = sqlx::query_as(
                r#"
                SELECT COUNT(*) FROM task_executions WHERE status = $1
                "#,
            )
            .bind(status)
            .fetch_one(pool)
            .await
            .map_err(|e| {
                error!("Failed to count task executions by status: {}", e);
                OrcherError::internal(format!("Failed to count task executions: {}", e))
            })?;
            total += count.0;
        }

        Ok(total)
    }

    /// Get average task duration by task type
    pub async fn average_duration_by_type(&self, task_type: &str) -> OrcherResult<Option<f64>> {
        debug!("Calculating average duration for task_type: {}", task_type);

        // A sum and a count per pool, so the average over every pool is exact.
        let (mut seconds, mut count) = (0.0_f64, 0_i64);
        for pool in self.scan_pools() {
            let (sum, n): (Option<f64>, i64) = sqlx::query_as(
                r#"
                SELECT SUM(EXTRACT(EPOCH FROM (completed_at - started_at)))::float8, COUNT(*)
                FROM task_executions
                WHERE task_type = $1
                  AND status = 'completed'
                  AND started_at IS NOT NULL
                  AND completed_at IS NOT NULL
                "#,
            )
            .bind(task_type)
            .fetch_one(pool)
            .await
            .map_err(|e| {
                error!("Failed to calculate average duration: {}", e);
                OrcherError::internal(format!("Failed to calculate average duration: {}", e))
            })?;
            seconds += sum.unwrap_or(0.0);
            count += n;
        }

        Ok((count > 0).then(|| seconds / count as f64))
    }

    /// Calculate success rate for a task type
    pub async fn success_rate_by_type(&self, task_type: &str) -> OrcherResult<f64> {
        debug!("Calculating success rate for task_type: {}", task_type);

        // Counted on every pool and summed.
        let (mut completed, mut total) = (0_i64, 0_i64);
        for pool in self.scan_pools() {
            let (c, t): (i64, i64) = sqlx::query_as(
                r#"
                SELECT
                    COUNT(*) FILTER (WHERE status = 'completed') as completed,
                    COUNT(*) as total
                FROM task_executions
                WHERE task_type = $1
                  AND status IN ('completed', 'failed', 'timed_out')
                "#,
            )
            .bind(task_type)
            .fetch_one(pool)
            .await
            .map_err(|e| {
                error!("Failed to calculate success rate: {}", e);
                OrcherError::internal(format!("Failed to calculate success rate: {}", e))
            })?;
            completed += c;
            total += t;
        }
        if total == 0 {
            Ok(0.0)
        } else {
            Ok((completed as f64) / (total as f64) * 100.0)
        }
    }

    /// Whether any of the workflow's tasks is still unfinished (`scheduled`
    /// or `started`).
    ///
    /// What a task's completion needs to decide whether to wake its workflow,
    /// answered from `idx_task_executions_open` (V077), which holds only
    /// unfinished tasks: its cost does not grow with the workflow's length,
    /// where counting every task by status read all of them on every
    /// completion.
    pub async fn has_open_tasks(&self, workflow_id: WorkflowId) -> OrcherResult<bool> {
        sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (SELECT 1 FROM task_executions
                           WHERE workflow_id = $1 AND status IN ('scheduled', 'started'))
            "#,
        )
        .bind(workflow_id.as_uuid())
        .fetch_one(self.pool_for(workflow_id.as_uuid()))
        .await
        .map_err(|e| {
            error!(workflow_id = %workflow_id, error = %e, "Failed to check the workflow's open tasks");
            OrcherError::internal(format!("Failed to check the workflow's open tasks: {}", e))
        })
    }

    /// Get task execution summary for a workflow
    ///
    /// Returns counts of tasks in different states for efficient workflow resume logic.
    /// This method uses a single efficient query with COUNT and FILTER to get all
    /// state counts in one database round-trip.
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - UUID of the workflow to summarize
    ///
    /// # Returns
    ///
    /// * `Ok(WorkflowTaskSummary)` - Summary with counts by status
    /// * `Err(_)` - If the query fails
    pub async fn get_workflow_task_summary(
        &self,
        workflow_id: WorkflowId,
    ) -> OrcherResult<WorkflowTaskSummary> {
        debug!(
            workflow_id = %workflow_id,
            "Getting task execution summary for workflow"
        );

        let result: Option<(i64, i64, i64, i64, i64, i64, i64)> = sqlx::query_as(
            r#"
            SELECT
                COUNT(*) as total_tasks,
                COUNT(*) FILTER (WHERE status = 'completed') as completed_tasks,
                COUNT(*) FILTER (WHERE status = 'failed') as failed_tasks,
                COUNT(*) FILTER (WHERE status IN ('cancelled', 'canceled')) as canceled_tasks,
                COUNT(*) FILTER (WHERE status = 'timed_out') as timed_out_tasks,
                COUNT(*) FILTER (WHERE status = 'started') as running_tasks,
                COUNT(*) FILTER (WHERE status = 'scheduled') as scheduled_tasks
            FROM task_executions
            WHERE workflow_id = $1
            "#,
        )
        .bind(workflow_id.as_uuid())
        // Read the summary from the workflow's shard (task rows are co-located there).
        .fetch_optional(self.pool_for(workflow_id.as_uuid()))
        .await
        .map_err(|e| {
            error!(
                workflow_id = %workflow_id,
                error = %e,
                "Failed to get workflow task summary"
            );
            OrcherError::internal(format!("Failed to get workflow task summary: {}", e))
        })?;

        let (
            total_tasks,
            completed_tasks,
            failed_tasks,
            canceled_tasks,
            timed_out_tasks,
            running_tasks,
            scheduled_tasks,
        ) = result.unwrap_or((0, 0, 0, 0, 0, 0, 0));

        debug!(
            workflow_id = %workflow_id,
            total = total_tasks,
            completed = completed_tasks,
            failed = failed_tasks,
            canceled = canceled_tasks,
            timed_out = timed_out_tasks,
            running = running_tasks,
            scheduled = scheduled_tasks,
            "Workflow task summary retrieved"
        );

        Ok(WorkflowTaskSummary {
            total_tasks,
            completed_tasks,
            failed_tasks,
            canceled_tasks,
            timed_out_tasks,
            running_tasks,
            scheduled_tasks,
        })
    }
}

// ================================================================================================
// CRUD OPERATIONS TRAIT IMPLEMENTATION
// ================================================================================================

#[async_trait]
impl CrudOperations<TaskExecution, TaskExecutionId> for TaskExecutionRepository {
    async fn create(&self, entity: TaskExecution) -> OrcherResult<TaskExecution> {
        debug!("Creating task execution via CRUD trait");

        // No `shard_id`: unlike the scheduling path above this writes to the primary
        // pool without routing, so a shard stamped here could name a shard the row
        // does not live on. Nothing in the engine reaches this — the scheduling path
        // is what creates task rows — so route it before stamping it.

        let row: TaskExecutionRow = sqlx::query_as(
            r#"
            INSERT INTO task_executions (
                id, task_id, task_type, workflow_id, execution_id,
                task_queue, namespace, service_identity, attempt_number, status,
                scheduled_at, started_at, completed_at, task_token,
                schedule_to_start_timeout_secs, start_to_close_timeout_secs,
                heartbeat_timeout_secs, input, result, error_message, error_type,
                error_details, retry_policy, headers, metadata,
                last_heartbeat_at, heartbeat_details, created_at, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29)
            RETURNING id, organization_id, task_id, task_type, workflow_id, execution_id,
                      task_queue, namespace, service_identity, attempt_number, status,
                      scheduled_at, started_at, completed_at, task_token,
                      schedule_to_start_timeout_secs, start_to_close_timeout_secs,
                      heartbeat_timeout_secs, input, result, error_message, error_type,
                      error_details, retry_policy, headers, metadata,
                      last_heartbeat_at, heartbeat_details, created_at, updated_at
            "#,
        )
        .bind(entity.id.as_uuid())
        .bind(&entity.task_id)
        .bind(&entity.task_type)
        .bind(entity.workflow_id.as_uuid())
        .bind(entity.execution_id.as_uuid())
        .bind(&entity.task_queue)
        .bind(&entity.namespace)
        .bind(&entity.service_identity)
        .bind(entity.attempt_number)
        .bind(&entity.status)
        .bind(entity.scheduled_at)
        .bind(entity.started_at)
        .bind(entity.completed_at)
        .bind(&entity.task_token)
        .bind(entity.schedule_to_start_timeout_secs)
        .bind(entity.start_to_close_timeout_secs)
        .bind(entity.heartbeat_timeout_secs)
        .bind(&entity.input)
        .bind(&entity.result)
        .bind(&entity.error_message)
        .bind(&entity.error_type)
        .bind(&entity.error_details)
        .bind(&entity.retry_policy)
        .bind(&entity.headers)
        .bind(&entity.metadata)
        .bind(entity.last_heartbeat_at)
        .bind(&entity.heartbeat_details)
        .bind(entity.created_at)
        .bind(entity.updated_at)
        .fetch_one(self.pool_for(entity.workflow_id.as_uuid()))
        .await
        .map_err(|e| {
            error!("Failed to create task execution: {}", e);
            OrcherError::internal(format!("Failed to create task execution: {}", e))
        })?;

        Ok(row.into())
    }

    async fn find_by_id(&self, id: TaskExecutionId) -> OrcherResult<Option<TaskExecution>> {
        self.find_by_id(id).await
    }

    async fn update(&self, entity: TaskExecution) -> OrcherResult<TaskExecution> {
        debug!("Updating task execution: id={}", entity.id);

        let now = Timestamptz::now();

        let row: TaskExecutionRow = sqlx::query_as(
            r#"
            UPDATE task_executions
            SET task_id = $2,
                task_type = $3,
                workflow_id = $4,
                execution_id = $5,
                task_queue = $6,
                namespace = $7,
                service_identity = $8,
                attempt_number = $9,
                status = $10,
                scheduled_at = $11,
                started_at = $12,
                completed_at = $13,
                task_token = $14,
                schedule_to_start_timeout_secs = $15,
                start_to_close_timeout_secs = $16,
                heartbeat_timeout_secs = $17,
                input = $18,
                result = $19,
                error_message = $20,
                error_type = $21,
                error_details = $22,
                retry_policy = $23,
                headers = $24,
                metadata = $25,
                last_heartbeat_at = $26,
                heartbeat_details = $27,
                updated_at = $28
            WHERE id = $1
            RETURNING id, organization_id, task_id, task_type, workflow_id, execution_id,
                      task_queue, namespace, service_identity, attempt_number, status,
                      scheduled_at, started_at, completed_at, task_token,
                      schedule_to_start_timeout_secs, start_to_close_timeout_secs,
                      heartbeat_timeout_secs, input, result, error_message, error_type,
                      error_details, retry_policy, headers, metadata,
                      last_heartbeat_at, heartbeat_details, created_at, updated_at
            "#,
        )
        .bind(entity.id.as_uuid())
        .bind(&entity.task_id)
        .bind(&entity.task_type)
        .bind(entity.workflow_id.as_uuid())
        .bind(entity.execution_id.as_uuid())
        .bind(&entity.task_queue)
        .bind(&entity.namespace)
        .bind(&entity.service_identity)
        .bind(entity.attempt_number)
        .bind(&entity.status)
        .bind(entity.scheduled_at)
        .bind(entity.started_at)
        .bind(entity.completed_at)
        .bind(&entity.task_token)
        .bind(entity.schedule_to_start_timeout_secs)
        .bind(entity.start_to_close_timeout_secs)
        .bind(entity.heartbeat_timeout_secs)
        .bind(&entity.input)
        .bind(&entity.result)
        .bind(&entity.error_message)
        .bind(&entity.error_type)
        .bind(&entity.error_details)
        .bind(&entity.retry_policy)
        .bind(&entity.headers)
        .bind(&entity.metadata)
        .bind(entity.last_heartbeat_at)
        .bind(&entity.heartbeat_details)
        .bind(now)
        .fetch_one(self.pool_for(entity.workflow_id.as_uuid()))
        .await
        .map_err(|e| {
            error!("Failed to update task execution: {}", e);
            OrcherError::internal(format!("Failed to update task execution: {}", e))
        })?;

        Ok(row.into())
    }

    async fn delete(&self, id: TaskExecutionId) -> OrcherResult<bool> {
        debug!("Deleting task execution: id={}", id);

        let mut result = sqlx::postgres::PgQueryResult::default();
        for pool in self.scan_pools() {
            result = sqlx::query(
                r#"
                DELETE FROM task_executions WHERE id = $1
                "#,
            )
            .bind(id.as_uuid())
            .execute(pool)
            .await
            .map_err(|e| {
                error!("Failed to delete task execution: {}", e);
                OrcherError::internal(format!("Failed to delete task execution: {}", e))
            })?;
            if result.rows_affected() > 0 {
                break;
            }
        }

        let deleted = result.rows_affected() > 0;
        if deleted {
            info!("Deleted task execution: id={}", id);
        } else {
            warn!("Task execution not found for deletion: id={}", id);
        }

        Ok(deleted)
    }

    async fn exists(&self, id: TaskExecutionId) -> OrcherResult<bool> {
        debug!("Checking if task execution exists: id={}", id);

        let mut count: (i64,) = (0,);
        for pool in self.scan_pools() {
            count = sqlx::query_as(
                r#"
                SELECT COUNT(*) FROM task_executions WHERE id = $1
                "#,
            )
            .bind(id.as_uuid())
            .fetch_one(pool)
            .await
            .map_err(|e| {
                error!("Failed to check task execution existence: {}", e);
                OrcherError::internal(format!("Failed to check task execution existence: {}", e))
            })?;
            if count.0 > 0 {
                break;
            }
        }

        Ok(count.0 > 0)
    }
}

// ========== SESSION FAIL-FAST CLEANUP ==========

impl TaskExecutionRepository {}

#[cfg(test)]
mod tests {
    use super::*;
    use orcher_common::types::identifiers::{ExecutionId, TaskExecutionId, WorkflowId};
    use sqlx::PgPool;
    use std::env;
    use uuid::Uuid;

    /// Test helper: Create a test database pool
    ///
    /// Set TEST_DATABASE_URL environment variable to use a specific test database.
    /// Example: postgresql://postgres:postgres@localhost:5432/orcher_test
    async fn create_test_pool() -> PgPool {
        let database_url = env::var("TEST_DATABASE_URL").unwrap_or_else(|_| {
            "postgresql://postgres:postgres@localhost:5432/orcher_test".to_string()
        });

        PgPool::connect(&database_url)
            .await
            .expect("Failed to connect to test database. Make sure PostgreSQL is running and TEST_DATABASE_URL is set.")
    }

    /// Test helper: Clean up test data after each test
    async fn cleanup_test_data(pool: &PgPool, task_execution_id: TaskExecutionId) {
        let _ = sqlx::query("DELETE FROM task_executions WHERE id = $1")
            .bind(task_execution_id.as_uuid())
            .execute(pool)
            .await;
    }

    /// Test helper: Create a sample scheduled task execution
    async fn create_sample_task(repo: &TaskExecutionRepository) -> OrcherResult<TaskExecution> {
        let workflow_id = WorkflowId::from_uuid(Uuid::new_v4());
        let execution_id = ExecutionId::from_uuid(Uuid::new_v4());

        repo.create_scheduled(
            "test-task-1".to_string(),
            "TestTask".to_string(),
            workflow_id,
            execution_id,
            "test-queue".to_string(),
            "default".to_string(),
            1,
            Some(b"test input".to_vec()),
            Some(serde_json::json!({"max_attempts": 3})),
            Some(300), // schedule_to_start_timeout
            Some(600), // start_to_close_timeout
            Some(30),  // heartbeat_timeout
            Some(serde_json::json!({"key": "value"})),
            Some(serde_json::json!({"meta": "data"})),
        )
        .await
    }

    #[test]
    fn test_task_execution_statuses() {
        let statuses = [
            "scheduled",
            "started",
            "completed",
            "failed",
            "timed_out",
            "cancelled",
        ];
        assert_eq!(statuses.len(), 6);
    }

    // ==================================================================================
    // UNIT TESTS: create_scheduled
    // ==================================================================================

    #[tokio::test]
    #[ignore] // Run with: cargo test --package orcher-storage -- --ignored
    async fn test_create_scheduled_task_execution() {
        let pool = create_test_pool().await;
        let repo = TaskExecutionRepository::new(pool.clone());

        let workflow_id = WorkflowId::from_uuid(Uuid::new_v4());
        let execution_id = ExecutionId::from_uuid(Uuid::new_v4());

        let task = repo
            .create_scheduled(
                "task-1".to_string(),
                "SendEmail".to_string(),
                workflow_id,
                execution_id,
                "email-queue".to_string(),
                "production".to_string(),
                1, // first attempt
                Some(b"test input data".to_vec()),
                Some(serde_json::json!({"max_attempts": 3, "initial_interval_ms": 1000})),
                Some(300),
                Some(600),
                Some(30),
                Some(serde_json::json!({"x-request-id": "req-123"})),
                Some(serde_json::json!({"priority": "high"})),
            )
            .await
            .expect("Failed to create scheduled task");

        // Verify task fields
        assert_eq!(task.task_id, "task-1");
        assert_eq!(task.task_type, "SendEmail");
        assert_eq!(task.workflow_id, workflow_id);
        assert_eq!(task.execution_id, execution_id);
        assert_eq!(task.task_queue, "email-queue");
        assert_eq!(task.namespace, "production");
        assert_eq!(task.attempt_number, 1);
        assert_eq!(task.status, "scheduled");
        assert!(task.service_identity.is_none());
        assert!(task.started_at.is_none());
        assert!(task.completed_at.is_none());
        assert!(task.task_token.is_none());
        assert_eq!(task.schedule_to_start_timeout_secs, Some(300));
        assert_eq!(task.start_to_close_timeout_secs, Some(600));
        assert_eq!(task.heartbeat_timeout_secs, Some(30));
        assert_eq!(task.input, Some(b"test input data".to_vec()));
        assert!(task.result.is_none());
        assert!(task.error_message.is_none());

        // Cleanup
        cleanup_test_data(&pool, task.id).await;
        pool.close().await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_create_scheduled_with_minimal_fields() {
        let pool = create_test_pool().await;
        let repo = TaskExecutionRepository::new(pool.clone());

        let workflow_id = WorkflowId::from_uuid(Uuid::new_v4());
        let execution_id = ExecutionId::from_uuid(Uuid::new_v4());

        let task = repo
            .create_scheduled(
                "minimal-task".to_string(),
                "MinimalTask".to_string(),
                workflow_id,
                execution_id,
                "default-queue".to_string(),
                "default".to_string(),
                1,
                None, // no input
                None, // no retry policy
                None, // no schedule_to_start timeout
                None, // no start_to_close timeout
                None, // no heartbeat timeout
                None, // no headers
                None, // no metadata
            )
            .await
            .expect("Failed to create minimal task");

        assert_eq!(task.status, "scheduled");
        assert!(task.input.is_none());
        assert!(task.retry_policy.is_none());
        assert_eq!(task.headers, serde_json::json!({}));
        assert_eq!(task.metadata, serde_json::json!({}));

        cleanup_test_data(&pool, task.id).await;
        pool.close().await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_create_scheduled_with_retry_attempt() {
        let pool = create_test_pool().await;
        let repo = TaskExecutionRepository::new(pool.clone());

        let workflow_id = WorkflowId::from_uuid(Uuid::new_v4());
        let execution_id = ExecutionId::from_uuid(Uuid::new_v4());

        // Create second attempt (retry)
        let task = repo
            .create_scheduled(
                "retry-task".to_string(),
                "RetryTask".to_string(),
                workflow_id,
                execution_id,
                "retry-queue".to_string(),
                "default".to_string(),
                2, // second attempt
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .await
            .expect("Failed to create retry task");

        assert_eq!(task.attempt_number, 2);
        assert_eq!(task.status, "scheduled");

        cleanup_test_data(&pool, task.id).await;
        pool.close().await;
    }

    // ==================================================================================
    // UNIT TESTS: mark_started
    // ==================================================================================

    #[tokio::test]
    #[ignore]
    async fn test_mark_started_success() {
        let pool = create_test_pool().await;
        let repo = TaskExecutionRepository::new(pool.clone());

        // Create a scheduled task
        let task = create_sample_task(&repo)
            .await
            .expect("Failed to create task");
        assert_eq!(task.status, "scheduled");

        // Mark as started
        let started_task = repo
            .mark_started(
                task.id,
                "service-worker-123".to_string(),
                "task-token-abc-xyz".to_string(),
            )
            .await
            .expect("Failed to mark task as started");

        assert_eq!(started_task.status, "started");
        assert_eq!(
            started_task.service_identity,
            Some("service-worker-123".to_string())
        );
        assert_eq!(
            started_task.task_token,
            Some("task-token-abc-xyz".to_string())
        );
        assert!(started_task.started_at.is_some());
        assert!(started_task.completed_at.is_none());

        cleanup_test_data(&pool, task.id).await;
        pool.close().await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_mark_started_idempotency() {
        let pool = create_test_pool().await;
        let repo = TaskExecutionRepository::new(pool.clone());

        let task = create_sample_task(&repo)
            .await
            .expect("Failed to create task");

        // Mark as started
        let _ = repo
            .mark_started(task.id, "service-1".to_string(), "token-1".to_string())
            .await
            .expect("Failed to mark as started");

        // Try to mark started again (should fail - status is no longer 'scheduled')
        let result = repo
            .mark_started(task.id, "service-2".to_string(), "token-2".to_string())
            .await;

        assert!(result.is_err(), "Should not allow marking started twice");

        cleanup_test_data(&pool, task.id).await;
        pool.close().await;
    }

    // ==================================================================================
    // UNIT TESTS: mark_completed
    // ==================================================================================

    #[tokio::test]
    #[ignore]
    async fn test_mark_completed_success() {
        let pool = create_test_pool().await;
        let repo = TaskExecutionRepository::new(pool.clone());

        // Create and start a task
        let task = create_sample_task(&repo)
            .await
            .expect("Failed to create task");
        let started = repo
            .mark_started(task.id, "service-1".to_string(), "token-1".to_string())
            .await
            .expect("Failed to mark started");

        // Mark as completed
        let result_data = b"task result data".to_vec();
        let completed = repo
            .mark_completed(started.id, result_data.clone())
            .await
            .expect("Failed to mark completed");

        assert_eq!(completed.status, "completed");
        assert_eq!(completed.result, Some(result_data));
        assert!(completed.completed_at.is_some());
        assert!(completed.error_message.is_none());

        cleanup_test_data(&pool, task.id).await;
        pool.close().await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_mark_completed_requires_started_status() {
        let pool = create_test_pool().await;
        let repo = TaskExecutionRepository::new(pool.clone());

        // Create a scheduled task (not started)
        let task = create_sample_task(&repo)
            .await
            .expect("Failed to create task");

        // Try to mark as completed without starting
        let result = repo.mark_completed(task.id, b"result".to_vec()).await;

        assert!(
            result.is_err(),
            "Should not allow completing a non-started task"
        );

        cleanup_test_data(&pool, task.id).await;
        pool.close().await;
    }

    // ==================================================================================
    // UNIT TESTS: mark_failed
    // ==================================================================================

    #[tokio::test]
    #[ignore]
    async fn test_mark_failed_success() {
        let pool = create_test_pool().await;
        let repo = TaskExecutionRepository::new(pool.clone());

        // Create and start a task
        let task = create_sample_task(&repo)
            .await
            .expect("Failed to create task");
        let started = repo
            .mark_started(task.id, "service-1".to_string(), "token-1".to_string())
            .await
            .expect("Failed to mark started");

        // Mark as failed
        let failed = repo
            .mark_failed(
                started.id,
                "Connection timeout".to_string(),
                Some("NetworkError".to_string()),
                Some(serde_json::json!({"timeout_ms": 5000, "host": "api.example.com"})),
            )
            .await
            .expect("Failed to mark failed");

        assert_eq!(failed.status, "failed");
        assert_eq!(failed.error_message, Some("Connection timeout".to_string()));
        assert_eq!(failed.error_type, Some("NetworkError".to_string()));
        assert!(failed.error_details.is_some());
        assert!(failed.completed_at.is_some());
        assert!(failed.result.is_none());

        cleanup_test_data(&pool, task.id).await;
        pool.close().await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_mark_failed_with_minimal_error_info() {
        let pool = create_test_pool().await;
        let repo = TaskExecutionRepository::new(pool.clone());

        let task = create_sample_task(&repo)
            .await
            .expect("Failed to create task");
        let started = repo
            .mark_started(task.id, "service-1".to_string(), "token-1".to_string())
            .await
            .expect("Failed to mark started");

        // Mark as failed with minimal error info
        let failed = repo
            .mark_failed(started.id, "Unknown error".to_string(), None, None)
            .await
            .expect("Failed to mark failed");

        assert_eq!(failed.status, "failed");
        assert_eq!(failed.error_message, Some("Unknown error".to_string()));
        assert!(failed.error_type.is_none());
        assert!(failed.error_details.is_none());

        cleanup_test_data(&pool, task.id).await;
        pool.close().await;
    }

    // ==================================================================================
    // UNIT TESTS: mark_timed_out
    // ==================================================================================

    #[tokio::test]
    #[ignore]
    async fn test_mark_timed_out_success() {
        let pool = create_test_pool().await;
        let repo = TaskExecutionRepository::new(pool.clone());

        let task = create_sample_task(&repo)
            .await
            .expect("Failed to create task");

        // Mark as timed out (can happen in scheduled or started state)
        let timed_out = repo
            .mark_timed_out(task.id, "schedule-to-start".to_string())
            .await
            .expect("Failed to mark timed out");

        assert_eq!(timed_out.status, "timed_out");
        assert_eq!(timed_out.error_type, Some("schedule-to-start".to_string()));
        assert!(timed_out.completed_at.is_some());

        cleanup_test_data(&pool, task.id).await;
        pool.close().await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_mark_timed_out_started_task() {
        let pool = create_test_pool().await;
        let repo = TaskExecutionRepository::new(pool.clone());

        let task = create_sample_task(&repo)
            .await
            .expect("Failed to create task");
        let started = repo
            .mark_started(task.id, "service-1".to_string(), "token-1".to_string())
            .await
            .expect("Failed to mark started");

        // Mark as timed out (start-to-close timeout)
        let timed_out = repo
            .mark_timed_out(started.id, "start-to-close".to_string())
            .await
            .expect("Failed to mark timed out");

        assert_eq!(timed_out.status, "timed_out");
        assert_eq!(timed_out.error_type, Some("start-to-close".to_string()));

        cleanup_test_data(&pool, task.id).await;
        pool.close().await;
    }

    // ==================================================================================
    // UNIT TESTS: update_heartbeat
    // ==================================================================================

    #[tokio::test]
    #[ignore]
    async fn test_update_heartbeat_success() {
        let pool = create_test_pool().await;
        let repo = TaskExecutionRepository::new(pool.clone());

        let task = create_sample_task(&repo)
            .await
            .expect("Failed to create task");
        let started = repo
            .mark_started(task.id, "service-1".to_string(), "token-1".to_string())
            .await
            .expect("Failed to mark started");

        // Update heartbeat
        let heartbeat_details = b"progress: 50%".to_vec();
        let updated = repo
            .update_heartbeat(started.id, Some(heartbeat_details.clone()))
            .await
            .expect("Failed to update heartbeat");

        assert!(updated.last_heartbeat_at.is_some());
        assert_eq!(updated.heartbeat_details, Some(heartbeat_details));
        assert_eq!(updated.status, "started"); // Status unchanged

        cleanup_test_data(&pool, task.id).await;
        pool.close().await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_update_heartbeat_without_details() {
        let pool = create_test_pool().await;
        let repo = TaskExecutionRepository::new(pool.clone());

        let task = create_sample_task(&repo)
            .await
            .expect("Failed to create task");
        let started = repo
            .mark_started(task.id, "service-1".to_string(), "token-1".to_string())
            .await
            .expect("Failed to mark started");

        // Update heartbeat without details
        let updated = repo
            .update_heartbeat(started.id, None)
            .await
            .expect("Failed to update heartbeat");

        assert!(updated.last_heartbeat_at.is_some());
        assert!(updated.heartbeat_details.is_none());

        cleanup_test_data(&pool, task.id).await;
        pool.close().await;
    }

    // ==================================================================================
    // UNIT TESTS: Query Methods
    // ==================================================================================

    #[tokio::test]
    #[ignore]
    async fn test_find_by_id_success() {
        let pool = create_test_pool().await;
        let repo = TaskExecutionRepository::new(pool.clone());

        let task = create_sample_task(&repo)
            .await
            .expect("Failed to create task");

        // Find by ID
        let found = repo.find_by_id(task.id).await.expect("Failed to find task");

        assert!(found.is_some());
        let found = found.unwrap();
        assert_eq!(found.id, task.id);
        assert_eq!(found.task_id, task.task_id);
        assert_eq!(found.status, "scheduled");

        cleanup_test_data(&pool, task.id).await;
        pool.close().await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_find_by_id_not_found() {
        let pool = create_test_pool().await;
        let repo = TaskExecutionRepository::new(pool.clone());

        let non_existent_id = TaskExecutionId::from_uuid(Uuid::new_v4());

        let found = repo
            .find_by_id(non_existent_id)
            .await
            .expect("Query should succeed");

        assert!(found.is_none());

        pool.close().await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_find_by_workflow_id() {
        let pool = create_test_pool().await;
        let repo = TaskExecutionRepository::new(pool.clone());

        let workflow_id = WorkflowId::from_uuid(Uuid::new_v4());
        let execution_id = ExecutionId::from_uuid(Uuid::new_v4());

        // Create multiple tasks for the same workflow
        let task1 = repo
            .create_scheduled(
                "task-1".to_string(),
                "Task1".to_string(),
                workflow_id,
                execution_id,
                "queue".to_string(),
                "default".to_string(),
                1,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .await
            .expect("Failed to create task1");

        let task2 = repo
            .create_scheduled(
                "task-2".to_string(),
                "Task2".to_string(),
                workflow_id,
                execution_id,
                "queue".to_string(),
                "default".to_string(),
                1,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .await
            .expect("Failed to create task2");

        // Find by workflow ID
        let tasks = repo
            .find_by_workflow_id(workflow_id)
            .await
            .expect("Failed to find tasks");

        assert!(tasks.len() >= 2);
        assert!(tasks.iter().any(|t| t.id == task1.id));
        assert!(tasks.iter().any(|t| t.id == task2.id));

        cleanup_test_data(&pool, task1.id).await;
        cleanup_test_data(&pool, task2.id).await;
        pool.close().await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_find_by_task_token() {
        let pool = create_test_pool().await;
        let repo = TaskExecutionRepository::new(pool.clone());

        let task = create_sample_task(&repo)
            .await
            .expect("Failed to create task");
        let started = repo
            .mark_started(
                task.id,
                "service-1".to_string(),
                "unique-token-123".to_string(),
            )
            .await
            .expect("Failed to mark started");

        // Find by task token
        let found = repo
            .find_by_task_token("unique-token-123")
            .await
            .expect("Failed to find by token");

        assert!(found.is_some());
        let found = found.unwrap();
        assert_eq!(found.id, started.id);
        assert_eq!(found.task_token, Some("unique-token-123".to_string()));

        cleanup_test_data(&pool, task.id).await;
        pool.close().await;
    }

    /// A task is owned by its execution. Task rows have never had their own
    /// `organization_id` written — every one in a real deployment was found
    /// unowned while every execution was owned — so the completion guard, which
    /// authorises on what this lookup reports, refused every tenant's own
    /// completions until it reported the execution's owner instead.
    #[tokio::test]
    #[ignore]
    async fn a_task_found_by_token_carries_its_executions_owner() {
        let pool = create_test_pool().await;
        let repo = TaskExecutionRepository::new(pool.clone());

        let owner_user = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO users (id, email, password_hash, hash_algorithm, password_changed_at) \
             VALUES ($1, $2, 'x', 'argon2', NOW())",
        )
        .bind(owner_user)
        .bind(format!("owner-{owner_user}@repository.test"))
        .execute(&pool)
        .await
        .expect("seed user");

        let organization = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO organizations (id, name, display_name, slug, owner_id) \
             VALUES ($1, $2, $2, $2, $3)",
        )
        .bind(organization)
        .bind(format!("org-{organization}"))
        .bind(owner_user)
        .execute(&pool)
        .await
        .expect("seed organization");

        let execution = Uuid::new_v4();
        sqlx::query(
            r#"INSERT INTO workflow_executions
                 (id, execution_id, workflow_id, workflow_name, workflow_type, workflow_version,
                  namespace, task_queue, status, trigger_type, organization_id,
                  submitted_at, created_at, updated_at)
               VALUES ($1, $2, $1, 'owner-test', 'owner-test', '1.0.0', 'default', 'q',
                       'running', 'api', $3, NOW(), NOW(), NOW())"#,
        )
        .bind(execution)
        .bind(execution.to_string())
        .bind(organization)
        .execute(&pool)
        .await
        .expect("seed execution");

        // The task row itself is created the way the engine creates it: with
        // no owner of its own.
        let task = repo
            .create_scheduled(
                "owned-task".to_string(),
                "OwnedTask".to_string(),
                WorkflowId::from_uuid(execution),
                ExecutionId::from_uuid(Uuid::new_v4()),
                "q".to_string(),
                "default".to_string(),
                1,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .await
            .expect("create task");
        let token = format!("owner-token-{}", Uuid::new_v4());
        repo.mark_started(task.id, "svc".to_string(), token.clone())
            .await
            .expect("mark started");

        let found = repo
            .find_by_task_token(&token)
            .await
            .expect("lookup")
            .expect("task found");
        assert_eq!(
            found.organization_id,
            Some(organization),
            "the task reported the row's own (empty) owner instead of its execution's"
        );

        cleanup_test_data(&pool, task.id).await;
        let _ = sqlx::query("DELETE FROM workflow_executions WHERE id = $1")
            .bind(execution)
            .execute(&pool)
            .await;
        let _ = sqlx::query("DELETE FROM organizations WHERE id = $1")
            .bind(organization)
            .execute(&pool)
            .await;
        let _ = sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(owner_user)
            .execute(&pool)
            .await;
        pool.close().await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_find_by_status() {
        let pool = create_test_pool().await;
        let repo = TaskExecutionRepository::new(pool.clone());

        // Create a scheduled task
        let task = create_sample_task(&repo)
            .await
            .expect("Failed to create task");

        // Find scheduled tasks
        let scheduled = repo
            .find_by_status("scheduled")
            .await
            .expect("Failed to find by status");

        assert!(scheduled.iter().any(|t| t.id == task.id));

        cleanup_test_data(&pool, task.id).await;
        pool.close().await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_exists() {
        let pool = create_test_pool().await;
        let repo = TaskExecutionRepository::new(pool.clone());

        let task = create_sample_task(&repo)
            .await
            .expect("Failed to create task");

        // Check exists
        let exists = repo.exists(task.id).await.expect("Failed to check exists");

        assert!(exists);

        // Check non-existent
        let non_existent_id = TaskExecutionId::from_uuid(Uuid::new_v4());
        let not_exists = repo
            .exists(non_existent_id)
            .await
            .expect("Failed to check exists");

        assert!(!not_exists);

        cleanup_test_data(&pool, task.id).await;
        pool.close().await;
    }

    // ==================================================================================
    // UNIT TESTS: Lifecycle Transitions
    // ==================================================================================

    #[tokio::test]
    #[ignore]
    async fn test_full_lifecycle_success() {
        let pool = create_test_pool().await;
        let repo = TaskExecutionRepository::new(pool.clone());

        // 1. Create scheduled
        let task = create_sample_task(&repo)
            .await
            .expect("Failed to create task");
        assert_eq!(task.status, "scheduled");

        // 2. Mark started
        let started = repo
            .mark_started(task.id, "service-1".to_string(), "token-1".to_string())
            .await
            .expect("Failed to mark started");
        assert_eq!(started.status, "started");

        // 3. Update heartbeat
        let heartbeat = repo
            .update_heartbeat(started.id, Some(b"50%".to_vec()))
            .await
            .expect("Failed to update heartbeat");
        assert!(heartbeat.last_heartbeat_at.is_some());

        // 4. Mark completed
        let completed = repo
            .mark_completed(started.id, b"final result".to_vec())
            .await
            .expect("Failed to mark completed");
        assert_eq!(completed.status, "completed");
        assert!(completed.result.is_some());

        cleanup_test_data(&pool, task.id).await;
        pool.close().await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_full_lifecycle_failure() {
        let pool = create_test_pool().await;
        let repo = TaskExecutionRepository::new(pool.clone());

        // 1. Create scheduled
        let task = create_sample_task(&repo)
            .await
            .expect("Failed to create task");

        // 2. Mark started
        let started = repo
            .mark_started(task.id, "service-1".to_string(), "token-1".to_string())
            .await
            .expect("Failed to mark started");

        // 3. Mark failed
        let failed = repo
            .mark_failed(
                started.id,
                "Task execution error".to_string(),
                Some("ExecutionError".to_string()),
                None,
            )
            .await
            .expect("Failed to mark failed");
        assert_eq!(failed.status, "failed");
        assert!(failed.error_message.is_some());

        cleanup_test_data(&pool, task.id).await;
        pool.close().await;
    }

    #[tokio::test]
    #[ignore = "Requires database"]
    async fn test_get_workflow_task_summary() {
        let pool = create_test_pool().await;
        let repo = TaskExecutionRepository::new(pool.clone());
        let workflow_id = WorkflowId::from_uuid(Uuid::new_v4());

        // Create multiple tasks in different states
        // 1. Completed task
        let task1 = repo
            .create_scheduled(
                "task-1".to_string(),
                "TestTask".to_string(),
                workflow_id,
                ExecutionId::from_uuid(Uuid::new_v4()),
                "queue".to_string(),
                "default".to_string(),
                1,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .await
            .expect("Failed to create task1");
        let started1 = repo
            .mark_started(task1.id, "service-1".to_string(), "token-1".to_string())
            .await
            .expect("Failed to mark task1 as started");
        repo.mark_completed(started1.id, b"result1".to_vec())
            .await
            .expect("Failed to mark task1 as completed");

        // 2. Failed task
        let task2 = repo
            .create_scheduled(
                "task-2".to_string(),
                "TestTask".to_string(),
                workflow_id,
                ExecutionId::from_uuid(Uuid::new_v4()),
                "queue".to_string(),
                "default".to_string(),
                1,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .await
            .expect("Failed to create task2");
        let started2 = repo
            .mark_started(task2.id, "service-2".to_string(), "token-2".to_string())
            .await
            .expect("Failed to mark task2 as started");
        repo.mark_failed(started2.id, "Error".to_string(), None, None)
            .await
            .expect("Failed to mark task2 as failed");

        // 3. Running task (started but not completed)
        let task3 = repo
            .create_scheduled(
                "task-3".to_string(),
                "TestTask".to_string(),
                workflow_id,
                ExecutionId::from_uuid(Uuid::new_v4()),
                "queue".to_string(),
                "default".to_string(),
                1,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .await
            .expect("Failed to create task3");
        repo.mark_started(task3.id, "service-3".to_string(), "token-3".to_string())
            .await
            .expect("Failed to mark task3 as started");

        // 4. Scheduled task (not started)
        let task4 = repo
            .create_scheduled(
                "task-4".to_string(),
                "TestTask".to_string(),
                workflow_id,
                ExecutionId::from_uuid(Uuid::new_v4()),
                "queue".to_string(),
                "default".to_string(),
                1,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .await
            .expect("Failed to create task4");

        // Get summary
        let summary = repo
            .get_workflow_task_summary(workflow_id)
            .await
            .expect("Failed to get workflow task summary");

        // Verify counts
        assert_eq!(summary.total_tasks, 4);
        assert_eq!(summary.completed_tasks, 1);
        assert_eq!(summary.failed_tasks, 1);
        assert_eq!(summary.running_tasks, 1);
        assert_eq!(summary.scheduled_tasks, 1);

        // Cleanup
        cleanup_test_data(&pool, task1.id).await;
        cleanup_test_data(&pool, task2.id).await;
        cleanup_test_data(&pool, task3.id).await;
        cleanup_test_data(&pool, task4.id).await;
        pool.close().await;
    }

    #[tokio::test]
    #[ignore = "Requires database"]
    async fn test_get_workflow_task_summary_all_complete() {
        let pool = create_test_pool().await;
        let repo = TaskExecutionRepository::new(pool.clone());
        let workflow_id = WorkflowId::from_uuid(Uuid::new_v4());

        // Create 2 completed tasks and 1 failed task
        let mut task_ids = Vec::new();
        for i in 0..3 {
            let task = repo
                .create_scheduled(
                    format!("task-{}", i),
                    "TestTask".to_string(),
                    workflow_id,
                    ExecutionId::from_uuid(Uuid::new_v4()),
                    "queue".to_string(),
                    "default".to_string(),
                    1,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                )
                .await
                .expect("Failed to create task");

            task_ids.push(task.id);

            let started = repo
                .mark_started(task.id, format!("service-{}", i), format!("token-{}", i))
                .await
                .expect("Failed to mark as started");

            if i < 2 {
                repo.mark_completed(started.id, b"result".to_vec())
                    .await
                    .expect("Failed to mark as completed");
            } else {
                repo.mark_failed(started.id, "Error".to_string(), None, None)
                    .await
                    .expect("Failed to mark as failed");
            }
        }

        // Get summary
        let summary = repo
            .get_workflow_task_summary(workflow_id)
            .await
            .expect("Failed to get workflow task summary");

        // All tasks are in terminal states
        assert_eq!(summary.total_tasks, 3);
        assert_eq!(summary.completed_tasks, 2);
        assert_eq!(summary.failed_tasks, 1);
        assert_eq!(summary.running_tasks, 0);
        assert_eq!(summary.scheduled_tasks, 0);

        // Verify terminal count equals total (should trigger re-queue)
        assert_eq!(
            summary.completed_tasks + summary.failed_tasks,
            summary.total_tasks
        );

        // Cleanup
        for task_id in task_ids {
            cleanup_test_data(&pool, task_id).await;
        }

        pool.close().await;
    }

    #[tokio::test]
    #[ignore] // Requires a database (create_test_pool); run with: cargo test -- --ignored
    async fn test_get_workflow_task_summary_no_tasks() {
        let pool = create_test_pool().await;
        let repo = TaskExecutionRepository::new(pool.clone());
        let workflow_id = WorkflowId::from_uuid(Uuid::new_v4());

        // Get summary for workflow with no tasks
        let summary = repo
            .get_workflow_task_summary(workflow_id)
            .await
            .expect("Failed to get workflow task summary");

        // All counts should be zero
        assert_eq!(summary.total_tasks, 0);
        assert_eq!(summary.completed_tasks, 0);
        assert_eq!(summary.failed_tasks, 0);
        assert_eq!(summary.running_tasks, 0);
        assert_eq!(summary.scheduled_tasks, 0);

        pool.close().await;
    }
}
