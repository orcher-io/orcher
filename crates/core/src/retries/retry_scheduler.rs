//! Background retry scheduling.

use orcher_common::error::{OrcherError, OrcherResult};
use orcher_common::types::identifiers::{ExecutionId, TaskExecutionId, WorkflowId};
use orcher_common::types::time::Timestamptz;
use sqlx::PgPool;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use super::retry_policy::{DlqReason, RetryDecision, RetryPolicyEvaluator};
use orcher_storage::schema::types::execution::TaskExecution;
use orcher_storage::sharding::pool_routing::ShardPoolResolver;

/// Workflow statuses after which none of a workflow's tasks is to be
/// dispatched: neither a retry nor a first attempt found never dispatched.
const FINISHED_WORKFLOW_STATUSES: &[&str] = &[
    "completed",
    "failed",
    "cancelled",
    "terminated",
    "timed_out",
];

// ================================================================================================
// RETRY SCHEDULER
// ================================================================================================

/// How long a swept retry is left alone before the sweep considers it again.
///
/// The sweep does not own these rows — a worker claims them — so this is not a
/// lock, just a way of not re-reading the same head of the queue every tick
/// while the rest of the backlog goes unseen.
const RETRY_DISPATCH_LEASE_SECS: i64 = 30;
/// Service for scheduling task retries and managing retry lifecycle
#[derive(Debug, Clone)]
pub struct TaskRetryScheduler {
    pool: PgPool,
    evaluator: RetryPolicyEvaluator,
    /// Optional shard-pool routing for partitioned persistence. A retry's task rows live
    /// on the same shard as their workflow (routing key = `workflow_executions.id`).
    /// Unset ⇒ the single `pool` is used, byte-identical to before.
    shard_resolver: std::sync::Arc<std::sync::OnceLock<std::sync::Arc<dyn ShardPoolResolver>>>,
}

impl TaskRetryScheduler {
    /// Create a new task retry scheduler
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            evaluator: RetryPolicyEvaluator::new(),
            shard_resolver: std::sync::Arc::new(std::sync::OnceLock::new()),
        }
    }

    /// Create a new task retry scheduler with a custom evaluator
    pub fn with_evaluator(pool: PgPool, evaluator: RetryPolicyEvaluator) -> Self {
        Self {
            pool,
            evaluator,
            shard_resolver: std::sync::Arc::new(std::sync::OnceLock::new()),
        }
    }

    /// Inject the shard-pool resolver (partitioned persistence). Idempotent; a second
    /// call is ignored. Wired once at startup after the router is built.
    pub fn set_shard_resolver(&self, resolver: std::sync::Arc<dyn ShardPoolResolver>) {
        let _ = self.shard_resolver.set(resolver);
    }

    /// The pool owning `workflow_id`'s shard (resolver's pick if set, else the single
    /// pool). `None` namespace ⇒ shared default set (OSS / single-tenant routing).
    fn pool_for(&self, workflow_id: Uuid) -> &PgPool {
        match self.shard_resolver.get() {
            Some(resolver) => resolver.pool_for_workflow(None, workflow_id),
            None => &self.pool,
        }
    }

    /// Pools to scan for an op keyed only by status/time (not the routing id): every
    /// shard pool when a resolver is set, else the single pool.
    fn scan_pools(&self) -> Vec<&PgPool> {
        match self.shard_resolver.get() {
            Some(resolver) => resolver.all_pools(),
            None => vec![&self.pool],
        }
    }

    /// Handle a failed task execution - evaluate retry policy and either reschedule or move to DLQ
    ///
    /// # Arguments
    ///
    /// * `failed_task` - The failed task execution
    ///
    /// # Returns
    ///
    /// The result of the retry decision: either a new scheduled task execution or DLQ information
    pub async fn handle_task_failure(
        &self,
        failed_task: &TaskExecution,
    ) -> OrcherResult<RetryOutcome> {
        info!(
            "Handling task failure: task_id={}, attempt={}, error_type={:?}",
            failed_task.task_id, failed_task.attempt_number, failed_task.error_type
        );

        // Evaluate retry decision
        let decision = self.evaluator.evaluate_retry(
            failed_task.attempt_number,
            failed_task.retry_policy.as_ref(),
            failed_task.error_type.as_deref(),
        )?;

        match decision {
            RetryDecision::Retry {
                backoff_duration,
                next_attempt,
                scheduled_at,
            } => {
                info!(
                    "Scheduling retry attempt {} with backoff {:?}",
                    next_attempt, backoff_duration
                );
                let new_task = self
                    .schedule_retry(failed_task, next_attempt, scheduled_at)
                    .await?;
                Ok(RetryOutcome::Rescheduled {
                    new_task_execution_id: new_task.id,
                    next_attempt,
                    scheduled_at,
                })
            }
            RetryDecision::MoveToDlq { reason } => {
                warn!(
                    "Moving task to DLQ: task_id={}, reason={}",
                    failed_task.task_id, reason
                );
                self.move_to_dlq(failed_task, reason.clone()).await?;
                Ok(RetryOutcome::MovedToDlq { reason })
            }
        }
    }

    /// Whether a failed task is retried or moved to the dead-letter queue,
    /// decided from its policy, its attempt and the kind of failure — nothing
    /// is written.
    ///
    /// `error_type` is the failure being reported now. It has to be passed in
    /// because the decision is taken before the row is marked failed, and a
    /// started row has no error type yet: reading the row's instead made every
    /// reported type look unknown, so a failure the policy names as
    /// non-retryable was retried to exhaustion. The row's own type is used
    /// only when the report names none.
    ///
    /// `non_retryable` is the reporter saying no retry can succeed. It is
    /// final: the policy's attempts and its list of types are not consulted,
    /// because the code that raised the failure knows something the policy
    /// was written without.
    pub fn decide(
        &self,
        failed_task: &TaskExecution,
        error_type: Option<&str>,
        non_retryable: bool,
    ) -> OrcherResult<RetryDecision> {
        let error_type = error_type
            .filter(|t| !t.is_empty())
            .or(failed_task.error_type.as_deref());
        if non_retryable {
            return Ok(RetryDecision::MoveToDlq {
                reason: DlqReason::NonRetryableError {
                    error_type: error_type.unwrap_or("Unknown").to_string(),
                },
            });
        }
        self.evaluator.evaluate_retry(
            failed_task.attempt_number,
            failed_task.retry_policy.as_ref(),
            error_type,
        )
    }

    /// Decide what a timed-out attempt gets, without writing anything: the
    /// timeout's counterpart of [`decide`](Self::decide), carried out the
    /// same way with [`apply_on`](Self::apply_on).
    pub fn decide_timeout(
        &self,
        timed_out_task: &TaskExecution,
        timeout_type: &str,
    ) -> OrcherResult<RetryDecision> {
        self.evaluator.evaluate_timeout_retry(
            timed_out_task.attempt_number,
            timed_out_task.retry_policy.as_ref(),
            timeout_type,
        )
    }

    /// Carry out a [`decide`](Self::decide) decision on `conn`, so the retry
    /// row or dead-letter mark commits together with whatever else the
    /// caller's transaction records about the failure.
    pub async fn apply_on(
        &self,
        conn: &mut sqlx::PgConnection,
        failed_task: &TaskExecution,
        decision: RetryDecision,
    ) -> OrcherResult<RetryOutcome> {
        match decision {
            RetryDecision::Retry {
                next_attempt,
                scheduled_at,
                ..
            } => {
                let new_task =
                    Self::schedule_retry_on(&mut *conn, failed_task, next_attempt, scheduled_at)
                        .await?;
                Ok(RetryOutcome::Rescheduled {
                    new_task_execution_id: new_task.id,
                    next_attempt,
                    scheduled_at,
                })
            }
            RetryDecision::MoveToDlq { reason } => {
                Self::move_to_dlq_on(&mut *conn, failed_task, reason.clone()).await?;
                Ok(RetryOutcome::MovedToDlq { reason })
            }
        }
    }

    /// Handle a timed-out task execution
    ///
    /// # Arguments
    ///
    /// * `timed_out_task` - The timed-out task execution
    /// * `timeout_type` - The type of timeout (e.g., "ScheduleToStart", "StartToClose", "Heartbeat")
    pub async fn handle_task_timeout(
        &self,
        timed_out_task: &TaskExecution,
        timeout_type: &str,
    ) -> OrcherResult<RetryOutcome> {
        info!(
            "Handling task timeout: task_id={}, attempt={}, timeout_type={}",
            timed_out_task.task_id, timed_out_task.attempt_number, timeout_type
        );

        let decision = self.decide_timeout(timed_out_task, timeout_type)?;

        match decision {
            RetryDecision::Retry {
                backoff_duration,
                next_attempt,
                scheduled_at,
            } => {
                info!(
                    "Scheduling retry attempt {} after timeout with backoff {:?}",
                    next_attempt, backoff_duration
                );
                let new_task = self
                    .schedule_retry(timed_out_task, next_attempt, scheduled_at)
                    .await?;
                Ok(RetryOutcome::Rescheduled {
                    new_task_execution_id: new_task.id,
                    next_attempt,
                    scheduled_at,
                })
            }
            RetryDecision::MoveToDlq { reason } => {
                warn!(
                    "Moving timed-out task to DLQ: task_id={}, reason={}",
                    timed_out_task.task_id, reason
                );
                self.move_to_dlq(timed_out_task, reason.clone()).await?;
                Ok(RetryOutcome::MovedToDlq { reason })
            }
        }
    }

    /// Schedule a retry attempt by creating a new task_execution record
    async fn schedule_retry(
        &self,
        original_task: &TaskExecution,
        next_attempt: i32,
        scheduled_at: Timestamptz,
    ) -> OrcherResult<TaskExecution> {
        // Co-locate the retry attempt on the workflow's shard (matches the create path).
        Self::schedule_retry_on(
            self.pool_for(original_task.workflow_id.as_uuid()),
            original_task,
            next_attempt,
            scheduled_at,
        )
        .await
    }

    /// [`schedule_retry`](Self::schedule_retry) on a given connection, so it
    /// can join a caller's transaction.
    async fn schedule_retry_on<'e, E: sqlx::PgExecutor<'e>>(
        executor: E,
        original_task: &TaskExecution,
        next_attempt: i32,
        scheduled_at: Timestamptz,
    ) -> OrcherResult<TaskExecution> {
        debug!(
            "Creating new task execution for retry: task_id={}, attempt={}",
            original_task.task_id, next_attempt
        );

        // Create new scheduled task execution with incremented attempt number
        let new_task_id = TaskExecutionId::new_v4();

        let result = sqlx::query_as::<_, TaskExecutionRow>(
            r#"
            INSERT INTO task_executions (
                id, task_id, task_type, workflow_id, execution_id,
                task_queue, namespace, attempt_number, status, scheduled_at,
                schedule_to_start_timeout_secs, start_to_close_timeout_secs,
                heartbeat_timeout_secs, input, retry_policy, headers, metadata,
                created_at, updated_at, organization_id
            )
            -- The workflow's organization where the workflow is on this
            -- database, the attempt's own otherwise: an attempt written before
            -- tasks were stamped carries none, and its retry would otherwise
            -- reach only a worker acting for none.
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19,
                    COALESCE(
                        (SELECT organization_id FROM workflow_executions WHERE id = $4),
                        $20
                    ))
            RETURNING id, organization_id, task_id, task_type, workflow_id, execution_id,
                      task_queue, namespace, service_identity, attempt_number, status,
                      scheduled_at, started_at, completed_at, task_token,
                      schedule_to_start_timeout_secs, start_to_close_timeout_secs,
                      heartbeat_timeout_secs, input, result, error_message, error_type,
                      error_details, retry_policy, headers, metadata,
                      last_heartbeat_at, heartbeat_details, created_at, updated_at
            "#,
        )
        .bind(new_task_id.as_uuid())
        .bind(&original_task.task_id)
        .bind(&original_task.task_type)
        .bind(original_task.workflow_id.as_uuid())
        .bind(original_task.execution_id.as_uuid())
        .bind(&original_task.task_queue)
        .bind(&original_task.namespace)
        .bind(next_attempt)
        .bind("scheduled")
        .bind(scheduled_at)
        .bind(original_task.schedule_to_start_timeout_secs)
        .bind(original_task.start_to_close_timeout_secs)
        .bind(original_task.heartbeat_timeout_secs)
        .bind(&original_task.input)
        .bind(&original_task.retry_policy)
        .bind(&original_task.headers)
        .bind(&original_task.metadata)
        .bind(Timestamptz::now())
        .bind(Timestamptz::now())
        .bind(original_task.organization_id)
        .fetch_one(executor)
        .await
        .map_err(|e| {
            error!("Failed to create retry task execution: {}", e);
            OrcherError::internal(format!("Failed to schedule retry: {}", e))
        })?;

        let new_task = TaskExecution::from(result);

        info!(
            "Created retry task execution: id={}, attempt={}",
            new_task.id, next_attempt
        );

        Ok(new_task)
    }

    /// Move a task to the dead letter queue
    async fn move_to_dlq(&self, task: &TaskExecution, reason: DlqReason) -> OrcherResult<()> {
        Self::move_to_dlq_on(self.pool_for(task.workflow_id.as_uuid()), task, reason).await
    }

    /// [`move_to_dlq`](Self::move_to_dlq) on a given connection.
    async fn move_to_dlq_on<'e, E: sqlx::PgExecutor<'e>>(
        executor: E,
        task: &TaskExecution,
        reason: DlqReason,
    ) -> OrcherResult<()> {
        debug!(
            "Moving task to DLQ: task_id={}, attempt={}, reason={}",
            task.task_id, task.attempt_number, reason
        );

        // Store DLQ information in the task execution metadata
        let mut metadata = task.metadata.clone();
        if let Some(obj) = metadata.as_object_mut() {
            obj.insert("dlq_reason".to_string(), serde_json::json!(reason));
            obj.insert(
                "dlq_timestamp".to_string(),
                serde_json::json!(Timestamptz::now()),
            );
        }

        // Update task execution status to 'dead_letter'
        sqlx::query(
            r#"
            UPDATE task_executions
            SET metadata = $1,
                updated_at = $2
            WHERE id = $3
            "#,
        )
        .bind(&metadata)
        .bind(Timestamptz::now())
        .bind(task.id.as_uuid())
        .execute(executor)
        .await
        .map_err(|e| {
            error!("Failed to update task execution DLQ metadata: {}", e);
            OrcherError::internal(format!("Failed to move task to DLQ: {}", e))
        })?;

        info!(
            "Task moved to DLQ: task_id={}, attempt={}, reason={}",
            task.task_id, task.attempt_number, reason
        );

        Ok(())
    }

    /// Check if a task should be retried (without performing the retry)
    pub fn should_retry_task(&self, task: &TaskExecution) -> bool {
        self.evaluator.should_retry(
            task.attempt_number,
            task.retry_policy.as_ref(),
            task.error_type.as_deref(),
        )
    }

    /// Get retry statistics for a workflow execution
    pub async fn get_retry_stats(
        &self,
        workflow_id: &WorkflowId,
        execution_id: &ExecutionId,
    ) -> OrcherResult<RetryStats> {
        let result = sqlx::query_as::<_, RetryStatsRow>(
            r#"
            SELECT
                COUNT(*) as total_tasks,
                COUNT(DISTINCT task_id) as unique_tasks,
                SUM(CASE WHEN attempt_number > 1 THEN 1 ELSE 0 END) as retried_tasks,
                MAX(attempt_number) as max_attempts,
                AVG(attempt_number) as avg_attempts,
                COUNT(CASE WHEN metadata->>'dlq_reason' IS NOT NULL THEN 1 END) as dlq_tasks
            FROM task_executions
            WHERE workflow_id = $1 AND execution_id = $2
            "#,
        )
        .bind(workflow_id.as_uuid())
        .bind(execution_id.as_uuid())
        .fetch_one(self.pool_for(workflow_id.as_uuid()))
        .await
        .map_err(|e| {
            error!("Failed to fetch retry stats: {}", e);
            OrcherError::internal(format!("Failed to get retry stats: {}", e))
        })?;

        Ok(RetryStats {
            total_tasks: result.total_tasks.unwrap_or(0) as u32,
            unique_tasks: result.unique_tasks.unwrap_or(0) as u32,
            retried_tasks: result.retried_tasks.unwrap_or(0) as u32,
            max_attempts: result.max_attempts.unwrap_or(1),
            avg_attempts: result.avg_attempts.unwrap_or(1.0),
            dlq_tasks: result.dlq_tasks.unwrap_or(0) as u32,
        })
    }

    /// Note that a retry has just been handed to the outbox.
    ///
    /// The sweep does not own the row — a worker claims it — so this only stops
    /// the next sweep from re-reading the same head of the queue while the rest
    /// of the backlog goes unseen.
    pub async fn mark_dispatched(&self, id: &TaskExecutionId) -> OrcherResult<()> {
        for pool in self.scan_pools() {
            sqlx::query("UPDATE task_executions SET last_dispatched_at = NOW() WHERE id = $1")
                .bind(id.as_uuid())
                .execute(pool)
                .await
                .map_err(|e| {
                    OrcherError::internal(format!("Failed to record retry dispatch: {e}"))
                })?;
        }
        Ok(())
    }

    /// Find all tasks ready for retry (scheduled time has passed).
    ///
    /// Keyed by status/time, not the routing id, so it scans every shard pool and
    /// merges — a retry row lives on its workflow's shard. With no resolver this is a
    /// single pool, identical to before.
    pub async fn find_ready_retries(&self) -> OrcherResult<Vec<TaskExecution>> {
        let sql = r#"
            -- The organization is the workflow's, as for a claim: the dispatch
            -- made from this row is stamped with it, and a row from before
            -- tasks were stamped carries none of its own.
            SELECT id,
                   COALESCE(
                       (SELECT w.organization_id FROM workflow_executions w
                        WHERE w.id = t.workflow_id),
                       t.organization_id
                   ) AS organization_id,
                   task_id, task_type, workflow_id, execution_id,
                   task_queue, namespace, service_identity, attempt_number, status,
                   scheduled_at, started_at, completed_at, task_token,
                   schedule_to_start_timeout_secs, start_to_close_timeout_secs,
                   heartbeat_timeout_secs, input, result, error_message, error_type,
                   error_details, retry_policy, headers, metadata,
                   last_heartbeat_at, heartbeat_details, created_at, updated_at
            FROM task_executions t
            WHERE t.status = 'scheduled'
              AND t.attempt_number > 1
              AND t.scheduled_at <= $1
              -- Skip what this sweep handed over recently.
              --
              -- A row stays 'scheduled' until a worker claims it, so a retry
              -- nobody will run — its queue has no workers left, its workflow
              -- is gone — held its place at the head of this ordering for good.
              -- A hundred of those and every newer retry was starved, silently:
              -- tasks stopped retrying and nothing said so.
              AND (t.last_dispatched_at IS NULL OR t.last_dispatched_at <= $2)
              -- And skip retries for workflows that have already finished.
              -- Nothing prunes those rows, so they accumulate forever; the
              -- oldest found in practice was two weeks old, still at the head.
              AND NOT EXISTS (
                  SELECT 1 FROM workflow_executions w
                  WHERE w.id = t.workflow_id
                    AND w.status = ANY($3)
              )
            ORDER BY t.scheduled_at ASC
            LIMIT 100
            "#;
        let now = Timestamptz::now();
        // How long a row waits before this sweep looks at it again. Long enough
        // that a worker has a fair chance to claim it first, short enough that a
        // genuine redelivery is not left for minutes.
        let lease_cutoff = now
            .checked_sub(jiff::SignedDuration::from_secs(RETRY_DISPATCH_LEASE_SECS))
            .unwrap_or(now);
        let mut rows: Vec<TaskExecutionRow> = Vec::new();
        for pool in self.scan_pools() {
            let batch = sqlx::query_as::<_, TaskExecutionRow>(sql)
                .bind(now)
                .bind(lease_cutoff)
                .bind(FINISHED_WORKFLOW_STATUSES)
                .fetch_all(pool)
                .await
                .map_err(|e| {
                    error!("Failed to fetch ready retries: {}", e);
                    OrcherError::internal(format!("Failed to find ready retries: {}", e))
                })?;
            rows.extend(batch);
        }
        // Merge across shards: earliest-scheduled first, capped at the per-cycle budget.
        rows.sort_by(|a, b| a.scheduled_at.cmp(&b.scheduled_at));
        rows.truncate(100);

        Ok(rows.into_iter().map(TaskExecution::from).collect())
    }

    /// Find first attempts scheduled between `scheduled_since` and `grace`
    /// ago that were never marked dispatched.
    ///
    /// Before a task row and its dispatch were written in one transaction, an
    /// engine that stopped between the two left the row scheduled with nothing
    /// to deliver it. The workflow's next replay repairs its own row, but only
    /// if something replays it; these are the rows found without one.
    ///
    /// A row found here may yet have its dispatch: one written by an engine
    /// from before rows were marked. Claiming it writes its dispatch under the
    /// attempt's outbox key, which finds that one and adds nothing, and marks
    /// the row, so it is not found again.
    ///
    /// A row younger than `grace` is left to the engine that may still be
    /// writing its dispatch — during a rolling upgrade, an older engine writes
    /// the two apart. That is only to spare work: the dispatch is claimed on
    /// the row and keyed in the outbox, so finding a row too early can never
    /// dispatch its task twice.
    ///
    /// A row older than `scheduled_since` is never found. Its outbox row, if
    /// it had one, may have been deleted by data retention, and without it a
    /// dispatched task cannot be told from one that was never dispatched; see
    /// [`undispatched_repair_window`](crate::engine::undispatched_repair_window).
    pub async fn find_undispatched_first_attempts(
        &self,
        grace: std::time::Duration,
        scheduled_since: Timestamptz,
    ) -> OrcherResult<Vec<TaskExecution>> {
        let sql = r#"
            -- The organization is the workflow's, as for a claim: the dispatch
            -- made from this row is stamped with it, and a row from before
            -- tasks were stamped carries none of its own.
            SELECT id,
                   COALESCE(
                       (SELECT w.organization_id FROM workflow_executions w
                        WHERE w.id = t.workflow_id),
                       t.organization_id
                   ) AS organization_id,
                   task_id, task_type, workflow_id, execution_id,
                   task_queue, namespace, service_identity, attempt_number, status,
                   scheduled_at, started_at, completed_at, task_token,
                   schedule_to_start_timeout_secs, start_to_close_timeout_secs,
                   heartbeat_timeout_secs, input, result, error_message, error_type,
                   error_details, retry_policy, headers, metadata,
                   last_heartbeat_at, heartbeat_details, created_at, updated_at
            FROM task_executions t
            WHERE t.status = 'scheduled'
              AND t.attempt_number = 1
              AND t.last_dispatched_at IS NULL
              AND t.scheduled_at >= $1
              AND t.scheduled_at <= $2
              AND NOT EXISTS (
                  SELECT 1 FROM workflow_executions w
                  WHERE w.id = t.workflow_id
                    AND w.status = ANY($3)
              )
            ORDER BY t.scheduled_at ASC
            LIMIT 100
            "#;
        let now = Timestamptz::now();
        let cutoff = jiff::SignedDuration::try_from(grace)
            .ok()
            .and_then(|g| now.checked_sub(g))
            .unwrap_or(now);
        let mut rows: Vec<TaskExecutionRow> = Vec::new();
        for pool in self.scan_pools() {
            let batch = sqlx::query_as::<_, TaskExecutionRow>(sql)
                .bind(scheduled_since)
                .bind(cutoff)
                .bind(FINISHED_WORKFLOW_STATUSES)
                .fetch_all(pool)
                .await
                .map_err(|e| {
                    error!("Failed to find undispatched tasks: {}", e);
                    OrcherError::internal(format!("Failed to find undispatched tasks: {}", e))
                })?;
            rows.extend(batch);
        }
        rows.sort_by(|a, b| a.scheduled_at.cmp(&b.scheduled_at));
        rows.truncate(100);

        Ok(rows.into_iter().map(TaskExecution::from).collect())
    }
}

// ================================================================================================
// TYPES
// ================================================================================================

/// Outcome of a retry operation
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetryOutcome {
    /// Task was rescheduled for retry
    Rescheduled {
        /// ID of the new task execution
        new_task_execution_id: TaskExecutionId,
        /// Next attempt number
        next_attempt: i32,
        /// Scheduled time for the retry
        scheduled_at: Timestamptz,
    },
    /// Task was moved to dead letter queue
    MovedToDlq {
        /// Reason for moving to DLQ
        reason: DlqReason,
    },
}

/// Statistics about task retries for a workflow execution
#[derive(Debug, Clone, PartialEq)]
pub struct RetryStats {
    /// Total number of task execution records
    pub total_tasks: u32,
    /// Number of unique task IDs
    pub unique_tasks: u32,
    /// Number of tasks that were retried (attempt > 1)
    pub retried_tasks: u32,
    /// Maximum attempt number seen
    pub max_attempts: i32,
    /// Average attempt number
    pub avg_attempts: f64,
    /// Number of tasks in DLQ
    pub dlq_tasks: u32,
}

// ================================================================================================
// DATABASE TYPES
// ================================================================================================

#[derive(Debug, sqlx::FromRow)]
struct TaskExecutionRow {
    id: uuid::Uuid,
    organization_id: Option<uuid::Uuid>,
    task_id: String,
    task_type: String,
    workflow_id: uuid::Uuid,
    execution_id: uuid::Uuid,
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
    error_details: Option<serde_json::Value>,
    retry_policy: Option<serde_json::Value>,
    headers: serde_json::Value,
    metadata: serde_json::Value,
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

#[derive(Debug, sqlx::FromRow)]
struct RetryStatsRow {
    total_tasks: Option<i64>,
    unique_tasks: Option<i64>,
    retried_tasks: Option<i64>,
    max_attempts: Option<i32>,
    avg_attempts: Option<f64>,
    dlq_tasks: Option<i64>,
}

// ================================================================================================
// TESTS
// ================================================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retry_outcome_equality() {
        let outcome1 = RetryOutcome::Rescheduled {
            new_task_execution_id: TaskExecutionId::new_v4(),
            next_attempt: 2,
            scheduled_at: Timestamptz::now(),
        };

        let outcome2 = RetryOutcome::MovedToDlq {
            reason: DlqReason::MaxAttemptsExceeded {
                max_attempts: 3,
                current_attempt: 3,
            },
        };

        assert_ne!(outcome1, outcome2);
    }

    #[test]
    fn test_retry_stats_default() {
        let stats = RetryStats {
            total_tasks: 10,
            unique_tasks: 5,
            retried_tasks: 3,
            max_attempts: 4,
            avg_attempts: 1.8,
            dlq_tasks: 1,
        };

        assert_eq!(stats.total_tasks, 10);
        assert_eq!(stats.unique_tasks, 5);
        assert_eq!(stats.retried_tasks, 3);
        assert_eq!(stats.max_attempts, 4);
        assert!((stats.avg_attempts - 1.8).abs() < 0.01);
        assert_eq!(stats.dlq_tasks, 1);
    }
}
