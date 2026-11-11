//! Background retry scheduling service.

use orcher_common::error::{OrcherError, OrcherResult};
use orcher_common::types::time::Timestamptz;
use orcher_storage::repositories::domains::child_workflow::ChildWorkflowRepository;
use orcher_storage::repositories::domains::workflow_execution::is_workflow_id_held;
use orcher_storage::repositories::domains::workflow_execution::repository::WorkflowExecutionRepository;
use orcher_storage::schema::types::execution::WorkflowExecution;
use serde::Deserialize;
use sqlx::postgres::{PgListener, PgNotification};
use sqlx::PgPool;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use super::policy::{
    recorded_non_retryable, AbandonReason, WorkflowRetryDecision, WorkflowRetryPolicy,
    WorkflowRetryPolicyEvaluator,
};

// ================================================================================================
// CONFIGURATION
// ================================================================================================

/// Configuration for the workflow retry scheduler
#[derive(Debug, Clone)]
pub struct WorkflowRetrySchedulerConfig {
    /// How often to poll for failed workflows (in seconds)
    pub poll_interval_secs: u64,

    /// Maximum number of workflows to process per poll
    pub batch_size: usize,

    /// Enable metrics collection
    pub enable_metrics: bool,

    /// Also act on every workflow failure as it is written
    /// (`workflow_failed`), not only on a retry being scheduled.
    ///
    /// A scheduled retry wakes the scheduler whether or not this is set — the
    /// failure path announces it on `workflow_retry_due` as it decides it, and
    /// the scheduler starts the retry when its backoff ends — as long as the
    /// scheduler has a database pool to listen on. The poll remains the
    /// backstop for an announcement that is missed.
    ///
    /// Default: false
    pub enable_realtime_notifications: bool,

    /// How long a run may stay taken for a retry with no successor recorded
    /// before the scheduler concludes the one that took it has gone, and
    /// creates the successor itself.
    pub stranded_retry_grace_secs: u64,
}

impl Default for WorkflowRetrySchedulerConfig {
    fn default() -> Self {
        Self {
            poll_interval_secs: 30,
            batch_size: 50,
            enable_metrics: true,
            enable_realtime_notifications: false, // Disabled by default for backward compatibility
            stranded_retry_grace_secs: 120,
        }
    }
}

// ================================================================================================
// STATISTICS
// ================================================================================================

/// Statistics about retry operations
#[derive(Debug, Clone, Default)]
pub struct RetryStatistics {
    /// Total number of workflows scheduled for retry
    pub workflows_retried: u64,

    /// Total number of workflows abandoned (max retries exceeded)
    pub workflows_abandoned: u64,

    /// Total number of poll cycles completed
    pub total_polls: u64,

    /// Number of errors encountered during polling
    pub poll_errors: u64,

    /// Timestamp of last successful poll
    pub last_poll_at: Option<Timestamptz>,

    /// Timestamp of last error
    pub last_error_at: Option<Timestamptz>,
}

impl RetryStatistics {
    /// Calculate the success rate of retries
    pub fn success_rate(&self) -> f64 {
        let total = self.workflows_retried + self.workflows_abandoned;
        if total == 0 {
            0.0
        } else {
            (self.workflows_retried as f64 / total as f64) * 100.0
        }
    }

    /// Calculate the abandonment rate
    pub fn abandonment_rate(&self) -> f64 {
        let total = self.workflows_retried + self.workflows_abandoned;
        if total == 0 {
            0.0
        } else {
            (self.workflows_abandoned as f64 / total as f64) * 100.0
        }
    }
}

// ================================================================================================
// RETRY OUTCOME
// ================================================================================================

/// Outcome of handling a failed workflow
#[derive(Debug, Clone)]
pub enum RetryOutcome {
    /// Workflow was scheduled for retry
    Rescheduled {
        /// Workflow ID
        workflow_id: Uuid,
        /// Next attempt number
        next_attempt: u32,
        /// Scheduled time for retry
        scheduled_at: Timestamptz,
    },
    /// Workflow retry was abandoned
    Abandoned {
        /// Workflow ID
        workflow_id: Uuid,
        /// Reason for abandonment
        reason: AbandonReason,
    },
    /// Nothing done: another scheduler retried the run first, or it was
    /// cancelled or terminated while it waited.
    AlreadyHandled {
        /// Workflow ID
        workflow_id: Uuid,
    },
}

// ================================================================================================
// WORKFLOW RETRY SCHEDULER
// ================================================================================================

/// Background service for scheduling workflow retries
///
/// Continuously polls the database for failed workflows and evaluates
/// whether they should be retried based on their retry policies.
///
/// Supports two modes:
/// 1. Polling (default): Checks for failed workflows every N seconds
/// 2. Real-time notifications (optional): Uses PostgreSQL LISTEN/NOTIFY for immediate detection
pub struct WorkflowRetryScheduler {
    /// Workflow execution repository
    repository: Arc<WorkflowExecutionRepository>,

    /// Scheduler configuration
    config: WorkflowRetrySchedulerConfig,

    /// Retry policy evaluator
    evaluator: WorkflowRetryPolicyEvaluator,

    /// Shutdown signal
    shutdown: Arc<AtomicBool>,

    /// Wakes both loops out of their waits when `stop` is called. The flag
    /// alone was only seen after a full poll interval or listener timeout,
    /// which shutdown then sat through.
    stop_signal: Arc<tokio::sync::watch::Sender<bool>>,

    /// Statistics (thread-safe)
    stats: Arc<RwLock<RetryStatistics>>,

    /// Atomic counters for lock-free metrics
    workflows_retried: Arc<AtomicU64>,
    workflows_abandoned: Arc<AtomicU64>,
    total_polls: Arc<AtomicU64>,
    poll_errors: Arc<AtomicU64>,

    /// Database connection pool (for LISTEN/NOTIFY)
    db_pool: Option<Arc<PgPool>>,

    /// Notification listener handle
    listener_handle: Arc<RwLock<Option<JoinHandle<()>>>>,

    /// Child tracking, moved along when a child is retried. Unset, the child
    /// reconciliation sweep makes the move instead.
    child_repository: Option<Arc<ChildWorkflowRepository>>,
}

/// The channel the failure path announces a scheduled retry on.
const RETRY_DUE_CHANNEL: &str = "workflow_retry_due";

/// Payload of a `workflow_retry_due` notification.
#[derive(Debug, Deserialize)]
struct RetryDuePayload {
    execution_id: String,
    /// When the retry is due, in milliseconds since the epoch.
    due_at_ms: i64,
}

/// Payload structure for workflow_failed notifications
#[derive(Debug, Deserialize)]
struct WorkflowFailedPayload {
    workflow_id: String,
    execution_id: String,
    namespace: String,
    #[allow(dead_code)]
    workflow_name: String,
    #[allow(dead_code)]
    error: Option<String>,
    #[allow(dead_code)]
    retry_count: i32,
    #[allow(dead_code)]
    max_retries: i32,
}

impl WorkflowRetryScheduler {
    /// Create a new workflow retry scheduler
    pub fn new(
        repository: Arc<WorkflowExecutionRepository>,
        config: WorkflowRetrySchedulerConfig,
    ) -> Self {
        Self {
            repository,
            config,
            evaluator: WorkflowRetryPolicyEvaluator::new(),
            shutdown: Arc::new(AtomicBool::new(false)),
            stop_signal: Arc::new(tokio::sync::watch::channel(false).0),
            stats: Arc::new(RwLock::new(RetryStatistics::default())),
            workflows_retried: Arc::new(AtomicU64::new(0)),
            workflows_abandoned: Arc::new(AtomicU64::new(0)),
            total_polls: Arc::new(AtomicU64::new(0)),
            poll_errors: Arc::new(AtomicU64::new(0)),
            db_pool: None,
            listener_handle: Arc::new(RwLock::new(None)),
            child_repository: None,
        }
    }

    /// Create a new workflow retry scheduler with database pool for real-time notifications
    pub fn new_with_pool(
        repository: Arc<WorkflowExecutionRepository>,
        config: WorkflowRetrySchedulerConfig,
        db_pool: Arc<PgPool>,
    ) -> Self {
        Self {
            repository,
            config,
            evaluator: WorkflowRetryPolicyEvaluator::new(),
            shutdown: Arc::new(AtomicBool::new(false)),
            stop_signal: Arc::new(tokio::sync::watch::channel(false).0),
            stats: Arc::new(RwLock::new(RetryStatistics::default())),
            workflows_retried: Arc::new(AtomicU64::new(0)),
            workflows_abandoned: Arc::new(AtomicU64::new(0)),
            total_polls: Arc::new(AtomicU64::new(0)),
            poll_errors: Arc::new(AtomicU64::new(0)),
            db_pool: Some(db_pool),
            listener_handle: Arc::new(RwLock::new(None)),
            child_repository: None,
        }
    }

    /// Keep children attached to their parent across retries: a retried
    /// child's tracking row is moved to the successor run as it is created.
    pub fn with_child_repository(mut self, child_repository: Arc<ChildWorkflowRepository>) -> Self {
        self.child_repository = Some(child_repository);
        self
    }

    /// Handle a single failed workflow execution
    ///
    /// Evaluates the retry policy and either schedules a retry or abandons the workflow.
    ///
    /// # Arguments
    ///
    /// * `workflow_execution` - The failed workflow execution to evaluate
    ///
    /// # Returns
    ///
    /// The outcome of the retry evaluation (Rescheduled or Abandoned)
    pub async fn handle_workflow_failure(
        &self,
        workflow_execution: &WorkflowExecution,
    ) -> OrcherResult<RetryOutcome> {
        let workflow_id: Uuid = workflow_execution.id.into();
        let current_attempt = workflow_execution.retry_count.max(0) as u32;
        let max_retries = workflow_execution.max_retries.max(0) as u32;

        debug!(
            workflow_id = %workflow_id,
            execution_id = %workflow_execution.execution_id,
            current_attempt = current_attempt,
            max_retries = max_retries,
            "Evaluating workflow for retry"
        );

        // The policy the run was started with, read the one way every path
        // reads it. This used to deserialize the stored durations as integers;
        // the start path writes them fractional, so every stored policy failed
        // to parse and fell back to defaults, non-retryable types and all.
        let policy = WorkflowRetryPolicy::for_execution(workflow_execution);

        // The retry would start now. Past the workflow's execution timeout no
        // run of it may start, whatever the policy says: the failure or
        // timeout stands as the outcome. The take below refuses it as well,
        // so this only decides what a refused take would leave undecided.
        if workflow_execution.execution_expired_by(Timestamptz::now()) {
            let reason = AbandonReason::ExecutionTimedOut;
            warn!(
                workflow_id = %workflow_id,
                execution_id = %workflow_execution.execution_id,
                reason = %reason,
                "Abandoning workflow retry"
            );
            self.abandon_workflow(workflow_execution, reason.clone())
                .await?;
            self.workflows_abandoned.fetch_add(1, Ordering::Relaxed);
            return Ok(RetryOutcome::Abandoned {
                workflow_id,
                reason,
            });
        }

        // Evaluate retry decision
        // The failure's type, where the failure recorded one, is what a
        // policy's non-retryable types are matched against — the same input
        // the failure path decided on. The message stands in for rows that
        // recorded no type, as it always has.
        let failure_type = workflow_execution
            .error_details
            .as_ref()
            .and_then(|details| details.get("failure_type"))
            .and_then(|t| t.as_str())
            .filter(|t| !t.is_empty())
            .or(workflow_execution.error.as_deref());
        // And the failure's own mark, as the failure path recorded it: a
        // failure raised as non-retryable is not retried here either.
        let non_retryable = recorded_non_retryable(workflow_execution.error_details.as_ref());
        let decision = self.evaluator.evaluate_failure(
            current_attempt,
            Some(&policy),
            failure_type,
            non_retryable,
        )?;

        match decision {
            WorkflowRetryDecision::Retry {
                backoff_duration,
                next_attempt,
                scheduled_at,
            } => {
                info!(
                    workflow_id = %workflow_id,
                    execution_id = %workflow_execution.execution_id,
                    next_attempt = next_attempt,
                    backoff_secs = backoff_duration.as_secs(),
                    scheduled_at = %scheduled_at,
                    "Scheduling workflow for retry"
                );

                if !self
                    .schedule_retry(workflow_execution, next_attempt, scheduled_at)
                    .await?
                {
                    return Ok(RetryOutcome::AlreadyHandled { workflow_id });
                }

                self.workflows_retried.fetch_add(1, Ordering::Relaxed);

                Ok(RetryOutcome::Rescheduled {
                    workflow_id,
                    next_attempt,
                    scheduled_at,
                })
            }
            WorkflowRetryDecision::Abandon { reason } => {
                warn!(
                    workflow_id = %workflow_id,
                    execution_id = %workflow_execution.execution_id,
                    reason = ?reason,
                    "Abandoning workflow retry"
                );

                self.abandon_workflow(workflow_execution, reason.clone())
                    .await?;

                self.workflows_abandoned.fetch_add(1, Ordering::Relaxed);

                Ok(RetryOutcome::Abandoned {
                    workflow_id,
                    reason,
                })
            }
        }
    }

    /// Schedule a workflow for retry by RE-EXECUTING it from the start.
    ///
    /// A workflow-level retry must not replay the failed execution's journal (which
    /// ends in `WorkflowExecutionFailed` and would just re-fail deterministically).
    /// Instead — mirroring the restart-fresh mechanism — mark the
    /// failed execution terminal as `restarted_fresh` (the result path follows the
    /// `parent_execution_id`/`workflow_id` chain to the successor) and create a fresh
    /// successor run: a new `execution_id` with an empty (event-1) journal and a
    /// `dispatch_step` outbox task, inheriting the workflow's type/input/config and
    /// carrying `retry_count` so the retry cap still holds.
    async fn schedule_retry(
        &self,
        workflow: &WorkflowExecution,
        next_attempt: u32,
        scheduled_at: Timestamptz,
    ) -> OrcherResult<bool> {
        // Mark the failed execution terminal — if it is still a failure
        // waiting for its retry. Every replica's scheduler can pick up the
        // same failure, and each used to mark it and create a successor: two
        // retries of one failure, both running. Only the caller that makes
        // this transition goes on; it also refuses a run cancelled or
        // terminated while it waited. The result path recognises the
        // `restarted_fresh` chain and follows it to the successor's result.
        //
        // With clustering on, the take is fenced: a node that does not own the
        // run is refused. That is not a failure here — the owner's scheduler
        // retries it — so it reads as "not mine", like a retry already taken.
        let taken = match self.repository.claim_for_retry(workflow.id.as_uuid()).await {
            Ok(taken) => taken,
            Err(e) if orcher_storage::sharding::is_ownership_lost(&e.to_string()) => {
                debug!(
                    execution_id = %workflow.execution_id,
                    "Retry left to the node that owns the run"
                );
                return Ok(false);
            }
            Err(e) => return Err(e),
        };
        if !taken {
            debug!(
                execution_id = %workflow.execution_id,
                "Retry already taken, or the run ended while it waited"
            );
            return Ok(false);
        }

        let created = match self.create_retry_successor(workflow, next_attempt).await {
            Ok(created) => created,
            Err(e) => {
                // Give the retry back, so the failure is retried again rather
                // than left replaced by nothing. If even that fails, the run
                // stays marked and the stranded-retry pass finishes it.
                if let Err(release) = self
                    .repository
                    .release_retry_claim(workflow.id.as_uuid())
                    .await
                {
                    if orcher_storage::sharding::is_ownership_lost(&release.to_string()) {
                        // Ownership moved since the take: the new owner's
                        // stranded-retry pass finishes it.
                        debug!(
                            execution_id = %workflow.execution_id,
                            "Retry give-back left to the node that now owns the run"
                        );
                        return Err(e);
                    }
                    error!(
                        execution_id = %workflow.execution_id,
                        error = %release,
                        "Could not give back a retry whose successor was not created; \
                         the stranded-retry pass will create it"
                    );
                }
                return Err(e);
            }
        };
        if !created {
            return Ok(false);
        }

        info!(
            workflow_id = ?workflow.workflow_id,
            execution_id = %workflow.execution_id,
            next_attempt = next_attempt,
            scheduled_at = %scheduled_at,
            "Workflow retry: created fresh successor run (re-executes from event 1)"
        );
        Ok(true)
    }

    /// The id of the run that retries `workflow`.
    ///
    /// Derived from the failed run's id, so however often the successor is
    /// created — a replica racing past the claim, a retry of a create that did
    /// commit, the stranded-retry pass — there is only ever one row it can
    /// be: a second insert collides with the first. Being derived, it also
    /// routes to one shard every time.
    ///
    /// A run holding its workflow id is succeeded on the database its id's
    /// runs share, so the id's hold still sees them all: the first derived id
    /// that routes there. With one database that is always the first, which
    /// is the id this has always been.
    fn retry_successor_id(&self, workflow: &WorkflowExecution) -> Uuid {
        let derived = |n: u32| {
            let seed = if n == 0 {
                format!("orcher:workflow-retry:{}", workflow.id.as_uuid())
            } else {
                format!("orcher:workflow-retry:{}:{}", workflow.id.as_uuid(), n)
            };
            Uuid::new_v5(&Uuid::NAMESPACE_OID, seed.as_bytes())
        };
        match (workflow.holds_workflow_id, workflow.workflow_id.as_deref()) {
            (true, Some(workflow_id)) => self.repository.id_beside_workflow_id(
                workflow.organization_id,
                &workflow.namespace,
                workflow_id,
                derived,
            ),
            _ => derived(0),
        }
    }

    /// Create the successor that retries `workflow`, already taken for a retry,
    /// and record that it exists. Succeeds if it already existed.
    ///
    /// Returns whether the successor exists. It is not created — and the run
    /// is closed with its failure or timeout as its outcome — when a newer
    /// start holds the workflow id, or when the workflow's execution timeout
    /// has run out: no run of it may start from then on.
    async fn create_retry_successor(
        &self,
        workflow: &WorkflowExecution,
        next_attempt: u32,
    ) -> OrcherResult<bool> {
        let now = Timestamptz::now();
        let old_execution_id = workflow.execution_id.clone();
        let successor_id = self.retry_successor_id(workflow);
        let new_execution_id = successor_id.to_string();
        if workflow.execution_expired_by(now) {
            // Taken in time, but the execution timeout ran out before the
            // successor was created: a scheduler that stopped between the
            // two, picked up late by the stranded-retry pass. A successor that
            // did get created stands.
            if self.repository.find_by_id(successor_id).await?.is_some() {
                self.successor_exists(workflow, &new_execution_id).await;
                return Ok(true);
            }
            warn!(
                workflow_id = ?workflow.workflow_id,
                execution_id = %old_execution_id,
                "Workflow retry not started: the workflow execution timeout has run out"
            );
            self.repository.give_up_retry(workflow.id.as_uuid()).await?;
            return Ok(false);
        }
        let mut new_execution = WorkflowExecution::builder()
            .execution_id(new_execution_id.clone())
            .workflow_name(workflow.workflow_name.clone())
            .workflow_version(workflow.workflow_version.clone())
            .namespace(workflow.namespace.clone())
            .parameters(workflow.parameters.clone())
            // Triggered by the engine's retry loop; "workflow" is in the trigger check set.
            .trigger_type("workflow".to_string())
            .build();
        new_execution.workflow_id = workflow.workflow_id.clone();
        new_execution.workflow_type = workflow.workflow_type.clone();
        new_execution.status = "running".to_string();
        new_execution.started_at = Some(now);
        new_execution.root_execution_id = workflow.root_execution_id.clone();
        new_execution.parent_execution_id = Some(old_execution_id.clone());
        new_execution.task_queue = workflow.task_queue.clone();
        new_execution.max_retries = workflow.max_retries;
        // Carry the retry count so the successor inherits the cap (else it resets to 0
        // and retries forever).
        new_execution.retry_count = next_attempt as i32;
        new_execution.retry_policy = workflow.retry_policy.clone();
        new_execution.labels = workflow.labels.clone();
        // The workflow's annotations go on with it. Its start's request id
        // stays with the run the start made, and its delay was spent before
        // that run: a retry starts when it is created.
        new_execution.annotations = workflow.annotations.clone();
        // A retry is a new run: it is held to the same run and task timeouts,
        // and its run clock starts with it. The execution timeout is not
        // restarted: the retry carries the deadline the workflow started with.
        new_execution.run_timeout_secs = workflow.run_timeout_secs;
        new_execution.task_timeout_secs = workflow.task_timeout_secs;
        new_execution.timeout_secs = workflow.timeout_secs;
        new_execution.execution_deadline = workflow.execution_expires_at();
        new_execution.id = orcher_common::types::identifiers::ExecutionId::from_uuid(successor_id);
        // The retry is the same workflow going on: it belongs to the same
        // organization, and takes over the workflow id's hold, which the failed
        // run gave up when it was taken for this retry.
        new_execution.organization_id = workflow.organization_id;
        new_execution.holds_workflow_id = workflow.holds_workflow_id;
        new_execution.idempotency_key = Some(format!("workflow_retry_{}", old_execution_id));

        let outbox_payload = serde_json::json!({
            "execution_id": new_execution.execution_id,
            "workflow_name": new_execution.workflow_name,
            "workflow_version": new_execution.workflow_version,
            "namespace": new_execution.namespace,
            "task_queue": new_execution.task_queue,
            "parameters": new_execution.parameters,
        });
        let idempotency_key = format!("workflow_retry_{}", old_execution_id);

        if let Err(e) = self
            .repository
            .create_with_outbox_task(
                &new_execution,
                outbox_payload,
                idempotency_key,
                &workflow.task_queue,
            )
            .await
        {
            // A start of the workflow id got in between this run being taken
            // for the retry and its successor being created: the id is held
            // by that newer run, and a retry beside it would be a second run
            // of one workflow. The failure stands as this run's outcome.
            if is_workflow_id_held(&e) {
                warn!(
                    workflow_id = ?workflow.workflow_id,
                    execution_id = %old_execution_id,
                    "Workflow retry superseded: a newer start holds the workflow id; \
                     the failure is final"
                );
                self.repository.give_up_retry(workflow.id.as_uuid()).await?;
                return Ok(false);
            }
            // Already created — an earlier attempt committed and then reported
            // an error, or another pass got here first — is success.
            let exists = self
                .repository
                .find_by_id(successor_id)
                .await
                .ok()
                .flatten()
                .is_some();
            if !exists {
                return Err(OrcherError::internal(format!(
                    "workflow retry: failed to create successor: {}",
                    e
                )));
            }
        }

        self.successor_exists(workflow, &new_execution_id).await;
        Ok(true)
    }

    /// Record that `workflow`'s retry has its successor `new_execution_id`:
    /// clear the in-flight marker and keep a retried child attached to its
    /// parent.
    async fn successor_exists(&self, workflow: &WorkflowExecution, new_execution_id: &str) {
        // A retried child goes on in the successor. Moving its tracking row is
        // what keeps the successor its parent's child — the successor's own
        // `parent_execution_id` names the failed run, not the parent. Not
        // fatal: the successor exists, and the child reconciliation sweep
        // makes the same move when it finds the row naming a replaced run.
        if let Some(ref children) = self.child_repository {
            if let Err(e) = children
                .repoint_child_execution(&workflow.execution_id, new_execution_id)
                .await
            {
                warn!(
                    old_execution_id = %workflow.execution_id,
                    new_execution_id = %new_execution_id,
                    error = %e,
                    "Could not move child tracking to the retry; the sweep will"
                );
            }
        }
        // Left set, the stranded-retry pass finds the run again and, finding
        // the successor, clears it then.
        if let Err(e) = self
            .repository
            .retry_successor_created(workflow.id.as_uuid())
            .await
        {
            warn!(
                execution_id = %workflow.execution_id,
                error = %e,
                "Could not clear the retry's in-flight marker"
            );
        }
    }

    /// Finish retries whose scheduler stopped after taking the run and before
    /// creating its successor — a crash, a deploy. The run was marked
    /// `restarted_fresh`, which nothing else retries, and no successor was
    /// ever created: the workflow had simply stopped, with nothing reporting
    /// it. Each such run gets its successor now (under the same derived id,
    /// so a successor that did get created is not duplicated). A run whose
    /// successor exists under another id — retried by an engine from before
    /// successors had derived ids — only has its marker cleared.
    ///
    /// Restart-fresh and reset successors are not covered: their input is
    /// not recorded anywhere the run's row can recreate it from, and their
    /// ids are random.
    pub async fn recover_stranded_retries(&self) -> OrcherResult<usize> {
        let stranded = self
            .repository
            .find_stranded_retries(
                self.config.stranded_retry_grace_secs,
                self.config.batch_size,
            )
            .await?;
        let mut recovered = 0;
        for run in stranded {
            let existing = match self
                .repository
                .find_by_id(self.retry_successor_id(&run))
                .await?
            {
                Some(successor) => Some(successor),
                None => match run.workflow_id.as_deref() {
                    Some(workflow_id) => {
                        self.repository
                            .find_latest_successor(&run.execution_id, workflow_id)
                            .await?
                    }
                    None => None,
                },
            };
            match existing {
                Some(successor) => self.successor_exists(&run, &successor.execution_id).await,
                None => {
                    warn!(
                        execution_id = %run.execution_id,
                        "Retry was taken but its successor never created; creating it now"
                    );
                    if self
                        .create_retry_successor(&run, run.retry_count.max(0) as u32 + 1)
                        .await?
                    {
                        recovered += 1;
                    }
                }
            }
        }
        Ok(recovered)
    }

    /// Settle a failure the policy will not retry, so it stays final.
    ///
    /// This used to only log, leaving the run `failed` with retries still
    /// nominally left — so it matched the eligibility query on every poll,
    /// was re-evaluated to the same answer, and nothing could tell from the
    /// row that its failure was final. Settling records the decision: the run
    /// is no longer eligible, and a parent waiting on it is told by the child
    /// reconciliation sweep.
    async fn abandon_workflow(
        &self,
        workflow: &WorkflowExecution,
        reason: AbandonReason,
    ) -> OrcherResult<()> {
        self.repository
            .settle_failure(&workflow.execution_id)
            .await?;

        info!(
            workflow_id = %workflow.id,
            execution_id = %workflow.execution_id,
            reason = %reason,
            "Workflow retry abandoned; the failure is final"
        );

        Ok(())
    }

    /// Find failed workflows eligible for retry
    ///
    /// Queries the database for workflows that:
    /// - Failed, or timed out on their run timeout with a retry scheduled
    /// - Have retry_count < max_retries
    /// - Are past their next_retry_at time (or have no next_retry_at set)
    async fn find_failed_workflows(&self) -> OrcherResult<Vec<WorkflowExecution>> {
        let workflows = self
            .repository
            .find_due_for_retry(self.config.batch_size)
            .await?;

        debug!(
            count = workflows.len(),
            batch_size = self.config.batch_size,
            "Found failed workflows eligible for retry"
        );

        Ok(workflows)
    }

    /// Poll for failed workflows and schedule retries
    ///
    /// This is the main work function that runs in the background loop.
    async fn poll_and_schedule_retries(&self) -> OrcherResult<usize> {
        if let Err(e) = self.recover_stranded_retries().await {
            warn!(error = %e, "Stranded-retry pass failed; retrying next poll");
        }
        let failed_workflows = self.find_failed_workflows().await?;
        let count = failed_workflows.len();

        if count == 0 {
            debug!("No failed workflows found for retry");
            return Ok(0);
        }

        info!("Processing {} failed workflows for retry", count);

        let mut scheduled_count = 0;
        let mut abandoned_count = 0;

        for workflow in failed_workflows {
            match self.handle_workflow_failure(&workflow).await {
                Ok(RetryOutcome::Rescheduled { .. }) => {
                    scheduled_count += 1;
                }
                Ok(RetryOutcome::Abandoned { .. }) => {
                    abandoned_count += 1;
                }
                Ok(RetryOutcome::AlreadyHandled { .. }) => {}
                Err(e) => {
                    error!(
                        workflow_id = %workflow.id,
                        execution_id = %workflow.execution_id,
                        error = %e,
                        "Failed to handle workflow retry"
                    );
                }
            }
        }

        info!(
            scheduled = scheduled_count,
            abandoned = abandoned_count,
            total = count,
            "Completed retry scheduling poll"
        );

        Ok(scheduled_count)
    }

    /// Background polling loop
    ///
    /// Runs continuously until shutdown signal is received.
    async fn run(self: Arc<Self>) {
        let interval = Duration::from_secs(self.config.poll_interval_secs);

        info!(
            poll_interval_secs = self.config.poll_interval_secs,
            batch_size = self.config.batch_size,
            realtime_notifications = self.config.enable_realtime_notifications,
            "WorkflowRetryScheduler started"
        );

        // The listener wakes the scheduler for a retry as its failure path
        // schedules it, so a retry is started when its backoff ends rather
        // than at the next poll. It runs whenever there is a database to
        // listen on: a retry with a one-second backoff used to wait for the
        // poll, 30 seconds by default, unless real-time notifications were
        // switched on. The switch now only adds acting on every failure as it
        // is written, before its fate is decided.
        if self.db_pool.is_some() {
            let listener_scheduler = self.clone();
            let handle = tokio::spawn(async move {
                listener_scheduler.run_notification_listener().await;
            });
            *self.listener_handle.write().await = Some(handle);
            info!(
                failures = self.config.enable_realtime_notifications,
                "Retry notification listener started"
            );
        } else if self.config.enable_realtime_notifications {
            warn!("Real-time notifications enabled but database pool not provided - falling back to polling only");
        }

        loop {
            // Check shutdown signal
            if self.shutdown.load(Ordering::Relaxed) {
                info!("WorkflowRetryScheduler received shutdown signal");
                break;
            }

            // Increment poll counter
            self.total_polls.fetch_add(1, Ordering::Relaxed);

            // Execute poll and scheduling
            match self.poll_and_schedule_retries().await {
                Ok(count) => {
                    if count > 0 {
                        debug!("Scheduled {} workflows for retry", count);
                    }

                    // Update statistics
                    if self.config.enable_metrics {
                        let mut stats = self.stats.write().await;
                        stats.workflows_retried = self.workflows_retried.load(Ordering::Relaxed);
                        stats.workflows_abandoned =
                            self.workflows_abandoned.load(Ordering::Relaxed);
                        stats.total_polls = self.total_polls.load(Ordering::Relaxed);
                        stats.poll_errors = self.poll_errors.load(Ordering::Relaxed);
                        stats.last_poll_at = Some(Timestamptz::now());
                    }
                }
                Err(e) => {
                    error!(
                        error = %e,
                        "Error in workflow retry scheduler poll"
                    );

                    self.poll_errors.fetch_add(1, Ordering::Relaxed);

                    // Update error statistics
                    if self.config.enable_metrics {
                        let mut stats = self.stats.write().await;
                        stats.poll_errors = self.poll_errors.load(Ordering::Relaxed);
                        stats.last_error_at = Some(Timestamptz::now());
                    }
                }
            }

            // Sleep until next poll interval
            tokio::select! {
                _ = tokio::time::sleep(interval) => {}
                _ = self.stopped() => {}
            }
        }

        // Stop notification listener if running
        if let Some(handle) = self.listener_handle.write().await.take() {
            handle.abort();
            info!("Notification listener stopped");
        }

        info!("WorkflowRetryScheduler stopped");
    }

    /// Run the PostgreSQL LISTEN/NOTIFY notification listener
    ///
    /// This runs in a separate task and processes workflow failure notifications in real-time.
    async fn run_notification_listener(self: Arc<Self>) {
        let Some(pool) = &self.db_pool else {
            error!("Cannot start notification listener: database pool not available");
            return;
        };

        info!("Starting PostgreSQL LISTEN/NOTIFY listener for workflow failures");

        loop {
            if self.shutdown.load(Ordering::Relaxed) {
                info!("Notification listener received shutdown signal");
                break;
            }

            // Create listener
            let mut listener = match PgListener::connect_with(pool.as_ref()).await {
                Ok(l) => l,
                Err(e) => {
                    error!(error = %e, "Failed to create PostgreSQL listener, retrying in 5s...");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
            };

            // `workflow_failed` fires as a failure is written, before its fate
            // is decided; `workflow_retry_due` fires when the decision is a
            // retry, carrying when it is due. It is the one that lets a short
            // backoff be honoured without waiting for the next poll.
            let channels: &[&str] = if self.config.enable_realtime_notifications {
                &["workflow_failed", RETRY_DUE_CHANNEL]
            } else {
                &[RETRY_DUE_CHANNEL]
            };
            if let Err(e) = listener.listen_all(channels.iter().copied()).await {
                error!(error = %e, "Failed to LISTEN on the workflow retry channels, retrying in 5s...");
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }

            info!("Successfully subscribed to workflow failure and retry notifications");

            // Process notifications
            loop {
                if self.shutdown.load(Ordering::Relaxed) {
                    break;
                }

                // Wait for notification with timeout
                let received = tokio::select! {
                    received = tokio::time::timeout(Duration::from_secs(5), listener.recv()) => received,
                    _ = self.stopped() => break,
                };
                match received {
                    Ok(Ok(notification)) if notification.channel() == RETRY_DUE_CHANNEL => {
                        self.clone().retry_when_due(notification);
                    }
                    Ok(Ok(notification)) => {
                        // Process the notification
                        self.handle_notification(notification).await;
                    }
                    Ok(Err(e)) => {
                        error!(error = %e, "Error receiving notification, reconnecting...");
                        break; // Break inner loop to reconnect
                    }
                    Err(_) => {
                        // Timeout - just check shutdown flag and continue
                        continue;
                    }
                }
            }

            // Clean shutdown or reconnection
            let _ = listener.unlisten_all().await;
        }

        info!("Notification listener stopped");
    }

    /// Act on a `workflow_retry_due` notification: retry the run once its
    /// backoff has passed.
    ///
    /// Every replica hears it and every one may try; the retry claim lets one
    /// through. A wait longer than the poll interval is left to the poll,
    /// which will reach it first anyway, so no replica holds a task per
    /// far-off retry.
    fn retry_when_due(self: Arc<Self>, notification: PgNotification) {
        let due: RetryDuePayload = match serde_json::from_str(notification.payload()) {
            Ok(due) => due,
            Err(e) => {
                warn!(error = %e, payload = %notification.payload(), "Unreadable retry-due notification");
                return;
            }
        };
        let now_ms = Timestamptz::now().timestamp_millis();
        let wait = Duration::from_millis(due.due_at_ms.saturating_sub(now_ms).max(0) as u64);
        if wait > Duration::from_secs(self.config.poll_interval_secs) {
            return;
        }
        tokio::spawn(async move {
            // A little past due, so the check against this process's clock
            // agrees the retry is due.
            tokio::select! {
                _ = tokio::time::sleep(wait + Duration::from_millis(20)) => {}
                _ = self.stopped() => return,
            }
            if let Err(e) = self.process_single_workflow(&due.execution_id).await {
                warn!(
                    execution_id = %due.execution_id,
                    error = %e,
                    "Could not retry on its notification; the poll will"
                );
            }
        });
    }

    /// Handle a single workflow failure notification
    async fn handle_notification(&self, notification: PgNotification) {
        let payload = notification.payload();

        debug!(payload = %payload, "Received workflow failure notification");

        // Parse the JSON payload
        let workflow_data: WorkflowFailedPayload = match serde_json::from_str(payload) {
            Ok(data) => data,
            Err(e) => {
                error!(
                    error = %e,
                    payload = %payload,
                    "Failed to parse workflow failure notification payload"
                );
                return;
            }
        };

        info!(
            workflow_id = %workflow_data.workflow_id,
            execution_id = %workflow_data.execution_id,
            namespace = %workflow_data.namespace,
            "Processing workflow failure from real-time notification"
        );

        // Process this specific failed workflow
        if let Err(e) = self
            .process_single_workflow(&workflow_data.execution_id)
            .await
        {
            error!(
                error = %e,
                workflow_id = %workflow_data.workflow_id,
                execution_id = %workflow_data.execution_id,
                "Failed to process workflow from notification"
            );
        }
    }

    /// Process a single failed workflow by execution_id, as a failure
    /// notification does.
    pub async fn process_single_workflow(&self, execution_id: &str) -> OrcherResult<()> {
        // Fetch the workflow execution
        let execution = match self.repository.find_by_execution_id(execution_id).await? {
            Some(exec) => exec,
            None => {
                warn!(execution_id = %execution_id, "Workflow execution not found for notification");
                return Ok(());
            }
        };

        // Only process if still failed, or timed out on its run timeout
        if !matches!(execution.status.as_str(), "failed" | "timed_out") {
            debug!(
                execution_id = %execution_id,
                status = %execution.status,
                "Workflow no longer failed or timed out, skipping"
            );
            return Ok(());
        }

        // The failure path decides a failure's fate as it records it, and
        // this notification fires on the status write — before the decision
        // is on the row. Acting now would decide it a second time, possibly
        // differently: retry a failure the policy is about to settle, or retry
        // without the backoff. So only a retry that is recorded and already
        // due is taken up here. Anything else is left: a settled failure is
        // final, a pending retry is picked up by the polling pass when due,
        // and an undecided one by the polling pass once it has stayed
        // undecided past its grace period.
        let now = Timestamptz::now();
        let due = matches!(execution.next_retry_at, Some(at) if at <= now);
        if !execution.can_retry() || !due {
            debug!(
                execution_id = %execution_id,
                next_retry_at = ?execution.next_retry_at,
                "Failure not yet due for retry here; leaving it to its decision"
            );
            return Ok(());
        }

        // Handle the failure (will schedule retry or abandon)
        match self.handle_workflow_failure(&execution).await? {
            RetryOutcome::Rescheduled {
                workflow_id,
                next_attempt,
                scheduled_at,
            } => {
                info!(
                    workflow_id = %workflow_id,
                    next_attempt = next_attempt,
                    scheduled_at = %scheduled_at,
                    "Workflow scheduled for retry (from notification)"
                );
                self.workflows_retried.fetch_add(1, Ordering::Relaxed);
            }
            RetryOutcome::Abandoned {
                workflow_id,
                reason,
            } => {
                info!(
                    workflow_id = %workflow_id,
                    reason = ?reason,
                    "Workflow retry abandoned (from notification)"
                );
                self.workflows_abandoned.fetch_add(1, Ordering::Relaxed);
            }
            RetryOutcome::AlreadyHandled { .. } => {}
        }

        Ok(())
    }

    /// Start the retry scheduler in a background task
    ///
    /// Returns a JoinHandle that can be used to wait for the scheduler to stop.
    pub fn start(self: Arc<Self>) -> JoinHandle<()> {
        tokio::spawn(async move {
            self.run().await;
        })
    }

    /// Stop the retry scheduler gracefully
    ///
    /// Sets the shutdown flag and waits for notification listener to stop.
    pub async fn stop(&self) {
        info!("Stopping WorkflowRetryScheduler...");
        self.shutdown.store(true, Ordering::Relaxed);
        self.stop_signal.send_replace(true);

        // Wait for notification listener to stop if running
        if let Some(handle) = self.listener_handle.write().await.take() {
            info!("Waiting for notification listener to stop...");
            let _ = handle.await;
        }
    }

    /// Resolves once `stop` has been called, however long before.
    async fn stopped(&self) {
        let _ = self
            .stop_signal
            .subscribe()
            .wait_for(|stopped| *stopped)
            .await;
    }

    /// Get current statistics
    pub async fn statistics(&self) -> RetryStatistics {
        self.stats.read().await.clone()
    }

    /// Check if scheduler is running
    pub fn is_running(&self) -> bool {
        !self.shutdown.load(Ordering::Relaxed)
    }
}

// ================================================================================================
// TESTS
// ================================================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = WorkflowRetrySchedulerConfig::default();
        assert_eq!(config.poll_interval_secs, 30);
        assert_eq!(config.batch_size, 50);
        assert!(config.enable_metrics);
        assert!(!config.enable_realtime_notifications); // Disabled by default for backward compatibility
    }

    #[test]
    fn test_statistics_success_rate() {
        let stats = RetryStatistics {
            workflows_retried: 80,
            workflows_abandoned: 20,
            ..Default::default()
        };

        assert_eq!(stats.success_rate(), 80.0);
        assert_eq!(stats.abandonment_rate(), 20.0);
    }

    #[test]
    fn test_statistics_zero_division() {
        let stats = RetryStatistics::default();
        assert_eq!(stats.success_rate(), 0.0);
        assert_eq!(stats.abandonment_rate(), 0.0);
    }

    #[test]
    fn test_retry_outcome_debug() {
        let outcome = RetryOutcome::Rescheduled {
            workflow_id: Uuid::new_v4(),
            next_attempt: 2,
            scheduled_at: Timestamptz::now(),
        };
        let debug_str = format!("{:?}", outcome);
        assert!(debug_str.contains("Rescheduled"));
    }

    // Note: Integration tests requiring database are in crates/core/tests/
}
