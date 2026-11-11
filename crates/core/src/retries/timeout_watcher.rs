//! Task timeout detection and handling.

use orcher_common::error::{OrcherError, OrcherResult};
use orcher_common::types::identifiers::{ExecutionId, TaskExecutionId, WorkflowId};
use orcher_common::types::time::Timestamptz;
use orcher_storage::repositories::domains::workflow_execution::repository::EXECUTION_EXPIRES_AT;
use orcher_storage::schema::types::execution::{
    TaskExecution, EXECUTION_TIMED_OUT_FAILURE_TYPE, RUN_TIMED_OUT_FAILURE_TYPE,
};
use orcher_storage::sharding::pool_routing::ShardPoolResolver;
use sqlx::PgPool;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use super::retry_scheduler::{RetryOutcome, TaskRetryScheduler};
use super::task_failure;
use crate::workflows::{WorkflowRetryDecision, WorkflowRetryPolicy, WorkflowRetryPolicyEvaluator};
use jiff::SignedDuration;

/// What separates a session's queue name from the queue it was opened on:
/// `{task_queue}__session__{worker}`. Only the session's worker polls it.
const SESSION_QUEUE_SEPARATOR: &str = "__session__";

/// The timeout recorded on the tasks of a session found lost through another
/// of its tasks.
const SESSION_LOST: &str = "SessionLost";

/// Whether `task_queue` is a session's queue, polled only by its worker.
fn is_session_queue(task_queue: &str) -> bool {
    task_queue.contains(SESSION_QUEUE_SEPARATOR)
}

// ================================================================================================
// CONFIGURATION
// ================================================================================================

/// Configuration for the timeout watcher
#[derive(Debug, Clone)]
pub struct TimeoutWatcherConfig {
    /// How often to check for timeouts (in seconds)
    pub poll_interval_secs: u64,

    /// Maximum number of tasks to process per poll
    pub batch_size: usize,

    /// Enable schedule-to-start timeout detection
    pub enable_schedule_to_start: bool,

    /// Enable start-to-close timeout detection
    pub enable_start_to_close: bool,

    /// Enable heartbeat timeout detection
    pub enable_heartbeat: bool,

    /// Enable workflow timeout detection: the execution timeout (`timeout_secs`)
    /// and the run timeout (`run_timeout_secs`)
    pub enable_workflow_timeout: bool,

    /// Grace period before marking timeout (seconds)
    /// Prevents false positives due to clock skew
    pub grace_period_secs: i64,
}

impl Default for TimeoutWatcherConfig {
    fn default() -> Self {
        Self {
            poll_interval_secs: 30,
            batch_size: 100,
            enable_schedule_to_start: true,
            enable_start_to_close: true,
            enable_heartbeat: true,
            enable_workflow_timeout: true,
            grace_period_secs: 5,
        }
    }
}

// ================================================================================================
// TIMEOUT WATCHER
// ================================================================================================

/// Background service that monitors task executions for timeout violations
#[derive(Clone)]
pub struct TimeoutWatcher {
    pool: PgPool,
    config: TimeoutWatcherConfig,
    retry_scheduler: TaskRetryScheduler,
    running: Arc<AtomicBool>,
    /// Wakes the loop out of its sleep when `stop` is called. Without it the
    /// loop noticed only after a full poll interval, which is longer than
    /// shutdown waits for it, so every shutdown gave up on it instead.
    stop_signal: Arc<tokio::sync::Notify>,
    /// Optional shard-pool routing for partitioned persistence. Timeout sweeps scan
    /// every shard (a task/workflow row lives on its own shard); per-row handling routes
    /// by the workflow id. Unset ⇒ the single `pool` is used, byte-identical to before.
    shard_resolver: Arc<std::sync::OnceLock<Arc<dyn ShardPoolResolver>>>,
    /// How long after its claim an activation counts as in flight when a
    /// timed-out task wakes its workflow; see `release_claim_to_resume_on`.
    in_flight_bound_secs: Arc<std::sync::atomic::AtomicI64>,
}

impl TimeoutWatcher {
    /// Create a new timeout watcher
    pub fn new(pool: PgPool, config: TimeoutWatcherConfig) -> Self {
        Self {
            pool: pool.clone(),
            config,
            retry_scheduler: TaskRetryScheduler::new(pool),
            running: Arc::new(AtomicBool::new(false)),
            stop_signal: Arc::new(tokio::sync::Notify::new()),
            shard_resolver: Arc::new(std::sync::OnceLock::new()),
            in_flight_bound_secs: Arc::new(std::sync::atomic::AtomicI64::new(
                orcher_storage::repositories::domains::workflow_execution::DEFAULT_IN_FLIGHT_BOUND_SECS,
            )),
        }
    }

    /// How long after its claim an activation counts as in flight when a
    /// timed-out task wakes its workflow. Set alongside the execution
    /// repository's, so both wake-up paths agree.
    pub fn set_activation_in_flight_bound(&self, bound: std::time::Duration) {
        self.in_flight_bound_secs.store(
            bound.as_secs().max(1) as i64,
            std::sync::atomic::Ordering::Relaxed,
        );
    }

    /// Inject the shard-pool resolver (partitioned persistence). Idempotent; propagates
    /// to the inner retry scheduler so rescheduled/DLQ'd rows also co-locate.
    pub fn set_shard_resolver(&self, resolver: Arc<dyn ShardPoolResolver>) {
        let _ = self.shard_resolver.set(resolver.clone());
        self.retry_scheduler.set_shard_resolver(resolver);
    }

    /// The pool owning `workflow_id`'s shard (resolver's pick if set, else the single
    /// pool). `None` namespace ⇒ shared default set (OSS / single-tenant routing).
    fn pool_for(&self, workflow_id: Uuid) -> &PgPool {
        match self.shard_resolver.get() {
            Some(resolver) => resolver.pool_for_workflow(None, workflow_id),
            None => &self.pool,
        }
    }

    /// Pools to scan for a sweep keyed by status/time (not the routing id): every shard
    /// pool when a resolver is set, else the single pool.
    fn scan_pools(&self) -> Vec<&PgPool> {
        match self.shard_resolver.get() {
            Some(resolver) => resolver.all_pools(),
            None => vec![&self.pool],
        }
    }

    /// Start the timeout watcher in the background
    pub fn start(&self) -> JoinHandle<OrcherResult<()>> {
        info!(
            "Starting timeout watcher (interval: {}s, batch: {})",
            self.config.poll_interval_secs, self.config.batch_size
        );

        self.running.store(true, Ordering::SeqCst);
        let watcher = self.clone();

        tokio::spawn(async move { watcher.run_loop().await })
    }

    /// Stop the timeout watcher gracefully
    pub async fn stop(&self) {
        info!("Stopping timeout watcher...");
        self.running.store(false, Ordering::SeqCst);
        // notify_one keeps the wake-up if the loop is mid-check rather than
        // asleep, so it is not lost.
        self.stop_signal.notify_one();
    }

    /// Check if the watcher is running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Main loop that runs continuously
    async fn run_loop(&self) -> OrcherResult<()> {
        info!("Timeout watcher loop started");

        while self.is_running() {
            if let Err(e) = self.check_timeouts().await {
                error!("Error checking timeouts: {}", e);
            }

            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(self.config.poll_interval_secs)) => {}
                _ = self.stop_signal.notified() => {}
            }
        }

        info!("Timeout watcher loop stopped");
        Ok(())
    }

    /// Single iteration of timeout checking
    pub async fn check_timeouts(&self) -> OrcherResult<()> {
        debug!("Checking for timed-out tasks...");

        let mut total_timeouts = 0;

        // End the queued tasks a cancellation interrupted, before any of them
        // could be taken for a schedule-to-start timeout and recorded as one.
        // Not behind a switch: the start refuses such a task, so nothing else
        // ends it when the request and the poll that was refused it missed it.
        match self.cancel_interrupted_queued_tasks().await {
            Ok(count) => {
                if count > 0 {
                    info!(
                        "Cancelled {} queued tasks their workflow's cancellation interrupted",
                        count
                    );
                }
            }
            Err(e) => error!(
                "Error cancelling queued tasks interrupted by cancellation: {}",
                e
            ),
        }

        // And the tasks still waiting for a run that has ended: nothing will
        // ever read their results, and with no timeout nothing else ends them.
        match self.cancel_scheduled_of_ended_runs().await {
            Ok(count) if count > 0 => {
                info!("Cancelled {} scheduled tasks whose run had ended", count)
            }
            Ok(_) => {}
            Err(e) => error!("Error cancelling scheduled tasks of ended runs: {}", e),
        }

        // Check schedule-to-start timeouts
        if self.config.enable_schedule_to_start {
            match self.check_schedule_to_start_timeouts().await {
                Ok(count) => {
                    if count > 0 {
                        info!("Detected {} schedule-to-start timeouts", count);
                        total_timeouts += count;
                    }
                }
                Err(e) => error!("Error checking schedule-to-start timeouts: {}", e),
            }
        }

        // Check start-to-close timeouts
        if self.config.enable_start_to_close {
            match self.check_start_to_close_timeouts().await {
                Ok(count) => {
                    if count > 0 {
                        info!("Detected {} start-to-close timeouts", count);
                        total_timeouts += count;
                    }
                }
                Err(e) => error!("Error checking start-to-close timeouts: {}", e),
            }
        }

        // Check heartbeat timeouts
        if self.config.enable_heartbeat {
            match self.check_heartbeat_timeouts().await {
                Ok(count) => {
                    if count > 0 {
                        info!("Detected {} heartbeat timeouts", count);
                        total_timeouts += count;
                    }
                }
                Err(e) => error!("Error checking heartbeat timeouts: {}", e),
            }
        }

        // Check workflow execution timeouts
        if self.config.enable_workflow_timeout {
            match self.check_workflow_execution_timeouts().await {
                Ok(count) => {
                    if count > 0 {
                        info!("Detected {} workflow execution timeouts", count);
                        total_timeouts += count;
                    }
                }
                Err(e) => error!("Error checking workflow execution timeouts: {}", e),
            }
        }

        // Terminate cancelled workflows whose cleanup time is up. Not behind a
        // switch: a deadline is only ever set because a caller asked for one.
        match self.terminate_past_cancel_deadline().await {
            Ok(count) => {
                if count > 0 {
                    info!(
                        "Terminated {} workflows past their cancellation deadline",
                        count
                    );
                    total_timeouts += count;
                }
            }
            Err(e) => error!(
                "Error terminating workflows past their cancellation deadline: {}",
                e
            ),
        }

        if total_timeouts > 0 {
            info!("Total timeouts detected: {}", total_timeouts);
        }

        Ok(())
    }

    /// End, as cancelled before they started, the scheduled tasks their
    /// workflow's cancellation stops, and wake their workflows. Returns how
    /// many were ended. See [`super::cancelled_before_start`].
    async fn cancel_interrupted_queued_tasks(&self) -> OrcherResult<usize> {
        use orcher_storage::repositories::domains::task_execution::TaskExecutionRepository;
        use orcher_storage::repositories::PostgresExecutionLogRepository;

        let execution_log = PostgresExecutionLogRepository::new(self.pool.clone());
        let mut ended = 0;
        let mut woken = std::collections::HashSet::new();
        for pool in self.scan_pools() {
            let queued = TaskExecutionRepository::scheduled_stopped_by_cancellation_on(
                pool,
                None,
                self.config.batch_size as i64,
            )
            .await?;
            for task in &queued {
                match super::cancelled_before_start::cancel_task(pool, &execution_log, task).await {
                    Ok(true) => {
                        ended += 1;
                        woken.insert(task.workflow_id.as_uuid());
                    }
                    Ok(false) => {}
                    Err(e) => error!(
                        task_id = %task.task_id,
                        "Failed to cancel a queued task its workflow's cancellation interrupted: {}",
                        e
                    ),
                }
            }
        }
        for workflow_id in woken {
            self.trigger_workflow_requeue_for_task(workflow_id).await;
        }
        Ok(ended)
    }

    /// End the tasks still scheduled whose run has ended, on every shard.
    /// Returns how many were ended. See
    /// [`TaskExecutionRepository::cancel_scheduled_of_ended_runs_on`].
    async fn cancel_scheduled_of_ended_runs(&self) -> OrcherResult<u64> {
        use orcher_storage::repositories::domains::task_execution::TaskExecutionRepository;
        let mut ended = 0;
        for pool in self.scan_pools() {
            ended += TaskExecutionRepository::cancel_scheduled_of_ended_runs_on(
                pool,
                self.config.batch_size as i64,
            )
            .await?;
        }
        Ok(ended)
    }

    /// Check for schedule-to-start timeouts
    ///
    /// Finds tasks that are scheduled but haven't started within the timeout period
    async fn check_schedule_to_start_timeouts(&self) -> OrcherResult<usize> {
        let now = Timestamptz::now();
        let grace_period_secs = SignedDuration::from_secs(self.config.grace_period_secs).as_secs();

        // Keyed by status/time, not the routing id — scan every shard (a task lives on
        // its workflow's shard). With no resolver this is a single pool, unchanged.
        let sql = r#"
            SELECT id, organization_id, task_id, task_type, workflow_id, execution_id,
                   task_queue, namespace, service_identity, attempt_number, status,
                   scheduled_at, started_at, completed_at, task_token,
                   schedule_to_start_timeout_secs, start_to_close_timeout_secs,
                   heartbeat_timeout_secs, input, result, error_message, error_type,
                   error_details, retry_policy, headers, metadata,
                   last_heartbeat_at, heartbeat_details, created_at, updated_at
            FROM task_executions
            WHERE status = 'scheduled'
              AND schedule_to_start_timeout_secs IS NOT NULL
              AND scheduled_at + (schedule_to_start_timeout_secs || ' seconds')::interval + ($1 || ' seconds')::interval < $2
            ORDER BY scheduled_at ASC
            LIMIT $3
            "#;
        let mut timed_out_tasks: Vec<TaskExecutionRow> = Vec::new();
        for pool in self.scan_pools() {
            let batch = sqlx::query_as::<_, TaskExecutionRow>(sql)
                .bind(grace_period_secs)
                .bind(now)
                .bind(self.config.batch_size as i64)
                .fetch_all(pool)
                .await
                .map_err(|e| {
                    error!("Failed to query schedule-to-start timeouts: {}", e);
                    OrcherError::internal(format!("Failed to query timeouts: {}", e))
                })?;
            timed_out_tasks.extend(batch);
        }

        let count = timed_out_tasks.len();

        for row in timed_out_tasks {
            let task = TaskExecution::from(row);
            if let Err(e) = self.handle_timeout(task, "ScheduleToStart").await {
                error!("Failed to handle schedule-to-start timeout: {}", e);
            }
        }

        Ok(count)
    }

    /// Check for start-to-close timeouts
    ///
    /// Finds tasks that started but haven't completed within the timeout period
    async fn check_start_to_close_timeouts(&self) -> OrcherResult<usize> {
        let now = Timestamptz::now();
        let grace_period_secs = SignedDuration::from_secs(self.config.grace_period_secs).as_secs();

        // Scan every shard (task rows are shard-resident); single pool when unpartitioned.
        let sql = r#"
            SELECT id, organization_id, task_id, task_type, workflow_id, execution_id,
                   task_queue, namespace, service_identity, attempt_number, status,
                   scheduled_at, started_at, completed_at, task_token,
                   schedule_to_start_timeout_secs, start_to_close_timeout_secs,
                   heartbeat_timeout_secs, input, result, error_message, error_type,
                   error_details, retry_policy, headers, metadata,
                   last_heartbeat_at, heartbeat_details, created_at, updated_at
            FROM task_executions
            WHERE status = 'started'
              AND start_to_close_timeout_secs IS NOT NULL
              AND started_at IS NOT NULL
              AND started_at + (start_to_close_timeout_secs || ' seconds')::interval + ($1 || ' seconds')::interval < $2
            ORDER BY started_at ASC
            LIMIT $3
            "#;
        let mut timed_out_tasks: Vec<TaskExecutionRow> = Vec::new();
        for pool in self.scan_pools() {
            let batch = sqlx::query_as::<_, TaskExecutionRow>(sql)
                .bind(grace_period_secs)
                .bind(now)
                .bind(self.config.batch_size as i64)
                .fetch_all(pool)
                .await
                .map_err(|e| {
                    error!("Failed to query start-to-close timeouts: {}", e);
                    OrcherError::internal(format!("Failed to query timeouts: {}", e))
                })?;
            timed_out_tasks.extend(batch);
        }

        let count = timed_out_tasks.len();

        for row in timed_out_tasks {
            let task = TaskExecution::from(row);
            if let Err(e) = self.handle_timeout(task, "StartToClose").await {
                error!("Failed to handle start-to-close timeout: {}", e);
            }
        }

        Ok(count)
    }

    /// Check for heartbeat timeouts
    ///
    /// Finds tasks that are running but haven't sent a heartbeat within the timeout period:
    /// the task's own, or — for an attempt taken by a worker that heartbeats every task on
    /// its own — the default it was given when it started (`auto_heartbeat_timeout_secs`).
    /// That default is what recovers an attempt whose worker died, or whose poll response
    /// was lost, when the task sets no timeout at all.
    async fn check_heartbeat_timeouts(&self) -> OrcherResult<usize> {
        let now = Timestamptz::now();
        let grace_period_secs = SignedDuration::from_secs(self.config.grace_period_secs).as_secs();

        // Scan every shard (task rows are shard-resident); single pool when unpartitioned.
        let sql = r#"
            SELECT id, organization_id, task_id, task_type, workflow_id, execution_id,
                   task_queue, namespace, service_identity, attempt_number, status,
                   scheduled_at, started_at, completed_at, task_token,
                   schedule_to_start_timeout_secs, start_to_close_timeout_secs,
                   heartbeat_timeout_secs, input, result, error_message, error_type,
                   error_details, retry_policy, headers, metadata,
                   last_heartbeat_at, heartbeat_details, created_at, updated_at
            FROM task_executions
            WHERE status = 'started'
              AND COALESCE(heartbeat_timeout_secs, auto_heartbeat_timeout_secs) IS NOT NULL
              AND started_at IS NOT NULL
              AND COALESCE(last_heartbeat_at, started_at)
                  + (COALESCE(heartbeat_timeout_secs, auto_heartbeat_timeout_secs) || ' seconds')::interval
                  + ($1 || ' seconds')::interval < $2
            ORDER BY COALESCE(last_heartbeat_at, started_at) ASC
            LIMIT $3
            "#;
        let mut timed_out_tasks: Vec<TaskExecutionRow> = Vec::new();
        for pool in self.scan_pools() {
            let batch = sqlx::query_as::<_, TaskExecutionRow>(sql)
                .bind(grace_period_secs)
                .bind(now)
                .bind(self.config.batch_size as i64)
                .fetch_all(pool)
                .await
                .map_err(|e| {
                    error!("Failed to query heartbeat timeouts: {}", e);
                    OrcherError::internal(format!("Failed to query timeouts: {}", e))
                })?;
            timed_out_tasks.extend(batch);
        }

        let count = timed_out_tasks.len();

        for row in timed_out_tasks {
            let task = TaskExecution::from(row);
            if let Err(e) = self.handle_timeout(task, "Heartbeat").await {
                error!("Failed to handle heartbeat timeout: {}", e);
            }
        }

        Ok(count)
    }

    /// Time out, now, the running attempts of worker processes that have
    /// stopped being seen, rather than when their heartbeat timeouts run out.
    /// Returns how many were found to time out. Each is handled as a heartbeat timeout
    /// is — retried under its retry policy, or failed and its workflow woken —
    /// and only while it is still running, so an attempt that finished in the
    /// meantime, or that another engine timed out first, is left as it is.
    ///
    /// A process that stopped polling may only be busy: a worker with no room
    /// for more work does not poll. A second copy of a task is not harmless
    /// the way a second activation is, so an attempt is timed out here only
    /// on evidence of its own as well. It must run under a heartbeat timeout
    /// — its own, or the default for a worker that heartbeats on its own —
    /// and must not have heartbeated for `lapse_secs`, nor for half that
    /// timeout: a worker heartbeating on its own does so at a third of it or
    /// less, so a live one has heartbeated since. An attempt with no heartbeat
    /// timeout gives no such evidence, and is left to its other timeouts.
    pub async fn time_out_tasks_of_lapsed_workers(
        &self,
        lapsed: &[orcher_storage::repositories::domains::worker::LapsedInstance],
        lapse_secs: i64,
    ) -> OrcherResult<usize> {
        if lapsed.is_empty() {
            return Ok(0);
        }
        let organizations: Vec<Uuid> = lapsed.iter().map(|l| l.organization_id).collect();
        let namespaces: Vec<&str> = lapsed.iter().map(|l| l.namespace.as_str()).collect();
        let instances: Vec<&str> = lapsed
            .iter()
            .map(|l| l.worker_instance_id.as_str())
            .collect();
        let sql = r#"
            SELECT t.id, t.organization_id, t.task_id, t.task_type, t.workflow_id, t.execution_id,
                   t.task_queue, t.namespace, t.service_identity, t.attempt_number, t.status,
                   t.scheduled_at, t.started_at, t.completed_at, t.task_token,
                   t.schedule_to_start_timeout_secs, t.start_to_close_timeout_secs,
                   t.heartbeat_timeout_secs, t.input, t.result, t.error_message, t.error_type,
                   t.error_details, t.retry_policy, t.headers, t.metadata,
                   t.last_heartbeat_at, t.heartbeat_details, t.created_at, t.updated_at
            FROM unnest($1::uuid[], $2::text[], $3::text[]) AS lapsed(organization_id, namespace, instance)
            JOIN task_executions t
              ON t.worker_instance_id = lapsed.instance
             AND t.status = 'started'
             AND t.worker_instance_id IS NOT NULL
             AND t.namespace = lapsed.namespace
             AND COALESCE(t.organization_id, '00000000-0000-0000-0000-000000000000'::uuid)
                 = lapsed.organization_id
            WHERE t.started_at IS NOT NULL
              AND COALESCE(t.heartbeat_timeout_secs, t.auto_heartbeat_timeout_secs) IS NOT NULL
              AND COALESCE(t.last_heartbeat_at, t.started_at) < NOW() - make_interval(
                    secs => GREATEST(
                        $4::double precision,
                        COALESCE(t.heartbeat_timeout_secs, t.auto_heartbeat_timeout_secs) / 2.0
                    ))
            ORDER BY COALESCE(t.last_heartbeat_at, t.started_at)
            LIMIT $5
            "#;
        let mut stale: Vec<TaskExecutionRow> = Vec::new();
        for pool in self.scan_pools() {
            let batch = sqlx::query_as::<_, TaskExecutionRow>(sql)
                .bind(&organizations)
                .bind(&namespaces)
                .bind(&instances)
                .bind(lapse_secs as f64)
                .bind(self.config.batch_size as i64)
                .fetch_all(pool)
                .await
                .map_err(|e| {
                    OrcherError::internal(format!(
                        "Failed to read the tasks of lapsed worker processes: {e}"
                    ))
                })?;
            stale.extend(batch);
        }
        let count = stale.len();
        for row in stale {
            let task = TaskExecution::from(row);
            info!(
                task_id = %task.task_id,
                attempt = task.attempt_number,
                "Timing out a task whose worker process stopped being seen"
            );
            if let Err(e) = self.handle_timeout(task, "Heartbeat").await {
                error!(
                    "Failed to time out a task of a lapsed worker process: {}",
                    e
                );
            }
        }
        Ok(count)
    }

    /// Check for workflow execution and run timeouts
    ///
    /// Finds workflows past their execution timeout or their run timeout
    /// (`run_timeout_secs`, V065). The execution timeout is one deadline the
    /// whole chain of runs shares (`execution_deadline`, V067), so a successor
    /// run is held to the deadline its workflow started with; the run timeout
    /// is measured from when the run's own clocks start — its `submitted_at`,
    /// so a successor's run clock starts when it is created, or for a delayed
    /// start the time it is due (`start_after`, V069), so a delay is not spent
    /// from it. A run past both is reported as past its execution timeout.
    async fn check_workflow_execution_timeouts(&self) -> OrcherResult<usize> {
        let now = Timestamptz::now();
        let grace_period_secs = SignedDuration::from_secs(self.config.grace_period_secs).as_secs();

        // Workflow rows are shard-resident — scan every shard; single pool when unpartitioned.
        let sql = format!(
            r#"
            SELECT id, execution_id, workflow_id, task_queue, namespace,
                   submitted_at, timeout_secs, run_timeout_secs, retry_count, max_retries,
                   retry_policy,
                   {expires} AS execution_deadline,
                   NOT COALESCE({expires} + ($1 || ' seconds')::interval < $2, FALSE)
                       AS run_deadline
            FROM workflow_executions
            WHERE status NOT IN ('completed', 'failed', 'cancelled', 'terminated', 'timed_out',
                                 'restarted_fresh', 'reset')
              AND ({expires} + ($1 || ' seconds')::interval < $2
                   OR (run_timeout_secs IS NOT NULL
                       AND COALESCE(start_after, submitted_at)
                           + (run_timeout_secs || ' seconds')::interval
                           + ($1 || ' seconds')::interval < $2))
            ORDER BY submitted_at ASC
            LIMIT $3
            "#,
            expires = EXECUTION_EXPIRES_AT,
        );
        let mut timed_out: Vec<WorkflowExecutionTimeoutRow> = Vec::new();
        for pool in self.scan_pools() {
            let batch = sqlx::query_as::<_, WorkflowExecutionTimeoutRow>(&sql)
                .bind(grace_period_secs)
                .bind(now)
                .bind(self.config.batch_size as i64)
                .fetch_all(pool)
                .await
                .map_err(|e| {
                    error!("Failed to query workflow execution timeouts: {}", e);
                    OrcherError::internal(format!("Failed to query workflow timeouts: {}", e))
                })?;
            timed_out.extend(batch);
        }

        let count = timed_out.len();

        for row in timed_out {
            if let Err(e) = self.handle_workflow_timeout(&row).await {
                error!(
                    execution_id = %row.execution_id,
                    "Failed to handle workflow execution timeout: {}",
                    e
                );
            }
        }

        Ok(count)
    }

    /// Terminate the runs still live past the deadline their cancellation gave
    /// them to clean up. Guarded in the `UPDATE` like every other ending, so a
    /// run that ended meanwhile keeps its ending, and a failure waiting for its
    /// retry is ended with the retry, as a client's termination does.
    async fn terminate_past_cancel_deadline(&self) -> OrcherResult<usize> {
        let sql = r#"
            UPDATE workflow_executions
            SET status = 'terminated',
                completed_at = COALESCE(completed_at, NOW()),
                next_retry_at = NULL,
                updated_at = NOW()
            WHERE id IN (
                SELECT id FROM workflow_executions
                WHERE cancel_deadline IS NOT NULL
                  AND cancel_deadline < $1
                  AND (status NOT IN ('completed', 'failed', 'cancelled', 'terminated',
                                      'timed_out', 'restarted_fresh', 'reset')
                       OR (status IN ('failed', 'timed_out') AND retries_settled_at IS NULL
                           AND retry_count < max_retries AND next_retry_at IS NOT NULL))
                ORDER BY cancel_deadline
                LIMIT $2
                FOR UPDATE SKIP LOCKED
            )
            RETURNING execution_id
            "#;
        let now = Timestamptz::now();
        let mut terminated = 0;
        for pool in self.scan_pools() {
            let ended: Vec<String> = sqlx::query_scalar(sql)
                .bind(now)
                .bind(self.config.batch_size as i64)
                .fetch_all(pool)
                .await
                .map_err(|e| {
                    OrcherError::internal(format!(
                        "Failed to terminate workflows past their cancellation deadline: {}",
                        e
                    ))
                })?;
            for execution_id in &ended {
                info!(
                    execution_id = %execution_id,
                    "Workflow terminated: it had not ended by its cancellation deadline"
                );
            }
            terminated += ended.len();
        }
        Ok(terminated)
    }

    /// Mark a workflow execution as timed_out, and decide in the same write
    /// whether it is retried.
    ///
    /// A run replaced by a successor (`restarted_fresh`, `reset`) is left
    /// alone even though it still carries the deadline it started with: it
    /// has ended already, and marking it timed out would report a live
    /// workflow — a child's successor, say — as over.
    ///
    /// A run past its run timeout is retried as a failure is: its workflow's
    /// retry policy decides, on the failure type `WorkflowRunTimedOut`, and no
    /// retry is scheduled that would start past the execution timeout. The
    /// retry scheduler then replaces it with a fresh run, which starts its own
    /// run clock. A run past the execution timeout is never retried: that
    /// timeout covers every run of the workflow. Either way the decision is
    /// written with the timeout — `next_retry_at`, or `retries_settled_at` —
    /// so no reader ever sees a timed-out run whose fate is undecided.
    async fn handle_workflow_timeout(&self, row: &WorkflowExecutionTimeoutRow) -> OrcherResult<()> {
        let (error, failure_type) = if row.run_deadline {
            ("Workflow run timed out", RUN_TIMED_OUT_FAILURE_TYPE)
        } else {
            (
                "Workflow execution timed out",
                EXECUTION_TIMED_OUT_FAILURE_TYPE,
            )
        };
        let next_retry_at = if row.run_deadline {
            let policy =
                WorkflowRetryPolicy::from_stored(row.max_retries, row.retry_policy.as_ref());
            match WorkflowRetryPolicyEvaluator::new().decide(
                row.retry_count,
                &policy,
                Some(failure_type),
                // A timeout is never raised by the workflow's code.
                false,
                row.execution_deadline,
            ) {
                WorkflowRetryDecision::Retry { scheduled_at, .. } => Some(scheduled_at),
                WorkflowRetryDecision::Abandon { .. } => None,
            }
        } else {
            None
        };
        info!(
            execution_id = %row.execution_id,
            timeout_secs = row.timeout_secs,
            run_timeout_secs = row.run_timeout_secs,
            retry_at = ?next_retry_at,
            "Handling workflow timeout: {}",
            error
        );

        let now = Timestamptz::now();

        // Atomically mark as timed_out, release the claim, and record the
        // retry decision. A scheduled retry is announced on the channel the
        // retry scheduler's realtime listener takes due retries from, by the
        // same statement, so it is sent if and only if the decision commits.
        let timed_out: Vec<String> = sqlx::query_scalar(
            r#"
            WITH timed_out AS (
                UPDATE workflow_executions
                SET status             = 'timed_out',
                    error              = $3,
                    error_details      = jsonb_build_object('failure_type', $4::text),
                    completed_at       = $1,
                    claimed_by         = NULL,
                    claimed_at         = NULL,
                    claim_timeout_at   = NULL,
                    claimed_on_node    = NULL,
                    next_retry_at      = $5,
                    retries_settled_at = CASE WHEN $5::timestamptz IS NULL THEN $1 END,
                    updated_at         = NOW()
                WHERE id = $2
                  AND status NOT IN ('completed', 'failed', 'cancelled', 'terminated',
                                     'timed_out', 'restarted_fresh', 'reset')
                RETURNING execution_id, next_retry_at
            )
            SELECT execution_id
            FROM timed_out
            LEFT JOIN LATERAL (
                     SELECT pg_notify(
                         'workflow_retry_due',
                         json_build_object(
                             'execution_id', execution_id,
                             'due_at_ms', (EXTRACT(EPOCH FROM next_retry_at) * 1000)::bigint
                         )::text
                     )
                     WHERE next_retry_at IS NOT NULL
                 ) AS announced ON TRUE
            "#,
        )
        .bind(now)
        .bind(row.id)
        .bind(error)
        .bind(failure_type)
        .bind(next_retry_at)
        .fetch_all(self.pool_for(row.id))
        .await
        .map_err(|e| {
            OrcherError::internal(format!("Failed to mark workflow as timed_out: {}", e))
        })?;

        if timed_out.is_empty() {
            // Another concurrent process already handled this workflow, or it
            // ended on its own first.
            debug!(
                execution_id = %row.execution_id,
                "Workflow had already ended; its timeout is not recorded"
            );
        }
        Ok(())
    }

    /// Handle a timed-out task
    /// Handle one timed-out attempt, and, when it ran on a session's queue,
    /// the rest of that session's work.
    ///
    /// A session's tasks run on a queue only its worker polls. An attempt
    /// there that times out means that worker is gone (or no longer heard
    /// from), so it cannot be retried on that queue, where nothing would ever
    /// take it: it fails, as does every task still waiting or running on the
    /// queue, each recorded as a terminal failure the workflow reads. The
    /// workflow learns its session is lost and can open another.
    async fn handle_timeout(&self, task: TaskExecution, timeout_type: &str) -> OrcherResult<()> {
        let session_queue = is_session_queue(&task.task_queue).then(|| task.task_queue.clone());
        let task_id = task.id;
        self.record_timeout(task, timeout_type).await?;
        if let Some(queue) = session_queue {
            self.fail_rest_of_session(&queue, task_id).await;
        }
        Ok(())
    }

    /// Fail every task still waiting or running on the lost session's
    /// `queue`, other than `except`, each recorded as a terminal failure.
    /// Errors are logged: a task that could not be failed now is failed by
    /// its own timeout later, by the same path.
    async fn fail_rest_of_session(&self, queue: &str, except: TaskExecutionId) {
        let sql = r#"
            SELECT id, organization_id, task_id, task_type, workflow_id, execution_id,
                   task_queue, namespace, service_identity, attempt_number, status,
                   scheduled_at, started_at, completed_at, task_token,
                   schedule_to_start_timeout_secs, start_to_close_timeout_secs,
                   heartbeat_timeout_secs, input, result, error_message, error_type,
                   error_details, retry_policy, headers, metadata,
                   last_heartbeat_at, heartbeat_details, created_at, updated_at
            FROM task_executions
            WHERE task_queue = $1 AND status IN ('scheduled', 'started') AND id <> $2
            "#;
        for pool in self.scan_pools() {
            let rows = match sqlx::query_as::<_, TaskExecutionRow>(sql)
                .bind(queue)
                .bind(except.as_uuid())
                .fetch_all(pool)
                .await
            {
                Ok(rows) => rows,
                Err(e) => {
                    error!(session_queue = %queue, error = %e, "Failed to find the lost session's tasks");
                    continue;
                }
            };
            if !rows.is_empty() {
                warn!(
                    session_queue = %queue,
                    count = rows.len(),
                    "Failing the rest of a lost session's tasks"
                );
            }
            for row in rows {
                let task = TaskExecution::from(row);
                if let Err(e) = self.record_timeout(task, SESSION_LOST).await {
                    error!(session_queue = %queue, error = %e, "Failed to fail a lost session's task");
                }
            }
        }
    }

    /// Record one timed-out attempt: retried under its retry policy, or
    /// failed for good and its workflow woken.
    async fn record_timeout(&self, task: TaskExecution, timeout_type: &str) -> OrcherResult<()> {
        info!(
            "Handling {} timeout for task {} (attempt {})",
            timeout_type, task.task_id, task.attempt_number
        );

        // Mark task as timed out
        let mut timed_out_task = task.clone();
        timed_out_task.mark_timed_out(timeout_type.to_string());

        // Recorded as a worker-reported failure is, and in the same order.
        //
        // This used to mark the attempt timed out, schedule the retry or
        // dead-letter it, and for a terminal timeout then record the failed
        // step and wake the workflow, each written on its own. A recording
        // that failed was only logged; the attempt had already left
        // `started`, so no sweep selected it again, and the woken workflow,
        // which finds a task's outcome only in its step rows, waited on the
        // task forever.
        //
        // Now the retry is decided first, without writing anything. A
        // terminal timeout's step row is written next, guarded so it never
        // replaces a completion that raced it. Then one transaction marks the
        // attempt timed out, writes the retry row or dead-letter mark, and
        // appends TaskFailed. Any error leaves the attempt in the state the
        // sweep found it in, so the next sweep takes it again; the step row is
        // an upsert, so writing it twice changes nothing.
        let mut decision = self
            .retry_scheduler
            .decide_timeout(&timed_out_task, timeout_type)?;
        // Work its workflow's cancellation interrupted is not retried: the
        // retry would only run it again.
        if matches!(decision, crate::retries::RetryDecision::Retry { .. })
            && orcher_storage::repositories::domains::task_execution::TaskExecutionRepository::stopped_by_cancellation_on(
                self.pool_for(task.workflow_id.as_uuid()),
                task.id,
            )
            .await?
        {
            decision = crate::retries::RetryDecision::MoveToDlq {
                reason: crate::retries::DlqReason::StoppedByCancellation,
            };
        }
        // A task pinned to a session runs only on that session's worker; a
        // retry would be queued where nothing polls.
        if is_session_queue(&task.task_queue) {
            decision = crate::retries::RetryDecision::MoveToDlq {
                reason: crate::retries::DlqReason::SessionLost,
            };
            timed_out_task.error_message = Some(format!(
                "The task's session was lost: its worker stopped ({timeout_type} timeout), and \
                 only that worker could run the session's tasks"
            ));
        }
        let terminal = !matches!(decision, crate::retries::RetryDecision::Retry { .. });
        let now = Timestamptz::now();
        let message = timed_out_task
            .error_message
            .clone()
            .unwrap_or_else(|| "Task timed out".to_string());
        let details = timed_out_task.error_details.clone();

        let step = terminal
            .then(|| task_failure::failed_step(&timed_out_task, &message, details.as_ref(), now));
        if let Some(step) = &step {
            use orcher_storage::repositories::{
                ExecutionLogRepository, PostgresExecutionLogRepository,
            };
            let written = PostgresExecutionLogRepository::new(self.pool.clone())
                .store_failure(step)
                .await
                .map_err(|e| {
                    OrcherError::internal(format!("Failed to record the failed step: {e}"))
                })?;
            if !written {
                debug!(
                    task_id = %task.task_id,
                    "The step already completed; its result stands"
                );
            }
        }

        // Only while the attempt is still in the state it was found in. It
        // may have been completed, failed or cancelled since the sweep read
        // it; timing it out then would overwrite that outcome and schedule a
        // retry of a task that already ran, which the default heartbeat
        // timeout, applying to every attempt of a worker that heartbeats on
        // its own, would make far from rare.
        let mut tx = self
            .pool_for(task.workflow_id.as_uuid())
            .begin()
            .await
            .map_err(|e| OrcherError::internal(format!("Failed to begin: {e}")))?;
        let marked = sqlx::query(
            r#"
            UPDATE task_executions
            SET status = 'timed_out',
                error_message = $1,
                error_type = $5,
                -- A task failed before it started (the rest of a lost
                -- session's work) is given its end time as its start:
                -- every status but 'scheduled' requires one.
                started_at = COALESCE(started_at, $2),
                completed_at = $2,
                updated_at = $3
            WHERE id = $4 AND status = $6
            "#,
        )
        .bind(timed_out_task.error_message.as_ref())
        .bind(timed_out_task.completed_at)
        .bind(timed_out_task.updated_at)
        .bind(task.id.as_uuid())
        .bind(timed_out_task.error_type.as_ref())
        .bind(&task.status)
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            error!("Failed to mark task as timed out: {}", e);
            OrcherError::internal(format!("Failed to update task status: {}", e))
        })?
        .rows_affected();
        if marked == 0 {
            debug!(
                task_id = %task.task_id,
                timeout_type,
                "Task left its state before it could be timed out; nothing to do"
            );
            return Ok(());
        }
        let outcome = self
            .retry_scheduler
            .apply_on(&mut tx, &timed_out_task, decision)
            .await?;
        orcher_storage::repositories::domains::journal::JournalRepository::append_next_on(
            &mut tx,
            task_failure::task_failed_entry(&timed_out_task, &message, details.as_ref(), now),
        )
        .await
        .map_err(|e| OrcherError::internal(format!("Failed to journal the task failure: {e}")))?;
        // A timeout that ends the step settles it, with the rest; a step that
        // completed first keeps its result.
        if let Some(step) = &step {
            crate::workflows::journal::SettledStep::from(step)
                .append_on(
                    &mut tx,
                    &timed_out_task.workflow_id.to_string(),
                    &timed_out_task.execution_id.to_string(),
                )
                .await?;
        }
        tx.commit()
            .await
            .map_err(|e| OrcherError::internal(format!("Failed to record the timeout: {e}")))?;

        match outcome {
            RetryOutcome::Rescheduled {
                new_task_execution_id: _,
                next_attempt,
                scheduled_at,
            } => {
                info!(
                    "Scheduled retry for task {} (attempt {}) at {}",
                    task.task_id, next_attempt, scheduled_at
                );
            }
            RetryOutcome::MovedToDlq { reason } => {
                warn!(
                    "Task {} moved to DLQ after {} timeout: {}",
                    task.task_id, timeout_type, reason
                );
                // No more retries, and the failure is recorded where the
                // workflow's replay reads it: wake the workflow to handle it.
                self.trigger_workflow_requeue_for_task(task.workflow_id.as_uuid())
                    .await;
            }
        }

        Ok(())
    }

    /// Wake the workflow after its task times out with no remaining retries.
    ///
    /// The same wake-up every other resume path gives: the claim is released,
    /// so the pollers' scan hands the workflow out again within a poll
    /// interval — or, when an activation of it is out with a worker, the
    /// wake-up is left for that activation's completion to take. It used to
    /// set `pending_requeue` and nothing more, which nothing read: a workflow
    /// parked on a task that timed out waited for its claim to time out too.
    /// No continuation is queued: that is in-process state of the polling
    /// manager, which this watcher does not have, and the scan finds the
    /// workflow without it.
    async fn trigger_workflow_requeue_for_task(&self, workflow_id: uuid::Uuid) {
        // Nothing retries this later: the task is already timed out, so the
        // watcher will not come back to it. A transient failure is retried
        // here, and one that persists is an error, because the workflow then
        // waits on a task that has finished until something else wakes it.
        const ATTEMPTS: u32 = 3;
        let mut delay = std::time::Duration::from_millis(100);
        for attempt in 1..=ATTEMPTS {
            let result =
                orcher_storage::repositories::domains::workflow_execution::release_claim_to_resume_on(
                    self.pool_for(workflow_id),
                    "id = $1::uuid AND status NOT IN \
                     ('completed', 'failed', 'cancelled', 'terminated', 'timed_out')",
                    &workflow_id.to_string(),
                    self.in_flight_bound_secs
                        .load(std::sync::atomic::Ordering::Relaxed),
                    // Held for nobody: a wake-up after a task timed out is
                    // rare, and this watcher is not told the hold.
                    std::time::Duration::ZERO,
                )
                .await;
            match result {
                Ok(resume) => {
                    if resume
                        == orcher_storage::repositories::domains::workflow_execution::Resume::NotFound
                    {
                        debug!(
                            workflow_id = %workflow_id,
                            "No running workflow to requeue after the task ended"
                        );
                    }
                    return;
                }
                Err(e) if attempt < ATTEMPTS => {
                    warn!(workflow_id = %workflow_id, attempt, error = %e, "Requeue failed; retrying");
                    tokio::time::sleep(delay).await;
                    delay *= 2;
                }
                Err(e) => {
                    error!(
                        workflow_id = %workflow_id,
                        error = %e,
                        "Could not requeue the workflow after its task ended: it will wait \
                         until its claim times out or it is woken another way"
                    );
                }
            }
        }
    }

    /// Bulk-fail all pending/started tasks on an orphaned session queue.
    ///
    /// Get statistics about timeout detection
    pub async fn get_statistics(&self) -> OrcherResult<TimeoutStatistics> {
        // Global aggregate — fan out across every shard and sum the per-pool counts.
        let sql = r#"
            SELECT
                COUNT(CASE WHEN status = 'timed_out' AND error_message LIKE '%ScheduleToStart%' THEN 1 END) as schedule_to_start_timeouts,
                COUNT(CASE WHEN status = 'timed_out' AND error_message LIKE '%StartToClose%' THEN 1 END) as start_to_close_timeouts,
                COUNT(CASE WHEN status = 'timed_out' AND error_message LIKE '%Heartbeat%' THEN 1 END) as heartbeat_timeouts,
                COUNT(CASE WHEN status = 'timed_out' THEN 1 END) as total_timeouts,
                COUNT(CASE WHEN status = 'started'
                    AND schedule_to_start_timeout_secs IS NOT NULL
                    AND scheduled_at + (schedule_to_start_timeout_secs || ' seconds')::interval < NOW()
                    THEN 1 END) as pending_schedule_to_start,
                COUNT(CASE WHEN status = 'started'
                    AND start_to_close_timeout_secs IS NOT NULL
                    AND started_at + (start_to_close_timeout_secs || ' seconds')::interval < NOW()
                    THEN 1 END) as pending_start_to_close,
                COUNT(CASE WHEN status = 'started'
                    AND COALESCE(heartbeat_timeout_secs, auto_heartbeat_timeout_secs) IS NOT NULL
                    AND COALESCE(last_heartbeat_at, started_at)
                        + (COALESCE(heartbeat_timeout_secs, auto_heartbeat_timeout_secs) || ' seconds')::interval < NOW()
                    THEN 1 END) as pending_heartbeat
            FROM task_executions
            WHERE created_at > NOW() - INTERVAL '24 hours'
            "#;
        let mut out = TimeoutStatistics {
            schedule_to_start_timeouts: 0,
            start_to_close_timeouts: 0,
            heartbeat_timeouts: 0,
            total_timeouts: 0,
            pending_schedule_to_start: 0,
            pending_start_to_close: 0,
            pending_heartbeat: 0,
        };
        for pool in self.scan_pools() {
            let stats = sqlx::query_as::<_, TimeoutStatsRow>(sql)
                .fetch_one(pool)
                .await
                .map_err(|e| {
                    error!("Failed to fetch timeout statistics: {}", e);
                    OrcherError::internal(format!("Failed to fetch statistics: {}", e))
                })?;
            out.schedule_to_start_timeouts += stats.schedule_to_start_timeouts.unwrap_or(0) as u64;
            out.start_to_close_timeouts += stats.start_to_close_timeouts.unwrap_or(0) as u64;
            out.heartbeat_timeouts += stats.heartbeat_timeouts.unwrap_or(0) as u64;
            out.total_timeouts += stats.total_timeouts.unwrap_or(0) as u64;
            out.pending_schedule_to_start += stats.pending_schedule_to_start.unwrap_or(0) as u64;
            out.pending_start_to_close += stats.pending_start_to_close.unwrap_or(0) as u64;
            out.pending_heartbeat += stats.pending_heartbeat.unwrap_or(0) as u64;
        }
        Ok(out)
    }
}

// ================================================================================================
// TYPES
// ================================================================================================

/// Statistics about timeout detection
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimeoutStatistics {
    /// Number of schedule-to-start timeouts (last 24h)
    pub schedule_to_start_timeouts: u64,
    /// Number of start-to-close timeouts (last 24h)
    pub start_to_close_timeouts: u64,
    /// Number of heartbeat timeouts (last 24h)
    pub heartbeat_timeouts: u64,
    /// Total timeouts (last 24h)
    pub total_timeouts: u64,
    /// Currently pending schedule-to-start timeouts
    pub pending_schedule_to_start: u64,
    /// Currently pending start-to-close timeouts
    pub pending_start_to_close: u64,
    /// Currently pending heartbeat timeouts
    pub pending_heartbeat: u64,
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

/// Minimal projection of `workflow_executions` used for timeout detection.
#[derive(Debug, sqlx::FromRow)]
struct WorkflowExecutionTimeoutRow {
    id: uuid::Uuid,
    execution_id: String,
    #[allow(dead_code)]
    workflow_id: Option<String>,
    #[allow(dead_code)]
    task_queue: String,
    #[allow(dead_code)]
    namespace: String,
    #[allow(dead_code)]
    submitted_at: Timestamptz,
    timeout_secs: Option<i32>,
    run_timeout_secs: Option<i32>,
    retry_count: i32,
    max_retries: i32,
    retry_policy: Option<serde_json::Value>,
    /// When the workflow's execution timeout runs out, shared by every run of
    /// it (V067).
    execution_deadline: Option<Timestamptz>,
    /// The run is past its run timeout and not its execution timeout.
    run_deadline: bool,
}

#[derive(Debug, sqlx::FromRow)]
struct TimeoutStatsRow {
    schedule_to_start_timeouts: Option<i64>,
    start_to_close_timeouts: Option<i64>,
    heartbeat_timeouts: Option<i64>,
    total_timeouts: Option<i64>,
    pending_schedule_to_start: Option<i64>,
    pending_start_to_close: Option<i64>,
    pending_heartbeat: Option<i64>,
}

// ================================================================================================
// TESTS
// ================================================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// When a session's worker dies, its tasks are failed and their workflows
    /// woken. The wake-up matched on the wrong id — the task's execution_id
    /// instead of its workflow_id — so it updated nothing and the parents
    /// never saw the failure.
    /// A cancelled workflow that has not ended by its deadline is terminated;
    /// one still inside it, and one that already ended, are left alone.
    #[tokio::test]
    #[ignore] // Requires PostgreSQL
    async fn a_cancellation_past_its_deadline_is_terminated() {
        let url = std::env::var("TEST_DATABASE_URL").unwrap_or_else(|_| {
            "postgresql://postgres:postgres@localhost:5432/orcher_test".to_string()
        });
        let pool = PgPool::connect(&url)
            .await
            .expect("connect to the test database");
        let watcher = TimeoutWatcher::new(pool.clone(), TimeoutWatcherConfig::default());

        async fn seed(pool: &PgPool, status: &str, deadline_secs: i64) -> uuid::Uuid {
            let id = uuid::Uuid::new_v4();
            sqlx::query(
                r#"INSERT INTO workflow_executions
                     (id, execution_id, workflow_id, workflow_name, workflow_version, namespace,
                      task_queue, status, trigger_type, submitted_at, created_at, updated_at,
                      activation_seq, cancel_requested_at, cancel_deadline)
                   VALUES ($1, $2, $1, 'deadline-test', '1.0.0', 'default', 'deadline-q', $3,
                           'api', NOW(), NOW(), NOW(), 1, NOW(),
                           NOW() + make_interval(secs => $4))"#,
            )
            .bind(id)
            .bind(id.to_string())
            .bind(status)
            .bind(deadline_secs as f64)
            .execute(pool)
            .await
            .expect("seed workflow row");
            id
        }
        async fn status(pool: &PgPool, id: uuid::Uuid) -> String {
            sqlx::query_scalar("SELECT status FROM workflow_executions WHERE id = $1")
                .bind(id)
                .fetch_one(pool)
                .await
                .expect("read status")
        }

        let overdue = seed(&pool, "running", -5).await;
        let in_time = seed(&pool, "running", 3600).await;
        let finished = seed(&pool, "completed", -5).await;

        let terminated = watcher
            .terminate_past_cancel_deadline()
            .await
            .expect("sweep");

        assert!(terminated >= 1);
        assert_eq!(status(&pool, overdue).await, "terminated");
        assert_eq!(status(&pool, in_time).await, "running");
        assert_eq!(status(&pool, finished).await, "completed");

        for id in [overdue, in_time, finished] {
            let _ = sqlx::query("DELETE FROM workflow_executions WHERE id = $1")
                .bind(id)
                .execute(&pool)
                .await;
        }
    }

    /// A running, claimed workflow with one started task, `slow_0`, whose
    /// only attempt ran past its one-second start-to-close timeout an hour
    /// ago. Returns the workflow's row id.
    async fn seed_overdue_last_attempt(pool: &PgPool) -> Uuid {
        let workflow = Uuid::new_v4();
        let execution = Uuid::new_v4();
        sqlx::query(
            r#"INSERT INTO workflow_executions
                 (id, execution_id, workflow_name, workflow_version, namespace, task_queue,
                  status, parameters, context, environment, progress,
                  total_steps, completed_steps, failed_steps, retried_steps,
                  claimed_by, claimed_at, claim_timeout_at)
               VALUES ($1, $2, 'timeout-parent', '1.0.0', 'default', 'q', 'running',
                       '{}', '{}', '{}', '{}', 0, 0, 0, 0,
                       'worker', NOW(), NOW() + INTERVAL '5 minutes')"#,
        )
        .bind(workflow)
        .bind(execution.to_string())
        .execute(pool)
        .await
        .expect("seed workflow");
        let task =
            orcher_storage::repositories::domains::task_execution::TaskExecutionRepository::new(
                pool.clone(),
            )
            .create_scheduled(
                "slow_0".to_string(),
                "Slow".to_string(),
                orcher_common::types::identifiers::WorkflowId::from_uuid(workflow),
                orcher_common::types::identifiers::ExecutionId::from_uuid(execution),
                "q".to_string(),
                "default".to_string(),
                1,
                Some(br#"{"order":7}"#.to_vec()),
                Some(serde_json::json!({
                    "max_attempts": 1,
                    "initial_interval_seconds": 1.0,
                    "max_interval_seconds": 1.0,
                    "backoff_coefficient": 2.0
                })),
                None,
                Some(1),
                None,
                None,
                None,
            )
            .await
            .expect("seed the task");
        sqlx::query(
            "UPDATE task_executions SET status = 'started', started_at = NOW() - INTERVAL '1 hour' WHERE id = $1",
        )
        .bind(task.id.as_uuid())
        .execute(pool)
        .await
        .expect("start the task");
        workflow
    }

    async fn task_status(pool: &PgPool, workflow: Uuid) -> String {
        sqlx::query_scalar(
            "SELECT status FROM task_executions WHERE workflow_id = $1 AND task_id = 'slow_0'",
        )
        .bind(workflow)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    async fn tasks_failed(pool: &PgPool, workflow: Uuid) -> i64 {
        sqlx::query_scalar(
            "SELECT COUNT(*) FROM workflow_execution_journal WHERE workflow_id = $1 AND event_type = 'TaskFailed'",
        )
        .bind(workflow.to_string())
        .fetch_one(pool)
        .await
        .unwrap()
    }

    async fn claimed_by(pool: &PgPool, workflow: Uuid) -> Option<String> {
        sqlx::query_scalar("SELECT claimed_by FROM workflow_executions WHERE id = $1")
            .bind(workflow)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    async fn remove_workflow(pool: &PgPool, workflow: Uuid) {
        for sql in [
            "DELETE FROM execution_log WHERE workflow_id = $1",
            "DELETE FROM task_executions WHERE workflow_id = $1",
            "DELETE FROM workflow_executions WHERE id = $1",
        ] {
            let _ = sqlx::query(sql).bind(workflow).execute(pool).await;
        }
        let _ = sqlx::query("DELETE FROM workflow_execution_journal WHERE workflow_id = $1")
            .bind(workflow.to_string())
            .execute(pool)
            .await;
    }

    async fn test_pool() -> PgPool {
        let url = std::env::var("TEST_DATABASE_URL").unwrap_or_else(|_| {
            "postgresql://postgres:postgres@localhost:5432/orcher_test".to_string()
        });
        PgPool::connect(&url)
            .await
            .expect("connect to the test database")
    }

    /// A task that times out on its last attempt must reach its workflow:
    /// the failed step its replay reads, one TaskFailed, and the wake-up.
    /// Recording nothing but the timeout left the woken workflow waiting on
    /// the task forever. Driven through the sweep, so it covers the call.
    #[tokio::test]
    #[ignore] // Requires PostgreSQL
    async fn a_terminal_timeout_reaches_the_workflow() {
        let pool = test_pool().await;
        let watcher = TimeoutWatcher::new(pool.clone(), TimeoutWatcherConfig::default());
        let workflow = seed_overdue_last_attempt(&pool).await;

        watcher.check_timeouts().await.expect("sweep");

        assert_eq!(task_status(&pool, workflow).await, "timed_out");
        let step: (String, Option<String>, Option<serde_json::Value>) = sqlx::query_as(
            "SELECT status::text, error_message, input FROM execution_log WHERE workflow_id = $1 AND step_name = 'slow_0'",
        )
        .bind(workflow)
        .fetch_one(&pool)
        .await
        .expect("the failed step the workflow replays");
        assert_eq!(step.0, "failed");
        assert!(step.1.unwrap_or_default().contains("StartToClose"));
        assert_eq!(step.2, Some(serde_json::json!({"order": 7})));
        assert_eq!(tasks_failed(&pool, workflow).await, 1);
        assert_eq!(
            claimed_by(&pool, workflow).await,
            None,
            "the workflow was not woken"
        );

        // A second sweep finds nothing left to do.
        watcher.check_timeouts().await.expect("sweep");
        assert_eq!(tasks_failed(&pool, workflow).await, 1);

        remove_workflow(&pool, workflow).await;
    }

    /// A timeout whose recording fails is left for the next sweep. It used to
    /// be marked timed out regardless, so no sweep selected it again and the
    /// workflow waited forever.
    #[tokio::test]
    #[ignore] // Requires PostgreSQL
    async fn a_timeout_that_could_not_be_recorded_is_taken_again() {
        let pool = test_pool().await;
        let watcher = TimeoutWatcher::new(pool.clone(), TimeoutWatcherConfig::default());
        let workflow = seed_overdue_last_attempt(&pool).await;

        // Make every write of this workflow's step rows fail.
        let tag = workflow.simple().to_string();
        sqlx::query(&format!(
            "CREATE FUNCTION refuse_{tag}() RETURNS trigger AS $$ BEGIN \
             IF NEW.workflow_id = '{workflow}' THEN RAISE EXCEPTION 'refused'; END IF; \
             RETURN NEW; END $$ LANGUAGE plpgsql"
        ))
        .execute(&pool)
        .await
        .expect("create the refusal");
        sqlx::query(&format!(
            "CREATE TRIGGER refuse_{tag} BEFORE INSERT OR UPDATE ON execution_log \
             FOR EACH ROW EXECUTE FUNCTION refuse_{tag}()"
        ))
        .execute(&pool)
        .await
        .expect("install the refusal");

        watcher.check_timeouts().await.expect("sweep");
        assert_eq!(
            task_status(&pool, workflow).await,
            "started",
            "left for the next sweep"
        );
        assert_eq!(tasks_failed(&pool, workflow).await, 0);
        assert_eq!(claimed_by(&pool, workflow).await.as_deref(), Some("worker"));

        sqlx::query(&format!("DROP TRIGGER refuse_{tag} ON execution_log"))
            .execute(&pool)
            .await
            .expect("remove the refusal");
        sqlx::query(&format!("DROP FUNCTION refuse_{tag}()"))
            .execute(&pool)
            .await
            .expect("remove the refusal");

        watcher.check_timeouts().await.expect("sweep");
        assert_eq!(task_status(&pool, workflow).await, "timed_out");
        assert_eq!(tasks_failed(&pool, workflow).await, 1);
        assert_eq!(
            claimed_by(&pool, workflow).await,
            None,
            "the workflow was not woken"
        );

        remove_workflow(&pool, workflow).await;
    }

    /// A completion that raced the timeout is the step's outcome; the timeout
    /// must not replace it with a failure.
    #[tokio::test]
    #[ignore] // Requires PostgreSQL
    async fn a_timeout_never_replaces_a_completed_step() {
        let pool = test_pool().await;
        let watcher = TimeoutWatcher::new(pool.clone(), TimeoutWatcherConfig::default());
        let workflow = seed_overdue_last_attempt(&pool).await;
        sqlx::query(
            "INSERT INTO execution_log (workflow_id, step_name, status, result, completed_at) VALUES ($1, 'slow_0', 'completed', '\"done\"', NOW())",
        )
        .bind(workflow)
        .execute(&pool)
        .await
        .expect("seed the completed step");

        watcher.check_timeouts().await.expect("sweep");

        let status: String = sqlx::query_scalar(
            "SELECT status::text FROM execution_log WHERE workflow_id = $1 AND step_name = 'slow_0'",
        )
        .bind(workflow)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(status, "completed");

        remove_workflow(&pool, workflow).await;
    }

    /// A session's worker dies mid-task. Its running task times
    /// out with retries left, but a retry would wait on a queue only the dead
    /// worker polled: instead it fails for good, and so does the task still
    /// waiting on the session queue, each recorded where the workflow reads it,
    /// and the workflow is woken. Driven through the sweep itself.
    #[tokio::test]
    #[ignore] // Requires PostgreSQL
    async fn a_lost_sessions_tasks_fail_and_its_workflow_is_woken() {
        let pool = test_pool().await;
        let watcher = TimeoutWatcher::new(pool.clone(), TimeoutWatcherConfig::default());

        let workflow = Uuid::new_v4();
        let execution = Uuid::new_v4();
        sqlx::query(
            r#"INSERT INTO workflow_executions
                 (id, execution_id, workflow_name, workflow_version, namespace, task_queue,
                  status, parameters, context, environment, progress,
                  total_steps, completed_steps, failed_steps, retried_steps,
                  claimed_by, claimed_at, claim_timeout_at)
               VALUES ($1, $2, 'session-parent', '1.0.0', 'default', 'q', 'running',
                       '{}', '{}', '{}', '{}', 0, 0, 0, 0,
                       'worker', NOW(), NOW() + INTERVAL '5 minutes')"#,
        )
        .bind(workflow)
        .bind(execution.to_string())
        .execute(&pool)
        .await
        .expect("seed workflow");

        let session_queue = format!("q__session__{}", Uuid::new_v4());
        let tasks =
            orcher_storage::repositories::domains::task_execution::TaskExecutionRepository::new(
                pool.clone(),
            );
        let mut ids = Vec::new();
        for task_id in ["running_0", "waiting_1"] {
            let task = tasks
                .create_scheduled(
                    task_id.to_string(),
                    "SessionTask".to_string(),
                    orcher_common::types::identifiers::WorkflowId::from_uuid(workflow),
                    orcher_common::types::identifiers::ExecutionId::from_uuid(execution),
                    session_queue.clone(),
                    "default".to_string(),
                    1,
                    None,
                    // Retries left: a timeout here would normally be retried.
                    Some(serde_json::json!({
                        "max_attempts": 3,
                        "initial_interval_seconds": 1.0,
                        "max_interval_seconds": 1.0,
                        "backoff_coefficient": 2.0
                    })),
                    None,
                    Some(1),
                    None,
                    None,
                    None,
                )
                .await
                .expect("seed a task on the session queue");
            ids.push(task.id.as_uuid());
        }
        // The first is running, and its worker has not been heard from for an
        // hour; the second is still waiting on the session queue.
        sqlx::query(
            "UPDATE task_executions SET status = 'started', started_at = NOW() - INTERVAL '1 hour' WHERE id = $1",
        )
        .bind(ids[0])
        .execute(&pool)
        .await
        .expect("start the first task");

        watcher.check_timeouts().await.expect("sweep");

        let statuses: Vec<(String, String, i32)> = sqlx::query_as(
            "SELECT task_id, status, attempt_number FROM task_executions \
             WHERE workflow_id = $1 ORDER BY task_id, attempt_number",
        )
        .bind(workflow)
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(
            statuses,
            vec![
                ("running_0".to_string(), "timed_out".to_string(), 1),
                ("waiting_1".to_string(), "timed_out".to_string(), 1),
            ],
            "no retry on the lost session's queue, and its waiting task failed too"
        );
        assert_eq!(
            tasks_failed(&pool, workflow).await,
            2,
            "each failure is journaled for the workflow to read"
        );
        assert_eq!(
            claimed_by(&pool, workflow).await,
            None,
            "the workflow was not woken"
        );

        remove_workflow(&pool, workflow).await;
    }

    /// A task still scheduled when its run has ended is ended as cancelled;
    /// one of a run still going is left for it. Driven through
    /// the sweep.
    #[tokio::test]
    #[ignore] // Requires PostgreSQL
    async fn a_task_left_scheduled_by_an_ended_run_is_cancelled() {
        let pool = test_pool().await;
        let watcher = TimeoutWatcher::new(pool.clone(), TimeoutWatcherConfig::default());
        let tasks =
            orcher_storage::repositories::domains::task_execution::TaskExecutionRepository::new(
                pool.clone(),
            );
        let mut runs = Vec::new();
        for status in ["completed", "running"] {
            let workflow = Uuid::new_v4();
            let execution = Uuid::new_v4();
            sqlx::query(
                r#"INSERT INTO workflow_executions
                     (id, execution_id, workflow_name, workflow_version, namespace, task_queue,
                      status, parameters, context, environment, progress,
                      total_steps, completed_steps, failed_steps, retried_steps, completed_at)
                   VALUES ($1, $2, 'leftover-parent', '1.0.0', 'default', 'q', $3,
                           '{}', '{}', '{}', '{}', 0, 0, 0, 0,
                           CASE WHEN $3 = 'completed' THEN NOW() END)"#,
            )
            .bind(workflow)
            .bind(execution.to_string())
            .bind(status)
            .execute(&pool)
            .await
            .expect("seed workflow");
            tasks
                .create_scheduled(
                    "close_0".to_string(),
                    "__orcher_complete_session".to_string(),
                    orcher_common::types::identifiers::WorkflowId::from_uuid(workflow),
                    orcher_common::types::identifiers::ExecutionId::from_uuid(execution),
                    format!("q__session__{}", Uuid::new_v4()),
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
                .expect("seed the task");
            runs.push((status, workflow));
        }

        watcher.check_timeouts().await.expect("sweep");

        for (status, workflow) in &runs {
            let task: String =
                sqlx::query_scalar("SELECT status FROM task_executions WHERE workflow_id = $1")
                    .bind(workflow)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            let expected = if *status == "completed" {
                "cancelled"
            } else {
                "scheduled"
            };
            assert_eq!(task, expected, "the task of a {status} run");
        }
        for (_, workflow) in runs {
            remove_workflow(&pool, workflow).await;
        }
    }

    #[test]
    fn test_timeout_watcher_config_default() {
        let config = TimeoutWatcherConfig::default();
        assert_eq!(config.poll_interval_secs, 30);
        assert_eq!(config.batch_size, 100);
        assert!(config.enable_schedule_to_start);
        assert!(config.enable_start_to_close);
        assert!(config.enable_heartbeat);
        assert_eq!(config.grace_period_secs, 5);
    }

    #[test]
    fn test_timeout_watcher_config_custom() {
        let config = TimeoutWatcherConfig {
            poll_interval_secs: 60,
            batch_size: 50,
            enable_schedule_to_start: false,
            enable_start_to_close: true,
            enable_heartbeat: true,
            enable_workflow_timeout: false,
            grace_period_secs: 10,
        };

        assert_eq!(config.poll_interval_secs, 60);
        assert_eq!(config.batch_size, 50);
        assert!(!config.enable_schedule_to_start);
        assert!(config.enable_start_to_close);
        assert!(config.enable_heartbeat);
        assert_eq!(config.grace_period_secs, 10);
    }

    #[test]
    fn test_timeout_statistics_equality() {
        let stats1 = TimeoutStatistics {
            schedule_to_start_timeouts: 5,
            start_to_close_timeouts: 10,
            heartbeat_timeouts: 3,
            total_timeouts: 18,
            pending_schedule_to_start: 2,
            pending_start_to_close: 1,
            pending_heartbeat: 0,
        };

        let stats2 = TimeoutStatistics {
            schedule_to_start_timeouts: 5,
            start_to_close_timeouts: 10,
            heartbeat_timeouts: 3,
            total_timeouts: 18,
            pending_schedule_to_start: 2,
            pending_start_to_close: 1,
            pending_heartbeat: 0,
        };

        assert_eq!(stats1, stats2);
    }
}
