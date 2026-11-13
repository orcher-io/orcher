//! A cancellation request stops the work the workflow had queued, not only
//! the work already running.
//!
//! A running task learns of a request from its heartbeat, and stopped only if
//! it had started before the request. A task the workflow scheduled before the
//! request, but that no worker had picked up yet, started afterwards, was
//! taken for cleanup and ran to the end. Whether a task is interrupted is now
//! decided by when it was first scheduled: such a task is refused at its
//! start, ended as cancelled before it started with the failed step the
//! workflow replays, and its workflow woken. Interrupted work is never
//! retried. Work the workflow schedules after the request, its cleanup, runs.
//!
//! Requires PostgreSQL. Run with:
//!
//! ```bash
//! export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/orcher_test"
//! cargo test -p orcher-core --test cancel_queued_tasks -- --ignored
//! ```

use orcher_common::types::identifiers::{ExecutionId, TaskExecutionId, WorkflowId};
use orcher_core::engine::dispatch::PollingManager;
use orcher_core::engine::queues::outbox::OutboxQueue;
use orcher_core::engine::WorkflowCoordinator;
use orcher_core::events::EventBus;
use orcher_core::grpc::services::ExecutionService;
use orcher_core::retries::{QueuedTaskCancellation, TimeoutWatcher, TimeoutWatcherConfig};
use orcher_core::workflows::ChildWorkflowManager;
use orcher_core::workflows::WorkflowExecutionService;
use orcher_core::workflows::{CommandProcessor, JournalService};
use orcher_events::bus::EventBusConfig;
use orcher_proto::orcher::v1::execution_service_server::ExecutionService as ExecutionServiceTrait;
use orcher_proto::orcher::v1::{FailTaskExecutionRequest, Failure, RecordTaskHeartbeatRequest};
use orcher_storage::repositories::domains::{
    child_workflow::ChildWorkflowRepository, coordination::OutboxRepository,
    journal::JournalRepository, task_execution::TaskExecutionRepository,
    workflow_execution::WorkflowExecutionRepository,
};
use orcher_storage::repositories::{ExecutionLogRepository, PostgresExecutionLogRepository};
use orcher_storage::schema::types::TaskExecution;
use sqlx::PgPool;
use std::sync::Arc;
use tonic::Request;
use uuid::Uuid;

const NAMESPACE: &str = "default";
const TASK_QUEUE: &str = "cancel-queued";
const WORKER: &str = "worker-a";

/// Retries enough that only the cancellation can be what stops one.
fn retrying() -> serde_json::Value {
    serde_json::json!({
        "max_attempts": 5,
        "initial_interval_seconds": 1.0,
        "max_interval_seconds": 1.0,
        "backoff_coefficient": 2.0,
    })
}

struct Engine {
    pool: PgPool,
    tasks: TaskExecutionRepository,
    runs: Arc<WorkflowExecutionRepository>,
    workflows: Arc<WorkflowExecutionService>,
    service: ExecutionService,
}

impl Engine {
    async fn new() -> Self {
        let database_url = std::env::var("TEST_DATABASE_URL").unwrap_or_else(|_| {
            "postgresql://postgres:postgres@localhost:5432/orcher_test".to_string()
        });
        let pool = PgPool::connect(&database_url)
            .await
            .expect("connect to the test database");
        static MIGRATED: tokio::sync::OnceCell<()> = tokio::sync::OnceCell::const_new();
        MIGRATED
            .get_or_init(|| async {
                orcher_storage::setup::run_migrations(&pool)
                    .await
                    .expect("apply migrations");
            })
            .await;

        let journal_repo = Arc::new(JournalRepository::new(pool.clone()));
        let journal_service = Arc::new(JournalService::new(journal_repo.clone()));
        let child_repo = Arc::new(ChildWorkflowRepository::new(pool.clone()));
        let exec_repo = Arc::new(WorkflowExecutionRepository::new(pool.clone()));
        let outbox_repo = Arc::new(OutboxRepository::new(Arc::new(pool.clone())));
        let outbox_queue = Arc::new(OutboxQueue::new(outbox_repo));
        let event_bus = EventBus::new(EventBusConfig::default());
        let execution_log_repo = Arc::new(PostgresExecutionLogRepository::new(pool.clone()));
        let task_execution_repo = Arc::new(TaskExecutionRepository::new(pool.clone()));

        // Wired as the engine wires it: the request ends the queued tasks.
        let workflows = Arc::new(WorkflowExecutionService::new(
            exec_repo.clone(),
            journal_service.clone(),
            outbox_queue,
            event_bus.clone(),
            pool.clone(),
        ));
        workflows.set_queued_task_cancellation(QueuedTaskCancellation::new(
            task_execution_repo.clone(),
            execution_log_repo.clone(),
        ));
        let child_workflow_manager = Arc::new(ChildWorkflowManager::new(
            child_repo,
            journal_service.clone(),
            workflows.clone(),
        ));
        let coordinator = Arc::new(
            WorkflowCoordinator::new(
                Arc::new(pool.clone()),
                orcher_core::engine::CoordinatorConfig::default(),
            )
            .await
            .expect("create coordinator"),
        );
        let polling_manager = Arc::new(PollingManager::new(
            exec_repo.clone(),
            journal_repo.clone(),
            300,
        ));
        let command_processor = CommandProcessor::new(
            journal_service,
            child_workflow_manager,
            event_bus,
            execution_log_repo.clone(),
            task_execution_repo.clone(),
            exec_repo.clone(),
            Arc::new(pool.clone()),
        )
        .with_coordinator(coordinator)
        .with_polling_manager(polling_manager.clone());
        let service = ExecutionService::new(
            Arc::new(command_processor),
            polling_manager,
            execution_log_repo as Arc<dyn ExecutionLogRepository>,
            journal_repo,
            exec_repo.clone(),
            task_execution_repo,
            pool.clone(),
        );

        Self {
            tasks: TaskExecutionRepository::new(pool.clone()),
            pool,
            runs: exec_repo,
            workflows,
            service,
        }
    }

    /// A running workflow whose code has run, parked with its claim on a
    /// worker: what a cancellation request finds, and what a wake-up
    /// releases.
    async fn parked_workflow(&self) -> (Uuid, String) {
        let id = Uuid::new_v4();
        let execution_id = Uuid::new_v4().to_string();
        sqlx::query(
            r#"
            INSERT INTO workflow_executions (
                id, execution_id, workflow_name, workflow_version, namespace, task_queue,
                status, parameters, context, environment, progress,
                total_steps, completed_steps, failed_steps, retried_steps,
                claimed_by, claimed_at, claim_timeout_at,
                activation_seq, completed_activation
            )
            VALUES ($1, $2, 'cancel-queued', '1.0.0', $3, $4, 'running', '{}', '{}', '{}', '{}',
                    0, 0, 0, 0, $5, NOW(), NOW() + INTERVAL '5 minutes', 1, 1)
            "#,
        )
        .bind(id)
        .bind(&execution_id)
        .bind(NAMESPACE)
        .bind(TASK_QUEUE)
        .bind(WORKER)
        .execute(&self.pool)
        .await
        .expect("insert parked workflow");
        (id, execution_id)
    }

    /// The workflow schedules `task_id`, now.
    async fn schedule(
        &self,
        id: Uuid,
        execution_id: &str,
        task_id: &str,
        attempt: i32,
        start_to_close_secs: Option<i32>,
    ) -> TaskExecution {
        self.tasks
            .create_scheduled(
                task_id.to_string(),
                "TaskType".to_string(),
                WorkflowId::from_uuid(id),
                ExecutionId::from_uuid(Uuid::parse_str(execution_id).unwrap()),
                TASK_QUEUE.to_string(),
                NAMESPACE.to_string(),
                attempt,
                Some(br#"{"order":7}"#.to_vec()),
                Some(retrying()),
                None,
                start_to_close_secs,
                None,
                None,
                None,
            )
            .await
            .expect("schedule the task")
    }

    /// Start `task` as a poll does, returning the token its worker reports
    /// with, or `None` when the start is refused.
    async fn start(&self, task: &TaskExecution) -> Option<Vec<u8>> {
        let token =
            orcher_core::grpc::task_token::encode(&orcher_core::grpc::task_token::NewTaskToken {
                workflow_id: &task.workflow_id.to_string(),
                execution_id: &task.execution_id.to_string(),
                task_id: &task.task_id,
                attempt: task.attempt_number,
                started_at_ms: 0,
                organization_id: None,
            });
        self.tasks
            .mark_started_as(task.id, WORKER.to_string(), token.clone(), None, None)
            .await
            .ok()
            .map(|_| token.into_bytes())
    }

    /// Record the request only, as the storage does, without the request
    /// path's ending of queued tasks or its wake-up: what the start, the
    /// heartbeat, the retry decisions and the sweep each have to cope with
    /// on their own.
    async fn record_request(&self, execution_id: &str) {
        self.runs
            .request_cancel(execution_id, "customer changed their mind", None)
            .await
            .expect("record the request")
            .expect("a running workflow is asked");
    }

    /// Ask the workflow to cancel the way every caller does.
    async fn request(&self, execution_id: &str) {
        self.workflows
            .cancel_execution(execution_id, "customer changed their mind", None)
            .await
            .expect("request the cancellation");
    }

    async fn heartbeat(&self, token: &[u8]) -> bool {
        self.service
            .record_task_heartbeat(Request::new(RecordTaskHeartbeatRequest {
                task_token: token.to_vec(),
                namespace: NAMESPACE.to_string(),
                identity: WORKER.to_string(),
                ..Default::default()
            }))
            .await
            .expect("heartbeat")
            .into_inner()
            .cancel_requested
    }

    async fn status(&self, id: TaskExecutionId) -> String {
        sqlx::query_scalar("SELECT status FROM task_executions WHERE id = $1")
            .bind(id.as_uuid())
            .fetch_one(&self.pool)
            .await
            .expect("read the task")
    }

    async fn attempts(&self, id: Uuid, task_id: &str) -> i64 {
        sqlx::query_scalar(
            "SELECT count(*) FROM task_executions WHERE workflow_id = $1 AND task_id = $2",
        )
        .bind(id)
        .bind(task_id)
        .fetch_one(&self.pool)
        .await
        .expect("count attempts")
    }

    /// The step the workflow replays for `task_id`: its status and the
    /// failure type it records.
    async fn step(&self, id: Uuid, task_id: &str) -> Option<(String, Option<String>)> {
        sqlx::query_as(
            "SELECT status::text, error_details->>'failure_type' FROM execution_log \
             WHERE workflow_id = $1 AND step_name = $2",
        )
        .bind(id)
        .bind(task_id)
        .fetch_optional(&self.pool)
        .await
        .expect("read the step")
    }

    async fn tasks_failed(&self, id: Uuid, task_id: &str) -> i64 {
        sqlx::query_scalar(
            "SELECT count(*) FROM workflow_execution_journal \
             WHERE workflow_id = $1::text AND event_type = 'TaskFailed' AND payload->>'task_id' = $2",
        )
        .bind(id)
        .bind(task_id)
        .fetch_one(&self.pool)
        .await
        .expect("count TaskFailed")
    }

    async fn claimed_by(&self, id: Uuid) -> Option<String> {
        sqlx::query_scalar("SELECT claimed_by FROM workflow_executions WHERE id = $1")
            .bind(id)
            .fetch_one(&self.pool)
            .await
            .expect("read the claim")
    }

    /// Place every attempt of `task_id` a minute further back, as if the
    /// workflow had scheduled it well before anything that follows.
    async fn scheduled_a_minute_ago(&self, id: Uuid, task_id: &str) {
        sqlx::query(
            "UPDATE task_executions SET scheduled_at = scheduled_at - INTERVAL '1 minute' \
             WHERE workflow_id = $1 AND task_id = $2",
        )
        .bind(id)
        .bind(task_id)
        .execute(&self.pool)
        .await
        .expect("move the schedule back");
    }

    async fn cleanup(&self, id: Uuid) {
        for sql in [
            "DELETE FROM execution_log WHERE workflow_id = $1",
            "DELETE FROM task_executions WHERE workflow_id = $1",
            "DELETE FROM outbox_queue WHERE workflow_id = $1",
            "DELETE FROM workflow_executions WHERE id = $1",
        ] {
            let _ = sqlx::query(sql).bind(id).execute(&self.pool).await;
        }
        for sql in [
            "DELETE FROM workflow_execution_journal WHERE workflow_id = $1",
            "DELETE FROM workflow_execution_logs WHERE execution_id IN \
             (SELECT execution_id FROM workflow_executions WHERE id::text = $1)",
        ] {
            let _ = sqlx::query(sql)
                .bind(id.to_string())
                .execute(&self.pool)
                .await;
        }
    }
}

fn watcher(pool: &PgPool) -> TimeoutWatcher {
    TimeoutWatcher::new(
        pool.clone(),
        TimeoutWatcherConfig {
            grace_period_secs: 0,
            ..TimeoutWatcherConfig::default()
        },
    )
}

/// A task queued when the request came is not started afterwards, by a poll
/// or by the eager hand-out to a completing worker.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_task_queued_before_the_request_is_not_started() {
    let engine = Engine::new().await;
    let (id, execution_id) = engine.parked_workflow().await;
    let queued = engine
        .schedule(id, &execution_id, "charge_0", 1, None)
        .await;
    engine.scheduled_a_minute_ago(id, "charge_0").await;

    engine.record_request(&execution_id).await;

    assert!(
        engine.start(&queued).await.is_none(),
        "a poll started interrupted work"
    );
    let eager = engine
        .tasks
        .mark_started_eager(
            queued.id,
            WORKER.to_string(),
            "token".to_string(),
            orcher_common::types::time::Timestamptz::now(),
            None,
        )
        .await;
    assert!(
        eager.is_err(),
        "the eager hand-out started interrupted work"
    );
    assert_eq!(engine.status(queued.id).await, "scheduled");

    engine.cleanup(id).await;
}

/// Interrupted is decided by when the task was first scheduled: a retry
/// written after the request is still the work the request interrupted.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_retry_of_interrupted_work_is_not_started() {
    let engine = Engine::new().await;
    let (id, execution_id) = engine.parked_workflow().await;
    engine
        .schedule(id, &execution_id, "charge_0", 1, None)
        .await;
    engine.scheduled_a_minute_ago(id, "charge_0").await;

    engine.record_request(&execution_id).await;
    let retry = engine
        .schedule(id, &execution_id, "charge_0", 2, None)
        .await;

    assert!(
        engine.start(&retry).await.is_none(),
        "a retry of interrupted work was started"
    );

    engine.cleanup(id).await;
}

/// The request ends the task it interrupted as cancelled before it started,
/// and the workflow, woken by the request, replays the failure.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn the_request_ends_a_queued_task_and_tells_the_workflow() {
    let engine = Engine::new().await;
    let (id, execution_id) = engine.parked_workflow().await;
    let queued = engine
        .schedule(id, &execution_id, "charge_0", 1, None)
        .await;
    engine.scheduled_a_minute_ago(id, "charge_0").await;

    engine.request(&execution_id).await;

    assert_eq!(engine.status(queued.id).await, "cancelled");
    assert_eq!(
        engine.step(id, "charge_0").await,
        Some(("cancelled".to_string(), Some("Cancelled".to_string()))),
        "the workflow has no cancelled step to replay"
    );
    assert_eq!(engine.tasks_failed(id, "charge_0").await, 1);
    assert_eq!(
        engine.claimed_by(id).await,
        None,
        "the workflow was not woken"
    );
    assert_eq!(engine.attempts(id, "charge_0").await, 1, "it was retried");

    // Asking again ends nothing more.
    engine.request(&execution_id).await;
    assert_eq!(engine.tasks_failed(id, "charge_0").await, 1);

    engine.cleanup(id).await;
}

/// A queued task the request did not end — it failed, or the engine that
/// took the request has none of the repositories — is ended by the timeout
/// watcher's sweep, which wakes the workflow itself.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn the_sweep_ends_a_queued_task_the_request_missed() {
    let engine = Engine::new().await;
    let (id, execution_id) = engine.parked_workflow().await;
    let queued = engine
        .schedule(id, &execution_id, "charge_0", 1, None)
        .await;
    engine.scheduled_a_minute_ago(id, "charge_0").await;
    engine.record_request(&execution_id).await;
    assert_eq!(engine.claimed_by(id).await.as_deref(), Some(WORKER));

    let watcher = watcher(&engine.pool);
    watcher.check_timeouts().await.expect("sweep");

    assert_eq!(engine.status(queued.id).await, "cancelled");
    assert_eq!(
        engine.step(id, "charge_0").await.map(|s| s.0).as_deref(),
        Some("cancelled")
    );
    assert_eq!(engine.tasks_failed(id, "charge_0").await, 1);
    assert_eq!(
        engine.claimed_by(id).await,
        None,
        "the workflow was not woken"
    );

    // A second sweep finds nothing left to do.
    watcher.check_timeouts().await.expect("sweep");
    assert_eq!(engine.tasks_failed(id, "charge_0").await, 1);
    assert_eq!(engine.attempts(id, "charge_0").await, 1);

    engine.cleanup(id).await;
}

/// Cleanup is work the workflow schedules after the request: it starts, its
/// heartbeat does not stop it, and nothing ends it.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_task_scheduled_after_the_request_still_runs() {
    let engine = Engine::new().await;
    let (id, execution_id) = engine.parked_workflow().await;
    engine.request(&execution_id).await;

    let cleanup = engine
        .schedule(id, &execution_id, "refund_0", 1, None)
        .await;
    let token = engine
        .start(&cleanup)
        .await
        .expect("the workflow's cleanup was refused its start");
    assert!(
        !engine.heartbeat(&token).await,
        "the cleanup was told to stop"
    );
    watcher(&engine.pool).check_timeouts().await.expect("sweep");
    assert_eq!(engine.status(cleanup.id).await, "started");

    // And a cleanup still queued is not ended either.
    let queued_cleanup = engine
        .schedule(id, &execution_id, "notify_0", 1, None)
        .await;
    engine.request(&execution_id).await;
    watcher(&engine.pool).check_timeouts().await.expect("sweep");
    assert_eq!(engine.status(queued_cleanup.id).await, "scheduled");

    engine.cleanup(id).await;
}

/// The heartbeat stops a task scheduled before the request even when it
/// started after it: one an engine without the start's refusal handed out.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn the_heartbeat_stops_a_task_scheduled_before_the_request() {
    let engine = Engine::new().await;
    let (id, execution_id) = engine.parked_workflow().await;
    let queued = engine
        .schedule(id, &execution_id, "charge_0", 1, None)
        .await;
    engine.scheduled_a_minute_ago(id, "charge_0").await;
    let token = engine
        .start(&queued)
        .await
        .expect("start before the request");
    engine.record_request(&execution_id).await;
    // Started after the request, as far as the row says.
    sqlx::query(
        "UPDATE task_executions SET started_at = NOW() + INTERVAL '1 second' WHERE id = $1",
    )
    .bind(queued.id.as_uuid())
    .execute(&engine.pool)
    .await
    .expect("move the start");

    assert!(
        engine.heartbeat(&token).await,
        "interrupted work was not told to stop"
    );

    engine.cleanup(id).await;
}

/// Interrupted work that fails, as a task told to stop does, is not retried:
/// the workflow is given the failure at once.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn interrupted_work_that_fails_is_not_retried() {
    let engine = Engine::new().await;
    let (id, execution_id) = engine.parked_workflow().await;
    let running = engine
        .schedule(id, &execution_id, "charge_0", 1, None)
        .await;
    engine.scheduled_a_minute_ago(id, "charge_0").await;
    let token = engine
        .start(&running)
        .await
        .expect("start before the request");
    engine.record_request(&execution_id).await;

    engine
        .service
        .fail_task_execution(Request::new(FailTaskExecutionRequest {
            task_token: token,
            namespace: NAMESPACE.to_string(),
            identity: WORKER.to_string(),
            failure: Some(Failure {
                message: "stopped".to_string(),
                failure_type: "TaskCancelled".to_string(),
                ..Default::default()
            }),
        }))
        .await
        .expect("report the failure");

    assert_eq!(engine.attempts(id, "charge_0").await, 1, "it was retried");
    assert_eq!(
        engine.step(id, "charge_0").await.map(|s| s.0).as_deref(),
        Some("failed"),
        "the workflow has no failure to replay"
    );

    engine.cleanup(id).await;
}

/// Interrupted work that times out is not retried either.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn interrupted_work_that_times_out_is_not_retried() {
    let engine = Engine::new().await;
    let (id, execution_id) = engine.parked_workflow().await;
    let running = engine
        .schedule(id, &execution_id, "charge_0", 1, Some(1))
        .await;
    engine.scheduled_a_minute_ago(id, "charge_0").await;
    engine
        .start(&running)
        .await
        .expect("start before the request");
    sqlx::query("UPDATE task_executions SET started_at = NOW() - INTERVAL '1 hour' WHERE id = $1")
        .bind(running.id.as_uuid())
        .execute(&engine.pool)
        .await
        .expect("run it past its timeout");
    engine.record_request(&execution_id).await;

    watcher(&engine.pool).check_timeouts().await.expect("sweep");

    assert_eq!(engine.status(running.id).await, "timed_out");
    assert_eq!(engine.attempts(id, "charge_0").await, 1, "it was retried");
    assert_eq!(engine.tasks_failed(id, "charge_0").await, 1);
    assert_eq!(
        engine.step(id, "charge_0").await.map(|s| s.0).as_deref(),
        Some("failed"),
        "the workflow has no failure to replay"
    );

    engine.cleanup(id).await;
}
