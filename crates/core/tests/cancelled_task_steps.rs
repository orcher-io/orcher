//! A task that ends cancelled reaches its workflow.
//!
//! A worker that stops a task reports it with `cancel_task_execution`. The
//! cancellation was recorded, and the workflow woken, but the step it wrote
//! was dropped from the replay, so the woken workflow found its task still
//! outstanding and waited on it for good. A cancelled step now
//! replays as a failure of type `Cancelled`, never retried — the same type a
//! task its workflow's cancellation ended before it started replays with — so
//! a runtime has one value to map to its cancellation error.
//!
//! Cancelling a task that had not started was refused by the database: every
//! status past `scheduled` needs a start time, and the cancellation set none.
//!
//! Everything a worker does here goes through a real gRPC server and client.
//!
//! Requires PostgreSQL. Run with:
//!
//! ```bash
//! export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/orcher_test"
//! cargo test -p orcher-core --test cancelled_task_steps -- --ignored --test-threads=1
//! ```

use std::sync::Arc;

use orcher_common::types::identifiers::{ExecutionId, WorkflowId};
use orcher_core::engine::dispatch::{PollingManager, TaskPollingManager};
use orcher_core::engine::queues::outbox::OutboxQueue;
use orcher_core::engine::WorkflowCoordinator;
use orcher_core::events::EventBus;
use orcher_core::grpc::services::ExecutionService;
use orcher_core::retries::{TimeoutWatcher, TimeoutWatcherConfig};
use orcher_core::workflows::{
    ChildWorkflowManager, CommandProcessor, JournalService, WorkflowExecutionService,
};
use orcher_events::bus::EventBusConfig;
use orcher_proto::orcher::v1::execution_service_client::ExecutionServiceClient;
use orcher_proto::orcher::v1::execution_service_server::ExecutionServiceServer;
use orcher_proto::orcher::v1::journal_entry::Attributes;
use orcher_proto::orcher::v1::{
    command, CancelTaskExecutionRequest, Command, CommandType, CompleteTaskExecutionRequest,
    CompleteWorkflowExecutionRequest, PollTaskExecutionRequest, PollWorkflowExecutionRequest,
    PollWorkflowExecutionResponse, ScheduleTaskCommandAttributes, StepCompletedEventAttributes,
};
use orcher_storage::repositories::domains::{
    child_workflow::ChildWorkflowRepository, coordination::OutboxRepository,
    journal::JournalRepository, task_execution::TaskExecutionRepository,
    workflow_execution::WorkflowExecutionRepository,
};
use orcher_storage::repositories::{ExecutionLogRepository, PostgresExecutionLogRepository};
use orcher_storage::schema::types::TaskExecution;
use sqlx::PgPool;
use tonic::transport::Channel;
use tonic::Request;
use uuid::Uuid;

const NAMESPACE: &str = "default";
const WORKER: &str = "cancel-worker";
const CLAIM_TIMEOUT_SECS: i64 = 300;

async fn pool() -> PgPool {
    let url = std::env::var("TEST_DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:postgres@localhost:5432/orcher_test".to_string()
    });
    let pool = PgPool::connect(&url)
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
    pool
}

/// An engine serving its execution service over gRPC, and a worker's client
/// to it.
async fn engine(pool: &PgPool) -> ExecutionServiceClient<Channel> {
    let journal_repo = Arc::new(JournalRepository::new(pool.clone()));
    let journal_service = Arc::new(JournalService::new(journal_repo.clone()));
    let exec_repo = Arc::new(WorkflowExecutionRepository::new(pool.clone()));
    let task_execution_repo = Arc::new(TaskExecutionRepository::new(pool.clone()));
    let outbox_queue = Arc::new(OutboxQueue::new(Arc::new(OutboxRepository::new(Arc::new(
        pool.clone(),
    )))));
    let event_bus = EventBus::new(EventBusConfig::default());
    let execution_log_repo = Arc::new(PostgresExecutionLogRepository::new(pool.clone()));
    let child_workflow_manager = Arc::new(ChildWorkflowManager::new(
        Arc::new(ChildWorkflowRepository::new(pool.clone())),
        journal_service.clone(),
        Arc::new(WorkflowExecutionService::new(
            exec_repo.clone(),
            journal_service.clone(),
            outbox_queue.clone(),
            event_bus.clone(),
            pool.clone(),
        )),
    ));
    let polling_manager = Arc::new(PollingManager::new(
        exec_repo.clone(),
        journal_repo.clone(),
        CLAIM_TIMEOUT_SECS,
    ));
    let coordinator = Arc::new(
        WorkflowCoordinator::new(
            Arc::new(pool.clone()),
            orcher_core::engine::CoordinatorConfig::default(),
        )
        .await
        .expect("create coordinator"),
    );
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
        exec_repo,
        task_execution_repo,
        pool.clone(),
    )
    .with_task_polling_manager(Arc::new(TaskPollingManager::new(outbox_queue)));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a local port");
    let address = listener.local_addr().unwrap();
    let incoming = tonic::transport::server::TcpIncoming::from_listener(listener, true, None)
        .expect("incoming");
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(ExecutionServiceServer::new(service))
            .serve_with_incoming(incoming)
            .await
            .expect("engine server");
    });

    let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
        .unwrap()
        .connect()
        .await
        .expect("connect to the engine");
    ExecutionServiceClient::new(channel)
}

/// A running workflow, unclaimed, on a queue of its own.
async fn workflow(pool: &PgPool) -> (Uuid, String, String) {
    let id = Uuid::new_v4();
    let execution_id = Uuid::new_v4().to_string();
    let queue = format!("cancelled-steps-{}", id.simple());
    sqlx::query(
        r#"
        INSERT INTO workflow_executions (
            id, execution_id, workflow_id, workflow_name, workflow_version, namespace,
            task_queue, status, parameters, context, environment, progress,
            total_steps, completed_steps, failed_steps, retried_steps
        )
        VALUES ($1, $2, $1, 'cancelled-steps', '1.0.0', $3, $4, 'running',
                '{}', '{}', '{}', '{}', 0, 0, 0, 0)
        "#,
    )
    .bind(id)
    .bind(&execution_id)
    .bind(NAMESPACE)
    .bind(&queue)
    .execute(pool)
    .await
    .expect("insert workflow");
    (id, execution_id, queue)
}

async fn poll_workflow(
    client: &mut ExecutionServiceClient<Channel>,
    queue: &str,
    wait_secs: i64,
) -> PollWorkflowExecutionResponse {
    client
        .poll_workflow_execution(Request::new(PollWorkflowExecutionRequest {
            task_queue: queue.to_string(),
            namespace: NAMESPACE.to_string(),
            identity: WORKER.to_string(),
            poll_timeout: Some(orcher_proto::prost_types::Duration {
                seconds: wait_secs,
                nanos: 0,
            }),
            ..Default::default()
        }))
        .await
        .expect("poll for the workflow")
        .into_inner()
}

fn schedule(task_id: &str, queue: &str) -> Command {
    Command {
        command_type: CommandType::ScheduleTask as i32,
        attributes: Some(command::Attributes::ScheduleTask(
            ScheduleTaskCommandAttributes {
                task_id: task_id.to_string(),
                task_type: "Charge".to_string(),
                task_queue: queue.to_string(),
                input: b"null".to_vec(),
                ..Default::default()
            },
        )),
    }
}

/// Run the workflow's first activation, which schedules `task_id`, and take
/// the task as its worker does. Returns the token the worker reports with.
async fn started_task(
    client: &mut ExecutionServiceClient<Channel>,
    queue: &str,
    task_id: &str,
) -> Vec<u8> {
    let first = poll_workflow(client, queue, 10).await;
    assert!(
        !first.execution_id.is_empty(),
        "the workflow was not handed out"
    );
    let eager = client
        .complete_workflow_execution(Request::new(CompleteWorkflowExecutionRequest {
            workflow_id: first.workflow_id.clone(),
            execution_id: first.execution_id.clone(),
            namespace: NAMESPACE.to_string(),
            identity: WORKER.to_string(),
            stream_entry_id: first.stream_entry_id.clone(),
            commands: vec![schedule(task_id, queue)],
            ..Default::default()
        }))
        .await
        .expect("complete the activation")
        .into_inner()
        .eager_tasks;
    if let Some(task) = eager.into_iter().next() {
        return task.task_token;
    }
    let task = client
        .poll_task_execution(Request::new(PollTaskExecutionRequest {
            task_queue: queue.to_string(),
            namespace: NAMESPACE.to_string(),
            identity: WORKER.to_string(),
            poll_timeout: Some(orcher_proto::prost_types::Duration {
                seconds: 5,
                nanos: 0,
            }),
            ..Default::default()
        }))
        .await
        .expect("poll for the task")
        .into_inner();
    assert!(!task.task_token.is_empty(), "the task was not handed out");
    task.task_token
}

async fn cancel(client: &mut ExecutionServiceClient<Channel>, token: Vec<u8>) {
    client
        .cancel_task_execution(Request::new(CancelTaskExecutionRequest {
            task_token: token,
            namespace: NAMESPACE.to_string(),
            identity: WORKER.to_string(),
            ..Default::default()
        }))
        .await
        .expect("report the cancellation");
}

/// The StepCompleted entries an activation carries, which is all a worker
/// reads a task's outcome from.
fn steps(activation: &PollWorkflowExecutionResponse) -> Vec<&StepCompletedEventAttributes> {
    activation
        .journal
        .iter()
        .filter_map(|entry| match &entry.attributes {
            Some(Attributes::StepCompleted(step)) => Some(step),
            _ => None,
        })
        .collect()
}

async fn task_statuses(pool: &PgPool, id: Uuid, task_id: &str) -> Vec<String> {
    sqlx::query_scalar(
        "SELECT status FROM task_executions WHERE workflow_id = $1 AND task_id = $2 \
         ORDER BY attempt_number",
    )
    .bind(id)
    .bind(task_id)
    .fetch_all(pool)
    .await
    .expect("read the task")
}

/// The step the workflow replays for `task_id`: its status, if it has one.
async fn step_status(pool: &PgPool, id: Uuid, task_id: &str) -> Option<String> {
    sqlx::query_scalar(
        "SELECT status::text FROM execution_log WHERE workflow_id = $1 AND step_name = $2",
    )
    .bind(id)
    .bind(task_id)
    .fetch_optional(pool)
    .await
    .expect("read the step")
}

async fn cleanup(pool: &PgPool, id: Uuid) {
    for sql in [
        "DELETE FROM execution_log WHERE workflow_id = $1",
        "DELETE FROM task_executions WHERE workflow_id = $1",
        "DELETE FROM outbox_queue WHERE workflow_id = $1",
        "DELETE FROM workflow_executions WHERE id = $1",
    ] {
        let _ = sqlx::query(sql).bind(id).execute(pool).await;
    }
    let _ = sqlx::query("DELETE FROM workflow_execution_journal WHERE workflow_id = $1")
        .bind(id.to_string())
        .execute(pool)
        .await;
}

/// A task its worker cancelled wakes the workflow, which replays the step as
/// a never-retried failure of type `Cancelled` and can go on. The step used
/// to be dropped, leaving the woken workflow waiting on it for good.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_task_its_worker_cancelled_replays_as_a_cancellation() {
    let pool = pool().await;
    let mut client = engine(&pool).await;
    let (id, _, queue) = workflow(&pool).await;

    let token = started_task(&mut client, &queue, "charge_0").await;
    cancel(&mut client, token).await;

    assert_eq!(task_statuses(&pool, id, "charge_0").await, ["cancelled"]);
    let activation = poll_workflow(&mut client, &queue, 5).await;
    assert!(
        !activation.execution_id.is_empty(),
        "the workflow was not woken by its task's cancellation"
    );
    let delivered = steps(&activation);
    assert_eq!(
        delivered.len(),
        1,
        "the cancelled task's outcome is not in the activation"
    );
    assert_eq!(delivered[0].step_name, "charge_0");
    let failure = delivered[0]
        .failure
        .as_ref()
        .expect("the workflow did not hear that its task was cancelled");
    assert_eq!(failure.failure_type, "Cancelled");
    assert!(failure.non_retryable, "a cancellation replays as retryable");
    assert_eq!(failure.message, "Task cancelled by its worker");
    assert_eq!(
        task_statuses(&pool, id, "charge_0").await,
        ["cancelled"],
        "a cancelled task was retried"
    );

    cleanup(&pool, id).await;
}

/// A task its workflow's cancellation ended before it started replays the
/// same way, so both reach a runtime as the one cancellation type.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_task_cancelled_before_it_started_replays_the_same_way() {
    let pool = pool().await;
    let mut client = engine(&pool).await;
    let (id, execution_id, queue) = workflow(&pool).await;
    let tasks = TaskExecutionRepository::new(pool.clone());
    tasks
        .create_scheduled(
            "charge_0".to_string(),
            "Charge".to_string(),
            WorkflowId::from_uuid(id),
            ExecutionId::from_uuid(Uuid::parse_str(&execution_id).unwrap()),
            queue.clone(),
            NAMESPACE.to_string(),
            1,
            Some(b"null".to_vec()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("schedule the task");
    sqlx::query(
        "UPDATE task_executions SET scheduled_at = scheduled_at - INTERVAL '1 minute' \
         WHERE workflow_id = $1",
    )
    .bind(id)
    .execute(&pool)
    .await
    .expect("move the schedule back");
    // Its code has run once, scheduling the task: what a request applies to.
    sqlx::query(
        "UPDATE workflow_executions SET activation_seq = 1, completed_activation = 1 \
         WHERE id = $1",
    )
    .bind(id)
    .execute(&pool)
    .await
    .expect("record the first activation");
    WorkflowExecutionRepository::new(pool.clone())
        .request_cancel(&execution_id, "customer changed their mind", None)
        .await
        .expect("record the request")
        .expect("a running workflow is asked");
    TimeoutWatcher::new(
        pool.clone(),
        TimeoutWatcherConfig {
            grace_period_secs: 0,
            ..TimeoutWatcherConfig::default()
        },
    )
    .check_timeouts()
    .await
    .expect("sweep");

    let activation = poll_workflow(&mut client, &queue, 5).await;
    let delivered = steps(&activation);
    assert_eq!(delivered.len(), 1, "the step is not in the activation");
    let failure = delivered[0]
        .failure
        .as_ref()
        .expect("the workflow did not hear that its task was cancelled");
    assert_eq!(failure.failure_type, "Cancelled");
    assert!(failure.non_retryable);
    assert!(
        failure.message.contains("before it started"),
        "{}",
        failure.message
    );

    cleanup(&pool, id).await;
}

/// A cancellation reported for a task that already completed leaves the
/// completion as the step's outcome.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_cancellation_after_the_task_completed_changes_nothing() {
    let pool = pool().await;
    let mut client = engine(&pool).await;
    let (id, _, queue) = workflow(&pool).await;

    let token = started_task(&mut client, &queue, "charge_0").await;
    client
        .complete_task_execution(Request::new(CompleteTaskExecutionRequest {
            task_token: token.clone(),
            namespace: NAMESPACE.to_string(),
            identity: WORKER.to_string(),
            result: br#""charged""#.to_vec(),
        }))
        .await
        .expect("complete the task");
    cancel(&mut client, token).await;

    assert_eq!(task_statuses(&pool, id, "charge_0").await, ["completed"]);
    assert_eq!(
        step_status(&pool, id, "charge_0").await.as_deref(),
        Some("completed")
    );
    let activation = poll_workflow(&mut client, &queue, 5).await;
    let delivered = steps(&activation);
    assert_eq!(delivered.len(), 1);
    assert!(delivered[0].failure.is_none(), "{:?}", delivered[0].failure);
    assert_eq!(delivered[0].result, br#""charged""#.to_vec());

    cleanup(&pool, id).await;
}

/// A late cancellation for an attempt that failed, and is being retried,
/// writes no step: a cancelled step replays as a failure, so one written now
/// would end the step under the retry still running.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_cancellation_of_a_failed_attempt_leaves_its_retry_alone() {
    let pool = pool().await;
    let mut client = engine(&pool).await;
    let (id, execution_id, queue) = workflow(&pool).await;
    let tasks = TaskExecutionRepository::new(pool.clone());
    let attempt = |n: i32| {
        tasks.create_scheduled(
            "charge_0".to_string(),
            "Charge".to_string(),
            WorkflowId::from_uuid(id),
            ExecutionId::from_uuid(Uuid::parse_str(&execution_id).unwrap()),
            queue.clone(),
            NAMESPACE.to_string(),
            n,
            Some(b"null".to_vec()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
    };
    let first: TaskExecution = attempt(1).await.expect("schedule the first attempt");
    let token =
        orcher_core::grpc::task_token::encode(&orcher_core::grpc::task_token::NewTaskToken {
            workflow_id: &first.workflow_id.to_string(),
            execution_id: &first.execution_id.to_string(),
            task_id: &first.task_id,
            attempt: 1,
            started_at_ms: 0,
            organization_id: None,
        });
    tasks
        .mark_started_as(first.id, WORKER.to_string(), token.clone(), None, None)
        .await
        .expect("start the first attempt");
    tasks
        .mark_failed(first.id, "declined".to_string(), None, None)
        .await
        .expect("fail the first attempt");
    attempt(2).await.expect("schedule the retry");

    cancel(&mut client, token.into_bytes()).await;

    assert_eq!(
        task_statuses(&pool, id, "charge_0").await,
        ["failed", "scheduled"]
    );
    assert_eq!(
        step_status(&pool, id, "charge_0").await,
        None,
        "the retry's step was ended by a cancellation of the attempt before it"
    );

    cleanup(&pool, id).await;
}

/// A task that had not started can be cancelled, and failed: either takes
/// the moment it ended as its start, which every status past `scheduled`
/// needs. Both were refused by the database.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_scheduled_task_can_be_cancelled_or_failed() {
    let pool = pool().await;
    let (id, execution_id, queue) = workflow(&pool).await;
    let tasks = TaskExecutionRepository::new(pool.clone());
    let scheduled = |task_id: &'static str| {
        tasks.create_scheduled(
            task_id.to_string(),
            "Charge".to_string(),
            WorkflowId::from_uuid(id),
            ExecutionId::from_uuid(Uuid::parse_str(&execution_id).unwrap()),
            queue.clone(),
            NAMESPACE.to_string(),
            1,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
    };

    let cancelled = tasks
        .mark_cancelled(scheduled("a_0").await.expect("schedule").id)
        .await
        .expect("cancel a scheduled task");
    assert_eq!(cancelled.status, "cancelled");
    assert!(cancelled.started_at.is_some());

    let mut tx = pool.begin().await.expect("begin");
    let cancelled_on =
        TaskExecutionRepository::mark_cancelled_on(&mut tx, scheduled("b_0").await.unwrap().id)
            .await
            .expect("cancel a scheduled task in a transaction")
            .expect("the task was scheduled");
    tx.commit().await.expect("commit");
    assert_eq!(cancelled_on.status, "cancelled");
    assert!(cancelled_on.started_at.is_some());

    let failed = tasks
        .mark_failed(
            scheduled("c_0").await.unwrap().id,
            "refused".to_string(),
            None,
            None,
        )
        .await
        .expect("fail a scheduled task");
    assert_eq!(failed.status, "failed");
    assert!(failed.started_at.is_some());

    let mut tx = pool.begin().await.expect("begin");
    let failed_on = TaskExecutionRepository::mark_failed_on(
        &mut tx,
        scheduled("d_0").await.unwrap().id,
        "refused",
        None,
        None,
    )
    .await
    .expect("fail a scheduled task in a transaction")
    .expect("the task was scheduled");
    tx.commit().await.expect("commit");
    assert_eq!(failed_on.status, "failed");
    assert!(failed_on.started_at.is_some());

    // A task that had started keeps the start it had.
    let started = scheduled("e_0").await.unwrap();
    let started = tasks
        .mark_started_as(started.id, WORKER.to_string(), "t".to_string(), None, None)
        .await
        .expect("start");
    let cancelled = tasks.mark_cancelled(started.id).await.expect("cancel");
    assert_eq!(cancelled.started_at, started.started_at);

    cleanup(&pool, id).await;
}
