//! Recording a task's failure or cancellation all at once.
//!
//! A failure used to be written step by step — the task marked failed, the
//! retry row or dead-letter mark, the row the workflow replays to observe a
//! final failure, the TaskFailed journal entry — each failure only logged,
//! and the RPC reported success regardless. A task marked failed whose replay
//! row was then lost left its workflow waiting forever. Cancellation wrote a
//! status the table does not allow, so it never succeeded at all.
//!
//! Requires PostgreSQL. Run with:
//!
//! ```bash
//! export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/orcher_test"
//! cargo test -p orcher-core --test task_outcome -- --ignored
//! ```

use orcher_common::types::identifiers::{ExecutionId, WorkflowId};
use orcher_core::engine::dispatch::PollingManager;
use orcher_core::engine::queues::outbox::OutboxQueue;
use orcher_core::engine::WorkflowCoordinator;
use orcher_core::events::EventBus;
use orcher_core::grpc::services::ExecutionService;
use orcher_core::workflows::ChildWorkflowManager;
use orcher_core::workflows::WorkflowExecutionService;
use orcher_core::workflows::{CommandProcessor, JournalService};
use orcher_events::bus::EventBusConfig;
use orcher_proto::orcher::v1::execution_service_server::ExecutionService as ExecutionServiceTrait;
use orcher_proto::orcher::v1::{CancelTaskExecutionRequest, FailTaskExecutionRequest, Failure};
use orcher_storage::repositories::domains::{
    child_workflow::ChildWorkflowRepository, coordination::OutboxRepository,
    journal::JournalRepository, task_execution::TaskExecutionRepository,
    workflow_execution::WorkflowExecutionRepository,
};
use orcher_storage::repositories::{ExecutionLogRepository, PostgresExecutionLogRepository};
use sqlx::PgPool;
use std::sync::Arc;
use tonic::Request;
use uuid::Uuid;

const NAMESPACE: &str = "default";
const TASK_QUEUE: &str = "default";
const WORKER: &str = "worker-a";

struct Engine {
    pool: PgPool,
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
        // Tests in this binary run in parallel and each builds its own engine;
        // the migration runner is not safe to race against itself, so apply
        // the schema once per process.
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

        let workflow_execution_service = Arc::new(WorkflowExecutionService::new(
            exec_repo.clone(),
            journal_service.clone(),
            outbox_queue,
            event_bus.clone(),
            pool.clone(),
        ));
        let child_workflow_manager = Arc::new(ChildWorkflowManager::new(
            child_repo,
            journal_service.clone(),
            workflow_execution_service,
        ));
        let coordinator = Arc::new(
            WorkflowCoordinator::new(
                Arc::new(pool.clone()),
                orcher_core::engine::CoordinatorConfig::default(),
            )
            .await
            .expect("create coordinator"),
        );

        // The polling manager is what the in-workflow SendEvent handler uses to
        // release the target's claim and wake pollers; the fix under test
        // routes through it, so it has to be wired exactly as in production.
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
            exec_repo,
            task_execution_repo,
            pool.clone(),
        );

        Self { pool, service }
    }

    /// A running execution that a worker has claimed — the state every
    /// activation starts from.
    async fn claimed_workflow(&self, name: &str) -> (Uuid, String) {
        let id = Uuid::new_v4();
        let execution_id = Uuid::new_v4().to_string();
        sqlx::query(
            r#"
            INSERT INTO workflow_executions (
                id, execution_id, workflow_name, workflow_version, namespace, task_queue,
                status, parameters, context, environment, progress,
                total_steps, completed_steps, failed_steps, retried_steps,
                claimed_by, claimed_at, claim_timeout_at
            )
            VALUES ($1, $2, $3, '1.0.0', $4, $5, 'running', '{}', '{}', '{}', '{}',
                    0, 0, 0, 0, $6, NOW(), NOW() + INTERVAL '5 minutes')
            "#,
        )
        .bind(id)
        .bind(&execution_id)
        .bind(name)
        .bind(NAMESPACE)
        .bind(TASK_QUEUE)
        .bind(WORKER)
        .execute(&self.pool)
        .await
        .expect("insert claimed workflow");
        (id, execution_id)
    }

    async fn cleanup(&self, ids: &[Uuid]) {
        for id in ids {
            let _ =
                sqlx::query("DELETE FROM child_workflow_executions WHERE parent_workflow_id = $1")
                    .bind(id.to_string())
                    .execute(&self.pool)
                    .await;
            let _ = sqlx::query("DELETE FROM workflow_execution_journal WHERE workflow_id = $1")
                .bind(id.to_string())
                .execute(&self.pool)
                .await;
            let _ = sqlx::query("DELETE FROM workflow_executions WHERE id = $1")
                .bind(id)
                .execute(&self.pool)
                .await;
        }
    }
}

/// A started task, with the token its worker would report with.
async fn started_task(engine: &Engine, id: Uuid, execution_id: &str, task_id: &str) -> Vec<u8> {
    started_task_with_policy(engine, id, execution_id, task_id, None).await
}

async fn started_task_with_policy(
    engine: &Engine,
    id: Uuid,
    execution_id: &str,
    task_id: &str,
    retry_policy: Option<serde_json::Value>,
) -> Vec<u8> {
    let repo = TaskExecutionRepository::new(engine.pool.clone());
    let task = repo
        .create_scheduled(
            task_id.to_string(),
            "TaskType".to_string(),
            WorkflowId::from_uuid(id),
            ExecutionId::from_uuid(Uuid::parse_str(execution_id).unwrap()),
            TASK_QUEUE.to_string(),
            NAMESPACE.to_string(),
            1,
            None,
            retry_policy,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("schedule the task");
    let token =
        orcher_core::grpc::task_token::encode(&orcher_core::grpc::task_token::NewTaskToken {
            workflow_id: &id.to_string(),
            execution_id,
            task_id,
            attempt: 1,
            started_at_ms: 0,
            organization_id: None,
        });
    repo.mark_started(task.id, WORKER.to_string(), token.clone())
        .await
        .expect("start the task");
    token.into_bytes()
}

async fn count(pool: &PgPool, sql: &str, id: Uuid, task_id: &str) -> i64 {
    sqlx::query_scalar(sql)
        .bind(id)
        .bind(task_id)
        .fetch_one(pool)
        .await
        .expect(sql)
}

const TASKS: &str = "SELECT count(*) FROM task_executions WHERE workflow_id = $1 AND task_id = $2";
const FAILED: &str =
    "SELECT count(*) FROM task_executions WHERE workflow_id = $1 AND task_id = $2 AND status = 'failed'";
const CANCELLED: &str =
    "SELECT count(*) FROM task_executions WHERE workflow_id = $1 AND task_id = $2 AND status = 'cancelled'";
const REPLAY_ROWS: &str =
    "SELECT count(*) FROM execution_log WHERE workflow_id = $1 AND step_name = $2";
const TASK_FAILED: &str = "SELECT count(*) FROM workflow_execution_journal \
     WHERE workflow_id = $1::text AND event_type = 'TaskFailed' AND payload->>'task_id' = $2";
const TASK_CANCELED: &str = "SELECT count(*) FROM workflow_execution_journal \
     WHERE workflow_id = $1::text AND event_type = 'TaskCanceled' AND payload->>'task_id' = $2";

fn fail_request(token: &[u8]) -> Request<FailTaskExecutionRequest> {
    fail_request_of_type(token, "TaskExecutionError")
}

fn fail_request_of_type(token: &[u8], failure_type: &str) -> Request<FailTaskExecutionRequest> {
    fail_request_marked(token, failure_type, false)
}

fn fail_request_marked(
    token: &[u8],
    failure_type: &str,
    non_retryable: bool,
) -> Request<FailTaskExecutionRequest> {
    Request::new(FailTaskExecutionRequest {
        task_token: token.to_vec(),
        namespace: NAMESPACE.to_string(),
        identity: WORKER.to_string(),
        failure: Some(Failure {
            message: "boom".to_string(),
            failure_type: failure_type.to_string(),
            non_retryable,
            ..Default::default()
        }),
        ..Default::default()
    })
}

#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_task_failure_is_recorded_whole_and_once() {
    let engine = Engine::new().await;
    let (id, execution_id) = engine.claimed_workflow("task-failure").await;
    let task_id = "flaky_0";
    let token = started_task(&engine, id, &execution_id, task_id).await;

    engine
        .service
        .fail_task_execution(fail_request(&token))
        .await
        .expect("report the failure");

    assert_eq!(
        count(&engine.pool, FAILED, id, task_id).await,
        1,
        "the task is failed"
    );
    assert_eq!(
        count(&engine.pool, TASK_FAILED, id, task_id).await,
        1,
        "TaskFailed is journaled"
    );
    let retries = count(&engine.pool, TASKS, id, task_id).await - 1;
    let replay = count(&engine.pool, REPLAY_ROWS, id, task_id).await;
    assert!(
        (retries == 1) ^ (replay == 1),
        "either a retry is scheduled or the workflow can replay the failure: retries={retries} replay={replay}"
    );

    // The same report again records nothing more.
    engine
        .service
        .fail_task_execution(fail_request(&token))
        .await
        .expect("a repeated report is accepted");
    assert_eq!(count(&engine.pool, TASK_FAILED, id, task_id).await, 1);
    assert_eq!(count(&engine.pool, TASKS, id, task_id).await - 1, retries);

    engine.cleanup(&[id]).await;
}

#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_task_cancellation_is_recorded() {
    let engine = Engine::new().await;
    let (id, execution_id) = engine.claimed_workflow("task-cancel").await;
    let task_id = "slow_0";
    let token = started_task(&engine, id, &execution_id, task_id).await;

    engine
        .service
        .cancel_task_execution(Request::new(CancelTaskExecutionRequest {
            task_token: token,
            namespace: NAMESPACE.to_string(),
            identity: WORKER.to_string(),
            ..Default::default()
        }))
        .await
        .expect("cancel the task");

    assert_eq!(
        count(&engine.pool, CANCELLED, id, task_id).await,
        1,
        "the task is cancelled"
    );
    assert_eq!(
        count(&engine.pool, TASK_CANCELED, id, task_id).await,
        1,
        "TaskCanceled is journaled"
    );

    engine.cleanup(&[id]).await;
}

/// Fail a started task whose policy allows retries with `failure_type`, and
/// return how many retries were scheduled and whether the workflow was given
/// the failure to replay.
async fn fail_with_policy(
    name: &str,
    retry_policy: serde_json::Value,
    failure_type: &str,
) -> (i64, i64) {
    fail_marked_with_policy(name, retry_policy, failure_type, false).await
}

async fn fail_marked_with_policy(
    name: &str,
    retry_policy: serde_json::Value,
    failure_type: &str,
    non_retryable: bool,
) -> (i64, i64) {
    let engine = Engine::new().await;
    let (id, execution_id) = engine.claimed_workflow(name).await;
    let task_id = "charge_0";
    let token =
        started_task_with_policy(&engine, id, &execution_id, task_id, Some(retry_policy)).await;

    engine
        .service
        .fail_task_execution(fail_request_marked(&token, failure_type, non_retryable))
        .await
        .expect("report the failure");

    let retries = count(&engine.pool, TASKS, id, task_id).await - 1;
    let replay = count(&engine.pool, REPLAY_ROWS, id, task_id).await;
    engine.cleanup(&[id]).await;
    (retries, replay)
}

/// The failure type the worker reports decides a non-retryable failure. The
/// decision used to read the task row's type, which a started row does not
/// have yet, so every reported type looked unknown and was retried.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_reported_non_retryable_failure_type_is_not_retried() {
    let policy = serde_json::json!({
        "max_attempts": 5,
        "initial_interval_seconds": 1.0,
        "max_interval_seconds": 10.0,
        "backoff_coefficient": 2.0,
        "non_retryable_error_types": ["CardDeclined"],
    });

    let (retries, replay) = fail_with_policy("non-retryable", policy.clone(), "CardDeclined").await;
    assert_eq!(retries, 0, "a non-retryable failure is not retried");
    assert_eq!(replay, 1, "the workflow is given the failure at once");

    // Any other type is retried as before.
    let (retries, replay) = fail_with_policy("retryable", policy, "Timeout").await;
    assert_eq!((retries, replay), (1, 0), "another failure is retried");
}

/// A policy whose list is spelled as the SDKs spell it is honoured.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_policy_naming_non_retryable_errors_in_the_sdk_spelling_is_honoured() {
    let policy = serde_json::json!({
        "max_attempts": 5,
        "initial_interval_seconds": 1.0,
        "max_interval_seconds": 10.0,
        "backoff_coefficient": 2.0,
        "non_retryable_errors": ["CardDeclined"],
    });

    let (retries, replay) = fail_with_policy("sdk-spelling", policy, "CardDeclined").await;
    assert_eq!((retries, replay), (0, 1), "the SDK-named list is read");
}

/// A failure its worker marks non-retryable is final, whatever the policy
/// allows: the type is one the policy does not list, and attempts remain.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_failure_marked_non_retryable_is_not_retried() {
    let policy = serde_json::json!({
        "max_attempts": 5,
        "initial_interval_seconds": 1.0,
        "max_interval_seconds": 10.0,
        "backoff_coefficient": 2.0,
        "non_retryable_errors": ["CardDeclined"],
    });

    let (retries, replay) =
        fail_marked_with_policy("marked", policy.clone(), "AccountClosed", true).await;
    assert_eq!((retries, replay), (0, 1), "a marked failure is final");

    // The same type unmarked is retried, so it is the flag that decided.
    let (retries, replay) =
        fail_marked_with_policy("unmarked", policy, "AccountClosed", false).await;
    assert_eq!((retries, replay), (1, 0), "an unmarked failure is retried");
}
