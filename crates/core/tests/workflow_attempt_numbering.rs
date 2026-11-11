//! Which attempt a workflow run's activations are handed out as.
//!
//! A task's attempt is counted from 1 on every path it is handed out by, but a
//! run's activations carried the run's retry count: 0 for the first run, 1 for
//! the run of its first workflow-level retry. Each SDK read that differently —
//! one passed the 0 through, the others read both runs as attempt 1. A run's
//! activations now carry its attempt counted from 1, as a task's do: 1 for the
//! first run, 2 for the run of its first retry, and the same number at every
//! activation of a run, however often it is replayed.
//!
//! Requires PostgreSQL. Run with:
//!
//! ```bash
//! export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/orcher_test"
//! cargo test -p orcher-core --test workflow_attempt_numbering -- --ignored --test-threads=1
//! ```

use std::sync::Arc;

use orcher_core::engine::dispatch::{PollingManager, TaskPollingManager};
use orcher_core::engine::queues::outbox::OutboxQueue;
use orcher_core::engine::WorkflowCoordinator;
use orcher_core::events::EventBus;
use orcher_core::grpc::services::ExecutionService;
use orcher_core::workflows::{
    ChildWorkflowManager, CommandProcessor, JournalService, RetryOutcome, RunTimeouts,
    WorkflowExecutionService, WorkflowRetryScheduler, WorkflowRetrySchedulerConfig,
};
use orcher_events::bus::EventBusConfig;
use orcher_proto::orcher::v1::execution_service_server::ExecutionService as ExecutionServiceTrait;
use orcher_proto::orcher::v1::{
    command, Command, CommandType, CompleteWorkflowExecutionRequest, FailWorkflowCommandAttributes,
    Failure, PollWorkflowExecutionRequest, PollWorkflowExecutionResponse,
};
use orcher_storage::repositories::domains::{
    child_workflow::ChildWorkflowRepository, coordination::OutboxRepository,
    journal::JournalRepository, task_execution::TaskExecutionRepository,
    workflow_execution::WorkflowExecutionRepository,
};
use orcher_storage::repositories::{ExecutionLogRepository, PostgresExecutionLogRepository};
use sqlx::PgPool;
use tonic::Request;
use uuid::Uuid;

const NAMESPACE: &str = "default";
const WORKER: &str = "attempt-worker";

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

struct Engine {
    starts: WorkflowExecutionService,
    execution: ExecutionService,
    runs: Arc<WorkflowExecutionRepository>,
}

async fn engine(pool: &PgPool) -> Engine {
    let journal_repo = Arc::new(JournalRepository::new(pool.clone()));
    let journal_service = Arc::new(JournalService::new(journal_repo.clone()));
    let exec_repo = Arc::new(WorkflowExecutionRepository::new(pool.clone()));
    let outbox_queue = Arc::new(OutboxQueue::new(Arc::new(OutboxRepository::new(Arc::new(
        pool.clone(),
    )))));
    let event_bus = EventBus::new(EventBusConfig::default());
    let execution_log_repo = Arc::new(PostgresExecutionLogRepository::new(pool.clone()));
    let task_execution_repo = Arc::new(TaskExecutionRepository::new(pool.clone()));
    let polling_manager = Arc::new(PollingManager::new(
        exec_repo.clone(),
        journal_repo.clone(),
        300,
    ));
    let starts = WorkflowExecutionService::new(
        exec_repo.clone(),
        journal_service.clone(),
        outbox_queue.clone(),
        event_bus.clone(),
        pool.clone(),
    )
    .with_polling_manager(polling_manager.clone());
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
    let execution = ExecutionService::new(
        Arc::new(command_processor),
        polling_manager,
        execution_log_repo as Arc<dyn ExecutionLogRepository>,
        journal_repo.clone(),
        exec_repo.clone(),
        task_execution_repo.clone(),
        pool.clone(),
    )
    .with_task_polling_manager(Arc::new(TaskPollingManager::new(outbox_queue)));
    Engine {
        starts,
        execution,
        runs: exec_repo,
    }
}

async fn activation(engine: &Engine, task_queue: &str) -> PollWorkflowExecutionResponse {
    let activation = engine
        .execution
        .poll_workflow_execution(Request::new(PollWorkflowExecutionRequest {
            task_queue: task_queue.to_string(),
            namespace: NAMESPACE.to_string(),
            identity: WORKER.to_string(),
            poll_timeout: Some(orcher_proto::prost_types::Duration {
                seconds: 10,
                nanos: 0,
            }),
            ..Default::default()
        }))
        .await
        .expect("poll for the workflow")
        .into_inner();
    assert!(
        !activation.execution_id.is_empty(),
        "the workflow was not handed out"
    );
    activation
}

async fn complete(engine: &Engine, activation: &PollWorkflowExecutionResponse, command: Command) {
    engine
        .execution
        .complete_workflow_execution(Request::new(CompleteWorkflowExecutionRequest {
            workflow_id: activation.workflow_id.clone(),
            execution_id: activation.execution_id.clone(),
            namespace: NAMESPACE.to_string(),
            identity: WORKER.to_string(),
            stream_entry_id: activation.stream_entry_id.clone(),
            commands: vec![command],
            ..Default::default()
        }))
        .await
        .expect("complete the activation");
}

/// Hand the claimed activation back unanswered, as a claim that times out or
/// a worker that is gone does, so the next poll hands the run out again and
/// its worker replays it.
async fn redeliver(pool: &PgPool, execution_id: &str) {
    sqlx::query(
        "UPDATE workflow_executions \
         SET claimed_by = NULL, claimed_at = NULL, claim_timeout_at = NULL \
         WHERE execution_id = $1",
    )
    .bind(execution_id)
    .execute(pool)
    .await
    .expect("release the claim");
}

async fn cleanup(pool: &PgPool, task_queue: &str) {
    for q in [
        "DELETE FROM workflow_execution_journal WHERE execution_id IN \
         (SELECT execution_id FROM workflow_executions WHERE task_queue = $1)",
        "DELETE FROM workflow_execution_logs WHERE execution_id IN \
         (SELECT execution_id FROM workflow_executions WHERE task_queue = $1)",
        "DELETE FROM execution_log WHERE workflow_id IN \
         (SELECT id FROM workflow_executions WHERE task_queue = $1)",
        "DELETE FROM outbox_queue WHERE workflow_id IN \
         (SELECT id FROM workflow_executions WHERE task_queue = $1)",
        "DELETE FROM workflow_executions WHERE task_queue = $1",
    ] {
        let _ = sqlx::query(q).bind(task_queue).execute(pool).await;
    }
}

/// A run with one workflow-level retry: its first run is handed out as
/// attempt 1, again as 1 when it is replayed, and the run that retries it as
/// attempt 2.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_first_run_is_attempt_1_and_its_retry_attempt_2() {
    let pool = pool().await;
    let engine = engine(&pool).await;
    let task_queue = format!("wf-attempt-{}", Uuid::new_v4().simple());

    let run = engine
        .starts
        .start_workflow(
            orcher_core::workflows::RunStart {
                workflow_name: "orders".to_string(),
                workflow_version: "1.0".to_string(),
                namespace: NAMESPACE.to_string(),
                task_queue: task_queue.clone(),
                input: serde_json::Value::Null,
                workflow_id: None,
                labels: None,
                max_retries: Some(1),
                retry_policy: None,
                timeouts: RunTimeouts::default(),
                annotations: None,
                start_delay: None,
                request_id: None,
            },
            None,
            None,
        )
        .await
        .expect("start a run");

    let first = activation(&engine, &task_queue).await;
    assert_eq!(first.execution_id, run.execution_id);
    assert_eq!(first.attempt, 1, "a first run is attempt 1");

    redeliver(&pool, &run.execution_id).await;
    let replayed = activation(&engine, &task_queue).await;
    assert_eq!(replayed.execution_id, run.execution_id);
    assert_eq!(
        replayed.attempt, 1,
        "a run's attempt does not change when it is replayed"
    );

    complete(
        &engine,
        &replayed,
        Command {
            command_type: CommandType::FailWorkflow as i32,
            attributes: Some(command::Attributes::FailWorkflow(
                FailWorkflowCommandAttributes {
                    failure: Some(Failure {
                        message: "first run fails".to_string(),
                        failure_type: "Boom".to_string(),
                        ..Default::default()
                    }),
                },
            )),
        },
    )
    .await;

    let failed = engine
        .runs
        .find_by_execution_id(&run.execution_id)
        .await
        .expect("read the failed run")
        .expect("the failed run exists");
    assert_eq!(failed.status, "failed");
    let scheduler =
        WorkflowRetryScheduler::new(engine.runs.clone(), WorkflowRetrySchedulerConfig::default());
    let outcome = scheduler
        .handle_workflow_failure(&failed)
        .await
        .expect("retry the run");
    assert!(
        matches!(outcome, RetryOutcome::Rescheduled { .. }),
        "{outcome:?}"
    );

    let retry = activation(&engine, &task_queue).await;
    assert_ne!(
        retry.execution_id, run.execution_id,
        "the retry is a run of its own"
    );
    assert_eq!(retry.attempt, 2, "the run of the first retry is attempt 2");

    redeliver(&pool, &retry.execution_id).await;
    let retry_replayed = activation(&engine, &task_queue).await;
    assert_eq!(retry_replayed.execution_id, retry.execution_id);
    assert_eq!(retry_replayed.attempt, 2);

    cleanup(&pool, &task_queue).await;
}
