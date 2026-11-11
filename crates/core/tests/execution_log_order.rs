//! What a run's diagnostic log reads as through GetExecutionLogs.
//!
//! The log came back newest first, and said the run had started once when it
//! was submitted and again at every activation of a first run: the poll that
//! wrote the line asked whether the run had been retried, which it had not, not
//! whether this was the claim that started it. Lines now come back in the
//! order they were written, the order `after_id` follows them in, and the
//! start is written once, by the claim that first hands the run to a worker.
//!
//! Requires PostgreSQL. Run with:
//!
//! ```bash
//! export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/orcher_test"
//! cargo test -p orcher-core --test execution_log_order -- --ignored --test-threads=1
//! ```

use std::sync::Arc;

use orcher_common::types::time::Timestamptz;
use orcher_core::engine::dispatch::{PollingManager, TaskPollingManager};
use orcher_core::engine::queues::outbox::OutboxQueue;
use orcher_core::engine::WorkflowCoordinator;
use orcher_core::events::EventBus;
use orcher_core::grpc::services::{ExecutionService, QueryService};
use orcher_core::workflows::{
    ChildWorkflowManager, CommandProcessor, JournalService, RunTimeouts, WorkflowExecutionService,
};
use orcher_events::bus::EventBusConfig;
use orcher_proto::orcher::v1::execution_service_server::ExecutionService as ExecutionServiceTrait;
use orcher_proto::orcher::v1::query_service_server::QueryService as QueryServiceTrait;
use orcher_proto::orcher::v1::{
    command, Command, CommandType, CompleteTaskExecutionRequest, CompleteWorkflowCommandAttributes,
    CompleteWorkflowExecutionRequest, ExecutionLogEntry, GetExecutionLogsRequest,
    PollTaskExecutionRequest, PollWorkflowExecutionRequest, PollWorkflowExecutionResponse,
    ScheduleTaskCommandAttributes,
};
use orcher_storage::repositories::domains::{
    child_workflow::ChildWorkflowRepository,
    coordination::{CoordinationRepository, OutboxRepository},
    journal::JournalRepository,
    task_execution::TaskExecutionRepository,
    workflow_execution::{
        WorkflowExecutionLog, WorkflowExecutionLogsRepository, WorkflowExecutionRepository,
    },
};
use orcher_storage::repositories::{ExecutionLogRepository, PostgresExecutionLogRepository};
use sqlx::PgPool;
use tonic::Request;
use uuid::Uuid;

const NAMESPACE: &str = "default";
const WORKER: &str = "log-worker";

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
    query: QueryService,
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
    let query = QueryService::new(
        exec_repo,
        journal_repo,
        Arc::new(WorkflowExecutionLogsRepository::new(pool.clone())),
        task_execution_repo,
        Arc::new(CoordinationRepository::new(Arc::new(pool.clone()))),
    );
    Engine {
        starts,
        execution,
        query,
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
/// a worker that is gone does, so the next poll hands it out again.
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

async fn logs(
    engine: &Engine,
    execution_id: &str,
    page_size: i32,
    after_id: i64,
) -> Vec<ExecutionLogEntry> {
    engine
        .query
        .get_execution_logs(Request::new(GetExecutionLogsRequest {
            namespace: NAMESPACE.to_string(),
            execution_id: execution_id.to_string(),
            page_size,
            after_id,
            ..Default::default()
        }))
        .await
        .expect("read the run's log")
        .into_inner()
        .logs
}

async fn cleanup(pool: &PgPool, task_queues: &[&str]) {
    for task_queue in task_queues {
        for q in [
            "DELETE FROM workflow_execution_journal WHERE execution_id IN \
             (SELECT execution_id FROM workflow_executions WHERE task_queue = $1)",
            "DELETE FROM workflow_execution_logs WHERE execution_id IN \
             (SELECT execution_id FROM workflow_executions WHERE task_queue = $1)",
            "DELETE FROM execution_log WHERE workflow_id IN \
             (SELECT id FROM workflow_executions WHERE task_queue = $1)",
            "DELETE FROM outbox_queue WHERE workflow_id IN \
             (SELECT id FROM workflow_executions WHERE task_queue = $1)",
            "DELETE FROM task_executions WHERE task_queue = $1",
            "DELETE FROM workflow_executions WHERE task_queue = $1",
        ] {
            let _ = sqlx::query(q).bind(task_queue).execute(pool).await;
        }
    }
}

/// A run handed out four times — its first activation twice, as a claim
/// timeout does, then again for its task's result and for nothing more —
/// logs its start once, and its log reads in the order it was written: by
/// id, which is what a follow pages by, even past a line whose own clock
/// put it earlier.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_runs_log_reads_in_write_order_and_says_it_started_once() {
    let pool = pool().await;
    let engine = engine(&pool).await;
    let task_queue = format!("log-order-{}", Uuid::new_v4().simple());
    let task_queue_for_tasks = format!("{task_queue}-tasks");

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
                max_retries: None,
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
    let execution_id = run.execution_id.clone();

    let first = activation(&engine, &task_queue).await;
    redeliver(&pool, &execution_id).await;
    let again = activation(&engine, &task_queue).await;
    assert_eq!(again.execution_id, first.execution_id);
    complete(
        &engine,
        &again,
        Command {
            command_type: CommandType::ScheduleTask as i32,
            attributes: Some(command::Attributes::ScheduleTask(
                ScheduleTaskCommandAttributes {
                    task_id: "charge_0".to_string(),
                    task_type: "Charge".to_string(),
                    task_queue: task_queue_for_tasks.clone(),
                    input: b"null".to_vec(),
                    ..Default::default()
                },
            )),
        },
    )
    .await;

    let task = engine
        .execution
        .poll_task_execution(Request::new(PollTaskExecutionRequest {
            task_queue: task_queue_for_tasks.clone(),
            namespace: NAMESPACE.to_string(),
            identity: WORKER.to_string(),
            poll_timeout: Some(orcher_proto::prost_types::Duration {
                seconds: 10,
                nanos: 0,
            }),
            ..Default::default()
        }))
        .await
        .expect("poll for the task")
        .into_inner();
    assert_eq!(task.task_id, "charge_0");
    engine
        .execution
        .complete_task_execution(Request::new(CompleteTaskExecutionRequest {
            task_token: task.task_token,
            namespace: NAMESPACE.to_string(),
            identity: WORKER.to_string(),
            result: b"\"charged\"".to_vec(),
        }))
        .await
        .expect("complete the task");

    let last = activation(&engine, &task_queue).await;
    redeliver(&pool, &execution_id).await;
    let last = {
        let again = activation(&engine, &task_queue).await;
        assert_eq!(again.execution_id, last.execution_id);
        again
    };
    complete(
        &engine,
        &last,
        Command {
            command_type: CommandType::CompleteWorkflow as i32,
            attributes: Some(command::Attributes::CompleteWorkflow(
                CompleteWorkflowCommandAttributes {
                    result: b"\"done\"".to_vec(),
                },
            )),
        },
    )
    .await;

    // A line written last whose own clock is an hour behind: it reads last.
    WorkflowExecutionLogsRepository::new(pool.clone())
        .add_log(&WorkflowExecutionLog {
            timestamp: Timestamptz::now() - std::time::Duration::from_secs(3600),
            ..WorkflowExecutionLog::info(execution_id.clone(), "late clock".to_string())
        })
        .await
        .expect("write a line");

    let lines = logs(&engine, &execution_id, 1000, 0).await;
    let messages: Vec<&str> = lines.iter().map(|l| l.message.as_str()).collect();

    let ids: Vec<i64> = lines.iter().map(|l| l.id).collect();
    let mut written = ids.clone();
    written.sort_unstable();
    written.dedup();
    assert_eq!(ids, written, "lines out of write order: {messages:?}");

    let started: Vec<&&str> = messages
        .iter()
        .filter(|m| m.contains("execution started"))
        .collect();
    assert_eq!(
        started,
        [&"Workflow 'orders' execution started"],
        "the run's start is logged once: {messages:?}"
    );
    let position = |needle: &str| {
        messages
            .iter()
            .position(|m| m.contains(needle))
            .unwrap_or_else(|| panic!("no line with {needle:?} in {messages:?}"))
    };
    assert!(
        position("execution submitted") < position("execution started")
            && position("execution started") < position("completed successfully"),
        "{messages:?}"
    );
    assert_eq!(messages.last(), Some(&"late clock"), "{messages:?}");

    // A page is the latest lines, in the same order.
    let tail = logs(&engine, &execution_id, 2, 0).await;
    assert_eq!(tail, lines[lines.len() - 2..].to_vec());

    // Following the log from its first line reads every later line once.
    let mut followed = Vec::new();
    let mut after_id = lines[0].id;
    loop {
        let page = logs(&engine, &execution_id, 2, after_id).await;
        let Some(last) = page.last() else { break };
        after_id = last.id;
        followed.extend(page);
    }
    assert_eq!(followed, lines[1..].to_vec());

    cleanup(&pool, &[&task_queue, &task_queue_for_tasks]).await;
}
