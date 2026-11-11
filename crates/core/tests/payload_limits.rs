//! How large a payload may be, and what happens to one that is larger.
//!
//! A task result above about 2 MiB used to leave its workflow running for
//! good. The activation after it carried the result twice — once in the
//! TaskCompleted journal entry and again in the StepCompleted entry the
//! worker actually reads — so it was over the 4 MiB a worker could receive;
//! the worker refused it, the claim timed out, and the engine handed the
//! same activation out again, forever. A workflow result over 4 MiB did the
//! same, and the caller only ever saw a timeout.
//!
//! Everything here goes through a real gRPC server and client, so the
//! transport's own message limit is part of what is tested.
//!
//! Requires PostgreSQL. Run with:
//!
//! ```bash
//! export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/orcher_test"
//! cargo test -p orcher-core --test payload_limits -- --ignored --test-threads=1
//! ```

use std::sync::Arc;

use orcher_core::engine::dispatch::{PollingManager, TaskPollingManager};
use orcher_core::engine::queues::outbox::OutboxQueue;
use orcher_core::engine::WorkflowCoordinator;
use orcher_core::events::EventBus;
use orcher_core::grpc::payload_limits::{PayloadLimits, MAX_RECEIVE_MESSAGE_BYTES_HEADER, MIB};
use orcher_core::grpc::services::ExecutionService;
use orcher_core::workflows::{
    ChildWorkflowManager, CommandProcessor, JournalService, WorkflowExecutionService,
};
use orcher_events::bus::EventBusConfig;
use orcher_proto::orcher::v1::execution_service_client::ExecutionServiceClient;
use orcher_proto::orcher::v1::execution_service_server::ExecutionServiceServer;
use orcher_proto::orcher::v1::journal_entry::Attributes;
use orcher_proto::orcher::v1::{
    command, Command, CommandType, CompleteTaskExecutionRequest, CompleteWorkflowCommandAttributes,
    CompleteWorkflowExecutionRequest, PollTaskExecutionRequest, PollWorkflowExecutionRequest,
    PollWorkflowExecutionResponse, ScheduleTaskCommandAttributes, StepCompletedEventAttributes,
};
use orcher_storage::repositories::domains::{
    child_workflow::ChildWorkflowRepository, coordination::OutboxRepository,
    journal::JournalRepository, task_execution::TaskExecutionRepository,
    workflow_execution::WorkflowExecutionRepository,
};
use orcher_storage::repositories::{ExecutionLogRepository, PostgresExecutionLogRepository};
use prost::Message;
use sqlx::PgPool;
use tonic::transport::Channel;
use tonic::{Code, Request};
use uuid::Uuid;

const NAMESPACE: &str = "default";
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

/// An engine with the default limits, serving its execution service over
/// gRPC with them as `main` does, and a worker's client to it that can
/// receive as much as the engine sends.
async fn engine(pool: &PgPool) -> ExecutionServiceClient<Channel> {
    let limits = PayloadLimits::default();
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
    .with_polling_manager(polling_manager.clone())
    .with_payload_limits(limits);
    let service = ExecutionService::new(
        Arc::new(command_processor),
        polling_manager,
        execution_log_repo as Arc<dyn ExecutionLogRepository>,
        journal_repo,
        exec_repo,
        task_execution_repo,
        pool.clone(),
    )
    .with_task_polling_manager(Arc::new(TaskPollingManager::new(outbox_queue)))
    .with_payload_limits(limits);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a local port");
    let address = listener.local_addr().unwrap();
    let incoming = tonic::transport::server::TcpIncoming::from_listener(listener, true, None)
        .expect("incoming");
    let server = ExecutionServiceServer::new(service)
        .max_decoding_message_size(limits.max_message_bytes)
        .max_encoding_message_size(limits.max_message_bytes);
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(server)
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
        .max_decoding_message_size(limits.max_message_bytes)
        .max_encoding_message_size(limits.max_message_bytes)
}

/// A running workflow, unclaimed, on a queue of its own.
async fn workflow(pool: &PgPool) -> (Uuid, String) {
    let id = Uuid::new_v4();
    let queue = format!("payload-{}", id.simple());
    sqlx::query(
        r#"
        INSERT INTO workflow_executions (
            id, execution_id, workflow_id, workflow_name, workflow_version, namespace,
            task_queue, status, parameters, context, environment, progress,
            total_steps, completed_steps, failed_steps, retried_steps
        )
        VALUES ($1, $2, $1, 'payload-limits', '1.0.0', $3, $4, 'running',
                '{}', '{}', '{}', '{}', 0, 0, 0, 0)
        "#,
    )
    .bind(id)
    .bind(Uuid::new_v4().to_string())
    .bind(NAMESPACE)
    .bind(&queue)
    .execute(pool)
    .await
    .expect("insert workflow");
    (id, queue)
}

/// What the worker sending a request says it can receive: as much as the
/// engine sends, or — an SDK from before the limit could be raised — nothing.
#[derive(Clone, Copy)]
enum Worker {
    Current,
    Legacy,
}

fn sent_by<T>(worker: Worker, body: T) -> Request<T> {
    let mut request = Request::new(body);
    if let Worker::Current = worker {
        request.metadata_mut().insert(
            MAX_RECEIVE_MESSAGE_BYTES_HEADER,
            PayloadLimits::default()
                .max_message_bytes
                .to_string()
                .parse()
                .unwrap(),
        );
    }
    request
}

const WORKER: &str = "payload-worker";

async fn poll_workflow(
    client: &mut ExecutionServiceClient<Channel>,
    queue: &str,
    worker: Worker,
) -> PollWorkflowExecutionResponse {
    client
        .poll_workflow_execution(sent_by(
            worker,
            PollWorkflowExecutionRequest {
                task_queue: queue.to_string(),
                namespace: NAMESPACE.to_string(),
                identity: WORKER.to_string(),
                poll_timeout: Some(orcher_proto::prost_types::Duration {
                    seconds: 10,
                    nanos: 0,
                }),
                ..Default::default()
            },
        ))
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
                task_type: "Big".to_string(),
                task_queue: queue.to_string(),
                input: b"null".to_vec(),
                ..Default::default()
            },
        )),
    }
}

fn complete_workflow(result: Vec<u8>) -> Command {
    Command {
        command_type: CommandType::CompleteWorkflow as i32,
        attributes: Some(command::Attributes::CompleteWorkflow(
            CompleteWorkflowCommandAttributes { result },
        )),
    }
}

async fn complete_activation(
    client: &mut ExecutionServiceClient<Channel>,
    activation: &PollWorkflowExecutionResponse,
    commands: Vec<Command>,
) -> Vec<orcher_proto::orcher::v1::PollTaskExecutionResponse> {
    client
        .complete_workflow_execution(sent_by(
            Worker::Current,
            CompleteWorkflowExecutionRequest {
                workflow_id: activation.workflow_id.clone(),
                execution_id: activation.execution_id.clone(),
                namespace: NAMESPACE.to_string(),
                identity: WORKER.to_string(),
                stream_entry_id: activation.stream_entry_id.clone(),
                commands,
                ..Default::default()
            },
        ))
        .await
        .expect("complete the activation")
        .into_inner()
        .eager_tasks
}

/// A JSON string `bytes` long, as a task or workflow result.
fn json_of(bytes: usize) -> Vec<u8> {
    serde_json::to_vec(&"x".repeat(bytes - 2)).unwrap()
}

/// Run a workflow up to the point where its tasks have reported `results`:
/// one activation schedules a task per result, the tasks are taken and each
/// completed with its result. Returns the answer to each completion.
async fn tasks_report(
    client: &mut ExecutionServiceClient<Channel>,
    queue: &str,
    results: Vec<Vec<u8>>,
) -> Vec<Result<(), tonic::Status>> {
    let first = poll_workflow(client, queue, Worker::Current).await;
    assert!(
        !first.execution_id.is_empty(),
        "the workflow was not handed out"
    );
    let commands = (0..results.len())
        .map(|i| schedule(&format!("big_{i}"), queue))
        .collect();
    let mut tokens: Vec<Vec<u8>> = complete_activation(client, &first, commands)
        .await
        .into_iter()
        .map(|task| task.task_token)
        .collect();
    while tokens.len() < results.len() {
        let task = client
            .poll_task_execution(sent_by(
                Worker::Current,
                PollTaskExecutionRequest {
                    task_queue: queue.to_string(),
                    namespace: NAMESPACE.to_string(),
                    identity: WORKER.to_string(),
                    poll_timeout: Some(orcher_proto::prost_types::Duration {
                        seconds: 5,
                        nanos: 0,
                    }),
                    ..Default::default()
                },
            ))
            .await
            .expect("poll for a task")
            .into_inner();
        assert!(
            !task.task_token.is_empty(),
            "a scheduled task was not handed out"
        );
        tokens.push(task.task_token);
    }
    let mut answers = Vec::new();
    for (token, result) in tokens.into_iter().zip(results) {
        answers.push(
            client
                .complete_task_execution(sent_by(
                    Worker::Current,
                    CompleteTaskExecutionRequest {
                        task_token: token,
                        namespace: NAMESPACE.to_string(),
                        identity: WORKER.to_string(),
                        result,
                    },
                ))
                .await
                .map(|_| ()),
        );
    }
    answers
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

async fn workflow_row(pool: &PgPool, id: Uuid) -> (String, Option<String>) {
    sqlx::query_as("SELECT status, error FROM workflow_executions WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("read the workflow")
}

/// The activation after a task carries its result once. It used to carry it
/// twice — 6.3 MB for a 3 MB result — which is what put a 2 MiB result over
/// the 4 MiB a worker could receive.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_task_result_travels_once_in_the_activation_after_it() {
    let pool = pool().await;
    let mut client = engine(&pool).await;
    let (_, queue) = workflow(&pool).await;

    let result = json_of(3 * MIB);
    let answers = tasks_report(&mut client, &queue, vec![result.clone()]).await;
    assert!(answers[0].is_ok(), "{:?}", answers[0]);

    let activation = poll_workflow(&mut client, &queue, Worker::Current).await;
    let delivered = steps(&activation);
    assert_eq!(
        delivered.len(),
        1,
        "the task's outcome is not in the activation"
    );
    assert_eq!(
        delivered[0].result, result,
        "the task's result is not what it reported"
    );

    let size = activation.encoded_len();
    assert!(
        size < result.len() + result.len() / 10,
        "the activation is {size} bytes for a {} byte result: the result travels more than once",
        result.len()
    );
}

/// Between tonic's 4 MiB default and the engine's limit, a task result goes
/// through: the completion is accepted and the workflow reads it.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_task_result_between_the_old_and_new_limits_reaches_the_workflow() {
    let pool = pool().await;
    let mut client = engine(&pool).await;
    let (_, queue) = workflow(&pool).await;

    let result = json_of(5 * MIB);
    let answers = tasks_report(&mut client, &queue, vec![result.clone()]).await;
    assert!(
        answers[0].is_ok(),
        "a 5 MiB result was refused: {:?}",
        answers[0]
    );

    let activation = poll_workflow(&mut client, &queue, Worker::Current).await;
    let delivered = steps(&activation);
    assert_eq!(
        delivered.len(),
        1,
        "the task's outcome is not in the activation"
    );
    assert_eq!(delivered[0].result, result);
    assert!(delivered[0].failure.is_none(), "{:?}", delivered[0].failure);
}

/// A result over the payload limit fails its task — final, whatever the retry
/// policy — and the workflow hears a task failure that says why. The worker
/// is told the same, as an answer no SDK sends again.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_task_result_over_the_limit_fails_the_task_and_says_why() {
    let pool = pool().await;
    let mut client = engine(&pool).await;
    let (_, queue) = workflow(&pool).await;

    let answers = tasks_report(&mut client, &queue, vec![json_of(9 * MIB)]).await;
    let refusal = answers[0]
        .as_ref()
        .expect_err("a 9 MiB result was accepted");
    let expected = "the result of task \"big_0\" is 9 MiB; the limit is 8 MiB \
                    (payload_max_bytes). Store large data elsewhere and pass a reference.";
    assert_eq!(refusal.code(), Code::InvalidArgument);
    assert_eq!(refusal.message(), expected);

    let activation = poll_workflow(&mut client, &queue, Worker::Current).await;
    assert!(
        !activation.execution_id.is_empty(),
        "the workflow was not woken by its task's failure"
    );
    let delivered = steps(&activation);
    assert_eq!(
        delivered.len(),
        1,
        "the task's outcome is not in the activation"
    );
    let failure = delivered[0]
        .failure
        .as_ref()
        .expect("the workflow did not hear that its task failed");
    assert_eq!(failure.message, expected);
    assert!(
        delivered[0].result.len() < 1024,
        "the oversized result reached the workflow anyway"
    );

    let retries: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM task_executions WHERE workflow_id = $1 AND attempt_number > 1",
    )
    .bind(Uuid::parse_str(&activation.workflow_id).unwrap())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(retries, 0, "a result that will not fit was retried");
}

/// A worker that states no limit is held to tonic's 4 MiB default, which is
/// what SDKs from before the limit could be raised decode with. An activation
/// larger than that used to be handed to it, refused, and handed out again at
/// every claim timeout; now the workflow fails, saying why.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn an_activation_larger_than_an_old_worker_can_receive_fails_the_workflow() {
    let pool = pool().await;
    let mut client = engine(&pool).await;
    let (id, queue) = workflow(&pool).await;

    // Each fits on its own; together they do not.
    let answers = tasks_report(
        &mut client,
        &queue,
        vec![json_of(5 * MIB / 2), json_of(5 * MIB / 2)],
    )
    .await;
    assert!(answers.iter().all(Result::is_ok), "{answers:?}");

    let activation = poll_workflow(&mut client, &queue, Worker::Legacy).await;
    assert!(
        activation.execution_id.is_empty(),
        "a worker that can receive 4 MiB was handed a {} byte activation",
        activation.encoded_len()
    );

    let (status, error) = workflow_row(&pool, id).await;
    assert_eq!(status, "failed");
    let error = error.unwrap_or_default();
    assert!(
        error.contains("more than the 4 MiB the worker can receive") && error.contains("upgrade"),
        "{error}"
    );

    // And nothing is left to hand out again.
    let again = poll_workflow(&mut client, &queue, Worker::Current).await;
    assert!(
        again.execution_id.is_empty(),
        "the failed workflow was handed out again"
    );
}

/// A workflow result over the payload limit fails the workflow, saying why,
/// where it used to leave the workflow running and its caller waiting.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_workflow_result_over_the_limit_fails_the_workflow() {
    let pool = pool().await;
    let mut client = engine(&pool).await;
    let (id, queue) = workflow(&pool).await;

    let activation = poll_workflow(&mut client, &queue, Worker::Current).await;
    assert!(!activation.execution_id.is_empty());
    complete_activation(
        &mut client,
        &activation,
        vec![complete_workflow(json_of(9 * MIB))],
    )
    .await;

    let (status, error) = workflow_row(&pool, id).await;
    assert_eq!(status, "failed");
    assert_eq!(
        error.as_deref(),
        Some(
            "the workflow result is 9 MiB; the limit is 8 MiB (payload_max_bytes). \
             Store large data elsewhere and pass a reference."
        )
    );
}

/// A workflow result between tonic's default and the payload limit completes
/// the workflow.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_workflow_result_between_the_old_and_new_limits_completes_the_workflow() {
    let pool = pool().await;
    let mut client = engine(&pool).await;
    let (id, queue) = workflow(&pool).await;

    let activation = poll_workflow(&mut client, &queue, Worker::Current).await;
    assert!(!activation.execution_id.is_empty());
    complete_activation(
        &mut client,
        &activation,
        vec![complete_workflow(json_of(5 * MIB))],
    )
    .await;

    let (status, error) = workflow_row(&pool, id).await;
    assert_eq!(status, "completed", "{error:?}");
}
