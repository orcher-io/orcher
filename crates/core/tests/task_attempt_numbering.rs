//! Which attempt a task is handed to its worker as.
//!
//! A polled task carried the outbox row's delivery count as its attempt, 0 for
//! nearly every task, while an eagerly returned one carried the task's own
//! attempt number, 1 for the first. A handler branching on its attempt saw a
//! different number depending on how the task reached it, and a retry read as
//! attempt 0 like the attempt before it. Every path now hands an attempt out
//! as the task execution's own number, counted from 1, in the response and in
//! the token its worker reports with.
//!
//! Requires PostgreSQL. Run with:
//!
//! ```bash
//! export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/orcher_test"
//! cargo test -p orcher-core --test task_attempt_numbering -- --ignored --test-threads=1
//! ```

use std::sync::Arc;
use std::time::Duration;

use orcher_common::types::identifiers::{ExecutionId, WorkflowId};
use orcher_core::engine::dispatch::{PollingManager, TaskPollingManager};
use orcher_core::engine::queues::outbox::OutboxQueue;
use orcher_core::engine::WorkflowCoordinator;
use orcher_core::events::EventBus;
use orcher_core::grpc::services::ExecutionService;
use orcher_core::retries::{TaskRetryDispatcher, TaskRetryScheduler};
use orcher_core::workflows::{
    ChildWorkflowManager, CommandProcessor, JournalService, WorkflowExecutionService,
};
use orcher_events::bus::EventBusConfig;
use orcher_proto::orcher::v1::execution_service_server::ExecutionService as ExecutionServiceTrait;
use orcher_proto::orcher::v1::{
    command, Command, CommandType, CompleteWorkflowExecutionRequest, FailTaskExecutionRequest,
    Failure, PollTaskExecutionRequest, PollTaskExecutionResponse, PollWorkflowExecutionRequest,
    RetryPolicy, ScheduleTaskCommandAttributes,
};
use orcher_storage::repositories::domains::matching::{MatchTask, MatchingStore, PgMatchingStore};
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

/// An engine on `pool`, delivering tasks from the matching partitions when
/// `matching` is given, and the coordinator its dispatches go through.
async fn engine(
    pool: &PgPool,
    matching: Option<Arc<dyn MatchingStore>>,
) -> (ExecutionService, Arc<WorkflowCoordinator>) {
    let journal_repo = Arc::new(JournalRepository::new(pool.clone()));
    let journal_service = Arc::new(JournalService::new(journal_repo.clone()));
    let exec_repo = Arc::new(WorkflowExecutionRepository::new(pool.clone()));
    let outbox_queue = Arc::new(OutboxQueue::new(Arc::new(OutboxRepository::new(Arc::new(
        pool.clone(),
    )))));
    let event_bus = EventBus::new(EventBusConfig::default());
    let execution_log_repo = Arc::new(PostgresExecutionLogRepository::new(pool.clone()));
    let task_execution_repo = Arc::new(TaskExecutionRepository::new(pool.clone()));
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
        300,
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
    .with_coordinator(coordinator.clone())
    .with_polling_manager(polling_manager.clone());
    let task_polling_manager = Arc::new(TaskPollingManager::new(outbox_queue));
    if let Some(store) = matching {
        task_polling_manager.set_matching_store(store);
    }
    let service = ExecutionService::new(
        Arc::new(command_processor),
        polling_manager,
        execution_log_repo as Arc<dyn ExecutionLogRepository>,
        journal_repo,
        exec_repo,
        task_execution_repo,
        pool.clone(),
    )
    .with_task_polling_manager(task_polling_manager);
    (service, coordinator)
}

fn queue(prefix: &str) -> String {
    format!("{prefix}-{}", Uuid::new_v4().simple())
}

/// A running workflow on `task_queue`, not yet claimed.
async fn workflow(pool: &PgPool, task_queue: &str) -> (Uuid, String) {
    let id = Uuid::new_v4();
    let execution_id = Uuid::new_v4().to_string();
    sqlx::query(
        r#"
        INSERT INTO workflow_executions (
            id, execution_id, workflow_id, workflow_name, workflow_version, namespace,
            task_queue, status, parameters, context, environment, progress,
            total_steps, completed_steps, failed_steps, retried_steps
        )
        VALUES ($1, $2, $1, 'attempts', '1.0.0', $3, $4, 'running',
                '{}', '{}', '{}', '{}', 0, 0, 0, 0)
        "#,
    )
    .bind(id)
    .bind(&execution_id)
    .bind(NAMESPACE)
    .bind(task_queue)
    .execute(pool)
    .await
    .expect("insert workflow");
    (id, execution_id)
}

/// A retry policy that retries a second after a failure, up to three attempts.
fn retried() -> RetryPolicy {
    RetryPolicy {
        initial_interval: Some(orcher_proto::prost_types::Duration {
            seconds: 1,
            nanos: 0,
        }),
        backoff_coefficient: 1.0,
        maximum_interval: Some(orcher_proto::prost_types::Duration {
            seconds: 1,
            nanos: 0,
        }),
        maximum_attempts: 3,
        non_retryable_error_types: vec![],
    }
}

fn schedule(task_id: &str, task_queue: &str) -> Command {
    Command {
        command_type: CommandType::ScheduleTask as i32,
        attributes: Some(command::Attributes::ScheduleTask(
            ScheduleTaskCommandAttributes {
                task_id: task_id.to_string(),
                task_type: "Flaky".to_string(),
                task_queue: task_queue.to_string(),
                input: b"null".to_vec(),
                retry_policy: Some(retried()),
                ..Default::default()
            },
        )),
    }
}

fn task_poll(task_queue: &str) -> Request<PollTaskExecutionRequest> {
    Request::new(PollTaskExecutionRequest {
        task_queue: task_queue.to_string(),
        namespace: NAMESPACE.to_string(),
        identity: WORKER.to_string(),
        poll_timeout: Some(orcher_proto::prost_types::Duration {
            seconds: 10,
            nanos: 0,
        }),
        ..Default::default()
    })
}

async fn polled(service: &ExecutionService, task_queue: &str) -> PollTaskExecutionResponse {
    // A poll that claims a dispatch whose task was already taken — as the
    // first attempt of an eagerly returned task is — answers with nothing,
    // and the worker polls again.
    for _ in 0..5 {
        let task = service
            .poll_task_execution(task_poll(task_queue))
            .await
            .expect("poll for a task")
            .into_inner();
        if !task.task_token.is_empty() {
            return task;
        }
    }
    panic!("no task was handed out on {task_queue}");
}

/// The attempt `task` was handed out as, and the attempt its token reports.
fn handed_out_as(task: &PollTaskExecutionResponse) -> (i32, i32) {
    let token = orcher_core::grpc::task_token::decode(&task.task_token).expect("decode the token");
    (task.attempt, token.attempt)
}

async fn fail(service: &ExecutionService, task: &PollTaskExecutionResponse) {
    service
        .fail_task_execution(Request::new(FailTaskExecutionRequest {
            task_token: task.task_token.clone(),
            namespace: NAMESPACE.to_string(),
            identity: WORKER.to_string(),
            failure: Some(Failure {
                message: "flaked".to_string(),
                failure_type: "Flaked".to_string(),
                non_retryable: false,
                ..Default::default()
            }),
        }))
        .await
        .expect("fail the attempt");
}

async fn cleanup(pool: &PgPool, ids: &[Uuid]) {
    for id in ids {
        for q in [
            "DELETE FROM matching_partitions WHERE workflow_id = $1",
            "DELETE FROM outbox_queue WHERE workflow_id = $1",
            "DELETE FROM task_executions WHERE workflow_id = $1",
            "DELETE FROM execution_log WHERE workflow_id = $1",
        ] {
            let _ = sqlx::query(q).bind(id).execute(pool).await;
        }
        let _ = sqlx::query("DELETE FROM workflow_execution_journal WHERE workflow_id = $1")
            .bind(id.to_string())
            .execute(pool)
            .await;
        let _ = sqlx::query(
            "DELETE FROM workflow_execution_logs WHERE execution_id IN \
             (SELECT execution_id FROM workflow_executions WHERE id = $1)",
        )
        .bind(id)
        .execute(pool)
        .await;
        let _ = sqlx::query("DELETE FROM workflow_executions WHERE id = $1")
            .bind(id)
            .execute(pool)
            .await;
    }
}

/// The tasks `commands` schedule from an activation of a new workflow on
/// `task_queue`, completed by the worker it was handed to: those returned
/// eagerly with the answer, and the workflow's row id.
async fn scheduled(
    pool: &PgPool,
    service: &ExecutionService,
    task_queue: &str,
    commands: Vec<Command>,
) -> (Vec<PollTaskExecutionResponse>, Uuid) {
    let (id, _) = workflow(pool, task_queue).await;
    let activation = service
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
    let eager = service
        .complete_workflow_execution(Request::new(CompleteWorkflowExecutionRequest {
            workflow_id: activation.workflow_id.clone(),
            execution_id: activation.execution_id.clone(),
            namespace: NAMESPACE.to_string(),
            identity: WORKER.to_string(),
            stream_entry_id: activation.stream_entry_id.clone(),
            commands,
            ..Default::default()
        }))
        .await
        .expect("complete the activation")
        .into_inner()
        .eager_tasks;
    (eager, id)
}

/// A task returned eagerly with the activation that scheduled it, one handed
/// out by a poll of the outbox (here on a session's queue), and the retries of
/// both, which only a poll hands out: each first attempt is attempt 1 and each
/// first retry attempt 2.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_first_attempt_is_handed_out_as_1_and_its_first_retry_as_2() {
    let pool = pool().await;
    let (service, coordinator) = engine(&pool, None).await;

    // Hands each retry to the outbox once its backoff has run, as the engine
    // does.
    let retries = TaskRetryDispatcher::new(TaskRetryScheduler::new(pool.clone()), coordinator)
        .with_interval(Duration::from_millis(100))
        .start();

    // Eager: a task on the completing worker's own queue comes back with the
    // answer.
    let task_queue = queue("eager-attempts");
    let (eager, eager_workflow) = scheduled(
        &pool,
        &service,
        &task_queue,
        vec![schedule("eager_0", &task_queue)],
    )
    .await;
    assert_eq!(
        eager.iter().map(|t| t.task_id.as_str()).collect::<Vec<_>>(),
        ["eager_0"],
        "the task was not returned eagerly"
    );
    let eager = &eager[0];
    assert_eq!(handed_out_as(eager), (1, 1), "eager first attempt");
    assert_eq!(
        eager.retry_policy,
        Some(retried()),
        "an eager task is told the retry policy it was scheduled with"
    );

    // Polled from the outbox: a task on a queue of its own, a session's.
    let session_queue = format!("{}__session__{WORKER}", queue("polled-attempts"));
    let (none, polled_workflow) = scheduled(
        &pool,
        &service,
        &queue("polled-attempts"),
        vec![schedule("polled_0", &session_queue)],
    )
    .await;
    assert!(none.is_empty(), "a task on another queue came back eagerly");
    let first = polled(&service, &session_queue).await;
    assert_eq!(first.task_id, "polled_0");
    assert_eq!(handed_out_as(&first), (1, 1), "polled first attempt");
    assert_eq!(
        first.retry_policy,
        Some(retried()),
        "a polled task is told the retry policy it was scheduled with"
    );

    // Their retries, polled from the outbox.
    fail(&service, eager).await;
    let retry = polled(&service, &task_queue).await;
    assert_eq!(retry.task_id, "eager_0");
    assert_eq!(
        handed_out_as(&retry),
        (2, 2),
        "first retry of an eager attempt"
    );

    fail(&service, &first).await;
    let retry = polled(&service, &session_queue).await;
    assert_eq!(retry.task_id, "polled_0");
    assert_eq!(
        handed_out_as(&retry),
        (2, 2),
        "first retry of a polled attempt"
    );

    retries.abort();
    cleanup(&pool, &[eager_workflow, polled_workflow]).await;
}

/// A task delivered from a matching partition, whose row carries the outbox
/// dispatch's payload as it was transferred, is handed out as the attempt it
/// delivers.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_task_from_a_matching_partition_is_handed_out_as_its_attempt() {
    let pool = pool().await;
    let store = Arc::new(PgMatchingStore::new(pool.clone()));
    let (service, _) = engine(&pool, Some(store.clone())).await;
    let task_queue = queue("matched-attempts");
    let (id, execution_id) = workflow(&pool, &task_queue).await;

    for attempt in [1, 2] {
        let task_id = format!("matched_{attempt}");
        let task = TaskExecutionRepository::new(pool.clone())
            .create_scheduled(
                task_id.clone(),
                "Flaky".to_string(),
                WorkflowId::from_uuid(id),
                ExecutionId::from_uuid(Uuid::parse_str(&execution_id).unwrap()),
                task_queue.clone(),
                NAMESPACE.to_string(),
                attempt,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .await
            .expect("schedule the attempt");
        store
            .enqueue(&MatchTask {
                task_id: Uuid::new_v4(),
                namespace: NAMESPACE.to_string(),
                task_queue: task_queue.clone(),
                workflow_id: id,
                payload: WorkflowCoordinator::task_dispatch_payload(&task),
                organization_id: None,
            })
            .await
            .expect("enqueue");

        let handed = polled(&service, &task_queue).await;
        assert_eq!(handed.task_id, task_id);
        assert_eq!(handed_out_as(&handed), (attempt, attempt));
    }

    cleanup(&pool, &[id]).await;
}

/// A ScheduleTask policy's `maximum_attempts`, the first attempt counted, as
/// a workflow's is: 1 runs the task once, N runs it N times, and 0 does not
/// stop retrying. 0 used to be read as 3.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_tasks_maximum_attempts_counts_its_attempts_and_0_is_unlimited() {
    let pool = pool().await;
    let (service, coordinator) = engine(&pool, None).await;
    let retries = TaskRetryDispatcher::new(TaskRetryScheduler::new(pool.clone()), coordinator)
        .with_interval(Duration::from_millis(50))
        .start();

    // How many attempts a task allowed `maximum_attempts` is run, failing
    // each, up to `cap`.
    async fn attempts_run(
        pool: &PgPool,
        service: &ExecutionService,
        maximum_attempts: i32,
        cap: i32,
        ids: &mut Vec<Uuid>,
    ) -> i32 {
        let task_queue = queue("max-attempts");
        let brief = Some(orcher_proto::prost_types::Duration {
            seconds: 0,
            nanos: 10_000_000,
        });
        let mut command = schedule("charge_0", &task_queue);
        if let Some(command::Attributes::ScheduleTask(ref mut attrs)) = command.attributes {
            attrs.retry_policy = Some(RetryPolicy {
                maximum_attempts,
                initial_interval: brief,
                maximum_interval: brief,
                ..retried()
            });
        }
        let (eager, id) = scheduled(pool, service, &task_queue, vec![command]).await;
        ids.push(id);
        let mut task = eager.into_iter().next().expect("returned eagerly");
        let rows = |pool: PgPool| async move {
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM task_executions WHERE workflow_id = $1",
            )
            .bind(id)
            .fetch_one(&pool)
            .await
            .expect("count the attempts")
        };
        for attempt in 1..=cap {
            assert_eq!(handed_out_as(&task), (attempt, attempt));
            fail(service, &task).await;
            if rows(pool.clone()).await == attempt as i64 {
                // No retry: this attempt was the last.
                return attempt;
            }
            if attempt < cap {
                task = polled(service, &task_queue).await;
            }
        }
        cap
    }

    let mut ids = Vec::new();
    assert_eq!(
        attempts_run(&pool, &service, 1, 5, &mut ids).await,
        1,
        "1: no retry"
    );
    assert_eq!(
        attempts_run(&pool, &service, 3, 5, &mut ids).await,
        3,
        "3: three attempts"
    );
    let unlimited = attempts_run(&pool, &service, 0, 5, &mut ids).await;
    assert_eq!(unlimited, 5, "0: still retried after five attempts");
    let fifth_retried: i64 =
        sqlx::query_scalar("SELECT count(*) FROM task_executions WHERE workflow_id = $1")
            .bind(ids[2])
            .fetch_one(&pool)
            .await
            .expect("count the attempts");
    assert_eq!(fifth_retried, 6, "0: the fifth failure is retried too");

    retries.abort();
    cleanup(&pool, &ids).await;
}
