//! A workflow completion is applied once, and only for the activation it
//! answers.
//!
//! A completion used to be applied on the strength of its ids alone. Sent
//! twice — a worker retrying after its reply was lost — it was applied twice:
//! the workflow was completed twice, with two completion entries in its
//! journal. Sent by a worker whose claim had lapsed and been handed to another,
//! it was applied over the newer activation. Each activation is now numbered by
//! the claim that hands it out, the worker gets the number back in its
//! activation token, and a completion is held to it.
//!
//! A completion with no token — every SDK before this, and every failure
//! report today — is still accepted. For those the handlers themselves refuse
//! the damage a repeat does: a task is scheduled once, an ended run keeps its
//! ending.
//!
//! Requires PostgreSQL. The schema is created by the engine's own migration
//! runner. Run with:
//!
//! ```bash
//! export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/orcher_test"
//! cargo test -p orcher-core --test activation_fence -- --ignored --test-threads=1
//! ```

use std::sync::Arc;

use orcher_core::engine::dispatch::PollingManager;
use orcher_core::engine::queues::outbox::OutboxQueue;
use orcher_core::engine::WorkflowCoordinator;
use orcher_core::events::EventBus;
use orcher_core::grpc::services::ExecutionService;
use orcher_core::workflows::{
    ChildWorkflowManager, CommandProcessor, JournalService, WorkflowExecutionService,
};
use orcher_events::bus::EventBusConfig;
use orcher_proto::orcher::v1::execution_service_server::ExecutionService as ExecutionServiceTrait;
use orcher_proto::orcher::v1::FailWorkflowExecutionRequest;
use orcher_proto::orcher::v1::{
    command, Command, CommandType, CompleteWorkflowCommandAttributes,
    CompleteWorkflowExecutionRequest, PollWorkflowExecutionRequest, ScheduleTaskCommandAttributes,
};
use orcher_storage::repositories::domains::workflow_execution::HandOut;
use orcher_storage::repositories::domains::{
    child_workflow::ChildWorkflowRepository, coordination::OutboxRepository,
    journal::JournalRepository, task_execution::TaskExecutionRepository,
    workflow_execution::WorkflowExecutionRepository,
};
use orcher_storage::repositories::{ExecutionLogRepository, PostgresExecutionLogRepository};
use sqlx::PgPool;
use tonic::{Code, Request};
use uuid::Uuid;

const NAMESPACE: &str = "default";
const WORKER: &str = "worker-a";
const OTHER_WORKER: &str = "worker-b";

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

/// The token a poll hands the worker for activation `seq`.
fn token(execution_id: &str, seq: i64) -> String {
    orcher_core::grpc::activation_token::encode(execution_id, seq)
}

/// One engine's poll and completion paths, and its execution repository.
async fn engine(pool: &PgPool) -> (Arc<ExecutionService>, Arc<WorkflowExecutionRepository>) {
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
    (Arc::new(service), exec_repo)
}

/// A running workflow, unclaimed, alone on a task queue of its own.
async fn workflow(pool: &PgPool) -> (Uuid, String, String) {
    let id = Uuid::new_v4();
    let execution_id = Uuid::new_v4().to_string();
    let task_queue = format!("fence-{}", Uuid::new_v4().simple());
    sqlx::query(
        r#"
        INSERT INTO workflow_executions (
            id, execution_id, workflow_id, workflow_name, workflow_version, namespace,
            task_queue, status, parameters, context, environment, progress,
            total_steps, completed_steps, failed_steps, retried_steps
        )
        VALUES ($1, $2, $1, 'activation-fence', '1.0.0', $3, $4, 'running',
                '{}', '{}', '{}', '{}', 0, 0, 0, 0)
        "#,
    )
    .bind(id)
    .bind(&execution_id)
    .bind(NAMESPACE)
    .bind(&task_queue)
    .execute(pool)
    .await
    .expect("insert workflow");
    (id, execution_id, task_queue)
}

/// Hand the workflow to `worker`, as a poll does, and return the number the
/// claim gave the activation.
async fn claim(repo: &WorkflowExecutionRepository, task_queue: &str, worker: &str) -> i64 {
    repo.claim_and_get_next(task_queue, NAMESPACE, worker, 300, None, None)
        .await
        .expect("claim")
        .expect("the workflow is claimable")
        .activation_seq
}

/// The claim lapses, as the reclaimer finds it when a worker goes quiet.
async fn lapse_claim(pool: &PgPool, id: Uuid) {
    sqlx::query(
        "UPDATE workflow_executions SET claimed_by = NULL, claimed_at = NULL, \
         claim_timeout_at = NULL WHERE id = $1",
    )
    .bind(id)
    .execute(pool)
    .await
    .expect("lapse the claim");
}

fn schedule(task_id: &str) -> Command {
    Command {
        command_type: CommandType::ScheduleTask as i32,
        attributes: Some(command::Attributes::ScheduleTask(
            ScheduleTaskCommandAttributes {
                task_id: task_id.to_string(),
                task_type: "charge".to_string(),
                task_queue: "fence-tasks".to_string(),
                input: b"{}".to_vec(),
                ..Default::default()
            },
        )),
    }
}

fn complete() -> Command {
    Command {
        command_type: CommandType::CompleteWorkflow as i32,
        attributes: Some(command::Attributes::CompleteWorkflow(
            CompleteWorkflowCommandAttributes {
                result: br#"{"ok":true}"#.to_vec(),
            },
        )),
    }
}

/// A completion carrying `activation_token` where every SDK returns it.
fn completion(
    id: Uuid,
    execution_id: &str,
    activation_token: Option<String>,
    commands: Vec<Command>,
) -> CompleteWorkflowExecutionRequest {
    CompleteWorkflowExecutionRequest {
        workflow_id: id.to_string(),
        execution_id: execution_id.to_string(),
        namespace: NAMESPACE.to_string(),
        identity: WORKER.to_string(),
        // What SDKs send today in place of a real token.
        task_token: format!("{id}:{execution_id}").into_bytes(),
        stream_entry_id: activation_token.unwrap_or_default(),
        commands,
        ..Default::default()
    }
}

async fn status_of(pool: &PgPool, id: Uuid) -> String {
    sqlx::query_scalar("SELECT status FROM workflow_executions WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("read status")
}

async fn journal_count(pool: &PgPool, execution_id: &str, event_type: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM workflow_execution_journal \
         WHERE execution_id = $1 AND event_type = $2",
    )
    .bind(execution_id)
    .bind(event_type)
    .fetch_one(pool)
    .await
    .expect("count journal entries")
}

async fn task_rows(pool: &PgPool, execution_id: &str, task_id: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM task_executions WHERE execution_id = $1::uuid AND task_id = $2",
    )
    .bind(execution_id)
    .bind(task_id)
    .fetch_one(pool)
    .await
    .expect("count task rows")
}

async fn cleanup(pool: &PgPool, id: Uuid, execution_id: &str) {
    for (q, key) in [
        (
            "DELETE FROM workflow_execution_journal WHERE execution_id = $1",
            execution_id.to_string(),
        ),
        (
            "DELETE FROM workflow_execution_logs WHERE execution_id = $1",
            execution_id.to_string(),
        ),
        (
            "DELETE FROM task_executions WHERE execution_id = $1::uuid",
            execution_id.to_string(),
        ),
        (
            "DELETE FROM outbox_queue WHERE workflow_id = $1::uuid",
            id.to_string(),
        ),
        (
            "DELETE FROM workflow_executions WHERE id = $1::uuid",
            id.to_string(),
        ),
    ] {
        let _ = sqlx::query(q).bind(key).execute(pool).await;
    }
}

/// The same completion twice — a retry after a lost reply — is applied once,
/// and the retry is told it succeeded.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn the_same_completion_sent_twice_is_applied_once() {
    let pool = pool().await;
    let (service, repo) = engine(&pool).await;
    let (id, execution_id, task_queue) = workflow(&pool).await;

    // First activation schedules a task and suspends.
    let first = claim(&repo, &task_queue, WORKER).await;
    let schedule_a = completion(
        id,
        &execution_id,
        Some(token(&execution_id, first)),
        vec![schedule("task-a")],
    );
    for attempt in 0..2 {
        let answer = service
            .complete_workflow_execution(Request::new(schedule_a.clone()))
            .await
            .unwrap_or_else(|e| panic!("send {attempt} must succeed: {e:?}"));
        if attempt == 1 {
            assert!(
                answer.into_inner().eager_tasks.is_empty(),
                "a replay hands out nothing"
            );
        }
    }
    assert_eq!(task_rows(&pool, &execution_id, "task-a").await, 1);
    assert_eq!(
        journal_count(&pool, &execution_id, "TaskScheduled").await,
        1
    );

    // Second activation completes the workflow, and that is sent twice too.
    lapse_claim(&pool, id).await;
    let second = claim(&repo, &task_queue, WORKER).await;
    assert!(second > first, "every claim is a new activation");
    let finish = completion(
        id,
        &execution_id,
        Some(token(&execution_id, second)),
        vec![complete()],
    );
    for attempt in 0..2 {
        service
            .complete_workflow_execution(Request::new(finish.clone()))
            .await
            .unwrap_or_else(|e| panic!("send {attempt} must succeed: {e:?}"));
    }

    assert_eq!(status_of(&pool, id).await, "completed");
    assert_eq!(
        journal_count(&pool, &execution_id, "WorkflowExecutionCompleted").await,
        1,
        "the workflow ended once"
    );

    cleanup(&pool, id, &execution_id).await;
}

/// A worker whose claim lapsed, and whose workflow was handed to another,
/// cannot apply its completion over the newer activation. It is told its
/// completion was received — a refusal would only make its SDK back off as if
/// the engine were failing — and nothing is applied.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_stale_activation_is_acknowledged_and_applies_nothing() {
    let pool = pool().await;
    let (service, repo) = engine(&pool).await;
    let (id, execution_id, task_queue) = workflow(&pool).await;

    let stale = claim(&repo, &task_queue, WORKER).await;
    lapse_claim(&pool, id).await;
    let current = claim(&repo, &task_queue, OTHER_WORKER).await;

    let answer = service
        .complete_workflow_execution(Request::new(completion(
            id,
            &execution_id,
            Some(token(&execution_id, stale)),
            vec![schedule("task-stale"), complete()],
        )))
        .await
        .expect("the stale activation is acknowledged");
    assert!(answer.into_inner().eager_tasks.is_empty());

    assert_eq!(status_of(&pool, id).await, "running");
    assert_eq!(task_rows(&pool, &execution_id, "task-stale").await, 0);
    assert_eq!(
        journal_count(&pool, &execution_id, "WorkflowExecutionCompleted").await,
        0
    );

    // The current activation's completion goes through.
    service
        .complete_workflow_execution(Request::new(completion(
            id,
            &execution_id,
            Some(token(&execution_id, current)),
            vec![complete()],
        )))
        .await
        .expect("the current activation completes");
    assert_eq!(status_of(&pool, id).await, "completed");

    cleanup(&pool, id, &execution_id).await;
}

/// Copies of one completion arriving together are applied by one of them.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn concurrent_copies_of_a_completion_apply_once() {
    let pool = pool().await;
    let (service, repo) = engine(&pool).await;
    let (id, execution_id, task_queue) = workflow(&pool).await;

    let seq = claim(&repo, &task_queue, WORKER).await;
    let request = completion(
        id,
        &execution_id,
        Some(token(&execution_id, seq)),
        vec![schedule("task-c"), complete()],
    );

    let mut sends = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let service = service.clone();
        let request = request.clone();
        sends.spawn(async move {
            service
                .complete_workflow_execution(Request::new(request))
                .await
        });
    }
    while let Some(sent) = sends.join_next().await {
        sent.expect("join")
            .expect("every copy is answered with success");
    }

    assert_eq!(status_of(&pool, id).await, "completed");
    assert_eq!(task_rows(&pool, &execution_id, "task-c").await, 1);
    assert_eq!(
        journal_count(&pool, &execution_id, "TaskScheduled").await,
        1
    );
    assert_eq!(
        journal_count(&pool, &execution_id, "WorkflowExecutionCompleted").await,
        1
    );

    cleanup(&pool, id, &execution_id).await;
}

/// The token a poll hands out is the one the completion is held to, and it
/// arrives where SDKs return it without knowing what it is.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_polled_activation_token_fences_its_completion() {
    let pool = pool().await;
    let (service, _repo) = engine(&pool).await;
    let (id, execution_id, task_queue) = workflow(&pool).await;

    let polled = service
        .poll_workflow_execution(Request::new(PollWorkflowExecutionRequest {
            task_queue: task_queue.clone(),
            namespace: NAMESPACE.to_string(),
            identity: WORKER.to_string(),
            poll_timeout: Some(orcher_proto::prost_types::Duration {
                seconds: 2,
                nanos: 0,
            }),
            ..Default::default()
        }))
        .await
        .expect("poll")
        .into_inner();
    assert_eq!(polled.execution_id, execution_id);

    let decoded = orcher_core::grpc::activation_token::decode(polled.stream_entry_id.as_bytes())
        .expect("the poll carries an activation token")
        .expect("and it reads");
    let seq: i64 =
        sqlx::query_scalar("SELECT activation_seq FROM workflow_executions WHERE id = $1")
            .bind(id)
            .fetch_one(&pool)
            .await
            .expect("read activation");
    assert_eq!(decoded.execution_id, execution_id);
    assert_eq!(decoded.activation_seq, seq);

    // Returned exactly as the SDK returns it: the token in stream_entry_id.
    let finish = completion(
        id,
        &execution_id,
        Some(polled.stream_entry_id.clone()),
        vec![complete()],
    );
    service
        .complete_workflow_execution(Request::new(finish.clone()))
        .await
        .expect("completes");
    service
        .complete_workflow_execution(Request::new(finish))
        .await
        .expect("and its replay is acknowledged");
    assert_eq!(
        journal_count(&pool, &execution_id, "WorkflowExecutionCompleted").await,
        1
    );

    cleanup(&pool, id, &execution_id).await;
}

/// A completion from a worker that predates activation tokens is applied as
/// it always was — unless the engine is told to require one.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_completion_without_an_activation_token_is_still_accepted() {
    let pool = pool().await;
    let (service, repo) = engine(&pool).await;
    let (id, execution_id, task_queue) = workflow(&pool).await;
    claim(&repo, &task_queue, WORKER).await;

    service
        .complete_workflow_execution(Request::new(completion(
            id,
            &execution_id,
            None,
            vec![complete()],
        )))
        .await
        .expect("a legacy completion is accepted");
    assert_eq!(status_of(&pool, id).await, "completed");
    cleanup(&pool, id, &execution_id).await;

    // Required: refused, and nothing applied.
    let (id, execution_id, task_queue) = workflow(&pool).await;
    claim(&repo, &task_queue, WORKER).await;
    let strict = Arc::try_unwrap(engine(&pool).await.0)
        .ok()
        .expect("sole owner")
        .with_activation_token_required(true);
    let status = strict
        .complete_workflow_execution(Request::new(completion(
            id,
            &execution_id,
            None,
            vec![complete()],
        )))
        .await
        .expect_err("a token is required");
    assert_eq!(status.code(), Code::InvalidArgument, "{status:?}");
    assert_eq!(status_of(&pool, id).await, "running");
    cleanup(&pool, id, &execution_id).await;
}

/// A repeated ScheduleTask with no token to fence it — a legacy worker
/// retrying — does not run a task that has already finished a second time.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_finished_task_is_not_scheduled_again_by_a_repeated_command() {
    let pool = pool().await;
    let (service, repo) = engine(&pool).await;
    let (id, execution_id, task_queue) = workflow(&pool).await;
    claim(&repo, &task_queue, WORKER).await;

    let request = completion(id, &execution_id, None, vec![schedule("task-f")]);
    service
        .complete_workflow_execution(Request::new(request.clone()))
        .await
        .expect("schedules the task");

    // The task runs to completion.
    sqlx::query(
        "UPDATE task_executions SET status = 'completed', started_at = NOW(), \
         completed_at = NOW(), service_identity = 'worker-a' \
         WHERE execution_id = $1::uuid AND task_id = 'task-f'",
    )
    .bind(&execution_id)
    .execute(&pool)
    .await
    .expect("finish the task");

    service
        .complete_workflow_execution(Request::new(request))
        .await
        .expect("the repeat is accepted");

    assert_eq!(
        task_rows(&pool, &execution_id, "task-f").await,
        1,
        "the finished task must not be scheduled again"
    );
    assert_eq!(
        journal_count(&pool, &execution_id, "TaskScheduled").await,
        1
    );

    cleanup(&pool, id, &execution_id).await;
}

/// Completing a workflow that was cancelled meanwhile leaves it cancelled.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn completing_a_cancelled_workflow_leaves_it_cancelled() {
    let pool = pool().await;
    let (service, repo) = engine(&pool).await;
    let (id, execution_id, task_queue) = workflow(&pool).await;
    claim(&repo, &task_queue, WORKER).await;

    sqlx::query(
        "UPDATE workflow_executions SET status = 'cancelled', cancelled_at = NOW(), \
         completed_at = NOW() WHERE id = $1",
    )
    .bind(id)
    .execute(&pool)
    .await
    .expect("cancel");

    service
        .complete_workflow_execution(Request::new(completion(
            id,
            &execution_id,
            None,
            vec![complete()],
        )))
        .await
        .expect("the late completion is answered");

    assert_eq!(status_of(&pool, id).await, "cancelled");
    assert_eq!(
        journal_count(&pool, &execution_id, "WorkflowExecutionCompleted").await,
        0,
        "a cancelled workflow does not also complete"
    );

    cleanup(&pool, id, &execution_id).await;
}

/// Poll as `worker` on `task_queue`, as a worker does, taking a permit.
async fn poll(service: &ExecutionService, task_queue: &str, worker: &str) -> String {
    service
        .poll_workflow_execution(Request::new(PollWorkflowExecutionRequest {
            task_queue: task_queue.to_string(),
            namespace: NAMESPACE.to_string(),
            identity: worker.to_string(),
            poll_timeout: Some(orcher_proto::prost_types::Duration {
                seconds: 2,
                nanos: 0,
            }),
            ..Default::default()
        }))
        .await
        .expect("poll")
        .into_inner()
        .stream_entry_id
}

fn permits_used(service: &ExecutionService, task_queue: &str, worker: &str) -> usize {
    service
        .capacity_registry()
        .get(task_queue, NAMESPACE, worker)
        .map(|permits| permits.used())
        .unwrap_or(0)
}

/// A stale completion returns the permit its own activation took, and not the
/// one the newer activation took for the same execution.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_stale_completion_returns_its_own_permit_and_no_other() {
    let pool = pool().await;
    let (service, _repo) = engine(&pool).await;
    let (id, execution_id, task_queue) = workflow(&pool).await;

    let stale = poll(&service, &task_queue, WORKER).await;
    assert_eq!(permits_used(&service, &task_queue, WORKER), 1);
    lapse_claim(&pool, id).await;
    let _current = poll(&service, &task_queue, OTHER_WORKER).await;
    assert_eq!(permits_used(&service, &task_queue, OTHER_WORKER), 1);

    service
        .complete_workflow_execution(Request::new(completion(
            id,
            &execution_id,
            Some(stale),
            vec![complete()],
        )))
        .await
        .expect("acknowledged");

    assert_eq!(
        permits_used(&service, &task_queue, WORKER),
        0,
        "the stale worker gets its capacity back"
    );
    assert_eq!(
        permits_used(&service, &task_queue, OTHER_WORKER),
        1,
        "the current activation keeps its permit"
    );
    assert_eq!(status_of(&pool, id).await, "running");

    cleanup(&pool, id, &execution_id).await;
}

/// A token for an execution that does not exist is not a reason to apply the
/// commands unfenced.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_token_for_a_missing_execution_is_refused() {
    let pool = pool().await;
    let (service, _repo) = engine(&pool).await;
    let id = Uuid::new_v4();
    let execution_id = Uuid::new_v4().to_string();

    let status = service
        .complete_workflow_execution(Request::new(completion(
            id,
            &execution_id,
            Some(token(&execution_id, 1)),
            vec![schedule("task-missing")],
        )))
        .await
        .expect_err("no such execution");
    assert_eq!(status.code(), Code::NotFound, "{status:?}");
    assert_eq!(task_rows(&pool, &execution_id, "task-missing").await, 0);
}

/// Nor is a completion or failure without a token: there is no activation to
/// fence it with, and applying it anyway wrote task rows and journal entries
/// for a workflow the table had no record of.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_tokenless_completion_or_failure_of_a_missing_execution_is_refused() {
    let pool = pool().await;
    let (service, _repo) = engine(&pool).await;
    let id = Uuid::new_v4();
    let execution_id = Uuid::new_v4().to_string();

    let status = service
        .complete_workflow_execution(Request::new(completion(
            id,
            &execution_id,
            None,
            vec![schedule("task-missing"), complete()],
        )))
        .await
        .expect_err("no such execution");
    assert_eq!(status.code(), Code::NotFound, "{status:?}");
    assert_eq!(task_rows(&pool, &execution_id, "task-missing").await, 0);

    let status = service
        .fail_workflow_execution(Request::new(FailWorkflowExecutionRequest {
            workflow_id: id.to_string(),
            execution_id: execution_id.clone(),
            namespace: NAMESPACE.to_string(),
            identity: WORKER.to_string(),
            task_token: format!("{id}:{execution_id}").into_bytes(),
            failure: Some(orcher_proto::orcher::v1::Failure {
                message: "boom".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        }))
        .await
        .expect_err("no such execution");
    assert_eq!(status.code(), Code::NotFound, "{status:?}");

    let written: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM workflow_execution_journal WHERE execution_id = $1",
    )
    .bind(&execution_id)
    .fetch_one(&pool)
    .await
    .expect("count journal entries");
    assert_eq!(written, 0, "nothing is journaled");
}

/// Requiring tokens does not refuse the failure reports SDKs send today, which
/// carry a token of their own making and have nowhere else to put a real one.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn requiring_tokens_still_accepts_failure_reports_without_one() {
    let pool = pool().await;
    let (id, execution_id, task_queue) = workflow(&pool).await;
    let (_, repo) = engine(&pool).await;
    claim(&repo, &task_queue, WORKER).await;
    let strict = Arc::try_unwrap(engine(&pool).await.0)
        .ok()
        .expect("sole owner")
        .with_activation_token_required(true);

    strict
        .fail_workflow_execution(Request::new(FailWorkflowExecutionRequest {
            workflow_id: id.to_string(),
            execution_id: execution_id.clone(),
            namespace: NAMESPACE.to_string(),
            identity: WORKER.to_string(),
            task_token: format!("{id}:{execution_id}").into_bytes(),
            failure: Some(orcher_proto::orcher::v1::Failure {
                message: "boom".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        }))
        .await
        .expect("a failure report without a token is accepted");
    assert_eq!(status_of(&pool, id).await, "failed");

    cleanup(&pool, id, &execution_id).await;
}

async fn claim_state(pool: &PgPool, id: Uuid) -> (Option<String>, bool) {
    sqlx::query_as("SELECT claimed_by, pending_requeue FROM workflow_executions WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("read claim")
}

/// A wake-up — a task finishing, a child's outcome, an event — that arrives
/// while an activation is out does not hand the workflow to a second worker
/// alongside it. The claim stays with the activation, and its completion
/// releases it.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_wake_up_during_an_activation_waits_for_its_completion() {
    use orcher_storage::repositories::domains::workflow_execution::Resume;
    let pool = pool().await;
    let (service, repo) = engine(&pool).await;
    let (id, execution_id, task_queue) = workflow(&pool).await;

    let seq = claim(&repo, &task_queue, WORKER).await;
    assert_eq!(
        repo.release_claim_to_resume(&execution_id).await.unwrap(),
        Resume::Deferred,
        "the activation is still out"
    );
    assert_eq!(
        claim_state(&pool, id).await,
        (Some(WORKER.to_string()), true),
        "claim kept, wake-up recorded"
    );
    assert!(
        repo.claim_and_get_next(&task_queue, NAMESPACE, OTHER_WORKER, 300, None, None)
            .await
            .unwrap()
            .is_none(),
        "no second activation while the first is out"
    );

    // The activation completes, suspending on a task: the wake-up it missed
    // is taken, and the workflow is claimable again.
    service
        .complete_workflow_execution(Request::new(completion(
            id,
            &execution_id,
            Some(token(&execution_id, seq)),
            vec![schedule("task-w")],
        )))
        .await
        .expect("completes");
    assert_eq!(claim_state(&pool, id).await, (None, false));
    let next = claim(&repo, &task_queue, OTHER_WORKER).await;
    assert_eq!(next, seq + 1);

    // With nothing out, a wake-up releases the claim at once — here, that of
    // the activation just claimed, once it has completed.
    service
        .complete_workflow_execution(Request::new(completion(
            id,
            &execution_id,
            Some(token(&execution_id, next)),
            vec![],
        )))
        .await
        .expect("parks");
    assert_eq!(
        repo.release_claim_to_resume(&execution_id).await.unwrap(),
        Resume::Released
    );
    assert_eq!(claim_state(&pool, id).await, (None, false));

    cleanup(&pool, id, &execution_id).await;
}

/// A completion whose first attempt recorded itself and was then cut off —
/// its caller hung up before the claim and permit went back — has what it
/// missed done when it is sent again.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_replayed_completion_finishes_what_the_first_attempt_did_not() {
    let pool = pool().await;
    let (service, _repo) = engine(&pool).await;
    let (id, execution_id, task_queue) = workflow(&pool).await;

    let polled = poll(&service, &task_queue, WORKER).await;
    let seq = orcher_core::grpc::activation_token::decode(polled.as_bytes())
        .unwrap()
        .unwrap()
        .activation_seq;
    assert_eq!(permits_used(&service, &task_queue, WORKER), 1);

    // The first attempt applied and recorded itself, and got no further.
    sqlx::query(
        "UPDATE workflow_executions SET status = 'completed', completed_at = NOW(), \
         completed_activation = $2 WHERE id = $1",
    )
    .bind(id)
    .bind(seq)
    .execute(&pool)
    .await
    .expect("half-finished first attempt");

    service
        .complete_workflow_execution(Request::new(completion(
            id,
            &execution_id,
            Some(polled),
            vec![complete()],
        )))
        .await
        .expect("the replay is acknowledged");

    assert_eq!(claim_state(&pool, id).await.0, None, "claim released");
    assert_eq!(
        permits_used(&service, &task_queue, WORKER),
        0,
        "permit returned"
    );
    assert_eq!(
        journal_count(&pool, &execution_id, "WorkflowExecutionCompleted").await,
        0,
        "and the commands were still not applied again"
    );

    cleanup(&pool, id, &execution_id).await;
}

fn unknown_command() -> Command {
    Command {
        command_type: 9_999,
        attributes: None,
    }
}

/// A completion that fails after it was admitted gives its activation up:
/// the claim goes back, so the workflow is woken and handed out again at once
/// rather than every wake-up waiting on an activation nothing will complete.
/// The same completion sent again is still applied while nothing newer has
/// been handed out.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_failed_completion_gives_its_activation_up() {
    use orcher_storage::repositories::domains::workflow_execution::Resume;
    let pool = pool().await;
    let (service, repo) = engine(&pool).await;
    let (id, execution_id, task_queue) = workflow(&pool).await;

    let seq = claim(&repo, &task_queue, WORKER).await;
    let status = service
        .complete_workflow_execution(Request::new(completion(
            id,
            &execution_id,
            Some(token(&execution_id, seq)),
            vec![unknown_command()],
        )))
        .await
        .expect_err("an unknown command fails the completion");
    assert_eq!(status.code(), Code::Internal, "{status:?}");

    assert_eq!(
        claim_state(&pool, id).await,
        (None, false),
        "the failed activation's claim is given back"
    );
    assert_eq!(
        repo.release_claim_to_resume(&execution_id).await.unwrap(),
        Resume::Released,
        "a wake-up is not held for the failed activation"
    );

    // Retried, with nothing handed out since: applied.
    service
        .complete_workflow_execution(Request::new(completion(
            id,
            &execution_id,
            Some(token(&execution_id, seq)),
            vec![complete()],
        )))
        .await
        .expect("the retry is applied");
    assert_eq!(status_of(&pool, id).await, "completed");

    cleanup(&pool, id, &execution_id).await;
}

/// A completion without a token, sent by a worker whose claim has since gone
/// to another, answers the older activation: it is acknowledged and not
/// applied, and the newer activation's own completion is not mistaken for a
/// repeat of it. One sent by the claim holder — which polls under the name
/// sdk-core derives from the one it completes under — is applied.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_late_tokenless_completion_does_not_land_on_the_newer_activation() {
    let pool = pool().await;
    let (service, repo) = engine(&pool).await;
    let (id, execution_id, task_queue) = workflow(&pool).await;

    claim(&repo, &task_queue, &format!("{WORKER}-wf-driver-poller-0")).await;
    lapse_claim(&pool, id).await;
    let current = claim(
        &repo,
        &task_queue,
        &format!("{OTHER_WORKER}-wf-driver-poller-1"),
    )
    .await;

    // worker-a, whose claim lapsed, completes without a token.
    service
        .complete_workflow_execution(Request::new(completion(
            id,
            &execution_id,
            None,
            vec![complete()],
        )))
        .await
        .expect("acknowledged");
    assert_eq!(status_of(&pool, id).await, "running", "not applied");

    // worker-b's completion of the current activation is applied.
    let mut own = completion(
        id,
        &execution_id,
        Some(token(&execution_id, current)),
        vec![complete()],
    );
    own.identity = OTHER_WORKER.to_string();
    service
        .complete_workflow_execution(Request::new(own))
        .await
        .expect("applied");
    assert_eq!(status_of(&pool, id).await, "completed");
    cleanup(&pool, id, &execution_id).await;

    // A tokenless completion from the claim holder is applied.
    let (id, execution_id, task_queue) = workflow(&pool).await;
    claim(&repo, &task_queue, &format!("{WORKER}-wf-driver-poller-3")).await;
    service
        .complete_workflow_execution(Request::new(completion(
            id,
            &execution_id,
            None,
            vec![complete()],
        )))
        .await
        .expect("applied");
    assert_eq!(status_of(&pool, id).await, "completed");
    cleanup(&pool, id, &execution_id).await;
}

fn start_timer(timer_id: &str) -> Command {
    Command {
        command_type: CommandType::StartTimer as i32,
        attributes: Some(command::Attributes::StartTimer(
            orcher_proto::orcher::v1::StartTimerCommandAttributes {
                timer_id: timer_id.to_string(),
                start_to_fire_timeout: Some(orcher_proto::prost_types::Duration {
                    seconds: 3600,
                    nanos: 0,
                }),
            },
        )),
    }
}

/// A timer scheduled by an attempt that failed before journaling it is
/// journaled by the next replay, once.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_timer_missing_from_the_journal_is_journaled_on_replay() {
    let pool = pool().await;
    let (service, repo) = engine(&pool).await;
    let (id, execution_id, task_queue) = workflow(&pool).await;

    let first = claim(&repo, &task_queue, WORKER).await;
    service
        .complete_workflow_execution(Request::new(completion(
            id,
            &execution_id,
            Some(token(&execution_id, first)),
            vec![start_timer("nap")],
        )))
        .await
        .expect("starts the timer");
    // The attempt that scheduled it got no further than the timer row.
    sqlx::query(
        "DELETE FROM workflow_execution_journal WHERE execution_id = $1 \
         AND event_type = 'TimerStarted'",
    )
    .bind(&execution_id)
    .execute(&pool)
    .await
    .expect("lose the journal entry");

    for _ in 0..2 {
        lapse_claim(&pool, id).await;
        let replay = claim(&repo, &task_queue, WORKER).await;
        service
            .complete_workflow_execution(Request::new(completion(
                id,
                &execution_id,
                Some(token(&execution_id, replay)),
                vec![start_timer("nap")],
            )))
            .await
            .expect("replays");
    }
    assert_eq!(
        journal_count(&pool, &execution_id, "TimerStarted").await,
        1,
        "journaled again, and only once"
    );

    let _ = sqlx::query("DELETE FROM timer_queue WHERE workflow_id = $1")
        .bind(id)
        .execute(&pool)
        .await;
    cleanup(&pool, id, &execution_id).await;
}

/// An activation claimed long ago is not waited on: an engine from before
/// activations were recorded, or a worker that died, would otherwise hold
/// every wake-up until the claim timed out.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_wake_up_does_not_wait_on_an_old_activation() {
    use orcher_storage::repositories::domains::workflow_execution::Resume;
    let pool = pool().await;
    let (_service, repo) = engine(&pool).await;
    let (id, execution_id, task_queue) = workflow(&pool).await;

    claim(&repo, &task_queue, WORKER).await;
    sqlx::query(
        "UPDATE workflow_executions SET claimed_at = NOW() - INTERVAL '2 minutes' WHERE id = $1",
    )
    .bind(id)
    .execute(&pool)
    .await
    .expect("age the claim");

    assert_eq!(
        repo.release_claim_to_resume(&execution_id).await.unwrap(),
        Resume::Released
    );
    assert_eq!(claim_state(&pool, id).await, (None, false));

    cleanup(&pool, id, &execution_id).await;
}

/// A prefetched activation handed out from the buffer starts its claim when it
/// is handed out, not when its batch was claimed: an immediate wake-up waits
/// for it instead of handing the workflow to a second worker. One whose claim
/// expired in the buffer is not handed out.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_prefetched_activation_is_claimed_afresh_when_handed_out() {
    use orcher_storage::repositories::domains::workflow_execution::Resume;
    let pool = pool().await;
    let (_service, repo) = engine(&pool).await;
    let (id, execution_id, task_queue) = workflow(&pool).await;

    let seq = claim(&repo, &task_queue, "refiller-wf-driver-poller-0").await;
    // It sat in the buffer for a minute and a half.
    sqlx::query(
        "UPDATE workflow_executions SET claimed_at = NOW() - INTERVAL '90 seconds' WHERE id = $1",
    )
    .bind(id)
    .execute(&pool)
    .await
    .expect("age the batch claim");

    assert_eq!(
        repo.hand_to(
            &execution_id,
            seq,
            "taker-wf-driver-poller-1",
            300,
            None,
            None
        )
        .await
        .unwrap(),
        HandOut::Handed
    );
    assert_eq!(
        repo.release_claim_to_resume(&execution_id).await.unwrap(),
        Resume::Deferred,
        "the activation just handed out is in flight"
    );

    // Expired in the buffer: not handed out.
    sqlx::query("UPDATE workflow_executions SET claim_timeout_at = NOW() - INTERVAL '1 second' WHERE id = $1")
        .bind(id)
        .execute(&pool)
        .await
        .expect("expire the claim");
    assert_eq!(
        repo.hand_to(
            &execution_id,
            seq,
            "taker-wf-driver-poller-2",
            300,
            None,
            None
        )
        .await
        .unwrap(),
        HandOut::Gone
    );

    cleanup(&pool, id, &execution_id).await;
}

/// A tokenless completion from a worker whose claim went to another returns
/// that worker's own permit for the execution, and not the other's.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_tokenless_stale_completion_returns_only_its_senders_permit() {
    let pool = pool().await;
    let (service, _repo) = engine(&pool).await;
    let (id, execution_id, task_queue) = workflow(&pool).await;
    let poller_a = format!("{WORKER}-wf-driver-poller-0");
    let poller_b = format!("{OTHER_WORKER}-wf-driver-poller-0");

    poll(&service, &task_queue, &poller_a).await;
    lapse_claim(&pool, id).await;
    poll(&service, &task_queue, &poller_b).await;
    assert_eq!(permits_used(&service, &task_queue, &poller_a), 1);
    assert_eq!(permits_used(&service, &task_queue, &poller_b), 1);

    service
        .complete_workflow_execution(Request::new(completion(
            id,
            &execution_id,
            None,
            vec![complete()],
        )))
        .await
        .expect("acknowledged");

    assert_eq!(status_of(&pool, id).await, "running");
    assert_eq!(permits_used(&service, &task_queue, &poller_a), 0);
    assert_eq!(permits_used(&service, &task_queue, &poller_b), 1);

    cleanup(&pool, id, &execution_id).await;
}
