//! A worker that stops can say so: it hands back what it will not run, and it
//! stops being given anything new.
//!
//! Before these, a worker shutting down had no way to do either. An activation
//! it had received but would not run stayed claimed until the claim timed out,
//! five minutes by default, and the workflow waited with it. Its idle pollers
//! kept long-polling until the SDK's shutdown grace ran out, and each one that
//! was handed work in that time took it into a process that was going away.
//!
//! `ReleaseWorkflowExecution` gives an activation back through the same fence
//! as a completion, so it acts only on the activation that is current and not
//! yet applied, and only on an execution in the caller's organization and
//! namespace. `ShutdownWorker` answers the worker's open polls at once and
//! refuses it work afterwards, on every replica sharing the database; only the
//! process it names, when it names one, and only on the queues the caller's
//! credential may poll.
//!
//! Requires PostgreSQL. The schema is created by the engine's own migration
//! runner. Run with:
//!
//! ```bash
//! export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/orcher_test"
//! cargo test -p orcher-core --test worker_lifecycle -- --ignored --test-threads=1
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};

use orcher_core::engine::dispatch::{PollingManager, TaskPollingManager};
use orcher_core::engine::queues::outbox::OutboxQueue;
use orcher_core::engine::WorkflowCoordinator;
use orcher_core::events::EventBus;
use orcher_core::grpc::service_auth::{ServiceIdentity, ServicePermissions};
use orcher_core::grpc::services::ExecutionService;
use orcher_core::grpc::tenant::ORGANIZATION_METADATA_KEY;
use orcher_core::workflows::{
    ChildWorkflowManager, CommandProcessor, JournalService, WorkflowExecutionService,
};
use orcher_events::bus::EventBusConfig;
use orcher_proto::orcher::v1::execution_service_server::ExecutionService as ExecutionServiceTrait;
use orcher_proto::orcher::v1::{
    command, Command, CommandType, CompleteWorkflowCommandAttributes,
    CompleteWorkflowExecutionRequest, PollTaskExecutionRequest, PollWorkflowExecutionRequest,
    PollWorkflowExecutionResponse, ReleaseWorkflowExecutionRequest, ShutdownWorkerRequest,
    StartTimerCommandAttributes,
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

/// One engine replica on `pool`: its poll, completion and lifecycle paths,
/// for workflows and tasks alike.
async fn engine(pool: &PgPool) -> Arc<ExecutionService> {
    engine_and_polling(pool).await.0
}

/// An engine replica, and the polling manager its workflow polls wait in.
async fn engine_and_polling(pool: &PgPool) -> (Arc<ExecutionService>, Arc<PollingManager>) {
    let (service, polling, _) = engine_holding(
        pool,
        orcher_storage::repositories::domains::workflow_execution::DEFAULT_JOURNAL_HOLD,
    )
    .await;
    (service, polling)
}

/// An engine that holds a run released to resume for `hold` for the worker
/// process that keeps its journal, and its execution repository.
async fn engine_holding(
    pool: &PgPool,
    hold: Duration,
) -> (
    Arc<ExecutionService>,
    Arc<PollingManager>,
    Arc<WorkflowExecutionRepository>,
) {
    let journal_repo = Arc::new(JournalRepository::new(pool.clone()));
    let journal_service = Arc::new(JournalService::new(journal_repo.clone()));
    let child_repo = Arc::new(ChildWorkflowRepository::new(pool.clone()));
    let exec_repo = Arc::new(WorkflowExecutionRepository::new(pool.clone()));
    exec_repo.set_journal_holder_hold(hold);
    let outbox_repo = Arc::new(OutboxRepository::new(Arc::new(pool.clone())));
    let outbox_queue = Arc::new(OutboxQueue::new(outbox_repo));
    let event_bus = EventBus::new(EventBusConfig::default());
    let execution_log_repo = Arc::new(PostgresExecutionLogRepository::new(pool.clone()));
    let task_execution_repo = Arc::new(TaskExecutionRepository::new(pool.clone()));
    let workflow_execution_service = Arc::new(WorkflowExecutionService::new(
        exec_repo.clone(),
        journal_service.clone(),
        outbox_queue.clone(),
        event_bus.clone(),
        pool.clone(),
    ));
    let child_workflow_manager = Arc::new(ChildWorkflowManager::new(
        child_repo,
        journal_service.clone(),
        workflow_execution_service,
    ));
    let polling_manager = Arc::new(
        PollingManager::new(exec_repo.clone(), journal_repo.clone(), 300)
            .with_journal_holder_hold(hold),
    );
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
        polling_manager.clone(),
        execution_log_repo as Arc<dyn ExecutionLogRepository>,
        journal_repo,
        exec_repo.clone(),
        task_execution_repo,
        pool.clone(),
    )
    .with_task_polling_manager(Arc::new(TaskPollingManager::new(outbox_queue)));
    (Arc::new(service), polling_manager, exec_repo)
}

/// A running workflow, unclaimed, on `task_queue`.
async fn workflow_on(pool: &PgPool, task_queue: &str) -> (Uuid, String) {
    workflow_of(pool, task_queue, None).await
}

/// A running workflow, unclaimed, on `task_queue`, owned by `organization`.
async fn workflow_of(
    pool: &PgPool,
    task_queue: &str,
    organization: Option<Uuid>,
) -> (Uuid, String) {
    let id = Uuid::new_v4();
    let execution_id = Uuid::new_v4().to_string();
    sqlx::query(
        r#"
        INSERT INTO workflow_executions (
            id, execution_id, workflow_id, workflow_name, workflow_version, namespace,
            task_queue, status, parameters, context, environment, progress,
            total_steps, completed_steps, failed_steps, retried_steps, organization_id
        )
        VALUES ($1, $2, $1, 'worker-lifecycle', '1.0.0', $3, $4, 'running',
                '{}', '{}', '{}', '{}', 0, 0, 0, 0, $5)
        "#,
    )
    .bind(id)
    .bind(&execution_id)
    .bind(NAMESPACE)
    .bind(task_queue)
    .bind(organization)
    .execute(pool)
    .await
    .expect("insert workflow");
    (id, execution_id)
}

/// A new organization, and the user that owns it.
async fn organization(pool: &PgPool) -> Uuid {
    let owner = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO users (id, email, password_hash, hash_algorithm, password_changed_at) \
         VALUES ($1, $2, 'x', 'argon2', NOW())",
    )
    .bind(owner)
    .bind(format!("owner-{owner}@worker-lifecycle.test"))
    .execute(pool)
    .await
    .expect("seed user");
    let organization = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO organizations (id, name, display_name, slug, owner_id) \
         VALUES ($1, $2, $2, $2, $3)",
    )
    .bind(organization)
    .bind(format!("org-{organization}"))
    .bind(owner)
    .execute(pool)
    .await
    .expect("seed organization");
    organization
}

async fn forget_organization(pool: &PgPool, organization: Uuid) {
    let owner: Option<Uuid> =
        sqlx::query_scalar("SELECT owner_id FROM organizations WHERE id = $1")
            .bind(organization)
            .fetch_optional(pool)
            .await
            .ok()
            .flatten();
    let _ = sqlx::query("DELETE FROM organizations WHERE id = $1")
        .bind(organization)
        .execute(pool)
        .await;
    if let Some(owner) = owner {
        let _ = sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(owner)
            .execute(pool)
            .await;
    }
}

/// `request`, sent as `organization`, as the auth layer leaves it once it has
/// established who the caller acts for.
fn acting_for<T>(organization: Uuid, mut request: Request<T>) -> Request<T> {
    request.metadata_mut().insert(
        ORGANIZATION_METADATA_KEY,
        organization.to_string().parse().unwrap(),
    );
    request
}

/// A task queue nothing else uses.
fn queue() -> String {
    format!("lifecycle-{}", Uuid::new_v4().simple())
}

/// A worker identity nothing else uses, and the name sdk-core's first
/// workflow poller sends for it.
fn worker() -> (String, String) {
    let identity = format!("worker-{}", Uuid::new_v4().simple());
    let poller = format!("{identity}-wf-driver-poller-0");
    (identity, poller)
}

async fn poll_for(
    service: &ExecutionService,
    task_queue: &str,
    poller: &str,
    timeout_secs: i64,
) -> PollWorkflowExecutionResponse {
    poll_from(service, task_queue, poller, "", timeout_secs).await
}

/// A poll from the process `instance` of a worker.
async fn poll_from(
    service: &ExecutionService,
    task_queue: &str,
    poller: &str,
    instance: &str,
    timeout_secs: i64,
) -> PollWorkflowExecutionResponse {
    service
        .poll_workflow_execution(Request::new(PollWorkflowExecutionRequest {
            task_queue: task_queue.to_string(),
            namespace: NAMESPACE.to_string(),
            identity: poller.to_string(),
            poll_timeout: Some(orcher_proto::prost_types::Duration {
                seconds: timeout_secs,
                nanos: 0,
            }),
            worker_instance_id: instance.to_string(),
            ..Default::default()
        }))
        .await
        .expect("poll")
        .into_inner()
}

fn release(
    id: Uuid,
    execution_id: &str,
    identity: &str,
    token: &str,
) -> Request<ReleaseWorkflowExecutionRequest> {
    Request::new(ReleaseWorkflowExecutionRequest {
        workflow_id: id.to_string(),
        execution_id: execution_id.to_string(),
        namespace: NAMESPACE.to_string(),
        identity: identity.to_string(),
        task_token: token.as_bytes().to_vec(),
        ..Default::default()
    })
}

fn shutdown(identity: &str) -> Request<ShutdownWorkerRequest> {
    Request::new(ShutdownWorkerRequest {
        namespace: NAMESPACE.to_string(),
        identity: identity.to_string(),
        ..Default::default()
    })
}

/// A completion of the activation `token` names that parks the workflow on a
/// timer, which keeps its claim until the timer fires.
fn park_on_a_timer(
    id: Uuid,
    execution_id: &str,
    identity: &str,
    token: &str,
) -> Request<CompleteWorkflowExecutionRequest> {
    Request::new(CompleteWorkflowExecutionRequest {
        workflow_id: id.to_string(),
        execution_id: execution_id.to_string(),
        namespace: NAMESPACE.to_string(),
        identity: identity.to_string(),
        stream_entry_id: token.to_string(),
        commands: vec![Command {
            command_type: CommandType::StartTimer as i32,
            attributes: Some(command::Attributes::StartTimer(
                StartTimerCommandAttributes {
                    timer_id: "an-hour".to_string(),
                    start_to_fire_timeout: Some(orcher_proto::prost_types::Duration {
                        seconds: 3600,
                        nanos: 0,
                    }),
                },
            )),
        }],
        ..Default::default()
    })
}

/// A completion that ends the workflow, sent without a token as the SDKs
/// that predate tokens send it.
fn finish(
    id: Uuid,
    execution_id: &str,
    identity: &str,
) -> Request<CompleteWorkflowExecutionRequest> {
    Request::new(CompleteWorkflowExecutionRequest {
        workflow_id: id.to_string(),
        execution_id: execution_id.to_string(),
        namespace: NAMESPACE.to_string(),
        identity: identity.to_string(),
        commands: vec![Command {
            command_type: CommandType::CompleteWorkflow as i32,
            attributes: Some(command::Attributes::CompleteWorkflow(
                CompleteWorkflowCommandAttributes {
                    result: br#"{"ok":true}"#.to_vec(),
                },
            )),
        }],
        ..Default::default()
    })
}

async fn status_of(pool: &PgPool, id: Uuid) -> String {
    sqlx::query_scalar("SELECT status FROM workflow_executions WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("read the status")
}

/// Who holds the claim, which activation is current, and which was applied.
async fn activation_state(pool: &PgPool, id: Uuid) -> (Option<String>, i64, Option<i64>) {
    sqlx::query_as(
        "SELECT claimed_by, activation_seq, completed_activation \
         FROM workflow_executions WHERE id = $1",
    )
    .bind(id)
    .fetch_one(pool)
    .await
    .expect("read the activation state")
}

async fn journal_entries(pool: &PgPool, execution_id: &str) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM workflow_execution_journal WHERE execution_id = $1")
        .bind(execution_id)
        .fetch_one(pool)
        .await
        .expect("count journal entries")
}

async fn cleanup(pool: &PgPool, id: Uuid, execution_id: &str) {
    let _ = sqlx::query("DELETE FROM timer_queue WHERE workflow_id = $1")
        .bind(id)
        .execute(pool)
        .await;
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
            "DELETE FROM workflow_executions WHERE id = $1::uuid",
            id.to_string(),
        ),
    ] {
        let _ = sqlx::query(q).bind(key).execute(pool).await;
    }
}

async fn forget_shutdown(pool: &PgPool, identity: &str) {
    let _ = sqlx::query("DELETE FROM worker_shutdowns WHERE identity = $1")
        .bind(identity)
        .execute(pool)
        .await;
}

/// A released activation's claim is given up, nothing is recorded against
/// it, and the workflow goes to the next poll straight away — not after the
/// five-minute claim timeout, which is where it went before.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_released_activation_goes_straight_to_another_poll() {
    let pool = pool().await;
    let service = engine(&pool).await;
    let task_queue = queue();
    let (id, execution_id) = workflow_on(&pool, &task_queue).await;
    let (a, a_poller) = worker();
    let (_, b_poller) = worker();

    let handed = poll_for(&service, &task_queue, &a_poller, 2).await;
    assert_eq!(handed.execution_id, execution_id, "worker a is handed it");
    let journal_before = journal_entries(&pool, &execution_id).await;

    service
        .release_workflow_execution(release(id, &execution_id, &a, &handed.stream_entry_id))
        .await
        .expect("the release is acknowledged");

    let (claimed_by, current, applied) = activation_state(&pool, id).await;
    assert_eq!(claimed_by, None, "the claim is given up");
    assert_eq!(current, 1, "no activation is handed out by the release");
    assert_eq!(applied, None, "nothing is recorded as applied");
    assert_eq!(
        journal_entries(&pool, &execution_id).await,
        journal_before,
        "and nothing is journaled"
    );

    let started = Instant::now();
    let redelivered = poll_for(&service, &task_queue, &b_poller, 5).await;
    assert_eq!(
        redelivered.execution_id, execution_id,
        "worker b is handed it"
    );
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "promptly: {:?}",
        started.elapsed()
    );
    assert_eq!(activation_state(&pool, id).await.1, 2);

    cleanup(&pool, id, &execution_id).await;
}

/// A release sent again, one for an activation handed out again since, and
/// one for an activation already completed are all acknowledged and change
/// nothing — in particular, none of them takes the workflow back from the
/// worker that has it now.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_repeated_stale_or_completed_release_changes_nothing() {
    let pool = pool().await;
    let service = engine(&pool).await;
    let task_queue = queue();
    let (id, execution_id) = workflow_on(&pool, &task_queue).await;
    let (a, a_poller) = worker();
    let (b, b_poller) = worker();

    let first = poll_for(&service, &task_queue, &a_poller, 2).await;
    service
        .release_workflow_execution(release(id, &execution_id, &a, &first.stream_entry_id))
        .await
        .expect("released");
    let second = poll_for(&service, &task_queue, &b_poller, 2).await;
    assert_eq!(second.execution_id, execution_id);
    let held_by_b = activation_state(&pool, id).await;
    assert_eq!(held_by_b.0.as_deref(), Some(b_poller.as_str()));

    // The first release again, after its reply was lost: the activation it
    // names has been handed out again since.
    service
        .release_workflow_execution(release(id, &execution_id, &a, &first.stream_entry_id))
        .await
        .expect("a repeated release is acknowledged");
    assert_eq!(
        activation_state(&pool, id).await,
        held_by_b,
        "changes nothing"
    );

    // Worker b completes its activation (parking the workflow, which keeps
    // the claim), and then releases it as well: already applied.
    service
        .complete_workflow_execution(Request::new(CompleteWorkflowExecutionRequest {
            workflow_id: id.to_string(),
            execution_id: execution_id.clone(),
            namespace: NAMESPACE.to_string(),
            identity: b.clone(),
            stream_entry_id: second.stream_entry_id.clone(),
            ..Default::default()
        }))
        .await
        .expect("completed");
    let completed = activation_state(&pool, id).await;
    assert_eq!(completed.2, Some(2), "the completion is recorded");
    service
        .release_workflow_execution(release(id, &execution_id, &b, &second.stream_entry_id))
        .await
        .expect("a release after the completion is acknowledged");
    assert_eq!(
        activation_state(&pool, id).await,
        completed,
        "and changes nothing"
    );

    cleanup(&pool, id, &execution_id).await;
}

/// Without a token, only the worker holding the claim can release it. A
/// release from any other worker is acknowledged and ignored.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_tokenless_release_is_honoured_only_from_the_claim_holder() {
    let pool = pool().await;
    let service = engine(&pool).await;
    let task_queue = queue();
    let (id, execution_id) = workflow_on(&pool, &task_queue).await;
    let (a, a_poller) = worker();
    let (b, _) = worker();

    poll_for(&service, &task_queue, &a_poller, 2).await;

    service
        .release_workflow_execution(release(id, &execution_id, &b, ""))
        .await
        .expect("a release from another worker is acknowledged");
    assert_eq!(
        activation_state(&pool, id).await.0.as_deref(),
        Some(a_poller.as_str()),
        "and ignored"
    );

    service
        .release_workflow_execution(release(id, &execution_id, &a, ""))
        .await
        .expect("a release from the holder is acknowledged");
    assert_eq!(
        activation_state(&pool, id).await.0,
        None,
        "and gives the claim up"
    );

    cleanup(&pool, id, &execution_id).await;
}

/// A worker's open polls end as soon as it says it is shutting down, and its
/// later polls claim nothing, while another worker's polls are unaffected.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_shut_down_worker_is_answered_at_once_and_given_nothing() {
    let pool = pool().await;
    let service = engine(&pool).await;
    let task_queue = queue();
    let (a, a_poller) = worker();
    let (_, b_poller) = worker();
    let a_task_poller = format!("{a}-task-driver-poller-0");

    // Open long polls, for workflows and for tasks, on an empty queue.
    let open_workflow_poll = {
        let (service, task_queue, poller) = (service.clone(), task_queue.clone(), a_poller.clone());
        tokio::spawn(async move { poll_for(&service, &task_queue, &poller, 20).await })
    };
    let open_task_poll = {
        let (service, task_queue, poller) =
            (service.clone(), task_queue.clone(), a_task_poller.clone());
        tokio::spawn(async move {
            service
                .poll_task_execution(Request::new(PollTaskExecutionRequest {
                    task_queue,
                    namespace: NAMESPACE.to_string(),
                    identity: poller,
                    poll_timeout: Some(orcher_proto::prost_types::Duration {
                        seconds: 20,
                        nanos: 0,
                    }),
                    ..Default::default()
                }))
                .await
                .expect("task poll")
                .into_inner()
        })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;

    let shut_down_at = Instant::now();
    service
        .shutdown_worker(shutdown(&a))
        .await
        .expect("shutdown acknowledged");
    service
        .shutdown_worker(shutdown(&a))
        .await
        .expect("and acknowledged again");

    let answered = tokio::time::timeout(Duration::from_secs(1), open_workflow_poll)
        .await
        .expect("the open workflow poll is answered within a second")
        .unwrap();
    assert!(answered.execution_id.is_empty(), "with no work");
    let answered = tokio::time::timeout(Duration::from_secs(1), open_task_poll)
        .await
        .expect("the open task poll is answered within a second")
        .unwrap();
    assert!(answered.task_id.is_empty(), "with no task");
    assert!(shut_down_at.elapsed() < Duration::from_secs(1));

    // Work arrives: the shut-down worker's poll claims none of it.
    let (id, execution_id) = workflow_on(&pool, &task_queue).await;
    let refused = poll_for(&service, &task_queue, &a_poller, 1).await;
    assert!(
        refused.execution_id.is_empty(),
        "nothing for the shut-down worker"
    );
    assert_eq!(
        activation_state(&pool, id).await.0,
        None,
        "and nothing claimed on its behalf"
    );

    // Another worker is served as before.
    let served = poll_for(&service, &task_queue, &b_poller, 2).await;
    assert_eq!(served.execution_id, execution_id);

    cleanup(&pool, id, &execution_id).await;
    forget_shutdown(&pool, &a).await;
}

/// A shutdown taken by one replica ends a poll waiting on another, and a
/// replica that starts afterwards knows of it too: a worker's polls reach
/// whichever replica its load balancer picks.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_shutdown_reaches_every_replica() {
    let pool = pool().await;
    let first = engine(&pool).await;
    let second = engine(&pool).await;
    let _listening = second.start_worker_shutdown_listener();
    // The listener connects and subscribes in the background.
    tokio::time::timeout(Duration::from_secs(10), second.worker_shutdowns_listening())
        .await
        .expect("the second replica's listener subscribes");

    let task_queue = queue();
    let (a, a_poller) = worker();
    let (_, b_poller) = worker();

    let open_poll = {
        let (second, task_queue, poller) = (second.clone(), task_queue.clone(), a_poller.clone());
        tokio::spawn(async move { poll_for(&second, &task_queue, &poller, 20).await })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;

    first
        .shutdown_worker(shutdown(&a))
        .await
        .expect("shutdown acknowledged by the first replica");

    let answered = tokio::time::timeout(Duration::from_secs(1), open_poll)
        .await
        .expect("the poll on the second replica is answered within a second")
        .unwrap();
    assert!(answered.execution_id.is_empty(), "with no work");

    // A replica started after the shutdown reads it on connecting.
    let third = engine(&pool).await;
    let _listening = third.start_worker_shutdown_listener();
    tokio::time::timeout(Duration::from_secs(10), third.worker_shutdowns_listening())
        .await
        .expect("the third replica's listener subscribes and reads the table");

    let (id, execution_id) = workflow_on(&pool, &task_queue).await;
    for replica in [&second, &third] {
        let refused = poll_for(replica, &task_queue, &a_poller, 1).await;
        assert!(refused.execution_id.is_empty(), "refused on every replica");
    }
    assert_eq!(activation_state(&pool, id).await.0, None);

    let served = poll_for(&third, &task_queue, &b_poller, 2).await;
    assert_eq!(served.execution_id, execution_id, "others are served");

    cleanup(&pool, id, &execution_id).await;
    forget_shutdown(&pool, &a).await;
}

/// A workflow parked by its completion keeps its claim on purpose, until what
/// it waits for arrives. A release sent afterwards without a token, from the
/// worker that completed it, is a late repeat: it must not give that claim up,
/// or the workflow is handed out again while it waits.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_tokenless_release_after_a_completion_keeps_the_parked_claim() {
    let pool = pool().await;
    let service = engine(&pool).await;
    let task_queue = queue();
    let (id, execution_id) = workflow_on(&pool, &task_queue).await;
    let (a, a_poller) = worker();
    let (_, b_poller) = worker();

    let handed = poll_for(&service, &task_queue, &a_poller, 2).await;
    assert_eq!(handed.execution_id, execution_id);
    service
        .complete_workflow_execution(park_on_a_timer(
            id,
            &execution_id,
            &a,
            &handed.stream_entry_id,
        ))
        .await
        .expect("parked");
    let parked = activation_state(&pool, id).await;
    assert_eq!(parked.0.as_deref(), Some(a_poller.as_str()), "claim kept");
    assert_eq!(parked.2, Some(parked.1), "the activation is applied");

    service
        .release_workflow_execution(release(id, &execution_id, &a_poller, ""))
        .await
        .expect("the release is acknowledged");
    assert_eq!(
        activation_state(&pool, id).await,
        parked,
        "the parked workflow keeps its claim"
    );

    let other = poll_for(&service, &task_queue, &b_poller, 1).await;
    assert!(
        other.execution_id.is_empty(),
        "and is not handed to another poll"
    );

    cleanup(&pool, id, &execution_id).await;
}

/// An execution id is not enough to release or complete another
/// organization's workflow, or one in a namespace other than the one the
/// request was authorized for. Either is answered as though the execution
/// did not exist, and changes nothing; the owner's own requests go through.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_release_or_completion_reaches_only_the_callers_own_workflow() {
    let pool = pool().await;
    let service = engine(&pool).await;
    let owner = organization(&pool).await;
    let stranger = organization(&pool).await;
    let task_queue = queue();
    let (id, execution_id) = workflow_of(&pool, &task_queue, Some(owner)).await;
    let (a, a_poller) = worker();

    let handed = poll_for(&service, &task_queue, &a_poller, 2).await;
    assert_eq!(handed.execution_id, execution_id);
    let claimed = activation_state(&pool, id).await;

    // Another organization, naming the claim holder and the token.
    for token in [handed.stream_entry_id.as_str(), ""] {
        let refused = service
            .release_workflow_execution(acting_for(
                stranger,
                release(id, &execution_id, &a_poller, token),
            ))
            .await
            .expect_err("another organization's release is refused");
        assert_eq!(refused.code(), tonic::Code::NotFound);
    }
    let refused = service
        .complete_workflow_execution(acting_for(stranger, finish(id, &execution_id, &a_poller)))
        .await
        .expect_err("another organization's completion is refused");
    assert_eq!(refused.code(), tonic::Code::NotFound);

    // The owner's organization, but another namespace than the execution's.
    let mut elsewhere = release(id, &execution_id, &a_poller, "");
    elsewhere.get_mut().namespace = "elsewhere".to_string();
    let refused = service
        .release_workflow_execution(acting_for(owner, elsewhere))
        .await
        .expect_err("a release through another namespace is refused");
    assert_eq!(refused.code(), tonic::Code::NotFound);
    let mut elsewhere = finish(id, &execution_id, &a_poller);
    elsewhere.get_mut().namespace = "elsewhere".to_string();
    let refused = service
        .complete_workflow_execution(acting_for(owner, elsewhere))
        .await
        .expect_err("a completion through another namespace is refused");
    assert_eq!(refused.code(), tonic::Code::NotFound);

    // An execution that does not exist is answered the same way.
    let missing = Uuid::new_v4();
    let refused = service
        .release_workflow_execution(acting_for(
            stranger,
            release(missing, &missing.to_string(), &a_poller, ""),
        ))
        .await
        .expect_err("a release of nothing is refused");
    assert_eq!(refused.code(), tonic::Code::NotFound);

    assert_eq!(
        activation_state(&pool, id).await,
        claimed,
        "none of it changed the claim"
    );
    assert_eq!(status_of(&pool, id).await, "running", "nor the workflow");

    // The owner releases, then completes, its own workflow.
    service
        .release_workflow_execution(acting_for(
            owner,
            release(id, &execution_id, &a, &handed.stream_entry_id),
        ))
        .await
        .expect("the owner's release goes through");
    assert_eq!(activation_state(&pool, id).await.0, None);
    let handed = poll_for(&service, &task_queue, &a_poller, 2).await;
    assert_eq!(handed.execution_id, execution_id);
    service
        .complete_workflow_execution(acting_for(owner, finish(id, &execution_id, &a_poller)))
        .await
        .expect("the owner's completion goes through");
    assert_eq!(status_of(&pool, id).await, "completed");

    cleanup(&pool, id, &execution_id).await;
    forget_organization(&pool, owner).await;
    forget_organization(&pool, stranger).await;
}

/// A shutdown wakes the polls waiting in its namespace, but a poll that the
/// shutdown does not cover goes back to waiting without claiming: a shutdown
/// of another worker costs it no query.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn another_workers_shutdown_costs_a_waiting_poll_no_claim() {
    let pool = pool().await;
    let (service, polling) = engine_and_polling(&pool).await;
    let task_queue = queue();
    let (mine, my_poller) = worker();

    let open_poll = {
        let (service, task_queue, poller) =
            (service.clone(), task_queue.clone(), my_poller.clone());
        tokio::spawn(async move { poll_for(&service, &task_queue, &poller, 20).await })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;

    let others: Vec<String> = (0..30).map(|_| worker().0).collect();
    let before = polling.claim_scans();
    let started = Instant::now();
    for other in &others {
        service
            .shutdown_worker(shutdown(other))
            .await
            .expect("shutdown acknowledged");
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    let scans = polling.claim_scans() - before;
    // The poll retries its claim every 200ms whatever happens; anything
    // beyond that was caused by the shutdowns.
    let periodic = started.elapsed().as_millis() as u64 / 200 + 2;
    assert!(
        scans <= periodic,
        "{scans} claim queries while {} other workers shut down, where the \
         poll's own retries account for {periodic}",
        others.len()
    );
    assert!(!open_poll.is_finished(), "and the poll is still waiting");

    service
        .shutdown_worker(shutdown(&mine))
        .await
        .expect("shutdown acknowledged");
    let answered = tokio::time::timeout(Duration::from_secs(1), open_poll)
        .await
        .expect("its own shutdown ends the poll at once")
        .unwrap();
    assert!(answered.execution_id.is_empty());

    for identity in others.iter().chain([&mine]) {
        forget_shutdown(&pool, identity).await;
    }
}

/// A shutdown that names the stopping process refuses that process only. A
/// worker restarted under the same identity, as a fixed `service@host` name
/// is, sends a new instance id and is served at once, while the old process
/// stays shut out.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_restarted_worker_under_the_same_identity_is_served_at_once() {
    let pool = pool().await;
    let service = engine(&pool).await;
    let task_queue = queue();
    let (a, a_poller) = worker();

    let mut stopping = shutdown(&a);
    stopping.get_mut().worker_instance_id = "first-process".to_string();
    service
        .shutdown_worker(stopping)
        .await
        .expect("shutdown acknowledged");

    let (id, execution_id) = workflow_on(&pool, &task_queue).await;
    let refused = poll_from(&service, &task_queue, &a_poller, "first-process", 1).await;
    assert!(
        refused.execution_id.is_empty(),
        "the stopping process is given nothing"
    );
    assert_eq!(activation_state(&pool, id).await.0, None);

    let started = Instant::now();
    let served = poll_from(&service, &task_queue, &a_poller, "second-process", 5).await;
    assert_eq!(
        served.execution_id, execution_id,
        "the restarted process is"
    );
    assert!(started.elapsed() < Duration::from_secs(1), "at once");

    cleanup(&pool, id, &execution_id).await;
    forget_shutdown(&pool, &a).await;
}

/// A credential limited to some task queues can stop a worker's polls on
/// those queues only: it may not name another, and a shutdown naming none is
/// limited to the credential's own.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_queue_limited_credential_stops_polls_on_its_queues_only() {
    let pool = pool().await;
    let service = engine(&pool).await;
    let limited_to = queue();
    let elsewhere = queue();
    let (a, a_poller) = worker();
    let as_limited = |mut request: Request<ShutdownWorkerRequest>| {
        request.extensions_mut().insert(ServiceIdentity::new(
            "limited".to_string(),
            String::new(),
            vec![limited_to.clone()],
            ServicePermissions::worker(),
        ));
        request
    };

    let mut beyond = shutdown(&a);
    beyond.get_mut().task_queues = vec![elsewhere.clone()];
    let refused = service
        .shutdown_worker(as_limited(beyond))
        .await
        .expect_err("a queue the credential may not poll is refused");
    assert_eq!(refused.code(), tonic::Code::PermissionDenied);

    service
        .shutdown_worker(as_limited(shutdown(&a)))
        .await
        .expect("a shutdown naming no queue is acknowledged");

    let (on_its_queue, on_its_queue_exec) = workflow_on(&pool, &limited_to).await;
    let (on_another, on_another_exec) = workflow_on(&pool, &elsewhere).await;
    let refused = poll_for(&service, &limited_to, &a_poller, 1).await;
    assert!(
        refused.execution_id.is_empty(),
        "refused on the credential's queue"
    );
    let served = poll_for(&service, &elsewhere, &a_poller, 2).await;
    assert_eq!(
        served.execution_id, on_another_exec,
        "and served on any other"
    );

    cleanup(&pool, on_its_queue, &on_its_queue_exec).await;
    cleanup(&pool, on_another, &on_another_exec).await;
    forget_shutdown(&pool, &a).await;
}

/// Sent again with other queues, a shutdown covers the queues of both, on the
/// replica that took it and on one that only heard of it. And a shutdown that
/// names its process still covers, for a while, a poll that names none, on
/// either replica: what the announcement carries is enough to tell.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_shutdown_sent_again_covers_both_queue_sets_on_every_replica() {
    let pool = pool().await;
    let first = engine(&pool).await;
    let second = engine(&pool).await;
    let _listening = second.start_worker_shutdown_listener();
    tokio::time::timeout(Duration::from_secs(10), second.worker_shutdowns_listening())
        .await
        .expect("the second replica's listener subscribes");

    let (q1, q2, q3) = (queue(), queue(), queue());
    let (a, a_poller) = worker();
    for task_queue in [&q1, &q2] {
        let mut stopping = shutdown(&a);
        stopping.get_mut().worker_instance_id = "stopping".to_string();
        stopping.get_mut().task_queues = vec![task_queue.clone()];
        first
            .shutdown_worker(stopping)
            .await
            .expect("shutdown acknowledged");
    }
    let stored: Vec<String> =
        sqlx::query_scalar("SELECT task_queues FROM worker_shutdowns WHERE identity = $1")
            .bind(&a)
            .fetch_one(&pool)
            .await
            .expect("read the shutdown");
    let mut both = vec![q1.clone(), q2.clone()];
    both.sort();
    assert_eq!(stored, both, "stored as the union");

    // Polled on empty queues, so a poll that is not refused claims nothing
    // and only waits: a refused one is answered at once, one that is served
    // waits out its timeout. The second replica may not have heard the last
    // announcement yet, so it is given a few tries.
    for replica in [&first, &second] {
        for task_queue in [&q1, &q2] {
            for instance in ["stopping", ""] {
                let mut answered_at_once = false;
                for _ in 0..3 {
                    let started = Instant::now();
                    let polled = poll_from(replica, task_queue, &a_poller, instance, 1).await;
                    assert!(polled.execution_id.is_empty());
                    if started.elapsed() < Duration::from_millis(500) {
                        answered_at_once = true;
                        break;
                    }
                }
                assert!(
                    answered_at_once,
                    "refused on {task_queue} from {instance:?}"
                );
            }
        }
    }
    let (id, execution_id) = workflow_on(&pool, &q3).await;
    let workflows = vec![(id, execution_id.clone())];
    let served = poll_from(&second, &q3, &a_poller, "stopping", 2).await;
    assert_eq!(
        served.execution_id, execution_id,
        "a queue the shutdown does not cover is served"
    );

    for (id, execution_id) in &workflows {
        cleanup(&pool, *id, execution_id).await;
    }
    forget_shutdown(&pool, &a).await;
}

// ── Partial journals ───────────────────────────────────────────────────────
//
// A worker process that keeps run journals between activations
// (`history_cached`) may be sent only the entries after those its last
// activation of the run carried (`history_after_entry_id`). Any doubt means
// the whole journal, which is always correct.

/// A running workflow on `task_queue` whose step results are journal entries,
/// with `entries` entries journaled.
async fn journaled_workflow_on(pool: &PgPool, task_queue: &str, entries: usize) -> (Uuid, String) {
    let (id, execution_id) = workflow_on(pool, task_queue).await;
    sqlx::query("UPDATE workflow_executions SET step_results_journaled = true WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await
        .expect("journal step results");
    journal_more(pool, id, &execution_id, entries).await;
    (id, execution_id)
}

/// Journal `count` more entries for the run, numbered on from its last.
async fn journal_more(pool: &PgPool, id: Uuid, execution_id: &str, count: usize) {
    for _ in 0..count {
        sqlx::query(
            r#"
            INSERT INTO workflow_execution_journal
                (workflow_id, execution_id, event_id, event_type, timestamp, payload)
            VALUES ($1, $2,
                    (SELECT COALESCE(MAX(event_id), 0) + 1 FROM workflow_execution_journal
                     WHERE execution_id = $2),
                    'TimerFired', NOW(), '{}')
            "#,
        )
        .bind(id.to_string())
        .bind(execution_id)
        .execute(pool)
        .await
        .expect("journal an entry");
    }
}

/// A poll from the process `instance`, saying whether it keeps run journals.
async fn poll_keeping(
    service: &ExecutionService,
    task_queue: &str,
    poller: &str,
    instance: &str,
    history_cached: bool,
) -> PollWorkflowExecutionResponse {
    service
        .poll_workflow_execution(Request::new(PollWorkflowExecutionRequest {
            task_queue: task_queue.to_string(),
            namespace: NAMESPACE.to_string(),
            identity: poller.to_string(),
            poll_timeout: Some(orcher_proto::prost_types::Duration {
                seconds: 5,
                nanos: 0,
            }),
            worker_instance_id: instance.to_string(),
            history_cached,
            ..Default::default()
        }))
        .await
        .expect("poll")
        .into_inner()
}

/// Hand the activation back, saying whether the worker lacked the entries it
/// was to be joined to.
async fn hand_back(
    service: &ExecutionService,
    handed: &PollWorkflowExecutionResponse,
    identity: &str,
    history_missing: bool,
) {
    let mut request = release(
        handed.workflow_id.parse().expect("a workflow id"),
        &handed.execution_id,
        identity,
        &handed.stream_entry_id,
    );
    request.get_mut().history_missing = history_missing;
    service
        .release_workflow_execution(request)
        .await
        .expect("the release is acknowledged");
}

/// Which part of the journal an activation carried, and the ids it carried.
fn carried(handed: &PollWorkflowExecutionResponse) -> (i64, Vec<i64>) {
    (
        handed.history_after_entry_id,
        handed.journal.iter().map(|entry| entry.entry_id).collect(),
    )
}

/// The same process is sent only what it was not sent before, activation
/// after activation.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_process_keeping_a_runs_journal_is_sent_only_the_new_entries() {
    let pool = pool().await;
    let service = engine(&pool).await;
    let task_queue = queue();
    let (id, execution_id) = journaled_workflow_on(&pool, &task_queue, 3).await;
    let (identity, poller) = worker();
    let instance = Uuid::new_v4().to_string();

    let first = poll_keeping(&service, &task_queue, &poller, &instance, true).await;
    assert_eq!(first.execution_id, execution_id);
    assert_eq!(carried(&first), (0, vec![1, 2, 3]), "the first is whole");
    hand_back(&service, &first, &identity, false).await;

    journal_more(&pool, id, &execution_id, 2).await;
    let second = poll_keeping(&service, &task_queue, &poller, &instance, true).await;
    assert_eq!(
        carried(&second),
        (3, vec![4, 5]),
        "the next carries 4 and 5"
    );
    hand_back(&service, &second, &identity, false).await;

    journal_more(&pool, id, &execution_id, 1).await;
    let third = poll_keeping(&service, &task_queue, &poller, &instance, true).await;
    assert_eq!(carried(&third), (5, vec![6]), "and the one after, 6");

    // Nothing new: the activation carries no entries, after the last sent.
    hand_back(&service, &third, &identity, false).await;
    let fourth = poll_keeping(&service, &task_queue, &poller, &instance, true).await;
    assert_eq!(carried(&fourth), (6, vec![]));

    cleanup(&pool, id, &execution_id).await;
}

/// What was sent is recorded per process: another process is sent the whole
/// journal, and the first is too once the run has been to another.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn another_process_is_sent_the_whole_journal() {
    let pool = pool().await;
    let service = engine(&pool).await;
    let task_queue = queue();
    let (id, execution_id) = journaled_workflow_on(&pool, &task_queue, 3).await;
    let (identity, poller) = worker();
    let (x, y) = (Uuid::new_v4().to_string(), Uuid::new_v4().to_string());

    let to_x = poll_keeping(&service, &task_queue, &poller, &x, true).await;
    assert_eq!(carried(&to_x), (0, vec![1, 2, 3]));
    hand_back(&service, &to_x, &identity, false).await;

    journal_more(&pool, id, &execution_id, 1).await;
    let to_y = poll_keeping(&service, &task_queue, &poller, &y, true).await;
    assert_eq!(carried(&to_y), (0, vec![1, 2, 3, 4]), "y was sent nothing");
    hand_back(&service, &to_y, &identity, false).await;

    let back_to_x = poll_keeping(&service, &task_queue, &poller, &x, true).await;
    assert_eq!(
        carried(&back_to_x),
        (0, vec![1, 2, 3, 4]),
        "x is not assumed to hold what went to y"
    );

    cleanup(&pool, id, &execution_id).await;
}

/// A delivery to a poll that keeps nothing, or names no process, leaves
/// nothing to build on: the next activation is whole.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_delivery_to_a_poll_keeping_nothing_makes_the_next_whole() {
    let pool = pool().await;
    let service = engine(&pool).await;
    let task_queue = queue();
    let (id, execution_id) = journaled_workflow_on(&pool, &task_queue, 2).await;
    let (identity, poller) = worker();
    let instance = Uuid::new_v4().to_string();

    let kept = poll_keeping(&service, &task_queue, &poller, &instance, true).await;
    assert_eq!(carried(&kept), (0, vec![1, 2]));
    hand_back(&service, &kept, &identity, false).await;

    let not_kept = poll_keeping(&service, &task_queue, &poller, &instance, false).await;
    assert_eq!(
        carried(&not_kept),
        (0, vec![1, 2]),
        "a poll keeping nothing is sent it whole"
    );
    hand_back(&service, &not_kept, &identity, false).await;

    let again = poll_keeping(&service, &task_queue, &poller, &instance, true).await;
    assert_eq!(carried(&again), (0, vec![1, 2]), "and so is the next");
    hand_back(&service, &again, &identity, false).await;

    let anonymous = poll_keeping(&service, &task_queue, &poller, "", true).await;
    assert_eq!(
        carried(&anonymous),
        (0, vec![1, 2]),
        "as is a poll naming no process"
    );
    hand_back(&service, &anonymous, &identity, false).await;

    let after_anonymous = poll_keeping(&service, &task_queue, &poller, &instance, true).await;
    assert_eq!(
        carried(&after_anonymous),
        (0, vec![1, 2]),
        "and the one after it"
    );

    cleanup(&pool, id, &execution_id).await;
}

/// A worker that did not hold the entries an activation was joined to hands
/// it back with `history_missing`, and its next activation is whole.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_release_with_the_journal_missing_makes_the_next_whole() {
    let pool = pool().await;
    let service = engine(&pool).await;
    let task_queue = queue();
    let (id, execution_id) = journaled_workflow_on(&pool, &task_queue, 2).await;
    let (identity, poller) = worker();
    let instance = Uuid::new_v4().to_string();

    let first = poll_keeping(&service, &task_queue, &poller, &instance, true).await;
    hand_back(&service, &first, &identity, false).await;
    journal_more(&pool, id, &execution_id, 1).await;
    let partial = poll_keeping(&service, &task_queue, &poller, &instance, true).await;
    assert_eq!(carried(&partial), (2, vec![3]));

    // The worker restarted, or evicted the run, since entries 1 and 2.
    hand_back(&service, &partial, &identity, true).await;
    let whole = poll_keeping(&service, &task_queue, &poller, &instance, true).await;
    assert_eq!(carried(&whole), (0, vec![1, 2, 3]));
    hand_back(&service, &whole, &identity, false).await;

    let partial_again = poll_keeping(&service, &task_queue, &poller, &instance, true).await;
    assert_eq!(
        carried(&partial_again),
        (3, vec![]),
        "and partial again after"
    );

    cleanup(&pool, id, &execution_id).await;
}

/// A run whose step results are not journal entries has them rebuilt into
/// every activation, so every activation is whole.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_run_without_journaled_step_results_is_always_sent_whole() {
    let pool = pool().await;
    let service = engine(&pool).await;
    let task_queue = queue();
    let (id, execution_id) = workflow_on(&pool, &task_queue).await;
    journal_more(&pool, id, &execution_id, 2).await;
    let (identity, poller) = worker();
    let instance = Uuid::new_v4().to_string();

    let first = poll_keeping(&service, &task_queue, &poller, &instance, true).await;
    assert_eq!(carried(&first), (0, vec![1, 2]));
    hand_back(&service, &first, &identity, false).await;
    journal_more(&pool, id, &execution_id, 1).await;
    let second = poll_keeping(&service, &task_queue, &poller, &instance, true).await;
    assert_eq!(carried(&second), (0, vec![1, 2, 3]));

    cleanup(&pool, id, &execution_id).await;
}

/// Each engine node records only what it sent. A process that was sent more
/// by another node since can be sent entries it already holds; it holds more
/// than entries 1 to N, which the protocol says to take as missing, so the
/// cost is one round trip, never a wrong journal.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_node_that_sent_less_than_another_sends_entries_the_process_holds() {
    let pool = pool().await;
    let node_a = engine(&pool).await;
    let node_b = engine(&pool).await;
    let task_queue = queue();
    let (id, execution_id) = journaled_workflow_on(&pool, &task_queue, 3).await;
    let (identity, poller) = worker();
    let instance = Uuid::new_v4().to_string();

    let from_a = poll_keeping(&node_a, &task_queue, &poller, &instance, true).await;
    assert_eq!(carried(&from_a), (0, vec![1, 2, 3]));
    hand_back(&node_a, &from_a, &identity, false).await;

    journal_more(&pool, id, &execution_id, 1).await;
    let from_b = poll_keeping(&node_b, &task_queue, &poller, &instance, true).await;
    assert_eq!(
        carried(&from_b),
        (0, vec![1, 2, 3, 4]),
        "b sent nothing before"
    );
    hand_back(&node_b, &from_b, &identity, false).await;

    journal_more(&pool, id, &execution_id, 1).await;
    let from_a_again = poll_keeping(&node_a, &task_queue, &poller, &instance, true).await;
    assert_eq!(
        carried(&from_a_again),
        (3, vec![4, 5]),
        "a builds on what it sent, though the process also holds 4"
    );

    // The process holds 1 to 4, not exactly 1 to 3, and says so.
    hand_back(&node_a, &from_a_again, &identity, true).await;
    let whole = poll_keeping(&node_a, &task_queue, &poller, &instance, true).await;
    assert_eq!(carried(&whole), (0, vec![1, 2, 3, 4, 5]));

    cleanup(&pool, id, &execution_id).await;
}

// ── Routing a resumed run to its journal's holder ──────────────────────────
//
// A run released to resume is held a moment for the worker process that had
// its last activation, which keeps its journal: only that process may claim
// it until the hold passes, on every claim path; then anyone may.

/// A journaled run whose last activation went to the process `holder` and
/// was completed, parking it on a timer; then woken, as a finished task
/// wakes it. Returns the run's ids.
async fn resumed_after(
    pool: &PgPool,
    service: &ExecutionService,
    repo: &WorkflowExecutionRepository,
    task_queue: &str,
    holder: &str,
) -> (Uuid, String) {
    let (id, execution_id) = journaled_workflow_on(pool, task_queue, 2).await;
    let (identity, poller) = worker();
    let handed = poll_keeping(service, task_queue, &poller, holder, true).await;
    assert_eq!(handed.execution_id, execution_id);
    service
        .complete_workflow_execution(park_on_a_timer(
            id,
            &execution_id,
            &identity,
            &handed.stream_entry_id,
        ))
        .await
        .expect("the activation is completed");
    let resume = repo
        .release_claim_to_resume(&execution_id)
        .await
        .expect("release to resume");
    assert_eq!(
        resume,
        orcher_storage::repositories::domains::workflow_execution::Resume::Released
    );
    (id, execution_id)
}

/// A poll from `instance` that waits at most `secs` and returns what it got.
async fn poll_for_secs(
    service: &ExecutionService,
    task_queue: &str,
    instance: &str,
    secs: i64,
) -> PollWorkflowExecutionResponse {
    let (_, poller) = worker();
    service
        .poll_workflow_execution(Request::new(PollWorkflowExecutionRequest {
            task_queue: task_queue.to_string(),
            namespace: NAMESPACE.to_string(),
            identity: poller,
            poll_timeout: Some(orcher_proto::prost_types::Duration {
                seconds: secs,
                nanos: 0,
            }),
            worker_instance_id: instance.to_string(),
            history_cached: true,
            ..Default::default()
        }))
        .await
        .expect("poll")
        .into_inner()
}

/// While the hold lasts another process is handed nothing, and the holder
/// is handed the run at once, carrying only the entries it does not hold.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_resumed_run_is_held_for_the_process_keeping_its_journal() {
    let pool = pool().await;
    let (service, _, repo) = engine_holding(&pool, Duration::from_secs(30)).await;
    let task_queue = queue();
    let (holder, other) = (Uuid::new_v4().to_string(), Uuid::new_v4().to_string());
    let (id, execution_id) = resumed_after(&pool, &service, &repo, &task_queue, &holder).await;

    let to_other = poll_for_secs(&service, &task_queue, &other, 1).await;
    assert!(
        to_other.execution_id.is_empty(),
        "another process is handed nothing while the run is held"
    );

    let started = Instant::now();
    let to_holder = poll_for_secs(&service, &task_queue, &holder, 2).await;
    assert_eq!(
        to_holder.execution_id, execution_id,
        "the holder is handed it"
    );
    assert!(started.elapsed() < Duration::from_secs(1), "at once");
    assert!(
        to_holder.history_after_entry_id > 0,
        "carrying only what it does not hold"
    );

    cleanup(&pool, id, &execution_id).await;
}

/// The hold is a preference: once it passes, another process is handed the
/// run, whole, as before.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_held_run_goes_to_another_process_once_the_hold_passes() {
    let pool = pool().await;
    let hold = Duration::from_millis(500);
    let (service, _, repo) = engine_holding(&pool, hold).await;
    let task_queue = queue();
    let (holder, other) = (Uuid::new_v4().to_string(), Uuid::new_v4().to_string());
    let (id, execution_id) = resumed_after(&pool, &service, &repo, &task_queue, &holder).await;

    let started = Instant::now();
    let to_other = poll_for_secs(&service, &task_queue, &other, 5).await;
    assert_eq!(
        to_other.execution_id, execution_id,
        "the other process gets it"
    );
    let waited = started.elapsed();
    assert!(
        waited >= Duration::from_millis(400),
        "after the hold: {waited:?}"
    );
    assert!(
        waited < Duration::from_secs(2),
        "and soon after: {waited:?}"
    );
    assert_eq!(to_other.history_after_entry_id, 0, "whole");

    cleanup(&pool, id, &execution_id).await;
}

/// A zero hold holds nothing: another process is handed the run at once.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_zero_hold_holds_nothing() {
    let pool = pool().await;
    let (service, _, repo) = engine_holding(&pool, Duration::ZERO).await;
    let task_queue = queue();
    let (holder, other) = (Uuid::new_v4().to_string(), Uuid::new_v4().to_string());
    let (id, execution_id) = resumed_after(&pool, &service, &repo, &task_queue, &holder).await;

    let started = Instant::now();
    let to_other = poll_for_secs(&service, &task_queue, &other, 2).await;
    assert_eq!(to_other.execution_id, execution_id);
    assert!(started.elapsed() < Duration::from_secs(1), "at once");

    cleanup(&pool, id, &execution_id).await;
}

/// Every claim path keeps the hold: a claim by id from another process and
/// a batch claimed for an engine's buffer take nothing held for the holder;
/// a claim by id from the holder does.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn every_claim_path_keeps_the_hold() {
    let pool = pool().await;
    let (service, _, repo) = engine_holding(&pool, Duration::from_secs(30)).await;
    let task_queue = queue();
    let (holder, other) = (Uuid::new_v4().to_string(), Uuid::new_v4().to_string());
    let (id, execution_id) = resumed_after(&pool, &service, &repo, &task_queue, &holder).await;

    let by_other = repo
        .claim_execution_by_id_from(
            &execution_id,
            "other-poller",
            300,
            None,
            None,
            Some(&other),
            &[],
        )
        .await
        .expect("claim by id");
    assert!(by_other.is_none(), "not by id for another process");

    let batch = repo
        .claim_batch(&task_queue, NAMESPACE, "buffer", 300, 10, None, None)
        .await
        .expect("claim a batch");
    assert!(
        batch.iter().all(|e| e.execution_id != execution_id),
        "not into an engine's buffer"
    );

    let by_holder = repo
        .claim_execution_by_id_from(
            &execution_id,
            "holder-poller",
            300,
            None,
            None,
            Some(&holder),
            &[],
        )
        .await
        .expect("claim by id");
    assert!(by_holder.is_some(), "by id for the holder");

    cleanup(&pool, id, &execution_id).await;
}
