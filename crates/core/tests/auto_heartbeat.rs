//! A task whose worker stops answering for it is recovered, even when the task
//! sets no timeout of its own.
//!
//! A worker takes a task by polling for it, and the engine marks the task
//! started as it answers. If that answer never arrives — the connection drops
//! under it, the worker dies before running it — or the worker dies while the
//! task runs, the task stays `started`. With a timeout of its own, the timeout
//! watcher eventually times it out and retries it; with none, nothing ever did,
//! and the workflow waiting on it waited for good.
//!
//! A worker that heartbeats every task on its own says so on its poll
//! (`TaskCapabilities.auto_heartbeat`). An attempt it takes of a task that sets
//! no heartbeat timeout runs under the engine's default one, and is timed out,
//! then retried, once that long passes without a heartbeat. A worker that does
//! not say so is served as before: its long tasks, which it never heartbeats,
//! must not be timed out while they run.
//!
//! Requires PostgreSQL. Run with:
//!
//! ```bash
//! export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/orcher_test"
//! cargo test -p orcher-core --test auto_heartbeat -- --ignored --test-threads=1
//! ```

use std::sync::Arc;
use std::time::Duration;

use orcher_common::types::identifiers::{ExecutionId, WorkflowId};
use orcher_common::types::time::Timestamptz;
use orcher_core::engine::dispatch::task_manager::TaskPollingManager;
use orcher_core::engine::dispatch::PollingManager;
use orcher_core::engine::queues::outbox::OutboxQueue;
use orcher_core::engine::WorkflowCoordinator;
use orcher_core::events::EventBus;
use orcher_core::grpc::services::ExecutionService;
use orcher_core::retries::{TimeoutWatcher, TimeoutWatcherConfig};
use orcher_core::workflows::{
    ChildWorkflowManager, CommandProcessor, JournalService, WorkflowExecutionService,
};
use orcher_events::bus::EventBusConfig;
use orcher_proto::orcher::v1::execution_service_server::ExecutionService as ExecutionServiceTrait;
use orcher_proto::orcher::v1::{
    CompleteTaskExecutionRequest, PollTaskExecutionRequest, PollTaskExecutionResponse,
    RecordTaskHeartbeatRequest, TaskCapabilities,
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
const WORKER: &str = "worker-a";

/// The default heartbeat timeout the engine under test gives, in seconds.
const DEFAULT_HEARTBEAT_SECS: u64 = 1;

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

/// An engine on `pool` delivering tasks from the matching partitions of
/// `store`, giving a one-second default heartbeat timeout.
async fn engine(pool: &PgPool, store: Arc<dyn MatchingStore>) -> ExecutionService {
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
    .with_coordinator(coordinator)
    .with_polling_manager(polling_manager.clone());
    let task_polling_manager = Arc::new(TaskPollingManager::new(outbox_queue));
    task_polling_manager.set_matching_store(store);
    ExecutionService::new(
        Arc::new(command_processor),
        polling_manager,
        execution_log_repo as Arc<dyn ExecutionLogRepository>,
        journal_repo,
        exec_repo,
        task_execution_repo,
        pool.clone(),
    )
    .with_task_polling_manager(task_polling_manager)
    .with_default_heartbeat_timeout(Some(Duration::from_secs(DEFAULT_HEARTBEAT_SECS)))
}

/// A timeout watcher on `pool` that times out at the deadline, with no grace.
fn watcher(pool: &PgPool) -> TimeoutWatcher {
    TimeoutWatcher::new(
        pool.clone(),
        TimeoutWatcherConfig {
            grace_period_secs: 0,
            ..TimeoutWatcherConfig::default()
        },
    )
}

fn queue() -> String {
    format!("auto-heartbeat-{}", Uuid::new_v4().simple())
}

/// A new organization on `pool`, and the user that owns it.
async fn organization(pool: &PgPool) -> Uuid {
    let owner = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO users (id, email, password_hash, hash_algorithm, password_changed_at) \
         VALUES ($1, $2, 'x', 'argon2', NOW())",
    )
    .bind(owner)
    .bind(format!("owner-{owner}@auto-heartbeat.test"))
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

/// A running workflow on `pool`, belonging to `organization`.
async fn workflow(
    pool: &PgPool,
    id: Uuid,
    task_queue: &str,
    organization: Option<Uuid>,
) -> (Uuid, String) {
    let execution_id = Uuid::new_v4().to_string();
    sqlx::query(
        r#"
        INSERT INTO workflow_executions (
            id, execution_id, workflow_name, workflow_version, namespace, task_queue,
            status, parameters, context, environment, progress,
            total_steps, completed_steps, failed_steps, retried_steps, organization_id
        )
        VALUES ($1, $2, 'auto-heartbeat', '1.0.0', $3, $4, 'running', '{}', '{}', '{}', '{}',
                0, 0, 0, 0, $5)
        "#,
    )
    .bind(id)
    .bind(&execution_id)
    .bind(NAMESPACE)
    .bind(task_queue)
    .bind(organization)
    .execute(pool)
    .await
    .expect("insert a running workflow");
    (id, execution_id)
}

/// A scheduled task of the workflow, with `heartbeat_secs` as its own
/// heartbeat timeout and no other.
async fn scheduled_task(
    repo: &TaskExecutionRepository,
    id: Uuid,
    execution_id: &str,
    task_queue: &str,
    task_id: &str,
    heartbeat_secs: Option<i32>,
) -> orcher_storage::schema::types::TaskExecution {
    repo.create_scheduled(
        task_id.to_string(),
        "Echo".to_string(),
        WorkflowId::from_uuid(id),
        ExecutionId::from_uuid(Uuid::parse_str(execution_id).unwrap()),
        task_queue.to_string(),
        NAMESPACE.to_string(),
        1,
        None,
        None,
        None,
        None,
        heartbeat_secs,
        None,
        None,
    )
    .await
    .expect("schedule the task")
}

/// Offer `task` to pollers of its queue, as the transfer from the outbox does.
async fn offer(store: &PgMatchingStore, task: &orcher_storage::schema::types::TaskExecution) {
    let mut payload = serde_json::json!({
        "workflow_id": task.workflow_id.as_uuid().to_string(),
        "execution_id": task.execution_id.as_uuid().to_string(),
        "task_id": task.task_id,
        "task_type": task.task_type,
        "task_queue": task.task_queue,
        "namespace": NAMESPACE,
        "task_execution_id": task.id.to_string(),
    });
    if let Some(secs) = task.heartbeat_timeout_secs {
        payload["heartbeat_timeout_secs"] = serde_json::json!(secs);
    }
    store
        .enqueue(&MatchTask {
            task_id: Uuid::new_v4(),
            namespace: NAMESPACE.to_string(),
            task_queue: task.task_queue.clone(),
            workflow_id: task.workflow_id.as_uuid(),
            payload,
            organization_id: None,
        })
        .await
        .expect("offer the task");
}

/// Poll `task_queue` once, as a worker that heartbeats on its own when
/// `auto_heartbeat`, and as one released before that existed otherwise.
async fn poll(
    service: &ExecutionService,
    task_queue: &str,
    auto_heartbeat: bool,
) -> PollTaskExecutionResponse {
    let response = service
        .poll_task_execution(Request::new(PollTaskExecutionRequest {
            task_queue: task_queue.to_string(),
            namespace: NAMESPACE.to_string(),
            identity: WORKER.to_string(),
            poll_timeout: Some(orcher_proto::prost_types::Duration {
                seconds: 1,
                nanos: 0,
            }),
            task_capabilities: auto_heartbeat.then_some(TaskCapabilities {
                auto_heartbeat: true,
            }),
            ..Default::default()
        }))
        .await
        .expect("poll")
        .into_inner();
    assert!(!response.task_id.is_empty(), "the poll was handed no task");
    response
}

/// Each attempt of `task_id`, in order: `(attempt, status, error_type)`.
async fn attempts(pool: &PgPool, id: Uuid, task_id: &str) -> Vec<(i32, String, Option<String>)> {
    sqlx::query_as(
        "SELECT attempt_number, status, error_type FROM task_executions \
         WHERE workflow_id = $1 AND task_id = $2 ORDER BY attempt_number",
    )
    .bind(id)
    .bind(task_id)
    .fetch_all(pool)
    .await
    .expect("read the attempts")
}

/// Past the default heartbeat timeout, with a margin for the whole-second
/// timestamps the deadline is compared in.
async fn outlast_the_default() {
    tokio::time::sleep(Duration::from_millis(DEFAULT_HEARTBEAT_SECS * 1000 + 1200)).await;
}

async fn cleanup(pool: &PgPool, ids: &[Uuid]) {
    for id in ids {
        for q in [
            "DELETE FROM task_executions WHERE workflow_id = $1",
            "DELETE FROM matching_partitions WHERE workflow_id = $1",
            "DELETE FROM outbox_queue WHERE workflow_id = $1",
            "DELETE FROM workflow_executions WHERE id = $1",
        ] {
            let _ = sqlx::query(q).bind(id).execute(pool).await;
        }
    }
}

/// The task a worker that heartbeats on its own took, but never ran — its poll
/// response lost on the way — is timed out by missed heartbeats once the
/// default passes, though it sets no timeout at all, and retried.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_lost_poll_response_is_retried_after_the_default_heartbeat_timeout() {
    let pool = pool().await;
    let store = Arc::new(PgMatchingStore::new(pool.clone()));
    let service = engine(&pool, store.clone()).await;
    let repo = TaskExecutionRepository::new(pool.clone());
    let task_queue = queue();
    let (id, execution_id) = workflow(&pool, Uuid::new_v4(), &task_queue, None).await;
    let task = scheduled_task(&repo, id, &execution_id, &task_queue, "lost_0", None).await;
    offer(&store, &task).await;

    // The response that never reaches the worker.
    let lost = poll(&service, &task_queue, true).await;
    assert_eq!(lost.task_id, "lost_0");
    assert_eq!(
        lost.heartbeat_timeout.map(|d| d.seconds),
        Some(DEFAULT_HEARTBEAT_SECS as i64),
        "the worker is not told the heartbeat timeout it is held to"
    );

    outlast_the_default().await;
    watcher(&pool).check_timeouts().await.expect("sweep");

    let attempts = attempts(&pool, id, "lost_0").await;
    assert_eq!(
        attempts
            .first()
            .map(|(_, status, error)| (status.as_str(), error.as_deref())),
        Some(("timed_out", Some("HeartbeatTimeout"))),
        "the attempt nobody ran is still started: {attempts:?}"
    );
    assert_eq!(
        attempts.get(1).map(|(n, status, _)| (*n, status.as_str())),
        Some((2, "scheduled")),
        "the task is not retried: {attempts:?}"
    );

    cleanup(&pool, &[id]).await;
}

/// A worker that does not heartbeat on its own is not held to a heartbeat
/// timeout its task does not set: its long task runs past the default and
/// completes.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn an_old_workers_long_task_is_not_timed_out() {
    let pool = pool().await;
    let store = Arc::new(PgMatchingStore::new(pool.clone()));
    let service = engine(&pool, store.clone()).await;
    let repo = TaskExecutionRepository::new(pool.clone());
    let task_queue = queue();
    let (id, execution_id) = workflow(&pool, Uuid::new_v4(), &task_queue, None).await;
    let task = scheduled_task(&repo, id, &execution_id, &task_queue, "long_0", None).await;
    offer(&store, &task).await;

    let taken = poll(&service, &task_queue, false).await;
    assert_eq!(
        taken.heartbeat_timeout, None,
        "a worker that does not heartbeat on its own is given a heartbeat timeout"
    );

    outlast_the_default().await;
    watcher(&pool).check_timeouts().await.expect("sweep");
    assert_eq!(
        attempts(&pool, id, "long_0").await,
        vec![(1, "started".to_string(), None)],
        "a running task of a worker that never heartbeats was timed out"
    );

    service
        .complete_task_execution(Request::new(CompleteTaskExecutionRequest {
            task_token: taken.task_token,
            namespace: NAMESPACE.to_string(),
            identity: WORKER.to_string(),
            result: b"\"done\"".to_vec(),
        }))
        .await
        .expect("complete the task");
    assert_eq!(
        attempts(&pool, id, "long_0").await,
        vec![(1, "completed".to_string(), None)]
    );

    cleanup(&pool, &[id]).await;
}

/// Heartbeats keep a task that sets no timeout running well past the default,
/// and it completes as its only attempt.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn heartbeats_keep_the_task_past_the_default() {
    let pool = pool().await;
    let store = Arc::new(PgMatchingStore::new(pool.clone()));
    let service = engine(&pool, store.clone()).await;
    let repo = TaskExecutionRepository::new(pool.clone());
    let watcher = watcher(&pool);
    let task_queue = queue();
    let (id, execution_id) = workflow(&pool, Uuid::new_v4(), &task_queue, None).await;
    let task = scheduled_task(&repo, id, &execution_id, &task_queue, "beating_0", None).await;
    offer(&store, &task).await;
    let taken = poll(&service, &task_queue, true).await;

    // Three times the timeout, heartbeating at a third of it and sweeping as
    // often.
    for _ in 0..9 {
        tokio::time::sleep(Duration::from_millis(DEFAULT_HEARTBEAT_SECS * 1000 / 3)).await;
        service
            .record_task_heartbeat(Request::new(RecordTaskHeartbeatRequest {
                task_token: taken.task_token.clone(),
                namespace: NAMESPACE.to_string(),
                identity: WORKER.to_string(),
                details: vec![],
            }))
            .await
            .expect("heartbeat");
        watcher.check_timeouts().await.expect("sweep");
    }

    service
        .complete_task_execution(Request::new(CompleteTaskExecutionRequest {
            task_token: taken.task_token,
            namespace: NAMESPACE.to_string(),
            identity: WORKER.to_string(),
            result: b"\"done\"".to_vec(),
        }))
        .await
        .expect("complete the task");
    assert_eq!(
        attempts(&pool, id, "beating_0").await,
        vec![(1, "completed".to_string(), None)],
        "a task that kept heartbeating was timed out"
    );

    cleanup(&pool, &[id]).await;
}

/// A task with a heartbeat timeout of its own keeps it: the default applies
/// only to one that sets none.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_tasks_own_heartbeat_timeout_is_kept() {
    let pool = pool().await;
    let store = Arc::new(PgMatchingStore::new(pool.clone()));
    let service = engine(&pool, store.clone()).await;
    let repo = TaskExecutionRepository::new(pool.clone());
    let task_queue = queue();
    let (id, execution_id) = workflow(&pool, Uuid::new_v4(), &task_queue, None).await;
    let task = scheduled_task(&repo, id, &execution_id, &task_queue, "own_0", Some(30)).await;
    offer(&store, &task).await;

    let taken = poll(&service, &task_queue, true).await;
    assert_eq!(taken.heartbeat_timeout.map(|d| d.seconds), Some(30));
    outlast_the_default().await;
    watcher(&pool).check_timeouts().await.expect("sweep");
    assert_eq!(
        attempts(&pool, id, "own_0").await,
        vec![(1, "started".to_string(), None)],
        "the default replaced the task's own, longer heartbeat timeout"
    );

    cleanup(&pool, &[id]).await;
}

/// An eager task handed to a worker that heartbeats on its own gets no eager
/// TTL, which would hand it out again ten seconds in while it still ran; a
/// hand-over lost on the way is recovered by its heartbeat timeout instead.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn an_eager_task_is_left_to_its_heartbeat_timeout() {
    let pool = pool().await;
    let repo = TaskExecutionRepository::new(pool.clone());
    let task_queue = queue();
    let (id, execution_id) = workflow(&pool, Uuid::new_v4(), &task_queue, None).await;
    let task = scheduled_task(&repo, id, &execution_id, &task_queue, "eager_0", None).await;
    let token =
        orcher_core::grpc::task_token::encode(&orcher_core::grpc::task_token::NewTaskToken {
            workflow_id: &id.to_string(),
            execution_id: &execution_id,
            task_id: &task.task_id,
            attempt: 1,
            started_at_ms: Timestamptz::now().timestamp_millis(),
            organization_id: None,
        });
    repo.mark_started_eager(
        task.id,
        WORKER.to_string(),
        token,
        // A TTL already run out: the state of a task still running past it.
        Timestamptz::now() - Duration::from_secs(1),
        Some(DEFAULT_HEARTBEAT_SECS as i32),
    )
    .await
    .expect("hand the task over eagerly");

    let reset = repo
        .reclaim_expired_eager_tasks()
        .await
        .expect("run the eager reclaimer");
    assert!(reset.is_empty(), "the eager TTL reset a task still running");
    assert_eq!(
        attempts(&pool, id, "eager_0").await,
        vec![(1, "started".to_string(), None)]
    );

    outlast_the_default().await;
    watcher(&pool).check_timeouts().await.expect("sweep");
    let attempts = attempts(&pool, id, "eager_0").await;
    assert_eq!(
        attempts
            .iter()
            .map(|(n, status, _)| (*n, status.as_str()))
            .collect::<Vec<_>>(),
        vec![(1, "timed_out"), (2, "scheduled")],
        "a lost eager hand-over is not recovered by its heartbeat timeout"
    );

    cleanup(&pool, &[id]).await;
}

/// The test database, and a second database beside it for shards routed away
/// from the first, created and migrated on first use.
async fn shard_database(pool: &PgPool) -> PgPool {
    use sqlx::postgres::PgConnectOptions;
    use std::str::FromStr;

    let url = std::env::var("TEST_DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:postgres@localhost:5432/orcher_test".to_string()
    });
    let options = PgConnectOptions::from_str(&url).expect("TEST_DATABASE_URL");
    let name = format!(
        "{}_heartbeat_shard",
        options.get_database().unwrap_or("orcher_test")
    );
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)")
            .bind(&name)
            .fetch_one(pool)
            .await
            .expect("look for the shard database");
    if !exists {
        sqlx::query(&format!("CREATE DATABASE \"{name}\""))
            .execute(pool)
            .await
            .expect("create the shard database");
    }
    let shard = PgPool::connect_with(options.database(&name))
        .await
        .expect("connect to the shard database");
    orcher_storage::setup::run_migrations(&shard)
        .await
        .expect("migrate the shard database");
    shard
}

/// With a workflow's shard on a database of its own, its task's default
/// heartbeat timeout is recorded there, and the watcher times the task out
/// and retries it there, for the workflow's organization; nothing is written
/// to the other database.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_task_on_a_shard_database_is_timed_out_and_retried_there() {
    use orcher_cluster::{MultiPoolRouter, ShardRouter};

    let pool = pool().await;
    let shard_db = shard_database(&pool).await;
    let router = Arc::new(MultiPoolRouter::with_vshards(
        vec![pool.clone(), shard_db.clone()],
        16,
    ));
    let id = std::iter::repeat_with(Uuid::new_v4)
        .find(|id| {
            std::ptr::eq(
                ShardRouter::pool_for_workflow(router.as_ref(), &id.to_string()),
                ShardRouter::all_pools(router.as_ref())[1],
            )
        })
        .unwrap();
    let organization = organization(&shard_db).await;
    let task_queue = queue();
    let (id, execution_id) = workflow(&shard_db, id, &task_queue, Some(organization)).await;
    let repo = TaskExecutionRepository::new(pool.clone());
    repo.set_shard_resolver(router.clone());
    let task = scheduled_task(&repo, id, &execution_id, &task_queue, "sharded_0", None).await;
    repo.mark_started_as(
        task.id,
        WORKER.to_string(),
        format!("token-{}", Uuid::new_v4()),
        Some(DEFAULT_HEARTBEAT_SECS as i32),
        None,
    )
    .await
    .expect("start the task for a worker that heartbeats on its own");

    outlast_the_default().await;
    let watcher = watcher(&pool);
    watcher.set_shard_resolver(router.clone());
    watcher.check_timeouts().await.expect("sweep");

    let retried: Vec<(i32, String, Option<Uuid>)> = sqlx::query_as(
        "SELECT attempt_number, status, organization_id FROM task_executions \
         WHERE workflow_id = $1 ORDER BY attempt_number",
    )
    .bind(id)
    .fetch_all(&shard_db)
    .await
    .expect("read the attempts on the shard");
    assert_eq!(
        retried,
        vec![
            (1, "timed_out".to_string(), Some(organization)),
            (2, "scheduled".to_string(), Some(organization)),
        ],
        "the task on its shard's database was not timed out and retried there, \
         for its workflow's organization"
    );
    assert!(
        attempts(&pool, id, "sharded_0").await.is_empty(),
        "an attempt was written to a database the workflow does not live on"
    );

    cleanup(&shard_db, &[id]).await;
}
