//! What a task's completion, failure or heartbeat answers, and what it leaves
//! alone.
//!
//! A task's replay row in the execution log is keyed on the task, not the
//! attempt, and a completion was deduplicated on the task alone. So a repeated
//! completion rewrote the replay row and woke the workflow again, and a late
//! report from an attempt that had timed out and been retried — completion or
//! failure — overwrote the newer attempt's row with its own stale outcome. A
//! completion whose lookup failed, or found no task, wrote a Completed replay
//! row and answered OK with nothing journaled. Refusals that are the caller's
//! to act on came back as INTERNAL.
//!
//! Requires PostgreSQL. Run with:
//!
//! ```bash
//! export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/orcher_test"
//! cargo test -p orcher-core --test task_answers -- --ignored --test-threads=1
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
use orcher_proto::orcher::v1::{
    CompleteTaskExecutionRequest, FailTaskExecutionRequest, Failure, RecordTaskHeartbeatRequest,
};
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
    tasks: Arc<TaskExecutionRepository>,
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
            task_execution_repo.clone(),
            pool.clone(),
        );

        Self {
            pool,
            tasks: task_execution_repo,
            service,
        }
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
            let _ = sqlx::query("DELETE FROM task_executions WHERE workflow_id = $1")
                .bind(id)
                .execute(&self.pool)
                .await;
            let _ = sqlx::query("DELETE FROM execution_log WHERE workflow_id = $1")
                .bind(id)
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

/// A started attempt of `task_id`, with the token its worker would report with.
async fn started_attempt(
    engine: &Engine,
    id: Uuid,
    execution_id: &str,
    task_id: &str,
    attempt: i32,
) -> Vec<u8> {
    started_attempt_at(engine, id, execution_id, task_id, attempt, attempt as i64).await
}

/// [`started_attempt`], dispatched at `started_at_ms`: a second row of the same
/// attempt needs a token of its own.
async fn started_attempt_at(
    engine: &Engine,
    id: Uuid,
    execution_id: &str,
    task_id: &str,
    attempt: i32,
    started_at_ms: i64,
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
        .expect("schedule the task");
    // Tokens are unique per attempt: each carries its own start time.
    let token =
        orcher_core::grpc::task_token::encode(&orcher_core::grpc::task_token::NewTaskToken {
            workflow_id: &id.to_string(),
            execution_id,
            task_id,
            attempt,
            started_at_ms,
            organization_id: None,
        });
    repo.mark_started(task.id, WORKER.to_string(), token.clone())
        .await
        .expect("start the task");
    token.into_bytes()
}

/// Supersede the attempt holding `token` as a start-to-close timeout does.
/// Its policy has no attempts left, so any failure it reports is final.
async fn time_out(engine: &Engine, token: &[u8]) {
    sqlx::query(
        r#"
        UPDATE task_executions
        SET status = 'timed_out', completed_at = NOW(), updated_at = NOW(),
            error_message = 'Task timed out: StartToClose', error_type = 'StartToClose',
            retry_policy = '{"max_attempts": 1, "initial_interval_seconds": 1.0,
                             "max_interval_seconds": 1.0, "backoff_coefficient": 1.0}'
        WHERE task_token = $1
        "#,
    )
    .bind(std::str::from_utf8(token).unwrap())
    .execute(&engine.pool)
    .await
    .expect("time the attempt out");
}

fn completion(token: &[u8], result: &str) -> Request<CompleteTaskExecutionRequest> {
    Request::new(CompleteTaskExecutionRequest {
        task_token: token.to_vec(),
        namespace: NAMESPACE.to_string(),
        identity: WORKER.to_string(),
        result: serde_json::to_vec(result).unwrap(),
        ..Default::default()
    })
}

fn failure(token: &[u8], failure_type: &str) -> Request<FailTaskExecutionRequest> {
    Request::new(FailTaskExecutionRequest {
        task_token: token.to_vec(),
        namespace: NAMESPACE.to_string(),
        identity: WORKER.to_string(),
        failure: Some(Failure {
            message: "late".to_string(),
            failure_type: failure_type.to_string(),
            ..Default::default()
        }),
        ..Default::default()
    })
}

fn heartbeat(token: &[u8]) -> Request<RecordTaskHeartbeatRequest> {
    Request::new(RecordTaskHeartbeatRequest {
        task_token: token.to_vec(),
        namespace: NAMESPACE.to_string(),
        identity: WORKER.to_string(),
        ..Default::default()
    })
}

/// A token for a task no row was ever started with.
fn unknown_token(id: Uuid, execution_id: &str) -> Vec<u8> {
    orcher_core::grpc::task_token::encode(&orcher_core::grpc::task_token::NewTaskToken {
        workflow_id: &id.to_string(),
        execution_id,
        task_id: "never_0",
        attempt: 1,
        started_at_ms: 0,
        organization_id: None,
    })
    .into_bytes()
}

#[derive(Debug, PartialEq)]
struct ReplayRow {
    status: String,
    attempt: i32,
    result: Option<serde_json::Value>,
    completed_at: Option<String>,
}

async fn replay_row(pool: &PgPool, id: Uuid, task_id: &str) -> Option<ReplayRow> {
    sqlx::query_as::<_, (String, i32, Option<serde_json::Value>, Option<String>)>(
        "SELECT status::text, execution_attempt, result, completed_at::text FROM execution_log \
         WHERE workflow_id = $1 AND step_name = $2",
    )
    .bind(id)
    .bind(task_id)
    .fetch_optional(pool)
    .await
    .expect("read the replay row")
    .map(|(status, attempt, result, completed_at)| ReplayRow {
        status,
        attempt,
        result,
        completed_at,
    })
}

async fn task_completed_count(pool: &PgPool, id: Uuid, task_id: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM workflow_execution_journal \
         WHERE workflow_id = $1::text AND event_type = 'TaskCompleted' AND payload->>'task_id' = $2",
    )
    .bind(id)
    .bind(task_id)
    .fetch_one(pool)
    .await
    .expect("count TaskCompleted")
}

async fn attempt_status(pool: &PgPool, token: &[u8]) -> (String, String) {
    sqlx::query_as("SELECT status, updated_at::text FROM task_executions WHERE task_token = $1")
        .bind(std::str::from_utf8(token).unwrap())
        .fetch_one(pool)
        .await
        .expect("read the attempt")
}

async fn stored_result(pool: &PgPool, token: &[u8]) -> Vec<u8> {
    sqlx::query_scalar::<_, Option<Vec<u8>>>(
        "SELECT result FROM task_executions WHERE task_token = $1",
    )
    .bind(std::str::from_utf8(token).unwrap())
    .fetch_one(pool)
    .await
    .expect("read the attempt's result")
    .unwrap_or_default()
}

/// Whether the workflow has been released to continue since it was last
/// claimed: waking a workflow whose activation is not in flight releases its
/// claim so that a poller can take it.
async fn woken(pool: &PgPool, id: Uuid) -> bool {
    sqlx::query_scalar("SELECT claimed_by IS NULL FROM workflow_executions WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("read the workflow")
}

/// Claim the workflow again, as a poller taking the wake-up would.
async fn reclaim(pool: &PgPool, id: Uuid) {
    sqlx::query(
        "UPDATE workflow_executions SET claimed_by = $2, claimed_at = NOW(), \
         claim_timeout_at = NOW() + INTERVAL '5 minutes' WHERE id = $1",
    )
    .bind(id)
    .bind(WORKER)
    .execute(pool)
    .await
    .expect("claim the workflow again");
}

#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_repeated_completion_keeps_the_first_result() {
    let engine = Engine::new().await;
    let (id, execution_id) = engine.claimed_workflow("repeated-completion").await;
    let task_id = "charge_0";
    let token = started_attempt(&engine, id, &execution_id, task_id, 1).await;

    engine
        .service
        .complete_task_execution(completion(&token, "first"))
        .await
        .expect("complete the task");
    assert!(
        woken(&engine.pool, id).await,
        "the first completion wakes the workflow"
    );
    reclaim(&engine.pool, id).await;

    engine
        .service
        .complete_task_execution(completion(&token, "second"))
        .await
        .expect("a repeated completion is answered OK");

    assert_eq!(task_completed_count(&engine.pool, id, task_id).await, 1);
    let replay = replay_row(&engine.pool, id, task_id)
        .await
        .expect("the replay row is still there");
    assert_eq!(
        (replay.status.as_str(), replay.attempt, replay.result),
        ("completed", 1, Some(serde_json::json!("first"))),
        "the replay row holds the recorded result, not the repeat's"
    );
    assert_eq!(attempt_status(&engine.pool, &token).await.0, "completed");
    assert_eq!(
        stored_result(&engine.pool, &token).await,
        serde_json::to_vec("first").unwrap()
    );

    engine.cleanup(&[id]).await;
}

/// A completed task's step records the input the task was scheduled with:
/// it is read from the task's row, which every path that ends
/// a task shares, not from a journal lookup that never matched.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_completed_step_records_its_input() {
    let engine = Engine::new().await;
    let (id, execution_id) = engine.claimed_workflow("step-input").await;
    let task_id = "charge_0";
    let repo = TaskExecutionRepository::new(engine.pool.clone());
    let task = repo
        .create_scheduled(
            task_id.to_string(),
            "TaskType".to_string(),
            WorkflowId::from_uuid(id),
            ExecutionId::from_uuid(Uuid::parse_str(&execution_id).unwrap()),
            TASK_QUEUE.to_string(),
            NAMESPACE.to_string(),
            1,
            Some(br#"{"amount":7}"#.to_vec()),
            None,
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
            execution_id: &execution_id,
            task_id,
            attempt: 1,
            started_at_ms: 1,
            organization_id: None,
        });
    repo.mark_started(task.id, WORKER.to_string(), token.clone())
        .await
        .expect("start the task");

    engine
        .service
        .complete_task_execution(completion(token.as_bytes(), "charged"))
        .await
        .expect("complete the task");

    let input: Option<serde_json::Value> = sqlx::query_scalar(
        "SELECT input FROM execution_log WHERE workflow_id = $1 AND step_name = $2",
    )
    .bind(id)
    .bind(task_id)
    .fetch_one(&engine.pool)
    .await
    .expect("read the step");
    assert_eq!(input, Some(serde_json::json!({"amount": 7})));

    engine.cleanup(&[id]).await;
}

/// The first report commits the completion and journals it, then fails before
/// writing the replay row — the only way a result reaches the workflow. Its
/// retry must write it, or the workflow waits for the task forever.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_repeat_restores_a_replay_row_lost_after_the_commit() {
    let engine = Engine::new().await;
    let (id, execution_id) = engine.claimed_workflow("lost-replay-row").await;
    let task_id = "charge_0";
    let token = started_attempt(&engine, id, &execution_id, task_id, 1).await;

    engine
        .service
        .complete_task_execution(completion(&token, "first"))
        .await
        .expect("complete the task");
    sqlx::query("DELETE FROM execution_log WHERE workflow_id = $1 AND step_name = $2")
        .bind(id)
        .bind(task_id)
        .execute(&engine.pool)
        .await
        .expect("lose the replay row");
    reclaim(&engine.pool, id).await;

    engine
        .service
        .complete_task_execution(completion(&token, "retried"))
        .await
        .expect("the retried report is answered OK");

    let replay = replay_row(&engine.pool, id, task_id)
        .await
        .expect("the replay row is restored");
    assert_eq!(
        (replay.status.as_str(), replay.attempt, replay.result),
        ("completed", 1, Some(serde_json::json!("first")))
    );
    assert!(woken(&engine.pool, id).await, "the workflow is woken");
    assert_eq!(task_completed_count(&engine.pool, id, task_id).await, 1);

    engine.cleanup(&[id]).await;
}

/// A second row of the same attempt — as a failover re-drive can leave — is
/// finished when the first has already answered the task, with the answer
/// that was journaled, rather than left started for the timeout watcher to
/// run again.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_duplicate_row_is_finished_with_the_recorded_result() {
    let engine = Engine::new().await;
    let (id, execution_id) = engine.claimed_workflow("duplicate-row").await;
    let task_id = "charge_0";
    let first = started_attempt(&engine, id, &execution_id, task_id, 1).await;
    let duplicate = started_attempt_at(&engine, id, &execution_id, task_id, 1, 99).await;

    engine
        .service
        .complete_task_execution(completion(&first, "first"))
        .await
        .expect("the first row completes");
    engine
        .service
        .complete_task_execution(completion(&duplicate, "duplicate"))
        .await
        .expect("the duplicate is answered OK");

    assert_eq!(task_completed_count(&engine.pool, id, task_id).await, 1);
    assert_eq!(
        attempt_status(&engine.pool, &duplicate).await.0,
        "completed",
        "the duplicate row is not left running"
    );
    assert_eq!(
        stored_result(&engine.pool, &duplicate).await,
        serde_json::to_vec("first").unwrap(),
        "the duplicate row holds the journaled result"
    );
    let replay = replay_row(&engine.pool, id, task_id)
        .await
        .expect("a replay row");
    assert_eq!(replay.result, Some(serde_json::json!("first")));

    engine.cleanup(&[id]).await;
}

/// A token whose row the lookup finds on one database while the workflow
/// routes to another is an inconsistency to retry, not an attempt that ended.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_row_off_its_workflows_shard_is_unavailable() {
    use orcher_cluster::{MultiPoolRouter, ShardRouter};

    let engine = Engine::new().await;
    let shard_db = shard_database(&engine.pool).await;
    let router = Arc::new(MultiPoolRouter::with_vshards(
        vec![engine.pool.clone(), shard_db.clone()],
        16,
    ));
    engine.tasks.set_shard_resolver(router.clone());

    // A workflow routed to the first database, with its task row on the second.
    let (id, execution_id) = loop {
        let (id, execution_id) = engine.claimed_workflow("misrouted-task").await;
        if std::ptr::eq(
            ShardRouter::pool_for_workflow(router.as_ref(), &id.to_string()),
            ShardRouter::all_pools(router.as_ref())[0],
        ) {
            break (id, execution_id);
        }
        engine.cleanup(&[id]).await;
    };
    let misplaced = Engine {
        pool: shard_db.clone(),
        tasks: Arc::new(TaskExecutionRepository::new(shard_db.clone())),
        service: Engine::new().await.service,
    };
    let token = started_attempt(&misplaced, id, &execution_id, "charge_0", 1).await;

    let refused = engine
        .service
        .complete_task_execution(completion(&token, "anything"))
        .await
        .expect_err("a completion that cannot find its row where it routes fails");
    assert_eq!(refused.code(), tonic::Code::Unavailable, "{refused:?}");
    assert_eq!(attempt_status(&shard_db, &token).await.0, "started");

    sqlx::query("DELETE FROM task_executions WHERE workflow_id = $1")
        .bind(id)
        .execute(&shard_db)
        .await
        .expect("clean the shard database");
    engine.cleanup(&[id]).await;
}

/// A second database beside the test database, created and migrated on first
/// use.
async fn shard_database(pool: &PgPool) -> PgPool {
    use sqlx::postgres::PgConnectOptions;
    use std::str::FromStr;

    let url = std::env::var("TEST_DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:postgres@localhost:5432/orcher_test".to_string()
    });
    let options = PgConnectOptions::from_str(&url).expect("TEST_DATABASE_URL");
    let name = format!(
        "{}_misrouted_shard",
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

#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_superseded_attempt_cannot_complete_while_its_successor_runs() {
    let engine = Engine::new().await;
    let (id, execution_id) = engine.claimed_workflow("superseded-running").await;
    let task_id = "charge_0";
    let stale = started_attempt(&engine, id, &execution_id, task_id, 1).await;
    time_out(&engine, &stale).await;
    let current = started_attempt(&engine, id, &execution_id, task_id, 2).await;

    let refused = engine
        .service
        .complete_task_execution(completion(&stale, "stale"))
        .await
        .expect_err("a superseded attempt's completion is refused");
    assert_eq!(
        refused.code(),
        tonic::Code::FailedPrecondition,
        "{refused:?}"
    );

    assert_eq!(replay_row(&engine.pool, id, task_id).await, None);
    assert_eq!(task_completed_count(&engine.pool, id, task_id).await, 0);
    assert_eq!(attempt_status(&engine.pool, &current).await.0, "started");

    engine.cleanup(&[id]).await;
}

#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_superseded_attempt_cannot_overwrite_its_successors_result() {
    let engine = Engine::new().await;
    let (id, execution_id) = engine.claimed_workflow("superseded-completed").await;
    let task_id = "charge_0";
    let stale = started_attempt(&engine, id, &execution_id, task_id, 1).await;
    time_out(&engine, &stale).await;
    let current = started_attempt(&engine, id, &execution_id, task_id, 2).await;
    engine
        .service
        .complete_task_execution(completion(&current, "current"))
        .await
        .expect("the current attempt completes");
    let replay = replay_row(&engine.pool, id, task_id)
        .await
        .expect("a replay row");
    assert_eq!(replay.attempt, 2);
    assert_eq!(replay.result, Some(serde_json::json!("current")));
    reclaim(&engine.pool, id).await;

    let refused = engine
        .service
        .complete_task_execution(completion(&stale, "stale"))
        .await
        .expect_err("a superseded attempt's completion is refused");
    assert_eq!(
        refused.code(),
        tonic::Code::FailedPrecondition,
        "{refused:?}"
    );

    assert_eq!(
        replay_row(&engine.pool, id, task_id).await,
        Some(replay),
        "the replay row still holds the current attempt's result"
    );
    assert_eq!(task_completed_count(&engine.pool, id, task_id).await, 1);
    assert!(
        !woken(&engine.pool, id).await,
        "the workflow is not woken again"
    );

    engine.cleanup(&[id]).await;
}

#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_superseded_attempts_failure_leaves_its_successors_result() {
    let engine = Engine::new().await;
    let (id, execution_id) = engine.claimed_workflow("superseded-failure").await;
    let task_id = "charge_0";
    let stale = started_attempt(&engine, id, &execution_id, task_id, 1).await;
    time_out(&engine, &stale).await;
    let current = started_attempt(&engine, id, &execution_id, task_id, 2).await;
    engine
        .service
        .complete_task_execution(completion(&current, "current"))
        .await
        .expect("the current attempt completes");
    let replay = replay_row(&engine.pool, id, task_id)
        .await
        .expect("a replay row");

    engine
        .service
        .fail_task_execution(failure(&stale, "ValidationError"))
        .await
        .expect("a late failure is answered OK");

    assert_eq!(
        replay_row(&engine.pool, id, task_id).await,
        Some(replay),
        "the replay row still holds the current attempt's result"
    );
    let task_failed: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM workflow_execution_journal \
         WHERE workflow_id = $1::text AND event_type = 'TaskFailed'",
    )
    .bind(id)
    .fetch_one(&engine.pool)
    .await
    .unwrap();
    assert_eq!(task_failed, 0, "nothing is journaled for the stale attempt");

    engine.cleanup(&[id]).await;
}

#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_report_for_no_task_is_not_found() {
    let engine = Engine::new().await;
    let (id, execution_id) = engine.claimed_workflow("unknown-task").await;
    let token = unknown_token(id, &execution_id);

    let completed = engine
        .service
        .complete_task_execution(completion(&token, "anything"))
        .await
        .expect_err("a completion for no task is refused");
    assert_eq!(completed.code(), tonic::Code::NotFound, "{completed:?}");
    assert_eq!(replay_row(&engine.pool, id, "never_0").await, None);

    let failed = engine
        .service
        .fail_task_execution(failure(&token, "TaskExecutionError"))
        .await
        .expect_err("a failure for no task is refused");
    assert_eq!(failed.code(), tonic::Code::NotFound, "{failed:?}");
    let journaled: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM workflow_execution_journal WHERE workflow_id = $1::text",
    )
    .bind(id)
    .fetch_one(&engine.pool)
    .await
    .unwrap();
    assert_eq!(
        journaled, 0,
        "nothing is journaled for a task that does not exist"
    );

    engine.cleanup(&[id]).await;
}

#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_failed_lookup_is_unavailable() {
    let engine = Engine::new().await;
    let (id, execution_id) = engine.claimed_workflow("lookup-fails").await;
    let task_id = "charge_0";
    let token = started_attempt(&engine, id, &execution_id, task_id, 1).await;
    let cleanup = Engine::new().await;

    // Every repository shares this pool, so closing it fails the lookup.
    engine.pool.close().await;

    let completed = engine
        .service
        .complete_task_execution(completion(&token, "anything"))
        .await
        .expect_err("a completion that cannot load its task fails");
    assert_eq!(completed.code(), tonic::Code::Unavailable, "{completed:?}");

    let beat = engine
        .service
        .record_task_heartbeat(heartbeat(&token))
        .await
        .expect_err("a heartbeat that cannot load its task fails");
    assert_eq!(beat.code(), tonic::Code::Unavailable, "{beat:?}");

    assert_eq!(attempt_status(&cleanup.pool, &token).await.0, "started");
    assert_eq!(replay_row(&cleanup.pool, id, task_id).await, None);
    cleanup.cleanup(&[id]).await;
}

#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_heartbeat_for_a_finished_task_is_a_failed_precondition() {
    let engine = Engine::new().await;
    let (id, execution_id) = engine.claimed_workflow("finished-heartbeat").await;
    let token = started_attempt(&engine, id, &execution_id, "charge_0", 1).await;
    engine
        .service
        .complete_task_execution(completion(&token, "done"))
        .await
        .expect("complete the task");

    let beat = engine
        .service
        .record_task_heartbeat(heartbeat(&token))
        .await
        .expect_err("a finished task takes no heartbeat");
    assert_eq!(beat.code(), tonic::Code::FailedPrecondition, "{beat:?}");

    engine.cleanup(&[id]).await;
}
