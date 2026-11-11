//! A task that was scheduled is dispatched, once — even when the engine that
//! scheduled it stopped before dispatching it.
//!
//! A task row and its outbox dispatch used to be written in two transactions.
//! An engine killed between them left the row `scheduled` with no outbox row,
//! and the outbox row is the only thing that delivers a task to a worker. The
//! worker's SDK retried the completion on another engine; the ScheduleTask in
//! it found the row in flight and skipped it, as did every replay after, and
//! the workflow waited on the task for good.
//!
//! These tests put the database in that state — a first attempt scheduled,
//! never dispatched — and check that a replayed ScheduleTask and the
//! background sweep each dispatch it exactly once, and that neither dispatches
//! a task whose dispatch already exists or may still be being written.
//!
//! Requires PostgreSQL. The schema is created by the engine's own migration
//! runner. Run with:
//!
//! ```bash
//! export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/orcher_test"
//! cargo test -p orcher-core --test undispatched_task -- --ignored --test-threads=1
//! ```

use std::sync::Arc;
use std::time::Duration;

use orcher_common::types::identifiers::{ExecutionId, WorkflowId};
use orcher_core::engine::dispatch::PollingManager;
use orcher_core::engine::queues::outbox::OutboxQueue;
use orcher_core::engine::{OutboxTaskType, WorkflowCoordinator};
use orcher_core::events::EventBus;
use orcher_core::grpc::services::ExecutionService;
use orcher_core::retries::{TaskRetryDispatcher, TaskRetryScheduler};
use orcher_core::workflows::{
    ChildWorkflowManager, CommandProcessor, JournalService, WorkflowExecutionService,
};
use orcher_events::bus::EventBusConfig;
use orcher_proto::orcher::v1::execution_service_server::ExecutionService as ExecutionServiceTrait;
use orcher_proto::orcher::v1::{
    command, Command, CommandType, CompleteWorkflowExecutionRequest, ScheduleTaskCommandAttributes,
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
const WORKER: &str = "worker-a";
const TASK_QUEUE: &str = "undispatched-tasks";

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

/// One engine: its completion path, and the parts of it the tests drive
/// directly.
struct Engine {
    service: Arc<ExecutionService>,
    executions: Arc<WorkflowExecutionRepository>,
    tasks: Arc<TaskExecutionRepository>,
    coordinator: Arc<WorkflowCoordinator>,
}

async fn engine(pool: &PgPool) -> Engine {
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
    .with_coordinator(coordinator.clone())
    .with_polling_manager(polling_manager.clone());

    let service = ExecutionService::new(
        Arc::new(command_processor),
        polling_manager,
        execution_log_repo as Arc<dyn ExecutionLogRepository>,
        journal_repo,
        exec_repo.clone(),
        task_execution_repo.clone(),
        pool.clone(),
    );
    Engine {
        service: Arc::new(service),
        executions: exec_repo,
        tasks: task_execution_repo,
        coordinator,
    }
}

/// A running workflow, unclaimed, alone on a workflow queue of its own.
async fn workflow(pool: &PgPool) -> (Uuid, String, String) {
    let id = Uuid::new_v4();
    let execution_id = Uuid::new_v4().to_string();
    let task_queue = format!("undispatched-{}", Uuid::new_v4().simple());
    sqlx::query(
        r#"
        INSERT INTO workflow_executions (
            id, execution_id, workflow_id, workflow_name, workflow_version, namespace,
            task_queue, status, parameters, context, environment, progress,
            total_steps, completed_steps, failed_steps, retried_steps
        )
        VALUES ($1, $2, $1, 'undispatched-task', '1.0.0', $3, $4, 'running',
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

/// Hand the workflow to the worker, as a poll does, and return the activation
/// token the poll would carry.
async fn claim(engine: &Engine, execution_id: &str, task_queue: &str) -> String {
    let seq = engine
        .executions
        .claim_and_get_next(task_queue, NAMESPACE, WORKER, 300, None, None)
        .await
        .expect("claim")
        .expect("the workflow is claimable")
        .activation_seq;
    orcher_core::grpc::activation_token::encode(execution_id, seq)
}

/// The claim lapses, as the reclaimer finds it when a worker goes quiet, so
/// the workflow is handed out — and replayed — again.
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
                task_queue: TASK_QUEUE.to_string(),
                input: b"{}".to_vec(),
                ..Default::default()
            },
        )),
    }
}

async fn complete(
    engine: &Engine,
    id: Uuid,
    execution_id: &str,
    token: String,
    commands: Vec<Command>,
) {
    engine
        .service
        .complete_workflow_execution(Request::new(CompleteWorkflowExecutionRequest {
            workflow_id: id.to_string(),
            execution_id: execution_id.to_string(),
            namespace: NAMESPACE.to_string(),
            identity: WORKER.to_string(),
            task_token: format!("{id}:{execution_id}").into_bytes(),
            stream_entry_id: token,
            commands,
            ..Default::default()
        }))
        .await
        .expect("the completion is accepted");
}

/// The state a killed engine left: the task row committed, `scheduled`, and
/// its dispatch never written. `age` backdates it, as found some time later.
async fn scheduled_never_dispatched(
    pool: &PgPool,
    engine: &Engine,
    id: Uuid,
    execution_id: &str,
    task_id: &str,
    age: Duration,
) -> Uuid {
    let row = engine
        .tasks
        .create_scheduled_once(
            task_id.to_string(),
            "charge".to_string(),
            WorkflowId::from_uuid(id),
            ExecutionId::from_uuid(Uuid::parse_str(execution_id).unwrap()),
            TASK_QUEUE.to_string(),
            NAMESPACE.to_string(),
            1,
            Some(b"{}".to_vec()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("insert the task row")
        .expect("the row is new");
    sqlx::query(
        "UPDATE task_executions SET scheduled_at = NOW() - make_interval(secs => $2) \
         WHERE id = $1",
    )
    .bind(row.id.as_uuid())
    .bind(age.as_secs_f64())
    .execute(pool)
    .await
    .expect("backdate the task row");
    assert_eq!(dispatches(pool, id, task_id).await, 0);
    row.id.as_uuid()
}

/// How many outbox dispatches exist for the workflow's task.
async fn dispatches(pool: &PgPool, id: Uuid, task_id: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM outbox_queue \
         WHERE workflow_id = $1 AND task_type = 'dispatch_task' AND payload->>'task_id' = $2",
    )
    .bind(id)
    .bind(task_id)
    .fetch_one(pool)
    .await
    .expect("count dispatches")
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

/// Two engines' retry dispatchers — the sweep runs in each — sweeping often,
/// so a test sees several sweeps from both in a few seconds.
fn sweepers(engines: [&Engine; 2], pool: &PgPool) -> Vec<tokio::task::JoinHandle<()>> {
    engines
        .into_iter()
        .map(|engine| {
            TaskRetryDispatcher::new(
                TaskRetryScheduler::new(pool.clone()),
                engine.coordinator.clone(),
            )
            .with_interval(Duration::from_millis(50))
            .start()
        })
        .collect()
}

/// Wait up to `limit` for the task to have a dispatch.
async fn wait_for_dispatch(pool: &PgPool, id: Uuid, task_id: &str, limit: Duration) {
    let deadline = tokio::time::Instant::now() + limit;
    while dispatches(pool, id, task_id).await == 0 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
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

/// The completion the killed engine was applying is retried on another, and
/// its ScheduleTask finds the task scheduled and never dispatched. It
/// dispatches it — once, however many times the workflow replays after.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_replayed_schedule_dispatches_a_task_that_never_was() {
    let pool = pool().await;
    let dead = engine(&pool).await;
    let survivor = engine(&pool).await;
    let (id, execution_id, task_queue) = workflow(&pool).await;

    let token = claim(&dead, &execution_id, &task_queue).await;
    scheduled_never_dispatched(
        &pool,
        &dead,
        id,
        &execution_id,
        "task-a",
        Duration::from_secs(0),
    )
    .await;

    // The SDK retries the completion, with the same token, on the survivor.
    complete(
        &survivor,
        id,
        &execution_id,
        token.clone(),
        vec![schedule("task-a")],
    )
    .await;
    assert_eq!(
        dispatches(&pool, id, "task-a").await,
        1,
        "the retried completion dispatches the task the dead engine did not"
    );

    // Every replay after re-emits the ScheduleTask; none dispatches it again.
    for _ in 0..3 {
        lapse_claim(&pool, id).await;
        let token = claim(&survivor, &execution_id, &task_queue).await;
        complete(
            &survivor,
            id,
            &execution_id,
            token,
            vec![schedule("task-a")],
        )
        .await;
    }
    assert_eq!(dispatches(&pool, id, "task-a").await, 1);
    assert_eq!(task_rows(&pool, &execution_id, "task-a").await, 1);

    cleanup(&pool, id, &execution_id).await;
}

/// A workflow parked on a task that was never dispatched is not replayed for
/// it, so nothing but the sweep can deliver it. Two engines sweep; the task is
/// dispatched once.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn the_sweep_dispatches_a_task_that_never_was_once() {
    let pool = pool().await;
    let a = engine(&pool).await;
    let b = engine(&pool).await;
    let (id, execution_id, _) = workflow(&pool).await;
    scheduled_never_dispatched(
        &pool,
        &a,
        id,
        &execution_id,
        "task-b",
        Duration::from_secs(300),
    )
    .await;

    let running = sweepers([&a, &b], &pool);
    wait_for_dispatch(&pool, id, "task-b", Duration::from_secs(5)).await;
    // Let both go round several more times.
    tokio::time::sleep(Duration::from_secs(2)).await;
    running.iter().for_each(|h| h.abort());

    assert_eq!(
        dispatches(&pool, id, "task-b").await,
        1,
        "the sweep dispatches a task that was never dispatched, once"
    );

    cleanup(&pool, id, &execution_id).await;
}

/// A task whose dispatch exists — written by an engine from before dispatches
/// were marked on the row, or with the row as they are now — is dispatched by
/// neither a replay nor the sweep; nor is a task too young for the sweep, whose
/// dispatch may still be being written.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_task_whose_dispatch_exists_is_not_dispatched_again() {
    let pool = pool().await;
    let a = engine(&pool).await;
    let b = engine(&pool).await;
    let (id, execution_id, task_queue) = workflow(&pool).await;

    // Written the old way: the row, then its dispatch on its own.
    let old = scheduled_never_dispatched(
        &pool,
        &a,
        id,
        &execution_id,
        "task-old",
        Duration::from_secs(300),
    )
    .await;
    let row = a
        .tasks
        .find_by_id(orcher_common::types::identifiers::TaskExecutionId::from_uuid(old))
        .await
        .expect("read the row")
        .expect("the row exists");
    a.coordinator
        .dispatch_task(
            id,
            OutboxTaskType::DispatchTask,
            serde_json::json!({
                "workflow_id": id.to_string(),
                "execution_id": execution_id,
                "task_id": row.task_id,
                "task_type": row.task_type,
                "task_queue": row.task_queue,
                "namespace": row.namespace,
                "input": row.input.clone().unwrap_or_default(),
                "task_execution_id": old.to_string(),
            }),
        )
        .await
        .expect("dispatch it");

    // Written the way a ScheduleTask writes it.
    let token = claim(&a, &execution_id, &task_queue).await;
    complete(&a, id, &execution_id, token, vec![schedule("task-new")]).await;

    // Scheduled a moment ago, its dispatch not written yet.
    scheduled_never_dispatched(
        &pool,
        &a,
        id,
        &execution_id,
        "task-young",
        Duration::from_secs(0),
    )
    .await;

    for task in ["task-old", "task-new"] {
        assert_eq!(dispatches(&pool, id, task).await, 1, "{task}");
    }
    // A ScheduleTask commits the row already marked dispatched, because its
    // dispatch commits with it; the sweep never has cause to look at it.
    let marked: bool = sqlx::query_scalar(
        "SELECT last_dispatched_at IS NOT NULL FROM task_executions \
         WHERE execution_id = $1::uuid AND task_id = 'task-new'",
    )
    .bind(&execution_id)
    .fetch_one(&pool)
    .await
    .expect("read the row");
    assert!(marked, "the row and its dispatch are committed together");

    // A replay re-emits both.
    lapse_claim(&pool, id).await;
    let token = claim(&b, &execution_id, &task_queue).await;
    complete(
        &b,
        id,
        &execution_id,
        token,
        vec![schedule("task-old"), schedule("task-new")],
    )
    .await;

    // And two engines sweep.
    let running = sweepers([&a, &b], &pool);
    tokio::time::sleep(Duration::from_secs(3)).await;
    running.iter().for_each(|h| h.abort());

    for task in ["task-old", "task-new"] {
        assert_eq!(
            dispatches(&pool, id, task).await,
            1,
            "{task} is not dispatched again"
        );
        assert_eq!(task_rows(&pool, &execution_id, task).await, 1, "{task}");
    }
    assert_eq!(
        dispatches(&pool, id, "task-young").await,
        0,
        "a row younger than the grace is left to the engine writing its dispatch"
    );

    cleanup(&pool, id, &execution_id).await;
}

/// An outbox row as the 0.4.1 engine wrote a first attempt's dispatch: its
/// key spelled out as that engine built it, its payload without an attempt
/// number. `completed` as the outbox processor leaves it, `age` ago.
async fn outbox_row_as_0_4_1_wrote_it(
    pool: &PgPool,
    id: Uuid,
    execution_id: &str,
    task_id: &str,
    task_execution_id: Uuid,
    age: Duration,
) {
    let key = format!("{}:{}:dispatch_task:{}:1", id, execution_id, task_id);
    sqlx::query(
        r#"
        INSERT INTO outbox_queue (workflow_id, task_type, payload, idempotency_key, status,
                                  created_at, completed_at)
        VALUES ($1, 'dispatch_task', $2, $3, 'completed',
                NOW() - make_interval(secs => $4), NOW() - make_interval(secs => $4))
        "#,
    )
    .bind(id)
    .bind(serde_json::json!({
        "workflow_id": id.to_string(),
        "execution_id": execution_id,
        "task_id": task_id,
        "task_type": "charge",
        "task_queue": TASK_QUEUE,
        "namespace": NAMESPACE,
        "input": [123, 125],
        "task_execution_id": task_execution_id.to_string(),
    }))
    .bind(key)
    .bind(age.as_secs_f64())
    .execute(pool)
    .await
    .expect("insert the 0.4.1 outbox row");
}

/// What data retention does to a completed outbox row.
async fn retention_deletes_outbox_row(pool: &PgPool, id: Uuid, task_id: &str) {
    sqlx::query(
        "DELETE FROM outbox_queue WHERE workflow_id = $1 AND payload->>'task_id' = $2 \
         AND status = 'completed'",
    )
    .bind(id)
    .bind(task_id)
    .execute(pool)
    .await
    .expect("delete the outbox row");
}

/// Upgrading to this engine: V050 runs, backfill and all.
async fn upgrade(pool: &PgPool) {
    sqlx::query("DELETE FROM _sqlx_migrations WHERE version = 50")
        .execute(pool)
        .await
        .expect("forget V050");
    orcher_storage::setup::run_migrations(pool)
        .await
        .expect("apply V050 again");
}

fn secs(d: jiff::SignedDuration) -> Duration {
    Duration::from_secs(d.as_secs() as u64)
}

/// A first attempt 0.4.1 dispatched without marking its row, that no worker
/// served before data retention deleted its completed outbox row, looks like a
/// task never dispatched. It is not dispatched again: V050 marks such a row
/// while its outbox row is still there, and a row old enough that its outbox
/// row may have gone before the upgrade is beyond the repair window. A task
/// that truly never was dispatched, just inside the window, still is.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_dispatched_task_whose_outbox_row_retention_deleted_is_not_dispatched_again() {
    let pool = pool().await;
    let a = engine(&pool).await;
    let b = engine(&pool).await;
    // As the orchestrator configures them, from retention_days.outbox_queue.
    let retention_days = 7;
    a.coordinator.set_outbox_retention_days(retention_days);
    b.coordinator.set_outbox_retention_days(retention_days);
    let window = secs(orcher_core::engine::undispatched_repair_window(
        retention_days,
    ));
    let inside = window - Duration::from_secs(3600);
    let past = window + Duration::from_secs(3600);
    let (id, execution_id, task_queue) = workflow(&pool).await;

    // Dispatched by 0.4.1 just inside the window; its outbox row is deleted
    // after the upgrade.
    let row =
        scheduled_never_dispatched(&pool, &a, id, &execution_id, "served-inside", inside).await;
    outbox_row_as_0_4_1_wrote_it(&pool, id, &execution_id, "served-inside", row, inside).await;
    // Dispatched by 0.4.1 just past the window; its outbox row was deleted
    // before the upgrade.
    let row = scheduled_never_dispatched(&pool, &a, id, &execution_id, "served-past", past).await;
    outbox_row_as_0_4_1_wrote_it(&pool, id, &execution_id, "served-past", row, past).await;
    retention_deletes_outbox_row(&pool, id, "served-past").await;
    // Never dispatched, just inside the window.
    scheduled_never_dispatched(&pool, &a, id, &execution_id, "orphan-inside", inside).await;

    upgrade(&pool).await;
    retention_deletes_outbox_row(&pool, id, "served-inside").await;
    for task in ["served-inside", "served-past", "orphan-inside"] {
        assert_eq!(dispatches(&pool, id, task).await, 0, "{task}");
    }

    // The workflow replays, re-emitting all three, and two engines sweep.
    let token = claim(&b, &execution_id, &task_queue).await;
    complete(
        &b,
        id,
        &execution_id,
        token,
        vec![
            schedule("served-inside"),
            schedule("served-past"),
            schedule("orphan-inside"),
        ],
    )
    .await;
    let running = sweepers([&a, &b], &pool);
    tokio::time::sleep(Duration::from_secs(3)).await;
    running.iter().for_each(|h| h.abort());

    assert_eq!(
        dispatches(&pool, id, "served-inside").await,
        0,
        "a task 0.4.1 dispatched is not dispatched again once retention deletes its outbox row"
    );
    assert_eq!(
        dispatches(&pool, id, "served-past").await,
        0,
        "a task past the repair window is not dispatched, whatever its outbox shows"
    );
    assert_eq!(
        dispatches(&pool, id, "orphan-inside").await,
        1,
        "a task never dispatched, inside the window, is dispatched once"
    );

    cleanup(&pool, id, &execution_id).await;
}

/// Checking a task in flight for a missing dispatch is not the completion's
/// business: when it fails, the completion is still applied, and the sweep
/// dispatches the task afterwards.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_failed_dispatch_check_does_not_fail_the_completion() {
    let pool = pool().await;
    let dead = engine(&pool).await;
    let survivor = engine(&pool).await;
    let (id, execution_id, task_queue) = workflow(&pool).await;
    let token = claim(&dead, &execution_id, &task_queue).await;
    scheduled_never_dispatched(
        &pool,
        &dead,
        id,
        &execution_id,
        "task-refused",
        Duration::from_secs(300),
    )
    .await;

    // The outbox refuses this workflow's rows for now.
    let refuse = format!("undispatched_refuse_{}", id.simple());
    sqlx::raw_sql(&format!(
        "CREATE FUNCTION {refuse}() RETURNS trigger LANGUAGE plpgsql AS \
         $$ BEGIN RAISE EXCEPTION 'outbox unavailable'; END $$; \
         CREATE TRIGGER {refuse} BEFORE INSERT ON outbox_queue FOR EACH ROW \
         WHEN (NEW.workflow_id = '{id}') EXECUTE FUNCTION {refuse}();"
    ))
    .execute(&pool)
    .await
    .expect("make the outbox refuse the workflow's rows");

    // `complete` fails the test if the completion is refused.
    complete(
        &survivor,
        id,
        &execution_id,
        token,
        vec![schedule("task-refused")],
    )
    .await;
    assert_eq!(dispatches(&pool, id, "task-refused").await, 0);

    sqlx::raw_sql(&format!(
        "DROP TRIGGER {refuse} ON outbox_queue; DROP FUNCTION {refuse}();"
    ))
    .execute(&pool)
    .await
    .expect("let the outbox take the rows again");

    let running = sweepers([&dead, &survivor], &pool);
    wait_for_dispatch(&pool, id, "task-refused", Duration::from_secs(5)).await;
    running.iter().for_each(|h| h.abort());
    assert_eq!(
        dispatches(&pool, id, "task-refused").await,
        1,
        "the sweep dispatches what the completion could not"
    );

    cleanup(&pool, id, &execution_id).await;
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
        "{}_undispatched_shard",
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

/// With a workflow's shard on a database of its own, a task left undispatched
/// there is found by the sweep, claimed, and dispatched there — the claim and
/// the outbox row in one transaction on the shard's database, none on the
/// other.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_task_on_a_shard_database_is_dispatched_there() {
    use orcher_cluster::{MultiPoolRouter, ShardId, ShardRouter};

    let pool = pool().await;
    let shard_db = shard_database(&pool).await;
    let vshards = 16;
    let router = Arc::new(MultiPoolRouter::with_vshards(
        vec![pool.clone(), shard_db.clone()],
        vshards,
    ));
    let a = engine(&pool).await;
    a.coordinator.set_shard_router(router.clone());

    // A workflow whose shard lives on the second database.
    let id = std::iter::repeat_with(Uuid::new_v4)
        .find(|id| {
            std::ptr::eq(
                ShardRouter::pool_for_workflow(router.as_ref(), &id.to_string()),
                ShardRouter::all_pools(router.as_ref())[1],
            )
        })
        .unwrap();
    let execution_id = Uuid::new_v4().to_string();
    sqlx::query(
        r#"
        INSERT INTO workflow_executions (
            id, execution_id, workflow_id, workflow_name, workflow_version, namespace,
            task_queue, status, parameters, context, environment, progress,
            total_steps, completed_steps, failed_steps, retried_steps
        )
        VALUES ($1, $2, $1, 'undispatched-task', '1.0.0', $3, 'undispatched-shard', 'running',
                '{}', '{}', '{}', '{}', 0, 0, 0, 0)
        "#,
    )
    .bind(id)
    .bind(&execution_id)
    .bind(NAMESPACE)
    .execute(&shard_db)
    .await
    .expect("insert the workflow on its shard's database");
    let row = TaskExecutionRepository::new(shard_db.clone())
        .create_scheduled_once(
            "task-sharded".to_string(),
            "charge".to_string(),
            WorkflowId::from_uuid(id),
            ExecutionId::from_uuid(Uuid::parse_str(&execution_id).unwrap()),
            TASK_QUEUE.to_string(),
            NAMESPACE.to_string(),
            1,
            Some(b"{}".to_vec()),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("insert the task row")
        .expect("the row is new");
    sqlx::query(
        "UPDATE task_executions SET scheduled_at = NOW() - interval '5 minutes' WHERE id = $1",
    )
    .bind(row.id.as_uuid())
    .execute(&shard_db)
    .await
    .expect("backdate the task row");

    let scheduler = TaskRetryScheduler::new(pool.clone());
    scheduler.set_shard_resolver(router.clone());
    let running = TaskRetryDispatcher::new(scheduler, a.coordinator.clone())
        .with_interval(Duration::from_millis(50))
        .start();
    wait_for_dispatch(&shard_db, id, "task-sharded", Duration::from_secs(5)).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    running.abort();

    assert_eq!(dispatches(&shard_db, id, "task-sharded").await, 1);
    assert_eq!(
        dispatches(&pool, id, "task-sharded").await,
        0,
        "nothing is written to a database the workflow does not live on"
    );
    let (marked, shard_id): (bool, Option<i32>) = sqlx::query_as(
        "SELECT t.last_dispatched_at IS NOT NULL, o.shard_id FROM task_executions t \
         JOIN outbox_queue o ON o.payload->>'task_execution_id' = t.id::text \
         WHERE t.id = $1",
    )
    .bind(row.id.as_uuid())
    .fetch_one(&shard_db)
    .await
    .expect("read the row and its dispatch");
    assert!(marked, "the claim and the dispatch commit together");
    assert_eq!(
        shard_id,
        Some(ShardId::of(&id.to_string(), vshards).0 as i32)
    );

    cleanup(&shard_db, id, &execution_id).await;
}
