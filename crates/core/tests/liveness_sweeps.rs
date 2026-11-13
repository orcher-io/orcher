//! Work that stalled, or piled up, because nothing came back for it.
//!
//! * An eagerly dispatched task was reset to `scheduled` ten seconds after it
//!   was handed over, however long it ran: the engine is never told that a
//!   worker began it, and the reset handed it out again and left the first
//!   worker's completion with nothing to complete.
//! * A workflow whose task outcome was recorded but whose wake-up was lost —
//!   an engine stopping between the two — stayed parked until its claim timed
//!   out, five minutes later.
//! * A matching partition row stayed for good once claimed: nothing removed a
//!   delivered one, and nothing returned one whose lease ran out.
//!
//! Requires PostgreSQL. Run with:
//!
//! ```bash
//! export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/orcher_test"
//! cargo test -p orcher-core --test liveness_sweeps -- --ignored --test-threads=1
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
use orcher_core::workflows::{
    ChildWorkflowManager, CommandProcessor, JournalService, WorkflowExecutionService,
};
use orcher_events::bus::EventBusConfig;
use orcher_proto::orcher::v1::execution_service_server::ExecutionService as ExecutionServiceTrait;
use orcher_proto::orcher::v1::{
    CompleteTaskExecutionRequest, PollTaskExecutionRequest, RecordTaskHeartbeatRequest,
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
/// `matching` is given.
async fn engine(
    pool: &PgPool,
    matching: Option<Arc<dyn MatchingStore>>,
) -> (ExecutionService, Arc<PollingManager>) {
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
    if let Some(store) = matching {
        task_polling_manager.set_matching_store(store);
    }
    let service = ExecutionService::new(
        Arc::new(command_processor),
        polling_manager.clone(),
        execution_log_repo as Arc<dyn ExecutionLogRepository>,
        journal_repo,
        exec_repo,
        task_execution_repo,
        pool.clone(),
    )
    .with_task_polling_manager(task_polling_manager);
    (service, polling_manager)
}

fn queue() -> String {
    format!("liveness-{}", Uuid::new_v4().simple())
}

/// A running workflow, claimed `claimed_secs_ago`, whose last activation has
/// been applied: parked on whatever it waits for.
async fn parked_workflow(pool: &PgPool, task_queue: &str, claimed_secs_ago: i64) -> (Uuid, String) {
    let id = Uuid::new_v4();
    let execution_id = Uuid::new_v4().to_string();
    sqlx::query(
        r#"
        INSERT INTO workflow_executions (
            id, execution_id, workflow_name, workflow_version, namespace, task_queue,
            status, parameters, context, environment, progress,
            total_steps, completed_steps, failed_steps, retried_steps,
            claimed_by, claimed_at, claim_timeout_at, activation_seq, completed_activation
        )
        VALUES ($1, $2, 'liveness', '1.0.0', $3, $4, 'running', '{}', '{}', '{}', '{}',
                0, 0, 0, 0, $5, NOW() - make_interval(secs => $6),
                NOW() + INTERVAL '5 minutes', 1, 1)
        "#,
    )
    .bind(id)
    .bind(&execution_id)
    .bind(NAMESPACE)
    .bind(task_queue)
    .bind(WORKER)
    .bind(claimed_secs_ago as f64)
    .execute(pool)
    .await
    .expect("insert a parked workflow");
    (id, execution_id)
}

/// A scheduled task of the workflow, with `start_to_close_secs` as its own
/// timeout.
async fn scheduled_task(
    pool: &PgPool,
    id: Uuid,
    execution_id: &str,
    task_queue: &str,
    task_id: &str,
    start_to_close_secs: Option<i32>,
) -> orcher_storage::schema::types::TaskExecution {
    TaskExecutionRepository::new(pool.clone())
        .create_scheduled(
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
            start_to_close_secs,
            None,
            None,
            None,
        )
        .await
        .expect("schedule the task")
}

/// Hand `task` over eagerly with a claim TTL that has already run out: the
/// state of an eager task still running when its TTL passes.
async fn eagerly_started(
    pool: &PgPool,
    task: &orcher_storage::schema::types::TaskExecution,
) -> Vec<u8> {
    let token =
        orcher_core::grpc::task_token::encode(&orcher_core::grpc::task_token::NewTaskToken {
            workflow_id: &task.workflow_id.as_uuid().to_string(),
            execution_id: &task.execution_id.as_uuid().to_string(),
            task_id: &task.task_id,
            attempt: task.attempt_number,
            started_at_ms: Timestamptz::now().timestamp_millis(),
            organization_id: None,
        });
    TaskExecutionRepository::new(pool.clone())
        .mark_started_eager(
            task.id,
            WORKER.to_string(),
            token.clone(),
            Timestamptz::now() - Duration::from_secs(1),
            None,
        )
        .await
        .expect("hand the task over eagerly");
    token.into_bytes()
}

async fn task_state(pool: &PgPool, id: Uuid) -> (String, Option<String>) {
    sqlx::query_as("SELECT status, task_token FROM task_executions WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("read the task")
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

async fn claimed(pool: &PgPool, id: Uuid) -> bool {
    sqlx::query_scalar("SELECT claimed_by IS NOT NULL FROM workflow_executions WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("read the workflow")
}

/// Journal `event_type` for the workflow as recorded `secs_ago`, with no
/// wake-up following it.
async fn journaled_without_wake_up(
    pool: &PgPool,
    id: Uuid,
    execution_id: &str,
    event_type: &str,
    task_id: &str,
    secs_ago: i64,
) {
    sqlx::query(
        r#"
        INSERT INTO workflow_execution_journal
            (workflow_id, execution_id, event_id, event_type, timestamp, payload)
        VALUES ($1, $2,
                (SELECT COALESCE(MAX(event_id), 0) + 1 FROM workflow_execution_journal
                 WHERE execution_id = $2),
                $3, NOW() - make_interval(secs => $4), $5)
        "#,
    )
    .bind(id.to_string())
    .bind(execution_id)
    .bind(event_type)
    .bind(secs_ago as f64)
    .bind(serde_json::json!({ "task_id": task_id }))
    .execute(pool)
    .await
    .expect("journal the outcome");
}

async fn cleanup(pool: &PgPool, ids: &[Uuid]) {
    for id in ids {
        for q in [
            "DELETE FROM task_executions WHERE workflow_id = $1",
            "DELETE FROM execution_log WHERE workflow_id = $1",
            "DELETE FROM matching_partitions WHERE workflow_id = $1",
            "DELETE FROM outbox_queue WHERE workflow_id = $1",
        ] {
            let _ = sqlx::query(q).bind(id).execute(pool).await;
        }
        let _ = sqlx::query("DELETE FROM workflow_execution_journal WHERE workflow_id = $1")
            .bind(id.to_string())
            .execute(pool)
            .await;
        let _ = sqlx::query("DELETE FROM workflow_executions WHERE id = $1")
            .bind(id)
            .execute(pool)
            .await;
    }
}

/// An eager task with a timeout of its own, still running once its eager TTL
/// has passed, is left with its worker: not handed out again, and its
/// completion journaled when it comes.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn an_eager_task_running_past_its_ttl_is_not_run_again() {
    let pool = pool().await;
    let (service, _) = engine(&pool, None).await;
    let task_queue = queue();
    let (id, execution_id) = parked_workflow(&pool, &task_queue, 0).await;
    let task = scheduled_task(&pool, id, &execution_id, &task_queue, "slow_0", Some(300)).await;
    let token = eagerly_started(&pool, &task).await;

    TaskExecutionRepository::new(pool.clone())
        .reclaim_expired_eager_tasks()
        .await
        .expect("run the eager reclaimer");

    let (status, stored) = task_state(&pool, task.id.as_uuid()).await;
    assert_eq!(
        status, "started",
        "the running task was reset for another worker"
    );
    assert_eq!(stored.as_deref().map(str::as_bytes), Some(token.as_slice()));

    service
        .complete_task_execution(Request::new(CompleteTaskExecutionRequest {
            task_token: token,
            namespace: NAMESPACE.to_string(),
            identity: WORKER.to_string(),
            result: b"\"done\"".to_vec(),
        }))
        .await
        .expect("complete the task");
    assert_eq!(task_completed_count(&pool, id, "slow_0").await, 1);

    cleanup(&pool, &[id]).await;
}

/// A heartbeat tells the engine the worker has the task, so an eager task
/// with no timeout of its own is not reset once it has heartbeated; one that
/// never has — its hand-over possibly lost — still is.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_heartbeat_keeps_an_eager_task_with_its_worker() {
    let pool = pool().await;
    let (service, _) = engine(&pool, None).await;
    let task_queue = queue();
    let (id, execution_id) = parked_workflow(&pool, &task_queue, 0).await;
    let beating = scheduled_task(&pool, id, &execution_id, &task_queue, "beating_0", None).await;
    let silent = scheduled_task(&pool, id, &execution_id, &task_queue, "silent_0", None).await;
    let token = eagerly_started(&pool, &beating).await;
    eagerly_started(&pool, &silent).await;

    service
        .record_task_heartbeat(Request::new(RecordTaskHeartbeatRequest {
            task_token: token.clone(),
            namespace: NAMESPACE.to_string(),
            identity: WORKER.to_string(),
            ..Default::default()
        }))
        .await
        .expect("heartbeat");
    TaskExecutionRepository::new(pool.clone())
        .reclaim_expired_eager_tasks()
        .await
        .expect("run the eager reclaimer");

    let (status, stored) = task_state(&pool, beating.id.as_uuid()).await;
    assert_eq!(status, "started", "a task that heartbeated was reset");
    assert_eq!(stored.as_deref().map(str::as_bytes), Some(token.as_slice()));
    let (status, stored) = task_state(&pool, silent.id.as_uuid()).await;
    assert_eq!(
        status, "scheduled",
        "a hand-over nothing confirmed is still recovered"
    );
    assert_eq!(stored, None);

    cleanup(&pool, &[id]).await;
}

/// A parked workflow whose task outcome was recorded with no wake-up is woken
/// by the reclaimer's sweep within its interval, not left for the claim
/// timeout; one whose outcomes all predate its claim, or whose failed task has
/// a retry waiting, stays parked.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_lost_wake_up_is_recovered_by_the_sweep() {
    let pool = pool().await;
    let task_queue = queue();
    let (lost, lost_execution) = parked_workflow(&pool, &task_queue, 60).await;
    journaled_without_wake_up(&pool, lost, &lost_execution, "TaskCompleted", "a_0", 30).await;

    let (consumed, consumed_execution) = parked_workflow(&pool, &task_queue, 60).await;
    journaled_without_wake_up(
        &pool,
        consumed,
        &consumed_execution,
        "TaskCompleted",
        "a_0",
        90,
    )
    .await;

    let (retrying, retrying_execution) = parked_workflow(&pool, &task_queue, 60).await;
    scheduled_task(
        &pool,
        retrying,
        &retrying_execution,
        &task_queue,
        "b_0",
        None,
    )
    .await;
    journaled_without_wake_up(
        &pool,
        retrying,
        &retrying_execution,
        "TaskFailed",
        "b_0",
        30,
    )
    .await;

    let (_, polling_manager) = engine(&pool, None).await;
    let reclaimer = polling_manager.start().await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    while claimed(&pool, lost).await && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    reclaimer.abort();

    assert!(
        !claimed(&pool, lost).await,
        "the workflow whose wake-up was lost is still parked"
    );
    assert!(
        claimed(&pool, consumed).await,
        "a workflow with no outcome newer than its claim was woken"
    );
    assert!(
        claimed(&pool, retrying).await,
        "a workflow whose failed task is being retried was woken"
    );

    cleanup(&pool, &[lost, consumed, retrying]).await;
}

/// A fan-out is woken once its last task has finished, as the ordinary
/// wake-up would: not by each sibling's outcome while others still run, which
/// replayed it once per sweep for every sibling.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_fan_out_is_woken_only_when_its_last_task_has_finished() {
    let pool = pool().await;
    let task_queue = queue();
    let (id, execution_id) = parked_workflow(&pool, &task_queue, 60).await;
    let done = scheduled_task(&pool, id, &execution_id, &task_queue, "a_0", None).await;
    let pending = scheduled_task(&pool, id, &execution_id, &task_queue, "b_0", None).await;
    let finish = |task: Uuid| {
        let pool = pool.clone();
        async move {
            sqlx::query(
                "UPDATE task_executions SET status = 'completed', completed_at = NOW(), \
                 started_at = COALESCE(started_at, NOW()) WHERE id = $1",
            )
            .bind(task)
            .execute(&pool)
            .await
            .expect("finish the task");
        }
    };
    sqlx::query("UPDATE task_executions SET status = 'started', started_at = NOW() WHERE id = $1")
        .bind(pending.id.as_uuid())
        .execute(&pool)
        .await
        .expect("start the sibling");
    finish(done.id.as_uuid()).await;
    journaled_without_wake_up(&pool, id, &execution_id, "TaskCompleted", "a_0", 30).await;

    let executions = WorkflowExecutionRepository::new(pool.clone());
    let sweep = || {
        executions.wake_parked_with_unconsumed_outcomes(
            Timestamptz::now() - Duration::from_secs(300),
            10,
            200,
        )
    };
    sweep().await.expect("sweep");
    assert!(
        claimed(&pool, id).await,
        "woken while a sibling task is still running"
    );

    finish(pending.id.as_uuid()).await;
    journaled_without_wake_up(&pool, id, &execution_id, "TaskCompleted", "b_0", 30).await;
    let (woken, more) = sweep().await.expect("sweep");
    assert!(
        !claimed(&pool, id).await,
        "not woken once every task finished"
    );
    assert!(woken.iter().any(|w| w.execution_id == execution_id));
    assert!(!more, "a sweep under its batch reports more to read");

    cleanup(&pool, &[id]).await;
}

fn poll(task_queue: &str) -> Request<PollTaskExecutionRequest> {
    Request::new(PollTaskExecutionRequest {
        task_queue: task_queue.to_string(),
        namespace: NAMESPACE.to_string(),
        identity: format!("task-worker-{}", Uuid::new_v4().simple()),
        poll_timeout: Some(orcher_proto::prost_types::Duration {
            seconds: 1,
            nanos: 0,
        }),
        ..Default::default()
    })
}

/// A matching row for `task`, as the transfer from the outbox writes it.
async fn matching_row(
    store: &PgMatchingStore,
    task: &orcher_storage::schema::types::TaskExecution,
) -> Uuid {
    let row = MatchTask {
        task_id: Uuid::new_v4(),
        namespace: NAMESPACE.to_string(),
        task_queue: task.task_queue.clone(),
        workflow_id: task.workflow_id.as_uuid(),
        payload: serde_json::json!({
            "workflow_id": task.workflow_id.as_uuid().to_string(),
            "execution_id": task.execution_id.as_uuid().to_string(),
            "task_id": task.task_id,
            "task_type": task.task_type,
            "task_queue": task.task_queue,
            "namespace": NAMESPACE,
            "task_execution_id": task.id.to_string(),
        }),
        organization_id: None,
    };
    store.enqueue(&row).await.expect("enqueue");
    row.task_id
}

async fn matching_status(pool: &PgPool, task_id: Uuid) -> Option<String> {
    sqlx::query_scalar("SELECT status FROM matching_partitions WHERE task_id = $1")
        .bind(task_id)
        .fetch_optional(pool)
        .await
        .expect("read the matching row")
}

/// The row of a task handed to a worker is removed, and so is one whose task
/// another dispatch had already taken; neither stays claimed for good.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_delivered_tasks_matching_row_is_removed() {
    let pool = pool().await;
    let store = Arc::new(PgMatchingStore::new(pool.clone()));
    let (service, _) = engine(&pool, Some(store.clone())).await;
    let task_queue = queue();
    let (id, execution_id) = parked_workflow(&pool, &task_queue, 0).await;

    let task = scheduled_task(&pool, id, &execution_id, &task_queue, "echo_0", None).await;
    let row = matching_row(&store, &task).await;
    let polled = service
        .poll_task_execution(poll(&task_queue))
        .await
        .expect("poll")
        .into_inner();
    assert_eq!(polled.task_id, "echo_0");
    assert_eq!(
        matching_status(&pool, row).await,
        None,
        "the delivered task's row is still in its partition"
    );

    let taken = scheduled_task(&pool, id, &execution_id, &task_queue, "taken_0", None).await;
    eagerly_started(&pool, &taken).await;
    let row = matching_row(&store, &taken).await;
    let polled = service
        .poll_task_execution(poll(&task_queue))
        .await
        .expect("poll")
        .into_inner();
    assert!(
        polled.task_id.is_empty(),
        "a task already taken was handed out"
    );
    assert_eq!(
        matching_status(&pool, row).await,
        None,
        "the row of a task already taken is still in its partition"
    );

    cleanup(&pool, &[id]).await;
}

/// A row whose lease ran out before its task was taken is returned to its
/// partition by the polling reclaimer; one left claimed a whole retention
/// period past its lease is pruned by the retention job, and a row still
/// waiting is not.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn expired_matching_leases_are_returned_and_pruned() {
    let pool = pool().await;
    let store = Arc::new(PgMatchingStore::new(pool.clone()));
    let task_queue = queue();
    let (id, execution_id) = parked_workflow(&pool, &task_queue, 0).await;
    let task = scheduled_task(&pool, id, &execution_id, &task_queue, "lapsed_0", None).await;
    let lapsed = matching_row(&store, &task).await;
    let stale = matching_row(&store, &task).await;
    let waiting = matching_row(&store, &task).await;
    sqlx::query(
        "UPDATE matching_partitions SET status = 'claimed', lease_owner = 'gone', \
         lease_expires_at = NOW() - INTERVAL '1 second' WHERE task_id = $1",
    )
    .bind(lapsed)
    .execute(&pool)
    .await
    .expect("lapse a lease");
    sqlx::query(
        "UPDATE matching_partitions SET status = 'claimed', lease_owner = 'gone', \
         lease_expires_at = NOW() - INTERVAL '400 days', created_at = NOW() - INTERVAL '400 days' \
         WHERE task_id = $1",
    )
    .bind(stale)
    .execute(&pool)
    .await
    .expect("age a lease");
    sqlx::query(
        "UPDATE matching_partitions SET created_at = NOW() - INTERVAL '400 days' \
         WHERE task_id = $1",
    )
    .bind(waiting)
    .execute(&pool)
    .await
    .expect("age a waiting row");

    orcher_core::jobs::data_retention::run(&pool, &orcher_core::jobs::RetentionDays::default())
        .await
        .expect("run retention");
    assert_eq!(matching_status(&pool, stale).await, None, "not pruned");
    assert_eq!(
        matching_status(&pool, waiting).await.as_deref(),
        Some("ready"),
        "a row waiting to be delivered was pruned"
    );

    let (_, polling_manager) = engine(&pool, None).await;
    polling_manager.set_matching_store(store.clone());
    let reclaimer = polling_manager.start().await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    while matching_status(&pool, lapsed).await.as_deref() != Some("ready")
        && tokio::time::Instant::now() < deadline
    {
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    reclaimer.abort();
    assert_eq!(
        matching_status(&pool, lapsed).await.as_deref(),
        Some("ready"),
        "a lapsed lease was not returned to its partition"
    );

    cleanup(&pool, &[id]).await;
}
