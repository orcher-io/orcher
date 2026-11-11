//! A task dispatch is claimed by its own queue's pollers, once, and only while
//! its task is waiting.
//!
//! * The claim kept only the oldest pending dispatch of each workflow, across
//!   every queue, and a poll released what was not its own queue's. A
//!   workflow with a task on a queue nobody serves had its tasks on every
//!   other queue held back behind it, for as long as nobody served it.
//! * A task handed out eagerly, with the activation that scheduled it, left
//!   its dispatch pending; and a dispatch a poll delivered stayed claimed, to
//!   be put back by the stuck-task sweep and claimed again every few minutes.
//!   A dispatch is now open exactly while its task is waiting: taking the task
//!   settles it, and the eager reclaimer, putting the task back, opens it.
//!
//! Requires PostgreSQL. Run with:
//!
//! ```bash
//! export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/orcher_test"
//! cargo test -p orcher-core --test claim_per_queue -- --ignored --test-threads=1
//! ```

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use orcher_common::types::identifiers::{ExecutionId, WorkflowId};
use orcher_common::types::time::Timestamptz;
use orcher_core::engine::dispatch::task_manager::{
    PollTaskRequest, PollTaskResult, TaskPollingManager,
};
use orcher_core::engine::dispatch::PollingManager;
use orcher_core::engine::queues::outbox::OutboxQueue;
use orcher_core::engine::{OutboxTaskType, WorkflowCoordinator};
use orcher_core::events::EventBus;
use orcher_core::grpc::services::ExecutionService;
use orcher_core::workflows::{
    ChildWorkflowManager, CommandProcessor, JournalService, WorkflowExecutionService,
};
use orcher_events::bus::EventBusConfig;
use orcher_proto::orcher::v1::execution_service_server::ExecutionService as ExecutionServiceTrait;
use orcher_proto::orcher::v1::PollTaskExecutionRequest;
use orcher_storage::repositories::domains::{
    child_workflow::ChildWorkflowRepository, coordination::OutboxRepository,
    journal::JournalRepository, task_execution::TaskExecutionRepository,
    workflow_execution::WorkflowExecutionRepository,
};
use orcher_storage::repositories::{ExecutionLogRepository, PostgresExecutionLogRepository};
use orcher_storage::schema::types::TaskExecution;
use sqlx::PgPool;
use tonic::Request;
use uuid::Uuid;

const NAMESPACE: &str = "default";
const WORKER: &str = "worker-a";

async fn pool() -> PgPool {
    let url = std::env::var("TEST_DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:postgres@localhost:5432/orcher_test".to_string()
    });
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(20)
        .connect(&url)
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
    service: ExecutionService,
    tasks: Arc<TaskPollingManager>,
    coordinator: Arc<WorkflowCoordinator>,
}

/// An engine on `pool` delivering tasks from the outbox, the default.
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
    let tasks = Arc::new(TaskPollingManager::new(outbox_queue));
    let service = ExecutionService::new(
        Arc::new(command_processor),
        polling_manager,
        execution_log_repo as Arc<dyn ExecutionLogRepository>,
        journal_repo,
        exec_repo,
        task_execution_repo,
        pool.clone(),
    )
    .with_task_polling_manager(tasks.clone());
    Engine {
        service,
        tasks,
        coordinator,
    }
}

fn queue(name: &str) -> String {
    format!("claim-{name}-{}", Uuid::new_v4().simple())
}

/// A running workflow, parked on the tasks it scheduled.
async fn workflow(pool: &PgPool, task_queue: &str) -> (Uuid, String) {
    workflow_of(pool, task_queue, None).await
}

/// [`workflow`], owned by `organization`.
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
            id, execution_id, workflow_name, workflow_version, namespace, task_queue,
            status, parameters, context, environment, progress,
            total_steps, completed_steps, failed_steps, retried_steps,
            claimed_by, claimed_at, claim_timeout_at, activation_seq, completed_activation,
            organization_id
        )
        VALUES ($1, $2, 'claim-per-queue', '1.0.0', $3, $4, 'running', '{}', '{}', '{}', '{}',
                0, 0, 0, 0, $5, NOW(), NOW() + INTERVAL '5 minutes', 1, 1, $6)
        "#,
    )
    .bind(id)
    .bind(&execution_id)
    .bind(NAMESPACE)
    .bind(task_queue)
    .bind(WORKER)
    .bind(organization)
    .execute(pool)
    .await
    .expect("insert a workflow");
    (id, execution_id)
}

async fn organization(pool: &PgPool) -> Uuid {
    let owner = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO users (id, email, password_hash, hash_algorithm, password_changed_at) \
         VALUES ($1, $2, 'x', 'argon2', NOW())",
    )
    .bind(owner)
    .bind(format!("owner-{owner}@claim-per-queue.test"))
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

/// Claim everything an organization's pollers can, `pollers` of them at once,
/// each claiming `batch` at a time; the task ids, in the order taken when
/// there is one poller.
async fn claim_all(
    pool: &PgPool,
    organization: Option<Uuid>,
    task_queue: &str,
    pollers: usize,
    batch: usize,
) -> Vec<String> {
    let repo = Arc::new(OutboxRepository::new(Arc::new(pool.clone())));
    let start = Arc::new(tokio::sync::Barrier::new(pollers));
    let handles: Vec<_> = (0..pollers)
        .map(|_| {
            let repo = repo.clone();
            let start = start.clone();
            let task_queue = task_queue.to_string();
            tokio::spawn(async move {
                start.wait().await;
                let mut taken = Vec::new();
                loop {
                    let claimed = repo
                        .claim_dispatch_tasks(organization, batch, &task_queue, NAMESPACE)
                        .await
                        .expect("claim");
                    if claimed.is_empty() {
                        return taken;
                    }
                    taken.extend(
                        claimed
                            .into_iter()
                            .map(|t| t.payload["task_id"].as_str().unwrap().to_string()),
                    );
                }
            })
        })
        .collect();
    let mut taken = Vec::new();
    for handle in handles {
        taken.extend(handle.await.expect("poller"));
    }
    taken
}

/// Schedule `task_id` on `task_queue` for the workflow and dispatch it, as an
/// activation that schedules it does.
async fn dispatched(
    engine: &Engine,
    pool: &PgPool,
    (id, execution_id): &(Uuid, String),
    task_queue: &str,
    task_id: &str,
) -> TaskExecution {
    let task = TaskExecutionRepository::new(pool.clone())
        .create_scheduled(
            task_id.to_string(),
            "Echo".to_string(),
            WorkflowId::from_uuid(*id),
            ExecutionId::from_uuid(Uuid::parse_str(execution_id).unwrap()),
            task_queue.to_string(),
            NAMESPACE.to_string(),
            1,
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
    engine
        .coordinator
        .dispatch_task(
            *id,
            OutboxTaskType::DispatchTask,
            WorkflowCoordinator::task_dispatch_payload(&task),
        )
        .await
        .expect("dispatch the task");
    task
}

/// What a poll of `task_queue` is handed, from the outbox: the task id.
async fn poll(engine: &Engine, task_queue: &str) -> Option<String> {
    let request = PollTaskRequest::new(
        task_queue,
        NAMESPACE,
        "task-worker",
        Duration::from_millis(300),
    );
    match engine.tasks.poll_task(request).await.expect("poll") {
        Some(PollTaskResult::Claimed(task)) => task.payload["task_id"].as_str().map(str::to_string),
        Some(PollTaskResult::SyncMatched(_)) => panic!("nothing here sync-matches"),
        None => None,
    }
}

/// What a worker's poll of `task_queue` through the service is handed: the
/// task id, empty when nothing.
async fn worker_poll(engine: &Engine, task_queue: &str) -> String {
    engine
        .service
        .poll_task_execution(Request::new(PollTaskExecutionRequest {
            task_queue: task_queue.to_string(),
            namespace: NAMESPACE.to_string(),
            identity: format!("task-worker-{}", Uuid::new_v4().simple()),
            poll_timeout: Some(orcher_proto::prost_types::Duration {
                seconds: 1,
                nanos: 0,
            }),
            ..Default::default()
        }))
        .await
        .expect("poll the service")
        .into_inner()
        .task_id
}

/// The status and claim count of the workflow's dispatch of `task_id`.
async fn dispatch_state(pool: &PgPool, id: Uuid, task_id: &str) -> (String, i32) {
    sqlx::query_as(
        "SELECT status, attempt_count FROM outbox_queue \
         WHERE workflow_id = $1 AND task_type = 'dispatch_task' AND payload->>'task_id' = $2",
    )
    .bind(id)
    .bind(task_id)
    .fetch_one(pool)
    .await
    .expect("read the dispatch")
}

async fn task_status(pool: &PgPool, task: &TaskExecution) -> String {
    sqlx::query_scalar("SELECT status FROM task_executions WHERE id = $1")
        .bind(task.id.as_uuid())
        .fetch_one(pool)
        .await
        .expect("read the task")
}

/// Hand `task` over eagerly, with a claim TTL already run out when
/// `ttl_passed`.
async fn eagerly_started(pool: &PgPool, task: &TaskExecution, ttl_passed: bool) {
    let token =
        orcher_core::grpc::task_token::encode(&orcher_core::grpc::task_token::NewTaskToken {
            workflow_id: &task.workflow_id.as_uuid().to_string(),
            execution_id: &task.execution_id.as_uuid().to_string(),
            task_id: &task.task_id,
            attempt: task.attempt_number,
            started_at_ms: Timestamptz::now().timestamp_millis(),
            organization_id: None,
        });
    let expires_at = if ttl_passed {
        Timestamptz::now() - Duration::from_secs(1)
    } else {
        Timestamptz::now() + Duration::from_secs(60)
    };
    TaskExecutionRepository::new(pool.clone())
        .mark_started_eager(task.id, WORKER.to_string(), token, expires_at, None)
        .await
        .expect("hand the task over eagerly");
}

async fn cleanup(pool: &PgPool, ids: &[Uuid]) {
    for id in ids {
        for q in [
            "DELETE FROM task_executions WHERE workflow_id = $1",
            "DELETE FROM outbox_queue WHERE workflow_id = $1",
            "DELETE FROM workflow_executions WHERE id = $1",
        ] {
            let _ = sqlx::query(q).bind(id).execute(pool).await;
        }
    }
}

/// A workflow with a task on a queue nobody serves still has its task on a
/// served queue delivered, and the unserved one is left alone, not claimed
/// and put back by every poll of the other queue.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_dispatch_on_an_unserved_queue_holds_back_nothing_of_its_workflow() {
    let pool = pool().await;
    let engine = engine(&pool).await;
    let served = queue("served");
    let unserved = queue("unserved");
    let wf = workflow(&pool, &served).await;

    // The unserved queue's dispatch is the workflow's oldest.
    dispatched(&engine, &pool, &wf, &unserved, "nobody_serves_me").await;
    dispatched(&engine, &pool, &wf, &served, "served_0").await;

    assert_eq!(
        poll(&engine, &served).await.as_deref(),
        Some("served_0"),
        "the served queue's task was held back behind the unserved queue's"
    );
    assert_eq!(
        dispatch_state(&pool, wf.0, "nobody_serves_me").await,
        ("pending".to_string(), 0),
        "a poll of another queue claimed the unserved queue's dispatch"
    );
    assert_eq!(poll(&engine, &served).await, None, "delivered twice");

    cleanup(&pool, &[wf.0]).await;
}

/// A task handed out eagerly leaves no dispatch for a poll to claim; put back
/// by the reclaimer, its dispatch is open again and delivers it.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn an_eagerly_started_task_leaves_nothing_to_claim_until_it_is_put_back() {
    let pool = pool().await;
    let engine = engine(&pool).await;
    let task_queue = queue("eager");
    let wf = workflow(&pool, &task_queue).await;
    let eager = dispatched(&engine, &pool, &wf, &task_queue, "eager_0").await;

    eagerly_started(&pool, &eager, true).await;
    assert_eq!(
        dispatch_state(&pool, wf.0, "eager_0").await.0,
        "completed",
        "the eager hand-out left the task's dispatch open"
    );
    assert_eq!(
        poll(&engine, &task_queue).await,
        None,
        "a poll claimed the dispatch of a task already handed out"
    );

    // Its response lost and its TTL passed, the reclaimer puts it back.
    TaskExecutionRepository::new(pool.clone())
        .reclaim_expired_eager_tasks()
        .await
        .expect("run the eager reclaimer");
    assert_eq!(task_status(&pool, &eager).await, "scheduled");
    assert_eq!(
        worker_poll(&engine, &task_queue).await,
        "eager_0",
        "a task put back by the reclaimer was not delivered again"
    );
    assert_eq!(task_status(&pool, &eager).await, "started");

    cleanup(&pool, &[wf.0]).await;
}

/// A dispatch a worker's poll delivers is settled as the task is taken, and
/// one a poll cannot deliver, its task ended before it started, is settled
/// too: neither is left claimed for the stuck-task sweep to put back.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_dispatch_is_settled_once_its_task_is_not_waiting() {
    let pool = pool().await;
    let engine = engine(&pool).await;
    let task_queue = queue("settle");
    let wf = workflow(&pool, &task_queue).await;

    dispatched(&engine, &pool, &wf, &task_queue, "taken_0").await;
    assert_eq!(worker_poll(&engine, &task_queue).await, "taken_0");
    assert_eq!(
        dispatch_state(&pool, wf.0, "taken_0").await.0,
        "completed",
        "a delivered dispatch was left claimed"
    );

    let ended = dispatched(&engine, &pool, &wf, &task_queue, "ended_0").await;
    sqlx::query(
        "UPDATE task_executions SET status = 'failed', started_at = NOW(), completed_at = NOW() \
         WHERE id = $1",
    )
    .bind(ended.id.as_uuid())
    .execute(&pool)
    .await
    .expect("end the task before it starts");
    assert_eq!(worker_poll(&engine, &task_queue).await, "");
    assert_eq!(
        dispatch_state(&pool, wf.0, "ended_0").await.0,
        "completed",
        "an undeliverable dispatch was left claimed"
    );

    cleanup(&pool, &[wf.0]).await;
}

/// A queue's dispatches leave oldest first, whichever workflow they belong
/// to, and a workflow's own in the order they were made.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_queue_hands_out_its_oldest_dispatch_first() {
    let pool = pool().await;
    let engine = engine(&pool).await;
    let task_queue = queue("order");
    let a = workflow(&pool, &task_queue).await;
    let b = workflow(&pool, &task_queue).await;

    let mut made = Vec::new();
    for i in 0..3 {
        for (name, wf) in [("a", &a), ("b", &b)] {
            let task_id = format!("{name}_{i}");
            dispatched(&engine, &pool, wf, &task_queue, &task_id).await;
            made.push(task_id);
        }
    }

    let mut handed_out = Vec::new();
    while let Some(task_id) = poll(&engine, &task_queue).await {
        handed_out.push(task_id);
    }
    assert_eq!(handed_out, made, "not handed out oldest first");

    cleanup(&pool, &[a.0, b.0]).await;
}

/// Many pollers of one queue claiming at once never take the same dispatch
/// twice, and between them take every one.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn concurrent_pollers_never_take_the_same_dispatch() {
    let pool = pool().await;
    let engine = engine(&pool).await;
    let task_queue = queue("race");
    let mut workflows = Vec::new();
    let mut made = HashSet::new();
    for w in 0..4 {
        let wf = workflow(&pool, &task_queue).await;
        for t in 0..10 {
            let task_id = format!("w{w}_t{t}");
            dispatched(&engine, &pool, &wf, &task_queue, &task_id).await;
            made.insert(task_id);
        }
        workflows.push(wf.0);
    }

    let taken = claim_all(&pool, None, &task_queue, 8, 2).await;
    let distinct: HashSet<_> = taken.iter().cloned().collect();
    assert_eq!(distinct.len(), taken.len(), "a dispatch was taken twice");
    assert_eq!(distinct, made, "a dispatch was never taken");

    cleanup(&pool, &workflows).await;
}

/// An organization's polls take its dispatches, those stamped with it and
/// those written before dispatches were stamped alike, oldest first and each
/// once however many poll at a time, and none of another organization's on
/// the same queue.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn an_organizations_pollers_take_its_dispatches_oldest_first_and_once() {
    let pool = pool().await;
    let engine = engine(&pool).await;
    let task_queue = queue("tenant");
    let (mine, theirs) = (organization(&pool).await, organization(&pool).await);
    let a = workflow_of(&pool, &task_queue, Some(mine)).await;
    let b = workflow_of(&pool, &task_queue, Some(mine)).await;
    let other = workflow_of(&pool, &task_queue, Some(theirs)).await;

    // Every third of mine unstamped, as an engine before stamping wrote it.
    let mut made = Vec::new();
    for i in 0..24 {
        let wf = if i % 2 == 0 { &a } else { &b };
        let task_id = format!("mine_{i}");
        dispatched(&engine, &pool, wf, &task_queue, &task_id).await;
        if i % 3 == 0 {
            sqlx::query(
                "UPDATE outbox_queue SET organization_id = NULL \
                 WHERE workflow_id = $1 AND payload->>'task_id' = $2",
            )
            .bind(wf.0)
            .bind(&task_id)
            .execute(&pool)
            .await
            .expect("unstamp the dispatch");
        }
        dispatched(&engine, &pool, &other, &task_queue, &format!("theirs_{i}")).await;
        made.push(task_id);
    }

    // One poller, one at a time: in the order they were made.
    let first: Vec<String> = made[..6].to_vec();
    let mut in_order = Vec::new();
    for _ in 0..6 {
        let claimed = OutboxRepository::new(Arc::new(pool.clone()))
            .claim_dispatch_tasks(Some(mine), 1, &task_queue, NAMESPACE)
            .await
            .expect("claim");
        in_order.extend(
            claimed
                .into_iter()
                .map(|t| t.payload["task_id"].as_str().unwrap().to_string()),
        );
    }
    assert_eq!(in_order, first, "not handed out oldest first");

    // The rest, eight pollers at once.
    let taken = claim_all(&pool, Some(mine), &task_queue, 8, 1).await;
    let distinct: HashSet<_> = taken.iter().cloned().collect();
    assert_eq!(distinct.len(), taken.len(), "a dispatch was taken twice");
    assert_eq!(
        distinct,
        made[6..].iter().cloned().collect::<HashSet<_>>(),
        "not every dispatch of the organization was taken, or another's was"
    );

    cleanup(&pool, &[a.0, b.0, other.0]).await;
}
