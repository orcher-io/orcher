//! A worker process that dies holding an activation has it handed to another
//! worker within seconds, not after the claim timeout.
//!
//! A worker killed while it held an activation — just after it reported a
//! task's result, say, and the engine handed it the workflow's next step —
//! left the workflow claimed by the dead process for the whole claim timeout,
//! five minutes in production. Every engine now counts the polls open on it
//! by the process that sent them, records the processes it sees every refresh,
//! and releases what a process nobody has seen for a few refreshes held: its
//! unanswered activations, voided so that a late completion from it is not
//! applied, and its running task attempts once their heartbeats have stopped
//! too. A process is judged within the organization and namespace it polls.
//!
//! Requires PostgreSQL. The schema is created by the engine's own migration
//! runner. Run with:
//!
//! ```bash
//! export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/orcher_test"
//! cargo test -p orcher-core --test lapsed_worker_claims -- --ignored --test-threads=1
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};

use orcher_common::types::identifiers::{ExecutionId, WorkflowId};
use orcher_core::engine::dispatch::{
    LapsedWorkerClaims, LivenessSettings, PollingManager, TaskPollingManager,
};
use orcher_core::engine::queues::outbox::OutboxQueue;
use orcher_core::engine::WorkflowCoordinator;
use orcher_core::events::EventBus;
use orcher_core::grpc::services::ExecutionService;
use orcher_core::grpc::tenant::ORGANIZATION_METADATA_KEY;
use orcher_core::retries::{TimeoutWatcher, TimeoutWatcherConfig};
use orcher_core::workflows::{
    ChildWorkflowManager, CommandProcessor, JournalService, WorkflowExecutionService,
};
use orcher_events::bus::EventBusConfig;
use orcher_proto::orcher::v1::execution_service_server::ExecutionService as ExecutionServiceTrait;
use orcher_proto::orcher::v1::{
    command, Command, CommandType, CompleteWorkflowCommandAttributes,
    CompleteWorkflowExecutionRequest, PollTaskExecutionRequest, PollTaskExecutionResponse,
    PollWorkflowExecutionRequest, PollWorkflowExecutionResponse, RecordTaskHeartbeatRequest,
    StartTimerCommandAttributes, TaskCapabilities,
};
use orcher_storage::repositories::domains::matching::{MatchTask, MatchingStore, PgMatchingStore};
use orcher_storage::repositories::domains::worker::WorkerInstanceRepository;
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
/// What the claim timeout is in production, and here: nothing in these tests
/// waits for it, and no reclaimer runs.
const CLAIM_TIMEOUT_SECS: i64 = 300;
/// Each engine records what it sees every second here, and a process unseen
/// for three of those has lapsed.
const REFRESH: Duration = Duration::from_secs(1);
const MISSED_REFRESHES: u32 = 3;
/// The engine's default heartbeat timeout in the task test: long enough that
/// the heartbeat timeout itself could not be what times the attempt out first.
const DEFAULT_HEARTBEAT_SECS: u64 = 8;

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

fn lapse() -> Duration {
    REFRESH * MISSED_REFRESHES
}

/// One engine: its polls and completions, and its release of what lapsed
/// worker processes held, running as `main` runs it. The release stops when
/// this is dropped.
struct Engine {
    service: Arc<ExecutionService>,
    started: Instant,
    _stop: tokio::sync::oneshot::Sender<()>,
}

impl Engine {
    /// Wait until the release has watched long enough to judge anyone, so a
    /// test measures the release and not the warm-up.
    async fn warmed_up(&self) {
        let ready = lapse() + REFRESH * 2;
        let elapsed = self.started.elapsed();
        if elapsed < ready {
            tokio::time::sleep(ready - elapsed).await;
        }
    }
}

async fn engine(pool: &PgPool, shards: Option<Vec<PgPool>>) -> Engine {
    engine_with(pool, shards, None).await
}

async fn engine_with(
    pool: &PgPool,
    shards: Option<Vec<PgPool>>,
    matching: Option<Arc<PgMatchingStore>>,
) -> Engine {
    let journal_repo = Arc::new(JournalRepository::new(pool.clone()));
    let journal_service = Arc::new(JournalService::new(journal_repo.clone()));
    let exec_repo = Arc::new(WorkflowExecutionRepository::new(pool.clone()));
    let task_execution_repo = Arc::new(TaskExecutionRepository::new(pool.clone()));
    if let Some(pools) = shards {
        let router = Arc::new(orcher_cluster::MultiPoolRouter::with_vshards(pools, 16));
        exec_repo.set_shard_resolver(router.clone());
        task_execution_repo.set_shard_resolver(router.clone());
        journal_repo.set_shard_resolver(router);
    }
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
    .with_polling_manager(polling_manager.clone());
    let task_polling_manager = Arc::new(TaskPollingManager::new(outbox_queue));
    if let Some(store) = matching {
        task_polling_manager.set_matching_store(store as Arc<dyn MatchingStore>);
    }
    let service = Arc::new(
        ExecutionService::new(
            Arc::new(command_processor),
            polling_manager.clone(),
            execution_log_repo as Arc<dyn ExecutionLogRepository>,
            journal_repo,
            exec_repo.clone(),
            task_execution_repo,
            pool.clone(),
        )
        .with_task_polling_manager(task_polling_manager)
        .with_default_heartbeat_timeout(Some(Duration::from_secs(DEFAULT_HEARTBEAT_SECS))),
    );

    // As `main` wires it: the release records what this engine's polls see,
    // and times out tasks through the timeout watcher, whose own loop is not
    // running here.
    let release = LapsedWorkerClaims::new(
        service.worker_liveness(),
        WorkerInstanceRepository::new(pool.clone()),
        exec_repo,
        polling_manager,
        LivenessSettings {
            refresh: REFRESH,
            missed_refreshes: MISSED_REFRESHES,
            claim_timeout_secs: CLAIM_TIMEOUT_SECS,
        },
    )
    .with_tasks(TimeoutWatcher::new(
        pool.clone(),
        TimeoutWatcherConfig {
            grace_period_secs: 0,
            ..TimeoutWatcherConfig::default()
        },
    ));
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(Arc::new(release).run(async move {
        let _ = stopped.await;
    }));
    Engine {
        service,
        started: Instant::now(),
        _stop: stop,
    }
}

/// A running workflow, unclaimed, on `task_queue` in `db`, owned by
/// `organization`.
async fn workflow(
    db: &PgPool,
    id: Uuid,
    task_queue: &str,
    organization: Option<Uuid>,
) -> (Uuid, String) {
    let execution_id = Uuid::new_v4().to_string();
    sqlx::query(
        r#"
        INSERT INTO workflow_executions (
            id, execution_id, workflow_id, workflow_name, workflow_version, namespace,
            task_queue, status, parameters, context, environment, progress,
            total_steps, completed_steps, failed_steps, retried_steps, organization_id
        )
        VALUES ($1, $2, $1, 'lapsed-worker-claims', '1.0.0', $3, $4, 'running',
                '{}', '{}', '{}', '{}', 0, 0, 0, 0, $5)
        "#,
    )
    .bind(id)
    .bind(&execution_id)
    .bind(NAMESPACE)
    .bind(task_queue)
    .bind(organization)
    .execute(db)
    .await
    .expect("insert workflow");
    (id, execution_id)
}

fn queue() -> String {
    format!("lapsed-{}", Uuid::new_v4().simple())
}

/// A worker process: the identity its workflow poller sends, and the id its
/// polls name it by.
#[derive(Clone)]
struct Process {
    poller: String,
    instance: String,
    organization: Option<Uuid>,
}

fn process(organization: Option<Uuid>) -> Process {
    let identity = format!("worker-{}", Uuid::new_v4().simple());
    Process {
        poller: format!("{identity}-wf-driver-poller-0"),
        instance: Uuid::new_v4().to_string(),
        organization,
    }
}

/// `request`, sent for `organization` when there is one, as the auth layer
/// leaves it once it has established who the caller acts for.
fn acting_for<T>(organization: Option<Uuid>, mut request: Request<T>) -> Request<T> {
    if let Some(organization) = organization {
        request.metadata_mut().insert(
            ORGANIZATION_METADATA_KEY,
            organization.to_string().parse().unwrap(),
        );
    }
    request
}

/// A workflow poll from `process` that waits up to `timeout_secs`.
async fn poll(
    service: &ExecutionService,
    task_queue: &str,
    process: &Process,
    timeout_secs: i64,
) -> PollWorkflowExecutionResponse {
    service
        .poll_workflow_execution(acting_for(
            process.organization,
            Request::new(PollWorkflowExecutionRequest {
                task_queue: task_queue.to_string(),
                namespace: NAMESPACE.to_string(),
                identity: process.poller.clone(),
                poll_timeout: Some(orcher_proto::prost_types::Duration {
                    seconds: timeout_secs,
                    nanos: 0,
                }),
                worker_instance_id: process.instance.clone(),
                ..Default::default()
            }),
        ))
        .await
        .expect("poll")
        .into_inner()
}

/// The process's other pollers, idle on `task_queue`, polling for as long as
/// the process lives. Aborting the handle is the process dying: its polls end
/// with it, as a killed worker's connections do.
fn keep_polling(
    service: Arc<ExecutionService>,
    task_queue: String,
    process: Process,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let got = poll(&service, &task_queue, &process, 2).await;
            assert!(
                got.execution_id.is_empty(),
                "an idle poller of a live process was handed work"
            );
        }
    })
}

/// A completion of the activation `token` names that ends the workflow with
/// `result`.
fn finish(
    process: &Process,
    id: Uuid,
    execution_id: &str,
    token: &str,
    result: &str,
) -> Request<CompleteWorkflowExecutionRequest> {
    acting_for(
        process.organization,
        Request::new(CompleteWorkflowExecutionRequest {
            workflow_id: id.to_string(),
            execution_id: execution_id.to_string(),
            namespace: NAMESPACE.to_string(),
            identity: process.poller.clone(),
            stream_entry_id: token.to_string(),
            commands: vec![Command {
                command_type: CommandType::CompleteWorkflow as i32,
                attributes: Some(command::Attributes::CompleteWorkflow(
                    CompleteWorkflowCommandAttributes {
                        result: result.as_bytes().to_vec(),
                    },
                )),
            }],
            ..Default::default()
        }),
    )
}

/// A completion of the activation `token` names that parks the workflow on a
/// timer, which keeps its claim until the timer fires.
fn park_on_a_timer(
    process: &Process,
    id: Uuid,
    execution_id: &str,
    token: &str,
) -> Request<CompleteWorkflowExecutionRequest> {
    acting_for(
        process.organization,
        Request::new(CompleteWorkflowExecutionRequest {
            workflow_id: id.to_string(),
            execution_id: execution_id.to_string(),
            namespace: NAMESPACE.to_string(),
            identity: process.poller.clone(),
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
        }),
    )
}

/// Who holds the claim, which process it is recorded against, and which
/// activation is current.
async fn claim_of(db: &PgPool, id: Uuid) -> (Option<String>, Option<String>, i64) {
    sqlx::query_as(
        "SELECT claimed_by, claimed_by_instance, activation_seq FROM workflow_executions \
         WHERE id = $1",
    )
    .bind(id)
    .fetch_one(db)
    .await
    .expect("read the claim")
}

async fn status_and_result(db: &PgPool, id: Uuid) -> (String, Option<String>) {
    sqlx::query_as("SELECT status, result::text FROM workflow_executions WHERE id = $1")
        .bind(id)
        .fetch_one(db)
        .await
        .expect("read the status")
}

/// How many times the workflow's journal records it completing.
async fn completions_journaled(db: &PgPool, execution_id: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM workflow_execution_journal \
         WHERE execution_id = $1 AND event_type = 'WorkflowExecutionCompleted'",
    )
    .bind(execution_id)
    .fetch_one(db)
    .await
    .expect("count completions")
}

/// Wait up to `within` for the claim on `id` to be released.
async fn released_within(db: &PgPool, id: Uuid, within: Duration) -> bool {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        if claim_of(db, id).await.0.is_none() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

/// A new organization, and the user that owns it.
async fn organization(pool: &PgPool) -> Uuid {
    let owner = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO users (id, email, password_hash, hash_algorithm, password_changed_at) \
         VALUES ($1, $2, 'x', 'argon2', NOW())",
    )
    .bind(owner)
    .bind(format!("owner-{owner}@lapsed-worker-claims.test"))
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

async fn cleanup(db: &PgPool, ids: &[Uuid], processes: &[&Process]) {
    for id in ids {
        for q in [
            "DELETE FROM timer_queue WHERE workflow_id = $1",
            "DELETE FROM task_executions WHERE workflow_id = $1",
            "DELETE FROM matching_partitions WHERE workflow_id = $1",
            "DELETE FROM outbox_queue WHERE workflow_id = $1",
            "DELETE FROM workflow_execution_journal WHERE execution_id IN \
             (SELECT execution_id FROM workflow_executions WHERE id = $1)",
            "DELETE FROM workflow_execution_logs WHERE execution_id IN \
             (SELECT execution_id FROM workflow_executions WHERE id = $1)",
            "DELETE FROM workflow_executions WHERE id = $1",
        ] {
            let _ = sqlx::query(q).bind(id).execute(db).await;
        }
    }
    for process in processes {
        let _ = sqlx::query("DELETE FROM worker_instances WHERE worker_instance_id = $1")
            .bind(&process.instance)
            .execute(db)
            .await;
    }
}

/// The issue itself. A worker gets the workflow's next activation and dies
/// with it — its process killed, its other pollers gone with it. Another
/// worker is handed the activation within a few refreshes, finishes the
/// workflow, and the workflow finishes once. A second workflow the dead
/// worker had already parked on a timer keeps its claim: nothing waits on the
/// dead worker for it.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_dead_workers_activation_goes_to_another_worker_within_seconds() {
    let pool = pool().await;
    let engine = engine(&pool, None).await;
    let task_queue = queue();
    let (id, execution_id) = workflow(&pool, Uuid::new_v4(), &task_queue, None).await;
    let (parked, parked_execution) = workflow(&pool, Uuid::new_v4(), &task_queue, None).await;
    let a = process(None);

    // The first worker parks one workflow and takes the other's activation.
    let first = poll(&engine.service, &task_queue, &a, 5).await;
    let second = poll(&engine.service, &task_queue, &a, 5).await;
    let (to_park, held) = if first.execution_id == parked_execution {
        (first, second)
    } else {
        (second, first)
    };
    assert_eq!(held.execution_id, execution_id);
    engine
        .service
        .complete_workflow_execution(park_on_a_timer(
            &a,
            parked,
            &parked_execution,
            &to_park.stream_entry_id,
        ))
        .await
        .expect("park the other workflow");
    let pollers = keep_polling(engine.service.clone(), task_queue.clone(), a.clone());
    engine.warmed_up().await;
    // Alive, the worker keeps both.
    assert_eq!(
        claim_of(&pool, id).await.1.as_deref(),
        Some(a.instance.as_str())
    );

    // Killed: its polls end and it never polls again.
    pollers.abort();
    let killed = Instant::now();

    let b = process(None);
    let handed = poll(&engine.service, &task_queue, &b, 30).await;
    let waited = killed.elapsed();
    eprintln!("the dead worker's activation reached another worker {waited:?} after it died");
    assert_eq!(
        handed.execution_id, execution_id,
        "the second worker was not handed the dead worker's activation within 30s"
    );
    // The lapse, plus a refresh for the last sighting to be recorded, one for
    // a sweep to come round, and one to spare — not the claim timeout.
    let bound = lapse() + REFRESH * 3;
    assert!(
        waited <= bound,
        "the activation reached the second worker {waited:?} after the first died; \
         expected within {bound:?}"
    );

    engine
        .service
        .complete_workflow_execution(finish(
            &b,
            id,
            &execution_id,
            &handed.stream_entry_id,
            r#"{"by":"b"}"#,
        ))
        .await
        .expect("the second worker finishes the workflow");
    let (status, result) = status_and_result(&pool, id).await;
    assert_eq!(status, "completed");
    assert_eq!(result.as_deref(), Some(r#"{"by": "b"}"#));
    assert_eq!(completions_journaled(&pool, &execution_id).await, 1);

    // The parked workflow was not released: its activation was answered.
    assert_eq!(
        claim_of(&pool, parked).await.0.as_deref(),
        Some(a.poller.as_str())
    );

    cleanup(&pool, &[id, parked], &[&a, &b]).await;
}

/// The dead worker was only cut off, and answers after all. Its activation
/// was voided when it was released, so its completion is acknowledged and not
/// applied — before the activation is handed out again, and after — and the
/// workflow finishes once, as the worker that took over finished it.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_late_completion_from_the_presumed_dead_worker_is_not_applied() {
    let pool = pool().await;
    let engine = engine(&pool, None).await;
    let task_queue = queue();
    let (id, execution_id) = workflow(&pool, Uuid::new_v4(), &task_queue, None).await;
    let a = process(None);

    let held = poll(&engine.service, &task_queue, &a, 5).await;
    assert_eq!(held.execution_id, execution_id);
    let (_, _, activation) = claim_of(&pool, id).await;
    // Unseen from here on: no poll of its own is open.
    engine.warmed_up().await;
    assert!(
        released_within(&pool, id, lapse() + REFRESH * 3).await,
        "the claim of a process nobody sees was not released"
    );
    let (_, instance, voided) = claim_of(&pool, id).await;
    assert_eq!(instance, None);
    assert!(
        voided > activation,
        "the released activation was not voided"
    );

    // Late, with nobody holding the workflow yet.
    engine
        .service
        .complete_workflow_execution(finish(
            &a,
            id,
            &execution_id,
            &held.stream_entry_id,
            r#"{"by":"a"}"#,
        ))
        .await
        .expect("a stale completion is acknowledged");
    assert_eq!(
        status_and_result(&pool, id).await,
        ("running".to_string(), None)
    );
    assert_eq!(completions_journaled(&pool, &execution_id).await, 0);

    let b = process(None);
    let handed = poll(&engine.service, &task_queue, &b, 5).await;
    assert_eq!(handed.execution_id, execution_id);
    engine
        .service
        .complete_workflow_execution(finish(
            &b,
            id,
            &execution_id,
            &handed.stream_entry_id,
            r#"{"by":"b"}"#,
        ))
        .await
        .expect("the second worker finishes the workflow");

    // And late again, after the workflow finished.
    engine
        .service
        .complete_workflow_execution(finish(
            &a,
            id,
            &execution_id,
            &held.stream_entry_id,
            r#"{"by":"a"}"#,
        ))
        .await
        .expect("a stale completion is acknowledged");
    assert_eq!(
        status_and_result(&pool, id).await,
        ("completed".to_string(), Some(r#"{"by": "b"}"#.to_string()))
    );
    assert_eq!(completions_journaled(&pool, &execution_id).await, 1);

    cleanup(&pool, &[id], &[&a, &b]).await;
}

/// A worker that is still polling keeps what it holds, however long the
/// activation takes: only a process nobody sees is judged.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_live_worker_keeps_its_activation() {
    let pool = pool().await;
    let engine = engine(&pool, None).await;
    let task_queue = queue();
    let (id, execution_id) = workflow(&pool, Uuid::new_v4(), &task_queue, None).await;
    let a = process(None);

    let held = poll(&engine.service, &task_queue, &a, 5).await;
    assert_eq!(held.execution_id, execution_id);
    let claim = claim_of(&pool, id).await;
    // Its other pollers on a queue of their own, so that a claim released
    // under it could not be taken straight back by it and pass unnoticed.
    let pollers = keep_polling(engine.service.clone(), queue(), a.clone());
    engine.warmed_up().await;
    tokio::time::sleep(lapse() + REFRESH * 3).await;
    assert_eq!(
        claim_of(&pool, id).await,
        claim,
        "a live worker's activation was released or handed out again"
    );

    engine
        .service
        .complete_workflow_execution(finish(
            &a,
            id,
            &execution_id,
            &held.stream_entry_id,
            r#"{"by":"a"}"#,
        ))
        .await
        .expect("complete");
    pollers.abort();
    assert_eq!(
        status_and_result(&pool, id).await,
        ("completed".to_string(), Some(r#"{"by": "a"}"#.to_string()))
    );

    cleanup(&pool, &[id], &[&a]).await;
}

/// A process is known within the organization its polls were made for. Two
/// tenants' workers that send the same instance id are two processes: one
/// that keeps polling does not keep the other's claims alive, and the one
/// that dies has only its own organization's claims released.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_worker_is_judged_within_its_own_organization() {
    let pool = pool().await;
    let engine = engine(&pool, None).await;
    let (x, y) = (organization(&pool).await, organization(&pool).await);
    let task_queue = queue();
    let (in_x, execution_x) = workflow(&pool, Uuid::new_v4(), &task_queue, Some(x)).await;
    let (in_y, execution_y) = workflow(&pool, Uuid::new_v4(), &task_queue, Some(y)).await;
    let shared = Uuid::new_v4().to_string();
    let of_x = Process {
        instance: shared.clone(),
        ..process(Some(x))
    };
    let of_y = Process {
        instance: shared,
        ..process(Some(y))
    };

    assert_eq!(
        poll(&engine.service, &task_queue, &of_x, 5)
            .await
            .execution_id,
        execution_x
    );
    assert_eq!(
        poll(&engine.service, &task_queue, &of_y, 5)
            .await
            .execution_id,
        execution_y
    );
    let claim_y = claim_of(&pool, in_y).await;
    // The worker of y lives on, its other pollers on a queue of their own so
    // that a claim released under it could not be taken straight back; the
    // worker of x dies.
    let pollers = keep_polling(engine.service.clone(), queue(), of_y.clone());
    engine.warmed_up().await;

    assert!(
        released_within(&pool, in_x, lapse() + REFRESH * 3).await,
        "the dead worker's claim was kept alive by another tenant's process with its id"
    );
    tokio::time::sleep(REFRESH * 2).await;
    assert_eq!(
        claim_of(&pool, in_y).await,
        claim_y,
        "a live worker's claim was released for another tenant's dead process"
    );

    pollers.abort();
    cleanup(&pool, &[in_x, in_y], &[&of_x, &of_y]).await;
    forget_organization(&pool, x).await;
    forget_organization(&pool, y).await;
}

/// The test database, and a second one beside it for shards routed away from
/// the first, created and migrated on first use.
async fn shard_database(pool: &PgPool) -> PgPool {
    use sqlx::postgres::PgConnectOptions;
    use std::str::FromStr;

    let url = std::env::var("TEST_DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:postgres@localhost:5432/orcher_test".to_string()
    });
    let options = PgConnectOptions::from_str(&url).expect("TEST_DATABASE_URL");
    let name = format!(
        "{}_lapsed_shard",
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

/// With shard databases, a dead worker's claims are released on whichever
/// database they live; when it was last seen is read from the first.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_dead_workers_claim_on_a_shard_database_is_released() {
    use orcher_cluster::{MultiPoolRouter, ShardRouter};

    let pool = pool().await;
    let shard_db = shard_database(&pool).await;
    let pools = vec![pool.clone(), shard_db.clone()];
    let engine = engine(&pool, Some(pools.clone())).await;
    let router = MultiPoolRouter::with_vshards(pools, 16);
    let on_shard = std::iter::repeat_with(Uuid::new_v4)
        .find(|id| {
            std::ptr::eq(
                ShardRouter::pool_for_workflow(&router, &id.to_string()),
                ShardRouter::all_pools(&router)[1],
            )
        })
        .unwrap();
    let task_queue = queue();
    let (id, execution_id) = workflow(&shard_db, on_shard, &task_queue, None).await;
    let a = process(None);
    assert_eq!(
        poll(&engine.service, &task_queue, &a, 5).await.execution_id,
        execution_id
    );
    assert_eq!(
        claim_of(&shard_db, id).await.1.as_deref(),
        Some(a.instance.as_str())
    );
    engine.warmed_up().await;

    assert!(
        released_within(&shard_db, id, lapse() + REFRESH * 3).await,
        "the dead worker's claim on the shard database was not released"
    );
    let b = process(None);
    assert_eq!(
        poll(&engine.service, &task_queue, &b, 5).await.execution_id,
        execution_id
    );

    cleanup(&shard_db, &[id], &[]).await;
    cleanup(&pool, &[], &[&a, &b]).await;
}

/// A task poll from `process`, as a worker that heartbeats every task on its
/// own.
async fn take_task(
    service: &ExecutionService,
    task_queue: &str,
    process: &Process,
) -> PollTaskExecutionResponse {
    let response = service
        .poll_task_execution(Request::new(PollTaskExecutionRequest {
            task_queue: task_queue.to_string(),
            namespace: NAMESPACE.to_string(),
            identity: format!("{}-task", process.poller),
            poll_timeout: Some(orcher_proto::prost_types::Duration {
                seconds: 5,
                nanos: 0,
            }),
            task_capabilities: Some(TaskCapabilities {
                auto_heartbeat: true,
            }),
            worker_instance_id: process.instance.clone(),
            ..Default::default()
        }))
        .await
        .expect("poll for a task")
        .into_inner();
    assert!(!response.task_id.is_empty(), "the poll was handed no task");
    response
}

/// A task of the workflow `id`, scheduled and offered to its queue's pollers.
async fn offered_task(
    pool: &PgPool,
    store: &PgMatchingStore,
    id: Uuid,
    execution_id: &str,
    task_queue: &str,
    task_id: &str,
) {
    let task = TaskExecutionRepository::new(pool.clone())
        .create_scheduled(
            task_id.to_string(),
            "Charge".to_string(),
            WorkflowId::from_uuid(id),
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
    store
        .enqueue(&MatchTask {
            task_id: Uuid::new_v4(),
            namespace: NAMESPACE.to_string(),
            task_queue: task.task_queue.clone(),
            workflow_id: task.workflow_id.as_uuid(),
            payload: serde_json::json!({
                "workflow_id": id.to_string(),
                "execution_id": execution_id,
                "task_id": task.task_id,
                "task_type": task.task_type,
                "task_queue": task.task_queue,
                "namespace": NAMESPACE,
                "task_execution_id": task.id.to_string(),
            }),
            organization_id: None,
        })
        .await
        .expect("offer the task");
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

/// A task attempt the dead worker was running is timed out and retried once
/// its heartbeats stop too, well before its heartbeat timeout would have done
/// it. One whose worker stopped polling only because it is busy, and which it
/// keeps heartbeating, is left running: a second copy of a task is not
/// harmless the way a second activation is.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_dead_workers_task_is_retried_and_a_busy_ones_is_not() {
    let pool = pool().await;
    let store = Arc::new(PgMatchingStore::new(pool.clone()));
    let engine = engine_with(&pool, None, Some(store.clone())).await;
    let task_queue = queue();
    let (id, execution_id) = workflow(&pool, Uuid::new_v4(), &task_queue, None).await;
    let (dead, busy) = (process(None), process(None));

    offered_task(&pool, &store, id, &execution_id, &task_queue, "charge_0").await;
    let taken = take_task(&engine.service, &task_queue, &dead).await;
    assert_eq!(taken.task_id, "charge_0");
    offered_task(&pool, &store, id, &execution_id, &task_queue, "charge_1").await;
    let running = take_task(&engine.service, &task_queue, &busy).await;
    assert_eq!(running.task_id, "charge_1");

    // Neither polls again. The busy one heartbeats its task.
    let heartbeats = {
        let service = engine.service.clone();
        let token = running.task_token.clone();
        tokio::spawn(async move {
            loop {
                service
                    .record_task_heartbeat(Request::new(RecordTaskHeartbeatRequest {
                        task_token: token.clone(),
                        namespace: NAMESPACE.to_string(),
                        ..Default::default()
                    }))
                    .await
                    .expect("heartbeat");
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        })
    };
    let started = Instant::now();
    engine.warmed_up().await;

    let deadline = started + Duration::from_secs(DEFAULT_HEARTBEAT_SECS) - REFRESH;
    loop {
        let charged = attempts(&pool, id, "charge_0").await;
        if charged.len() == 2 {
            assert_eq!(charged[0].1, "timed_out");
            assert_eq!(charged[1].0, 2, "the timed-out attempt was not retried");
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the dead worker's task was not timed out before its heartbeat timeout: {charged:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // Long enough after for the busy one to have been timed out too, were
    // its heartbeats not evidence of its own.
    tokio::time::sleep(REFRESH * 2).await;
    heartbeats.abort();
    let busy_attempts = attempts(&pool, id, "charge_1").await;
    assert_eq!(busy_attempts.len(), 1);
    assert_eq!(busy_attempts[0].1, "started");

    cleanup(&pool, &[id], &[&dead, &busy]).await;
}
