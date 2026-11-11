//! The claims an engine that died left with an activation still out are
//! released by the engines that survive it, within the membership's staleness
//! window, not the claim timeout.
//!
//! Killing one engine of two left its outstanding claims — executions it had
//! batch-claimed into its prefetch buffer, activations whose reply never
//! reached the worker — held for the whole claim timeout, five minutes in
//! production, although the other engine knew within seconds that it was gone.
//! Every claim now names the engine that made it, and each survivor releases
//! the ones a member that stopped heartbeating left behind.
//!
//! Requires PostgreSQL. The schema is created by the engine's own migration
//! runner. Run with:
//!
//! ```bash
//! export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/orcher_test"
//! cargo test -p orcher-core --test dead_node_claims -- --ignored --test-threads=1
//! ```

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use orcher_common::types::dispatch::polling::PollExecutionRequest;
use orcher_core::engine::dispatch::{DeadNodeClaims, PollingManager, TaskPollingManager};
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
    command, Command, CommandType, CompleteWorkflowCommandAttributes,
    CompleteWorkflowExecutionRequest,
};
use orcher_storage::repositories::domains::matching::{MatchTask, MatchingStore, PgMatchingStore};
use orcher_storage::repositories::domains::workflow_execution::HandOut;
use orcher_storage::repositories::domains::{
    child_workflow::ChildWorkflowRepository, cluster::ClusterMembershipRepository,
    coordination::OutboxRepository, journal::JournalRepository,
    task_execution::TaskExecutionRepository, workflow_execution::WorkflowExecutionRepository,
};
use orcher_storage::repositories::{ExecutionLogRepository, PostgresExecutionLogRepository};
use sqlx::PgPool;
use tonic::Request;
use uuid::Uuid;

const NAMESPACE: &str = "default";
const WORKER: &str = "worker-a";
/// What the claim timeout is in production, and here.
const CLAIM_TIMEOUT_SECS: i64 = 300;
/// A member out of the live set after this long without a heartbeat. Shorter
/// than an engine's default (15s) so the test does not wait as long.
const STALE_AFTER_SECS: i64 = 2;
/// And treated as dead this long after that.
const GRACE_SECS: i64 = 1;

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

/// One engine of a cluster: its node id, its claims, its polls and its
/// completions, and its release of dead members' claims.
struct Engine {
    node: String,
    repo: Arc<WorkflowExecutionRepository>,
    polling: Arc<PollingManager>,
    service: Arc<ExecutionService>,
    dead_claims: DeadNodeClaims,
    membership: Arc<ClusterMembershipRepository>,
    matching: Arc<PgMatchingStore>,
    tasks: Arc<TaskExecutionRepository>,
}

impl Engine {
    /// Heartbeat, as the membership loop does on every tick.
    async fn heartbeat(&self) {
        self.membership
            .upsert_member(&self.node, &format!("{}:50051", self.node))
            .await
            .expect("heartbeat");
    }

    /// A poll by `worker` that waits up to `wait` for work.
    async fn poll(&self, task_queue: &str, worker: &str, wait: Duration) -> Option<(String, i64)> {
        self.polling
            .poll_execution(PollExecutionRequest::new(
                task_queue, NAMESPACE, worker, wait,
            ))
            .await
            .expect("poll")
            .map(|response| (response.execution_id, response.activation_seq))
    }
}

async fn engine(pool: &PgPool, shards: Option<Vec<PgPool>>) -> Engine {
    let node = format!("engine-{}", Uuid::new_v4().simple());
    let journal_repo = Arc::new(JournalRepository::new(pool.clone()));
    let journal_service = Arc::new(JournalService::new(journal_repo.clone()));
    let child_repo = Arc::new(ChildWorkflowRepository::new(pool.clone()));
    let exec_repo = Arc::new(WorkflowExecutionRepository::new(pool.clone()));
    exec_repo.set_claim_node(&node);
    if let Some(pools) = shards {
        exec_repo.set_shard_resolver(Arc::new(orcher_cluster::MultiPoolRouter::with_vshards(
            pools, 16,
        )));
    }
    let outbox_repo = Arc::new(OutboxRepository::new(Arc::new(pool.clone())));
    let outbox_queue = Arc::new(OutboxQueue::new(outbox_repo));
    let task_polling = Arc::new(TaskPollingManager::new(outbox_queue.clone()));
    let matching = Arc::new(PgMatchingStore::new(pool.clone()).with_node(node.clone()));
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
    let polling = Arc::new(PollingManager::new(
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
    .with_polling_manager(polling.clone());
    let service = ExecutionService::new(
        Arc::new(command_processor),
        polling.clone(),
        execution_log_repo as Arc<dyn ExecutionLogRepository>,
        journal_repo,
        exec_repo.clone(),
        task_execution_repo.clone(),
        pool.clone(),
    );
    let membership = Arc::new(ClusterMembershipRepository::new(pool.clone()));
    let dead_claims = DeadNodeClaims::new(
        node.clone(),
        membership.clone(),
        exec_repo.clone(),
        polling.clone(),
        STALE_AFTER_SECS,
        GRACE_SECS,
        CLAIM_TIMEOUT_SECS,
    )
    .with_tasks(matching.clone(), task_execution_repo.clone(), task_polling);
    let engine = Engine {
        node,
        repo: exec_repo,
        polling,
        service: Arc::new(service),
        dead_claims,
        membership,
        matching,
        tasks: task_execution_repo,
    };
    engine.heartbeat().await;
    engine
}

/// The engine died `secs` ago, as a killed one does: its last heartbeat and
/// every claim it has made, on each of `dbs`, are moved that far into the
/// past.
async fn died_ago(pool: &PgPool, dbs: &[&PgPool], engine: &Engine, secs: i64) {
    sqlx::query(
        "UPDATE cluster_members SET heartbeat_at = NOW() - make_interval(secs => $2) \
         WHERE node_id = $1",
    )
    .bind(&engine.node)
    .bind(secs as f64)
    .execute(pool)
    .await
    .expect("age the heartbeat");
    for db in dbs {
        for table in ["workflow_executions", "matching_partitions"] {
            sqlx::query(&format!(
                "UPDATE {table} SET claimed_at = claimed_at - make_interval(secs => $2) \
                 WHERE claimed_on_node = $1"
            ))
            .bind(&engine.node)
            .bind(secs as f64)
            .execute(*db)
            .await
            .expect("age the claims");
        }
    }
}

/// A running workflow, unclaimed, on `task_queue`, written to `db`.
async fn workflow(db: &PgPool, id: Uuid, task_queue: &str) -> String {
    let execution_id = Uuid::new_v4().to_string();
    sqlx::query(
        r#"
        INSERT INTO workflow_executions (
            id, execution_id, workflow_id, workflow_name, workflow_version, namespace,
            task_queue, status, parameters, context, environment, progress,
            total_steps, completed_steps, failed_steps, retried_steps
        )
        VALUES ($1, $2, $1, 'dead-node-claims', '1.0.0', $3, $4, 'running',
                '{}', '{}', '{}', '{}', 0, 0, 0, 0)
        "#,
    )
    .bind(id)
    .bind(&execution_id)
    .bind(NAMESPACE)
    .bind(task_queue)
    .execute(db)
    .await
    .expect("insert workflow");
    execution_id
}

fn queue() -> String {
    format!("dead-node-{}", Uuid::new_v4().simple())
}

async fn claim_of(db: &PgPool, execution_id: &str) -> (Option<String>, Option<String>, i64) {
    sqlx::query_as(
        "SELECT claimed_by, claimed_on_node, activation_seq FROM workflow_executions \
         WHERE execution_id = $1",
    )
    .bind(execution_id)
    .fetch_one(db)
    .await
    .expect("read the claim")
}

async fn status_of(db: &PgPool, execution_id: &str) -> String {
    sqlx::query_scalar("SELECT status FROM workflow_executions WHERE execution_id = $1")
        .bind(execution_id)
        .fetch_one(db)
        .await
        .expect("read status")
}

fn complete(id: &str, execution_id: &str, activation: i64) -> CompleteWorkflowExecutionRequest {
    CompleteWorkflowExecutionRequest {
        workflow_id: id.to_string(),
        execution_id: execution_id.to_string(),
        namespace: NAMESPACE.to_string(),
        identity: WORKER.to_string(),
        task_token: format!("{id}:{execution_id}").into_bytes(),
        stream_entry_id: orcher_core::grpc::activation_token::encode(execution_id, activation),
        commands: vec![Command {
            command_type: CommandType::CompleteWorkflow as i32,
            attributes: Some(command::Attributes::CompleteWorkflow(
                CompleteWorkflowCommandAttributes {
                    result: br#"{"ok":true}"#.to_vec(),
                },
            )),
        }],
        ..Default::default()
    }
}

/// Every continuation `engine` has queued on `task_queue`.
fn continuations(engine: &Engine, task_queue: &str) -> Vec<String> {
    std::iter::from_fn(|| {
        engine
            .polling
            .try_pop_continuation(task_queue, NAMESPACE, WORKER)
    })
    .collect()
}

async fn cleanup(db: &PgPool, task_queue: &str, engines: &[&Engine]) {
    for q in [
        "DELETE FROM workflow_execution_journal WHERE execution_id IN \
         (SELECT execution_id FROM workflow_executions WHERE task_queue = $1)",
        "DELETE FROM workflow_execution_logs WHERE execution_id IN \
         (SELECT execution_id FROM workflow_executions WHERE task_queue = $1)",
        "DELETE FROM workflow_executions WHERE task_queue = $1",
    ] {
        let _ = sqlx::query(q).bind(task_queue).execute(db).await;
    }
    for engine in engines {
        let _ = sqlx::query("DELETE FROM cluster_members WHERE node_id = $1")
            .bind(&engine.node)
            .execute(db)
            .await;
    }
}

/// An engine hands one activation to a worker and batch-claims two more
/// executions into its prefetch buffer, then dies. The survivor, heartbeating
/// and sweeping as its membership loop does, hands all three out again within
/// the staleness window and grace — not after the claim timeout, which is how
/// long they waited before.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_dead_engines_outstanding_claims_are_handed_out_again_within_the_staleness_window() {
    let pool = pool().await;
    let dying = engine(&pool, None).await;
    let survivor = engine(&pool, None).await;
    let task_queue = queue();
    let mut executions = Vec::new();
    for _ in 0..3 {
        executions.push(workflow(&pool, Uuid::new_v4(), &task_queue).await);
    }

    // One handed to a worker, two left in the buffer, all by `dying` while
    // it is still heartbeating.
    dying.heartbeat().await;
    let handed = dying
        .repo
        .claim_and_get_next(
            &task_queue,
            NAMESPACE,
            WORKER,
            CLAIM_TIMEOUT_SECS,
            None,
            None,
        )
        .await
        .expect("claim")
        .expect("claimable");
    let buffered = dying
        .repo
        .claim_batch(
            &task_queue,
            NAMESPACE,
            "prefetch",
            CLAIM_TIMEOUT_SECS,
            10,
            None,
            None,
        )
        .await
        .expect("batch claim");
    assert_eq!(buffered.len(), 2);
    for execution_id in &executions {
        let (claimed_by, node, _) = claim_of(&pool, execution_id).await;
        assert!(claimed_by.is_some());
        assert_eq!(
            node.as_deref(),
            Some(dying.node.as_str()),
            "a claim names the engine that made it"
        );
    }

    // `dying` stops here. Nothing is claimable while its claims stand.
    let died = Instant::now();
    assert_eq!(
        survivor
            .poll(&task_queue, WORKER, Duration::from_millis(300))
            .await,
        None
    );

    // The survivor's membership loop, at a quicker cadence than an engine's.
    let mut redelivered = HashSet::new();
    let deadline = Duration::from_secs((STALE_AFTER_SECS + GRACE_SECS + 5) as u64);
    while redelivered.len() < executions.len() && died.elapsed() < deadline {
        survivor.heartbeat().await;
        survivor.dead_claims.sweep().await.expect("sweep");
        while let Some((execution_id, activation)) = survivor
            .poll(&task_queue, "worker-b", Duration::from_millis(100))
            .await
        {
            assert!(
                activation > 1,
                "handed out again as a new activation, not the lost one"
            );
            redelivered.insert(execution_id);
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let took = died.elapsed();

    assert_eq!(
        redelivered,
        executions.iter().cloned().collect::<HashSet<_>>(),
        "every claim the dead engine left out is handed out again, took {took:?}"
    );
    assert!(
        took >= Duration::from_secs((STALE_AFTER_SECS + GRACE_SECS) as u64),
        "not before the engine is past the staleness window and grace, took {took:?}"
    );
    assert!(
        took < Duration::from_secs(CLAIM_TIMEOUT_SECS as u64),
        "within the staleness window, not the claim timeout"
    );
    assert!(
        redelivered.contains(&handed.execution_id),
        "the activation whose reply was lost is handed out again too"
    );

    cleanup(&pool, &task_queue, &[&dying, &survivor]).await;
}

/// A live engine's claims are not touched, and neither are a dead engine's
/// claims that are not stuck on it: a parked execution, whose activation
/// completed and which waits for a wake-up that does not need its engine, and
/// a claim made after the engine's heartbeat went stale — by an engine that
/// was only paused and is back.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn claims_not_stuck_on_a_dead_engine_are_left_alone() {
    let pool = pool().await;
    let live = engine(&pool, None).await;
    let dead = engine(&pool, None).await;
    let survivor = engine(&pool, None).await;
    let task_queue = queue();

    let of_live = workflow(&pool, Uuid::new_v4(), &task_queue).await;
    live.repo
        .claim_execution_by_id(&of_live, WORKER, CLAIM_TIMEOUT_SECS, None, None)
        .await
        .expect("claim")
        .expect("claimable");

    let parked = workflow(&pool, Uuid::new_v4(), &task_queue).await;
    dead.repo
        .claim_execution_by_id(&parked, WORKER, CLAIM_TIMEOUT_SECS, None, None)
        .await
        .expect("claim")
        .expect("claimable");
    sqlx::query(
        "UPDATE workflow_executions SET completed_activation = activation_seq \
         WHERE execution_id = $1",
    )
    .bind(&parked)
    .execute(&pool)
    .await
    .expect("complete the activation");

    let timed_out_already = workflow(&pool, Uuid::new_v4(), &task_queue).await;
    dead.repo
        .claim_execution_by_id(&timed_out_already, WORKER, CLAIM_TIMEOUT_SECS, None, None)
        .await
        .expect("claim")
        .expect("claimable");
    sqlx::query(
        "UPDATE workflow_executions \
         SET claimed_at = claimed_at - make_interval(secs => $2) WHERE execution_id = $1",
    )
    .bind(&timed_out_already)
    .bind((CLAIM_TIMEOUT_SECS + 10) as f64)
    .execute(&pool)
    .await
    .expect("claim it longer ago than the claim timeout");

    // `dead` stopped heartbeating a minute ago, and then made one more claim.
    died_ago(&pool, &[&pool], &dead, 60).await;
    let claimed_after = workflow(&pool, Uuid::new_v4(), &task_queue).await;
    dead.repo
        .claim_execution_by_id(&claimed_after, WORKER, CLAIM_TIMEOUT_SECS, None, None)
        .await
        .expect("claim")
        .expect("claimable");

    survivor.heartbeat().await;
    live.heartbeat().await;
    assert_eq!(
        survivor.dead_claims.sweep().await.expect("sweep").workflows,
        0
    );

    // The one claimed longer ago than the claim timeout is the reclaimer's.
    for execution_id in [&of_live, &parked, &claimed_after, &timed_out_already] {
        let (claimed_by, _, activation) = claim_of(&pool, execution_id).await;
        assert_eq!(claimed_by.as_deref(), Some(WORKER), "{execution_id}");
        assert_eq!(activation, 1, "{execution_id}");
    }
    assert!(continuations(&survivor, &task_queue).is_empty());

    cleanup(&pool, &task_queue, &[&live, &dead, &survivor]).await;
}

/// Two survivors observe the same death and sweep at once. Each claim is
/// released once, by one of them, and only that one queues its continuation.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn survivors_sweeping_at_once_release_each_claim_once() {
    let pool = pool().await;
    let dead = engine(&pool, None).await;
    let b = engine(&pool, None).await;
    let c = engine(&pool, None).await;
    let task_queue = queue();
    let mut executions = HashSet::new();
    for _ in 0..40 {
        executions.insert(workflow(&pool, Uuid::new_v4(), &task_queue).await);
    }
    let claimed = dead
        .repo
        .claim_batch(
            &task_queue,
            NAMESPACE,
            "prefetch",
            CLAIM_TIMEOUT_SECS,
            40,
            None,
            None,
        )
        .await
        .expect("batch claim");
    assert_eq!(claimed.len(), 40);
    died_ago(&pool, &[&pool], &dead, STALE_AFTER_SECS + GRACE_SECS + 1).await;

    let (by_b, by_c) = tokio::join!(b.dead_claims.sweep(), c.dead_claims.sweep());
    let (by_b, by_c) = (
        by_b.expect("sweep on b").workflows,
        by_c.expect("sweep on c").workflows,
    );
    assert_eq!(by_b + by_c, 40, "each claim released once: {by_b} + {by_c}");

    let queued_b = continuations(&b, &task_queue);
    let queued_c = continuations(&c, &task_queue);
    assert_eq!(queued_b.len(), by_b);
    assert_eq!(queued_c.len(), by_c);
    let queued: HashSet<String> = queued_b.iter().chain(&queued_c).cloned().collect();
    assert_eq!(queued.len(), 40, "no execution queued by both");
    assert_eq!(queued, executions);

    // Seen again on the next tick: nothing left to release.
    assert_eq!(
        b.dead_claims.sweep().await.expect("sweep again").workflows,
        0
    );
    assert_eq!(
        c.dead_claims.sweep().await.expect("sweep again").workflows,
        0
    );

    cleanup(&pool, &task_queue, &[&dead, &b, &c]).await;
}

/// A worker that did get the activation from the engine before it died, and
/// completes it through a survivor, is applied if nobody has taken the
/// execution since the release: the release numbers nothing. If a poller has,
/// the completion answers an activation that is no longer current, and is
/// acknowledged without being applied — the newer activation does the work.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_worker_that_got_the_activation_is_not_refused_for_the_release() {
    let pool = pool().await;
    let dead = engine(&pool, None).await;
    let survivor = engine(&pool, None).await;
    let task_queue = queue();
    let first = Uuid::new_v4();
    let first_execution = workflow(&pool, first, &task_queue).await;
    let second = Uuid::new_v4();
    let second_execution = workflow(&pool, second, &task_queue).await;
    let first_activation = dead
        .repo
        .claim_execution_by_id(&first_execution, WORKER, CLAIM_TIMEOUT_SECS, None, None)
        .await
        .expect("claim")
        .expect("claimable")
        .activation_seq;
    let second_activation = dead
        .repo
        .claim_execution_by_id(&second_execution, WORKER, CLAIM_TIMEOUT_SECS, None, None)
        .await
        .expect("claim")
        .expect("claimable")
        .activation_seq;
    died_ago(&pool, &[&pool], &dead, STALE_AFTER_SECS + GRACE_SECS + 1).await;
    assert_eq!(
        survivor.dead_claims.sweep().await.expect("sweep").workflows,
        2
    );

    // Completed before anyone claims it again: applied.
    survivor
        .service
        .complete_workflow_execution(Request::new(complete(
            &first.to_string(),
            &first_execution,
            first_activation,
        )))
        .await
        .expect("the completion is accepted");
    assert_eq!(status_of(&pool, &first_execution).await, "completed");

    // Claimed again first: acknowledged, not applied.
    let again = survivor
        .repo
        .claim_execution_by_id(
            &second_execution,
            "worker-b",
            CLAIM_TIMEOUT_SECS,
            None,
            None,
        )
        .await
        .expect("claim")
        .expect("claimable once released");
    assert_eq!(again.activation_seq, second_activation + 1);
    survivor
        .service
        .complete_workflow_execution(Request::new(complete(
            &second.to_string(),
            &second_execution,
            second_activation,
        )))
        .await
        .expect("a stale completion is acknowledged, not refused");
    assert_eq!(status_of(&pool, &second_execution).await, "running");

    cleanup(&pool, &task_queue, &[&dead, &survivor]).await;
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
        "{}_deadclaims_shard",
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

/// With shard databases, a dead engine's claims are released on whichever
/// database they live — membership is read from the first.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_dead_engines_claims_on_a_shard_database_are_released() {
    use orcher_cluster::{MultiPoolRouter, ShardRouter};

    let pool = pool().await;
    let shard_db = shard_database(&pool).await;
    let pools = vec![pool.clone(), shard_db.clone()];
    let dead = engine(&pool, Some(pools.clone())).await;
    let survivor = engine(&pool, Some(pools.clone())).await;
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
    let execution_id = workflow(&shard_db, on_shard, &task_queue).await;
    dead.repo
        .claim_execution_by_id(&execution_id, WORKER, CLAIM_TIMEOUT_SECS, None, None)
        .await
        .expect("claim")
        .expect("claimable");
    died_ago(
        &pool,
        &[&pool, &shard_db],
        &dead,
        STALE_AFTER_SECS + GRACE_SECS + 1,
    )
    .await;

    assert_eq!(
        survivor.dead_claims.sweep().await.expect("sweep").workflows,
        1
    );
    assert_eq!(claim_of(&shard_db, &execution_id).await.0, None);
    assert_eq!(continuations(&survivor, &task_queue), vec![execution_id]);

    cleanup(&shard_db, &task_queue, &[]).await;
    cleanup(&pool, &task_queue, &[&dead, &survivor]).await;
}

/// A task row, scheduled, for the workflow `id` / `execution_id`.
async fn scheduled_task(engine: &Engine, id: Uuid, execution_id: &str, task_id: &str) -> Uuid {
    use orcher_common::types::identifiers::{ExecutionId, WorkflowId};
    engine
        .tasks
        .create_scheduled_once(
            task_id.to_string(),
            "charge".to_string(),
            WorkflowId::from_uuid(id),
            ExecutionId::from_uuid(Uuid::parse_str(execution_id).unwrap()),
            "dead-node-tasks".to_string(),
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
        .expect("the row is new")
        .id
        .as_uuid()
}

async fn matching_status(pool: &PgPool, task_id: Uuid) -> String {
    sqlx::query_scalar("SELECT status FROM matching_partitions WHERE task_id = $1")
        .bind(task_id)
        .fetch_one(pool)
        .await
        .expect("read the matching row")
}

/// A poll on the engine that died had claimed a task's matching row and not
/// yet marked the task started; that task is returned to its queue and
/// claimed again. One it had marked started — whose worker may have it and be
/// running it — is left for its timeouts, like a task whose worker crashed.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_task_a_dead_engine_claimed_and_never_started_is_offered_again() {
    let pool = pool().await;
    let dead = engine(&pool, None).await;
    let survivor = engine(&pool, None).await;
    let task_queue = queue();
    let id = Uuid::new_v4();
    let execution_id = workflow(&pool, id, &task_queue).await;
    let never_started = scheduled_task(&dead, id, &execution_id, "never-started").await;
    let started = scheduled_task(&dead, id, &execution_id, "started").await;

    let mut matching_ids = Vec::new();
    for task_execution in [never_started, started] {
        let task_id = Uuid::new_v4();
        dead.matching
            .enqueue(&MatchTask {
                task_id,
                namespace: NAMESPACE.to_string(),
                task_queue: task_queue.clone(),
                workflow_id: id,
                payload: serde_json::json!({ "task_execution_id": task_execution.to_string() }),
                organization_id: None,
            })
            .await
            .expect("enqueue");
        let claimed = dead
            .matching
            .claim(
                NAMESPACE,
                &task_queue,
                WORKER,
                Duration::from_secs(300),
                None,
            )
            .await
            .expect("claim")
            .expect("claimable");
        assert_eq!(claimed.task_id, task_id);
        matching_ids.push(task_id);
    }
    dead.tasks
        .mark_started(
            orcher_common::types::identifiers::TaskExecutionId::from_uuid(started),
            WORKER.to_string(),
            "token".to_string(),
        )
        .await
        .expect("start the task");
    died_ago(&pool, &[&pool], &dead, STALE_AFTER_SECS + GRACE_SECS + 1).await;

    let swept = survivor.dead_claims.sweep().await.expect("sweep");
    assert_eq!(swept.tasks, 1);
    assert_eq!(matching_status(&pool, matching_ids[0]).await, "ready");
    assert_eq!(matching_status(&pool, matching_ids[1]).await, "claimed");
    let again = survivor
        .matching
        .claim(
            NAMESPACE,
            &task_queue,
            "worker-b",
            Duration::from_secs(300),
            None,
        )
        .await
        .expect("claim")
        .expect("offered again");
    assert_eq!(again.task_id, matching_ids[0]);
    assert_eq!(
        survivor
            .dead_claims
            .sweep()
            .await
            .expect("sweep again")
            .tasks,
        0,
        "claimed now by a live engine"
    );

    let _ = sqlx::query("DELETE FROM matching_partitions WHERE task_queue = $1")
        .bind(&task_queue)
        .execute(&pool)
        .await;
    let _ = sqlx::query("DELETE FROM task_executions WHERE execution_id = $1::uuid")
        .bind(&execution_id)
        .execute(&pool)
        .await;
    cleanup(&pool, &task_queue, &[&dead, &survivor]).await;
}

/// A completion being applied when the sweep runs holds the activation's
/// fence. Its claim is not released under it — a poller would otherwise claim
/// the next activation and replay a journal the completion has only partly
/// written — and once the completion is recorded, the execution is parked, not
/// released.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_completion_being_applied_is_not_released_under_it() {
    let pool = pool().await;
    let dead = engine(&pool, None).await;
    let survivor = engine(&pool, None).await;
    let task_queue = queue();
    let id = Uuid::new_v4();
    let execution_id = workflow(&pool, id, &task_queue).await;
    let activation = dead
        .repo
        .claim_execution_by_id(&execution_id, WORKER, CLAIM_TIMEOUT_SECS, None, None)
        .await
        .expect("claim")
        .expect("claimable")
        .activation_seq;
    died_ago(&pool, &[&pool], &dead, STALE_AFTER_SECS + GRACE_SECS + 1).await;

    // The worker's completion has been admitted and is being applied.
    let fence = survivor
        .repo
        .enter_activation(&id.to_string(), &execution_id)
        .await
        .expect("enter the fence")
        .expect("the execution exists");
    assert_eq!(
        survivor.dead_claims.sweep().await.expect("sweep").workflows,
        0,
        "not released while its completion is being applied"
    );
    assert_eq!(
        claim_of(&pool, &execution_id).await.0.as_deref(),
        Some(WORKER)
    );

    fence
        .applied(activation)
        .await
        .expect("record the activation");
    assert_eq!(
        survivor.dead_claims.sweep().await.expect("sweep").workflows,
        0,
        "completed, so parked rather than released"
    );
    let (claimed_by, _, seq) = claim_of(&pool, &execution_id).await;
    assert_eq!(claimed_by.as_deref(), Some(WORKER));
    assert_eq!(seq, activation, "no newer activation was handed out");
    assert!(continuations(&survivor, &task_queue).is_empty());

    cleanup(&pool, &task_queue, &[&dead, &survivor]).await;
}

async fn node_of(db: &PgPool, execution_id: &str) -> Option<String> {
    claim_of(db, execution_id).await.1
}

/// Whatever clears a claim clears the engine it names too, so a claim made
/// later by an engine that does not record one can never be taken for the
/// earlier engine's.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn clearing_a_claim_clears_the_engine_it_names() {
    let pool = pool().await;
    let a = engine(&pool, None).await;
    let task_queue = queue();

    // Woken with no activation out: released by release_claim_to_resume.
    let woken = workflow(&pool, Uuid::new_v4(), &task_queue).await;
    a.repo
        .claim_execution_by_id(&woken, WORKER, CLAIM_TIMEOUT_SECS, None, None)
        .await
        .expect("claim")
        .expect("claimable");
    sqlx::query(
        "UPDATE workflow_executions SET completed_activation = activation_seq \
         WHERE execution_id = $1",
    )
    .bind(&woken)
    .execute(&pool)
    .await
    .expect("complete the activation");
    assert_eq!(
        node_of(&pool, &woken).await.as_deref(),
        Some(a.node.as_str())
    );
    a.repo.release_claim_to_resume(&woken).await.expect("wake");
    assert_eq!(claim_of(&pool, &woken).await.0, None);
    assert_eq!(node_of(&pool, &woken).await, None);

    // Timed out: released by the reclaimer.
    let expired = workflow(&pool, Uuid::new_v4(), &task_queue).await;
    a.repo
        .claim_execution_by_id(&expired, WORKER, CLAIM_TIMEOUT_SECS, None, None)
        .await
        .expect("claim")
        .expect("claimable");
    sqlx::query(
        "UPDATE workflow_executions SET claim_timeout_at = NOW() - interval '1 second' \
         WHERE execution_id = $1",
    )
    .bind(&expired)
    .execute(&pool)
    .await
    .expect("expire the claim");
    a.repo.reclaim_expired().await.expect("reclaim");
    assert_eq!(claim_of(&pool, &expired).await.0, None);
    assert_eq!(node_of(&pool, &expired).await, None);

    // Released outright.
    let released = workflow(&pool, Uuid::new_v4(), &task_queue).await;
    a.repo
        .claim_execution_by_id(&released, WORKER, CLAIM_TIMEOUT_SECS, None, None)
        .await
        .expect("claim")
        .expect("claimable");
    a.repo.release_claim(&released).await.expect("release");
    assert_eq!(node_of(&pool, &released).await, None);

    cleanup(&pool, &task_queue, &[&a]).await;
}

/// An engine shutting down gives back the claims in its prefetch buffer at
/// once, rather than leaving them for the others to find after it has gone
/// stale. The one it handed out stays with the poller that has it.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_stopping_engine_releases_what_it_prefetched() {
    let pool = pool().await;
    let node = format!("engine-{}", Uuid::new_v4().simple());
    let repo = Arc::new(WorkflowExecutionRepository::new(pool.clone()));
    repo.set_claim_node(&node);
    let polling = PollingManager::new(
        repo.clone(),
        Arc::new(JournalRepository::new(pool.clone())),
        CLAIM_TIMEOUT_SECS,
    )
    .with_default_prefetch();
    let task_queue = queue();
    let mut executions = HashSet::new();
    for _ in 0..4 {
        executions.insert(workflow(&pool, Uuid::new_v4(), &task_queue).await);
    }

    let handed = polling
        .poll_execution(PollExecutionRequest::new(
            &task_queue,
            NAMESPACE,
            WORKER,
            Duration::from_secs(1),
        ))
        .await
        .expect("poll")
        .expect("work")
        .execution_id;
    for execution_id in &executions {
        assert!(
            claim_of(&pool, execution_id).await.0.is_some(),
            "batch-claimed"
        );
    }

    assert_eq!(polling.release_prefetched().await, 3);
    for execution_id in &executions {
        let (claimed_by, on_node, _) = claim_of(&pool, execution_id).await;
        if *execution_id == handed {
            assert_eq!(
                claimed_by.as_deref(),
                Some(WORKER),
                "the handed-out one is kept"
            );
        } else {
            assert_eq!(claimed_by, None, "{execution_id} is released");
            assert_eq!(on_node, None);
        }
    }
    assert_eq!(
        polling.release_prefetched().await,
        0,
        "nothing left to release"
    );

    for q in [
        "DELETE FROM workflow_execution_journal WHERE execution_id IN \
         (SELECT execution_id FROM workflow_executions WHERE task_queue = $1)",
        "DELETE FROM workflow_executions WHERE task_queue = $1",
    ] {
        let _ = sqlx::query(q).bind(&task_queue).execute(&pool).await;
    }
}

/// No claim hands out an execution while a completion of it is being applied:
/// its commands go in outside the fence's transaction, and a claim in between
/// would hand out an activation that replays a journal only partly written.
/// Each claim path skips the fenced row, and takes it once the fence is gone.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn no_claim_takes_an_execution_whose_completion_is_being_applied() {
    let pool = pool().await;
    let a = engine(&pool, None).await;
    let task_queue = queue();
    let id = Uuid::new_v4();
    let execution_id = workflow(&pool, id, &task_queue).await;

    let fence = a
        .repo
        .enter_activation(&id.to_string(), &execution_id)
        .await
        .expect("enter the fence")
        .expect("the execution exists");
    assert!(a
        .repo
        .claim_and_get_next(
            &task_queue,
            NAMESPACE,
            WORKER,
            CLAIM_TIMEOUT_SECS,
            None,
            None
        )
        .await
        .expect("claim")
        .is_none());
    assert!(a
        .repo
        .claim_batch(
            &task_queue,
            NAMESPACE,
            WORKER,
            CLAIM_TIMEOUT_SECS,
            10,
            None,
            None
        )
        .await
        .expect("batch claim")
        .is_empty());
    assert!(a
        .repo
        .claim_execution_by_id(&execution_id, WORKER, CLAIM_TIMEOUT_SECS, None, None)
        .await
        .expect("claim by id")
        .is_none());
    drop(fence);

    // Unfenced, it is claimed — and a claimed activation is not handed on
    // from the prefetch buffer while a completion holds the fence either.
    let claimed = a
        .repo
        .claim_batch(
            &task_queue,
            NAMESPACE,
            "prefetch",
            CLAIM_TIMEOUT_SECS,
            10,
            None,
            None,
        )
        .await
        .expect("batch claim");
    assert_eq!(claimed.len(), 1);
    let activation = claimed[0].activation_seq;
    let fence = a
        .repo
        .enter_activation(&id.to_string(), &execution_id)
        .await
        .expect("enter the fence")
        .expect("the execution exists");
    assert_eq!(
        a.repo
            .hand_to(
                &execution_id,
                activation,
                WORKER,
                CLAIM_TIMEOUT_SECS,
                None,
                None
            )
            .await
            .expect("hand out"),
        HandOut::Busy
    );
    drop(fence);
    assert_eq!(
        a.repo
            .hand_to(
                &execution_id,
                activation,
                WORKER,
                CLAIM_TIMEOUT_SECS,
                None,
                None
            )
            .await
            .expect("hand out"),
        HandOut::Handed
    );

    cleanup(&pool, &task_queue, &[&a]).await;
}

/// An engine that starts draining marks itself so and keeps only its
/// heartbeat's timestamp fresh. It drops out of the live set at once, so
/// another engine can take its shards without waiting for it to go stale; and
/// because it is still heartbeating, nobody releases the claims it is still
/// answering.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_draining_engine_hands_its_shards_over_and_keeps_its_claims() {
    let pool = pool().await;
    let draining = engine(&pool, None).await;
    let other = engine(&pool, None).await;
    let task_queue = queue();
    let shard = 1_000_000 + (Uuid::new_v4().as_u128() % 1_000_000) as i32;

    assert!(draining
        .membership
        .acquire_shard(shard, &draining.node, STALE_AFTER_SECS)
        .await
        .expect("acquire")
        .is_some());
    assert!(
        other
            .membership
            .acquire_shard(shard, &other.node, STALE_AFTER_SECS)
            .await
            .expect("acquire")
            .is_none(),
        "held by a live engine"
    );
    let execution_id = workflow(&pool, Uuid::new_v4(), &task_queue).await;
    draining
        .repo
        .claim_execution_by_id(&execution_id, WORKER, CLAIM_TIMEOUT_SECS, None, None)
        .await
        .expect("claim")
        .expect("claimable");

    // The drain begins: marked draining, heartbeating by timestamp only.
    draining
        .membership
        .set_member_status(&draining.node, "draining")
        .await
        .expect("mark draining");
    assert!(draining
        .membership
        .heartbeat(&draining.node)
        .await
        .expect("heartbeat"));

    let live = other
        .membership
        .live_members(STALE_AFTER_SECS)
        .await
        .expect("live members");
    assert!(live.iter().all(|m| m.node_id != draining.node));
    assert!(
        other
            .membership
            .acquire_shard(shard, &other.node, STALE_AFTER_SECS)
            .await
            .expect("acquire")
            .is_some(),
        "taken over at once"
    );
    assert_eq!(other.dead_claims.sweep().await.expect("sweep").workflows, 0);
    assert_eq!(
        claim_of(&pool, &execution_id).await.0.as_deref(),
        Some(WORKER),
        "a draining engine's claims are its own while it heartbeats"
    );

    let _ = sqlx::query("DELETE FROM cluster_shards WHERE shard_id = $1")
        .bind(shard)
        .execute(&pool)
        .await;
    cleanup(&pool, &task_queue, &[&draining, &other]).await;
}
