//! A workflow completion sent to a node that does not own the workflow must
//! still land — on the owner.
//!
//! Beside `write_fence_refuses_stale_owner`, which shows the fence turning a
//! non-owner away, this shows what happens to the request instead: the node
//! that received it relays it to the owner over gRPC, the owner applies it
//! under its own fence, and the caller gets the owner's answer. Two engines
//! share one database here, as two pods do, and the owner is a real gRPC
//! server on a local port.
//!
//! Requires PostgreSQL. The schema is created by the engine's own migration
//! runner. Run with:
//!
//! ```bash
//! export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/orcher_test"
//! cargo test -p orcher-core --test forwarded_completion_commits_on_owner -- --ignored --test-threads=1
//! ```

// Helpers return `tonic::Status`, the error the engine's gRPC services answer with.
#![allow(clippy::result_large_err)]

use std::sync::{Arc, Mutex};

use orcher_cluster::{
    ClusterStore, Membership, NodeId, PeerDirectory, ShardId, ShardMap, ShardOwnership,
    StaticCluster,
};
use orcher_core::engine::dispatch::PollingManager;
use orcher_core::engine::queues::outbox::OutboxQueue;
use orcher_core::engine::WorkflowCoordinator;
use orcher_core::events::EventBus;
use orcher_core::grpc::forward::{ForwardConfig, OwnerForwarder, FORWARDED_BY_HEADER};
use orcher_core::grpc::services::ExecutionService;
use orcher_core::workflows::{
    ChildWorkflowManager, CommandProcessor, JournalService, WorkflowExecutionService,
};
use orcher_events::bus::EventBusConfig;
use orcher_proto::orcher::v1::execution_service_server::{
    ExecutionService as ExecutionServiceTrait, ExecutionServiceServer,
};
use orcher_proto::orcher::v1::{
    command, Command, CommandType, CompleteWorkflowCommandAttributes,
    CompleteWorkflowExecutionRequest, ReleaseWorkflowExecutionRequest,
    ScheduleTaskCommandAttributes,
};
use orcher_storage::repositories::domains::cluster::ClusterMembershipRepository;
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
const STALE_AFTER_SECS: i64 = 30;

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

/// Two nodes, `a` and `b`, splitting the shards by range, with `b` reachable
/// at whatever address its test server bound.
struct TwoNodes {
    view: StaticCluster,
    b_address: Option<String>,
}

impl Membership for TwoNodes {
    fn this_node(&self) -> &NodeId {
        self.view.this_node()
    }
    fn live_nodes(&self) -> Vec<NodeId> {
        self.view.live_nodes()
    }
}
impl ShardMap for TwoNodes {
    fn shard_of(&self, workflow_id: &str) -> ShardId {
        self.view.shard_of(workflow_id)
    }
    fn owner_of(&self, shard: ShardId) -> Option<NodeId> {
        self.view.owner_of(shard)
    }
    fn owns(&self, shard: ShardId) -> bool {
        self.view.owns(shard)
    }
}
impl PeerDirectory for TwoNodes {
    fn address_of(&self, node: &NodeId) -> Option<String> {
        if node.as_str().ends_with("-b") {
            self.b_address.clone()
        } else {
            None
        }
    }
}

/// Node names unique to one test, so ownership left by another run is never
/// mistaken for a live peer's.
struct Names {
    a: NodeId,
    b: NodeId,
}

impl Names {
    fn new() -> Self {
        let run = Uuid::new_v4().simple().to_string();
        Names {
            a: NodeId::new(format!("fwd-{run}-a")),
            b: NodeId::new(format!("fwd-{run}-b")),
        }
    }
    fn view(&self, this: &NodeId, b_address: Option<String>) -> Arc<TwoNodes> {
        Arc::new(TwoNodes {
            view: StaticCluster::new(
                this.clone(),
                vec![self.a.clone(), self.b.clone()],
                orcher_cluster::DEFAULT_VSHARDS,
            ),
            b_address,
        })
    }
}

/// One engine's completion path, fenced by `ownership`.
fn engine(
    pool: &PgPool,
    coordinator: Arc<WorkflowCoordinator>,
    ownership: Arc<ShardOwnership>,
) -> ExecutionService {
    let journal_repo = Arc::new(JournalRepository::new(pool.clone()));
    let journal_service = Arc::new(JournalService::new(journal_repo.clone()));
    let child_repo = Arc::new(ChildWorkflowRepository::new(pool.clone()));
    let exec_repo = Arc::new(WorkflowExecutionRepository::new(pool.clone()));
    exec_repo.set_write_fence(ownership.clone());
    coordinator.set_shard_ownership(ownership);
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

    ExecutionService::new(
        Arc::new(command_processor),
        polling_manager,
        execution_log_repo as Arc<dyn ExecutionLogRepository>,
        journal_repo,
        exec_repo,
        task_execution_repo,
        pool.clone(),
    )
}

async fn coordinator(pool: &PgPool) -> Arc<WorkflowCoordinator> {
    Arc::new(
        WorkflowCoordinator::new(
            Arc::new(pool.clone()),
            orcher_core::engine::CoordinatorConfig::default(),
        )
        .await
        .expect("create coordinator"),
    )
}

/// Both nodes live in the membership table, as their heartbeats keep them.
async fn register(pool: &PgPool, names: &Names) -> Arc<dyn ClusterStore> {
    let repo = ClusterMembershipRepository::new(pool.clone());
    repo.upsert_member(names.a.as_str(), "127.0.0.1:1")
        .await
        .expect("register a");
    repo.upsert_member(names.b.as_str(), "127.0.0.1:1")
        .await
        .expect("register b");
    Arc::new(repo)
}

/// A claimed, running workflow whose shard `owner`'s range covers, with no
/// generation left on that shard by an earlier run.
async fn workflow_owned_by(pool: &PgPool, view: &TwoNodes, owner: &NodeId) -> (Uuid, String) {
    let id = loop {
        let id = Uuid::new_v4();
        if view.owner_of(view.shard_of(&id.to_string())).as_ref() == Some(owner) {
            break id;
        }
    };
    let shard = view.shard_of(&id.to_string());
    sqlx::query("DELETE FROM cluster_shards WHERE shard_id = $1")
        .bind(shard.0 as i32)
        .execute(pool)
        .await
        .expect("clear the shard");

    let execution_id = Uuid::new_v4().to_string();
    sqlx::query(
        r#"
        INSERT INTO workflow_executions (
            id, execution_id, workflow_id, workflow_name, workflow_version, namespace,
            task_queue, status, parameters, context, environment, progress,
            total_steps, completed_steps, failed_steps, retried_steps,
            claimed_by, claimed_at, claim_timeout_at
        )
        VALUES ($1, $2, $1, 'forwarded-completion', '1.0.0', $3, 'default', 'running',
                '{}', '{}', '{}', '{}', 0, 0, 0, 0, $4, NOW(), NOW() + INTERVAL '5 minutes')
        "#,
    )
    .bind(id)
    .bind(&execution_id)
    .bind(NAMESPACE)
    .bind(WORKER)
    .execute(pool)
    .await
    .expect("insert claimed workflow");
    (id, execution_id)
}

fn completion(id: Uuid, execution_id: &str) -> CompleteWorkflowExecutionRequest {
    CompleteWorkflowExecutionRequest {
        workflow_id: id.to_string(),
        execution_id: execution_id.to_string(),
        namespace: NAMESPACE.to_string(),
        identity: WORKER.to_string(),
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

async fn status_of(pool: &PgPool, id: Uuid) -> String {
    sqlx::query_scalar("SELECT status FROM workflow_executions WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("read status")
}

async fn shard_owner(pool: &PgPool, view: &TwoNodes, id: Uuid) -> Option<String> {
    sqlx::query_scalar("SELECT owner_node FROM cluster_shards WHERE shard_id = $1")
        .bind(view.shard_of(&id.to_string()).0 as i32)
        .fetch_optional(pool)
        .await
        .expect("read shard owner")
        .flatten()
}

async fn cleanup(pool: &PgPool, id: Uuid, names: &Names) {
    for q in [
        "DELETE FROM workflow_execution_journal WHERE workflow_id = $1::text",
        "DELETE FROM workflow_executions WHERE id = $1",
    ] {
        let _ = sqlx::query(q).bind(id).execute(pool).await;
    }
    for node in [&names.a, &names.b] {
        let _ = sqlx::query("DELETE FROM cluster_shards WHERE owner_node = $1")
            .bind(node.as_str())
            .execute(pool)
            .await;
        let _ = sqlx::query("DELETE FROM cluster_members WHERE node_id = $1")
            .bind(node.as_str())
            .execute(pool)
            .await;
    }
}

/// What the owner saw of each request it was sent.
/// Each request's relay mark and credential, in arrival order.
type Marks = Vec<(Option<String>, Option<String>)>;

#[derive(Default, Clone)]
struct Seen(Arc<Mutex<Marks>>);

/// Serve `service` as node b on a local port, recording the relay mark and
/// the credential on every request. Returns the address it listens on.
async fn serve_owner(service: ExecutionService, seen: Seen) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a local port");
    let address = listener.local_addr().unwrap().to_string();
    let incoming = tonic::transport::server::TcpIncoming::from_listener(listener, true, None)
        .expect("incoming");
    let server = ExecutionServiceServer::with_interceptor(service, move |request: Request<()>| {
        let header = |key: &str| {
            request
                .metadata()
                .get(key)
                .and_then(|v| v.to_str().ok())
                .map(str::to_string)
        };
        seen.0
            .lock()
            .unwrap()
            .push((header(FORWARDED_BY_HEADER), header("authorization")));
        Ok(request)
    });
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(server)
            .serve_with_incoming(incoming)
            .await
            .expect("owner server");
    });
    address
}

/// The whole point: the worker reached the wrong node, and the workflow still
/// completes — written by the owner, under the owner's generation.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_completion_sent_to_a_non_owner_commits_on_the_owner() {
    let pool = pool().await;
    let names = Names::new();
    let store = register(&pool, &names).await;

    // Node b: the owner, serving gRPC.
    let b_view = names.view(&names.b, None);
    let b_ownership = Arc::new(
        ShardOwnership::new(
            names.b.clone(),
            store.clone(),
            b_view.clone(),
            STALE_AFTER_SECS,
        )
        .with_range_strict(b_view.clone()),
    );
    let b = engine(&pool, coordinator(&pool).await, b_ownership);
    let seen = Seen::default();
    let b_address = serve_owner(b, seen.clone()).await;

    // Node a: received the completion, owns nothing it is asked about.
    let a_view = names.view(&names.a, Some(b_address));
    let a_ownership = Arc::new(
        ShardOwnership::new(
            names.a.clone(),
            store.clone(),
            a_view.clone(),
            STALE_AFTER_SECS,
        )
        .with_range_strict(a_view.clone()),
    );
    let forwarder = OwnerForwarder::new(a_view.clone(), ForwardConfig::default())
        .with_ownership(a_ownership.clone());
    let a = engine(&pool, coordinator(&pool).await, a_ownership)
        .with_owner_forwarder(Arc::new(forwarder));

    let (id, execution_id) = workflow_owned_by(&pool, &a_view, &names.b).await;

    let mut request = Request::new(completion(id, &execution_id));
    request
        .metadata_mut()
        .insert("authorization", "Bearer orch_worker_key".parse().unwrap());
    a.complete_workflow_execution(request)
        .await
        .expect("the completion must succeed, on the owner");

    assert_eq!(status_of(&pool, id).await, "completed");
    assert_eq!(
        shard_owner(&pool, &a_view, id).await.as_deref(),
        Some(names.b.as_str()),
        "the write must have been made by the owner, under its generation"
    );
    let seen = seen.0.lock().unwrap().clone();
    assert_eq!(seen.len(), 1, "exactly one relay to the owner");
    assert_eq!(
        seen[0].0.as_deref(),
        Some(names.a.as_str()),
        "marked as relayed by a"
    );
    assert_eq!(
        seen[0].1.as_deref(),
        Some("Bearer orch_worker_key"),
        "the owner must see the worker's own credential, to check it itself"
    );

    cleanup(&pool, id, &names).await;
}

/// A release gives up a claim, which is the owner's to give up, so it is
/// relayed to the owner just as a completion is.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_release_sent_to_a_non_owner_is_carried_out_by_the_owner() {
    let pool = pool().await;
    let names = Names::new();
    let store = register(&pool, &names).await;

    let b_view = names.view(&names.b, None);
    let b_ownership = Arc::new(
        ShardOwnership::new(
            names.b.clone(),
            store.clone(),
            b_view.clone(),
            STALE_AFTER_SECS,
        )
        .with_range_strict(b_view.clone()),
    );
    let b = engine(&pool, coordinator(&pool).await, b_ownership);
    let seen = Seen::default();
    let b_address = serve_owner(b, seen.clone()).await;

    let a_view = names.view(&names.a, Some(b_address));
    let a_ownership = Arc::new(
        ShardOwnership::new(
            names.a.clone(),
            store.clone(),
            a_view.clone(),
            STALE_AFTER_SECS,
        )
        .with_range_strict(a_view.clone()),
    );
    let forwarder = OwnerForwarder::new(a_view.clone(), ForwardConfig::default())
        .with_ownership(a_ownership.clone());
    let a = engine(&pool, coordinator(&pool).await, a_ownership)
        .with_owner_forwarder(Arc::new(forwarder));

    let (id, execution_id) = workflow_owned_by(&pool, &a_view, &names.b).await;

    // Without a token, from the worker holding the claim.
    a.release_workflow_execution(Request::new(ReleaseWorkflowExecutionRequest {
        workflow_id: id.to_string(),
        execution_id: execution_id.clone(),
        namespace: NAMESPACE.to_string(),
        identity: WORKER.to_string(),
        ..Default::default()
    }))
    .await
    .expect("the release is acknowledged, by the owner");

    let claimed_by: Option<String> =
        sqlx::query_scalar("SELECT claimed_by FROM workflow_executions WHERE id = $1")
            .bind(id)
            .fetch_one(&pool)
            .await
            .expect("read the claim");
    assert_eq!(claimed_by, None, "the claim is given up");
    let seen = seen.0.lock().unwrap().clone();
    assert_eq!(seen.len(), 1, "exactly one relay to the owner");
    assert_eq!(seen[0].0.as_deref(), Some(names.a.as_str()));

    cleanup(&pool, id, &names).await;
}

/// Serve `service` as node b, as a build from before `ReleaseWorkflowExecution`
/// would answer it: every request is refused as unimplemented, which is what
/// that build's router says of a method it does not have. Only releases are
/// sent to it here.
async fn serve_old_owner(service: ExecutionService, seen: Seen) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a local port");
    let address = listener.local_addr().unwrap().to_string();
    let incoming = tonic::transport::server::TcpIncoming::from_listener(listener, true, None)
        .expect("incoming");
    let server = ExecutionServiceServer::with_interceptor(service, move |request: Request<()>| {
        let relayed_by = request
            .metadata()
            .get(FORWARDED_BY_HEADER)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        seen.0.lock().unwrap().push((relayed_by, None));
        Err(tonic::Status::unimplemented("unknown method"))
    });
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(server)
            .serve_with_incoming(incoming)
            .await
            .expect("owner server");
    });
    address
}

/// During a rolling upgrade the owner may not have the release RPC yet. The
/// release is then carried out where it arrived, rather than failing and
/// leaving the workflow to wait out its claim.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_release_an_old_owner_does_not_know_is_carried_out_where_it_arrived() {
    let pool = pool().await;
    let names = Names::new();
    let store = register(&pool, &names).await;

    let b_view = names.view(&names.b, None);
    let b_ownership = Arc::new(
        ShardOwnership::new(
            names.b.clone(),
            store.clone(),
            b_view.clone(),
            STALE_AFTER_SECS,
        )
        .with_range_strict(b_view.clone()),
    );
    let b = engine(&pool, coordinator(&pool).await, b_ownership);
    let seen = Seen::default();
    let b_address = serve_old_owner(b, seen.clone()).await;

    let a_view = names.view(&names.a, Some(b_address));
    let a_ownership = Arc::new(
        ShardOwnership::new(
            names.a.clone(),
            store.clone(),
            a_view.clone(),
            STALE_AFTER_SECS,
        )
        .with_range_strict(a_view.clone()),
    );
    let forwarder = OwnerForwarder::new(a_view.clone(), ForwardConfig::default())
        .with_ownership(a_ownership.clone());
    let a = engine(&pool, coordinator(&pool).await, a_ownership)
        .with_owner_forwarder(Arc::new(forwarder));

    let (id, execution_id) = workflow_owned_by(&pool, &a_view, &names.b).await;

    a.release_workflow_execution(Request::new(ReleaseWorkflowExecutionRequest {
        workflow_id: id.to_string(),
        execution_id: execution_id.clone(),
        namespace: NAMESPACE.to_string(),
        identity: WORKER.to_string(),
        ..Default::default()
    }))
    .await
    .expect("the release is acknowledged, by the node it reached");

    let claimed_by: Option<String> =
        sqlx::query_scalar("SELECT claimed_by FROM workflow_executions WHERE id = $1")
            .bind(id)
            .fetch_one(&pool)
            .await
            .expect("read the claim");
    assert_eq!(claimed_by, None, "the claim is given up");
    assert_eq!(
        seen.0.lock().unwrap().len(),
        1,
        "after one attempt at the owner"
    );

    cleanup(&pool, id, &names).await;
}

/// A relayed request is handled where it arrives, never relayed again. Here
/// that is a node that does not own the workflow, so it is refused — before
/// anything is applied, and as retryable.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_relayed_completion_is_not_relayed_again_and_is_refused_retryably() {
    let pool = pool().await;
    let names = Names::new();
    let store = register(&pool, &names).await;

    // b would be reachable, if a were allowed to relay.
    let a_view = names.view(&names.a, Some("127.0.0.1:1".to_string()));
    let a_ownership = Arc::new(
        ShardOwnership::new(
            names.a.clone(),
            store.clone(),
            a_view.clone(),
            STALE_AFTER_SECS,
        )
        .with_range_strict(a_view.clone()),
    );
    let forwarder = OwnerForwarder::new(a_view.clone(), ForwardConfig::default())
        .with_ownership(a_ownership.clone());
    let a = engine(&pool, coordinator(&pool).await, a_ownership)
        .with_owner_forwarder(Arc::new(forwarder));

    let (id, execution_id) = workflow_owned_by(&pool, &a_view, &names.b).await;

    let mut request = Request::new(completion(id, &execution_id));
    request
        .metadata_mut()
        .insert(FORWARDED_BY_HEADER, "some-node".parse().unwrap());
    let status = a
        .complete_workflow_execution(request)
        .await
        .expect_err("a does not own the workflow");

    assert_eq!(status.code(), Code::Unavailable, "{status:?}");
    assert_eq!(status_of(&pool, id).await, "running");
    assert_eq!(
        shard_owner(&pool, &a_view, id).await,
        None,
        "a must not have taken b's shard"
    );

    cleanup(&pool, id, &names).await;
}

/// Without forwarding — today's behaviour when it is switched off — a node
/// whose shard a live peer holds is still refused, but the refusal now says
/// "retry" rather than "internal error".
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn without_forwarding_a_fence_refusal_is_retryable() {
    let pool = pool().await;
    let names = Names::new();
    let store = register(&pool, &names).await;

    let a_view = names.view(&names.a, None);
    let (id, execution_id) = workflow_owned_by(&pool, &a_view, &names.b).await;

    // b holds the shard, as it would after writing the workflow.
    let b_view = names.view(&names.b, None);
    let b_ownership = ShardOwnership::new(
        names.b.clone(),
        store.clone(),
        b_view.clone(),
        STALE_AFTER_SECS,
    );
    assert!(b_ownership
        .acquire_for(&id.to_string())
        .await
        .unwrap()
        .is_some());

    // a: first-touch ownership, no forwarder.
    let a_ownership = Arc::new(ShardOwnership::new(
        names.a.clone(),
        store.clone(),
        a_view.clone(),
        STALE_AFTER_SECS,
    ));
    let a = engine(&pool, coordinator(&pool).await, a_ownership);

    let status = a
        .complete_workflow_execution(Request::new(completion(id, &execution_id)))
        .await
        .expect_err("b holds the shard");
    assert_eq!(status.code(), Code::Unavailable, "{status:?}");
    assert_eq!(status_of(&pool, id).await, "running");

    cleanup(&pool, id, &names).await;
}

/// Rows the completion below would write, if any part of it were applied.
async fn applied(pool: &PgPool, id: Uuid, execution_id: &str) -> (i64, i64) {
    let journal: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM workflow_execution_journal WHERE execution_id = $1",
    )
    .bind(execution_id)
    .fetch_one(pool)
    .await
    .expect("count journal");
    let tasks: i64 =
        sqlx::query_scalar("SELECT count(*) FROM task_executions WHERE workflow_id = $1")
            .bind(id)
            .fetch_one(pool)
            .await
            .expect("count tasks");
    (journal, tasks)
}

/// A batch that schedules a task and then completes: the first command writes
/// nothing the fence guards, the second does.
fn schedule_then_complete(id: Uuid, execution_id: &str) -> CompleteWorkflowExecutionRequest {
    let mut request = completion(id, execution_id);
    request.commands.insert(
        0,
        Command {
            command_type: CommandType::ScheduleTask as i32,
            attributes: Some(command::Attributes::ScheduleTask(
                ScheduleTaskCommandAttributes {
                    task_id: "batch-task-1".to_string(),
                    task_type: "BatchTask".to_string(),
                    task_queue: "default".to_string(),
                    input: b"{}".to_vec(),
                    ..Default::default()
                },
            )),
        },
    );
    request
}

/// The owner is down but still in the live set, holding its shard. The relay
/// is refused, so the request comes back to be handled here — and must be
/// refused before its first command, not after it: a batch half applied and
/// then refused is sent again by the worker, and its first half twice.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_batch_that_cannot_reach_its_owner_is_refused_before_anything_is_applied() {
    let pool = pool().await;
    let names = Names::new();
    let store = register(&pool, &names).await;

    // b owns the workflow and holds its shard, but nothing listens at its
    // address any more.
    let a_view = names.view(&names.a, Some("127.0.0.1:1".to_string()));
    let (id, execution_id) = workflow_owned_by(&pool, &a_view, &names.b).await;
    let b_view = names.view(&names.b, None);
    assert!(
        ShardOwnership::new(names.b.clone(), store.clone(), b_view, STALE_AFTER_SECS)
            .acquire_for(&id.to_string())
            .await
            .unwrap()
            .is_some()
    );

    // a is wired as production wires it: range-strict by the forwarder's view
    // of which peers answer.
    let forwarder = OwnerForwarder::new(a_view.clone(), ForwardConfig::default());
    let a_ownership = Arc::new(
        ShardOwnership::new(
            names.a.clone(),
            store.clone(),
            a_view.clone(),
            STALE_AFTER_SECS,
        )
        .with_range_strict(forwarder.peers()),
    );
    let a = engine(&pool, coordinator(&pool).await, a_ownership.clone())
        .with_owner_forwarder(Arc::new(forwarder.with_ownership(a_ownership)));

    let status = a
        .complete_workflow_execution(Request::new(schedule_then_complete(id, &execution_id)))
        .await
        .expect_err("b holds the shard");

    assert_eq!(status.code(), Code::Unavailable, "{status:?}");
    assert_eq!(status_of(&pool, id).await, "running");
    assert_eq!(
        applied(&pool, id, &execution_id).await,
        (0, 0),
        "nothing of the batch may be applied — not even the task it schedules first"
    );

    let _ = sqlx::query("DELETE FROM task_executions WHERE workflow_id = $1")
        .bind(id)
        .execute(&pool)
        .await;
    cleanup(&pool, id, &names).await;
}

/// A completion with no commands — query or update results only, or a
/// workflow parking — writes nothing the fence guards, and is handled where
/// it lands, as before relaying existed.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_completion_without_commands_is_handled_wherever_it_lands() {
    let pool = pool().await;
    let names = Names::new();
    let store = register(&pool, &names).await;

    let a_view = names.view(&names.a, Some("127.0.0.1:1".to_string()));
    let (id, execution_id) = workflow_owned_by(&pool, &a_view, &names.b).await;
    let b_view = names.view(&names.b, None);
    assert!(
        ShardOwnership::new(names.b.clone(), store.clone(), b_view, STALE_AFTER_SECS)
            .acquire_for(&id.to_string())
            .await
            .unwrap()
            .is_some()
    );

    let forwarder = OwnerForwarder::new(a_view.clone(), ForwardConfig::default());
    let a_ownership = Arc::new(
        ShardOwnership::new(
            names.a.clone(),
            store.clone(),
            a_view.clone(),
            STALE_AFTER_SECS,
        )
        .with_range_strict(forwarder.peers()),
    );
    let a = engine(&pool, coordinator(&pool).await, a_ownership.clone())
        .with_owner_forwarder(Arc::new(forwarder.with_ownership(a_ownership)));

    let mut request = completion(id, &execution_id);
    request.commands.clear();
    a.complete_workflow_execution(Request::new(request))
        .await
        .expect("nothing fenced to write, so nothing to refuse");
    assert_eq!(status_of(&pool, id).await, "running");

    cleanup(&pool, id, &names).await;
}
