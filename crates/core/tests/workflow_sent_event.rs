//! An event one workflow sends another, with the SendEvent command, reaches
//! the target — once — in the journal the target is replayed from.
//!
//! The handler wrote `EventReceived` under the target's id rendered by its
//! `Display`, which left out the hyphens, while the target's journal is keyed
//! and read by the hyphenated UUID. The append succeeded, the target was woken,
//! and its replay found nothing: it stayed parked on the event forever. The
//! receiving side is checked here as the target's worker sees it — through the
//! poll response's journal — not by counting rows under whatever key the
//! sender happened to use.
//!
//! Requires PostgreSQL. Run with:
//!
//! ```bash
//! export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/orcher_test"
//! cargo test -p orcher-core --test workflow_sent_event -- --ignored --test-threads=1
//! ```

// Helpers return `tonic::Status`, the error the engine's gRPC services answer with.
#![allow(clippy::result_large_err)]

use std::sync::Arc;

use base64::Engine as _;
use orcher_core::engine::dispatch::{Converter, PollingManager};
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
    command, Command, CommandType, CompleteWorkflowExecutionRequest, SendEventCommandAttributes,
};
use orcher_storage::repositories::domains::{
    child_workflow::ChildWorkflowRepository, coordination::OutboxRepository,
    journal::JournalRepository, task_execution::TaskExecutionRepository,
    workflow_execution::WorkflowExecutionRepository,
};
use orcher_storage::repositories::{ExecutionLogRepository, PostgresExecutionLogRepository};
use orcher_storage::sharding::pool_routing::ShardPoolResolver;
use sqlx::PgPool;
use tonic::Request;
use uuid::Uuid;

const NAMESPACE: &str = "sent-events";
const WORKER: &str = "worker-a";
const EVENT: &str = "approve";
const PAYLOAD: &[u8] = br#"{"approved":true}"#;

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

/// One engine's completion path, routed across `shards` when given.
struct Engine {
    service: ExecutionService,
    executions: Arc<WorkflowExecutionRepository>,
    journal: Arc<JournalRepository>,
}

async fn engine(pool: &PgPool, shards: Option<Arc<dyn ShardPoolResolver>>) -> Engine {
    let journal_repo = Arc::new(JournalRepository::new(pool.clone()));
    let exec_repo = Arc::new(WorkflowExecutionRepository::new(pool.clone()));
    let task_execution_repo = Arc::new(TaskExecutionRepository::new(pool.clone()));
    if let Some(router) = shards {
        journal_repo.set_shard_resolver(router.clone());
        exec_repo.set_shard_resolver(router.clone());
        task_execution_repo.set_shard_resolver(router);
    }
    let journal_service = Arc::new(JournalService::new(journal_repo.clone()));
    let child_repo = Arc::new(ChildWorkflowRepository::new(pool.clone()));
    let outbox_queue = Arc::new(OutboxQueue::new(Arc::new(OutboxRepository::new(Arc::new(
        pool.clone(),
    )))));
    let event_bus = EventBus::new(EventBusConfig::default());
    let execution_log_repo = Arc::new(PostgresExecutionLogRepository::new(pool.clone()));
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
        journal_repo.clone(),
        exec_repo.clone(),
        task_execution_repo,
        pool.clone(),
    );
    Engine {
        service,
        executions: exec_repo,
        journal: journal_repo,
    }
}

/// A running workflow with the business id `workflow_id`, on `db`, claimed by
/// the worker — as a workflow parked on an event holds its claim. Returns its
/// internal id, execution id and the activation token its poll carried.
async fn running(
    engine: &Engine,
    db: &PgPool,
    id: Uuid,
    workflow_id: &str,
) -> (Uuid, String, String) {
    let execution_id = Uuid::new_v4().to_string();
    sqlx::query(
        r#"
        INSERT INTO workflow_executions (
            id, execution_id, workflow_id, workflow_name, workflow_type, workflow_version,
            namespace, task_queue, status, parameters, context, environment, progress,
            total_steps, completed_steps, failed_steps, retried_steps
        )
        VALUES ($1, $2, $3, 'sent-event', 'sent-event', '1.0.0', $4, 'sent-events', 'running',
                '{}', '{}', '{}', '{}', 0, 0, 0, 0)
        "#,
    )
    .bind(id)
    .bind(&execution_id)
    .bind(workflow_id)
    .bind(NAMESPACE)
    .execute(db)
    .await
    .expect("insert workflow");
    let seq = engine
        .executions
        .claim_execution_by_id(&execution_id, WORKER, 300, None, None)
        .await
        .expect("claim")
        .expect("the workflow is claimable")
        .activation_seq;
    let token = orcher_core::grpc::activation_token::encode(&execution_id, seq);
    (id, execution_id, token)
}

fn send_event(target_workflow_id: &str) -> Command {
    Command {
        command_type: CommandType::SendEvent as i32,
        attributes: Some(command::Attributes::SendEvent(SendEventCommandAttributes {
            workflow_id: target_workflow_id.to_string(),
            event_name: EVENT.to_string(),
            payload: PAYLOAD.to_vec(),
            ..Default::default()
        })),
    }
}

/// The sender's activation completes with `commands`, as its worker reports.
async fn complete(
    engine: &Engine,
    id: Uuid,
    execution_id: &str,
    token: &str,
    commands: Vec<Command>,
) -> Result<(), tonic::Status> {
    engine
        .service
        .complete_workflow_execution(Request::new(CompleteWorkflowExecutionRequest {
            workflow_id: id.to_string(),
            execution_id: execution_id.to_string(),
            namespace: NAMESPACE.to_string(),
            identity: WORKER.to_string(),
            task_token: format!("{id}:{execution_id}").into_bytes(),
            stream_entry_id: token.to_string(),
            commands,
            ..Default::default()
        }))
        .await
        .map(|_| ())
}

/// The `EventReceived` entries the target's worker is handed on its next poll,
/// as `(event name, payload)`.
async fn events_the_target_replays(engine: &Engine, execution_id: &str) -> Vec<(String, Vec<u8>)> {
    let target = engine
        .executions
        .find_by_execution_id(execution_id)
        .await
        .expect("find the target")
        .expect("the target exists");
    Converter::new(engine.journal.clone())
        .to_poll_response(&target)
        .await
        .expect("build the target's poll response")
        .journal
        .into_iter()
        .filter(|entry| entry.entry_type == "EventReceived")
        .map(|entry| {
            let payload = entry.payload.expect("EventReceived carries a payload");
            (
                payload["event_name"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string(),
                base64::engine::general_purpose::STANDARD
                    .decode(payload["payload"].as_str().unwrap_or_default())
                    .expect("the payload is base64"),
            )
        })
        .collect()
}

async fn claimed_by(db: &PgPool, id: Uuid) -> Option<String> {
    sqlx::query_scalar("SELECT claimed_by FROM workflow_executions WHERE id = $1")
        .bind(id)
        .fetch_one(db)
        .await
        .expect("read the claim")
}

async fn cleanup(db: &PgPool, runs: &[(Uuid, &str)]) {
    for (id, execution_id) in runs {
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
            let _ = sqlx::query(q).bind(key).execute(db).await;
        }
    }
}

/// Workflow A sends an event to running workflow B. B's next replay holds it,
/// exactly once, and B's claim is released so it is handed out again. A
/// retried completion of the same activation does not deliver it twice.
async fn a_sent_event_reaches_its_target(
    engine: &Engine,
    a: Uuid,
    a_db: &PgPool,
    b: Uuid,
    b_db: &PgPool,
) {
    let b_workflow_id = format!("receiver-{}", Uuid::new_v4());
    let (a, a_execution, a_token) =
        running(engine, a_db, a, &format!("sender-{}", Uuid::new_v4())).await;
    let (b, b_execution, b_token) = running(engine, b_db, b, &b_workflow_id).await;
    // B's activation ends with nothing to do: it is parked on the event, still
    // holding its claim.
    complete(engine, b, &b_execution, &b_token, vec![])
        .await
        .expect("the target's completion is accepted");
    assert_eq!(claimed_by(b_db, b).await.as_deref(), Some(WORKER));

    complete(
        engine,
        a,
        &a_execution,
        &a_token,
        vec![send_event(&b_workflow_id)],
    )
    .await
    .expect("the sender's completion is accepted");
    let released = claimed_by(b_db, b).await;

    // B's worker polls again, as the wake-up asks, and replays.
    let reclaimed = engine
        .executions
        .claim_execution_by_id(&b_execution, WORKER, 300, None, None)
        .await
        .expect("claim the target again")
        .is_some();
    let received = if reclaimed {
        events_the_target_replays(engine, &b_execution).await
    } else {
        Vec::new()
    };

    // The retried completion is answered or refused; either way it must not
    // deliver the event again.
    let _ = complete(
        engine,
        a,
        &a_execution,
        &a_token,
        vec![send_event(&b_workflow_id)],
    )
    .await;
    let after_retry = if reclaimed {
        events_the_target_replays(engine, &b_execution).await
    } else {
        Vec::new()
    };

    cleanup(a_db, &[(a, &a_execution)]).await;
    cleanup(b_db, &[(b, &b_execution)]).await;

    assert_eq!(
        released, None,
        "the target's claim is released so it runs again"
    );
    assert!(reclaimed, "the target is handed out again");
    assert_eq!(
        received,
        vec![(EVENT.to_string(), PAYLOAD.to_vec())],
        "the target's replay holds the sent event, once"
    );
    assert_eq!(
        after_retry, received,
        "a retried completion delivers nothing more"
    );
}

#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn an_event_one_workflow_sends_another_is_received_once() {
    let pool = pool().await;
    let engine = engine(&pool, None).await;
    a_sent_event_reaches_its_target(&engine, Uuid::new_v4(), &pool, Uuid::new_v4(), &pool).await;
}

/// The test database, and a second database beside it for shards routed away
/// from the first, created and migrated on first use.
/// An event addressed by the receiver's run id in the workflow id field, as
/// callers have long named runs, reaches that run: the command resolves its
/// target as every request naming a run does.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn an_event_sent_to_a_run_id_reaches_that_run() {
    let pool = pool().await;
    let engine = engine(&pool, None).await;
    let (a, a_execution, a_token) = running(
        &engine,
        &pool,
        Uuid::new_v4(),
        &format!("sender-{}", Uuid::new_v4()),
    )
    .await;
    let (b, b_execution, b_token) = running(
        &engine,
        &pool,
        Uuid::new_v4(),
        &format!("receiver-{}", Uuid::new_v4()),
    )
    .await;
    complete(&engine, b, &b_execution, &b_token, vec![])
        .await
        .expect("the target's completion is accepted");

    complete(
        &engine,
        a,
        &a_execution,
        &a_token,
        vec![send_event(&b_execution)],
    )
    .await
    .expect("the sender's completion is accepted");
    let reclaimed = engine
        .executions
        .claim_execution_by_id(&b_execution, WORKER, 300, None, None)
        .await
        .expect("claim the target again")
        .is_some();
    let received = if reclaimed {
        events_the_target_replays(&engine, &b_execution).await
    } else {
        Vec::new()
    };

    cleanup(&pool, &[(a, &a_execution), (b, &b_execution)]).await;

    assert!(reclaimed, "the target is handed out again");
    assert_eq!(
        received,
        vec![(EVENT.to_string(), PAYLOAD.to_vec())],
        "the run named by its id receives the event"
    );
}

async fn shard_database(pool: &PgPool) -> PgPool {
    use sqlx::postgres::PgConnectOptions;
    use std::str::FromStr;

    let url = std::env::var("TEST_DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:postgres@localhost:5432/orcher_test".to_string()
    });
    let options = PgConnectOptions::from_str(&url).expect("TEST_DATABASE_URL");
    let name = format!(
        "{}_sentevent_shard",
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

/// The same, with the target's shard on another database than the sender's:
/// the event is journaled where the target lives and read from there.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn an_event_reaches_a_target_on_another_shard_database() {
    use orcher_cluster::{MultiPoolRouter, ShardRouter};

    let pool = pool().await;
    let shard_db = shard_database(&pool).await;
    let router = Arc::new(MultiPoolRouter::with_vshards(
        vec![pool.clone(), shard_db.clone()],
        16,
    ));
    let on = |db: usize| {
        let router = router.clone();
        move |id: &Uuid| {
            std::ptr::eq(
                ShardRouter::pool_for_workflow(router.as_ref(), &id.to_string()),
                ShardRouter::all_pools(router.as_ref())[db],
            )
        }
    };
    let engine = engine(&pool, Some(router.clone())).await;
    // Each id is picked by where the router puts it: the sender on the first
    // database, the target on the second.
    let a = std::iter::repeat_with(Uuid::new_v4).find(on(0)).unwrap();
    let b = std::iter::repeat_with(Uuid::new_v4).find(on(1)).unwrap();
    a_sent_event_reaches_its_target(&engine, a, &pool, b, &shard_db).await;
}
