//! An event a client sends with a request id is delivered once, however often
//! the request is repeated.
//!
//! A client that loses the answer to a send retries it with the same request
//! id. SendEvent, and SendEventWithStartWorkflow when it finds the workflow
//! running, appended EventReceived again on every repeat, so the workflow was
//! handed the event twice. Delivery is now once per target run and request id;
//! another request id is another event, and a request without an id is
//! delivered each time, since nothing tells it from a new one. Checked as the
//! target's worker sees it: through the journal its poll response carries.
//!
//! Requires PostgreSQL. Run with:
//!
//! ```bash
//! export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/orcher_test"
//! cargo test -p orcher-core --test event_delivery_once -- --ignored --test-threads=1
//! ```

use std::sync::Arc;

use base64::Engine as _;
use orcher_core::engine::dispatch::{Converter, PollingManager};
use orcher_core::engine::queues::outbox::OutboxQueue;
use orcher_core::events::EventBus;
use orcher_core::grpc::services::WorkflowService;
use orcher_core::grpc::tenant::ORGANIZATION_METADATA_KEY;
use orcher_core::workflows::{JournalService, WorkflowExecutionService};
use orcher_events::bus::EventBusConfig;
use orcher_proto::orcher::v1::workflow_service_server::WorkflowService as WorkflowServiceTrait;
use orcher_proto::orcher::v1::{SendEventRequest, SendEventWithStartWorkflowRequest};
use orcher_storage::repositories::domains::{
    coordination::OutboxRepository, journal::JournalRepository,
    workflow_execution::WorkflowExecutionRepository,
};
use sqlx::PgPool;
use tonic::Request;
use uuid::Uuid;

const NAMESPACE: &str = "default";
const EVENT: &str = "approve";

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

struct Engine {
    service: WorkflowService,
    executions: Arc<WorkflowExecutionRepository>,
    journal: Arc<JournalRepository>,
}

fn engine(pool: &PgPool) -> Engine {
    let journal_repo = Arc::new(JournalRepository::new(pool.clone()));
    let exec_repo = Arc::new(WorkflowExecutionRepository::new(pool.clone()));
    let outbox_queue = Arc::new(OutboxQueue::new(Arc::new(OutboxRepository::new(Arc::new(
        pool.clone(),
    )))));
    let execution_service = Arc::new(WorkflowExecutionService::new(
        exec_repo.clone(),
        Arc::new(JournalService::new(journal_repo.clone())),
        outbox_queue,
        EventBus::new(EventBusConfig::default()),
        pool.clone(),
    ));
    let polling_manager = Arc::new(PollingManager::new(
        exec_repo.clone(),
        journal_repo.clone(),
        300,
    ));
    let service = WorkflowService::new(execution_service, exec_repo.clone())
        .with_event_support(journal_repo.clone(), polling_manager);
    Engine {
        service,
        executions: exec_repo,
        journal: journal_repo,
    }
}

async fn organization(pool: &PgPool) -> Uuid {
    let owner = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO users (id, email, password_hash, hash_algorithm, password_changed_at) \
         VALUES ($1, $2, 'x', 'argon2', NOW())",
    )
    .bind(owner)
    .bind(format!("owner-{owner}@event-delivery-once.test"))
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

/// A running workflow of `organization` with the business id `workflow_id`;
/// returns its execution id.
async fn running(pool: &PgPool, organization: Uuid, workflow_id: &str) -> String {
    let execution_id = Uuid::new_v4().to_string();
    sqlx::query(
        r#"INSERT INTO workflow_executions
             (id, execution_id, workflow_id, workflow_name, workflow_type, workflow_version,
              namespace, task_queue, status, trigger_type, organization_id,
              submitted_at, created_at, updated_at)
           VALUES ($1, $2, $3, 'orders', 'orders', '1.0.0', $4, 'q', 'running', 'api', $5,
                   NOW(), NOW(), NOW())"#,
    )
    .bind(Uuid::new_v4())
    .bind(&execution_id)
    .bind(workflow_id)
    .bind(NAMESPACE)
    .bind(organization)
    .execute(pool)
    .await
    .expect("seed workflow");
    execution_id
}

fn as_org<T>(organization: Uuid, message: T) -> Request<T> {
    let mut request = Request::new(message);
    request.metadata_mut().insert(
        ORGANIZATION_METADATA_KEY,
        organization.to_string().parse().unwrap(),
    );
    request
}

fn send_event(
    organization: Uuid,
    workflow_id: &str,
    payload: &[u8],
    request_id: &str,
) -> Request<SendEventRequest> {
    as_org(
        organization,
        SendEventRequest {
            workflow_id: workflow_id.to_string(),
            namespace: NAMESPACE.to_string(),
            event_name: EVENT.to_string(),
            payload: payload.to_vec(),
            request_id: request_id.to_string(),
            ..Default::default()
        },
    )
}

fn send_event_with_start(
    organization: Uuid,
    workflow_id: &str,
    payload: &[u8],
    request_id: &str,
) -> Request<SendEventWithStartWorkflowRequest> {
    as_org(
        organization,
        SendEventWithStartWorkflowRequest {
            workflow_id: workflow_id.to_string(),
            namespace: NAMESPACE.to_string(),
            event_name: EVENT.to_string(),
            event_payload: payload.to_vec(),
            workflow_type: "orders".to_string(),
            task_queue: "q".to_string(),
            request_id: request_id.to_string(),
            ..Default::default()
        },
    )
}

/// The events the run's worker is handed when it next claims the run, as
/// payloads.
async fn events_the_run_replays(engine: &Engine, execution_id: &str) -> Vec<Vec<u8>> {
    let run = engine
        .executions
        .claim_execution_by_id(execution_id, "worker-a", 300, None, None)
        .await
        .expect("claim the run")
        .expect("the run is claimable");
    Converter::new(engine.journal.clone())
        .to_poll_response(&run)
        .await
        .expect("build the run's poll response")
        .journal
        .into_iter()
        .filter(|entry| entry.entry_type == "EventReceived")
        .map(|entry| {
            let payload = entry.payload.expect("EventReceived carries a payload");
            assert_eq!(payload["event_name"].as_str(), Some(EVENT));
            base64::engine::general_purpose::STANDARD
                .decode(payload["payload"].as_str().unwrap_or_default())
                .expect("the payload is base64")
        })
        .collect()
}

async fn cleanup(pool: &PgPool, workflow_ids: &[&str]) {
    for workflow_id in workflow_ids {
        let _ = sqlx::query(
            "DELETE FROM workflow_execution_journal WHERE execution_id IN \
             (SELECT execution_id FROM workflow_executions WHERE workflow_id = $1)",
        )
        .bind(workflow_id)
        .execute(pool)
        .await;
        let _ = sqlx::query("DELETE FROM workflow_executions WHERE workflow_id = $1")
            .bind(workflow_id)
            .execute(pool)
            .await;
    }
}

/// A SendEvent repeated with its request id — one after the other, and two
/// racing — is recorded, and answered as recorded, every time, and the target
/// replays the event once.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_repeated_send_event_is_delivered_once() {
    let pool = pool().await;
    let engine = engine(&pool);
    let org = organization(&pool).await;
    let workflow_id = format!("order-{}", Uuid::new_v4());
    let execution_id = running(&pool, org, &workflow_id).await;
    let request_id = Uuid::new_v4().to_string();

    let mut answers = Vec::new();
    for _ in 0..2 {
        answers.push(
            engine
                .service
                .send_event(send_event(org, &workflow_id, b"first", &request_id))
                .await,
        );
    }
    let (a, b) = tokio::join!(
        engine
            .service
            .send_event(send_event(org, &workflow_id, b"first", &request_id)),
        engine
            .service
            .send_event(send_event(org, &workflow_id, b"first", &request_id)),
    );
    answers.extend([a, b]);
    let received = events_the_run_replays(&engine, &execution_id).await;

    cleanup(&pool, &[&workflow_id]).await;

    for answer in answers {
        assert!(
            answer
                .expect("a repeat is answered, not refused")
                .into_inner()
                .recorded,
            "a repeat is answered as recorded"
        );
    }
    assert_eq!(
        received,
        vec![b"first".to_vec()],
        "the target replays a repeated send once"
    );
}

/// Sends with different request ids are different events, both delivered; so
/// are two sends without a request id, which nothing tells apart from new ones.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn sends_with_different_request_ids_are_each_delivered() {
    let pool = pool().await;
    let engine = engine(&pool);
    let org = organization(&pool).await;
    let workflow_id = format!("order-{}", Uuid::new_v4());
    let execution_id = running(&pool, org, &workflow_id).await;

    for (payload, request_id) in [
        (&b"one"[..], Uuid::new_v4().to_string()),
        (&b"two"[..], Uuid::new_v4().to_string()),
        (&b"three"[..], String::new()),
        (&b"three"[..], String::new()),
    ] {
        engine
            .service
            .send_event(send_event(org, &workflow_id, payload, &request_id))
            .await
            .expect("send the event");
    }
    let received = events_the_run_replays(&engine, &execution_id).await;

    cleanup(&pool, &[&workflow_id]).await;

    assert_eq!(
        received,
        vec![
            b"one".to_vec(),
            b"two".to_vec(),
            b"three".to_vec(),
            b"three".to_vec()
        ],
        "each request id, and each send without one, is its own event"
    );
}

/// The scope is the target run and the request id: one request id given to
/// sends to two runs delivers to each.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_request_id_is_scoped_to_its_target_run() {
    let pool = pool().await;
    let engine = engine(&pool);
    let org = organization(&pool).await;
    let first = format!("order-{}", Uuid::new_v4());
    let second = format!("order-{}", Uuid::new_v4());
    let first_execution = running(&pool, org, &first).await;
    let second_execution = running(&pool, org, &second).await;
    let request_id = Uuid::new_v4().to_string();

    for workflow_id in [&first, &second] {
        engine
            .service
            .send_event(send_event(org, workflow_id, b"both", &request_id))
            .await
            .expect("send the event");
    }
    let first_received = events_the_run_replays(&engine, &first_execution).await;
    let second_received = events_the_run_replays(&engine, &second_execution).await;

    cleanup(&pool, &[&first, &second]).await;

    assert_eq!(first_received, vec![b"both".to_vec()]);
    assert_eq!(second_received, vec![b"both".to_vec()]);
}

/// A SendEventWithStartWorkflow that finds the workflow running, repeated with
/// its request id, delivers its event once and is answered with the run both
/// times, started by neither.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_repeated_event_with_start_to_a_running_workflow_is_delivered_once() {
    let pool = pool().await;
    let engine = engine(&pool);
    let org = organization(&pool).await;
    let workflow_id = format!("order-{}", Uuid::new_v4());
    let execution_id = running(&pool, org, &workflow_id).await;
    let request_id = Uuid::new_v4().to_string();

    let mut answers = Vec::new();
    for _ in 0..3 {
        answers.push(
            engine
                .service
                .send_event_with_start_workflow(send_event_with_start(
                    org,
                    &workflow_id,
                    b"started?",
                    &request_id,
                ))
                .await,
        );
    }
    // Another request id is another event.
    let other = engine
        .service
        .send_event_with_start_workflow(send_event_with_start(
            org,
            &workflow_id,
            b"another",
            &Uuid::new_v4().to_string(),
        ))
        .await;
    let received = events_the_run_replays(&engine, &execution_id).await;

    cleanup(&pool, &[&workflow_id]).await;

    for answer in answers {
        let answer = answer.expect("a repeat is answered").into_inner();
        assert!(!answer.workflow_started, "the workflow was running");
        assert_eq!(answer.execution_id, execution_id);
    }
    other.expect("another request id is delivered");
    assert_eq!(
        received,
        vec![b"started?".to_vec(), b"another".to_vec()],
        "the running workflow replays the repeated event once, and the other one"
    );
}
