//! An event sent by workflow id reaches the caller's own workflow, never
//! another organization's.
//!
//! A workflow id is unique only within an organization's namespace. The
//! event-with-start RPC looked the target up by id alone, so a caller could
//! append an event to another tenant's running workflow just by naming its id.
//! The caller must instead find nothing of its own and start its own workflow.
//!
//! Requires PostgreSQL. Run with:
//!
//! ```bash
//! export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/orcher_test"
//! cargo test -p orcher-core --test event_tenancy -- --ignored
//! ```

use orcher_core::engine::dispatch::PollingManager;
use orcher_core::engine::queues::outbox::OutboxQueue;
use orcher_core::events::EventBus;
use orcher_core::grpc::services::WorkflowService;
use orcher_core::grpc::tenant::ORGANIZATION_METADATA_KEY;
use orcher_core::workflows::{JournalService, WorkflowExecutionService};
use orcher_events::bus::EventBusConfig;
use orcher_proto::orcher::v1::workflow_service_server::WorkflowService as WorkflowServiceTrait;
use orcher_proto::orcher::v1::SendEventWithStartWorkflowRequest;
use orcher_storage::repositories::domains::{
    coordination::OutboxRepository, journal::JournalRepository,
    workflow_execution::WorkflowExecutionRepository,
};
use sqlx::PgPool;
use std::sync::Arc;
use tonic::Request;
use uuid::Uuid;

const NAMESPACE: &str = "default";

async fn pool() -> PgPool {
    let url = std::env::var("TEST_DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:postgres@localhost:5432/orcher_test".to_string()
    });
    PgPool::connect(&url)
        .await
        .expect("connect to the test database")
}

fn service(pool: &PgPool) -> WorkflowService {
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
    WorkflowService::new(execution_service, exec_repo)
        .with_event_support(journal_repo, polling_manager)
}

async fn organization(pool: &PgPool) -> Uuid {
    let owner = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO users (id, email, password_hash, hash_algorithm, password_changed_at) \
         VALUES ($1, $2, 'x', 'argon2', NOW())",
    )
    .bind(owner)
    .bind(format!("owner-{owner}@event-tenancy.test"))
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

/// A running workflow owned by `organization`, with the business id `workflow_id`.
async fn running_workflow(pool: &PgPool, organization: Uuid, workflow_id: &str) -> (Uuid, String) {
    let id = Uuid::new_v4();
    let execution_id = Uuid::new_v4().to_string();
    sqlx::query(
        r#"INSERT INTO workflow_executions
             (id, execution_id, workflow_id, workflow_name, workflow_type, workflow_version,
              namespace, task_queue, status, trigger_type, organization_id,
              submitted_at, created_at, updated_at)
           VALUES ($1, $2, $3, 'orders', 'orders', '1.0.0', $4, 'q', 'running', 'api', $5,
                   NOW(), NOW(), NOW())"#,
    )
    .bind(id)
    .bind(&execution_id)
    .bind(workflow_id)
    .bind(NAMESPACE)
    .bind(organization)
    .execute(pool)
    .await
    .expect("seed workflow");
    (id, execution_id)
}

async fn events_received(pool: &PgPool, execution_id: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM workflow_execution_journal \
         WHERE execution_id = $1 AND event_type = 'EventReceived'",
    )
    .bind(execution_id)
    .fetch_one(pool)
    .await
    .expect("count events")
}

fn send_as(organization: Uuid, workflow_id: &str) -> Request<SendEventWithStartWorkflowRequest> {
    let mut request = Request::new(SendEventWithStartWorkflowRequest {
        workflow_id: workflow_id.to_string(),
        namespace: NAMESPACE.to_string(),
        event_name: "approve".to_string(),
        event_payload: br#"{"approved":true}"#.to_vec(),
        workflow_type: "orders".to_string(),
        task_queue: "q".to_string(),
        ..Default::default()
    });
    request.metadata_mut().insert(
        ORGANIZATION_METADATA_KEY,
        organization.to_string().parse().unwrap(),
    );
    request
}

#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn an_event_never_reaches_another_organizations_workflow() {
    let pool = pool().await;
    let service = service(&pool);
    let workflow_id = format!("order-{}", Uuid::new_v4());

    let owner = organization(&pool).await;
    let other = organization(&pool).await;
    let (_, owners_execution) = running_workflow(&pool, owner, &workflow_id).await;

    // Another organization names the same workflow id.
    let response = service
        .send_event_with_start_workflow(send_as(other, &workflow_id))
        .await
        .expect("event-with-start as the other organization")
        .into_inner();

    assert_eq!(
        events_received(&pool, &owners_execution).await,
        0,
        "the other organization's event was delivered into the owner's workflow"
    );
    assert!(
        response.workflow_started && response.execution_id != owners_execution,
        "finding nothing of its own, the other organization starts its own workflow"
    );

    // The owner's own event still arrives.
    let response = service
        .send_event_with_start_workflow(send_as(owner, &workflow_id))
        .await
        .expect("event-with-start as the owner")
        .into_inner();
    assert!(!response.workflow_started);
    assert_eq!(response.execution_id, owners_execution);
    assert_eq!(events_received(&pool, &owners_execution).await, 1);

    let _ = sqlx::query("DELETE FROM workflow_execution_journal WHERE workflow_id IN (SELECT id::text FROM workflow_executions WHERE workflow_id = $1)")
        .bind(&workflow_id)
        .execute(&pool)
        .await;
    let _ = sqlx::query("DELETE FROM workflow_executions WHERE workflow_id = $1")
        .bind(&workflow_id)
        .execute(&pool)
        .await;
}
