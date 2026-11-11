//! A workflow id names at most one open execution in an organization's
//! namespace.
//!
//! StartWorkflow used to accept a second start of an id whose execution was
//! still running: it answered success and recorded a second execution, so a
//! caller that relied on the id to make its start idempotent got the work done
//! twice. A duplicate is now refused with ALREADY_EXISTS naming the execution
//! in the way, and the refusal is decided by a unique index, so starts racing
//! each other on different engines cannot both win.
//!
//! Requires PostgreSQL. Run with:
//!
//! ```bash
//! export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/orcher_test"
//! cargo test -p orcher-core --test workflow_id_reuse -- --ignored --test-threads=1
//! ```

use orcher_core::engine::dispatch::PollingManager;
use orcher_core::engine::queues::outbox::OutboxQueue;
use orcher_core::events::EventBus;
use orcher_core::grpc::services::WorkflowService;
use orcher_core::grpc::tenant::ORGANIZATION_METADATA_KEY;
use orcher_core::workflows::{JournalService, WorkflowExecutionService};
use orcher_events::bus::EventBusConfig;
use orcher_proto::orcher::v1::workflow_service_server::WorkflowService as WorkflowServiceTrait;
use orcher_proto::orcher::v1::{
    BatchStartWorkflowRequest, SendEventWithStartWorkflowRequest, StartWorkflowRequest,
    StartWorkflowResponse, WorkflowIdReusePolicy,
};
use orcher_storage::repositories::domains::{
    coordination::OutboxRepository, journal::JournalRepository,
    workflow_execution::WorkflowExecutionRepository,
};
use sqlx::PgPool;
use std::sync::Arc;
use tonic::{Code, Request, Status};
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
    services(pool).0
}

/// The RPC service, and the execution service under it for starts the RPCs
/// do not make (a child's).
fn services(pool: &PgPool) -> (WorkflowService, Arc<WorkflowExecutionService>) {
    let journal_repo = Arc::new(JournalRepository::new(pool.clone()));
    let exec_repo = Arc::new(WorkflowExecutionRepository::new(pool.clone()));
    let outbox_queue = Arc::new(OutboxQueue::new(Arc::new(OutboxRepository::new(Arc::new(
        pool.clone(),
    )))));
    let execution_service = Arc::new(WorkflowExecutionService::new(
        exec_repo.clone(),
        Arc::new(JournalService::new(journal_repo)),
        outbox_queue,
        EventBus::new(EventBusConfig::default()),
        pool.clone(),
    ));
    (
        WorkflowService::new(execution_service.clone(), exec_repo),
        execution_service,
    )
}

async fn organization(pool: &PgPool) -> Uuid {
    let owner = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO users (id, email, password_hash, hash_algorithm, password_changed_at) \
         VALUES ($1, $2, 'x', 'argon2', NOW())",
    )
    .bind(owner)
    .bind(format!("owner-{owner}@workflow-id-reuse.test"))
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

fn start_as(organization: Uuid, workflow_id: &str) -> Request<StartWorkflowRequest> {
    let mut request = Request::new(StartWorkflowRequest {
        workflow_id: workflow_id.to_string(),
        workflow_type: "orders".to_string(),
        task_queue: "q".to_string(),
        namespace: NAMESPACE.to_string(),
        ..Default::default()
    });
    request.metadata_mut().insert(
        ORGANIZATION_METADATA_KEY,
        organization.to_string().parse().unwrap(),
    );
    request
}

fn start_under(
    organization: Uuid,
    workflow_id: &str,
    policy: WorkflowIdReusePolicy,
) -> Request<StartWorkflowRequest> {
    let mut request = start_as(organization, workflow_id);
    request.get_mut().workflow_id_reuse_policy = policy as i32;
    request
}

async fn status_of(pool: &PgPool, execution_id: &str) -> String {
    sqlx::query_scalar("SELECT status FROM workflow_executions WHERE execution_id = $1")
        .bind(execution_id)
        .fetch_one(pool)
        .await
        .expect("read status")
}

async fn start(
    service: &WorkflowService,
    request: Request<StartWorkflowRequest>,
) -> Result<StartWorkflowResponse, Status> {
    service
        .start_workflow(request)
        .await
        .map(|r| r.into_inner())
}

/// The execution a refusal names, from its trailing metadata.
fn execution_in_the_way(status: &Status) -> Option<String> {
    status
        .metadata()
        .get("x-orcher-execution-id")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
}

async fn executions_of(pool: &PgPool, organization: Uuid, workflow_id: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM workflow_executions \
         WHERE workflow_id = $1 AND organization_id = $2",
    )
    .bind(workflow_id)
    .bind(organization)
    .fetch_one(pool)
    .await
    .expect("count executions")
}

async fn set_status(pool: &PgPool, execution_id: &str, status: &str) {
    sqlx::query(
        "UPDATE workflow_executions SET status = $2, completed_at = NOW() \
         WHERE execution_id = $1",
    )
    .bind(execution_id)
    .bind(status)
    .execute(pool)
    .await
    .expect("close execution");
}

async fn forget(pool: &PgPool, workflow_id: &str) {
    for sql in [
        "DELETE FROM workflow_execution_journal WHERE workflow_id IN \
         (SELECT id::text FROM workflow_executions WHERE workflow_id = $1)",
        "DELETE FROM outbox_queue WHERE workflow_id IN \
         (SELECT id FROM workflow_executions WHERE workflow_id = $1)",
        "DELETE FROM workflow_executions WHERE workflow_id = $1",
    ] {
        let _ = sqlx::query(sql).bind(workflow_id).execute(pool).await;
    }
}

#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_running_workflow_id_is_not_started_twice() {
    let pool = pool().await;
    let service = service(&pool);
    let organization = organization(&pool).await;
    let workflow_id = format!("order-{}", Uuid::new_v4());

    let first = start(&service, start_as(organization, &workflow_id))
        .await
        .expect("the first start");

    let refused = start(&service, start_as(organization, &workflow_id))
        .await
        .expect_err("a second start of a running workflow id was accepted");
    assert_eq!(refused.code(), Code::AlreadyExists, "{refused:?}");
    assert_eq!(
        execution_in_the_way(&refused).as_deref(),
        Some(first.execution_id.as_str()),
        "the refusal names the running execution"
    );
    assert_eq!(executions_of(&pool, organization, &workflow_id).await, 1);

    // Once it has closed, the id may be started again.
    set_status(&pool, &first.execution_id, "completed").await;
    let again = start(&service, start_as(organization, &workflow_id))
        .await
        .expect("a start after the previous run completed");
    assert_ne!(again.execution_id, first.execution_id);

    forget(&pool, &workflow_id).await;
}

#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn racing_starts_of_one_workflow_id_start_it_once() {
    let pool = pool().await;
    let service = Arc::new(service(&pool));
    let organization = organization(&pool).await;
    let workflow_id = format!("order-{}", Uuid::new_v4());

    let starts: Vec<_> = (0..16)
        .map(|_| {
            let service = service.clone();
            let workflow_id = workflow_id.clone();
            tokio::spawn(async move { start(&service, start_as(organization, &workflow_id)).await })
        })
        .collect();

    let mut started = Vec::new();
    for handle in starts {
        match handle.await.expect("start task") {
            Ok(response) => started.push(response.execution_id),
            Err(status) => assert_eq!(status.code(), Code::AlreadyExists, "{status:?}"),
        }
    }
    assert_eq!(
        started.len(),
        1,
        "racing starts each started a run: {started:?}"
    );
    assert_eq!(executions_of(&pool, organization, &workflow_id).await, 1);

    forget(&pool, &workflow_id).await;
}

#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_workflow_id_belongs_to_one_organization() {
    let pool = pool().await;
    let service = service(&pool);
    let owner = organization(&pool).await;
    let other = organization(&pool).await;
    let workflow_id = format!("order-{}", Uuid::new_v4());

    let owners = start(&service, start_as(owner, &workflow_id))
        .await
        .expect("the owner's start");

    // Another organization's id of the same name is its own.
    let others = start(&service, start_as(other, &workflow_id))
        .await
        .expect("another organization's start of the same id was refused");
    assert_ne!(others.execution_id, owners.execution_id);

    // And its refusal, when it comes, names its own execution, not the owner's.
    let refused = start(&service, start_as(other, &workflow_id))
        .await
        .expect_err("a second start in the other organization");
    assert_eq!(refused.code(), Code::AlreadyExists);
    assert_eq!(
        execution_in_the_way(&refused).as_deref(),
        Some(others.execution_id.as_str())
    );

    forget(&pool, &workflow_id).await;
}

#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn the_reuse_policy_decides_what_a_closed_run_allows() {
    use WorkflowIdReusePolicy as Policy;
    let pool = pool().await;
    let service = service(&pool);
    let organization = organization(&pool).await;
    let workflow_id = format!("order-{}", Uuid::new_v4());

    let first = start(&service, start_as(organization, &workflow_id))
        .await
        .expect("the first start");
    set_status(&pool, &first.execution_id, "completed").await;

    // A completed run refuses both a start that never reuses an id and one
    // that reuses it only after a failure, and names the run it judged by.
    for policy in [Policy::RejectDuplicate, Policy::AllowDuplicateFailedOnly] {
        let refused = start(&service, start_under(organization, &workflow_id, policy))
            .await
            .expect_err("a start the policy forbids after a completed run");
        assert_eq!(
            refused.code(),
            Code::AlreadyExists,
            "{policy:?}: {refused:?}"
        );
        assert_eq!(
            execution_in_the_way(&refused).as_deref(),
            Some(first.execution_id.as_str())
        );
    }

    // Allowing duplicates, it starts; a failed run then allows a start that
    // reuses only after failures, and still refuses one that never reuses.
    let second = start(
        &service,
        start_under(organization, &workflow_id, Policy::AllowDuplicate),
    )
    .await
    .expect("a duplicate after a completed run, allowed");
    set_status(&pool, &second.execution_id, "cancelled").await;
    start(
        &service,
        start_under(organization, &workflow_id, Policy::RejectDuplicate),
    )
    .await
    .expect_err("never reusing, even after a failure");
    let third = start(
        &service,
        start_under(organization, &workflow_id, Policy::AllowDuplicateFailedOnly),
    )
    .await
    .expect("a start after a cancelled run, allowed when only failures are");
    assert_eq!(executions_of(&pool, organization, &workflow_id).await, 3);

    // Running, it refuses under every policy that does not terminate.
    for policy in [
        Policy::Unspecified,
        Policy::AllowDuplicate,
        Policy::AllowDuplicateFailedOnly,
        Policy::RejectDuplicate,
    ] {
        let refused = start(&service, start_under(organization, &workflow_id, policy))
            .await
            .expect_err("a start of a running workflow id");
        assert_eq!(
            execution_in_the_way(&refused).as_deref(),
            Some(third.execution_id.as_str()),
            "{policy:?}"
        );
    }

    forget(&pool, &workflow_id).await;
}

#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn terminate_if_running_replaces_the_open_run() {
    let pool = pool().await;
    let service = service(&pool);
    let organization = organization(&pool).await;
    let workflow_id = format!("order-{}", Uuid::new_v4());

    let first = start(&service, start_as(organization, &workflow_id))
        .await
        .expect("the first start");
    let second = start(
        &service,
        start_under(
            organization,
            &workflow_id,
            WorkflowIdReusePolicy::TerminateIfRunning,
        ),
    )
    .await
    .expect("terminate-if-running starts over an open run");

    assert_ne!(second.execution_id, first.execution_id);
    assert_eq!(status_of(&pool, &first.execution_id).await, "terminated");
    assert_eq!(status_of(&pool, &second.execution_id).await, "running");

    forget(&pool, &workflow_id).await;
}

#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_failure_waiting_for_its_retry_still_holds_the_id() {
    let pool = pool().await;
    let service = service(&pool);
    let organization = organization(&pool).await;
    let workflow_id = format!("order-{}", Uuid::new_v4());

    let first = start(&service, start_as(organization, &workflow_id))
        .await
        .expect("the first start");
    // Failed, with retries left and none decided against: a retry is coming.
    sqlx::query(
        "UPDATE workflow_executions SET status = 'failed', max_retries = 3, retry_count = 0, \
         completed_at = NOW() WHERE execution_id = $1",
    )
    .bind(&first.execution_id)
    .execute(&pool)
    .await
    .expect("fail the run");

    let refused = start(&service, start_as(organization, &workflow_id))
        .await
        .expect_err("a start while a retry is coming");
    assert_eq!(
        execution_in_the_way(&refused).as_deref(),
        Some(first.execution_id.as_str())
    );

    // Settled, the failure is final and the id is free.
    sqlx::query(
        "UPDATE workflow_executions SET retries_settled_at = NOW() WHERE execution_id = $1",
    )
    .bind(&first.execution_id)
    .execute(&pool)
    .await
    .expect("settle the failure");
    start(&service, start_as(organization, &workflow_id))
        .await
        .expect("a start once the failure is final");

    forget(&pool, &workflow_id).await;
}

/// A child's workflow id is its parent's name for it ("child_0"), unique per
/// parent: two parents each running their child_0 is correct, and neither is
/// in the way of a top-level workflow that happens to share the name.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn children_of_one_name_run_side_by_side() {
    let pool = pool().await;
    let (service, executions) = services(&pool);
    let organization = organization(&pool).await;
    let child_id = format!("child-{}", Uuid::new_v4());

    let mut parents = Vec::new();
    for _ in 0..2 {
        let parent_id = format!("parent-{}", Uuid::new_v4());
        let parent = start(&service, start_as(organization, &parent_id))
            .await
            .expect("a parent");
        executions
            .start_workflow(
                orcher_core::workflows::RunStart {
                    workflow_name: "orders".to_string(),
                    workflow_version: "1.0".to_string(),
                    namespace: NAMESPACE.to_string(),
                    task_queue: "q".to_string(),
                    input: serde_json::Value::Null,
                    workflow_id: Some(child_id.clone()),
                    labels: None,
                    max_retries: None,
                    retry_policy: None,
                    timeouts: orcher_core::workflows::RunTimeouts::default(),
                    annotations: None,
                    start_delay: None,
                    request_id: None,
                },
                Some(parent.execution_id.clone()),
                Some(organization),
            )
            .await
            .expect("each parent starts its own child of the same name");
        parents.push(parent_id);
    }

    start(&service, start_as(organization, &child_id))
        .await
        .expect("a top-level workflow sharing a child's name");
    assert_eq!(executions_of(&pool, organization, &child_id).await, 3);

    forget(&pool, &child_id).await;
    for parent_id in parents {
        forget(&pool, &parent_id).await;
    }
}

#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_batch_naming_a_running_id_starts_nothing() {
    let pool = pool().await;
    let service = service(&pool);
    let organization = organization(&pool).await;
    let running = format!("order-{}", Uuid::new_v4());
    let fresh = format!("order-{}", Uuid::new_v4());

    start(&service, start_as(organization, &running))
        .await
        .expect("the running workflow");

    let mut batch = Request::new(BatchStartWorkflowRequest {
        workflows: vec![
            start_as(organization, &fresh).into_inner(),
            start_as(organization, &running).into_inner(),
        ],
    });
    batch.metadata_mut().insert(
        ORGANIZATION_METADATA_KEY,
        organization.to_string().parse().unwrap(),
    );
    let refused = service
        .batch_start_workflow(batch)
        .await
        .expect_err("a batch naming a running workflow id");
    assert_eq!(refused.code(), Code::AlreadyExists, "{refused:?}");
    assert_eq!(executions_of(&pool, organization, &fresh).await, 0);
    assert_eq!(executions_of(&pool, organization, &running).await, 1);

    forget(&pool, &running).await;
    forget(&pool, &fresh).await;
}

#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn an_unknown_reuse_policy_is_refused() {
    let pool = pool().await;
    let service = service(&pool);
    let organization = organization(&pool).await;
    let mut request = start_as(organization, &format!("order-{}", Uuid::new_v4()));
    request.get_mut().workflow_id_reuse_policy = 99;
    let refused = start(&service, request)
        .await
        .expect_err("a policy this server does not know");
    assert_eq!(refused.code(), Code::InvalidArgument);
}

/// V059 on a database that already has the defect's duplicates: the newest
/// open run of each id holds it, the others keep running unmarked, a child is
/// left alone, and the unique index still builds — through the runner, as a
/// deploy applies it.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn the_migration_takes_existing_duplicates_in_its_stride() {
    let pool = pool().await;
    let organization = organization(&pool).await;
    let workflow_id = format!("order-{}", Uuid::new_v4());

    // Two open runs of one id, as the defect left them, and one closed; then
    // a child of one of them that shares the id. None holds anything yet.
    let mut runs = Vec::new();
    for (status, age) in [("running", 3), ("running", 2), ("completed", 1)] {
        let execution_id = Uuid::new_v4().to_string();
        sqlx::query(
            r#"INSERT INTO workflow_executions
                 (id, execution_id, workflow_id, workflow_name, workflow_type, workflow_version,
                  namespace, task_queue, status, trigger_type, organization_id, root_execution_id,
                  submitted_at, created_at, updated_at)
               VALUES ($1, $2, $3, 'orders', 'orders', '1.0', $4, 'q', $5, 'api', $6, $2,
                       NOW() - make_interval(secs => $7), NOW(), NOW())"#,
        )
        .bind(Uuid::new_v4())
        .bind(&execution_id)
        .bind(&workflow_id)
        .bind(NAMESPACE)
        .bind(status)
        .bind(organization)
        .bind(age as f64)
        .execute(&pool)
        .await
        .expect("seed a run");
        runs.push(execution_id);
    }
    let child = Uuid::new_v4().to_string();
    sqlx::query(
        r#"INSERT INTO workflow_executions
             (id, execution_id, workflow_id, workflow_name, workflow_type, workflow_version,
              namespace, task_queue, status, trigger_type, organization_id, parent_execution_id,
              root_execution_id, submitted_at, created_at, updated_at)
           VALUES ($1, $2, $3, 'orders', 'orders', '1.0', $4, 'q', 'running', 'workflow', $5, $6,
                   $6, NOW(), NOW(), NOW())"#,
    )
    .bind(Uuid::new_v4())
    .bind(&child)
    .bind(format!("child-of-{workflow_id}"))
    .bind(NAMESPACE)
    .bind(organization)
    .bind(&runs[0])
    .execute(&pool)
    .await
    .expect("seed a child");

    // V067 rebuilds the index V059 builds, so it is applied again after it:
    // the index the rest of the suite meets is the current one.
    sqlx::query("DELETE FROM _sqlx_migrations WHERE version IN (59, 67)")
        .execute(&pool)
        .await
        .expect("forget V059");
    orcher_storage::setup::run_migrations(&pool)
        .await
        .expect("V059 applies over existing duplicates");

    let holds = |execution_id: String| {
        let pool = pool.clone();
        async move {
            sqlx::query_scalar::<_, bool>(
                "SELECT holds_workflow_id FROM workflow_executions WHERE execution_id = $1",
            )
            .bind(execution_id)
            .fetch_one(&pool)
            .await
            .expect("read the hold")
        }
    };
    assert!(!holds(runs[0].clone()).await, "the older duplicate holds");
    assert!(
        holds(runs[1].clone()).await,
        "the newest open run does not hold"
    );
    assert!(!holds(runs[2].clone()).await, "a closed run holds");
    assert!(!holds(child.clone()).await, "a child holds");
    assert_eq!(
        status_of(&pool, &runs[0]).await,
        "running",
        "a duplicate was stopped"
    );

    let valid: bool = sqlx::query_scalar(
        "SELECT i.indisvalid FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid \
         WHERE c.relname = 'uq_workflow_executions_open_workflow_id'",
    )
    .fetch_one(&pool)
    .await
    .expect("the index exists");
    assert!(valid);

    // And from here the newest run is what a start meets.
    let refused = start(&service(&pool), start_as(organization, &workflow_id))
        .await
        .expect_err("a start of the migrated id");
    assert_eq!(
        execution_in_the_way(&refused).as_deref(),
        Some(runs[1].as_str())
    );

    let _ = sqlx::query("DELETE FROM workflow_executions WHERE execution_id = $1")
        .bind(&child)
        .execute(&pool)
        .await;
    forget(&pool, &workflow_id).await;
}

/// Two event-with-starts of one id racing: both find nothing and both start
/// it, the hold lets one through, and the other delivers its event to the run
/// that won rather than failing or starting a second.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn racing_event_with_starts_share_one_run() {
    let pool = pool().await;
    let (_, executions) = services(&pool);
    let journal_repo = Arc::new(JournalRepository::new(pool.clone()));
    let exec_repo = Arc::new(WorkflowExecutionRepository::new(pool.clone()));
    let polling_manager = Arc::new(PollingManager::new(
        exec_repo.clone(),
        journal_repo.clone(),
        300,
    ));
    let service = Arc::new(
        WorkflowService::new(executions, exec_repo)
            .with_event_support(journal_repo, polling_manager),
    );
    let organization = organization(&pool).await;
    let workflow_id = format!("order-{}", Uuid::new_v4());

    let sends: Vec<_> = (0..8)
        .map(|i| {
            let service = service.clone();
            let workflow_id = workflow_id.clone();
            tokio::spawn(async move {
                let mut request = Request::new(SendEventWithStartWorkflowRequest {
                    workflow_id,
                    namespace: NAMESPACE.to_string(),
                    event_name: "approve".to_string(),
                    event_payload: br#"{"approved":true}"#.to_vec(),
                    workflow_type: "orders".to_string(),
                    task_queue: "q".to_string(),
                    request_id: format!("send-{i}"),
                    ..Default::default()
                });
                request.metadata_mut().insert(
                    ORGANIZATION_METADATA_KEY,
                    organization.to_string().parse().unwrap(),
                );
                service.send_event_with_start_workflow(request).await
            })
        })
        .collect();

    let mut runs = std::collections::HashSet::new();
    let mut started = 0;
    for send in sends {
        let response = send
            .await
            .expect("send task")
            .expect("an event-with-start of an id another is starting")
            .into_inner();
        started += usize::from(response.workflow_started);
        runs.insert(response.execution_id);
    }
    assert_eq!(
        started, 1,
        "more than one event-with-start started the workflow"
    );
    assert_eq!(runs.len(), 1, "the events went to different runs: {runs:?}");
    assert_eq!(executions_of(&pool, organization, &workflow_id).await, 1);

    forget(&pool, &workflow_id).await;
}
