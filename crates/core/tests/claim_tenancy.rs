//! A worker is handed only its own organization's work.
//!
//! A namespace name is unique only within an organization, and the claims that
//! hand a workflow activation or a task to a worker matched by queue and
//! namespace name alone. So a worker of one organization was handed another's
//! execution in a namespace of the same name. Now that a completion is refused
//! outside the caller's organization, that worker could not even complete it:
//! the execution went round its claim timeout for good. Every claim is scoped
//! by the organization the poll was made for, by the same rule completions
//! use: a poll that names an organization gets only that organization's work,
//! and one that names none gets anyone's, as a single-tenant deployment needs.
//!
//! Requires PostgreSQL. The schema is created by the engine's own migration
//! runner. Run with:
//!
//! ```bash
//! export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/orcher_test"
//! cargo test -p orcher-core --test claim_tenancy -- --ignored --test-threads=1
//! ```

use std::sync::Arc;
use std::time::Duration;

use orcher_core::engine::dispatch::{PollingManager, TaskPollingManager};
use orcher_core::engine::queues::outbox::OutboxQueue;
use orcher_core::engine::WorkflowCoordinator;
use orcher_core::events::EventBus;
use orcher_core::grpc::services::ExecutionService;
use orcher_core::grpc::tenant::ORGANIZATION_METADATA_KEY;
use orcher_core::workflows::{
    ChildWorkflowManager, CommandProcessor, JournalService, WorkflowExecutionService,
};
use orcher_events::bus::EventBusConfig;
use orcher_proto::orcher::v1::execution_service_server::ExecutionService as ExecutionServiceTrait;
use orcher_proto::orcher::v1::{
    command, Command, CommandType, CompleteWorkflowExecutionRequest, PollTaskExecutionRequest,
    PollWorkflowExecutionRequest, PollWorkflowExecutionResponse, ScheduleTaskCommandAttributes,
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

/// The namespace every organization here has one of, by the same name.
const NAMESPACE: &str = "default";

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

/// An engine replica on `pool`, and the polling manager its polls wait in;
/// with a prefetch buffer when `prefetch`, which is where a batch claimed for
/// one poll is handed to the next.
async fn engine(pool: &PgPool, prefetch: bool) -> (Arc<ExecutionService>, Arc<PollingManager>) {
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
    let mut polling_manager = PollingManager::new(exec_repo.clone(), journal_repo.clone(), 300);
    if prefetch {
        polling_manager =
            polling_manager.with_prefetch(orcher_dispatch::polling::PrefetchConfig::default());
    }
    let polling_manager = Arc::new(polling_manager);
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
        polling_manager.clone(),
        execution_log_repo as Arc<dyn ExecutionLogRepository>,
        journal_repo,
        exec_repo,
        task_execution_repo,
        pool.clone(),
    )
    .with_task_polling_manager(Arc::new(TaskPollingManager::new(outbox_queue)));
    (Arc::new(service), polling_manager)
}

/// A new organization, and the user that owns it.
async fn organization(pool: &PgPool) -> Uuid {
    let owner = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO users (id, email, password_hash, hash_algorithm, password_changed_at) \
         VALUES ($1, $2, 'x', 'argon2', NOW())",
    )
    .bind(owner)
    .bind(format!("owner-{owner}@claim-tenancy.test"))
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

/// A running workflow, unclaimed, on `task_queue`, owned by `organization`.
async fn workflow_of(pool: &PgPool, task_queue: &str, organization: Option<Uuid>) -> String {
    workflow_with_id(pool, task_queue, organization).await.1
}

/// [`workflow_of`], with the workflow's row id as well.
async fn workflow_with_id(
    pool: &PgPool,
    task_queue: &str,
    organization: Option<Uuid>,
) -> (Uuid, String) {
    let id = Uuid::new_v4();
    let execution_id = Uuid::new_v4().to_string();
    sqlx::query(
        r#"
        INSERT INTO workflow_executions (
            id, execution_id, workflow_id, workflow_name, workflow_version, namespace,
            task_queue, status, parameters, context, environment, progress,
            total_steps, completed_steps, failed_steps, retried_steps, organization_id
        )
        VALUES ($1, $2, $1, 'claim-tenancy', '1.0.0', $3, $4, 'running',
                '{}', '{}', '{}', '{}', 0, 0, 0, 0, $5)
        "#,
    )
    .bind(id)
    .bind(&execution_id)
    .bind(NAMESPACE)
    .bind(task_queue)
    .bind(organization)
    .execute(pool)
    .await
    .expect("insert workflow");
    (id, execution_id)
}

fn queue() -> String {
    format!("tenancy-{}", Uuid::new_v4().simple())
}

/// A workflow poll on `task_queue`, made for `organization` as the auth layer
/// leaves it once it has established who the caller acts for.
async fn poll_as(
    service: &ExecutionService,
    organization: Option<Uuid>,
    task_queue: &str,
    timeout_secs: i64,
) -> PollWorkflowExecutionResponse {
    let mut request = Request::new(PollWorkflowExecutionRequest {
        task_queue: task_queue.to_string(),
        namespace: NAMESPACE.to_string(),
        identity: format!("worker-{}", Uuid::new_v4().simple()),
        poll_timeout: Some(orcher_proto::prost_types::Duration {
            seconds: timeout_secs,
            nanos: 0,
        }),
        ..Default::default()
    });
    if let Some(organization) = organization {
        request.metadata_mut().insert(
            ORGANIZATION_METADATA_KEY,
            organization.to_string().parse().unwrap(),
        );
    }
    service
        .poll_workflow_execution(request)
        .await
        .expect("poll")
        .into_inner()
}

async fn claimed_by(pool: &PgPool, execution_id: &str) -> Option<String> {
    sqlx::query_scalar("SELECT claimed_by FROM workflow_executions WHERE execution_id = $1")
        .bind(execution_id)
        .fetch_one(pool)
        .await
        .expect("read the claim")
}

async fn cleanup(pool: &PgPool, task_queue: &str, organizations: &[Uuid]) {
    let _ = sqlx::query("DELETE FROM workflow_executions WHERE task_queue = $1")
        .bind(task_queue)
        .execute(pool)
        .await;
    for organization in organizations {
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
}

/// Two organizations each have a namespace called `default`. One's worker
/// polling it is never handed the other's execution; the owner's is.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_worker_is_not_handed_another_organizations_execution() {
    let pool = pool().await;
    let (service, _) = engine(&pool, false).await;
    let (a, b) = (organization(&pool).await, organization(&pool).await);
    let task_queue = queue();
    let execution_id = workflow_of(&pool, &task_queue, Some(a)).await;

    let polled = poll_as(&service, Some(b), &task_queue, 1).await;
    assert!(
        polled.execution_id.is_empty(),
        "organization b's worker was handed organization a's execution"
    );
    assert_eq!(claimed_by(&pool, &execution_id).await, None);

    let polled = poll_as(&service, Some(a), &task_queue, 2).await;
    assert_eq!(polled.execution_id, execution_id, "its owner's worker is");

    cleanup(&pool, &task_queue, &[a, b]).await;
}

/// An execution that belongs to no organization is not handed to a worker
/// acting for one, which could not complete it: completions are refused
/// outside the caller's organization, and an unowned row is refused to every
/// tenant. A worker acting for none is handed every organization's work, as a
/// single-tenant deployment needs.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn an_unowned_execution_goes_only_to_a_worker_acting_for_no_organization() {
    let pool = pool().await;
    let (service, _) = engine(&pool, false).await;
    let a = organization(&pool).await;
    let task_queue = queue();
    let unowned = workflow_of(&pool, &task_queue, None).await;

    let polled = poll_as(&service, Some(a), &task_queue, 1).await;
    assert!(
        polled.execution_id.is_empty(),
        "an organization's worker was handed an unowned execution"
    );

    let owned = workflow_of(&pool, &task_queue, Some(a)).await;
    let mut handed = vec![
        poll_as(&service, None, &task_queue, 2).await.execution_id,
        poll_as(&service, None, &task_queue, 2).await.execution_id,
    ];
    handed.sort();
    let mut expected = vec![unowned, owned];
    expected.sort();
    assert_eq!(handed, expected, "a worker acting for none gets both");

    cleanup(&pool, &task_queue, &[a]).await;
}

/// A batch claimed for one organization's poll is kept for that
/// organization's later polls, never handed from the buffer to another's.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_prefetched_batch_is_handed_only_within_its_organization() {
    let pool = pool().await;
    let (service, _) = engine(&pool, true).await;
    let (a, b) = (organization(&pool).await, organization(&pool).await);
    let task_queue = queue();
    let mut executions = Vec::new();
    for _ in 0..3 {
        executions.push(workflow_of(&pool, &task_queue, Some(a)).await);
    }

    let first = poll_as(&service, Some(a), &task_queue, 2).await;
    assert!(executions.contains(&first.execution_id), "a claims a batch");

    let polled = poll_as(&service, Some(b), &task_queue, 1).await;
    assert!(
        polled.execution_id.is_empty(),
        "organization b's worker was handed organization a's prefetched execution"
    );

    let second = poll_as(&service, Some(a), &task_queue, 2).await;
    assert!(
        executions.contains(&second.execution_id) && second.execution_id != first.execution_id,
        "the rest of the batch goes to a"
    );

    cleanup(&pool, &task_queue, &[a, b]).await;
}

/// A continuation names an execution but not whose it is. Another
/// organization's poll that pops it does not claim it, and its owner's poll
/// still finds it.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_continuation_is_claimed_only_within_its_organization() {
    let pool = pool().await;
    let (service, polling) = engine(&pool, false).await;
    let (a, b) = (organization(&pool).await, organization(&pool).await);
    let task_queue = queue();
    let execution_id = workflow_of(&pool, &task_queue, Some(a)).await;
    polling.push_continuation(&task_queue, NAMESPACE, &execution_id, None);

    let polled = poll_as(&service, Some(b), &task_queue, 1).await;
    assert!(
        polled.execution_id.is_empty(),
        "organization b's worker claimed organization a's continuation"
    );
    assert_eq!(claimed_by(&pool, &execution_id).await, None);
    // Put back for a's polls, and passed over by b's.
    assert_eq!(
        polling.try_pop_continuation_for(&task_queue, NAMESPACE, "b-worker", Some(b)),
        None,
        "b's polls pass it over"
    );
    assert_eq!(
        polling.try_pop_continuation_for(&task_queue, NAMESPACE, "a-worker", Some(a)),
        Some(execution_id.clone()),
        "the continuation b popped was put back for a"
    );

    let polled = poll_as(&service, Some(a), &task_queue, 2).await;
    assert_eq!(polled.execution_id, execution_id);

    cleanup(&pool, &task_queue, &[a, b]).await;
}

/// The matching layer's partitions are keyed by names too. A task is claimed
/// only for its own organization, or by a poll acting for none; a task that
/// belongs to no organization only by the latter.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_matching_partition_hands_a_task_only_within_its_organization() {
    let pool = pool().await;
    let store = PgMatchingStore::new(pool.clone());
    let (a, b) = (Uuid::new_v4(), Uuid::new_v4());
    let task_queue = queue();
    let lease = Duration::from_secs(60);
    let task = |organization_id| MatchTask {
        task_id: Uuid::new_v4(),
        namespace: NAMESPACE.to_string(),
        task_queue: task_queue.clone(),
        workflow_id: Uuid::new_v4(),
        payload: serde_json::json!({}),
        organization_id,
    };

    let owned = task(Some(a));
    store.enqueue(&owned).await.expect("enqueue");
    let claimed = store
        .claim(NAMESPACE, &task_queue, "b-worker", lease, Some(b))
        .await
        .expect("claim");
    assert!(claimed.is_none(), "b claimed a's task");
    let claimed = store
        .claim(NAMESPACE, &task_queue, "a-worker", lease, Some(a))
        .await
        .expect("claim")
        .expect("a claims its own");
    assert_eq!(claimed.task_id, owned.task_id);
    assert_eq!(claimed.organization_id, Some(a));

    let unowned = task(None);
    store.enqueue(&unowned).await.expect("enqueue");
    assert!(store
        .claim(NAMESPACE, &task_queue, "a-worker", lease, Some(a))
        .await
        .expect("claim")
        .is_none());
    let claimed = store
        .claim(NAMESPACE, &task_queue, "any-worker", lease, None)
        .await
        .expect("claim")
        .expect("a poll acting for none claims it");
    assert_eq!(claimed.task_id, unowned.task_id);

    let _ = sqlx::query("DELETE FROM matching_partitions WHERE task_queue = $1")
        .bind(&task_queue)
        .execute(&pool)
        .await;
}

/// A task a workflow schedules is stamped with the workflow's organization, so
/// that organization's worker is handed it and another's is not. The dispatch
/// used to carry no organization at all, which a worker acting for one never
/// claimed from the outbox.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_scheduled_task_goes_to_its_organizations_worker() {
    let pool = pool().await;
    let (service, _) = engine(&pool, false).await;
    let (a, b) = (organization(&pool).await, organization(&pool).await);
    let task_queue = queue();
    let execution_id = workflow_of(&pool, &task_queue, Some(a)).await;
    let handed = poll_as(&service, Some(a), &task_queue, 2).await;
    assert_eq!(handed.execution_id, execution_id);

    let mut complete = Request::new(CompleteWorkflowExecutionRequest {
        workflow_id: handed.workflow_id.clone(),
        execution_id: execution_id.clone(),
        namespace: NAMESPACE.to_string(),
        identity: "worker".to_string(),
        stream_entry_id: handed.stream_entry_id.clone(),
        commands: vec![Command {
            command_type: CommandType::ScheduleTask as i32,
            attributes: Some(command::Attributes::ScheduleTask(
                ScheduleTaskCommandAttributes {
                    task_id: "task_0".to_string(),
                    task_type: "Echo".to_string(),
                    task_queue: task_queue.clone(),
                    input: b"{}".to_vec(),
                    ..Default::default()
                },
            )),
        }],
        ..Default::default()
    });
    complete
        .metadata_mut()
        .insert(ORGANIZATION_METADATA_KEY, a.to_string().parse().unwrap());
    service
        .complete_workflow_execution(complete)
        .await
        .expect("the task is scheduled");

    let poll_task = |organization: Uuid, timeout_secs: i64| {
        let mut request = Request::new(PollTaskExecutionRequest {
            task_queue: task_queue.clone(),
            namespace: NAMESPACE.to_string(),
            identity: format!("task-worker-{}", Uuid::new_v4().simple()),
            poll_timeout: Some(orcher_proto::prost_types::Duration {
                seconds: timeout_secs,
                nanos: 0,
            }),
            ..Default::default()
        });
        request.metadata_mut().insert(
            ORGANIZATION_METADATA_KEY,
            organization.to_string().parse().unwrap(),
        );
        request
            .extensions_mut()
            .insert(orcher_core::grpc::auth_layer::TenantContext {
                organization_id: Some(organization),
            });
        request
    };
    let polled = service
        .poll_task_execution(poll_task(b, 1))
        .await
        .expect("poll")
        .into_inner();
    assert!(polled.task_id.is_empty(), "b's worker was handed a's task");
    let polled = service
        .poll_task_execution(poll_task(a, 3))
        .await
        .expect("poll")
        .into_inner();
    assert_eq!(polled.task_id, "task_0", "a's worker is handed its task");

    for q in [
        "DELETE FROM outbox_queue WHERE payload->>'task_queue' = $1",
        "DELETE FROM task_executions WHERE task_queue = $1",
    ] {
        let _ = sqlx::query(q).bind(&task_queue).execute(&pool).await;
    }
    cleanup(&pool, &task_queue, &[a, b]).await;
}

/// A dispatch written before dispatches were stamped carries no organization.
/// It goes to its workflow's organization, from the outbox and from a matching
/// partition alike, not to nobody and not to another organization.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn an_unstamped_dispatch_goes_to_its_workflows_organization() {
    let pool = pool().await;
    let (service, _) = engine(&pool, false).await;
    let (a, b) = (organization(&pool).await, organization(&pool).await);
    let task_queue = queue();
    let (workflow_id, execution_id) = workflow_with_id(&pool, &task_queue, Some(a)).await;
    // Claimed, so the workflow poll below does not interfere.
    sqlx::query(
        "UPDATE workflow_executions SET claimed_by = 'w', claimed_at = NOW() WHERE id = $1",
    )
    .bind(workflow_id)
    .execute(&pool)
    .await
    .expect("claim the workflow");
    sqlx::query(
        "INSERT INTO outbox_queue (workflow_id, task_type, payload) VALUES ($1, 'dispatch_task', $2)",
    )
    .bind(workflow_id)
    .bind(serde_json::json!({
        "workflow_id": workflow_id.to_string(),
        "execution_id": execution_id,
        "task_id": "legacy_task",
        "task_type": "Echo",
        "task_queue": task_queue,
        "namespace": NAMESPACE,
    }))
    .execute(&pool)
    .await
    .expect("insert an unstamped dispatch");

    let poll_task = |organization: Uuid, timeout_secs: i64| {
        let mut request = Request::new(PollTaskExecutionRequest {
            task_queue: task_queue.clone(),
            namespace: NAMESPACE.to_string(),
            identity: format!("task-worker-{}", Uuid::new_v4().simple()),
            poll_timeout: Some(orcher_proto::prost_types::Duration {
                seconds: timeout_secs,
                nanos: 0,
            }),
            ..Default::default()
        });
        request.metadata_mut().insert(
            ORGANIZATION_METADATA_KEY,
            organization.to_string().parse().unwrap(),
        );
        request
            .extensions_mut()
            .insert(orcher_core::grpc::auth_layer::TenantContext {
                organization_id: Some(organization),
            });
        request
    };
    let polled = service
        .poll_task_execution(poll_task(b, 1))
        .await
        .expect("poll")
        .into_inner();
    assert!(
        polled.task_id.is_empty(),
        "b's worker was handed a's dispatch"
    );
    let polled = service
        .poll_task_execution(poll_task(a, 3))
        .await
        .expect("poll")
        .into_inner();
    assert_eq!(polled.task_id, "legacy_task", "a's worker is handed it");

    // The same from a matching partition.
    let store = PgMatchingStore::new(pool.clone());
    let unstamped = MatchTask {
        task_id: Uuid::new_v4(),
        namespace: NAMESPACE.to_string(),
        task_queue: task_queue.clone(),
        workflow_id,
        payload: serde_json::json!({}),
        organization_id: None,
    };
    store.enqueue(&unstamped).await.expect("enqueue");
    let lease = Duration::from_secs(60);
    assert!(store
        .claim(NAMESPACE, &task_queue, "b-worker", lease, Some(b))
        .await
        .expect("claim")
        .is_none());
    let claimed = store
        .claim(NAMESPACE, &task_queue, "a-worker", lease, Some(a))
        .await
        .expect("claim")
        .expect("a claims it");
    assert_eq!(claimed.task_id, unstamped.task_id);

    for q in [
        "DELETE FROM outbox_queue WHERE workflow_id = $1",
        "DELETE FROM matching_partitions WHERE workflow_id = $1",
    ] {
        let _ = sqlx::query(q).bind(workflow_id).execute(&pool).await;
    }
    cleanup(&pool, &task_queue, &[a, b]).await;
}

/// A retry, and a repair of a dispatch never made, are dispatched for the
/// workflow's organization even when the attempt they come from carries none.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_retry_carries_its_workflows_organization() {
    let pool = pool().await;
    let a = organization(&pool).await;
    let task_queue = queue();
    let (workflow_id, execution_id) = workflow_with_id(&pool, &task_queue, Some(a)).await;
    let task_row = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO task_executions (
            id, task_id, task_type, workflow_id, execution_id, task_queue, namespace,
            attempt_number, status, scheduled_at
        )
        VALUES ($1, 'retried', 'Echo', $2, $3::uuid, $4, $5, 2, 'scheduled',
                '2000-01-01T00:00:00Z')
        "#,
    )
    .bind(task_row)
    .bind(workflow_id)
    .bind(&execution_id)
    .bind(&task_queue)
    .bind(NAMESPACE)
    .execute(&pool)
    .await
    .expect("insert an unstamped retry");

    let ready = orcher_core::retries::TaskRetryScheduler::new(pool.clone())
        .find_ready_retries()
        .await
        .expect("find ready retries");
    let retry = ready
        .iter()
        .find(|t| t.id.as_uuid() == task_row)
        .expect("the retry is ready");
    assert_eq!(retry.organization_id, Some(a));

    let _ = sqlx::query("DELETE FROM task_executions WHERE id = $1")
        .bind(task_row)
        .execute(&pool)
        .await;
    cleanup(&pool, &task_queue, &[a]).await;
}

/// V055 stamps a task, its dispatch and its delivery with the workflow's
/// organization when they were written without one, and leaves those of a
/// workflow that belongs to no organization as they are.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn the_backfill_stamps_rows_written_before_with_their_workflows_organization() {
    let pool = pool().await;
    let a = organization(&pool).await;
    let task_queue = queue();
    let mut rows = Vec::new();
    for owner in [Some(a), None] {
        let (workflow_id, execution_id) = workflow_with_id(&pool, &task_queue, owner).await;
        let task_row = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO task_executions (id, task_id, task_type, workflow_id, execution_id, \
             task_queue, namespace, attempt_number, status, scheduled_at) \
             VALUES ($1, 't', 'Echo', $2, $3::uuid, $4, $5, 1, 'scheduled', NOW())",
        )
        .bind(task_row)
        .bind(workflow_id)
        .bind(&execution_id)
        .bind(&task_queue)
        .bind(NAMESPACE)
        .execute(&pool)
        .await
        .expect("insert a task");
        sqlx::query(
            "INSERT INTO outbox_queue (workflow_id, task_type, payload) \
             VALUES ($1, 'dispatch_task', '{}')",
        )
        .bind(workflow_id)
        .execute(&pool)
        .await
        .expect("insert a dispatch");
        sqlx::query(
            "INSERT INTO matching_partitions (task_id, namespace, task_queue, workflow_id, payload) \
             VALUES ($1, $2, $3, $4, '{}')",
        )
        .bind(Uuid::new_v4())
        .bind(NAMESPACE)
        .bind(&task_queue)
        .bind(workflow_id)
        .execute(&pool)
        .await
        .expect("insert a delivery");
        rows.push((owner, workflow_id, task_row));
    }

    // Each block as the runner executes it: on its own, outside a transaction.
    let sql = orcher_storage::migrations::sql::postgres::V055_STAMP_TASK_ORGANIZATION;
    for block in sql.split("\nDO $$").skip(1) {
        sqlx::raw_sql(&format!("DO $${block}"))
            .execute(&pool)
            .await
            .expect("run a batch of the backfill");
    }

    for (owner, workflow_id, task_row) in &rows {
        let task: Option<Uuid> =
            sqlx::query_scalar("SELECT organization_id FROM task_executions WHERE id = $1")
                .bind(task_row)
                .fetch_one(&pool)
                .await
                .unwrap();
        let dispatch: Option<Uuid> =
            sqlx::query_scalar("SELECT organization_id FROM outbox_queue WHERE workflow_id = $1")
                .bind(workflow_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        let delivery: Option<Uuid> = sqlx::query_scalar(
            "SELECT organization_id FROM matching_partitions WHERE workflow_id = $1",
        )
        .bind(workflow_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!((task, dispatch, delivery), (*owner, *owner, *owner));
    }

    for (_, workflow_id, _) in &rows {
        for q in [
            "DELETE FROM task_executions WHERE workflow_id = $1",
            "DELETE FROM outbox_queue WHERE workflow_id = $1",
            "DELETE FROM matching_partitions WHERE workflow_id = $1",
        ] {
            let _ = sqlx::query(q).bind(workflow_id).execute(&pool).await;
        }
    }
    cleanup(&pool, &task_queue, &[a]).await;
}

/// A continuation put back for its own organization does not wait for it for
/// good: once polls of other organizations have passed it over often enough,
/// it is dropped, and its owner's poll finds the workflow by the scan instead.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_continuation_marked_for_another_organization_does_not_linger() {
    let pool = pool().await;
    let (service, polling) = engine(&pool, false).await;
    let (a, b) = (organization(&pool).await, organization(&pool).await);
    let task_queue = queue();
    let execution_id = workflow_of(&pool, &task_queue, Some(a)).await;
    polling.push_continuation(&task_queue, NAMESPACE, &execution_id, None);

    // b's poll pops it, cannot claim it, and puts it back marked for a.
    let polled = poll_as(&service, Some(b), &task_queue, 1).await;
    assert!(polled.execution_id.is_empty());
    for _ in 0..orcher_core::engine::dispatch::polling_manager::MARKED_CONTINUATION_SKIPS {
        assert_eq!(
            polling.try_pop_continuation_for(&task_queue, NAMESPACE, "b-worker", Some(b)),
            None
        );
    }
    assert_eq!(
        polling.try_pop_continuation_for(&task_queue, NAMESPACE, "a-worker", Some(a)),
        None,
        "passed over as often as it may be, the entry is gone"
    );
    let polled = poll_as(&service, Some(a), &task_queue, 2).await;
    assert_eq!(
        polled.execution_id, execution_id,
        "and a's poll still finds the workflow by the scan"
    );

    cleanup(&pool, &task_queue, &[a, b]).await;
}
