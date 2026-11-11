//! A run is bound to a step-id scheme and a history lineage at its first
//! activation, handed only to workers that support its scheme, and keeps both
//! across a reset.
//!
//! A run's history is keyed by step ids. The kernel assigns them the same way
//! in every SDK, but a worker from before the kernel assigned them used its
//! SDK's own, and a history written under one scheme is not found under
//! another. So the scheme is bound once, by the first worker to report its
//! schemes, and routing refuses rather than falls through: every claim path
//! hands a bound run only to a worker supporting its scheme, and a worker
//! reporting none only runs nothing is bound to.
//!
//! Requires a live database:
//!   DATABASE_URL=postgres://…/orcher cargo test -p orcher-core --test step_id_binding

use std::sync::Arc;
use std::time::Duration;

use orcher_common::types::dispatch::polling::PollExecutionRequest;
use orcher_core::engine::dispatch::PollingManager;
use orcher_core::engine::queues::outbox::OutboxQueue;
use orcher_core::events::EventBus;
use orcher_core::grpc::services::WorkflowService;
use orcher_core::workflows::{JournalService, WorkflowExecutionService};
use orcher_events::bus::EventBusConfig;
use orcher_proto::orcher::v1::workflow_service_server::WorkflowService as WorkflowServiceTrait;
use orcher_proto::orcher::v1::{ResetWorkflowRequest, StartWorkflowRequest, StepIdScheme};
use orcher_storage::repositories::domains::{
    coordination::OutboxRepository, journal::JournalRepository,
    workflow_execution::WorkflowExecutionRepository,
};
use orcher_storage::schema::types::execution::WorkflowExecution;
use sqlx::PgPool;
use tonic::Request;
use uuid::Uuid;

const NAMESPACE: &str = "default";
const KERNEL: i16 = StepIdScheme::Kernel as i16;
const RUST_V0: i16 = StepIdScheme::RustV0 as i16;
const PYTHON_V0: i16 = StepIdScheme::PythonV0 as i16;

async fn pool() -> PgPool {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/orcher".to_string());
    let pool = PgPool::connect(&url)
        .await
        .expect("live database required; set DATABASE_URL");
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

/// A claimable run on `queue`, which each test has to itself.
async fn seed(pool: &PgPool, queue: &str) -> Uuid {
    let id = Uuid::new_v4();
    sqlx::query(
        r#"INSERT INTO workflow_executions
             (id, execution_id, workflow_id, workflow_name, workflow_version, namespace,
              task_queue, status, trigger_type, submitted_at, created_at, updated_at)
           VALUES ($1, $2, $1, 'step-id-test', '1.0.0', 'default', $3, 'pending', 'api',
                   NOW(), NOW(), NOW())"#,
    )
    .bind(id)
    .bind(id.to_string())
    .bind(queue)
    .execute(pool)
    .await
    .expect("seed workflow");
    id
}

/// The scheme, lineage and whether a worker reporting none activated `id`.
async fn binding(pool: &PgPool, id: Uuid) -> (Option<i16>, Option<String>, bool) {
    sqlx::query_as(
        "SELECT step_id_scheme, history_lineage, activated_without_scheme \
         FROM workflow_executions WHERE id = $1",
    )
    .bind(id)
    .fetch_one(pool)
    .await
    .expect("read binding")
}

/// Let the run be claimed again, as when an activation ends with the run
/// still going.
async fn release(pool: &PgPool, id: Uuid) {
    sqlx::query(
        "UPDATE workflow_executions SET claimed_by = NULL, claimed_at = NULL, \
         claim_timeout_at = NULL, status = 'running' WHERE id = $1",
    )
    .bind(id)
    .execute(pool)
    .await
    .expect("release claim");
}

async fn clean_up(pool: &PgPool, queue: &str) {
    sqlx::query("DELETE FROM workflow_executions WHERE task_queue = $1")
        .bind(queue)
        .execute(pool)
        .await
        .expect("clean up");
}

/// Claim the next run of `queue` for a worker supporting `schemes`.
async fn claim(
    repo: &WorkflowExecutionRepository,
    queue: &str,
    schemes: &[i16],
) -> Option<WorkflowExecution> {
    repo.claim_next_from(queue, NAMESPACE, "worker", 60, None, None, None, schemes)
        .await
        .expect("claim")
}

fn queue(name: &str) -> String {
    format!("step-ids-{name}-{}", Uuid::new_v4())
}

#[tokio::test]
async fn a_new_run_binds_the_kernel_scheme_and_its_own_lineage() {
    let pool = pool().await;
    let repo = WorkflowExecutionRepository::new(pool.clone());
    let queue = queue("kernel");
    let id = seed(&pool, &queue).await;

    let run = claim(&repo, &queue, &[KERNEL, RUST_V0])
        .await
        .expect("claimed");
    assert_eq!(run.step_id_scheme, Some(KERNEL));
    assert_eq!(run.lineage(), id.to_string());
    assert_eq!(
        binding(&pool, id).await,
        (Some(KERNEL), Some(id.to_string()), false)
    );

    // Never rebound: a later claim by a worker supporting more keeps it.
    release(&pool, id).await;
    claim(&repo, &queue, &[KERNEL, PYTHON_V0])
        .await
        .expect("claimed again");
    assert_eq!(binding(&pool, id).await.0, Some(KERNEL));

    clean_up(&pool, &queue).await;
}

#[tokio::test]
async fn a_bound_run_goes_only_to_a_worker_supporting_its_scheme() {
    let pool = pool().await;
    let repo = WorkflowExecutionRepository::new(pool.clone());
    let queue = queue("refuse");
    let id = seed(&pool, &queue).await;
    claim(&repo, &queue, &[KERNEL, RUST_V0])
        .await
        .expect("claimed");
    release(&pool, id).await;

    assert!(
        claim(&repo, &queue, &[PYTHON_V0]).await.is_none(),
        "a worker without the kernel scheme was handed a run bound to it"
    );
    assert!(
        claim(&repo, &queue, &[]).await.is_none(),
        "a worker reporting no scheme was handed a bound run"
    );
    assert!(claim(&repo, &queue, &[KERNEL]).await.is_some());

    clean_up(&pool, &queue).await;
}

#[tokio::test]
async fn a_run_first_activated_without_a_scheme_binds_the_sdk_scheme() {
    let pool = pool().await;
    let repo = WorkflowExecutionRepository::new(pool.clone());
    let queue = queue("legacy");
    let id = seed(&pool, &queue).await;

    // A worker from before schemes binds nothing, and says it was there.
    let run = claim(&repo, &queue, &[]).await.expect("claimed");
    assert_eq!(run.step_id_scheme, None);
    assert_eq!(binding(&pool, id).await.0, None);
    assert!(binding(&pool, id).await.2);
    release(&pool, id).await;

    // Its history may carry its SDK's ids, so a worker that can only use the
    // kernel's is refused it, and one with its SDK's own scheme binds that.
    assert!(
        claim(&repo, &queue, &[KERNEL]).await.is_none(),
        "a kernel-only worker was handed a run with SDK step ids"
    );
    let run = claim(&repo, &queue, &[KERNEL, PYTHON_V0])
        .await
        .expect("claimed by an upgraded worker");
    assert_eq!(run.step_id_scheme, Some(PYTHON_V0));

    clean_up(&pool, &queue).await;
}

#[tokio::test]
async fn the_batch_and_by_id_claims_route_and_bind_alike() {
    let pool = pool().await;
    let repo = WorkflowExecutionRepository::new(pool.clone());

    // Batch.
    let queue_a = queue("batch");
    let batch_run = seed(&pool, &queue_a).await;
    let claimed = repo
        .claim_batch_with_offset(
            &queue_a,
            NAMESPACE,
            "worker",
            60,
            10,
            0,
            None,
            None,
            &[KERNEL, RUST_V0],
        )
        .await
        .expect("batch claim");
    assert_eq!(claimed.len(), 1);
    assert_eq!(
        binding(&pool, batch_run).await,
        (Some(KERNEL), Some(batch_run.to_string()), false)
    );
    release(&pool, batch_run).await;
    let refused = repo
        .claim_batch_with_offset(
            &queue_a,
            NAMESPACE,
            "worker",
            60,
            10,
            0,
            None,
            None,
            &[PYTHON_V0],
        )
        .await
        .expect("batch claim");
    assert!(refused.is_empty(), "the batch claim ignored the binding");

    // By id.
    let queue_b = queue("by-id");
    let by_id_run = seed(&pool, &queue_b).await;
    let execution_id = by_id_run.to_string();
    repo.claim_execution_by_id_from(
        &execution_id,
        "worker",
        60,
        None,
        None,
        None,
        &[KERNEL, RUST_V0],
    )
    .await
    .expect("claim by id")
    .expect("claimed");
    assert_eq!(binding(&pool, by_id_run).await.0, Some(KERNEL));
    release(&pool, by_id_run).await;
    let refused = repo
        .claim_execution_by_id_from(&execution_id, "worker", 60, None, None, None, &[])
        .await
        .expect("claim by id");
    assert!(refused.is_none(), "the by-id claim ignored the binding");

    clean_up(&pool, &queue_a).await;
    clean_up(&pool, &queue_b).await;
}

fn poll(queue: &str, schemes: &[i32]) -> PollExecutionRequest {
    PollExecutionRequest::new(queue, NAMESPACE, "worker", Duration::from_millis(300))
        .with_step_ids(schemes)
}

#[tokio::test]
async fn a_prefetched_run_is_handed_only_to_polls_supporting_its_scheme() {
    let pool = pool().await;
    let queue = queue("prefetch");
    seed(&pool, &queue).await;
    seed(&pool, &queue).await;
    let manager = PollingManager::new(
        Arc::new(WorkflowExecutionRepository::new(pool.clone())),
        Arc::new(JournalRepository::new(pool.clone())),
        300,
    )
    .with_prefetch(orcher_dispatch::polling::PrefetchConfig::default());
    let kernel_poll = [StepIdScheme::Kernel as i32, StepIdScheme::RustV0 as i32];

    // The first poll claims both in one batch, bound to the kernel scheme,
    // and leaves one in the buffer.
    let first = manager
        .poll_execution(poll(&queue, &kernel_poll))
        .await
        .expect("poll")
        .expect("a run");
    assert_eq!(first.step_id_scheme, Some(KERNEL));
    assert!(!first.history_lineage.is_empty());

    let handed = manager
        .poll_execution(poll(&queue, &[StepIdScheme::PythonV0 as i32]))
        .await
        .expect("poll");
    assert!(
        handed.is_none(),
        "a poll without the kernel scheme was handed {:?}, bound to it",
        handed.map(|r| r.execution_id)
    );

    // The same set in another order is the same buffer.
    let second = manager
        .poll_execution(poll(&queue, &[kernel_poll[1], kernel_poll[0]]))
        .await
        .expect("poll")
        .expect("the buffered run");
    assert_ne!(first.execution_id, second.execution_id);

    clean_up(&pool, &queue).await;
}

#[tokio::test]
async fn a_reset_keeps_the_scheme_and_the_lineage() {
    let pool = pool().await;
    let queue = queue("reset");
    let executions = Arc::new(WorkflowExecutionRepository::new(pool.clone()));
    let journal = Arc::new(JournalService::new(Arc::new(JournalRepository::new(
        pool.clone(),
    ))));
    let outbox = Arc::new(OutboxQueue::new(Arc::new(OutboxRepository::new(Arc::new(
        pool.clone(),
    )))));
    let journal_repo = Arc::new(JournalRepository::new(pool.clone()));
    let polling = Arc::new(PollingManager::new(
        executions.clone(),
        journal_repo.clone(),
        300,
    ));
    let workflows = WorkflowService::new(
        Arc::new(WorkflowExecutionService::new(
            executions.clone(),
            journal,
            outbox,
            EventBus::new(EventBusConfig::default()),
            pool.clone(),
        )),
        executions.clone(),
    )
    .with_event_support(journal_repo, polling);

    let started = workflows
        .start_workflow(Request::new(StartWorkflowRequest {
            workflow_id: format!("wf-{}", Uuid::new_v4()),
            workflow_type: "orders".to_string(),
            task_queue: queue.clone(),
            namespace: NAMESPACE.to_string(),
            ..Default::default()
        }))
        .await
        .expect("start")
        .into_inner();
    let run = claim(&executions, &queue, &[KERNEL, RUST_V0])
        .await
        .expect("claimed");
    assert_eq!(run.execution_id, started.execution_id);

    let reset = workflows
        .reset_workflow(Request::new(ResetWorkflowRequest {
            execution_id: run.execution_id.clone(),
            namespace: NAMESPACE.to_string(),
            target_event_id: 1,
            ..Default::default()
        }))
        .await
        .expect("reset")
        .into_inner();
    let successor = executions
        .find_by_execution_id(&reset.new_execution_id)
        .await
        .expect("read the reset run")
        .expect("the reset run exists");
    assert_ne!(successor.id, run.id);
    assert_eq!(successor.step_id_scheme, Some(KERNEL));
    assert_eq!(successor.history_lineage, Some(run.lineage()));

    // And it is routed by the scheme it kept.
    assert!(claim(&executions, &queue, &[RUST_V0]).await.is_none());
    let claimed = claim(&executions, &queue, &[KERNEL])
        .await
        .expect("claimed by a kernel worker");
    assert_eq!(claimed.execution_id, successor.execution_id);
    assert_eq!(claimed.lineage(), run.lineage());

    clean_up(&pool, &queue).await;
}
