//! A reset replaces its run in one transaction, or not at all.
//!
//! It used to end the old run, copy its journal, create the new run and
//! journal the reset as separate writes, so a failure after the first left the
//! old run reset with no run to replace it, its workflow id released, and a
//! second reset of the same run could make a second successor.
//!
//! Requires PostgreSQL. Run with:
//!
//! ```bash
//! export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/orcher_test"
//! cargo test -p orcher-core --test reset_atomicity -- --ignored --test-threads=1
//! ```

use std::sync::Arc;

use orcher_core::engine::dispatch::PollingManager;
use orcher_core::engine::queues::outbox::OutboxQueue;
use orcher_core::events::EventBus;
use orcher_core::grpc::services::WorkflowService;
use orcher_core::workflows::{JournalService, WorkflowExecutionService};
use orcher_events::bus::EventBusConfig;
use orcher_proto::orcher::v1::workflow_service_server::WorkflowService as WorkflowServiceTrait;
use orcher_proto::orcher::v1::{ResetWorkflowRequest, StartWorkflowRequest};
use orcher_storage::repositories::domains::{
    coordination::OutboxRepository, journal::JournalRepository,
    workflow_execution::WorkflowExecutionRepository,
};
use sqlx::PgPool;
use tonic::{Code, Request};
use uuid::Uuid;

async fn pool() -> PgPool {
    let url = std::env::var("TEST_DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:postgres@localhost:5432/orcher_test".to_string()
    });
    PgPool::connect(&url)
        .await
        .expect("connect to the test database")
}

fn workflow_service(pool: &PgPool) -> WorkflowService {
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

/// Start a run of a fresh workflow id; returns its workflow id and run id.
async fn start(workflows: &WorkflowService) -> (String, String) {
    let workflow_id = format!("reset-{}", Uuid::new_v4());
    let started = workflows
        .start_workflow(Request::new(StartWorkflowRequest {
            workflow_id: workflow_id.clone(),
            workflow_type: "orders".to_string(),
            task_queue: format!("reset-{}", Uuid::new_v4().simple()),
            namespace: "default".to_string(),
            ..Default::default()
        }))
        .await
        .expect("start the workflow")
        .into_inner();
    (workflow_id, started.execution_id)
}

fn reset_request(execution_id: &str) -> Request<ResetWorkflowRequest> {
    Request::new(ResetWorkflowRequest {
        execution_id: execution_id.to_string(),
        namespace: "default".to_string(),
        target_event_id: 1,
        reason: "test".to_string(),
        ..Default::default()
    })
}

async fn status_of(pool: &PgPool, execution_id: &str) -> String {
    sqlx::query_scalar("SELECT status FROM workflow_executions WHERE execution_id = $1")
        .bind(execution_id)
        .fetch_one(pool)
        .await
        .expect("read the run")
}

/// The runs that replaced `execution_id`.
async fn successors(pool: &PgPool, execution_id: &str) -> Vec<String> {
    sqlx::query_scalar(
        "SELECT execution_id FROM workflow_executions WHERE parent_execution_id = $1",
    )
    .bind(execution_id)
    .fetch_all(pool)
    .await
    .expect("read the successors")
}

async fn reset_entries(pool: &PgPool, execution_id: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM workflow_execution_journal \
         WHERE execution_id = $1 AND event_type = 'WorkflowExecutionReset'",
    )
    .bind(execution_id)
    .fetch_one(pool)
    .await
    .expect("read the journal")
}

async fn forget(pool: &PgPool, workflow_id: &str) {
    let ids: Vec<Uuid> =
        sqlx::query_scalar("SELECT id FROM workflow_executions WHERE workflow_id = $1")
            .bind(workflow_id)
            .fetch_all(pool)
            .await
            .expect("read the runs");
    for id in ids {
        for sql in [
            "DELETE FROM workflow_execution_journal WHERE workflow_id = $1::text",
            "DELETE FROM outbox_tasks WHERE workflow_id = $1",
            "DELETE FROM workflow_executions WHERE id = $1",
        ] {
            let _ = sqlx::query(sql).bind(id).execute(pool).await;
        }
    }
}

/// A reset that fails after it began leaves the old run as it was: still
/// running, holding its workflow id, with no reset journaled and no run
/// replacing it. Creating the new run is made to fail, after the old run's
/// status and the journal copy were written in the same transaction.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_reset_that_fails_part_way_leaves_the_run_as_it_was() {
    let pool = pool().await;
    let workflows = workflow_service(&pool);
    let (workflow_id, execution_id) = start(&workflows).await;

    // Refuse the successor's row, and only this test's.
    let function = format!("refuse_reset_{}", Uuid::new_v4().simple());
    sqlx::query(&format!(
        "CREATE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql AS $$ \
         BEGIN IF NEW.parent_execution_id = '{execution_id}' THEN \
         RAISE EXCEPTION 'refused by the test'; END IF; RETURN NEW; END $$"
    ))
    .execute(&pool)
    .await
    .expect("create the refusing function");
    sqlx::query(&format!(
        "CREATE TRIGGER {function} BEFORE INSERT ON workflow_executions \
         FOR EACH ROW EXECUTE FUNCTION {function}()"
    ))
    .execute(&pool)
    .await
    .expect("create the refusing trigger");

    let refused = workflows.reset_workflow(reset_request(&execution_id)).await;

    sqlx::query(&format!("DROP TRIGGER {function} ON workflow_executions"))
        .execute(&pool)
        .await
        .expect("drop the trigger");
    sqlx::query(&format!("DROP FUNCTION {function}()"))
        .execute(&pool)
        .await
        .expect("drop the function");

    assert!(refused.is_err(), "the reset reported success");
    assert_eq!(
        status_of(&pool, &execution_id).await,
        "running",
        "the run was ended with nothing to replace it"
    );
    assert_eq!(reset_entries(&pool, &execution_id).await, 0);
    assert!(successors(&pool, &execution_id).await.is_empty());

    // Nothing was left behind to trip a second try.
    let reset = workflows
        .reset_workflow(reset_request(&execution_id))
        .await
        .expect("the reset, tried again")
        .into_inner();
    assert_eq!(status_of(&pool, &execution_id).await, "reset");
    assert_eq!(
        successors(&pool, &execution_id).await,
        vec![reset.new_execution_id.clone()]
    );
    assert_eq!(status_of(&pool, &reset.new_execution_id).await, "running");
    assert_eq!(reset_entries(&pool, &execution_id).await, 1);

    forget(&pool, &workflow_id).await;
}

/// A run is replaced once: resetting it again is refused, and of two resets
/// racing for it one replaces it and the other is refused, where both used to
/// make a successor. A run that holds its workflow id was kept to one by the
/// hold; one that does not, by nothing.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_run_is_replaced_by_one_reset() {
    let pool = pool().await;
    let workflows = workflow_service(&pool);

    for holds in [true, false] {
        let (workflow_id, execution_id) = start_holding(&workflows, &pool, holds).await;
        let first = workflows
            .reset_workflow(reset_request(&execution_id))
            .await
            .expect("the first reset");
        let again = workflows
            .reset_workflow(reset_request(&execution_id))
            .await
            .expect_err("a second reset of the replaced run");
        assert_eq!(
            again.code(),
            Code::FailedPrecondition,
            "holds {holds}: {again}"
        );
        assert_eq!(
            successors(&pool, &execution_id).await,
            vec![first.into_inner().new_execution_id],
            "holds {holds}"
        );
        forget(&pool, &workflow_id).await;

        for _ in 0..5 {
            let (workflow_id, execution_id) = start_holding(&workflows, &pool, holds).await;
            let (a, b) = tokio::join!(
                workflows.reset_workflow(reset_request(&execution_id)),
                workflows.reset_workflow(reset_request(&execution_id)),
            );
            let replaced = [&a, &b].iter().filter(|r| r.is_ok()).count();
            assert_eq!(replaced, 1, "holds {holds}: {a:?} {b:?}");
            assert_eq!(successors(&pool, &execution_id).await.len(), 1);
            assert_eq!(reset_entries(&pool, &execution_id).await, 1);
            forget(&pool, &workflow_id).await;
        }
    }
}

/// [`start`], with the run holding its workflow id or not.
async fn start_holding(
    workflows: &WorkflowService,
    pool: &PgPool,
    holds: bool,
) -> (String, String) {
    let (workflow_id, execution_id) = start(workflows).await;
    sqlx::query("UPDATE workflow_executions SET holds_workflow_id = $2 WHERE execution_id = $1")
        .bind(&execution_id)
        .bind(holds)
        .execute(pool)
        .await
        .expect("set the hold");
    (workflow_id, execution_id)
}
