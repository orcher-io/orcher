//! A run that fails with a long error is recorded as failed.
//!
//! A trigger once sent a failed run's error with pg_notify, whose payload
//! Postgres limits to 8000 bytes, so marking a run failed with a longer error
//! (a deep stack trace is one) was refused along with the trigger: the run
//! stayed running and its activation was retried for good. Both ways a run is
//! failed are checked here with an error well over that limit.
//!
//! Requires PostgreSQL. Run with:
//!
//! ```bash
//! export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/orcher_test"
//! cargo test -p orcher-storage --test long_failure_error -- --ignored --test-threads=1
//! ```

use orcher_common::types::time::Timestamptz;
use orcher_storage::repositories::domains::workflow_execution::WorkflowExecutionRepository;
use sqlx::PgPool;
use uuid::Uuid;

/// Well over pg_notify's 8000-byte payload limit.
const LONG_ERROR_BYTES: usize = 20_000;

async fn pool() -> PgPool {
    let url = std::env::var("TEST_DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:postgres@localhost:5432/orcher_test".to_string()
    });
    let pool = PgPool::connect(&url)
        .await
        .expect("connect to the test database");
    orcher_storage::setup::run_migrations(&pool)
        .await
        .expect("apply migrations");
    pool
}

/// A running run; its row id and execution id.
async fn running_run(pool: &PgPool) -> (Uuid, String) {
    let execution_id = format!("long-error-{}", Uuid::new_v4().simple());
    let id: Uuid = sqlx::query_scalar(
        "INSERT INTO workflow_executions \
         (execution_id, workflow_name, workflow_version, namespace, task_queue, status) \
         VALUES ($1, 'fails_loudly', '1.0', 'default', 'long-error', 'running') \
         RETURNING id",
    )
    .bind(&execution_id)
    .fetch_one(pool)
    .await
    .expect("insert a running run");
    (id, execution_id)
}

async fn status(pool: &PgPool, id: Uuid) -> String {
    sqlx::query_scalar("SELECT status FROM workflow_executions WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("read the run's status")
}

async fn remove(pool: &PgPool, id: Uuid) {
    let _ = sqlx::query("DELETE FROM workflow_executions WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await;
}

#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_failure_the_worker_reports_with_a_long_error_is_recorded() {
    let pool = pool().await;
    let repo = WorkflowExecutionRepository::new(pool.clone());
    let (id, execution_id) = running_run(&pool).await;

    let recorded = repo
        .record_failure(&execution_id, &"x".repeat(LONG_ERROR_BYTES), None)
        .await;
    let now = status(&pool, id).await;
    remove(&pool, id).await;

    assert!(
        recorded.expect("record the failure"),
        "the failure was not recorded"
    );
    assert_eq!(now, "failed");
}

#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_workflow_failed_with_a_long_error_is_marked_failed() {
    let pool = pool().await;
    let repo = WorkflowExecutionRepository::new(pool.clone());
    let (id, _) = running_run(&pool).await;

    let failed = repo
        .fail_workflow_atomic(
            &id.to_string(),
            "x".repeat(LONG_ERROR_BYTES),
            None,
            Timestamptz::now(),
        )
        .await;
    let now = status(&pool, id).await;
    remove(&pool, id).await;

    assert!(
        failed.expect("fail the workflow"),
        "the workflow was not failed"
    );
    assert_eq!(now, "failed");
}
