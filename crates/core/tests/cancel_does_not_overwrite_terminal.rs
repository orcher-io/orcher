//! Can a cancellation overwrite a workflow that already finished?
//!
//! The gRPC handler reads the execution, refuses if it is already terminal, and
//! then writes. That read is necessary for a good error message but cannot be
//! the guard: a workflow completing between the read and the write would have
//! its result overwritten by the cancellation, and a client that already
//! received a result would then see the run as cancelled.
//!
//! `cancel` decides it inside the UPDATE, so the window does not exist. These
//! tests pin that, including the reverse direction — cancel must still work on
//! a live execution, or this is just breakage.
//!
//! Requires a live database:
//!   DATABASE_URL=postgres://…/orcher cargo test -p orcher-core --test cancel_does_not_overwrite_terminal

use std::sync::Arc;

use orcher_storage::repositories::domains::workflow_execution::WorkflowExecutionRepository;
use sqlx::PgPool;
use uuid::Uuid;

async fn pool() -> Arc<PgPool> {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/orcher".to_string());
    Arc::new(
        PgPool::connect(&url)
            .await
            .expect("live database required; set DATABASE_URL"),
    )
}

async fn seed(pool: &PgPool, status: &str) -> (Uuid, String) {
    let id = Uuid::new_v4();
    let execution_id = format!("cancel-test-{id}");
    sqlx::query(
        r#"INSERT INTO workflow_executions
             (id, execution_id, workflow_id, workflow_name, workflow_type, workflow_version,
              namespace, task_queue, status, trigger_type, submitted_at, created_at, updated_at)
           VALUES ($1, $2, $1, 'cancel-test', 'cancel-test', '1.0.0', 'default', 'cancel-q',
                   $3, 'api', NOW(), NOW(), NOW())"#,
    )
    .bind(id)
    .bind(&execution_id)
    .bind(status)
    .execute(pool)
    .await
    .expect("seed workflow");
    (id, execution_id)
}

async fn status_of(pool: &PgPool, id: Uuid) -> String {
    sqlx::query_scalar::<_, String>("SELECT status FROM workflow_executions WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("read status")
}

async fn cleanup(pool: &PgPool, id: Uuid) {
    let _ = sqlx::query("DELETE FROM workflow_executions WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await;
}

/// The case that matters: the workflow finished, and a cancellation arrives.
#[tokio::test]
async fn cancelling_a_completed_execution_does_not_overwrite_it() {
    let pool = pool().await;
    let (id, execution_id) = seed(&pool, "completed").await;
    let repo = WorkflowExecutionRepository::new((*pool).clone());

    let cancelled = repo.cancel(&execution_id).await.expect("cancel call");

    assert!(
        !cancelled,
        "cancel reported success against a completed execution"
    );
    assert_eq!(
        status_of(&pool, id).await,
        "completed",
        "a completed workflow was overwritten to cancelled — a client holding its \
         result would now see the run as cancelled"
    );

    cleanup(&pool, id).await;
}

/// Same for the other terminal states, so the guard is not accidentally
/// specific to `completed`.
#[tokio::test]
async fn cancelling_a_failed_or_cancelled_execution_is_refused() {
    let pool = pool().await;
    let repo = WorkflowExecutionRepository::new((*pool).clone());

    for terminal in ["failed", "cancelled"] {
        let (id, execution_id) = seed(&pool, terminal).await;
        let cancelled = repo.cancel(&execution_id).await.expect("cancel call");
        assert!(!cancelled, "cancel reported success against '{terminal}'");
        assert_eq!(status_of(&pool, id).await, terminal);
        cleanup(&pool, id).await;
    }
}

/// The positive control. A guard that refused everything would pass the tests
/// above while making cancellation useless.
#[tokio::test]
async fn cancelling_a_running_execution_succeeds() {
    let pool = pool().await;
    let (id, execution_id) = seed(&pool, "running").await;
    let repo = WorkflowExecutionRepository::new((*pool).clone());

    let cancelled = repo.cancel(&execution_id).await.expect("cancel call");

    assert!(cancelled, "a running execution could not be cancelled");
    assert_eq!(status_of(&pool, id).await, "cancelled");

    cleanup(&pool, id).await;
}

/// Cancel records `cancelled_at`. The previous status-only write left it NULL,
/// so user-cancelled and batch-cancelled executions disagreed on the same field.
#[tokio::test]
async fn cancelling_records_when_it_happened() {
    let pool = pool().await;
    let (id, execution_id) = seed(&pool, "running").await;
    let repo = WorkflowExecutionRepository::new((*pool).clone());

    repo.cancel(&execution_id).await.expect("cancel");

    let cancelled_at: Option<chrono::DateTime<chrono::Utc>> =
        sqlx::query_scalar("SELECT cancelled_at FROM workflow_executions WHERE id = $1")
            .bind(id)
            .fetch_one(&*pool)
            .await
            .expect("read cancelled_at");

    assert!(
        cancelled_at.is_some(),
        "cancelled_at was left NULL, so nothing records when the cancellation happened"
    );

    cleanup(&pool, id).await;
}

/// Cancelling something that does not exist is not a success.
#[tokio::test]
async fn cancelling_a_missing_execution_reports_no_cancellation() {
    let pool = pool().await;
    let repo = WorkflowExecutionRepository::new((*pool).clone());

    let cancelled = repo
        .cancel(&format!("does-not-exist-{}", Uuid::new_v4()))
        .await
        .expect("cancel call");

    assert!(
        !cancelled,
        "cancel reported success for a missing execution"
    );
}

/// Why the handler had to stop using `update_status`.
///
/// The tests above pass against the old code too — `cancel` always had the
/// terminal-state predicate. The defect was that the gRPC cancel path did not
/// call it, and used `update_status`, which writes the status unconditionally.
/// This pins that difference so the handler is not "simplified" back later.
#[tokio::test]
async fn the_unconditional_status_write_would_overwrite_a_completed_run() {
    let pool = pool().await;
    let (id, execution_id) = seed(&pool, "completed").await;
    let repo = WorkflowExecutionRepository::new((*pool).clone());

    // Exactly what the cancel handler used to do after its check.
    repo.update_status(&execution_id, "cancelled", None, None)
        .await
        .expect("update_status");

    assert_eq!(
        status_of(&pool, id).await,
        "cancelled",
        "update_status is expected to overwrite unconditionally — if this ever \
         fails, the guard moved into update_status and the cancel handler can be \
         reconsidered"
    );

    cleanup(&pool, id).await;
}
