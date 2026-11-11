//! Cancelling a workflow whose code has run is a request it can clean up
//! after, not a final status write.
//!
//! Requires a live database:
//!   DATABASE_URL=postgres://…/orcher cargo test -p orcher-core --test workflow_cancellation_requests

use std::sync::Arc;

use orcher_common::types::time::Timestamptz;
use orcher_core::workflows::JournalService;
use orcher_storage::repositories::domains::journal::JournalRepository;
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

/// A run in `status` that has been handed `activations` activations.
async fn seed(pool: &PgPool, status: &str, activations: i64) -> Uuid {
    let id = Uuid::new_v4();
    sqlx::query(
        r#"INSERT INTO workflow_executions
             (id, execution_id, workflow_id, workflow_name, workflow_version, namespace,
              task_queue, status, trigger_type, submitted_at, created_at, updated_at,
              activation_seq)
           VALUES ($1, $2, $1, 'cancel-request-test', '1.0.0', 'default', 'cancel-q', $3,
                   'api', NOW(), NOW(), NOW(), $4)"#,
    )
    .bind(id)
    .bind(id.to_string())
    .bind(status)
    .bind(activations)
    .execute(pool)
    .await
    .expect("seed workflow row");
    id
}

async fn remove(pool: &PgPool, id: Uuid) {
    let _ = sqlx::query("DELETE FROM workflow_execution_journal WHERE execution_id = $1")
        .bind(id.to_string())
        .execute(pool)
        .await;
    let _ = sqlx::query("DELETE FROM workflow_executions WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await;
}

async fn request_columns(
    pool: &PgPool,
    id: Uuid,
) -> (
    String,
    Option<Timestamptz>,
    Option<String>,
    Option<Timestamptz>,
) {
    sqlx::query_as(
        "SELECT status, cancel_requested_at, cancel_reason, cancel_deadline
           FROM workflow_executions WHERE id = $1",
    )
    .bind(id)
    .fetch_one(pool)
    .await
    .expect("read row")
}

#[tokio::test]
async fn a_running_workflow_is_asked_and_keeps_running() {
    let pool = pool().await;
    let id = seed(&pool, "running", 2).await;
    let repo = WorkflowExecutionRepository::new((*pool).clone());

    let target = repo
        .request_cancel(&id.to_string(), "customer changed their mind", None)
        .await
        .expect("request");

    let target = target.expect("a run whose code has run is asked, not ended");
    assert_eq!(target.workflow_id, id);
    let (status, requested, reason, deadline) = request_columns(&pool, id).await;
    assert_eq!(status, "running", "the workflow ends itself");
    assert!(requested.is_some());
    assert_eq!(reason.as_deref(), Some("customer changed their mind"));
    assert!(deadline.is_none(), "no limit unless one is given");
    assert_eq!(repo.pending_cancel_request(id).await.unwrap(), requested);
    remove(&pool, id).await;
}

#[tokio::test]
async fn asking_again_keeps_the_first_request() {
    let pool = pool().await;
    let id = seed(&pool, "running", 1).await;
    let repo = WorkflowExecutionRepository::new((*pool).clone());
    let first_deadline = Timestamptz::now() + jiff::SignedDuration::from_secs(60);

    repo.request_cancel(&id.to_string(), "first", Some(first_deadline))
        .await
        .unwrap()
        .expect("applies");
    let (_, first_at, _, _) = request_columns(&pool, id).await;

    let again = repo
        .request_cancel(&id.to_string(), "second", Some(Timestamptz::now()))
        .await
        .unwrap();

    assert!(again.is_some(), "asking again is harmless, not an error");
    let (_, at, reason, deadline) = request_columns(&pool, id).await;
    assert_eq!(at, first_at);
    assert_eq!(reason.as_deref(), Some("first"));
    assert_eq!(
        deadline.map(|d| d.timestamp_millis()),
        Some(first_deadline.timestamp_millis())
    );
    remove(&pool, id).await;
}

/// No activation handed out means no code ran, so nothing to clean up: the
/// caller ends it directly.
#[tokio::test]
async fn a_workflow_whose_code_never_ran_is_not_asked() {
    let pool = pool().await;
    let id = seed(&pool, "pending", 0).await;
    let repo = WorkflowExecutionRepository::new((*pool).clone());

    let target = repo
        .request_cancel(&id.to_string(), "r", None)
        .await
        .unwrap();

    assert!(target.is_none());
    assert!(request_columns(&pool, id).await.1.is_none());
    remove(&pool, id).await;
}

#[tokio::test]
async fn an_ended_workflow_is_not_asked() {
    let pool = pool().await;
    let repo = WorkflowExecutionRepository::new((*pool).clone());
    for ending in [
        "completed",
        "cancelled",
        "terminated",
        "timed_out",
        "failed",
    ] {
        let id = seed(&pool, ending, 3).await;
        let target = repo
            .request_cancel(&id.to_string(), "r", None)
            .await
            .unwrap();
        assert!(target.is_none(), "a {ending} run was asked to cancel");
        assert!(repo.pending_cancel_request(id).await.unwrap().is_none());
        remove(&pool, id).await;
    }
}

/// The workflow observes the request once, however often it is made.
#[tokio::test]
async fn the_request_is_journalled_once() {
    let pool = pool().await;
    let id = seed(&pool, "running", 1).await;
    let journal = JournalService::new(Arc::new(JournalRepository::new((*pool).clone())));

    let first = journal
        .append_workflow_cancel_requested(&id.to_string(), &id.to_string(), "r")
        .await
        .expect("append");
    let second = journal
        .append_workflow_cancel_requested(&id.to_string(), &id.to_string(), "r")
        .await
        .expect("append again");

    assert!(first);
    assert!(!second);
    let entries: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM workflow_execution_journal
          WHERE execution_id = $1 AND event_type = 'WorkflowExecutionCancelRequested'",
    )
    .bind(id.to_string())
    .fetch_one(pool.as_ref())
    .await
    .unwrap();
    assert_eq!(entries, 1);
    remove(&pool, id).await;
}

/// Whether a request is new decides whether the run is woken; asking again
/// must not wake it a second time.
#[tokio::test]
async fn only_the_first_request_is_new() {
    let pool = pool().await;
    let id = seed(&pool, "running", 1).await;
    let repo = WorkflowExecutionRepository::new((*pool).clone());

    let first = repo
        .request_cancel(&id.to_string(), "r", None)
        .await
        .unwrap()
        .expect("applies");
    let again = repo
        .request_cancel(&id.to_string(), "r", None)
        .await
        .unwrap()
        .expect("still applies");

    assert!(first.newly_requested);
    assert!(!again.newly_requested);
    remove(&pool, id).await;
}

/// A batch reaches every live match once: not one already asked, not one that
/// has ended, not another namespace's.
#[tokio::test]
async fn a_batch_selects_only_runs_it_has_not_reached() {
    let pool = pool().await;
    let namespace = format!("batch-cancel-{}", Uuid::new_v4());
    let seed_in = |status: &'static str, activations: i64, ns: String| {
        let pool = pool.clone();
        async move {
            let id = seed(&pool, status, activations).await;
            sqlx::query("UPDATE workflow_executions SET namespace = $2 WHERE id = $1")
                .bind(id)
                .bind(ns)
                .execute(pool.as_ref())
                .await
                .unwrap();
            id
        }
    };
    let started = seed_in("running", 1, namespace.clone()).await;
    let never_ran = seed_in("pending", 0, namespace.clone()).await;
    let asked = seed_in("running", 1, namespace.clone()).await;
    let ended = seed_in("completed", 3, namespace.clone()).await;
    let elsewhere = seed_in("running", 1, format!("{namespace}-other")).await;
    let repo = WorkflowExecutionRepository::new((*pool).clone());
    repo.request_cancel(&asked.to_string(), "r", None)
        .await
        .unwrap()
        .expect("applies");

    let mut selected = repo
        .select_for_batch_cancel(&namespace, None, None, None, None, None, None, 100)
        .await
        .unwrap();
    selected.sort();

    let mut expected = vec![started.to_string(), never_ran.to_string()];
    expected.sort();
    assert_eq!(selected, expected);
    for id in [started, never_ran, asked, ended, elsewhere] {
        remove(&pool, id).await;
    }
}
