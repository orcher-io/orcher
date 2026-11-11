//! With clustering on, a workflow's retry is its owner's to make.
//!
//! Every node runs the retry scheduler. Taking a retry is a fenced write, so a
//! node that does not own the failed run is refused — which used to surface as
//! an error on every node but one, every poll. These pin the two halves of the
//! fix: each node looks only at failures on its own shards, and a refusal that
//! still happens (ownership moved between the scan and the take) reads as
//! "not mine", not as a failure.
//!
//! Requires PostgreSQL. Run with:
//!
//! ```bash
//! export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/orcher_test"
//! cargo test -p orcher-core --test retry_left_to_owner -- --ignored --test-threads=1
//! ```

use std::sync::Arc;

use orcher_core::workflows::{RetryOutcome, WorkflowRetryScheduler, WorkflowRetrySchedulerConfig};
use orcher_storage::repositories::domains::workflow_execution::WorkflowExecutionRepository;
use orcher_storage::sharding::{ClaimScope, FenceToken, WriteFence};
use sqlx::PgPool;
use uuid::Uuid;

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

/// A failed run, due for its retry, on `shard`.
async fn failed_run(pool: &PgPool, shard: i32) -> Uuid {
    let id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO workflow_executions (
            id, execution_id, workflow_id, workflow_name, workflow_version, namespace,
            task_queue, status, parameters, context, environment, progress,
            total_steps, completed_steps, failed_steps, retried_steps,
            retry_count, max_retries, next_retry_at, shard_id, error
        )
        VALUES ($1, $2, $1, 'retry-owner', '1.0.0', 'default', 'default', 'failed',
                '{}', '{}', '{}', '{}', 1, 0, 1, 0,
                0, 3, NOW() - INTERVAL '1 minute', $3, 'boom')
        "#,
    )
    .bind(id)
    .bind(Uuid::new_v4().to_string())
    .bind(shard)
    .execute(pool)
    .await
    .expect("insert failed run");
    id
}

async fn delete(pool: &PgPool, ids: &[Uuid]) {
    for id in ids {
        let _ = sqlx::query("DELETE FROM workflow_executions WHERE id = $1 OR workflow_id = $1")
            .bind(id)
            .execute(pool)
            .await;
    }
}

/// This node owns the lower half of the shard space.
#[derive(Debug)]
struct LowerHalf;
impl ClaimScope for LowerHalf {
    fn owned_ranges(&self) -> Vec<(i32, i32)> {
        vec![(0, 2047)]
    }
}

/// Ownership not yet known.
#[derive(Debug)]
struct Unknown;
impl ClaimScope for Unknown {
    fn owned_ranges(&self) -> Vec<(i32, i32)> {
        Vec::new()
    }
}

/// The fence's answer on a node that does not own the run.
#[derive(Debug)]
struct NotOwner;
#[async_trait::async_trait]
impl WriteFence for NotOwner {
    async fn guard(&self, _workflow_id: Uuid) -> Option<FenceToken> {
        None
    }
}

#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn each_node_looks_only_at_failures_on_its_own_shards() {
    let pool = pool().await;
    let mine = failed_run(&pool, 10).await;
    let theirs = failed_run(&pool, 3000).await;

    let repo = WorkflowExecutionRepository::new(pool.clone());
    repo.set_claim_scope(Arc::new(LowerHalf));
    let found: Vec<Uuid> = repo
        .find_due_for_retry(100_000)
        .await
        .expect("scan")
        .into_iter()
        .map(|run| run.id.as_uuid())
        .collect();

    assert!(
        found.contains(&mine),
        "a failure on this node's shard is its to retry"
    );
    assert!(
        !found.contains(&theirs),
        "a failure on another node's shard is that node's to retry"
    );

    delete(&pool, &[mine, theirs]).await;
}

#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn with_ownership_unknown_every_failure_is_looked_at() {
    let pool = pool().await;
    let a = failed_run(&pool, 10).await;
    let b = failed_run(&pool, 3000).await;

    let repo = WorkflowExecutionRepository::new(pool.clone());
    repo.set_claim_scope(Arc::new(Unknown));
    let found: Vec<Uuid> = repo
        .find_due_for_retry(100_000)
        .await
        .expect("scan")
        .into_iter()
        .map(|run| run.id.as_uuid())
        .collect();
    assert!(found.contains(&a) && found.contains(&b));

    delete(&pool, &[a, b]).await;
}

#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_retry_refused_for_ownership_is_not_mine_rather_than_an_error() {
    let pool = pool().await;
    let id = failed_run(&pool, 10).await;

    let repo = Arc::new(WorkflowExecutionRepository::new(pool.clone()));
    repo.set_write_fence(Arc::new(NotOwner));
    let run = repo
        .find_by_id(id)
        .await
        .expect("read")
        .expect("the run exists");
    let scheduler =
        WorkflowRetryScheduler::new(repo.clone(), WorkflowRetrySchedulerConfig::default());

    let outcome = scheduler
        .handle_workflow_failure(&run)
        .await
        .expect("a refusal for ownership is not an error");
    assert!(
        matches!(outcome, RetryOutcome::AlreadyHandled { .. }),
        "{outcome:?}"
    );
    let status: String = sqlx::query_scalar("SELECT status FROM workflow_executions WHERE id = $1")
        .bind(id)
        .fetch_one(&pool)
        .await
        .expect("status");
    assert_eq!(status, "failed", "left for the owner to retry");

    delete(&pool, &[id]).await;
}
