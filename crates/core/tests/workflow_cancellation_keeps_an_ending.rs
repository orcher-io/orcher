//! A workflow that ends itself as cancelled must not overwrite an ending the
//! run already has, and must be fenced like every other replay-driven write.
//!
//! The cancellation used to go through the generic status update, which has
//! no "still live" predicate: a late cancellation turned a completed or
//! terminated run into a cancelled one.
//!
//! Requires a live database:
//!   DATABASE_URL=postgres://…/orcher cargo test -p orcher-core --test workflow_cancellation_keeps_an_ending

use std::sync::Arc;

use orcher_storage::repositories::domains::workflow_execution::WorkflowExecutionRepository;
use orcher_storage::sharding::{FenceToken, WriteFence};
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Debug)]
struct PinnedFence {
    shard_id: i32,
    generation: i64,
}

#[async_trait::async_trait]
impl WriteFence for PinnedFence {
    async fn guard(&self, _workflow_id: Uuid) -> Option<FenceToken> {
        Some(FenceToken {
            shard_id: self.shard_id,
            generation: self.generation,
        })
    }
}

async fn pool() -> Arc<PgPool> {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/orcher".to_string());
    Arc::new(
        PgPool::connect(&url)
            .await
            .expect("live database required; set DATABASE_URL"),
    )
}

async fn seed(pool: &PgPool, status: &str) -> Uuid {
    let id = Uuid::new_v4();
    sqlx::query(
        r#"INSERT INTO workflow_executions
             (id, execution_id, workflow_id, workflow_name, workflow_version, namespace,
              task_queue, status, trigger_type, submitted_at, created_at, updated_at)
           VALUES ($1, $2, $1, 'cancel-test', '1.0.0', 'default', 'cancel-q', $3, 'api',
                   NOW(), NOW(), NOW())"#,
    )
    .bind(id)
    .bind(id.to_string())
    .bind(status)
    .execute(pool)
    .await
    .expect("seed workflow row");
    id
}

async fn row(pool: &PgPool, id: Uuid) -> (String, bool, bool) {
    sqlx::query_as::<_, (String, bool, bool)>(
        "SELECT status, cancelled_at IS NOT NULL, completed_at IS NOT NULL
           FROM workflow_executions WHERE id = $1",
    )
    .bind(id)
    .fetch_one(pool)
    .await
    .expect("read row")
}

async fn remove(pool: &PgPool, id: Uuid) {
    let _ = sqlx::query("DELETE FROM workflow_executions WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await;
}

#[tokio::test]
async fn a_running_workflow_is_recorded_as_cancelled() {
    let pool = pool().await;
    let id = seed(&pool, "running").await;
    let repo = WorkflowExecutionRepository::new((*pool).clone());

    let recorded = repo
        .cancel_workflow_atomic(&id.to_string(), jiff::Timestamp::now().into())
        .await
        .expect("cancel");

    assert!(recorded);
    let (status, cancelled_at, completed_at) = row(&pool, id).await;
    assert_eq!(status, "cancelled");
    assert!(
        cancelled_at,
        "cancelled_at is the timestamp a cancellation sets"
    );
    assert!(!completed_at, "a cancellation is not a completion");
    remove(&pool, id).await;
}

#[tokio::test]
async fn a_finished_run_keeps_its_ending() {
    let pool = pool().await;
    let repo = WorkflowExecutionRepository::new((*pool).clone());

    for ending in ["completed", "terminated", "timed_out", "cancelled"] {
        let id = seed(&pool, ending).await;
        let recorded = repo
            .cancel_workflow_atomic(&id.to_string(), jiff::Timestamp::now().into())
            .await
            .expect("cancel");
        assert!(!recorded, "a {ending} run was recorded as cancelled");
        assert_eq!(
            row(&pool, id).await.0,
            ending,
            "a {ending} run lost its ending"
        );
        remove(&pool, id).await;
    }
}

#[tokio::test]
async fn a_superseded_owner_cannot_cancel_a_workflow() {
    let pool = pool().await;
    let shard_id = 4094;
    let id = seed(&pool, "running").await;
    sqlx::query(
        r#"INSERT INTO cluster_shards (shard_id, owner_node, generation, updated_at)
           VALUES ($1, 'the-new-owner', 7, NOW())
           ON CONFLICT (shard_id) DO UPDATE
             SET owner_node = EXCLUDED.owner_node,
                 generation = EXCLUDED.generation,
                 updated_at = NOW()"#,
    )
    .bind(shard_id)
    .execute(pool.as_ref())
    .await
    .expect("take shard");

    let repo = WorkflowExecutionRepository::new((*pool).clone());
    repo.set_write_fence(Arc::new(PinnedFence {
        shard_id,
        generation: 6,
    }));

    let result = repo
        .cancel_workflow_atomic(&id.to_string(), jiff::Timestamp::now().into())
        .await;

    assert!(result.is_err(), "a stale generation cancelled the workflow");
    assert_eq!(row(&pool, id).await.0, "running");
    remove(&pool, id).await;
}
