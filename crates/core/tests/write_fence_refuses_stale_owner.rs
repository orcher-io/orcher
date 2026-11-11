//! Does the write fence actually refuse?
//!
//! The fence exists for one failure that `FOR UPDATE SKIP LOCKED` cannot cover:
//! a node stalls, its claim is reassigned to a live node, and it then wakes up
//! and writes. Its transaction is new, nothing is locked, and without a
//! generation check the write lands and overwrites state the new owner produced.
//!
//! Confirming that shards get acquired proves the mechanism is *reached*. It
//! does not prove it *rejects* anything — a fence that always says yes looks
//! identical in a passing test. So this drives the refusal directly: hold a
//! generation, let the shard be taken by someone else, and assert the write is
//! turned away.
//!
//! Requires a live database:
//!   DATABASE_URL=postgres://…/orcher cargo test -p orcher-core --test write_fence_refuses_stale_owner

use std::sync::Arc;

use orcher_storage::repositories::domains::workflow_execution::WorkflowExecutionRepository;
use orcher_storage::sharding::{FenceToken, WriteFence};
use sqlx::PgPool;
use uuid::Uuid;

/// A fence pinned to one generation, standing in for a node that acquired the
/// shard and has been holding the token in memory ever since.
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

/// A fence that reports the shard as lost, which is what the real implementation
/// returns when a live other node owns it.
#[derive(Debug)]
struct LostShard;

#[async_trait::async_trait]
impl WriteFence for LostShard {
    async fn guard(&self, _workflow_id: Uuid) -> Option<FenceToken> {
        None
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

/// Put a workflow row in place so the completion has something to update.
async fn seed_workflow(pool: &PgPool, ns: &str) -> Uuid {
    let id = Uuid::new_v4();
    sqlx::query(
        r#"INSERT INTO workflow_executions
             (id, execution_id, workflow_id, workflow_name, workflow_version, namespace,
              task_queue, status, trigger_type, submitted_at, created_at, updated_at)
           VALUES ($1, $2, $1, 'fence-test', '1.0.0', $3, 'fence-q', 'running', 'api',
                   NOW(), NOW(), NOW())"#,
    )
    .bind(id)
    .bind(id.to_string())
    .bind(ns)
    .execute(pool)
    .await
    .expect("seed workflow row");
    id
}

/// Claim the shard at `generation` for some *other* node, which is the state the
/// database is left in after ownership moves.
async fn take_shard(pool: &PgPool, shard_id: i32, generation: i64, owner: &str) {
    sqlx::query(
        r#"INSERT INTO cluster_shards (shard_id, owner_node, generation, updated_at)
           VALUES ($1, $2, $3, NOW())
           ON CONFLICT (shard_id) DO UPDATE
             SET owner_node = EXCLUDED.owner_node,
                 generation = EXCLUDED.generation,
                 updated_at = NOW()"#,
    )
    .bind(shard_id)
    .bind(owner)
    .bind(generation)
    .execute(pool)
    .await
    .expect("take shard");
}

async fn status_of(pool: &PgPool, id: Uuid) -> String {
    sqlx::query_scalar::<_, String>("SELECT status FROM workflow_executions WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("read status")
}

/// The whole point: a superseded owner must not be able to finalise a workflow.
#[tokio::test]
async fn a_superseded_owner_cannot_complete_a_workflow() {
    let pool = pool().await;
    let shard_id = 4090;
    let id = seed_workflow(&pool, "default").await;

    // This node acquired the shard at generation 1 and still believes it holds it.
    // Meanwhile ownership moved: another node now holds it at generation 2.
    take_shard(&pool, shard_id, 2, "the-new-owner").await;

    let repo = WorkflowExecutionRepository::new((*pool).clone());
    repo.set_write_fence(Arc::new(PinnedFence {
        shard_id,
        generation: 1,
    }));

    let result = repo
        .complete_workflow_atomic(
            &id.to_string(),
            None,
            jiff::Timestamp::now().into(),
            1,
            1,
            0,
            0,
        )
        .await;

    assert!(
        result.is_err(),
        "a stale generation completed the workflow — the fence is not refusing anything"
    );
    assert_eq!(
        status_of(&pool, id).await,
        "running",
        "the workflow was finalised despite the refusal, so the write escaped the transaction"
    );

    let _ = sqlx::query("DELETE FROM workflow_executions WHERE id = $1")
        .bind(id)
        .execute(pool.as_ref())
        .await;
}

/// When the fence reports the shard lost outright, the write is refused rather
/// than falling through to an unguarded one.
#[tokio::test]
async fn a_lost_shard_refuses_rather_than_falling_back() {
    let pool = pool().await;
    let id = seed_workflow(&pool, "default").await;

    let repo = WorkflowExecutionRepository::new((*pool).clone());
    repo.set_write_fence(Arc::new(LostShard));

    let result = repo
        .complete_workflow_atomic(
            &id.to_string(),
            None,
            jiff::Timestamp::now().into(),
            1,
            1,
            0,
            0,
        )
        .await;

    assert!(
        result.is_err(),
        "a lost shard still allowed the write through"
    );
    assert_eq!(status_of(&pool, id).await, "running");

    let _ = sqlx::query("DELETE FROM workflow_executions WHERE id = $1")
        .bind(id)
        .execute(pool.as_ref())
        .await;
}

/// The positive half: holding the current generation must still work, or the
/// fence is simply breaking writes rather than authorising them.
#[tokio::test]
async fn the_current_owner_completes_normally() {
    let pool = pool().await;
    let shard_id = 4091;
    let id = seed_workflow(&pool, "default").await;

    take_shard(&pool, shard_id, 5, "this-node").await;

    let repo = WorkflowExecutionRepository::new((*pool).clone());
    repo.set_write_fence(Arc::new(PinnedFence {
        shard_id,
        generation: 5,
    }));

    repo.complete_workflow_atomic(
        &id.to_string(),
        None,
        jiff::Timestamp::now().into(),
        1,
        1,
        0,
        0,
    )
    .await
    .expect("the current owner must be allowed to complete");

    assert_eq!(status_of(&pool, id).await, "completed");

    let _ = sqlx::query("DELETE FROM workflow_executions WHERE id = $1")
        .bind(id)
        .execute(pool.as_ref())
        .await;
}

/// With no fence injected — every single-node deployment — writes proceed
/// exactly as before.
#[tokio::test]
async fn without_a_fence_writes_are_unguarded() {
    let pool = pool().await;
    let id = seed_workflow(&pool, "default").await;

    let repo = WorkflowExecutionRepository::new((*pool).clone());
    // No set_write_fence call at all.

    repo.complete_workflow_atomic(
        &id.to_string(),
        None,
        jiff::Timestamp::now().into(),
        1,
        1,
        0,
        0,
    )
    .await
    .expect("an unfenced repository must complete normally");

    assert_eq!(status_of(&pool, id).await, "completed");

    let _ = sqlx::query("DELETE FROM workflow_executions WHERE id = $1")
        .bind(id)
        .execute(pool.as_ref())
        .await;
}

/// Failing a workflow is as destructive as completing it — a superseded owner
/// marking something failed is exactly the outcome the fence exists to stop.
#[tokio::test]
async fn a_superseded_owner_cannot_fail_a_workflow() {
    let pool = pool().await;
    let shard_id = 4092;
    let id = seed_workflow(&pool, "default").await;

    take_shard(&pool, shard_id, 9, "the-new-owner").await;

    let repo = WorkflowExecutionRepository::new((*pool).clone());
    repo.set_write_fence(Arc::new(PinnedFence {
        shard_id,
        generation: 8, // one behind
    }));

    let result = repo
        .fail_workflow_atomic(
            &id.to_string(),
            "boom".to_string(),
            None,
            jiff::Timestamp::now().into(),
        )
        .await;

    assert!(result.is_err(), "a stale generation failed the workflow");
    assert_eq!(
        status_of(&pool, id).await,
        "running",
        "the workflow was marked failed despite the refusal"
    );

    let _ = sqlx::query("DELETE FROM workflow_executions WHERE id = $1")
        .bind(id)
        .execute(pool.as_ref())
        .await;
}

/// The generic status update is the one most likely to be reached by a
/// resumed node partway through a transition.
#[tokio::test]
async fn a_superseded_owner_cannot_move_status() {
    let pool = pool().await;
    let shard_id = 4093;
    let id = seed_workflow(&pool, "default").await;

    take_shard(&pool, shard_id, 4, "the-new-owner").await;

    let repo = WorkflowExecutionRepository::new((*pool).clone());
    repo.set_write_fence(Arc::new(PinnedFence {
        shard_id,
        generation: 3,
    }));

    let result = repo
        .update_status_by_workflow_id(&id.to_string(), "paused", None, None)
        .await;

    assert!(result.is_err(), "a stale generation moved the status");
    assert_eq!(status_of(&pool, id).await, "running");

    let _ = sqlx::query("DELETE FROM workflow_executions WHERE id = $1")
        .bind(id)
        .execute(pool.as_ref())
        .await;
}
