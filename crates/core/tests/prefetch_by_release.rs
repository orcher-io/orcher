//! A prefetched run goes only to a poll of the release it was bound to.
//!
//! With prefetch on (the default), a poll that finds its buffer empty claims a
//! batch, binds every run in it to its own release, and leaves the rest in a
//! buffer the next polls are served from. The buffer was shared by every poll
//! of the queue, so during a rollout a run bound to the outgoing release could
//! be handed to a worker of the incoming one: the divergence binding exists to
//! prevent. A batch is now handed only to polls of the release it was claimed
//! for.
//!
//! Requires a live database:
//!   DATABASE_URL=postgres://…/orcher cargo test -p orcher-core --test prefetch_by_release

use std::sync::Arc;
use std::time::Duration;

use orcher_common::types::dispatch::polling::PollExecutionRequest;
use orcher_core::engine::dispatch::PollingManager;
use orcher_storage::repositories::domains::{
    journal::JournalRepository, workflow_execution::WorkflowExecutionRepository,
};
use sqlx::PgPool;
use uuid::Uuid;

async fn pool() -> PgPool {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/orcher".to_string());
    PgPool::connect(&url)
        .await
        .expect("live database required; set DATABASE_URL")
}

/// A claimable run on `queue`.
async fn seed(pool: &PgPool, queue: &str) -> Uuid {
    let id = Uuid::new_v4();
    sqlx::query(
        r#"INSERT INTO workflow_executions
             (id, execution_id, workflow_id, workflow_name, workflow_version, namespace,
              task_queue, status, trigger_type, submitted_at, created_at, updated_at)
           VALUES ($1, $2, $1, 'prefetch-test', '1.0.0', 'default', $3, 'pending', 'api',
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

async fn bound_version(pool: &PgPool, id: Uuid) -> Option<String> {
    sqlx::query_scalar("SELECT bound_version_id FROM workflow_executions WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("read binding")
}

fn poll(queue: &str, release: &str) -> PollExecutionRequest {
    let mut request = PollExecutionRequest::new(
        queue,
        "default",
        format!("worker-{release}"),
        Duration::from_millis(300),
    );
    request.version_id = Some(release.to_string());
    request
}

#[tokio::test]
async fn a_prefetched_run_is_handed_only_to_its_own_release() {
    let pool = pool().await;
    let queue = format!("prefetch-release-{}", Uuid::new_v4());
    let runs = [seed(&pool, &queue).await, seed(&pool, &queue).await];
    let manager = PollingManager::new(
        Arc::new(WorkflowExecutionRepository::new(pool.clone())),
        Arc::new(JournalRepository::new(pool.clone())),
        300,
    )
    .with_prefetch(orcher_dispatch::polling::PrefetchConfig::default());

    // Release a's poll claims both in one batch, binding both to a, and is
    // handed one; the other waits in the buffer.
    let first = manager
        .poll_execution(poll(&queue, "a"))
        .await
        .expect("poll")
        .expect("release a gets a run");
    for run in runs {
        assert_eq!(bound_version(&pool, run).await.as_deref(), Some("a"));
    }

    // Release b must not be handed the run bound to a.
    let handed_to_b = manager
        .poll_execution(poll(&queue, "b"))
        .await
        .expect("poll");
    assert!(
        handed_to_b.is_none(),
        "release b was handed {:?}, bound to release a",
        handed_to_b.map(|r| r.execution_id)
    );

    // Release a still gets it.
    let second = manager
        .poll_execution(poll(&queue, "a"))
        .await
        .expect("poll")
        .expect("release a gets the buffered run");
    assert_ne!(first.execution_id, second.execution_id);

    sqlx::query("DELETE FROM workflow_executions WHERE task_queue = $1")
        .bind(&queue)
        .execute(&pool)
        .await
        .expect("clean up");
}
