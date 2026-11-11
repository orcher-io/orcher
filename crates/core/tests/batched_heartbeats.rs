//! The heartbeats of several running tasks are recorded together.
//!
//! A worker running many tasks sends their heartbeats in one call, and the
//! engine finds the tasks and records them in a statement each, however many
//! there are. Each running task is recorded as one heartbeat would record it;
//! a task that is not running, or a token that names no task, is left out.
//!
//! Requires PostgreSQL. The schema is created by the engine's own migration
//! runner. Run with:
//!
//! ```bash
//! export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/orcher_test"
//! cargo test -p orcher-core --test batched_heartbeats -- --ignored --test-threads=1
//! ```

use orcher_common::types::identifiers::{ExecutionId, WorkflowId};
use orcher_storage::repositories::domains::task_execution::TaskExecutionRepository;
use orcher_storage::schema::types::execution::TaskExecution;
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

async fn scheduled(repo: &TaskExecutionRepository, queue: &str, n: usize) -> TaskExecution {
    repo.create_scheduled(
        format!("charge_{n}"),
        "charge".to_string(),
        WorkflowId::from_uuid(Uuid::new_v4()),
        ExecutionId::from_uuid(Uuid::new_v4()),
        queue.to_string(),
        "default".to_string(),
        1,
        None,
        None,
        None,
        Some(600),
        Some(30),
        None,
        None,
    )
    .await
    .expect("schedule a task")
}

#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn running_tasks_are_found_and_recorded_together() {
    let pool = pool().await;
    let repo = TaskExecutionRepository::new(pool.clone());
    let queue = format!("heartbeats-{}", Uuid::new_v4().simple());
    let token = |n: usize| format!("{queue}-token-{n}");

    let mut running = Vec::new();
    for n in 0..2 {
        let task = scheduled(&repo, &queue, n).await;
        running.push(
            repo.mark_started(task.id, "worker".to_string(), token(n))
                .await
                .expect("start a task"),
        );
    }
    let waiting = scheduled(&repo, &queue, 2).await;

    let found = repo
        .find_by_task_tokens(&[token(0), token(1), format!("{queue}-unknown")])
        .await
        .expect("find tasks by token");
    let mut found_ids: Vec<_> = found.iter().map(|task| task.id).collect();
    found_ids.sort_by_key(|id| id.as_uuid());
    let mut running_ids: Vec<_> = running.iter().map(|task| task.id).collect();
    running_ids.sort_by_key(|id| id.as_uuid());
    assert_eq!(found_ids, running_ids, "found other than the running tasks");

    let recorded = repo
        .record_heartbeats(&[
            (running[0].id, Some(b"half".to_vec())),
            (running[1].id, None),
            // Not running: left out.
            (waiting.id, Some(b"never".to_vec())),
            // Named twice: recorded once.
            (running[1].id, None),
        ])
        .await
        .expect("record heartbeats");
    let mut recorded_ids: Vec<_> = recorded.iter().map(|(id, _)| *id).collect();
    recorded_ids.sort_by_key(|id| id.as_uuid());
    assert_eq!(recorded_ids, running_ids);
    assert!(
        recorded.iter().all(|(_, stopped)| !stopped),
        "a task of no cancelled workflow read as stopped"
    );

    let first = repo.find_by_id(running[0].id).await.unwrap().unwrap();
    assert!(first.last_heartbeat_at.is_some());
    assert_eq!(first.heartbeat_details.as_deref(), Some(&b"half"[..]));
    let second = repo.find_by_id(running[1].id).await.unwrap().unwrap();
    assert!(second.last_heartbeat_at.is_some());
    assert_eq!(second.heartbeat_details, None);
    let untouched = repo.find_by_id(waiting.id).await.unwrap().unwrap();
    assert!(untouched.last_heartbeat_at.is_none());

    sqlx::query("DELETE FROM task_executions WHERE task_queue = $1")
        .bind(&queue)
        .execute(&pool)
        .await
        .expect("clean up");
}
