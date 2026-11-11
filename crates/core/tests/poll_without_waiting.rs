//! A workflow poll that may not wait still looks once.
//!
//! A poll for several activations waits for the first and takes the rest
//! with polls that may not wait at all. Such a poll answered before it had
//! looked, so it never found anything: it must claim what is ready, and
//! answer at once when nothing is.
//!
//! Requires PostgreSQL. The schema is created by the engine's own migration
//! runner. Run with:
//!
//! ```bash
//! export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/orcher_test"
//! cargo test -p orcher-core --test poll_without_waiting -- --ignored --test-threads=1
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};

use orcher_common::types::dispatch::polling::PollExecutionRequest;
use orcher_core::engine::dispatch::PollingManager;
use orcher_core::engine::queues::outbox::OutboxQueue;
use orcher_core::events::EventBus;
use orcher_core::workflows::{JournalService, WorkflowExecutionService};
use orcher_events::bus::EventBusConfig;
use orcher_storage::repositories::domains::{
    coordination::OutboxRepository, journal::JournalRepository,
    workflow_execution::WorkflowExecutionRepository,
};
use sqlx::PgPool;
use uuid::Uuid;

const NAMESPACE: &str = "default";
const CLAIM_TIMEOUT_SECS: i64 = 300;
/// Under a poll's periodic look (200ms): a poll that may not wait never
/// waits for one.
const AT_ONCE: Duration = Duration::from_millis(150);

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

async fn cleanup(db: &PgPool, task_queue: &str) {
    for q in [
        "DELETE FROM workflow_execution_journal WHERE execution_id IN \
         (SELECT execution_id FROM workflow_executions WHERE task_queue = $1)",
        "DELETE FROM workflow_execution_logs WHERE execution_id IN \
         (SELECT execution_id FROM workflow_executions WHERE task_queue = $1)",
        "DELETE FROM outbox_queue WHERE workflow_id IN \
         (SELECT id FROM workflow_executions WHERE task_queue = $1)",
        "DELETE FROM workflow_executions WHERE task_queue = $1",
    ] {
        let _ = sqlx::query(q).bind(task_queue).execute(db).await;
    }
}

fn run_start(task_queue: &str) -> orcher_core::workflows::RunStart {
    orcher_core::workflows::RunStart {
        workflow_name: "orders".to_string(),
        workflow_version: "1.0".to_string(),
        namespace: NAMESPACE.to_string(),
        task_queue: task_queue.to_string(),
        input: serde_json::Value::Null,
        workflow_id: None,
        labels: None,
        max_retries: None,
        retry_policy: None,
        timeouts: orcher_core::workflows::RunTimeouts::default(),
        annotations: None,
        start_delay: None,
        request_id: None,
    }
}

#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_poll_that_may_not_wait_claims_what_is_ready_and_no_more() {
    let pool = pool().await;
    let task_queue = format!("no-wait-{}", Uuid::new_v4().simple());

    let journal_repo = Arc::new(JournalRepository::new(pool.clone()));
    let exec_repo = Arc::new(WorkflowExecutionRepository::new(pool.clone()));
    let outbox_queue = Arc::new(OutboxQueue::new(Arc::new(OutboxRepository::new(Arc::new(
        pool.clone(),
    )))));
    let polling = Arc::new(PollingManager::new(
        exec_repo.clone(),
        journal_repo.clone(),
        CLAIM_TIMEOUT_SECS,
    ));
    let executions = WorkflowExecutionService::new(
        exec_repo,
        Arc::new(JournalService::new(journal_repo)),
        outbox_queue,
        EventBus::new(EventBusConfig::default()),
        pool.clone(),
    )
    .with_polling_manager(polling.clone());

    for _ in 0..2 {
        executions
            .start_workflow(run_start(&task_queue), None, None)
            .await
            .expect("start a run");
    }
    let poll = || {
        polling.poll_execution(PollExecutionRequest::new(
            task_queue.clone(),
            NAMESPACE,
            "worker-a",
            Duration::ZERO,
        ))
    };

    let mut claimed = Vec::new();
    for _ in 0..2 {
        let asked = Instant::now();
        let response = poll().await.expect("poll");
        assert!(asked.elapsed() < AT_ONCE, "waited {:?}", asked.elapsed());
        claimed.push(response.expect("a ready run is claimed").execution_id);
    }
    let asked = Instant::now();
    let none = poll().await.expect("poll");
    let elapsed = asked.elapsed();
    cleanup(&pool, &task_queue).await;

    assert_ne!(claimed[0], claimed[1], "the same run was claimed twice");
    assert!(none.is_none(), "claimed a run that was not there");
    assert!(elapsed < AT_ONCE, "an empty look waited {elapsed:?}");
}
