//! A new run wakes the workflow polls already waiting on its queue.
//!
//! Starting a workflow woke the outbox processor, which wakes task pollers,
//! but nothing woke the workflow polls. A poll already waiting found the new
//! run only on its next periodic look, up to 200ms later, so every run's first
//! activation waited for it. A start now wakes them as an event or a task
//! outcome does.
//!
//! The poll's periodic look is set far beyond the time allowed, so a poll is
//! answered in time only if it was woken. Against the default 200ms the test
//! measured the claim query too, and on a slow database a woken poll's claim
//! alone could pass the bound set under it.
//!
//! Requires PostgreSQL. The schema is created by the engine's own migration
//! runner. Run with:
//!
//! ```bash
//! export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/orcher_test"
//! cargo test -p orcher-core --test start_wakes_polls -- --ignored --test-threads=1
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
/// A poll that nothing wakes looks again only after this long.
const RETRY_INTERVAL: Duration = Duration::from_secs(60);
/// A woken poll answers after one claim query: tens of milliseconds, a few
/// hundred on a loaded database, and nowhere near the retry interval.
const WOKEN_WITHIN: Duration = Duration::from_secs(5);

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

#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_new_run_wakes_a_waiting_poll() {
    let pool = pool().await;
    let task_queue = format!("start-wakes-{}", Uuid::new_v4().simple());

    let journal_repo = Arc::new(JournalRepository::new(pool.clone()));
    let exec_repo = Arc::new(WorkflowExecutionRepository::new(pool.clone()));
    let outbox_queue = Arc::new(OutboxQueue::new(Arc::new(OutboxRepository::new(Arc::new(
        pool.clone(),
    )))));
    let polling = Arc::new(
        PollingManager::new(exec_repo.clone(), journal_repo.clone(), CLAIM_TIMEOUT_SECS)
            .with_retry_interval(RETRY_INTERVAL),
    );
    let executions = WorkflowExecutionService::new(
        exec_repo,
        Arc::new(JournalService::new(journal_repo)),
        outbox_queue,
        EventBus::new(EventBusConfig::default()),
        pool.clone(),
    )
    .with_polling_manager(polling.clone());

    // A poll that finds nothing and settles into its wait.
    let poll = {
        let polling = polling.clone();
        let task_queue = task_queue.clone();
        tokio::spawn(async move {
            let response = polling
                .poll_execution(PollExecutionRequest::new(
                    task_queue,
                    NAMESPACE,
                    "worker-a",
                    RETRY_INTERVAL * 2,
                ))
                .await
                .expect("poll");
            (response, Instant::now())
        })
    };
    tokio::time::sleep(Duration::from_millis(30)).await;

    let started = executions
        .start_workflow(
            orcher_core::workflows::RunStart {
                workflow_name: "orders".to_string(),
                workflow_version: "1.0".to_string(),
                namespace: NAMESPACE.to_string(),
                task_queue: task_queue.clone(),
                input: serde_json::Value::Null,
                workflow_id: None,
                labels: None,
                max_retries: None,
                retry_policy: None,
                timeouts: orcher_core::workflows::RunTimeouts::default(),
                annotations: None,
                start_delay: None,
                request_id: None,
            },
            None,
            None,
        )
        .await
        .expect("start a run");
    let started_at = Instant::now();

    let answered = tokio::time::timeout(WOKEN_WITHIN, poll).await;
    cleanup(&pool, &task_queue).await;

    let Ok(answered) = answered else {
        panic!(
            "the waiting poll did not see the new run within {WOKEN_WITHIN:?}; it was not woken"
        );
    };
    let (response, answered_at) = answered.expect("poll task");
    let response = response.expect("the waiting poll is answered with the new run");
    assert_eq!(response.execution_id, started.execution_id);
    eprintln!(
        "woken poll answered {:?} after the start returned",
        answered_at.saturating_duration_since(started_at)
    );
}
