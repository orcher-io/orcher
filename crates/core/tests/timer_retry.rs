//! A timer whose fire fails is retried after a backoff, until it fires.
//!
//! A failed fire used to put the timer straight back, due at once, and the
//! processor claimed it again on its next pass: a fault lasting a moment spent
//! all three attempts in milliseconds, the timer ended failed, and the
//! workflow parked on it never woke (orcher-io/orcher#45).
//!
//! Requires PostgreSQL. Run with:
//!
//! ```bash
//! export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/orcher_test"
//! cargo test -p orcher-core --test timer_retry -- --ignored --test-threads=1
//! ```

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use orcher_common::error::OrcherError;
use orcher_common::types::time::Timestamptz;
use orcher_core::engine::dispatch::PollingManager;
use orcher_core::engine::processors::{TimerProcessor, TimerProcessorConfig};
use orcher_core::engine::queues::{TimerQueue, TimerTask};
use orcher_core::services::TimerExecutionService;
use orcher_core::workflows::JournalService;
use orcher_storage::repositories::domains::coordination::TimerRepository;
use orcher_storage::repositories::domains::journal::JournalRepository;
use orcher_storage::repositories::domains::workflow_execution::WorkflowExecutionRepository;
use orcher_storage::schema::types::coordination::TimerTaskType;
use sqlx::PgPool;
use uuid::Uuid;

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

/// A timer due now, in an organization of its own so only this test claims it.
async fn due_timer(timers: &TimerRepository) -> (Uuid, Uuid) {
    let organization = Uuid::new_v4();
    let timer = timers
        .create(TimerTask::new_with_org(
            organization,
            Uuid::new_v4(),
            TimerTaskType::FireTimer,
            serde_json::json!({ "timer_name": "nap" }),
            Timestamptz::now(),
        ))
        .await
        .expect("create the timer");
    (organization, timer.task_id)
}

/// The timer's status, attempts, and seconds until it is due.
async fn timer_state(pool: &PgPool, task_id: Uuid) -> (String, i32, f64) {
    sqlx::query_as(
        "SELECT status, attempt_count, EXTRACT(EPOCH FROM fire_at - NOW())::float8 \
         FROM timer_queue WHERE task_id = $1",
    )
    .bind(task_id)
    .fetch_one(pool)
    .await
    .expect("read the timer")
}

async fn make_due(pool: &PgPool, task_id: Uuid) {
    sqlx::query("UPDATE timer_queue SET fire_at = NOW() WHERE task_id = $1")
        .bind(task_id)
        .execute(pool)
        .await
        .expect("make the timer due");
}

async fn forget(pool: &PgPool, task_id: Uuid) {
    let _ = sqlx::query("DELETE FROM timer_queue WHERE task_id = $1")
        .bind(task_id)
        .execute(pool)
        .await;
}

/// Each failed attempt puts the timer back due after a backoff that doubles
/// from a second, spread by a fifth either way, and capped at five minutes;
/// it is still pending after more attempts than it used to be allowed.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_failed_fire_is_retried_after_a_growing_backoff() {
    let pool = pool().await;
    let timers = TimerRepository::new(Arc::new(pool.clone()));
    let (organization, task_id) = due_timer(&timers).await;

    for attempt in 1..=5 {
        let claimed = timers
            .claim_ready_tasks(Some(organization), 10)
            .await
            .expect("claim");
        assert_eq!(claimed.len(), 1, "attempt {attempt}");
        assert!(timers
            .fail(task_id, "the database blinked".to_string())
            .await
            .expect("fail the fire"));

        let (status, attempts, due_in) = timer_state(&pool, task_id).await;
        assert_eq!(status, "pending", "attempt {attempt}");
        assert_eq!(attempts, attempt);
        let backoff = f64::from(1 << (attempt - 1));
        assert!(
            due_in > backoff * 0.8 - 0.2 && due_in <= backoff * 1.2,
            "attempt {attempt}: due in {due_in}s, backoff {backoff}s"
        );
        // Not handed out again before it is due.
        assert!(timers
            .claim_ready_tasks(Some(organization), 10)
            .await
            .expect("claim")
            .is_empty());
        make_due(&pool, task_id).await;
    }

    sqlx::query("UPDATE timer_queue SET attempt_count = 40 WHERE task_id = $1")
        .bind(task_id)
        .execute(&pool)
        .await
        .expect("age the timer");
    timers
        .claim_ready_tasks(Some(organization), 10)
        .await
        .expect("claim");
    timers
        .fail(task_id, "still down".to_string())
        .await
        .expect("fail the fire");
    let (status, _, due_in) = timer_state(&pool, task_id).await;
    assert_eq!(status, "pending");
    assert!(due_in > 240.0 - 1.0 && due_in <= 360.0, "due in {due_in}s");

    forget(&pool, task_id).await;
}

/// A fire whose claim the stuck-timer sweep took back leaves the timer to
/// whoever holds it now.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_fire_that_lost_its_claim_leaves_the_timer_alone() {
    let pool = pool().await;
    let timers = TimerRepository::new(Arc::new(pool.clone()));
    let (organization, task_id) = due_timer(&timers).await;
    timers
        .claim_ready_tasks(Some(organization), 10)
        .await
        .expect("claim");
    sqlx::query(
        "UPDATE timer_queue SET status = 'pending', processing_started_at = NULL \
         WHERE task_id = $1",
    )
    .bind(task_id)
    .execute(&pool)
    .await
    .expect("release the claim");

    assert!(!timers
        .fail(task_id, "late".to_string())
        .await
        .expect("fail the fire"));
    let (status, _, due_in) = timer_state(&pool, task_id).await;
    assert_eq!(status, "pending");
    assert!(due_in <= 0.0, "the timer was pushed back: due in {due_in}s");

    forget(&pool, task_id).await;
}

/// The processor does not hand a failed timer out again at once, and fires it
/// when it is retried.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn the_processor_retries_a_failed_fire_later_and_fires_it() {
    let pool = pool().await;
    let timers = Arc::new(TimerRepository::new(Arc::new(pool.clone())));
    let (_, task_id) = due_timer(&timers).await;

    let processor = Arc::new(TimerProcessor::new(
        TimerQueue::new(timers.clone()),
        TimerProcessorConfig {
            max_sleep_duration: Duration::from_millis(50),
            ..TimerProcessorConfig::default()
        },
    ));
    let fires = Arc::new(AtomicUsize::new(0));
    let counted = fires.clone();
    processor
        .clone()
        .start(move |timer: TimerTask| {
            let counted = counted.clone();
            Box::pin(async move {
                if timer.task_id != task_id {
                    // Another test's timer; leave it be.
                    return Err(OrcherError::internal("not this test's timer"));
                }
                if counted.fetch_add(1, Ordering::SeqCst) == 0 {
                    Err(OrcherError::internal("the database blinked"))
                } else {
                    Ok(())
                }
            })
        })
        .await
        .expect("start the processor");

    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(
        fires.load(Ordering::SeqCst),
        1,
        "retried before its backoff"
    );

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while timer_state(&pool, task_id).await.0 != "completed" {
        assert!(tokio::time::Instant::now() < deadline, "never fired");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(fires.load(Ordering::SeqCst), 2);

    processor.shutdown().await;
    forget(&pool, task_id).await;
}

/// A timer whose workflow no longer exists has nothing to resume: its fire
/// succeeds rather than being retried for good.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_timer_whose_workflow_is_gone_is_done() {
    let pool = pool().await;
    let executions = Arc::new(WorkflowExecutionRepository::new(pool.clone()));
    let journal = Arc::new(JournalRepository::new(pool.clone()));
    let service = TimerExecutionService::new(
        executions.clone(),
        Arc::new(JournalService::new(journal.clone())),
        Arc::new(PollingManager::new(executions, journal, 300)),
    );
    service
        .execute_task(TimerTask::new(
            Uuid::new_v4(),
            TimerTaskType::FireTimer,
            serde_json::json!({ "timer_name": "nap" }),
            Timestamptz::now(),
        ))
        .await
        .expect("a fire for a workflow that is gone");
}
