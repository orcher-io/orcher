//! Does a schedule's overlap policy see the run its last fire started?
//!
//! The check counted `schedule_instances`, a table nothing writes, and a fire
//! recorded its run where the check did not look, so every policy let every
//! tick through: a `skip` schedule whose job outlasted its interval piled up
//! runs (orcher-io/orcher#46). Each test here fires a schedule through the
//! processor, leaves the run it started open, and fires again.
//!
//! Requires a live database:
//!   DATABASE_URL=postgres://…/orcher cargo test -p orcher-core --test schedule_overlap -- --ignored --test-threads=1

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use orcher_common::types::time::Timestamptz;
use orcher_common::OrcherResult;
use orcher_core::engine::processors::schedule_processor::{
    ScheduleProcessor, ScheduleProcessorConfig,
};
use orcher_core::engine::queues::ScheduleQueue;
use orcher_storage::repositories::base::CrudOperations;
use orcher_storage::repositories::domains::schedule::ScheduleRepository;
use orcher_storage::schema::types::{OverlapPolicy, Schedule};
use sqlx::PgPool;
use uuid::Uuid;

type Fire = Pin<Box<dyn Future<Output = OrcherResult<Uuid>> + Send>>;
type Executor = dyn Fn(Schedule, Vec<String>) -> Fire + Send + Sync;

async fn pool() -> PgPool {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/orcher".to_string());
    PgPool::connect(&url)
        .await
        .expect("live database required; set DATABASE_URL")
}

/// A schedule under test, the processor that fires it, and the runs its
/// executor started.
struct Harness {
    pool: PgPool,
    repo: ScheduleRepository,
    processor: ScheduleProcessor,
    schedule: Schedule,
    /// Each fire's `replacing`, in order.
    fires: Arc<Mutex<Vec<Vec<String>>>>,
    /// Each started run's (row id, execution id).
    runs: Arc<Mutex<Vec<(Uuid, String)>>>,
    /// Whether the next start fails.
    fail_next: Arc<Mutex<bool>>,
}

impl Harness {
    async fn new(policy: OverlapPolicy, max_instances: i32) -> Self {
        let pool = pool().await;
        let repo = ScheduleRepository::new(pool.clone());
        let mut schedule = Schedule::new(
            format!("overlap-test-{}", Uuid::new_v4()),
            "0 0 0 * * *".to_string(),
            "UTC".to_string(),
            serde_json::json!({ "workflow_type": "overlap-test" }),
        );
        schedule.overlap_policy = policy;
        schedule.max_instances = max_instances;
        schedule.next_run_at = Some(Timestamptz::now());
        let schedule = CrudOperations::create(&repo, schedule)
            .await
            .expect("create schedule");
        let processor = ScheduleProcessor::new(
            ScheduleQueue::new(Arc::new(repo.clone())),
            ScheduleProcessorConfig {
                queue_recheck: Duration::from_secs(1),
                ..Default::default()
            },
        );
        Self {
            pool,
            repo,
            processor,
            schedule,
            fires: Arc::default(),
            runs: Arc::default(),
            fail_next: Arc::default(),
        }
    }

    /// An executor that starts a run the way a fire does, as a row the
    /// overlap check reads: open, under the schedule's own workflow id.
    fn executor(&self) -> Arc<Box<Executor>> {
        let pool = self.pool.clone();
        let fires = self.fires.clone();
        let runs = self.runs.clone();
        let fail_next = self.fail_next.clone();
        Arc::new(Box::new(
            move |schedule: Schedule, replacing: Vec<String>| {
                let pool = pool.clone();
                let fires = fires.clone();
                let runs = runs.clone();
                let fail = std::mem::take(&mut *fail_next.lock().unwrap());
                Box::pin(async move {
                    fires.lock().unwrap().push(replacing);
                    if fail {
                        return Err(orcher_common::error::OrcherError::internal(
                            "start refused".to_string(),
                        ));
                    }
                    let n = runs.lock().unwrap().len();
                    let run = seed_run(&pool, &format!("{}@{n}", schedule.id), "running").await;
                    runs.lock().unwrap().push(run.clone());
                    Ok(run.0)
                }) as Fire
            },
        ))
    }

    /// Process the schedule once, as the processor does a claimed one.
    async fn tick(&mut self) {
        let executor = self.executor();
        self.processor
            .process_schedule(&self.schedule, executor)
            .await
            .ok();
        self.schedule = CrudOperations::find_by_id(&self.repo, self.schedule.id)
            .await
            .expect("read schedule")
            .expect("schedule exists");
    }

    fn fire_count(&self) -> usize {
        self.fires.lock().unwrap().len()
    }

    fn run(&self, i: usize) -> (Uuid, String) {
        self.runs.lock().unwrap()[i].clone()
    }

    async fn end_run(&self, i: usize) {
        set_status(&self.pool, self.run(i).0, "completed").await;
    }

    async fn fire_statuses(&self) -> Vec<String> {
        sqlx::query_scalar(
            "SELECT status FROM schedule_executions WHERE schedule_id = $1 ORDER BY created_at",
        )
        .bind(self.schedule.id.as_uuid())
        .fetch_all(&self.pool)
        .await
        .expect("read fires")
    }

    async fn cleanup(self) {
        let _ = sqlx::query("DELETE FROM schedules WHERE id = $1")
            .bind(self.schedule.id.as_uuid())
            .execute(&self.pool)
            .await;
        let _ = sqlx::query("DELETE FROM workflow_executions WHERE workflow_id LIKE $1")
            .bind(format!("{}@%", self.schedule.id))
            .execute(&self.pool)
            .await;
    }
}

async fn seed_run(pool: &PgPool, workflow_id: &str, status: &str) -> (Uuid, String) {
    let id = Uuid::new_v4();
    let execution_id = Uuid::new_v4().to_string();
    sqlx::query(
        r#"INSERT INTO workflow_executions
             (id, execution_id, workflow_id, workflow_name, workflow_type, workflow_version,
              namespace, task_queue, status, trigger_type, submitted_at, created_at, updated_at)
           VALUES ($1, $2, $3, 'overlap-test', 'overlap-test', '1.0', 'default', 'overlap-q',
                   $4, 'scheduled', NOW(), NOW(), NOW())"#,
    )
    .bind(id)
    .bind(&execution_id)
    .bind(workflow_id)
    .bind(status)
    .execute(pool)
    .await
    .expect("seed run");
    (id, execution_id)
}

async fn set_status(pool: &PgPool, id: Uuid, status: &str) {
    sqlx::query(
        "UPDATE workflow_executions SET status = $2, \
         completed_at = CASE WHEN $2 IN ('completed', 'failed', 'cancelled', 'reset') \
                        THEN NOW() END WHERE id = $1",
    )
    .bind(id)
    .bind(status)
    .execute(pool)
    .await
    .expect("set status");
}

#[tokio::test]
#[ignore = "requires a live database"]
async fn skip_drops_a_tick_while_the_last_run_is_open() {
    let mut h = Harness::new(OverlapPolicy::Skip, 1).await;

    h.tick().await;
    assert_eq!(h.fire_count(), 1, "the first tick fires");

    h.tick().await;
    assert_eq!(h.fire_count(), 1, "a tick while the run is open is skipped");
    assert_eq!(
        h.schedule.run_count, 1,
        "a skipped tick is not counted as a run"
    );
    assert!(
        h.schedule.next_run_at.expect("due again") > Timestamptz::now(),
        "and the schedule is due at its next cron time"
    );

    h.end_run(0).await;
    h.tick().await;
    assert_eq!(h.fire_count(), 2, "once the run ends, the next tick fires");
    assert_eq!(
        h.fire_statuses().await,
        ["completed", "running"],
        "the first fire is settled to how its run ended"
    );
    h.cleanup().await;
}

#[tokio::test]
#[ignore = "requires a live database"]
async fn queue_holds_a_tick_until_the_last_run_ends() {
    let mut h = Harness::new(OverlapPolicy::Queue, 1).await;

    h.tick().await;
    h.tick().await;
    assert_eq!(h.fire_count(), 1, "a tick while the run is open waits");
    let due = h.schedule.next_run_at.expect("held, not dropped");
    assert!(
        due <= Timestamptz::now() + jiff::SignedDuration::from_secs(2),
        "a held tick is looked at again soon, not at the next cron time"
    );

    h.tick().await;
    assert_eq!(h.fire_count(), 1, "and keeps waiting while the run is open");

    h.end_run(0).await;
    h.tick().await;
    assert_eq!(h.fire_count(), 2, "then fires once the run ends");
    h.cleanup().await;
}

#[tokio::test]
#[ignore = "requires a live database"]
async fn replace_starts_a_run_and_names_the_open_one_to_cancel() {
    let mut h = Harness::new(OverlapPolicy::Replace, 1).await;

    h.tick().await;
    h.tick().await;
    let fires = h.fires.lock().unwrap().clone();
    assert_eq!(fires.len(), 2, "a replace tick always fires");
    assert!(
        fires[0].is_empty(),
        "with nothing open, nothing is replaced"
    );
    assert_eq!(fires[1], [h.run(0).1], "the open run is the one replaced");
    h.cleanup().await;
}

#[tokio::test]
#[ignore = "requires a live database"]
async fn parallel_runs_up_to_max_instances() {
    let mut h = Harness::new(OverlapPolicy::Parallel, 2).await;
    for _ in 0..3 {
        h.tick().await;
    }
    assert_eq!(h.fire_count(), 2, "two open runs at most");
    h.end_run(0).await;
    h.tick().await;
    assert_eq!(h.fire_count(), 3, "one ending makes room for another");
    h.cleanup().await;
}

/// A run that carries on after the one a fire started — a reset's run, or a
/// failure waiting for its retry — keeps the fire open.
#[tokio::test]
#[ignore = "requires a live database"]
async fn a_run_carried_on_under_the_fires_workflow_id_keeps_it_open() {
    let mut h = Harness::new(OverlapPolicy::Skip, 1).await;
    h.tick().await;
    let (first, _) = h.run(0);
    let workflow_id = format!("{}@0", h.schedule.id);

    set_status(&h.pool, first, "reset").await;
    let (after_reset, _) = seed_run(&h.pool, &workflow_id, "running").await;
    h.tick().await;
    assert_eq!(h.fire_count(), 1, "the reset's run is still open");

    set_status(&h.pool, after_reset, "failed").await;
    sqlx::query(
        "UPDATE workflow_executions SET max_retries = 3, retry_count = 0, \
         next_retry_at = NOW() + INTERVAL '1 hour' WHERE id = $1",
    )
    .bind(after_reset)
    .execute(&h.pool)
    .await
    .expect("schedule a retry");
    h.tick().await;
    assert_eq!(h.fire_count(), 1, "a failure waiting for its retry is open");

    sqlx::query("UPDATE workflow_executions SET next_retry_at = NULL WHERE id = $1")
        .bind(after_reset)
        .execute(&h.pool)
        .await
        .expect("give up the retry");
    h.tick().await;
    assert_eq!(
        h.fire_count(),
        2,
        "once every run has ended, the tick fires"
    );
    assert_eq!(
        h.fire_statuses().await,
        ["failed", "running"],
        "settled to how its last run ended"
    );
    h.cleanup().await;
}

#[tokio::test]
#[ignore = "requires a live database"]
async fn a_failed_start_leaves_nothing_open() {
    let mut h = Harness::new(OverlapPolicy::Skip, 1).await;
    *h.fail_next.lock().unwrap() = true;
    h.tick().await;
    assert_eq!(h.fire_statuses().await, ["failed"], "recorded as failed");

    h.tick().await;
    assert_eq!(h.fire_count(), 2, "and the next tick fires");
    h.cleanup().await;
}
