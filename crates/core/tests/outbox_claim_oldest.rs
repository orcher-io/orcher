//! The outbox processor claims the oldest ready rows, whatever their workflow.
//!
//! The claim kept only the oldest ready row of each workflow and chose which
//! workflows by their id, not by age, before it locked anything:
//!
//! * a workflow with several ready rows had one claimed per poll, the others
//!   left for the polls after it;
//! * with more workflows waiting than a claim takes, the ones whose ids sort
//!   last waited for every workflow whose id sorts before them, however much
//!   older their rows were;
//! * a row another claimer held still counted towards the claim's limit, so
//!   the claim came back with less than was ready, or with nothing.
//!
//! One row per workflow was never an ordering the processor needed (see
//! `OutboxRepository::claim_tasks_excluding_types`). The claim now takes the
//! oldest ready rows, skipping only what another claimer holds.
//!
//! Requires PostgreSQL. Run with:
//!
//! ```bash
//! export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/orcher_test"
//! cargo test -p orcher-core --test outbox_claim_oldest -- --ignored --test-threads=1
//! ```

use std::sync::Arc;

use orcher_core::engine::queues::outbox::OutboxQueue;
use orcher_storage::repositories::domains::coordination::OutboxRepository;
use sqlx::PgPool;
use uuid::Uuid;

/// What the outbox processor leaves out of its claim by default: task
/// dispatches, which the task pollers claim.
const PROCESSOR_EXCLUDES: &[&str] = &["dispatch_task"];

async fn pool() -> PgPool {
    let url = std::env::var("TEST_DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:postgres@localhost:5432/orcher_test".to_string()
    });
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .connect(&url)
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

/// The outbox as the processor reads it.
fn outbox(pool: &PgPool) -> OutboxQueue {
    OutboxQueue::new(Arc::new(OutboxRepository::new(Arc::new(pool.clone()))))
}

/// An organization of its own, so a claim made for it sees only this test's
/// rows.
async fn organization(pool: &PgPool) -> Uuid {
    let owner = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO users (id, email, password_hash, hash_algorithm, password_changed_at) \
         VALUES ($1, $2, 'x', 'argon2', NOW())",
    )
    .bind(owner)
    .bind(format!("owner-{owner}@outbox-claim-oldest.test"))
    .execute(pool)
    .await
    .expect("seed user");
    let organization = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO organizations (id, name, display_name, slug, owner_id) \
         VALUES ($1, $2, $2, $2, $3)",
    )
    .bind(organization)
    .bind(format!("org-{organization}"))
    .bind(owner)
    .execute(pool)
    .await
    .expect("seed organization");
    organization
}

/// A workflow id whose first byte is `first`, so a test can place it before
/// or after another in id order.
fn workflow_id(first: u8) -> Uuid {
    let mut bytes = *Uuid::new_v4().as_bytes();
    bytes[0] = first;
    Uuid::from_bytes(bytes)
}

/// A pending `dispatch_step` row of `workflow`, made `age_secs` ago, waiting
/// out a backoff for `backoff_secs` more when that is positive.
async fn row(
    pool: &PgPool,
    organization: Uuid,
    workflow: Uuid,
    age_secs: i64,
    backoff_secs: i64,
) -> Uuid {
    let task_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO outbox_queue
            (task_id, organization_id, workflow_id, task_type, payload, idempotency_key,
             status, created_at, attempt_count, max_retries, backoff_multiplier, next_retry_at)
        VALUES ($1, $2, $3, 'dispatch_step',
                jsonb_build_object('task_queue', 'q', 'namespace', 'default',
                                   'execution_id', $1::text),
                $1::text, 'pending', NOW() - make_interval(secs => $4), 0, 3, 2.0,
                CASE WHEN $5 > 0 THEN NOW() + make_interval(secs => $5) END)
        "#,
    )
    .bind(task_id)
    .bind(organization)
    .bind(workflow)
    .bind(age_secs as f64)
    .bind(backoff_secs as f64)
    .execute(pool)
    .await
    .expect("seed outbox row");
    task_id
}

async fn claim(pool: &PgPool, organization: Uuid, batch: usize) -> Vec<Uuid> {
    outbox(pool)
        .claim_tasks_excluding_types(Some(organization), batch, PROCESSOR_EXCLUDES)
        .await
        .expect("claim")
        .into_iter()
        .map(|t| t.task_id)
        .collect()
}

async fn status(pool: &PgPool, task_id: Uuid) -> String {
    sqlx::query_scalar("SELECT status FROM outbox_queue WHERE task_id = $1")
        .bind(task_id)
        .fetch_one(pool)
        .await
        .expect("read status")
}

/// Every ready row of one workflow is claimed by one claim with room for them.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_workflows_ready_rows_are_claimed_together() {
    let pool = pool().await;
    let org = organization(&pool).await;
    let wf = workflow_id(0x80);
    let rows = vec![
        row(&pool, org, wf, 30, 0).await,
        row(&pool, org, wf, 20, 0).await,
        row(&pool, org, wf, 10, 0).await,
    ];

    let mut claimed = claim(&pool, org, 10).await;
    claimed.sort();
    let mut expected = rows.clone();
    expected.sort();
    assert_eq!(
        claimed, expected,
        "one claim must take every ready row of a workflow, not one per poll"
    );
}

/// With more ready rows than a claim takes, the oldest go first, whatever
/// their workflow's id.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn the_oldest_ready_row_goes_first_whatever_its_workflow_id() {
    let pool = pool().await;
    let org = organization(&pool).await;
    // The older row's workflow sorts last by id, the newer one's first.
    let older = row(&pool, org, workflow_id(0xff), 60, 0).await;
    let newer = row(&pool, org, workflow_id(0x00), 1, 0).await;

    assert_eq!(
        claim(&pool, org, 1).await,
        vec![older],
        "the oldest ready row must be claimed first, not the one whose workflow id sorts first"
    );
    assert_eq!(claim(&pool, org, 1).await, vec![newer]);
}

/// A row another claimer holds is skipped, and the claim takes the next ready
/// row in its place.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_row_another_claimer_holds_stands_in_front_of_nothing() {
    let pool = pool().await;
    let org = organization(&pool).await;
    let held = row(&pool, org, workflow_id(0x00), 60, 0).await;
    let free = row(&pool, org, workflow_id(0xff), 30, 0).await;

    // Another claimer, mid-claim, holding the oldest row.
    let mut other = pool.begin().await.expect("begin");
    sqlx::query("SELECT 1 FROM outbox_queue WHERE task_id = $1 FOR UPDATE")
        .bind(held)
        .execute(&mut *other)
        .await
        .expect("hold a row");

    let claimed = claim(&pool, org, 1).await;
    other.rollback().await.expect("release the row");

    assert_eq!(
        claimed,
        vec![free],
        "a held row must not use up the claim; the next ready row must be taken"
    );
    assert_eq!(status(&pool, held).await, "pending");
}

/// A row waiting out a backoff holds back neither its workflow's newer rows
/// nor anyone else's, and is not claimed before its time.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_row_in_backoff_holds_back_nothing() {
    let pool = pool().await;
    let org = organization(&pool).await;
    let wf = workflow_id(0x40);
    let backing_off = row(&pool, org, wf, 60, 300).await;
    let ready = row(&pool, org, wf, 30, 0).await;
    let elsewhere = row(&pool, org, workflow_id(0x41), 10, 0).await;

    let mut claimed = claim(&pool, org, 10).await;
    claimed.sort();
    let mut expected = vec![ready, elsewhere];
    expected.sort();
    assert_eq!(
        claimed, expected,
        "the ready rows must be claimed, the row in backoff left until its time"
    );
    assert_eq!(status(&pool, backing_off).await, "pending");
}

/// Two claimers at once take every ready row between them, each row once.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn concurrent_claims_take_each_ready_row_once() {
    let pool = pool().await;
    let org = organization(&pool).await;
    let mut rows = Vec::new();
    for w in 0..10u8 {
        let wf = workflow_id(w.wrapping_mul(25));
        for age in 0..4 {
            rows.push(row(&pool, org, wf, 100 - (w as i64) * 4 - age, 0).await);
        }
    }

    let mut taken = Vec::new();
    for _ in 0..20 {
        let (a, b) = tokio::join!(claim(&pool, org, 7), claim(&pool, org, 7));
        if a.is_empty() && b.is_empty() {
            break;
        }
        taken.extend(a);
        taken.extend(b);
    }
    let claimed = taken.len();
    taken.sort();
    taken.dedup();
    assert_eq!(taken.len(), claimed, "a row was claimed twice");
    rows.sort();
    assert_eq!(taken, rows, "every ready row must be claimed");
}
