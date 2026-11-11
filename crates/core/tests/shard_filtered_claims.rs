//! Does the shard filter actually filter?
//!
//! The unit tests cover the range maths and the bind values; nothing there
//! proves the SQL restricts what a claim returns. A single-node E2E cannot prove
//! it either — one node owns every shard, so the filter is a no-op and the run is
//! green whether the predicate works or not.
//!
//! These tests give a repository a scope that owns *part* of the space, put rows
//! on both sides of the boundary, and assert what comes back. They are the reason
//! to believe the filter is load-bearing rather than decorative.
//!
//! Requires PostgreSQL. Run with:
//!
//! ```bash
//! export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/orcher_test"
//! cargo test -p orcher-core --test shard_filtered_claims -- --ignored --test-threads=1
//! ```

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use orcher_storage::repositories::domains::coordination::OutboxRepository;
use orcher_storage::schema::types::coordination::{OutboxTask, OutboxTaskType};
use orcher_storage::sharding::ClaimScope;
use sqlx::PgPool;
use uuid::Uuid;

/// A scope with a fixed answer, so a test can place the ownership boundary
/// exactly where it wants it.
#[derive(Debug)]
struct FixedScope {
    ranges: Vec<(i32, i32)>,
    sweep_every: u32,
    polls: AtomicU32,
}

impl FixedScope {
    /// Owns `ranges`, never sweeps — so a test sees the filter in isolation
    /// rather than the filter plus its escape hatch.
    fn owning(ranges: Vec<(i32, i32)>) -> Arc<dyn ClaimScope> {
        Arc::new(FixedScope {
            ranges,
            sweep_every: 0,
            polls: AtomicU32::new(0),
        })
    }

    fn sweeping_every(ranges: Vec<(i32, i32)>, n: u32) -> Arc<dyn ClaimScope> {
        Arc::new(FixedScope {
            ranges,
            sweep_every: n,
            polls: AtomicU32::new(0),
        })
    }
}

impl ClaimScope for FixedScope {
    fn owned_ranges(&self) -> Vec<(i32, i32)> {
        self.ranges.clone()
    }
    fn sweep_interval(&self) -> u32 {
        self.sweep_every
    }
    fn should_sweep(&self) -> bool {
        match self.sweep_every {
            0 => false,
            1 => true,
            n => self.polls.fetch_add(1, Ordering::Relaxed).wrapping_add(1) % n == 0,
        }
    }
}

async fn pool() -> Arc<PgPool> {
    let url = std::env::var("TEST_DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:postgres@localhost:5432/orcher_test".to_string()
    });
    Arc::new(
        PgPool::connect(&url)
            .await
            .expect("connect to the test database"),
    )
}

/// Insert a pending outbox row carrying `shard_id`, owned by `org`.
///
/// Every test uses its own `organization_id` and claims with it. The claim query
/// filters on that, so a test sees only its own rows — without it these tests
/// compete with whatever else is pending in a shared development database and
/// fail for reasons that have nothing to do with sharding.
async fn seed(pool: &PgPool, org: Uuid, shard_id: Option<i32>) -> Uuid {
    let task_id = Uuid::new_v4();
    sqlx::query(
        r#"INSERT INTO outbox_queue
             (task_id, organization_id, workflow_id, task_type, payload, idempotency_key,
              status, created_at, attempt_count, max_retries, backoff_multiplier, shard_id)
           VALUES ($1, $2, $3, 'dispatch_task', $4, $5, 'pending', NOW(), 0, 3, 2.0, $6)"#,
    )
    .bind(task_id)
    .bind(org)
    .bind(Uuid::new_v4())
    .bind(serde_json::json!({ "org": org.to_string() }))
    .bind(format!("{org}:{task_id}"))
    .bind(shard_id)
    .execute(pool)
    .await
    .expect("seed row");
    task_id
}

/// `outbox_queue.organization_id` carries a foreign key, so the tenant has to
/// exist before its rows can.
async fn create_org(pool: &PgPool, org: Uuid) {
    sqlx::query(
        "INSERT INTO organizations (id, name, display_name, slug, owner_id) \
         VALUES ($1, $2, $2, $2, $3)",
    )
    .bind(org)
    .bind(format!("shardfilter-{org}"))
    .bind(Uuid::new_v4()) // owner_id is NOT NULL but unconstrained; no user needed
    .execute(pool)
    .await
    .expect("create test organization");
}

async fn cleanup(pool: &PgPool, org: Uuid) {
    let _ = sqlx::query("DELETE FROM outbox_queue WHERE organization_id = $1")
        .bind(org)
        .execute(pool)
        .await;
    let _ = sqlx::query("DELETE FROM organizations WHERE id = $1")
        .bind(org)
        .execute(pool)
        .await;
}

/// Claim repeatedly until drained, returning the shard of every row claimed.
async fn drain(repo: &OutboxRepository, org: Uuid, rounds: usize) -> Vec<Option<i32>> {
    let mut seen = Vec::new();
    for _ in 0..rounds {
        let batch = repo
            .claim_tasks_excluding_types(Some(org), 50, &[])
            .await
            .expect("claim");
        if batch.is_empty() {
            break;
        }
        seen.extend(batch.iter().map(|t| t.shard_id));
    }
    seen
}

#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_node_claims_only_rows_on_the_shards_it_owns() {
    let pool = pool().await;
    let org = Uuid::new_v4();
    create_org(&pool, org).await;

    // Two rows inside the owned range, two well outside it.
    seed(&pool, org, Some(10)).await;
    seed(&pool, org, Some(500)).await;
    seed(&pool, org, Some(3000)).await;
    seed(&pool, org, Some(4095)).await;

    let repo = OutboxRepository::new(pool.clone());
    repo.set_claim_scope(FixedScope::owning(vec![(0, 999)]));

    let claimed = drain(&repo, org, 10).await;
    let ours: Vec<i32> = claimed
        .iter()
        .flatten()
        .copied()
        .filter(|s| *s == 10 || *s == 500 || *s == 3000 || *s == 4095)
        .collect();
    cleanup(&pool, org).await;

    assert!(
        ours.contains(&10) && ours.contains(&500),
        "owned shards must be claimed, got {ours:?}"
    );
    assert!(
        !ours.contains(&3000) && !ours.contains(&4095),
        "shards outside the owned range must NOT be claimed — the filter is not \
         restricting anything. got {ours:?}"
    );
}

/// Rows written before `shard_id` was populated must stay claimable, or an
/// upgrade strands whatever was already in the queue.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn rows_without_a_shard_are_still_claimable() {
    let pool = pool().await;
    let org = Uuid::new_v4();
    create_org(&pool, org).await;

    seed(&pool, org, None).await;

    let repo = OutboxRepository::new(pool.clone());
    // An owned range that deliberately excludes everything else.
    repo.set_claim_scope(FixedScope::owning(vec![(0, 0)]));

    let claimed = drain(&repo, org, 5).await;
    cleanup(&pool, org).await;

    assert!(
        claimed.iter().any(|s| s.is_none()),
        "a NULL-shard row was not claimed; upgrading would strand every pre-existing row"
    );
}

/// The stall guard: a row nobody owns must still be picked up by the periodic
/// unfiltered sweep, so a wrong membership view degrades throughput instead of
/// silently halting work.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn the_sweep_eventually_claims_a_row_no_one_owns() {
    let pool = pool().await;
    let org = Uuid::new_v4();
    create_org(&pool, org).await;

    seed(&pool, org, Some(2500)).await;

    let repo = OutboxRepository::new(pool.clone());
    // Owns a range that excludes 2500, but sweeps every 3rd poll.
    repo.set_claim_scope(FixedScope::sweeping_every(vec![(0, 99)], 3));

    let mut found = false;
    for _ in 0..12 {
        let batch = repo
            .claim_tasks_excluding_types(Some(org), 50, &[])
            .await
            .expect("claim");
        if batch.iter().any(|t| t.shard_id == Some(2500)) {
            found = true;
            break;
        }
    }
    cleanup(&pool, org).await;

    assert!(
        found,
        "an unowned row was never claimed — without the sweep, a stale membership \
         view strands rows indefinitely"
    );
}

/// With no scope injected the repository must behave exactly as before, or every
/// single-node deployment changes behaviour on upgrade.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn without_a_scope_every_shard_is_claimable() {
    let pool = pool().await;
    let org = Uuid::new_v4();
    create_org(&pool, org).await;

    seed(&pool, org, Some(4000)).await;

    let repo = OutboxRepository::new(pool.clone());
    // No set_claim_scope call at all.

    let claimed = drain(&repo, org, 10).await;
    cleanup(&pool, org).await;

    assert!(
        claimed.iter().flatten().any(|s| *s == 4000),
        "an unfiltered repository must claim any shard"
    );
}

// Keeps the unused-import warning honest if OutboxTask/OutboxTaskType stop being
// needed by the seeding helper.
#[allow(dead_code)]
fn _type_anchor(_: OutboxTask, _: OutboxTaskType) {}
