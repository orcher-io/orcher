//! Does an execution bind to the release that first ran it — and stay bound?
//!
//! The binding is the foundation the rest of versioning sits on. Its whole value
//! is the "never rebind" half: an execution replays its journal, so it must keep
//! running against the code it started on. A binding that silently moves when a
//! newer worker claims the execution would be worse than none, because it would
//! look like protection while providing none.
//!
//! Requires a live database:
//!   DATABASE_URL=postgres://…/orcher cargo test -p orcher-core --test execution_version_binding

use std::sync::Arc;

use orcher_storage::repositories::domains::workflow_execution::WorkflowExecutionRepository;
use sqlx::PgPool;
use uuid::Uuid;

async fn pool() -> Arc<PgPool> {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/orcher".to_string());
    Arc::new(
        PgPool::connect(&url)
            .await
            .expect("live database required; set DATABASE_URL"),
    )
}

/// A claimable execution on its own queue, so parallel tests do not race for it.
async fn seed(pool: &PgPool, queue: &str) -> Uuid {
    let id = Uuid::new_v4();
    sqlx::query(
        r#"INSERT INTO workflow_executions
             (id, execution_id, workflow_id, workflow_name, workflow_version, namespace,
              task_queue, status, trigger_type, submitted_at, created_at, updated_at)
           VALUES ($1, $2, $1, 'binding-test', '1.0.0', 'default', $3, 'pending', 'api',
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
    sqlx::query_scalar::<_, Option<String>>(
        "SELECT bound_version_id FROM workflow_executions WHERE id = $1",
    )
    .bind(id)
    .fetch_one(pool)
    .await
    .expect("read binding")
}

/// Release the claim so the same execution can be claimed again, standing in for
/// a worker that finished its slice and left the workflow still running.
async fn release_claim(pool: &PgPool, id: Uuid) {
    sqlx::query(
        "UPDATE workflow_executions SET claimed_by = NULL, claimed_at = NULL, \
         claim_timeout_at = NULL, status = 'running' WHERE id = $1",
    )
    .bind(id)
    .execute(pool)
    .await
    .expect("release claim");
}

async fn cleanup(pool: &PgPool, id: Uuid) {
    let _ = sqlx::query("DELETE FROM workflow_executions WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await;
}

#[tokio::test]
async fn first_claim_binds_the_execution_to_the_worker_release() {
    let pool = pool().await;
    let queue = format!("bind-a-{}", Uuid::new_v4());
    let id = seed(&pool, &queue).await;

    let repo = WorkflowExecutionRepository::new((*pool).clone());
    let claimed = repo
        .claim_and_get_next(&queue, "default", "worker-1", 60, Some("release-abc"), None)
        .await
        .expect("claim");

    assert!(claimed.is_some(), "the execution should have been claimed");
    assert_eq!(
        bound_version(&pool, id).await.as_deref(),
        Some("release-abc"),
        "first claim did not record the worker's release"
    );

    cleanup(&pool, id).await;
}

/// The half that matters. A later claim by a different release must not move an
/// in-flight execution onto new code.
#[tokio::test]
async fn a_later_claim_by_a_different_release_does_not_rebind() {
    let pool = pool().await;
    let queue = format!("bind-b-{}", Uuid::new_v4());
    let id = seed(&pool, &queue).await;
    let repo = WorkflowExecutionRepository::new((*pool).clone());

    repo.claim_and_get_next(&queue, "default", "worker-old", 60, Some("release-1"), None)
        .await
        .expect("first claim");
    assert_eq!(bound_version(&pool, id).await.as_deref(), Some("release-1"));

    release_claim(&pool, id).await;

    repo.claim_and_get_next(&queue, "default", "worker-new", 60, Some("release-2"), None)
        .await
        .expect("second claim");

    assert_eq!(
        bound_version(&pool, id).await.as_deref(),
        Some("release-1"),
        "the execution was rebound to a newer release — replay would then run \
         against code the journal was not written by"
    );

    cleanup(&pool, id).await;
}

/// A worker that declares nothing must not clear an existing binding either.
#[tokio::test]
async fn a_worker_without_a_release_does_not_clear_the_binding() {
    let pool = pool().await;
    let queue = format!("bind-c-{}", Uuid::new_v4());
    let id = seed(&pool, &queue).await;
    let repo = WorkflowExecutionRepository::new((*pool).clone());

    repo.claim_and_get_next(&queue, "default", "worker-1", 60, Some("release-1"), None)
        .await
        .expect("first claim");
    release_claim(&pool, id).await;

    repo.claim_and_get_next(&queue, "default", "worker-2", 60, None, None)
        .await
        .expect("second claim");

    assert_eq!(
        bound_version(&pool, id).await.as_deref(),
        Some("release-1"),
        "an undeclared worker erased the binding"
    );

    cleanup(&pool, id).await;
}

/// Today's behaviour: no worker declares a release, so nothing binds and claims
/// work exactly as before.
#[tokio::test]
async fn without_a_declared_release_nothing_is_bound() {
    let pool = pool().await;
    let queue = format!("bind-d-{}", Uuid::new_v4());
    let id = seed(&pool, &queue).await;
    let repo = WorkflowExecutionRepository::new((*pool).clone());

    let claimed = repo
        .claim_and_get_next(&queue, "default", "worker-1", 60, None, None)
        .await
        .expect("claim");

    assert!(claimed.is_some(), "claiming must still work");
    assert_eq!(
        bound_version(&pool, id).await,
        None,
        "an execution was bound even though the worker declared nothing"
    );

    cleanup(&pool, id).await;
}

// ── routing ────────────────────────────────────────────────────────────────
//
// Binding is only worth having if the server refuses to hand a bound execution
// to a worker on a different release. These drive that directly.

/// The case the whole feature exists for: old release retired, workflow wakes
/// up, only new workers are running. It must not be handed over.
#[tokio::test]
async fn a_bound_execution_is_not_claimable_by_a_different_release() {
    let pool = pool().await;
    let queue = format!("route-a-{}", Uuid::new_v4());
    let id = seed(&pool, &queue).await;
    let repo = WorkflowExecutionRepository::new((*pool).clone());

    repo.claim_and_get_next(&queue, "default", "worker-old", 60, Some("release-1"), None)
        .await
        .expect("first claim");
    release_claim(&pool, id).await;

    let claimed = repo
        .claim_and_get_next(&queue, "default", "worker-new", 60, Some("release-2"), None)
        .await
        .expect("claim call should succeed even when it yields nothing");

    assert!(
        claimed.is_none(),
        "a worker on release-2 claimed an execution bound to release-1 — it would \
         now replay against code its journal was not written by"
    );

    cleanup(&pool, id).await;
}

/// The other half: the right release must still get it, or this is just breakage.
#[tokio::test]
async fn a_bound_execution_is_claimable_by_its_own_release() {
    let pool = pool().await;
    let queue = format!("route-b-{}", Uuid::new_v4());
    let id = seed(&pool, &queue).await;
    let repo = WorkflowExecutionRepository::new((*pool).clone());

    repo.claim_and_get_next(&queue, "default", "worker-1", 60, Some("release-1"), None)
        .await
        .expect("first claim");
    release_claim(&pool, id).await;

    let claimed = repo
        .claim_and_get_next(&queue, "default", "worker-2", 60, Some("release-1"), None)
        .await
        .expect("claim");

    assert!(
        claimed.is_some(),
        "the execution's own release could not reclaim it"
    );

    cleanup(&pool, id).await;
}

/// Unbound executions stay claimable by anyone. This is every execution today,
/// and it is also the escape hatch: not binding an execution lets any release
/// run it, without needing a fall-through timer.
#[tokio::test]
async fn an_unbound_execution_is_claimable_by_any_release() {
    let pool = pool().await;
    let queue = format!("route-c-{}", Uuid::new_v4());
    let id = seed(&pool, &queue).await;
    let repo = WorkflowExecutionRepository::new((*pool).clone());

    let claimed = repo
        .claim_and_get_next(&queue, "default", "worker-1", 60, Some("any-release"), None)
        .await
        .expect("claim");

    assert!(
        claimed.is_some(),
        "an unbound execution was refused; unbound must mean claimable by anyone"
    );

    cleanup(&pool, id).await;
}

/// A worker declaring nothing must not pick up work bound to a release — it
/// cannot be the release the execution needs.
#[tokio::test]
async fn an_undeclared_worker_cannot_claim_a_bound_execution() {
    let pool = pool().await;
    let queue = format!("route-d-{}", Uuid::new_v4());
    let id = seed(&pool, &queue).await;
    let repo = WorkflowExecutionRepository::new((*pool).clone());

    repo.claim_and_get_next(&queue, "default", "worker-1", 60, Some("release-1"), None)
        .await
        .expect("first claim");
    release_claim(&pool, id).await;

    let claimed = repo
        .claim_and_get_next(&queue, "default", "worker-2", 60, None, None)
        .await
        .expect("claim");

    assert!(
        claimed.is_none(),
        "a worker declaring no release claimed an execution bound to one"
    );

    cleanup(&pool, id).await;
}

/// Option A's cost is a stall, so the stall has to be visible. This is the query
/// an operator runs during an incident ("what is stuck, and on what release")
/// and before retiring an old release ("does anything still need it").
#[tokio::test]
async fn waiting_work_is_reported_per_release() {
    let pool = pool().await;
    let queue = format!("route-e-{}", Uuid::new_v4());
    let repo = WorkflowExecutionRepository::new((*pool).clone());

    // Claim each one while the previous is still held, so every claim binds the
    // execution just seeded. Releasing as we go would let a claim re-pick an
    // earlier execution instead, and the bindings would land on the wrong rows.
    let mut ids = Vec::new();
    for (release, count) in [("release-1", 2), ("release-2", 1)] {
        for _ in 0..count {
            ids.push(seed(&pool, &queue).await);
            repo.claim_and_get_next(&queue, "default", "w", 60, Some(release), None)
                .await
                .expect("claim");
        }
    }
    // Now they are all in flight and unclaimed — waiting for their release.
    for id in &ids {
        release_claim(&pool, *id).await;
    }

    let waiting = repo
        .releases_with_waiting_work(&queue, "default", None)
        .await
        .expect("query");

    assert_eq!(
        waiting,
        vec![("release-1".to_string(), 2), ("release-2".to_string(), 1)],
        "waiting work was not reported per release, ordered by how much is stuck"
    );

    for id in ids {
        cleanup(&pool, id).await;
    }
}

// ── every claim path, not just the one ─────────────────────────────────────
//
// The routing predicate first went into claim_and_get_next alone. A live test
// then showed a worker on the wrong release running a bound execution anyway,
// because the poller reaches the database through claim_batch_with_offset,
// which had neither the filter nor the binding. These cover each claim entry
// point so "routing is enforced" cannot again be true of only one of them.

/// The batch path is what the live poller actually uses.
#[tokio::test]
async fn the_batch_claim_refuses_a_different_release() {
    let pool = pool().await;
    let queue = format!("batch-a-{}", Uuid::new_v4());
    let id = seed(&pool, &queue).await;
    let repo = WorkflowExecutionRepository::new((*pool).clone());

    repo.claim_and_get_next(&queue, "default", "worker-1", 60, Some("release-1"), None)
        .await
        .expect("first claim");
    release_claim(&pool, id).await;

    let claimed = repo
        .claim_batch(
            &queue,
            "default",
            "worker-2",
            60,
            10,
            Some("release-2"),
            None,
        )
        .await
        .expect("batch claim");

    assert!(
        claimed.is_empty(),
        "the batch path handed a release-1 execution to a release-2 worker"
    );

    cleanup(&pool, id).await;
}

/// Positive control for the batch path.
#[tokio::test]
async fn the_batch_claim_allows_the_matching_release() {
    let pool = pool().await;
    let queue = format!("batch-b-{}", Uuid::new_v4());
    let id = seed(&pool, &queue).await;
    let repo = WorkflowExecutionRepository::new((*pool).clone());

    repo.claim_and_get_next(&queue, "default", "worker-1", 60, Some("release-1"), None)
        .await
        .expect("first claim");
    release_claim(&pool, id).await;

    let claimed = repo
        .claim_batch(
            &queue,
            "default",
            "worker-2",
            60,
            10,
            Some("release-1"),
            None,
        )
        .await
        .expect("batch claim");

    assert_eq!(
        claimed.len(),
        1,
        "the matching release could not batch-claim"
    );

    cleanup(&pool, id).await;
}

/// The batch path did not bind at all, so an execution first claimed in a batch
/// stayed unbound forever and every later release could pick it up.
#[tokio::test]
async fn the_batch_claim_binds_on_first_claim() {
    let pool = pool().await;
    let queue = format!("batch-c-{}", Uuid::new_v4());
    let id = seed(&pool, &queue).await;
    let repo = WorkflowExecutionRepository::new((*pool).clone());

    repo.claim_batch(
        &queue,
        "default",
        "worker-1",
        60,
        10,
        Some("release-batch"),
        None,
    )
    .await
    .expect("batch claim");

    assert_eq!(
        bound_version(&pool, id).await.as_deref(),
        Some("release-batch"),
        "a batch claim left the execution unbound"
    );

    cleanup(&pool, id).await;
}

/// The targeted continuation path.
#[tokio::test]
async fn the_by_id_claim_refuses_a_different_release() {
    let pool = pool().await;
    let queue = format!("byid-a-{}", Uuid::new_v4());
    let id = seed(&pool, &queue).await;
    let repo = WorkflowExecutionRepository::new((*pool).clone());

    let claimed = repo
        .claim_and_get_next(&queue, "default", "worker-1", 60, Some("release-1"), None)
        .await
        .expect("first claim")
        .expect("something to claim");
    let execution_id = claimed.execution_id.clone();
    release_claim(&pool, id).await;

    let mismatched = repo
        .claim_execution_by_id(&execution_id, "worker-2", 60, Some("release-2"), None)
        .await
        .expect("by-id claim");
    assert!(
        mismatched.is_none(),
        "the targeted path handed a release-1 execution to a release-2 worker"
    );

    let matching = repo
        .claim_execution_by_id(&execution_id, "worker-3", 60, Some("release-1"), None)
        .await
        .expect("by-id claim");
    assert!(
        matching.is_some(),
        "the targeted path refused the execution's own release"
    );

    cleanup(&pool, id).await;
}

// ── successor executions ───────────────────────────────────────────────────
//
// Three paths create a successor from an existing execution, and they must not
// all behave the same. The distinction is whether the successor inherits the
// journal:
//
//   reset          — replays a copied journal prefix  → MUST keep the pin
//   restart-fresh  — starts an empty journal          → must NOT keep it
//   workflow retry — starts an empty journal          → must NOT keep it
//
// A successor that replays inherited history under a different release is
// exactly the divergence binding exists to prevent, so reset losing the pin
// silently reopens the hole.

/// The pin must survive `create_with_outbox_task` — the function the reset path
/// actually calls to write its successor.
///
/// Deliberately exercises that function rather than issuing its own INSERT: the
/// bug was that the column was **absent from that statement**, so a pin set on
/// the struct was silently dropped. A test with its own INSERT would pass either
/// way and prove nothing.
#[tokio::test]
async fn a_successor_created_with_an_outbox_task_keeps_its_pin() {
    let pool = pool().await;
    let queue = format!("succ-{}", Uuid::new_v4());
    let repo = WorkflowExecutionRepository::new((*pool).clone());

    let mut successor = orcher_storage::schema::types::WorkflowExecution::new(
        "succ".to_string(),
        "1.0.0".to_string(),
        "default".to_string(),
        serde_json::json!({}),
    );
    successor.task_queue = queue.clone();
    successor.workflow_type = "succ".to_string();
    // What the reset path does: carry the pin of the run being continued.
    successor.bound_version_id = Some("release-1".to_string());

    let (created, _task_id) = repo
        .create_with_outbox_task(
            &successor,
            serde_json::json!({"task_queue": queue, "namespace": "default"}),
            format!("succ-{}", Uuid::new_v4()),
            &queue,
        )
        .await
        .expect("create successor with outbox task");

    assert_eq!(
        created.bound_version_id.as_deref(),
        Some("release-1"),
        "create_with_outbox_task dropped the pin — a reset successor would replay \
         its parent's journal on any release"
    );

    let persisted: Option<String> = sqlx::query_scalar(
        "SELECT bound_version_id FROM workflow_executions WHERE execution_id = $1",
    )
    .bind(&created.execution_id)
    .fetch_one(&*pool)
    .await
    .expect("read back");

    assert_eq!(
        persisted.as_deref(),
        Some("release-1"),
        "the pin was returned but not persisted"
    );

    let _ = sqlx::query("DELETE FROM workflow_executions WHERE task_queue = $1")
        .bind(&queue)
        .execute(&*pool)
        .await;
}
