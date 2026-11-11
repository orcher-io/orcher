//! Where does NEW work go while two releases are deployed side by side?
//!
//! Binding decides where an execution that has already started must run. It says
//! nothing about a brand-new execution: unbound work is claimable by anyone, so
//! during a rollout it lands on whichever worker polls first. That is safe, but
//! it makes a rollout unsteerable and retirement non-monotonic — new work keeps
//! binding to the outgoing release until its workers stop.
//!
//! A queue's *current release* fixes that. These tests cover both directions,
//! including the default (no policy set), which must behave exactly as before.
//!
//! Requires a live database:
//!   DATABASE_URL=postgres://…/orcher cargo test -p orcher-core --test current_release_routing

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

async fn seed(pool: &PgPool, queue: &str) -> Uuid {
    let id = Uuid::new_v4();
    sqlx::query(
        r#"INSERT INTO workflow_executions
             (id, execution_id, workflow_id, workflow_name, workflow_type, workflow_version,
              namespace, task_queue, status, trigger_type, submitted_at, created_at, updated_at)
           VALUES ($1, $2, $1, 'release-test', 'release-test', '1.0.0', 'default', $3,
                   'pending', 'api', NOW(), NOW(), NOW())"#,
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

async fn cleanup(pool: &PgPool, id: Uuid, queue: &str) {
    let _ = sqlx::query("DELETE FROM workflow_executions WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await;
    let _ = sqlx::query("DELETE FROM task_queue_releases WHERE task_queue = $1")
        .bind(queue)
        .execute(pool)
        .await;
}

/// The rollout case: v1 workers are still up, but new work must go to v2.
#[tokio::test]
async fn new_work_goes_only_to_the_current_release() {
    let pool = pool().await;
    let queue = format!("cur-a-{}", Uuid::new_v4());
    let id = seed(&pool, &queue).await;
    let repo = WorkflowExecutionRepository::new((*pool).clone());

    repo.set_current_release(&queue, "default", Some("release-2"), None)
        .await
        .expect("set current release");

    let old = repo
        .claim_and_get_next(&queue, "default", "worker-old", 60, Some("release-1"), None)
        .await
        .expect("claim");
    assert!(
        old.is_none(),
        "a worker on the outgoing release took new work after the current release moved on"
    );

    let new = repo
        .claim_and_get_next(&queue, "default", "worker-new", 60, Some("release-2"), None)
        .await
        .expect("claim");
    assert!(new.is_some(), "the current release could not take new work");
    assert_eq!(bound_version(&pool, id).await.as_deref(), Some("release-2"));

    cleanup(&pool, id, &queue).await;
}

/// The default. No policy means any release may take new work — today's
/// behaviour, and what every existing queue keeps doing.
#[tokio::test]
async fn without_a_current_release_any_worker_may_take_new_work() {
    let pool = pool().await;
    let queue = format!("cur-b-{}", Uuid::new_v4());
    let id = seed(&pool, &queue).await;
    let repo = WorkflowExecutionRepository::new((*pool).clone());

    let claimed = repo
        .claim_and_get_next(&queue, "default", "worker", 60, Some("anything"), None)
        .await
        .expect("claim");

    assert!(
        claimed.is_some(),
        "a queue with no release policy refused new work"
    );

    cleanup(&pool, id, &queue).await;
}

/// A worker that declares nothing must not take new work on a queue that has
/// named a current release — it cannot be that release.
#[tokio::test]
async fn an_undeclared_worker_cannot_take_new_work_from_a_pinned_queue() {
    let pool = pool().await;
    let queue = format!("cur-c-{}", Uuid::new_v4());
    let id = seed(&pool, &queue).await;
    let repo = WorkflowExecutionRepository::new((*pool).clone());

    repo.set_current_release(&queue, "default", Some("release-2"), None)
        .await
        .expect("set current release");

    let claimed = repo
        .claim_and_get_next(&queue, "default", "worker", 60, None, None)
        .await
        .expect("claim");

    assert!(
        claimed.is_none(),
        "a worker declaring no release took new work from a queue pinned to one"
    );

    cleanup(&pool, id, &queue).await;
}

/// The batch path is the one the live poller uses, so the rule has to hold there
/// too — this is exactly where the original routing filter was missing.
#[tokio::test]
async fn the_batch_path_also_honours_the_current_release() {
    let pool = pool().await;
    let queue = format!("cur-d-{}", Uuid::new_v4());
    let id = seed(&pool, &queue).await;
    let repo = WorkflowExecutionRepository::new((*pool).clone());

    repo.set_current_release(&queue, "default", Some("release-2"), None)
        .await
        .expect("set current release");

    let old = repo
        .claim_batch(
            &queue,
            "default",
            "worker-old",
            60,
            10,
            Some("release-1"),
            None,
        )
        .await
        .expect("batch claim");
    assert!(
        old.is_empty(),
        "the batch path handed new work to the outgoing release"
    );

    let new = repo
        .claim_batch(
            &queue,
            "default",
            "worker-new",
            60,
            10,
            Some("release-2"),
            None,
        )
        .await
        .expect("batch claim");
    assert_eq!(
        new.len(),
        1,
        "the current release could not batch-claim new work"
    );

    cleanup(&pool, id, &queue).await;
}

/// Clearing the policy restores the default.
#[tokio::test]
async fn clearing_the_current_release_restores_open_claiming() {
    let pool = pool().await;
    let queue = format!("cur-e-{}", Uuid::new_v4());
    let id = seed(&pool, &queue).await;
    let repo = WorkflowExecutionRepository::new((*pool).clone());

    repo.set_current_release(&queue, "default", Some("release-2"), None)
        .await
        .expect("set");
    assert_eq!(
        repo.current_release(&queue, "default", None)
            .await
            .expect("read"),
        Some("release-2".to_string())
    );

    repo.set_current_release(&queue, "default", None, None)
        .await
        .expect("clear");
    assert_eq!(
        repo.current_release(&queue, "default", None)
            .await
            .expect("read"),
        None
    );

    let claimed = repo
        .claim_and_get_next(&queue, "default", "worker", 60, Some("release-1"), None)
        .await
        .expect("claim");
    assert!(
        claimed.is_some(),
        "clearing the policy did not reopen claiming"
    );

    cleanup(&pool, id, &queue).await;
}

/// A blank release is not a release. Storing one would pin the queue to a
/// release named "" and nothing could ever claim new work again.
#[tokio::test]
async fn a_blank_current_release_is_stored_as_no_policy() {
    let pool = pool().await;
    let queue = format!("cur-f-{}", Uuid::new_v4());
    let id = seed(&pool, &queue).await;
    let repo = WorkflowExecutionRepository::new((*pool).clone());

    repo.set_current_release(&queue, "default", Some("   "), None)
        .await
        .expect("set blank");

    assert_eq!(
        repo.current_release(&queue, "default", None)
            .await
            .expect("read"),
        None
    );
    let claimed = repo
        .claim_and_get_next(&queue, "default", "worker", 60, Some("release-1"), None)
        .await
        .expect("claim");
    assert!(
        claimed.is_some(),
        "a blank release policy blocked all new work"
    );

    cleanup(&pool, id, &queue).await;
}

/// An execution already bound keeps its release regardless of the queue policy.
/// The current release decides where NEW work goes, never where running work
/// continues — that distinction is the whole point.
#[tokio::test]
async fn a_bound_execution_ignores_the_current_release() {
    let pool = pool().await;
    let queue = format!("cur-g-{}", Uuid::new_v4());
    let id = seed(&pool, &queue).await;
    let repo = WorkflowExecutionRepository::new((*pool).clone());

    // Bind it to release-1, then move the queue on to release-2.
    repo.claim_and_get_next(&queue, "default", "w1", 60, Some("release-1"), None)
        .await
        .expect("first claim");
    sqlx::query(
        "UPDATE workflow_executions SET claimed_by = NULL, claimed_at = NULL, status = 'running' \
         WHERE id = $1",
    )
    .bind(id)
    .execute(&*pool)
    .await
    .expect("release claim");
    repo.set_current_release(&queue, "default", Some("release-2"), None)
        .await
        .expect("set current release");

    let its_own = repo
        .claim_and_get_next(&queue, "default", "w2", 60, Some("release-1"), None)
        .await
        .expect("claim");
    assert!(
        its_own.is_some(),
        "an in-flight execution was cut off from its own release by a queue policy"
    );

    cleanup(&pool, id, &queue).await;
}

// ── the escape hatch ───────────────────────────────────────────────────────
//
// Routing introduces exactly one failure mode: an execution bound to a release
// nobody runs any more waits indefinitely. Starting a worker on that release is
// the first answer; rebinding is for when that build is gone.

/// Move a stalled execution onto a release that is actually running.
#[tokio::test]
async fn rebinding_moves_a_stalled_execution_to_a_live_release() {
    let pool = pool().await;
    let queue = format!("rebind-a-{}", Uuid::new_v4());
    let id = seed(&pool, &queue).await;
    let repo = WorkflowExecutionRepository::new((*pool).clone());

    // Bound to a release we are about to retire, and released mid-flight.
    repo.claim_and_get_next(&queue, "default", "w1", 60, Some("retired"), None)
        .await
        .expect("first claim");
    sqlx::query(
        "UPDATE workflow_executions SET claimed_by = NULL, claimed_at = NULL, status = 'running' \
         WHERE id = $1",
    )
    .bind(id)
    .execute(&*pool)
    .await
    .expect("release claim");

    let execution_id = id.to_string();
    let stuck = repo
        .claim_and_get_next(&queue, "default", "w2", 60, Some("live"), None)
        .await
        .expect("claim");
    assert!(stuck.is_none(), "precondition: it should be stalled");

    let rebound = repo
        .rebind_execution(&execution_id, Some("live"), None)
        .await
        .expect("rebind");
    assert!(rebound, "rebind reported nothing to move");
    assert_eq!(bound_version(&pool, id).await.as_deref(), Some("live"));

    let now = repo
        .claim_and_get_next(&queue, "default", "w3", 60, Some("live"), None)
        .await
        .expect("claim");
    assert!(
        now.is_some(),
        "the execution is still stuck after rebinding"
    );

    cleanup(&pool, id, &queue).await;
}

/// Unbinding reopens it to any release.
#[tokio::test]
async fn rebinding_to_none_unbinds_the_execution() {
    let pool = pool().await;
    let queue = format!("rebind-b-{}", Uuid::new_v4());
    let id = seed(&pool, &queue).await;
    let repo = WorkflowExecutionRepository::new((*pool).clone());

    repo.claim_and_get_next(&queue, "default", "w1", 60, Some("retired"), None)
        .await
        .expect("first claim");
    sqlx::query(
        "UPDATE workflow_executions SET claimed_by = NULL, claimed_at = NULL, status = 'running' \
         WHERE id = $1",
    )
    .bind(id)
    .execute(&*pool)
    .await
    .expect("release claim");

    repo.rebind_execution(&id.to_string(), None, None)
        .await
        .expect("unbind");

    assert_eq!(bound_version(&pool, id).await, None);

    cleanup(&pool, id, &queue).await;
}

/// A finished workflow has nothing left to run, so rebinding it is refused
/// rather than silently rewriting a terminal row.
#[tokio::test]
async fn rebinding_a_terminal_execution_is_refused() {
    let pool = pool().await;
    let queue = format!("rebind-c-{}", Uuid::new_v4());
    let id = seed(&pool, &queue).await;
    let repo = WorkflowExecutionRepository::new((*pool).clone());

    sqlx::query("UPDATE workflow_executions SET status = 'completed' WHERE id = $1")
        .bind(id)
        .execute(&*pool)
        .await
        .expect("complete it");

    let rebound = repo
        .rebind_execution(&id.to_string(), Some("live"), None)
        .await
        .expect("rebind call");

    assert!(!rebound, "a completed execution was rebound");

    cleanup(&pool, id, &queue).await;
}

// ─── Tenancy ─────────────────────────────────────────────────────────────────
//
// A current release used to be keyed by namespace *name* and task queue alone.
// Namespace names are unique within an organisation, not across a deployment,
// so two tenants each running a namespace called 'default' shared one policy
// row — and pointing a queue at a release is precisely how a deploy drains the
// previous one. One tenant finishing a rollout therefore pinned the other
// tenant's queue to a release that tenant had never heard of and had no workers
// for. Their new executions bound to it and were never claimed: no error, no
// log above debug, work simply stopped.

async fn seed_org(pool: &PgPool, label: &str) -> Uuid {
    let id = Uuid::new_v4();
    // `organizations.owner_id` is NOT NULL, so an organisation cannot exist
    // without a user to own it.
    let owner = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO users (id, email, password_hash, hash_algorithm, password_changed_at) \
         VALUES ($1, $2, 'x', 'argon2', NOW())",
    )
    .bind(owner)
    .bind(format!("{label}-{id}@contract.test"))
    .execute(pool)
    .await
    .expect("seed owner");

    sqlx::query(
        "INSERT INTO organizations (id, name, display_name, slug, owner_id) \
         VALUES ($1, $2, $2, $3, $4)",
    )
    .bind(id)
    .bind(format!("{label}-{id}"))
    .bind(format!("{label}-{id}"))
    .bind(owner)
    .execute(pool)
    .await
    .expect("seed organization");
    id
}

async fn seed_owned(pool: &PgPool, queue: &str, organization_id: Uuid) -> Uuid {
    let id = seed(pool, queue).await;
    sqlx::query("UPDATE workflow_executions SET organization_id = $1 WHERE id = $2")
        .bind(organization_id)
        .bind(id)
        .execute(pool)
        .await
        .expect("stamp owner");
    id
}

async fn cleanup_org(pool: &PgPool, organization_id: Uuid) {
    let owner: Option<Uuid> =
        sqlx::query_scalar("SELECT owner_id FROM organizations WHERE id = $1")
            .bind(organization_id)
            .fetch_optional(pool)
            .await
            .ok()
            .flatten();
    let _ = sqlx::query("DELETE FROM organizations WHERE id = $1")
        .bind(organization_id)
        .execute(pool)
        .await;
    if let Some(owner) = owner {
        let _ = sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(owner)
            .execute(pool)
            .await;
    }
}

/// Two organisations, one queue name: each keeps its own policy.
#[tokio::test]
async fn a_current_release_is_scoped_to_one_organisation() {
    let pool = pool().await;
    let queue = format!("cur-tenant-{}", Uuid::new_v4());
    let alpha = seed_org(&pool, "alpha").await;
    let beta = seed_org(&pool, "beta").await;
    let repo = WorkflowExecutionRepository::new((*pool).clone());

    repo.set_current_release(&queue, "default", Some("alpha-release"), Some(alpha))
        .await
        .expect("set alpha policy");
    repo.set_current_release(&queue, "default", Some("beta-release"), Some(beta))
        .await
        .expect("set beta policy");

    // Before the owner was part of the key, the second write replaced the
    // first and both reads returned "beta-release".
    assert_eq!(
        repo.current_release(&queue, "default", Some(alpha))
            .await
            .expect("read alpha"),
        Some("alpha-release".to_string()),
    );
    assert_eq!(
        repo.current_release(&queue, "default", Some(beta))
            .await
            .expect("read beta"),
        Some("beta-release".to_string()),
    );

    cleanup_org(&pool, alpha).await;
    cleanup_org(&pool, beta).await;
    let _ = sqlx::query("DELETE FROM task_queue_releases WHERE task_queue = $1")
        .bind(&queue)
        .execute(&*pool)
        .await;
}

/// The consequence, seen from the claim path: one tenant's policy must not
/// decide where another tenant's new work may be claimed.
#[tokio::test]
async fn one_tenants_release_policy_does_not_pin_another_tenants_work() {
    let pool = pool().await;
    let queue = format!("cur-tenant-claim-{}", Uuid::new_v4());
    let alpha = seed_org(&pool, "alpha").await;
    let beta = seed_org(&pool, "beta").await;
    let repo = WorkflowExecutionRepository::new((*pool).clone());

    // Alpha pins the queue to a release only alpha runs.
    repo.set_current_release(&queue, "default", Some("alpha-only"), Some(alpha))
        .await
        .expect("set alpha policy");

    // Beta has no policy, so any of beta's releases may take beta's new work.
    let beta_execution = seed_owned(&pool, &queue, beta).await;
    let claimed = repo
        .claim_and_get_next(
            &queue,
            "default",
            "beta-worker",
            60,
            Some("beta-release"),
            None,
        )
        .await
        .expect("claim");

    assert!(
        claimed.is_some(),
        "beta's work was not claimable: alpha's policy is still deciding it",
    );
    assert_eq!(
        claimed.map(|e| e.execution_id),
        Some(beta_execution.to_string()),
    );

    cleanup(&pool, beta_execution, &queue).await;
    cleanup_org(&pool, alpha).await;
    cleanup_org(&pool, beta).await;
}
