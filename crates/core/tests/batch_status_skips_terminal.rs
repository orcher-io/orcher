//! Can a batch status update overwrite a workflow that already finished?
//!
//! `batch_update_status` selects its targets in a subquery and updates the rows
//! that come back. Under READ COMMITTED, a row changed by a transaction that
//! commits after the statement's snapshot is re-checked against the command's
//! WHERE using the NEW row version — but a subquery in that WHERE still reflects
//! the ORIGINAL snapshot. So a guard living only inside the subquery does not
//! protect a workflow that completes mid-statement: it still satisfies
//! `execution_id IN (…)` and gets overwritten, losing its result.
//!
//! The predicate therefore also sits in the outer WHERE. These tests pin the
//! observable guarantee — terminal executions are never modified by a batch —
//! plus the positive control, since a filter that matched nothing would satisfy
//! the negative cases while making the operation useless.
//!
//! Requires a live database:
//!   DATABASE_URL=postgres://…/orcher cargo test -p orcher-core --test batch_status_skips_terminal

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

/// An organisation, with the user it must be owned by.
async fn an_organisation(pool: &PgPool) -> Uuid {
    let id = Uuid::new_v4();
    let slug = format!("batch-{}", &id.to_string()[..8]);
    let owner = Uuid::new_v4();
    sqlx::query("INSERT INTO users (id, email, password_hash) VALUES ($1, $2, 'x')")
        .bind(owner)
        .bind(format!("{slug}@example.test"))
        .execute(pool)
        .await
        .expect("seed user");
    sqlx::query(
        "INSERT INTO organizations (id, name, display_name, slug, owner_id) \
         VALUES ($1, $2, $2, $3, $4)",
    )
    .bind(id)
    .bind(&slug)
    .bind(&slug)
    .bind(owner)
    .execute(pool)
    .await
    .expect("seed organization");
    id
}

/// A workflow belonging to a named organisation, in a shared namespace name.
async fn seed_owned(pool: &PgPool, namespace: &str, organization_id: Uuid) -> Uuid {
    let id = Uuid::new_v4();
    sqlx::query(
        r#"INSERT INTO workflow_executions
             (id, execution_id, workflow_id, workflow_name, workflow_type, workflow_version,
              namespace, task_queue, status, trigger_type, organization_id,
              submitted_at, created_at, updated_at)
           VALUES ($1, $2, $1, 'batch-test', 'batch-test', '1.0.0', $3, 'batch-q',
                   'running', 'api', $4, NOW(), NOW(), NOW())"#,
    )
    .bind(id)
    .bind(format!("batch-{id}"))
    .bind(namespace)
    .bind(organization_id)
    .execute(pool)
    .await
    .expect("seed owned workflow");
    id
}

/// Each test uses its own namespace so a batch cannot touch another test's rows.
async fn seed(pool: &PgPool, namespace: &str, status: &str) -> Uuid {
    let id = Uuid::new_v4();
    sqlx::query(
        r#"INSERT INTO workflow_executions
             (id, execution_id, workflow_id, workflow_name, workflow_type, workflow_version,
              namespace, task_queue, status, trigger_type, submitted_at, created_at, updated_at)
           VALUES ($1, $2, $1, 'batch-test', 'batch-test', '1.0.0', $3, 'batch-q',
                   $4, 'api', NOW(), NOW(), NOW())"#,
    )
    .bind(id)
    .bind(format!("batch-{id}"))
    .bind(namespace)
    .bind(status)
    .execute(pool)
    .await
    .expect("seed workflow");
    id
}

async fn status_of(pool: &PgPool, id: Uuid) -> String {
    sqlx::query_scalar::<_, String>("SELECT status FROM workflow_executions WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("read status")
}

async fn cleanup(pool: &PgPool, namespace: &str) {
    let _ = sqlx::query("DELETE FROM workflow_executions WHERE namespace = $1")
        .bind(namespace)
        .execute(pool)
        .await;
}

/// A completed workflow must survive a batch that sweeps its namespace.
#[tokio::test]
async fn a_batch_does_not_overwrite_completed_executions() {
    let pool = pool().await;
    let ns = format!("batch-a-{}", Uuid::new_v4());
    let done = seed(&pool, &ns, "completed").await;
    let running = seed(&pool, &ns, "running").await;
    let repo = WorkflowExecutionRepository::new((*pool).clone());

    let updated = repo
        .batch_update_status(
            &ns,
            None,
            None,
            None,
            None,
            None,
            None,
            "cancelled",
            None,
            100,
        )
        .await
        .expect("batch update");

    assert_eq!(
        status_of(&pool, done).await,
        "completed",
        "a completed workflow was swept into 'cancelled' by a batch operation"
    );
    assert_eq!(
        status_of(&pool, running).await,
        "cancelled",
        "the running workflow should have been updated"
    );
    assert_eq!(
        updated.len(),
        1,
        "only the running execution should be returned"
    );

    cleanup(&pool, &ns).await;
}

/// Every terminal state, so the guard is not accidentally specific to one.
#[tokio::test]
async fn a_batch_skips_all_terminal_states() {
    let pool = pool().await;
    let ns = format!("batch-b-{}", Uuid::new_v4());
    let repo = WorkflowExecutionRepository::new((*pool).clone());

    let mut seeded = Vec::new();
    for terminal in [
        "completed",
        "failed",
        "cancelled",
        "terminated",
        "timed_out",
    ] {
        seeded.push((terminal, seed(&pool, &ns, terminal).await));
    }

    let updated = repo
        .batch_update_status(
            &ns,
            None,
            None,
            None,
            None,
            None,
            None,
            "cancelled",
            None,
            100,
        )
        .await
        .expect("batch update");

    assert!(
        updated.is_empty(),
        "a batch reported updating terminal executions: {updated:?}"
    );
    for (terminal, id) in seeded {
        assert_eq!(
            status_of(&pool, id).await,
            terminal,
            "'{terminal}' was modified by a batch operation"
        );
    }

    cleanup(&pool, &ns).await;
}

/// Positive control: the batch must still do its job.
#[tokio::test]
async fn a_batch_updates_non_terminal_executions() {
    let pool = pool().await;
    let ns = format!("batch-c-{}", Uuid::new_v4());
    let repo = WorkflowExecutionRepository::new((*pool).clone());

    let mut ids = Vec::new();
    for status in ["pending", "running", "retrying"] {
        ids.push(seed(&pool, &ns, status).await);
    }

    let updated = repo
        .batch_update_status(
            &ns,
            None,
            None,
            None,
            None,
            None,
            None,
            "cancelled",
            None,
            100,
        )
        .await
        .expect("batch update");

    assert_eq!(
        updated.len(),
        3,
        "the batch did not update every live execution"
    );
    for id in ids {
        assert_eq!(status_of(&pool, id).await, "cancelled");
    }

    cleanup(&pool, &ns).await;
}

/// The test that actually distinguishes this fix from the old code.
///
/// The three above pass either way: with no contention, the subquery guard
/// already excludes terminal rows. The defect only appears when a workflow
/// completes *while the batch statement is running*, so this reproduces that
/// interleaving directly with row locks:
///
///   1. session A opens a transaction and marks the row completed, holding the
///      lock without committing
///   2. the batch starts and blocks on that row
///   3. A commits — the row is now `completed`
///   4. the batch is released and re-checks the row
///
/// At step 4, READ COMMITTED re-evaluates the command's WHERE against the new
/// row version, but a subquery there still sees the pre-commit snapshot. With
/// the guard only in the subquery the row still matches and is overwritten,
/// losing a completed workflow's result. With it in the outer WHERE, it is
/// skipped.
#[tokio::test]
async fn a_workflow_completing_mid_batch_is_not_overwritten() {
    let pool = pool().await;
    let ns = format!("batch-race-{}", Uuid::new_v4());
    let id = seed(&pool, &ns, "running").await;
    let repo = WorkflowExecutionRepository::new((*pool).clone());

    // 1. Hold the row completed but uncommitted.
    let mut holder = pool.begin().await.expect("begin holder tx");
    sqlx::query("UPDATE workflow_executions SET status = 'completed' WHERE id = $1")
        .bind(id)
        .execute(&mut *holder)
        .await
        .expect("mark completed");

    // 2. The batch blocks on the locked row.
    let ns_for_batch = ns.clone();
    let batch = tokio::spawn(async move {
        repo.batch_update_status(
            &ns_for_batch,
            None,
            None,
            None,
            None,
            None,
            None,
            "cancelled",
            None,
            100,
        )
        .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // 3. Release it.
    holder.commit().await.expect("commit holder tx");

    // 4. The batch resumes and re-checks.
    let updated = batch.await.expect("batch task").expect("batch update");

    assert_eq!(
        status_of(&pool, id).await,
        "completed",
        "a workflow that completed while the batch was running was overwritten to \
         'cancelled' — its result is now unreachable"
    );
    assert!(
        updated.is_empty(),
        "the batch reported updating a row that had completed: {updated:?}"
    );

    cleanup(&pool, &ns).await;
}

/// A batch names a namespace, and namespace names are unique per organisation
/// rather than per deployment — every tenant has a "default". Scoped by name
/// alone, one BatchCancelWorkflow cancelled every tenant's running work in any
/// namespace whose name the caller could guess.
#[tokio::test]
#[ignore = "requires a live database"]
async fn a_batch_does_not_reach_another_organisations_workflows() {
    let pool = pool().await;
    let shared_name = format!("default-{}", Uuid::new_v4());
    let mine = an_organisation(&pool).await;
    let theirs = an_organisation(&pool).await;

    let my_workflow = seed_owned(&pool, &shared_name, mine).await;
    let their_workflow = seed_owned(&pool, &shared_name, theirs).await;

    let repo = WorkflowExecutionRepository::new((*pool).clone());
    let cancelled = repo
        .batch_update_status(
            &shared_name,
            Some(mine),
            None,
            None,
            None,
            None,
            None,
            "cancelled",
            None,
            100,
        )
        .await
        .expect("batch cancel");

    assert_eq!(cancelled.len(), 1, "only my workflow was cancelled");
    assert_eq!(status_of(&pool, my_workflow).await, "cancelled");
    assert_eq!(
        status_of(&pool, their_workflow).await,
        "running",
        "another organisation's workflow is untouched"
    );
}

/// A deployment with no organisations still cancels its own work: scoping to
/// NULL would match nothing and the batch would silently do nothing at all.
#[tokio::test]
#[ignore = "requires a live database"]
async fn without_an_organisation_a_batch_still_works() {
    let pool = pool().await;
    let ns = format!("unscoped-{}", Uuid::new_v4());
    let workflow = seed(&pool, &ns, "running").await;

    let repo = WorkflowExecutionRepository::new((*pool).clone());
    let cancelled = repo
        .batch_update_status(
            &ns,
            None,
            None,
            None,
            None,
            None,
            None,
            "cancelled",
            None,
            100,
        )
        .await
        .expect("batch cancel");

    assert_eq!(cancelled.len(), 1);
    assert_eq!(status_of(&pool, workflow).await, "cancelled");
}
