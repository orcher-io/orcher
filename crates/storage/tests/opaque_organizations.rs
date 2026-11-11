//! An organization id is a tenancy key the engine compares and never looks up.
//!
//! The engine's tables carried foreign keys to `organizations` and `users`,
//! tables it does not own, so nothing could be stored for an organization
//! that table did not hold: a tenant named by an identity provider's token,
//! or any row on a shard database, whose `organizations` table is empty. V085
//! drops them. The account tables keep the keys between themselves.
//!
//! Requires PostgreSQL. Run with:
//!
//! ```bash
//! export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/orcher_test"
//! cargo test -p orcher-storage --test opaque_organizations -- --ignored --test-threads=1
//! ```

use sqlx::PgPool;
use uuid::Uuid;

/// The engine's tables that carry an organization id.
const ENGINE_TABLES: [&str; 17] = [
    "workflow_executions",
    "workflow_execution_logs",
    "workflow_execution_metrics",
    "workflow_execution_journal",
    "child_workflow_executions",
    "task_executions",
    "execution_log",
    "outbox_queue",
    "timer_queue",
    "dead_letter_queue",
    "schedules",
    "schedule_executions",
    "schedule_instances",
    "actor_state",
    "batch_operations",
    "task_queue_releases",
    "api_keys",
];

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

/// `table`'s foreign keys to `organizations` or `users`.
async fn keys_to_accounts(pool: &PgPool, table: &str) -> Vec<String> {
    sqlx::query_scalar(
        "SELECT conname::text FROM pg_constraint \
         WHERE contype = 'f' AND conrelid = to_regclass($1) \
           AND confrelid IN (to_regclass('organizations'), to_regclass('users'))",
    )
    .bind(table)
    .fetch_all(pool)
    .await
    .expect("read the table's foreign keys")
}

#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn no_engine_table_references_the_account_tables() {
    let pool = pool().await;
    let mut left = Vec::new();
    for table in ENGINE_TABLES {
        for key in keys_to_accounts(&pool, table).await {
            left.push(format!("{table}.{key}"));
        }
    }
    assert!(
        left.is_empty(),
        "still referencing the account tables: {left:?}"
    );

    // The account tables keep theirs: only the engine's tables were touched.
    assert!(
        !keys_to_accounts(&pool, "organization_members")
            .await
            .is_empty(),
        "the account tables lost their foreign keys too"
    );
}

#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_run_and_a_key_are_stored_for_an_organization_with_no_row() {
    let pool = pool().await;
    let organization = Uuid::new_v4();

    let run: Result<Uuid, _> = sqlx::query_scalar(
        "INSERT INTO workflow_executions \
         (execution_id, workflow_name, workflow_version, namespace, task_queue, status, \
          organization_id) \
         VALUES ($1, 'orders', '1.0', 'default', 'opaque-org', 'pending', $2) RETURNING id",
    )
    .bind(format!("opaque-org-{}", Uuid::new_v4().simple()))
    .bind(organization)
    .fetch_one(&pool)
    .await;

    let key: Result<Uuid, _> = sqlx::query_scalar(
        "INSERT INTO api_keys (name, key_prefix, key_hash, organization_id) \
         VALUES ('opaque-org', 'orch_', $1, $2) RETURNING id",
    )
    .bind(format!("hash-{}", Uuid::new_v4().simple()))
    .bind(organization)
    .fetch_one(&pool)
    .await;

    if let Ok(id) = &run {
        let _ = sqlx::query("DELETE FROM workflow_executions WHERE id = $1")
            .bind(id)
            .execute(&pool)
            .await;
    }
    if let Ok(id) = &key {
        let _ = sqlx::query("DELETE FROM api_keys WHERE id = $1")
            .bind(id)
            .execute(&pool)
            .await;
    }

    run.expect("store a run for an organization with no row");
    key.expect("store a key for an organization with no row");
}
