//! A column added under a running engine breaks none of its claims and none
//! of its journal reads or writes.
//!
//! sqlx prepares each statement once per connection and keeps it, and
//! Postgres refuses a kept statement whose result shape a migration changed
//! ("cached plan must not change result type"). The engine's statements read
//! `*` from the tables a migration extends, so while a new release rolled out
//! and migrated, every engine still on the old build failed every claim until
//! it was replaced. The statements now name their columns, which an added
//! column does not change.
//!
//! Everything here runs on one connection, so every statement the second
//! round runs is one the first round prepared on the connection the columns
//! are then added under. A statement reading whole rows, prepared the same
//! way, is refused afterwards: that is the failure, reproduced, beside the
//! statements that no longer have it.
//!
//! Requires PostgreSQL. Run with:
//!
//! ```bash
//! export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/orcher_test"
//! cargo test -p orcher-storage --test additive_migration -- --ignored --test-threads=1
//! ```

use std::sync::Arc;
use std::time::Duration;

use orcher_common::types::identifiers::{ExecutionId, WorkflowId};
use orcher_storage::repositories::domains::coordination::OutboxRepository;
use orcher_storage::repositories::domains::journal::JournalRepository;
use orcher_storage::repositories::domains::task_execution::TaskExecutionRepository;
use orcher_storage::repositories::domains::worker::{SeenInstance, WorkerInstanceRepository};
use orcher_storage::repositories::domains::workflow_execution::WorkflowExecutionRepository;
use orcher_storage::schema::types::journal::JournalEntry;
use orcher_storage::stale_plan::{is_stale_plan, recycle_stale_connections};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use uuid::Uuid;

const NAMESPACE: &str = "default";

/// The tables a release is most likely to add a column to, and whose rows
/// the claim and journal paths read back.
const TABLES: [&str; 5] = [
    "workflow_executions",
    "task_executions",
    "workflow_execution_journal",
    "worker_instances",
    "outbox_queue",
];

/// A pool of exactly one connection, set up as the engine sets up its own.
async fn one_connection() -> PgPool {
    let url = std::env::var("TEST_DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:postgres@localhost:5432/orcher_test".to_string()
    });
    let pool = recycle_stale_connections(PgPoolOptions::new())
        .max_connections(1)
        .min_connections(1)
        .acquire_timeout(Duration::from_secs(30))
        .connect(&url)
        .await
        .expect("connect to the test database");
    orcher_storage::setup::run_migrations(&pool)
        .await
        .expect("apply migrations");
    pool
}

/// A workflow execution waiting to be claimed from `task_queue`.
async fn waiting_execution(pool: &PgPool, task_queue: &str) -> Uuid {
    let id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO workflow_executions (
            id, execution_id, workflow_id, workflow_name, workflow_version, namespace,
            task_queue, status, parameters, context, environment, progress,
            total_steps, completed_steps, failed_steps, retried_steps
        )
        VALUES ($1, $2, $1::text, 'additive-migration', '1.0.0', $3, $4, 'pending',
                '{}', '{}', '{}', '{}', 0, 0, 0, 0)
        "#,
    )
    .bind(id)
    .bind(Uuid::new_v4().to_string())
    .bind(NAMESPACE)
    .bind(task_queue)
    .execute(pool)
    .await
    .expect("insert a waiting execution");
    id
}

struct Repositories {
    executions: WorkflowExecutionRepository,
    journal: JournalRepository,
    outbox: OutboxRepository,
    tasks: TaskExecutionRepository,
    instances: WorkerInstanceRepository,
}

/// What an engine does with the tables in [`TABLES`], once: claim a workflow
/// activation singly and in a batch, write and read its journal, claim its
/// outbox work, schedule and start a task, and see a worker process.
/// `round` tells the two rounds' assertion messages apart.
async fn run_the_engine(pool: &PgPool, repos: &Repositories, task_queue: &str, round: &str) {
    let claimed = repos
        .executions
        .claim_next_from(
            task_queue,
            NAMESPACE,
            "additive-migration-worker",
            60,
            None,
            None,
            Some("additive-migration-process"),
            &[],
        )
        .await
        .unwrap_or_else(|e| panic!("{round}: claim a workflow activation: {e}"))
        .unwrap_or_else(|| panic!("{round}: a waiting execution was not claimed"));
    let batch = repos
        .executions
        .claim_batch(
            task_queue,
            NAMESPACE,
            "additive-migration-worker",
            60,
            1,
            None,
            None,
        )
        .await
        .unwrap_or_else(|e| panic!("{round}: claim a batch of activations: {e}"));
    assert_eq!(
        batch.len(),
        1,
        "{round}: a waiting execution was not batch-claimed"
    );

    let routing_id = claimed.id.as_uuid();
    let workflow_id = routing_id.to_string();
    let execution_id = claimed.execution_id.clone();
    let entry = |event_type: &str| {
        JournalEntry::new(
            workflow_id.clone(),
            execution_id.clone(),
            0,
            event_type.to_string(),
            serde_json::json!({ "key": "value" }),
        )
    };

    let started = repos
        .journal
        .append_next(entry("WorkflowExecutionStarted"))
        .await
        .unwrap_or_else(|e| panic!("{round}: append to the journal: {e}"));
    let once = repos
        .journal
        .append_next_once(entry("TaskScheduled"), &["TaskScheduled"], "key", "value")
        .await
        .unwrap_or_else(|e| panic!("{round}: append to the journal once: {e}"));
    assert!(
        once.is_some(),
        "{round}: a first append-once appended nothing"
    );
    let mut explicit = entry("MarkerRecorded");
    explicit.event_id = started.event_id + 10;
    repos
        .journal
        .append(explicit)
        .await
        .unwrap_or_else(|e| panic!("{round}: append at an explicit event id: {e}"));

    let journal = repos
        .journal
        .get_journal(&workflow_id, &execution_id)
        .await
        .unwrap_or_else(|e| panic!("{round}: read the journal: {e}"));
    assert_eq!(
        journal.len(),
        3,
        "{round}: the journal holds what was appended"
    );
    repos
        .journal
        .get_entry(&workflow_id, &execution_id, started.event_id)
        .await
        .unwrap_or_else(|e| panic!("{round}: read a journal entry: {e}"))
        .unwrap_or_else(|| panic!("{round}: an appended entry is not there"));
    let range = repos
        .journal
        .get_range(&workflow_id, &execution_id, 0, i64::MAX)
        .await
        .unwrap_or_else(|e| panic!("{round}: read a journal range: {e}"));
    assert_eq!(range.len(), 3, "{round}: the range holds what was appended");
    repos
        .journal
        .get_entries_after(&workflow_id, &execution_id, 0)
        .await
        .unwrap_or_else(|e| panic!("{round}: read the journal after an event: {e}"));

    sqlx::query(
        "INSERT INTO outbox_queue (workflow_id, task_type, payload) \
         VALUES ($1, 'dispatch_task', '{}')",
    )
    .bind(routing_id)
    .execute(pool)
    .await
    .unwrap_or_else(|e| panic!("{round}: queue outbox work: {e}"));
    let outbox = repos
        .outbox
        .claim_tasks_for_workflow(None, routing_id, 10)
        .await
        .unwrap_or_else(|e| panic!("{round}: claim outbox work: {e}"));
    assert_eq!(
        outbox.len(),
        1,
        "{round}: the queued outbox work was not claimed"
    );

    let task = repos
        .tasks
        .create_scheduled(
            format!("task-{}", Uuid::new_v4()),
            "AdditiveMigrationTask".to_string(),
            WorkflowId::from_uuid(routing_id),
            ExecutionId::from_uuid(Uuid::parse_str(&execution_id).expect("execution id")),
            task_queue.to_string(),
            NAMESPACE.to_string(),
            1,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap_or_else(|e| panic!("{round}: schedule a task: {e}"));
    let scheduled = repos
        .tasks
        .find_scheduled_for_workflow(WorkflowId::from_uuid(routing_id), task_queue, NAMESPACE)
        .await
        .unwrap_or_else(|e| panic!("{round}: read the scheduled tasks: {e}"));
    assert_eq!(
        scheduled.len(),
        1,
        "{round}: the scheduled task is not there"
    );
    repos
        .tasks
        .mark_started_as(
            task.id,
            "additive-migration-worker".to_string(),
            format!("token-{}", Uuid::new_v4()),
            None,
            Some("additive-migration-process"),
        )
        .await
        .unwrap_or_else(|e| panic!("{round}: start the task: {e}"));

    repos
        .instances
        .record_seen(&[SeenInstance {
            organization_id: Uuid::nil(),
            namespace: NAMESPACE.to_string(),
            worker_instance_id: format!("additive-migration-{}", Uuid::new_v4()),
            identity: "additive-migration-worker".to_string(),
        }])
        .await
        .unwrap_or_else(|e| panic!("{round}: record a worker process seen: {e}"));
    repos
        .instances
        .lapsed(3600, 7200)
        .await
        .unwrap_or_else(|e| panic!("{round}: read the lapsed worker processes: {e}"));
}

#[tokio::test]
#[ignore = "requires PostgreSQL"]
async fn an_added_column_breaks_no_claim_and_no_journal_read_or_write() {
    let pool = one_connection().await;
    let repos = Repositories {
        executions: WorkflowExecutionRepository::new(pool.clone()),
        journal: JournalRepository::new(pool.clone()),
        outbox: OutboxRepository::new(Arc::new(pool.clone())),
        tasks: TaskExecutionRepository::new(pool.clone()),
        instances: WorkerInstanceRepository::new(pool.clone()),
    };
    let task_queue = format!("additive-migration-{}", Uuid::new_v4());
    let before = waiting_execution(&pool, &task_queue).await;
    waiting_execution(&pool, &task_queue).await;

    // Before: every statement is prepared, and kept, on the one connection.
    run_the_engine(&pool, &repos, &task_queue, "before the migration").await;
    let whole_row = "SELECT * FROM workflow_executions WHERE id = $1";
    sqlx::query(whole_row)
        .bind(before)
        .fetch_one(&pool)
        .await
        .expect("read a whole row before the migration");

    waiting_execution(&pool, &task_queue).await;
    waiting_execution(&pool, &task_queue).await;

    // The migration, under that connection.
    let column = format!("added_{}", &Uuid::new_v4().simple().to_string()[..12]);
    for table in TABLES {
        sqlx::raw_sql(&format!("ALTER TABLE {table} ADD COLUMN {column} TEXT"))
            .execute(&pool)
            .await
            .unwrap_or_else(|e| panic!("add a column to {table}: {e}"));
    }

    // The failure is real here: the whole-row statement it prepared before
    // is refused now.
    let refused = sqlx::query(whole_row)
        .bind(before)
        .fetch_one(&pool)
        .await
        .expect_err("a whole-row statement prepared before the migration was not refused");
    assert!(
        is_stale_plan(&refused),
        "a whole-row statement was refused for another reason: {refused}"
    );

    // And none of the engine's statements has it.
    run_the_engine(&pool, &repos, &task_queue, "after the migration").await;

    for table in TABLES {
        sqlx::raw_sql(&format!("ALTER TABLE {table} DROP COLUMN {column}"))
            .execute(&pool)
            .await
            .unwrap_or_else(|e| panic!("drop the added column from {table}: {e}"));
    }
}
