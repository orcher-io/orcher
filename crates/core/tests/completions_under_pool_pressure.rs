//! Many completions at once, against a small connection pool, all finish.
//!
//! A completion takes connections from the engine's pool to apply its
//! commands. Where a step held one connection — a transaction, an advisory
//! lock — and then waited for a second from the same pool, enough concurrent
//! completions each held their first and waited for a second that none of
//! them would give back. With a pool of ten, three hundred concurrent
//! completions mostly timed out; with four, the pool deadlocked outright.
//!
//! The pool here is deliberately small and the completions many, each with
//! several commands, so that any step that nests one acquire inside another
//! shows up as a timeout rather than as a latency tail nobody reads.
//!
//! Requires PostgreSQL. Run with:
//!
//! ```bash
//! export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/orcher_test"
//! cargo test -p orcher-core --test completions_under_pool_pressure -- --ignored --test-threads=1
//! ```

use std::sync::Arc;
use std::time::Duration;

use orcher_core::engine::dispatch::PollingManager;
use orcher_core::engine::queues::outbox::OutboxQueue;
use orcher_core::engine::WorkflowCoordinator;
use orcher_core::events::EventBus;
use orcher_core::grpc::services::ExecutionService;
use orcher_core::workflows::{
    ChildWorkflowManager, CommandProcessor, JournalService, WorkflowExecutionService,
};
use orcher_events::bus::EventBusConfig;
use orcher_proto::orcher::v1::execution_service_server::ExecutionService as ExecutionServiceTrait;
use orcher_proto::orcher::v1::{
    command, Command, CommandType, CompleteWorkflowExecutionRequest, ScheduleTaskCommandAttributes,
    StartTimerCommandAttributes,
};
use orcher_storage::repositories::domains::{
    child_workflow::ChildWorkflowRepository, coordination::OutboxRepository,
    journal::JournalRepository, task_execution::TaskExecutionRepository,
    workflow_execution::WorkflowExecutionRepository,
};
use orcher_storage::repositories::{ExecutionLogRepository, PostgresExecutionLogRepository};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use tonic::Request;
use uuid::Uuid;

const NAMESPACE: &str = "default";
const WORKER: &str = "worker-a";
const POOL_SIZE: u32 = 6;
const COMPLETIONS: usize = 200;
/// Far longer than the work takes when nothing waits on itself, and far
/// shorter than the pool's acquire timeout multiplied out over a queue of
/// completions stuck behind each other.
const DEADLINE: Duration = Duration::from_secs(60);

fn url() -> String {
    std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://postgres:postgres@localhost:5432/orcher_test".to_string())
}

/// A roomy pool for setting up and inspecting; the engine gets the small one.
async fn admin_pool() -> PgPool {
    let pool = PgPool::connect(&url())
        .await
        .expect("connect to the test database");
    orcher_storage::setup::run_migrations(&pool)
        .await
        .expect("apply migrations");
    pool
}

async fn small_pool() -> PgPool {
    PgPoolOptions::new()
        .max_connections(POOL_SIZE)
        .acquire_timeout(Duration::from_secs(20))
        .connect(&url())
        .await
        .expect("connect the small pool")
}

async fn engine(pool: &PgPool) -> (Arc<ExecutionService>, Arc<WorkflowExecutionRepository>) {
    let journal_repo = Arc::new(JournalRepository::new(pool.clone()));
    let journal_service = Arc::new(JournalService::new(journal_repo.clone()));
    let child_repo = Arc::new(ChildWorkflowRepository::new(pool.clone()));
    let exec_repo = Arc::new(WorkflowExecutionRepository::new(pool.clone()));
    let outbox_repo = Arc::new(OutboxRepository::new(Arc::new(pool.clone())));
    let outbox_queue = Arc::new(OutboxQueue::new(outbox_repo));
    let event_bus = EventBus::new(EventBusConfig::default());
    let execution_log_repo = Arc::new(PostgresExecutionLogRepository::new(pool.clone()));
    let task_execution_repo = Arc::new(TaskExecutionRepository::new(pool.clone()));
    let workflow_execution_service = Arc::new(WorkflowExecutionService::new(
        exec_repo.clone(),
        journal_service.clone(),
        outbox_queue,
        event_bus.clone(),
        pool.clone(),
    ));
    let child_workflow_manager = Arc::new(ChildWorkflowManager::new(
        child_repo,
        journal_service.clone(),
        workflow_execution_service,
    ));
    let polling_manager = Arc::new(PollingManager::new(
        exec_repo.clone(),
        journal_repo.clone(),
        300,
    ));
    let coordinator = Arc::new(
        WorkflowCoordinator::new(
            Arc::new(pool.clone()),
            orcher_core::engine::CoordinatorConfig::default(),
        )
        .await
        .expect("create coordinator"),
    );
    let command_processor = CommandProcessor::new(
        journal_service,
        child_workflow_manager,
        event_bus,
        execution_log_repo.clone(),
        task_execution_repo.clone(),
        exec_repo.clone(),
        Arc::new(pool.clone()),
    )
    .with_coordinator(coordinator)
    .with_polling_manager(polling_manager.clone());

    let service = ExecutionService::new(
        Arc::new(command_processor),
        polling_manager,
        execution_log_repo as Arc<dyn ExecutionLogRepository>,
        journal_repo,
        exec_repo.clone(),
        task_execution_repo,
        pool.clone(),
    );
    (Arc::new(service), exec_repo)
}

/// A running workflow, claimed by the worker, alone on a task queue.
async fn claimed_workflow(
    pool: &PgPool,
    repo: &WorkflowExecutionRepository,
) -> (Uuid, String, i64) {
    let id = Uuid::new_v4();
    let execution_id = Uuid::new_v4().to_string();
    let task_queue = format!("pressure-{}", Uuid::new_v4().simple());
    sqlx::query(
        r#"
        INSERT INTO workflow_executions (
            id, execution_id, workflow_id, workflow_name, workflow_version, namespace,
            task_queue, status, parameters, context, environment, progress,
            total_steps, completed_steps, failed_steps, retried_steps
        )
        VALUES ($1, $2, $1, 'pool-pressure', '1.0.0', $3, $4, 'running',
                '{}', '{}', '{}', '{}', 0, 0, 0, 0)
        "#,
    )
    .bind(id)
    .bind(&execution_id)
    .bind(NAMESPACE)
    .bind(&task_queue)
    .execute(pool)
    .await
    .expect("insert workflow");
    let claimed = repo
        .claim_and_get_next(&task_queue, NAMESPACE, WORKER, 300, None, None)
        .await
        .expect("claim")
        .expect("claimable");
    (id, execution_id, claimed.activation_seq)
}

fn commands() -> Vec<Command> {
    let mut commands: Vec<Command> = (0..3)
        .map(|n| Command {
            command_type: CommandType::ScheduleTask as i32,
            attributes: Some(command::Attributes::ScheduleTask(
                ScheduleTaskCommandAttributes {
                    task_id: format!("task-{n}"),
                    task_type: "work".to_string(),
                    task_queue: "pressure-tasks".to_string(),
                    input: b"{}".to_vec(),
                    ..Default::default()
                },
            )),
        })
        .collect();
    commands.push(Command {
        command_type: CommandType::StartTimer as i32,
        attributes: Some(command::Attributes::StartTimer(
            StartTimerCommandAttributes {
                timer_id: "timer-0".to_string(),
                start_to_fire_timeout: Some(orcher_proto::prost_types::Duration {
                    seconds: 3600,
                    nanos: 0,
                }),
            },
        )),
    });
    commands
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore] // Requires PostgreSQL
async fn many_concurrent_completions_finish_on_a_small_pool() {
    let admin = admin_pool().await;
    let pool = small_pool().await;
    let (service, _) = engine(&pool).await;
    // Claims are made through a repository on the roomy pool, so setting up
    // does not compete with what is being measured.
    let setup_repo = WorkflowExecutionRepository::new(admin.clone());

    let mut workflows = Vec::with_capacity(COMPLETIONS);
    for _ in 0..COMPLETIONS {
        workflows.push(claimed_workflow(&admin, &setup_repo).await);
    }

    let started = std::time::Instant::now();
    let mut sends = tokio::task::JoinSet::new();
    for (id, execution_id, seq) in workflows.clone() {
        let service = service.clone();
        sends.spawn(async move {
            let request = CompleteWorkflowExecutionRequest {
                workflow_id: id.to_string(),
                execution_id: execution_id.clone(),
                namespace: NAMESPACE.to_string(),
                identity: WORKER.to_string(),
                task_token: format!("{id}:{execution_id}").into_bytes(),
                stream_entry_id: orcher_core::grpc::activation_token::encode(&execution_id, seq),
                commands: commands(),
                ..Default::default()
            };
            service
                .complete_workflow_execution(Request::new(request))
                .await
                .map(|_| ())
                .map_err(|status| status.to_string())
        });
    }

    let outcome = tokio::time::timeout(DEADLINE, async {
        let mut failures = Vec::new();
        while let Some(sent) = sends.join_next().await {
            if let Err(e) = sent.expect("join") {
                failures.push(e);
            }
        }
        failures
    })
    .await;
    let elapsed = started.elapsed();

    let failures = outcome.unwrap_or_else(|_| {
        panic!(
            "{COMPLETIONS} completions on a pool of {POOL_SIZE} did not finish within \
             {DEADLINE:?}: something holds a connection while waiting for another"
        )
    });
    assert!(
        failures.is_empty(),
        "{} of {COMPLETIONS} completions failed in {elapsed:?}; first: {}",
        failures.len(),
        failures[0]
    );

    let scheduled: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM task_executions WHERE execution_id = ANY($1::uuid[])",
    )
    .bind(
        workflows
            .iter()
            .map(|(_, e, _)| Uuid::parse_str(e).unwrap())
            .collect::<Vec<_>>(),
    )
    .fetch_one(&admin)
    .await
    .expect("count tasks");
    assert_eq!(
        scheduled,
        (COMPLETIONS * 3) as i64,
        "every task scheduled once"
    );
    eprintln!("{COMPLETIONS} completions on a pool of {POOL_SIZE}: {elapsed:?}");

    for (id, execution_id, _) in workflows {
        for q in [
            "DELETE FROM workflow_execution_journal WHERE execution_id = $1",
            "DELETE FROM workflow_execution_logs WHERE execution_id = $1",
            "DELETE FROM task_executions WHERE execution_id = $1::uuid",
        ] {
            let _ = sqlx::query(q).bind(&execution_id).execute(&admin).await;
        }
        for q in [
            "DELETE FROM outbox_queue WHERE workflow_id = $1",
            "DELETE FROM timer_queue WHERE workflow_id = $1",
            "DELETE FROM workflow_executions WHERE id = $1",
        ] {
            let _ = sqlx::query(q).bind(id).execute(&admin).await;
        }
    }
}
