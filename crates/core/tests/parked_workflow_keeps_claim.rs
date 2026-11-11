//! A workflow parked on an external event must not be re-dispatched until the
//! event arrives.
//!
//! Background: a workflow that waits for an event returns from its activation
//! with no commands (the SDKs' WaitForEvent produces no proto command). The
//! engine used to treat "no commands" as "release the claim", which made the
//! parked execution claimable again immediately — pollers re-dispatched it
//! within a poll interval, the worker replayed and parked again, and so on,
//! at roughly sixteen dispatches a second per worker until the event came.
//!
//! These tests pin the two halves of the fix:
//! 1. a completion that neither completes nor fails the workflow keeps the
//!    claim, even when it carries no commands;
//! 2. delivering an event through the in-workflow SendEvent command releases
//!    the target's claim (the SendEvent RPCs already did), so the target is
//!    runnable exactly when it has something to consume.
//!
//! Requires PostgreSQL. The schema is created by the engine's own migration
//! runner. Run with:
//!
//! ```bash
//! export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/orcher_test"
//! cargo test -p orcher-core --test parked_workflow_keeps_claim -- --ignored
//! ```

use orcher_core::engine::dispatch::PollingManager;
use orcher_core::engine::queues::outbox::OutboxQueue;
use orcher_core::engine::WorkflowCoordinator;
use orcher_core::events::EventBus;
use orcher_core::grpc::services::ExecutionService;
use orcher_core::workflows::ChildWorkflowManager;
use orcher_core::workflows::WorkflowExecutionService;
use orcher_core::workflows::{CommandProcessor, JournalService};
use orcher_events::bus::EventBusConfig;
use orcher_proto::orcher::v1::execution_service_server::ExecutionService as ExecutionServiceTrait;
use orcher_proto::orcher::v1::{
    command, Command, CommandType, CompleteWorkflowExecutionRequest, SendEventCommandAttributes,
};
use orcher_storage::repositories::domains::{
    child_workflow::ChildWorkflowRepository, coordination::OutboxRepository,
    journal::JournalRepository, task_execution::TaskExecutionRepository,
    workflow_execution::WorkflowExecutionRepository,
};
use orcher_storage::repositories::{ExecutionLogRepository, PostgresExecutionLogRepository};
use sqlx::PgPool;
use std::sync::Arc;
use tonic::Request;
use uuid::Uuid;

const NAMESPACE: &str = "default";
const TASK_QUEUE: &str = "default";
const WORKER: &str = "worker-a";

struct Engine {
    pool: PgPool,
    service: ExecutionService,
}

impl Engine {
    async fn new() -> Self {
        let database_url = std::env::var("TEST_DATABASE_URL").unwrap_or_else(|_| {
            "postgresql://postgres:postgres@localhost:5432/orcher_test".to_string()
        });
        let pool = PgPool::connect(&database_url)
            .await
            .expect("connect to the test database");
        // Tests in this binary run in parallel and each builds its own engine;
        // the migration runner is not safe to race against itself, so apply
        // the schema once per process.
        static MIGRATED: tokio::sync::OnceCell<()> = tokio::sync::OnceCell::const_new();
        MIGRATED
            .get_or_init(|| async {
                orcher_storage::setup::run_migrations(&pool)
                    .await
                    .expect("apply migrations");
            })
            .await;

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
        let coordinator = Arc::new(
            WorkflowCoordinator::new(
                Arc::new(pool.clone()),
                orcher_core::engine::CoordinatorConfig::default(),
            )
            .await
            .expect("create coordinator"),
        );

        // The polling manager is what the in-workflow SendEvent handler uses to
        // release the target's claim and wake pollers; the fix under test
        // routes through it, so it has to be wired exactly as in production.
        let polling_manager = Arc::new(PollingManager::new(
            exec_repo.clone(),
            journal_repo.clone(),
            300,
        ));

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
            exec_repo,
            task_execution_repo,
            pool.clone(),
        );

        Self { pool, service }
    }

    /// A running execution that a worker has claimed — the state every
    /// activation starts from.
    async fn claimed_workflow(&self, name: &str) -> (Uuid, String) {
        let id = Uuid::new_v4();
        let execution_id = Uuid::new_v4().to_string();
        sqlx::query(
            r#"
            INSERT INTO workflow_executions (
                id, execution_id, workflow_name, workflow_version, namespace, task_queue,
                status, parameters, context, environment, progress,
                total_steps, completed_steps, failed_steps, retried_steps,
                claimed_by, claimed_at, claim_timeout_at
            )
            VALUES ($1, $2, $3, '1.0.0', $4, $5, 'running', '{}', '{}', '{}', '{}',
                    0, 0, 0, 0, $6, NOW(), NOW() + INTERVAL '5 minutes')
            "#,
        )
        .bind(id)
        .bind(&execution_id)
        .bind(name)
        .bind(NAMESPACE)
        .bind(TASK_QUEUE)
        .bind(WORKER)
        .execute(&self.pool)
        .await
        .expect("insert claimed workflow");
        (id, execution_id)
    }

    async fn claimed_by(&self, execution_id: &str) -> Option<String> {
        sqlx::query_scalar("SELECT claimed_by FROM workflow_executions WHERE execution_id = $1")
            .bind(execution_id)
            .fetch_one(&self.pool)
            .await
            .expect("read claim")
    }

    async fn complete(&self, id: Uuid, execution_id: &str, commands: Vec<Command>) {
        let request = CompleteWorkflowExecutionRequest {
            workflow_id: id.to_string(),
            execution_id: execution_id.to_string(),
            namespace: NAMESPACE.to_string(),
            identity: WORKER.to_string(),
            commands,
            ..Default::default()
        };
        self.service
            .complete_workflow_execution(Request::new(request))
            .await
            .expect("complete workflow execution");
    }

    async fn cleanup(&self, ids: &[Uuid]) {
        for id in ids {
            let _ = sqlx::query("DELETE FROM workflow_execution_journal WHERE workflow_id = $1")
                .bind(id.to_string())
                .execute(&self.pool)
                .await;
            let _ = sqlx::query("DELETE FROM workflow_executions WHERE id = $1")
                .bind(id)
                .execute(&self.pool)
                .await;
        }
    }
}

#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_workflow_parked_on_an_event_keeps_its_claim() {
    let engine = Engine::new().await;
    let (id, execution_id) = engine.claimed_workflow("parked-on-event").await;

    // The activation returns with nothing to do: it is waiting for an event.
    engine.complete(id, &execution_id, vec![]).await;

    assert_eq!(
        engine.claimed_by(&execution_id).await.as_deref(),
        Some(WORKER),
        "a parked workflow must stay claimed, or the pollers re-dispatch it at once"
    );

    engine.cleanup(&[id]).await;
}

#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn an_event_from_another_workflow_releases_the_parked_targets_claim() {
    let engine = Engine::new().await;
    let (target_id, target_execution) = engine.claimed_workflow("event-target").await;
    let (source_id, source_execution) = engine.claimed_workflow("event-source").await;

    // The target parks on an event and keeps its claim.
    engine.complete(target_id, &target_execution, vec![]).await;
    assert_eq!(
        engine.claimed_by(&target_execution).await.as_deref(),
        Some(WORKER)
    );

    // The source sends it the event from inside its own activation.
    let send_event = Command {
        command_type: CommandType::SendEvent as i32,
        attributes: Some(command::Attributes::SendEvent(SendEventCommandAttributes {
            workflow_id: target_id.to_string(),
            run_id: target_execution.clone(),
            event_name: "order_shipped".to_string(),
            payload: br#"{"status":"SHIPPED"}"#.to_vec(),
            headers: Default::default(),
        })),
    };
    engine
        .complete(source_id, &source_execution, vec![send_event])
        .await;

    assert_eq!(
        engine.claimed_by(&target_execution).await,
        None,
        "delivering the event must release the target so a poller can resume it"
    );
    let events: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM workflow_execution_journal \
         WHERE execution_id = $1 AND event_type = 'EventReceived'",
    )
    .bind(&target_execution)
    .fetch_one(&engine.pool)
    .await
    .expect("count journal entries");
    assert_eq!(
        events, 1,
        "the event must be journaled for the replay to consume"
    );

    engine.cleanup(&[target_id, source_id]).await;
}

fn start_timer(timer_id: &str, secs: i64) -> Command {
    Command {
        command_type: CommandType::StartTimer as i32,
        attributes: Some(command::Attributes::StartTimer(
            orcher_proto::orcher::v1::StartTimerCommandAttributes {
                timer_id: timer_id.to_string(),
                start_to_fire_timeout: Some(prost_types::Duration {
                    seconds: secs,
                    nanos: 0,
                }),
            },
        )),
    }
}

impl Engine {
    async fn timer_rows(&self, id: Uuid, timer_id: &str) -> i64 {
        sqlx::query_scalar(
            "SELECT COUNT(*) FROM timer_queue \
             WHERE workflow_id = $1 AND payload->>'timer_name' = $2",
        )
        .bind(id)
        .bind(timer_id)
        .fetch_one(&self.pool)
        .await
        .expect("count timers")
    }

    async fn timer_started_entries(&self, execution_id: &str) -> i64 {
        sqlx::query_scalar(
            "SELECT COUNT(*) FROM workflow_execution_journal \
             WHERE execution_id = $1 AND event_type = 'TimerStarted'",
        )
        .bind(execution_id)
        .fetch_one(&self.pool)
        .await
        .expect("count TimerStarted")
    }

    async fn delete_timers(&self, ids: &[Uuid]) {
        for id in ids {
            let _ = sqlx::query("DELETE FROM timer_queue WHERE workflow_id = $1")
                .bind(id)
                .execute(&self.pool)
                .await;
        }
    }
}

/// A workflow parked on a timed wait replays before the timer fires (after
/// the claim-timeout reclaimer hands it back, or after an unrelated event)
/// and emits the same StartTimer each time. Every copy used to become a
/// timer; each copy's firing re-dispatched the workflow, which emitted its
/// current timer again, so a loop of timed waits accumulated timers.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_replayed_start_timer_schedules_nothing_new() {
    let engine = Engine::new().await;
    let (id, execution_id) = engine.claimed_workflow("timed-wait").await;

    for _ in 0..3 {
        engine
            .complete(
                id,
                &execution_id,
                vec![start_timer("event_timeout_status_1", 900)],
            )
            .await;
    }

    assert_eq!(engine.timer_rows(id, "event_timeout_status_1").await, 1);
    assert_eq!(engine.timer_started_entries(&execution_id).await, 1);

    // The next round's timer is a new name and is scheduled.
    engine
        .complete(
            id,
            &execution_id,
            vec![start_timer("event_timeout_status_2", 900)],
        )
        .await;
    assert_eq!(engine.timer_rows(id, "event_timeout_status_2").await, 1);

    engine.delete_timers(&[id]).await;
    engine.cleanup(&[id]).await;
}

/// The check is per execution row, not per journal: a reset successor copies
/// its predecessor's TimerStarted entries but none of its timers, and must
/// schedule its own under the same name.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn another_execution_still_gets_its_own_timer_of_the_same_name() {
    let engine = Engine::new().await;
    let (first, first_execution) = engine.claimed_workflow("timed-wait-a").await;
    let (second, second_execution) = engine.claimed_workflow("timed-wait-b").await;

    engine
        .complete(first, &first_execution, vec![start_timer("timer_1", 900)])
        .await;
    engine
        .complete(second, &second_execution, vec![start_timer("timer_1", 900)])
        .await;

    assert_eq!(engine.timer_rows(first, "timer_1").await, 1);
    assert_eq!(engine.timer_rows(second, "timer_1").await, 1);

    engine.delete_timers(&[first, second]).await;
    engine.cleanup(&[first, second]).await;
}
