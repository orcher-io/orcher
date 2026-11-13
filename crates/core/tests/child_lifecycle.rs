//! A child workflow's outcome reaches its real parent, once, and only when the
//! child has ended for good.
//!
//! - A child's tracking row used to hold an execution id no execution had, so
//!   nothing could get from a finished run back to its parent except the run's
//!   own `parent_execution_id` — which a retried or continued run points at the
//!   run it replaced. Delivery followed it there: a retried top-level workflow
//!   "notified" its own failed first attempt, and a retried child's final
//!   outcome went to its failed attempt instead of its parent.
//! - A child's retry policy was dropped, and a failure was reported to the
//!   parent even when it was about to be retried.
//! - A child that was cancelled, timed out or terminated was never reported at
//!   all, and parent close policies were never applied.
//!
//! Requires PostgreSQL. Run with:
//!
//! ```bash
//! export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/orcher_test"
//! cargo test -p orcher-core --test child_lifecycle -- --ignored
//! ```

// Helpers return `tonic::Status`, the error the engine's gRPC services answer with.
#![allow(clippy::result_large_err)]

use orcher_core::engine::dispatch::PollingManager;
use orcher_core::engine::queues::outbox::OutboxQueue;
use orcher_core::engine::WorkflowCoordinator;
use orcher_core::events::EventBus;
use orcher_core::grpc::services::{ExecutionService, WorkflowService};
use orcher_core::retries::timeout_watcher::{TimeoutWatcher, TimeoutWatcherConfig};
use orcher_core::workflows::{
    ChildReconciler, ChildReconcilerConfig, ChildWorkflowManager, CommandProcessor, JournalMapper,
    JournalService, WorkflowExecutionService, WorkflowRetryScheduler, WorkflowRetrySchedulerConfig,
};
use orcher_events::bus::EventBusConfig;
use orcher_proto::orcher::v1::execution_service_server::ExecutionService as ExecutionServiceTrait;
use orcher_proto::orcher::v1::journal_entry::Attributes;
use orcher_proto::orcher::v1::workflow_service_server::WorkflowService as WorkflowServiceTrait;
use orcher_proto::orcher::v1::{
    command, CancelChildWorkflowCommandAttributes, CancelWorkflowRequest, Command, CommandType,
    CompleteWorkflowCommandAttributes, CompleteWorkflowExecutionRequest,
    FailWorkflowCommandAttributes, FailWorkflowExecutionRequest, Failure, ResetWorkflowRequest,
    RestartFreshCommandAttributes, RetryPolicy, StartChildWorkflowCommandAttributes,
    TerminateWorkflowRequest,
};
use orcher_storage::repositories::domains::{
    child_workflow::ChildWorkflowRepository, coordination::OutboxRepository,
    journal::JournalRepository, task_execution::TaskExecutionRepository,
    workflow_execution::WorkflowExecutionRepository,
};
use orcher_storage::repositories::{ExecutionLogRepository, PostgresExecutionLogRepository};
use orcher_storage::schema::types::child_workflow::{ChildWorkflowExecution, OrphanPolicy};
use orcher_storage::schema::types::execution::WorkflowExecution;
use sqlx::PgPool;
use std::sync::Arc;
use tonic::Request;
use uuid::Uuid;

const NAMESPACE: &str = "default";
const TASK_QUEUE: &str = "child-lifecycle";
const WORKER: &str = "worker-a";

const ORPHAN_TERMINATE: i32 = 1;
const ORPHAN_ABANDON: i32 = 3;

struct Engine {
    pool: PgPool,
    executions: Arc<WorkflowExecutionRepository>,
    service: ExecutionService,
    workflows: WorkflowService,
    retries: WorkflowRetryScheduler,
    reconciler: ChildReconciler,
    polling_manager: Arc<PollingManager>,
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
            child_repo.clone(),
            journal_service.clone(),
            workflow_execution_service.clone(),
        ));
        let coordinator = Arc::new(
            WorkflowCoordinator::new(
                Arc::new(pool.clone()),
                orcher_core::engine::CoordinatorConfig::default(),
            )
            .await
            .expect("create coordinator"),
        );
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
            polling_manager.clone(),
            execution_log_repo as Arc<dyn ExecutionLogRepository>,
            journal_repo.clone(),
            exec_repo.clone(),
            task_execution_repo,
            pool.clone(),
        );

        let workflows = WorkflowService::new(workflow_execution_service, exec_repo.clone())
            .with_event_support(journal_repo, polling_manager.clone())
            .with_child_repository(child_repo.clone());
        let retries =
            WorkflowRetryScheduler::new(exec_repo.clone(), WorkflowRetrySchedulerConfig::default())
                .with_child_repository(child_repo);
        // Big enough to take every open row in one pass, whatever other tests
        // have left in the database.
        let reconciler = ChildReconciler::new(
            service.child_outcome_delivery(),
            exec_repo.clone(),
            ChildReconcilerConfig {
                poll_interval_secs: 1,
                batch_size: 1_000_000,
            },
        );

        Self {
            pool,
            executions: exec_repo,
            service,
            workflows,
            retries,
            reconciler,
            polling_manager,
        }
    }

    /// Another sweep over the same rows, as a second replica runs one.
    fn reconciler(&self, batch_size: i64) -> ChildReconciler {
        ChildReconciler::new(
            self.service.child_outcome_delivery(),
            self.executions.clone(),
            ChildReconcilerConfig {
                poll_interval_secs: 1,
                batch_size,
            },
        )
    }

    /// How many times `execution_id` has been handed back to a worker since
    /// the last call: each wake queues one continuation.
    fn wakes(&self, execution_id: &str) -> usize {
        let mut wakes = 0;
        while let Some(woken) = self
            .polling_manager
            .try_pop_continuation(TASK_QUEUE, NAMESPACE, WORKER)
        {
            if woken == execution_id {
                wakes += 1;
            }
        }
        wakes
    }

    async fn sql(&self, sql: &str, id: &str) {
        sqlx::query(sql)
            .bind(id)
            .execute(&self.pool)
            .await
            .expect("fixture update");
    }

    /// A running execution that a worker has claimed — the state every
    /// activation starts from.
    async fn claimed_workflow(&self, name: &str) -> (Uuid, String) {
        let id = Uuid::new_v4();
        let execution_id = Uuid::new_v4().to_string();
        sqlx::query(
            r#"
            INSERT INTO workflow_executions (
                id, execution_id, workflow_id, workflow_name, workflow_version, namespace,
                task_queue, status, parameters, context, environment, progress,
                total_steps, completed_steps, failed_steps, retried_steps,
                max_retries, claimed_by, claimed_at, claim_timeout_at
            )
            VALUES ($1, $2, $3, $4, '1.0.0', $5, $6, 'running', '{}', '{}', '{}', '{}',
                    0, 0, 0, 0, 0, $7, NOW(), NOW() + INTERVAL '5 minutes')
            "#,
        )
        .bind(id)
        .bind(&execution_id)
        .bind(format!("wf-{id}"))
        .bind(name)
        .bind(NAMESPACE)
        .bind(TASK_QUEUE)
        .bind(WORKER)
        .execute(&self.pool)
        .await
        .expect("insert claimed workflow");
        (id, execution_id)
    }

    async fn complete(&self, id: Uuid, execution_id: &str, commands: Vec<Command>) {
        // Each completion answers a fresh activation, as a worker's does: it
        // polls for the workflow again between two of them, and the poll
        // hands it out as the next activation. Without this, a second
        // completion from the claim holder answers the activation the first
        // already applied, and is acknowledged as a repeat of it.
        sqlx::query(
            "UPDATE workflow_executions SET activation_seq = activation_seq + 1 \
             WHERE execution_id = $1",
        )
        .bind(execution_id)
        .execute(&self.pool)
        .await
        .expect("hand the workflow out again");
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
            .expect("complete workflow task");
    }

    /// A worker reporting that the workflow task itself failed.
    async fn fail(&self, run: &WorkflowExecution, message: &str) {
        let request = FailWorkflowExecutionRequest {
            workflow_id: run.id.as_uuid().to_string(),
            execution_id: run.execution_id.clone(),
            namespace: NAMESPACE.to_string(),
            identity: WORKER.to_string(),
            failure: Some(Failure {
                message: message.to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };
        self.service
            .fail_workflow_execution(Request::new(request))
            .await
            .expect("fail workflow");
    }

    async fn run(&self, execution_id: &str) -> WorkflowExecution {
        self.executions
            .find_by_execution_id(execution_id)
            .await
            .expect("read run")
            .expect("run exists")
    }

    /// The first run the parent started under `child_workflow_id`.
    async fn child(&self, parent_execution_id: &str, child_workflow_id: &str) -> WorkflowExecution {
        self.executions
            .find_child(parent_execution_id, child_workflow_id)
            .await
            .expect("read child")
            .expect("child exists")
    }

    async fn tracked_run(&self, parent_execution_id: &str, child_workflow_id: &str) -> String {
        sqlx::query_scalar(
            "SELECT child_execution_id FROM child_workflow_executions \
             WHERE parent_execution_id = $1 AND child_workflow_id = $2",
        )
        .bind(parent_execution_id)
        .bind(child_workflow_id)
        .fetch_one(&self.pool)
        .await
        .expect("read tracking row")
    }

    async fn tracked_state(&self, parent_execution_id: &str, child_workflow_id: &str) -> String {
        sqlx::query_scalar(
            "SELECT state FROM child_workflow_executions \
             WHERE parent_execution_id = $1 AND child_workflow_id = $2",
        )
        .bind(parent_execution_id)
        .bind(child_workflow_id)
        .fetch_one(&self.pool)
        .await
        .expect("read tracking state")
    }

    /// Every child outcome in `execution_id`'s journal: (event type, payload).
    async fn child_outcomes(&self, execution_id: &str) -> Vec<(String, serde_json::Value)> {
        sqlx::query_as(
            "SELECT event_type, payload FROM workflow_execution_journal \
             WHERE execution_id = $1 AND event_type LIKE 'ChildWorkflowExecution%' \
               AND event_type NOT IN ('ChildWorkflowExecutionInitiated', \
                                      'ChildWorkflowExecutionStarted') \
             ORDER BY event_id",
        )
        .bind(execution_id)
        .fetch_all(&self.pool)
        .await
        .expect("read child outcomes")
    }

    /// Everything a test created under a top-level run: its descendants and
    /// successors carry it as their root.
    async fn cleanup(&self, root_execution_id: &str) {
        let ids: Vec<String> = sqlx::query_scalar(
            "SELECT execution_id FROM workflow_executions \
             WHERE execution_id = $1 OR root_execution_id = $1",
        )
        .bind(root_execution_id)
        .fetch_all(&self.pool)
        .await
        .unwrap_or_default();
        for id in &ids {
            for sql in [
                "DELETE FROM child_workflow_executions WHERE parent_execution_id = $1",
                "DELETE FROM workflow_execution_journal WHERE execution_id = $1",
                "DELETE FROM workflow_execution_logs WHERE execution_id = $1",
                "DELETE FROM workflow_executions WHERE execution_id = $1",
            ] {
                let _ = sqlx::query(sql).bind(id).execute(&self.pool).await;
            }
        }
    }
}

fn start_child(child_workflow_id: &str, orphan_policy: i32, retry: Option<RetryPolicy>) -> Command {
    Command {
        command_type: CommandType::StartChildWorkflow as i32,
        attributes: Some(command::Attributes::StartChildWorkflow(
            StartChildWorkflowCommandAttributes {
                workflow_id: child_workflow_id.to_string(),
                workflow_type: "child-type".to_string(),
                task_queue: TASK_QUEUE.to_string(),
                input: br#"{"n":1}"#.to_vec(),
                orphan_policy,
                retry_policy: retry,
                ..Default::default()
            },
        )),
    }
}

fn complete_workflow(result: serde_json::Value) -> Command {
    Command {
        command_type: CommandType::CompleteWorkflow as i32,
        attributes: Some(command::Attributes::CompleteWorkflow(
            CompleteWorkflowCommandAttributes {
                result: serde_json::to_vec(&result).unwrap(),
            },
        )),
    }
}

fn fail_workflow(message: &str, failure_type: &str) -> Command {
    Command {
        command_type: CommandType::FailWorkflow as i32,
        attributes: Some(command::Attributes::FailWorkflow(
            FailWorkflowCommandAttributes {
                failure: Some(Failure {
                    message: message.to_string(),
                    failure_type: failure_type.to_string(),
                    ..Default::default()
                }),
            },
        )),
    }
}

/// A policy allowing `retries` retries: `maximum_attempts` counts the first
/// run too.
fn retries(retries: i32, non_retryable: &[&str]) -> RetryPolicy {
    RetryPolicy {
        initial_interval: Some(prost_types::Duration {
            seconds: 0,
            nanos: 500_000_000,
        }),
        backoff_coefficient: 2.0,
        maximum_interval: Some(prost_types::Duration {
            seconds: 10,
            nanos: 0,
        }),
        maximum_attempts: retries + 1,
        non_retryable_error_types: non_retryable.iter().map(|s| s.to_string()).collect(),
    }
}

/// The tracking row names the run the child actually executes as.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_child_is_tracked_under_the_id_it_runs_as() {
    let engine = Engine::new().await;
    let (parent_id, parent) = engine.claimed_workflow("parent-tracks-child").await;

    engine
        .complete(
            parent_id,
            &parent,
            vec![start_child("child_0", ORPHAN_ABANDON, None)],
        )
        .await;

    let child = engine.child(&parent, "child_0").await;
    assert_eq!(
        engine.tracked_run(&parent, "child_0").await,
        child.execution_id,
        "the tracking row must name the child's real execution"
    );

    engine.cleanup(&parent).await;
}

/// What the parent asked for its child's retries is what the child's run
/// carries — in the form the retry scheduler reads.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn the_retry_policy_of_a_child_is_stored_on_its_run() {
    let engine = Engine::new().await;
    let (parent_id, parent) = engine.claimed_workflow("parent-child-retry-policy").await;

    engine
        .complete(
            parent_id,
            &parent,
            vec![start_child(
                "child_0",
                ORPHAN_ABANDON,
                Some(retries(2, &["Fatal"])),
            )],
        )
        .await;

    let child = engine.child(&parent, "child_0").await;
    assert_eq!(child.max_retries, 2);
    let policy = child.retry_policy.expect("the child's policy is stored");
    assert_eq!(policy["initial_backoff_secs"], 0.5);
    assert_eq!(policy["max_backoff_secs"], 10.0);
    assert_eq!(policy["non_retryable_error_types"][0], "Fatal");

    engine.cleanup(&parent).await;
}

/// A failed attempt that will be retried is not the child's outcome; the
/// successor's is, and it reaches the parent — not the failed attempt.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_retried_child_reports_its_final_outcome_once_to_its_parent() {
    let engine = Engine::new().await;
    let (parent_id, parent) = engine.claimed_workflow("parent-retried-child").await;
    engine
        .complete(
            parent_id,
            &parent,
            vec![start_child(
                "child_0",
                ORPHAN_ABANDON,
                Some(retries(1, &[])),
            )],
        )
        .await;
    let first = engine.child(&parent, "child_0").await;

    engine.fail(&first, "first attempt failed").await;

    assert!(
        engine.child_outcomes(&parent).await.is_empty(),
        "a failure that will be retried must not reach the parent"
    );
    let first = engine.run(&first.execution_id).await;
    assert_eq!(first.status, "failed");
    assert!(
        first.next_retry_at.is_some(),
        "the retry must be scheduled when the failure is recorded"
    );

    engine
        .retries
        .handle_workflow_failure(&first)
        .await
        .expect("retry the child");
    let successor = engine
        .executions
        .find_latest_successor(&first.execution_id, "child_0")
        .await
        .unwrap()
        .expect("the retry created a successor");
    assert_eq!(
        engine.tracked_run(&parent, "child_0").await,
        successor.execution_id,
        "the tracking row must follow the child to its successor"
    );

    engine
        .complete(
            successor.id.as_uuid(),
            &successor.execution_id,
            vec![complete_workflow(serde_json::json!({ "ok": true }))],
        )
        .await;

    let outcomes = engine.child_outcomes(&parent).await;
    assert_eq!(outcomes.len(), 1, "exactly one outcome: {outcomes:?}");
    let (event_type, payload) = &outcomes[0];
    assert_eq!(event_type, "ChildWorkflowExecutionCompleted");
    assert_eq!(payload["child_workflow_id"], "child_0");
    assert_eq!(
        payload["child_execution_id"],
        successor.execution_id.as_str()
    );
    assert_eq!(payload["result"]["ok"], true);
    assert!(
        engine.child_outcomes(&first.execution_id).await.is_empty(),
        "the failed attempt is not the successor's parent"
    );
    assert_eq!(engine.tracked_state(&parent, "child_0").await, "COMPLETED");

    // The sweep finds nothing left to do for it.
    engine.reconciler.sweep().await.expect("sweep");
    assert_eq!(engine.child_outcomes(&parent).await.len(), 1);

    engine.cleanup(&parent).await;
}

/// Out of retries: the last attempt's failure reaches the parent once, and the
/// run is settled so the retry scheduler stops picking it up.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_child_out_of_retries_reports_once_and_is_settled() {
    let engine = Engine::new().await;
    let (parent_id, parent) = engine.claimed_workflow("parent-exhausted-child").await;
    engine
        .complete(
            parent_id,
            &parent,
            vec![start_child(
                "child_0",
                ORPHAN_ABANDON,
                Some(retries(1, &[])),
            )],
        )
        .await;
    let first = engine.child(&parent, "child_0").await;
    engine.fail(&first, "first attempt failed").await;
    let first = engine.run(&first.execution_id).await;
    engine
        .retries
        .handle_workflow_failure(&first)
        .await
        .expect("retry the child");
    let last = engine
        .executions
        .find_latest_successor(&first.execution_id, "child_0")
        .await
        .unwrap()
        .expect("successor");

    engine
        .complete(
            last.id.as_uuid(),
            &last.execution_id,
            vec![fail_workflow("second attempt failed", "")],
        )
        .await;

    let outcomes = engine.child_outcomes(&parent).await;
    assert_eq!(outcomes.len(), 1, "exactly one outcome: {outcomes:?}");
    assert_eq!(outcomes[0].0, "ChildWorkflowExecutionFailed");
    assert_eq!(
        outcomes[0].1["child_execution_id"],
        last.execution_id.as_str()
    );
    assert_eq!(outcomes[0].1["failure_reason"], "second attempt failed");

    let last = engine.run(&last.execution_id).await;
    assert_eq!(last.status, "failed");
    assert_eq!(last.retry_count, last.max_retries, "the failure is settled");
    let eligible = engine
        .executions
        .find_due_for_retry(1_000_000)
        .await
        .expect("eligible");
    assert!(
        !eligible.iter().any(|r| r.execution_id == last.execution_id),
        "a settled failure must not be picked up for retry again"
    );

    engine.cleanup(&parent).await;
}

/// A non-retryable failure is final at once, however many attempts are left.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_non_retryable_child_failure_is_final_at_once() {
    let engine = Engine::new().await;
    let (parent_id, parent) = engine.claimed_workflow("parent-fatal-child").await;
    engine
        .complete(
            parent_id,
            &parent,
            vec![start_child(
                "child_0",
                ORPHAN_ABANDON,
                Some(retries(3, &["Fatal"])),
            )],
        )
        .await;
    let child = engine.child(&parent, "child_0").await;

    engine
        .complete(
            child.id.as_uuid(),
            &child.execution_id,
            vec![fail_workflow("cannot go on", "Fatal")],
        )
        .await;

    let outcomes = engine.child_outcomes(&parent).await;
    assert_eq!(outcomes.len(), 1, "exactly one outcome: {outcomes:?}");
    assert_eq!(outcomes[0].0, "ChildWorkflowExecutionFailed");
    let child = engine.run(&child.execution_id).await;
    assert!(child.retries_settled_at.is_some(), "the failure is settled");
    assert_eq!(child.max_retries, 3, "settling keeps the policy's cap");
    assert!(child.next_retry_at.is_none());
    assert!(
        !engine
            .executions
            .find_due_for_retry(1_000_000)
            .await
            .unwrap()
            .iter()
            .any(|r| r.execution_id == child.execution_id),
        "a settled failure must not be retried"
    );

    engine.cleanup(&parent).await;
}

/// Cancelled through the API, the child reaches its parent — as a failure,
/// the only ending besides completion every SDK understands, typed so the
/// parent can tell it was a cancellation.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_child_cancelled_through_the_api_reaches_its_parent() {
    let engine = Engine::new().await;
    let (parent_id, parent) = engine.claimed_workflow("parent-cancelled-child").await;
    engine
        .complete(
            parent_id,
            &parent,
            vec![start_child("child_0", ORPHAN_ABANDON, None)],
        )
        .await;
    let child = engine.child(&parent, "child_0").await;

    engine
        .workflows
        .cancel_workflow(Request::new(CancelWorkflowRequest {
            execution_id: child.execution_id.clone(),
            namespace: NAMESPACE.to_string(),
            reason: "no longer needed".to_string(),
            ..Default::default()
        }))
        .await
        .expect("cancel the child");
    assert_eq!(engine.run(&child.execution_id).await.status, "cancelled");
    assert!(engine.child_outcomes(&parent).await.is_empty());

    engine.reconciler.sweep().await.expect("sweep");

    let outcomes = engine.child_outcomes(&parent).await;
    assert_eq!(outcomes.len(), 1, "exactly one outcome: {outcomes:?}");
    let (event_type, payload) = &outcomes[0];
    assert_eq!(event_type, "ChildWorkflowExecutionFailed");
    assert_eq!(payload["failure_type"], "ChildWorkflowCanceled");
    assert_eq!(payload["child_workflow_id"], "child_0");
    assert_eq!(engine.tracked_state(&parent, "child_0").await, "CANCELED");

    // And the parent's SDK sees the type.
    let entries =
        orcher_storage::repositories::domains::journal::JournalRepository::new(engine.pool.clone())
            .get_journal(&parent_id.to_string(), &parent)
            .await
            .expect("read journal");
    let failed = entries
        .iter()
        .find(|e| e.event_type == "ChildWorkflowExecutionFailed")
        .expect("outcome entry");
    match JournalMapper::new().to_proto(failed).unwrap().attributes {
        Some(Attributes::ChildWorkflowExecutionFailed(attrs)) => {
            assert_eq!(attrs.failure.unwrap().failure_type, "ChildWorkflowCanceled")
        }
        other => panic!("expected ChildWorkflowExecutionFailed, got {other:?}"),
    }

    engine.cleanup(&parent).await;
}

/// When the parent is done, a TERMINATE child is terminated and an ABANDON
/// child is left to run.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_finished_parent_applies_its_close_policies() {
    let engine = Engine::new().await;
    let (parent_id, parent) = engine.claimed_workflow("parent-close-policy").await;
    engine
        .complete(
            parent_id,
            &parent,
            vec![
                start_child("child_terminate", ORPHAN_TERMINATE, None),
                start_child("child_abandon", ORPHAN_ABANDON, None),
            ],
        )
        .await;
    let terminated = engine.child(&parent, "child_terminate").await;
    let abandoned = engine.child(&parent, "child_abandon").await;

    engine
        .complete(
            parent_id,
            &parent,
            vec![complete_workflow(serde_json::json!(null))],
        )
        .await;
    assert_eq!(engine.run(&parent).await.status, "completed");

    engine.reconciler.sweep().await.expect("sweep");

    assert_eq!(
        engine.run(&terminated.execution_id).await.status,
        "terminated"
    );
    assert_eq!(
        engine.tracked_state(&parent, "child_terminate").await,
        "TERMINATED"
    );
    assert_eq!(engine.run(&abandoned.execution_id).await.status, "running");
    assert_eq!(
        engine.tracked_state(&parent, "child_abandon").await,
        "INITIATED"
    );
    let outcomes = engine.child_outcomes(&parent).await;
    assert_eq!(outcomes.len(), 1, "{outcomes:?}");
    assert_eq!(outcomes[0].1["failure_type"], "ChildWorkflowTerminated");

    engine.cleanup(&parent).await;
}

/// A retried top-level workflow's successor points back at the failed run
/// through `parent_execution_id`, but that run is not its parent.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_retried_workflows_successor_does_not_report_to_its_predecessor() {
    let engine = Engine::new().await;
    let (_, first) = engine.claimed_workflow("retried-top-level").await;
    sqlx::query("UPDATE workflow_executions SET max_retries = 1 WHERE execution_id = $1")
        .bind(&first)
        .execute(&engine.pool)
        .await
        .expect("allow one retry");

    let run = engine.run(&first).await;
    engine.fail(&run, "first attempt failed").await;
    let run = engine.run(&first).await;
    engine
        .retries
        .handle_workflow_failure(&run)
        .await
        .expect("retry");
    let successor = engine
        .executions
        .find_latest_successor(&first, run.workflow_id.as_deref().unwrap())
        .await
        .unwrap()
        .expect("successor");
    assert_eq!(
        successor.parent_execution_id.as_deref(),
        Some(first.as_str())
    );

    engine
        .complete(
            successor.id.as_uuid(),
            &successor.execution_id,
            vec![complete_workflow(serde_json::json!({ "ok": true }))],
        )
        .await;

    assert!(
        engine.child_outcomes(&first).await.is_empty(),
        "the predecessor was told of its successor as if it were a child"
    );

    let _ = sqlx::query(
        "UPDATE workflow_executions SET root_execution_id = $1 WHERE execution_id = $2",
    )
    .bind(&first)
    .bind(&successor.execution_id)
    .execute(&engine.pool)
    .await;
    engine.cleanup(&first).await;
}

const ORPHAN_REQUEST_CANCEL: i32 = 2;

impl Engine {
    /// A claimed parent that has started one child, `child_0`.
    async fn parent_with_child(
        &self,
        name: &str,
        orphan_policy: i32,
        retry: Option<RetryPolicy>,
    ) -> (Uuid, String, WorkflowExecution) {
        let (parent_id, parent) = self.claimed_workflow(name).await;
        self.complete(
            parent_id,
            &parent,
            vec![start_child("child_0", orphan_policy, retry)],
        )
        .await;
        let child = self.child(&parent, "child_0").await;
        (parent_id, parent, child)
    }

    async fn cancel_via_api(&self, execution_id: &str) -> Result<(), tonic::Status> {
        self.workflows
            .cancel_workflow(Request::new(CancelWorkflowRequest {
                execution_id: execution_id.to_string(),
                namespace: NAMESPACE.to_string(),
                ..Default::default()
            }))
            .await
            .map(|_| ())
    }

    async fn terminate_via_api(&self, execution_id: &str) -> Result<(), tonic::Status> {
        self.workflows
            .terminate_workflow(Request::new(TerminateWorkflowRequest {
                execution_id: execution_id.to_string(),
                namespace: NAMESPACE.to_string(),
                ..Default::default()
            }))
            .await
            .map(|_| ())
    }

    /// The runs that replaced `execution_id`.
    async fn successors(&self, execution_id: &str, workflow_id: &str) -> i64 {
        sqlx::query_scalar(
            "SELECT COUNT(*) FROM workflow_executions \
             WHERE parent_execution_id = $1 AND workflow_id = $2",
        )
        .bind(execution_id)
        .bind(workflow_id)
        .fetch_one(&self.pool)
        .await
        .expect("count successors")
    }
}

/// A tracking row written by an engine from before rows named their run —
/// an old replica during a rolling deploy — still reaches its parent when the
/// child completes.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_legacy_record_is_found_when_its_child_completes() {
    let engine = Engine::new().await;
    let (_, parent, child) = engine
        .parent_with_child("parent-legacy-inline", ORPHAN_ABANDON, None)
        .await;
    engine
        .sql(
            "UPDATE child_workflow_executions SET child_execution_id = 'legacy-' || id \
             WHERE parent_execution_id = $1",
            &parent,
        )
        .await;

    engine
        .complete(
            child.id.as_uuid(),
            &child.execution_id,
            vec![complete_workflow(serde_json::json!(7))],
        )
        .await;

    let outcomes = engine.child_outcomes(&parent).await;
    assert_eq!(outcomes.len(), 1, "exactly one outcome: {outcomes:?}");
    assert_eq!(outcomes[0].0, "ChildWorkflowExecutionCompleted");
    assert_eq!(
        engine.tracked_run(&parent, "child_0").await,
        child.execution_id
    );
    assert_eq!(engine.wakes(&parent), 1);

    engine.cleanup(&parent).await;
}

/// A legacy record whose child was retried: the successor cannot be traced
/// inline (it points at its predecessor), so the sweep finds the child's
/// first run by its business id and follows the chain.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn the_sweep_finds_a_legacy_records_child_through_its_retries() {
    let engine = Engine::new().await;
    let (_, parent, first) = engine
        .parent_with_child("parent-legacy-sweep", ORPHAN_ABANDON, Some(retries(1, &[])))
        .await;
    engine
        .sql(
            "UPDATE child_workflow_executions SET child_execution_id = 'legacy-' || id \
             WHERE parent_execution_id = $1",
            &parent,
        )
        .await;
    engine.fail(&first, "first attempt failed").await;
    let first = engine.run(&first.execution_id).await;
    engine
        .retries
        .handle_workflow_failure(&first)
        .await
        .unwrap();
    let successor = engine
        .executions
        .find_latest_successor(&first.execution_id, "child_0")
        .await
        .unwrap()
        .expect("successor");
    engine
        .complete(
            successor.id.as_uuid(),
            &successor.execution_id,
            vec![complete_workflow(serde_json::json!("done"))],
        )
        .await;
    assert!(engine.child_outcomes(&parent).await.is_empty());

    engine.reconciler.sweep().await.expect("sweep");

    let outcomes = engine.child_outcomes(&parent).await;
    assert_eq!(outcomes.len(), 1, "exactly one outcome: {outcomes:?}");
    assert_eq!(
        outcomes[0].1["child_execution_id"],
        successor.execution_id.as_str()
    );
    assert_eq!(
        engine.tracked_run(&parent, "child_0").await,
        successor.execution_id
    );

    engine.cleanup(&parent).await;
}

/// Two sweeps — two replicas — over the same finished child: the outcome is
/// recorded once and the parent woken once. A second wake would release a
/// claim the parent may by then hold for a worker running it.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn two_sweeps_at_once_deliver_once_and_wake_the_parent_once() {
    let engine = Engine::new().await;
    let (_, parent, child) = engine
        .parent_with_child("parent-two-sweeps", ORPHAN_ABANDON, None)
        .await;
    engine.cancel_via_api(&child.execution_id).await.unwrap();
    engine.wakes(&parent);

    let (a, b) = (engine.reconciler(1_000_000), engine.reconciler(1_000_000));
    let (ra, rb) = tokio::join!(a.sweep(), b.sweep());
    let delivered = ra.unwrap().delivered + rb.unwrap().delivered;

    assert_eq!(delivered, 1, "exactly one sweep delivers");
    assert_eq!(engine.child_outcomes(&parent).await.len(), 1);
    assert_eq!(engine.wakes(&parent), 1, "the parent is woken once");

    engine.cleanup(&parent).await;
}

/// Two retry schedulers taking up the same failure create one successor.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn two_schedulers_retrying_one_failure_create_one_successor() {
    let engine = Engine::new().await;
    let (_, parent, first) = engine
        .parent_with_child("parent-double-retry", ORPHAN_ABANDON, Some(retries(2, &[])))
        .await;
    engine.fail(&first, "failed").await;
    let failed = engine.run(&first.execution_id).await;

    let other = WorkflowRetryScheduler::new(
        engine.executions.clone(),
        WorkflowRetrySchedulerConfig::default(),
    );
    let (a, b) = tokio::join!(
        engine.retries.handle_workflow_failure(&failed),
        other.handle_workflow_failure(&failed)
    );
    a.unwrap();
    b.unwrap();
    // And a late repeat, from a row read before either acted.
    engine
        .retries
        .handle_workflow_failure(&failed)
        .await
        .unwrap();

    assert_eq!(engine.successors(&first.execution_id, "child_0").await, 1);

    engine.cleanup(&parent).await;
}

/// A failure notification arrives as the status is written, before the
/// failure's fate is recorded. It must not decide that fate itself.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_failure_notification_leaves_the_failure_to_its_decision() {
    let engine = Engine::new().await;
    let (_, parent, first) = engine
        .parent_with_child(
            "parent-notified-failure",
            ORPHAN_ABANDON,
            Some(retries(2, &[])),
        )
        .await;

    // Recorded but not yet decided — the moment the notification fires.
    engine
        .sql(
            "UPDATE workflow_executions SET status = 'failed' WHERE execution_id = $1",
            &first.execution_id,
        )
        .await;
    engine
        .retries
        .process_single_workflow(&first.execution_id)
        .await
        .unwrap();
    assert_eq!(engine.successors(&first.execution_id, "child_0").await, 0);
    assert!(
        !engine
            .executions
            .find_due_for_retry(1_000_000)
            .await
            .unwrap()
            .iter()
            .any(|r| r.execution_id == first.execution_id),
        "an undecided failure is not eligible within its grace period"
    );

    // Decided: a retry after its backoff. Still not due, still left alone.
    engine
        .sql(
            "UPDATE workflow_executions SET status = 'running' WHERE execution_id = $1",
            &first.execution_id,
        )
        .await;
    engine.fail(&first, "failed").await;
    engine
        .retries
        .process_single_workflow(&first.execution_id)
        .await
        .unwrap();
    assert_eq!(engine.successors(&first.execution_id, "child_0").await, 0);

    engine.cleanup(&parent).await;
}

/// A failure from before failures were decided when recorded has no decision
/// on it, and must still be retried — once its grace period has passed.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn an_undecided_failure_from_before_the_upgrade_is_still_retried() {
    let engine = Engine::new().await;
    let insert = |age: &'static str| {
        let pool = engine.pool.clone();
        async move {
            let execution_id = Uuid::new_v4().to_string();
            sqlx::query(&format!(
                "INSERT INTO workflow_executions \
                 (id, execution_id, workflow_id, workflow_name, workflow_version, namespace, \
                  task_queue, status, max_retries, retry_count, trigger_type, root_execution_id, \
                  submitted_at, created_at, updated_at) \
                 VALUES ($1, $2, 'legacy-failure', 'legacy', '1.0', 'default', 'q', 'failed', \
                         1, 0, 'api', $2, NOW(), NOW(), NOW() - INTERVAL '{age}')"
            ))
            .bind(Uuid::new_v4())
            .bind(&execution_id)
            .execute(&pool)
            .await
            .expect("insert legacy failure");
            execution_id
        }
    };
    let old = insert("5 minutes").await;
    let fresh = insert("0 seconds").await;

    let eligible: Vec<String> = engine
        .executions
        .find_due_for_retry(1_000_000)
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.execution_id)
        .collect();
    assert!(
        eligible.contains(&old),
        "a legacy failure must still be retried"
    );
    assert!(
        !eligible.contains(&fresh),
        "a just-failed run awaits its decision"
    );

    engine.cleanup(&old).await;
    engine.cleanup(&fresh).await;
}

/// The failure type a worker reports is what the policy's non-retryable
/// types are matched against, and it is kept for the scheduler too.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_reported_failure_type_decides_a_non_retryable_failure() {
    let engine = Engine::new().await;
    let (_, parent, child) = engine
        .parent_with_child(
            "parent-typed-failure",
            ORPHAN_ABANDON,
            Some(retries(3, &["Fatal"])),
        )
        .await;

    engine
        .service
        .fail_workflow_execution(Request::new(FailWorkflowExecutionRequest {
            workflow_id: child.id.as_uuid().to_string(),
            execution_id: child.execution_id.clone(),
            namespace: NAMESPACE.to_string(),
            identity: WORKER.to_string(),
            failure: Some(Failure {
                message: "cannot go on".to_string(),
                failure_type: "Fatal".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        }))
        .await
        .unwrap();

    let child = engine.run(&child.execution_id).await;
    assert!(child.retries_settled_at.is_some(), "final at once");
    assert_eq!(
        child.error_details.as_ref().unwrap()["failure_type"],
        "Fatal"
    );
    let outcomes = engine.child_outcomes(&parent).await;
    assert_eq!(outcomes.len(), 1, "{outcomes:?}");
    assert_eq!(outcomes[0].1["failure_reason"], "cannot go on");

    engine.cleanup(&parent).await;
}

/// Cancelling a child whose retry is scheduled cancels the retry too: the
/// scheduler does not start it again, and the parent hears it was cancelled.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn cancelling_a_child_waiting_for_its_retry_wins() {
    let engine = Engine::new().await;
    let (_, parent, first) = engine
        .parent_with_child(
            "parent-cancel-pending-retry",
            ORPHAN_ABANDON,
            Some(retries(2, &[])),
        )
        .await;
    engine.fail(&first, "failed").await;
    let waiting = engine.run(&first.execution_id).await;
    assert!(waiting.next_retry_at.is_some());

    engine
        .cancel_via_api(&first.execution_id)
        .await
        .expect("a failure waiting for its retry can be cancelled");
    let cancelled = engine.run(&first.execution_id).await;
    assert_eq!(cancelled.status, "cancelled");
    assert!(cancelled.next_retry_at.is_none());

    // A scheduler holding the row from before the cancel does nothing.
    engine
        .retries
        .handle_workflow_failure(&waiting)
        .await
        .unwrap();
    assert_eq!(engine.successors(&first.execution_id, "child_0").await, 0);

    engine.reconciler.sweep().await.unwrap();
    let outcomes = engine.child_outcomes(&parent).await;
    assert_eq!(outcomes.len(), 1, "{outcomes:?}");
    assert_eq!(outcomes[0].1["failure_type"], "ChildWorkflowCanceled");

    engine.cleanup(&parent).await;
}

/// A failure reported after the run ended does not bring it back as a
/// failure — which would have made it retryable — and the worker is not
/// punished for the race.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_late_failure_does_not_undo_a_termination() {
    let engine = Engine::new().await;
    let (_, parent, child) = engine
        .parent_with_child("parent-late-failure", ORPHAN_ABANDON, Some(retries(2, &[])))
        .await;
    engine.terminate_via_api(&child.execution_id).await.unwrap();

    engine.fail(&child, "failed too late").await;
    let run = engine.run(&child.execution_id).await;
    assert_eq!(run.status, "terminated");
    assert!(run.next_retry_at.is_none());

    // Through a FailWorkflow command, too.
    engine
        .complete(
            child.id.as_uuid(),
            &child.execution_id,
            vec![fail_workflow("also too late", "")],
        )
        .await;
    assert_eq!(engine.run(&child.execution_id).await.status, "terminated");

    // And the refusal to terminate it again says how it ended.
    let refused = engine
        .terminate_via_api(&child.execution_id)
        .await
        .unwrap_err();
    assert!(
        refused
            .message()
            .contains("already ended with status: terminated"),
        "{}",
        refused.message()
    );

    engine.cleanup(&parent).await;
}

/// The CancelChildWorkflow command cancels the child's run; the parent hears
/// of it as a cancellation.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_parent_can_cancel_its_child_with_a_command() {
    let engine = Engine::new().await;
    let (parent_id, parent, child) = engine
        .parent_with_child("parent-cancels-child", ORPHAN_ABANDON, None)
        .await;

    engine
        .complete(
            parent_id,
            &parent,
            vec![Command {
                command_type: CommandType::CancelChildWorkflow as i32,
                attributes: Some(command::Attributes::CancelChildWorkflow(
                    CancelChildWorkflowCommandAttributes {
                        workflow_id: "child_0".to_string(),
                        run_id: String::new(),
                    },
                )),
            }],
        )
        .await;
    assert_eq!(engine.run(&child.execution_id).await.status, "cancelled");

    engine.reconciler.sweep().await.unwrap();
    let outcomes = engine.child_outcomes(&parent).await;
    assert_eq!(outcomes.len(), 1, "{outcomes:?}");
    assert_eq!(outcomes[0].1["failure_type"], "ChildWorkflowCanceled");

    engine.cleanup(&parent).await;
}

/// A child that continues as new stays its parent's child, and it is the
/// new run's outcome the parent receives.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_child_that_continues_as_new_reports_from_its_new_run() {
    let engine = Engine::new().await;
    let (_, parent, first) = engine
        .parent_with_child("parent-continued-child", ORPHAN_ABANDON, None)
        .await;

    engine
        .complete(
            first.id.as_uuid(),
            &first.execution_id,
            vec![Command {
                command_type: CommandType::RestartFresh as i32,
                attributes: Some(command::Attributes::RestartFresh(
                    RestartFreshCommandAttributes::default(),
                )),
            }],
        )
        .await;
    let next = engine
        .executions
        .find_latest_successor(&first.execution_id, "child_0")
        .await
        .unwrap()
        .expect("continued run");
    assert_eq!(
        engine.tracked_run(&parent, "child_0").await,
        next.execution_id
    );
    assert!(engine.child_outcomes(&parent).await.is_empty());

    engine
        .complete(
            next.id.as_uuid(),
            &next.execution_id,
            vec![complete_workflow(serde_json::json!("second"))],
        )
        .await;
    let outcomes = engine.child_outcomes(&parent).await;
    assert_eq!(outcomes.len(), 1, "{outcomes:?}");
    assert_eq!(
        outcomes[0].1["child_execution_id"],
        next.execution_id.as_str()
    );

    engine.cleanup(&parent).await;
}

/// Resetting a child keeps its successor its parent's child, and the reset
/// run carries the policy — cap and all — without the settled marker.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_reset_child_stays_its_parents_child_with_its_policy() {
    let engine = Engine::new().await;
    let (_, parent, child) = engine
        .parent_with_child(
            "parent-reset-child",
            ORPHAN_ABANDON,
            Some(retries(3, &["Fatal"])),
        )
        .await;
    engine
        .complete(
            child.id.as_uuid(),
            &child.execution_id,
            vec![fail_workflow("fatal", "Fatal")],
        )
        .await;
    assert!(engine
        .run(&child.execution_id)
        .await
        .retries_settled_at
        .is_some());
    engine.sql(
        "UPDATE child_workflow_executions SET state = 'INITIATED' WHERE parent_execution_id = $1",
        &parent,
    )
    .await;

    let reset = engine
        .workflows
        .reset_workflow(Request::new(ResetWorkflowRequest {
            execution_id: child.execution_id.clone(),
            namespace: NAMESPACE.to_string(),
            target_event_id: 1,
            ..Default::default()
        }))
        .await
        .expect("reset the child")
        .into_inner();

    assert_eq!(
        engine.tracked_run(&parent, "child_0").await,
        reset.new_execution_id
    );
    let run = engine.run(&reset.new_execution_id).await;
    assert_eq!(run.max_retries, 3, "the reset run keeps the policy's cap");
    assert!(run.retries_settled_at.is_none());

    engine.cleanup(&parent).await;
}

/// A child that times out reaches its parent as a typed failure.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_timed_out_child_reaches_its_parent() {
    let engine = Engine::new().await;
    let (_, parent, child) = engine
        .parent_with_child("parent-timed-out-child", ORPHAN_ABANDON, None)
        .await;
    engine
        .sql(
            "UPDATE workflow_executions SET timeout_secs = 1, \
             submitted_at = NOW() - INTERVAL '1 hour' WHERE execution_id = $1",
            &child.execution_id,
        )
        .await;
    let watcher = TimeoutWatcher::new(
        engine.pool.clone(),
        TimeoutWatcherConfig {
            enable_schedule_to_start: false,
            enable_start_to_close: false,
            enable_heartbeat: false,
            enable_workflow_timeout: true,
            grace_period_secs: 0,
            ..Default::default()
        },
    );
    watcher.check_timeouts().await.unwrap();
    assert_eq!(engine.run(&child.execution_id).await.status, "timed_out");

    engine.reconciler.sweep().await.unwrap();
    let outcomes = engine.child_outcomes(&parent).await;
    assert_eq!(outcomes.len(), 1, "{outcomes:?}");
    assert_eq!(outcomes[0].1["failure_type"], "ChildWorkflowTimedOut");
    assert_eq!(engine.tracked_state(&parent, "child_0").await, "TIMED_OUT");

    engine.cleanup(&parent).await;
}

/// REQUEST_CANCEL: the child of a finished parent is cancelled.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_finished_parent_cancels_a_request_cancel_child() {
    let engine = Engine::new().await;
    let (parent_id, parent, child) = engine
        .parent_with_child("parent-request-cancel", ORPHAN_REQUEST_CANCEL, None)
        .await;
    engine
        .complete(
            parent_id,
            &parent,
            vec![complete_workflow(serde_json::json!(null))],
        )
        .await;

    engine.reconciler.sweep().await.unwrap();

    assert_eq!(engine.run(&child.execution_id).await.status, "cancelled");
    assert_eq!(engine.tracked_state(&parent, "child_0").await, "CANCELED");
    assert_eq!(engine.wakes(&parent), 0, "a finished parent is not woken");

    engine.cleanup(&parent).await;
}

/// A record whose run was never created is closed once its parent is over,
/// without telling anyone; while the parent may still replay, it is left.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_record_with_no_run_is_closed_once_its_parent_is_over() {
    let engine = Engine::new().await;
    let (parent_id, parent) = engine.claimed_workflow("parent-zombie-child").await;
    ChildWorkflowRepository::new(engine.pool.clone())
        .create(&ChildWorkflowExecution::new(
            parent_id.to_string(),
            parent.clone(),
            1,
            "child_0".to_string(),
            Uuid::new_v4().to_string(),
            "child-type".to_string(),
            TASK_QUEUE.to_string(),
            serde_json::json!({}),
            OrphanPolicy::Abandon,
        ))
        .await
        .unwrap();

    engine.reconciler.sweep().await.unwrap();
    assert_eq!(
        engine.tracked_state(&parent, "child_0").await,
        "INITIATED",
        "the parent may still create it"
    );

    engine
        .complete(
            parent_id,
            &parent,
            vec![complete_workflow(serde_json::json!(null))],
        )
        .await;
    engine.reconciler.sweep().await.unwrap();
    assert_eq!(engine.tracked_state(&parent, "child_0").await, "FAILED");
    assert!(engine.child_outcomes(&parent).await.is_empty());

    engine.cleanup(&parent).await;
}

/// A sweep that takes a few rows at a time reaches every row, continuing
/// where it stopped — including rows behind a first batch that stays open.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_small_sweep_reaches_every_child_in_turn() {
    let engine = Engine::new().await;
    let (parent_id, parent) = engine.claimed_workflow("parent-many-children").await;
    let ids: Vec<String> = (0..7).map(|i| format!("child_{i}")).collect();
    engine
        .complete(
            parent_id,
            &parent,
            ids.iter()
                .map(|id| start_child(id, ORPHAN_ABANDON, None))
                .collect(),
        )
        .await;
    // The first two keep running, so they stay open and fill the first batch
    // on every pass that starts from the beginning.
    let ended = &ids[2..];
    for id in ended {
        let child = engine.child(&parent, id).await;
        engine.cancel_via_api(&child.execution_id).await.unwrap();
    }

    let sweep = engine.reconciler(2);
    let open: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM child_workflow_executions WHERE state IN ('INITIATED','STARTED')",
    )
    .fetch_one(&engine.pool)
    .await
    .unwrap();
    // Enough passes to walk every open row in the database twice over.
    for _ in 0..(open + 4) {
        sweep.sweep().await.unwrap();
    }

    let outcomes = engine.child_outcomes(&parent).await;
    assert_eq!(outcomes.len(), ended.len(), "{outcomes:?}");
    for id in ended {
        assert_eq!(engine.tracked_state(&parent, id).await, "CANCELED", "{id}");
    }
    for id in &ids[..2] {
        assert_eq!(engine.tracked_state(&parent, id).await, "INITIATED", "{id}");
    }

    engine.cleanup(&parent).await;
}

impl Engine {
    /// A top-level workflow, claimed and running, allowed one retry after
    /// `initial_backoff_secs`.
    async fn retryable_workflow(&self, name: &str, initial_backoff_secs: f64) -> WorkflowExecution {
        let (_, execution_id) = self.claimed_workflow(name).await;
        sqlx::query(
            "UPDATE workflow_executions SET max_retries = 1, retry_policy = $2 \
             WHERE execution_id = $1",
        )
        .bind(&execution_id)
        .bind(serde_json::json!({ "initial_backoff_secs": initial_backoff_secs }))
        .execute(&self.pool)
        .await
        .expect("allow one retry");
        self.run(&execution_id).await
    }
}

/// With realtime notifications on, a retry happens when its backoff has
/// passed, not at the next poll. The failure notification fires before the
/// failure's fate is decided, so on its own it can only defer to the poll;
/// the decision announces the retry and when it is due.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_retry_runs_when_due_without_waiting_for_the_poll() {
    let engine = Engine::new().await;
    let scheduler = Arc::new(WorkflowRetryScheduler::new_with_pool(
        engine.executions.clone(),
        WorkflowRetrySchedulerConfig {
            // Far longer than the test: only the realtime path can act.
            poll_interval_secs: 600,
            enable_realtime_notifications: true,
            ..Default::default()
        },
        Arc::new(engine.pool.clone()),
    ));
    let handle = scheduler.clone().start();
    // Let the first poll pass and the listener subscribe.
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;

    let run = engine.retryable_workflow("retried-in-realtime", 1.0).await;
    let workflow_id = run.workflow_id.clone().unwrap();
    engine.fail(&run, "transient").await;

    let started = std::time::Instant::now();
    let mut successors = 0;
    while started.elapsed() < std::time::Duration::from_secs(8) {
        successors = engine.successors(&run.execution_id, &workflow_id).await;
        if successors > 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    scheduler.stop().await;
    let _ = handle.await;

    assert_eq!(successors, 1, "the retry was not started when due");
    assert!(
        started.elapsed() >= std::time::Duration::from_millis(500),
        "the backoff was not honoured"
    );

    let successor = engine
        .executions
        .find_latest_successor(&run.execution_id, &workflow_id)
        .await
        .unwrap()
        .unwrap();
    engine
        .sql(
            "UPDATE workflow_executions SET root_execution_id = parent_execution_id \
             WHERE execution_id = $1",
            &successor.execution_id,
        )
        .await;
    engine.cleanup(&run.execution_id).await;
}

/// A child whose tracking row names no run is not matched with its parent's
/// own successor. A child run (child_0) that started a child of its own under
/// the same business id, never created, and was then retried: its successor
/// points at it under that id. Adopting the successor as the grandchild would
/// have the grandchild's TERMINATE policy kill the child's retry.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_record_with_no_run_does_not_adopt_its_parents_successor() {
    let engine = Engine::new().await;
    let (_, parent, first) = engine
        .parent_with_child("parent-no-adoption", ORPHAN_ABANDON, Some(retries(1, &[])))
        .await;
    ChildWorkflowRepository::new(engine.pool.clone())
        .create(&ChildWorkflowExecution::new(
            first.id.as_uuid().to_string(),
            first.execution_id.clone(),
            1,
            "child_0".to_string(),
            Uuid::new_v4().to_string(),
            "child-type".to_string(),
            TASK_QUEUE.to_string(),
            serde_json::json!({}),
            OrphanPolicy::Terminate,
        ))
        .await
        .unwrap();
    engine.fail(&first, "failed").await;
    let failed = engine.run(&first.execution_id).await;
    engine
        .retries
        .handle_workflow_failure(&failed)
        .await
        .unwrap();
    let retry = engine
        .executions
        .find_latest_successor(&first.execution_id, "child_0")
        .await
        .unwrap()
        .expect("the child was retried");
    // Only the grandchild's record: the retry is then named by no other row,
    // as when the parent's record sits where the sweep does not reach.
    engine
        .sql(
            "DELETE FROM child_workflow_executions WHERE parent_execution_id = $1",
            &parent,
        )
        .await;

    engine.reconciler.sweep().await.unwrap();

    assert_eq!(
        engine.run(&retry.execution_id).await.status,
        "running",
        "the grandchild's close policy was applied to the child's own retry"
    );
    assert_eq!(
        engine.tracked_state(&first.execution_id, "child_0").await,
        "FAILED",
        "the grandchild's record is closed: its run never existed"
    );

    let _ = sqlx::query("DELETE FROM child_workflow_executions WHERE parent_execution_id = $1")
        .bind(&first.execution_id)
        .execute(&engine.pool)
        .await;
    engine.cleanup(&parent).await;
}

/// A scheduler that took a run for a retry and stopped before creating its
/// successor left it replaced by nothing. Another pass finishes the retry, once.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_retry_whose_scheduler_stopped_half_way_is_finished() {
    let engine = Engine::new().await;
    let run = engine.retryable_workflow("stranded-retry", 0.0).await;
    let workflow_id = run.workflow_id.clone().unwrap();
    engine.fail(&run, "failed").await;

    // The first half of a retry, and nothing after it.
    assert!(engine
        .executions
        .claim_for_retry(run.id.as_uuid())
        .await
        .unwrap());
    assert_eq!(engine.successors(&run.execution_id, &workflow_id).await, 0);
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    let recovering = WorkflowRetryScheduler::new(
        engine.executions.clone(),
        WorkflowRetrySchedulerConfig {
            stranded_retry_grace_secs: 0,
            ..Default::default()
        },
    );
    assert_eq!(recovering.recover_stranded_retries().await.unwrap(), 1);
    assert_eq!(recovering.recover_stranded_retries().await.unwrap(), 0);

    assert_eq!(engine.successors(&run.execution_id, &workflow_id).await, 1);
    let replaced = engine.run(&run.execution_id).await;
    assert_eq!(replaced.status, "restarted_fresh");
    assert!(
        replaced.next_retry_at.is_none(),
        "the in-flight marker is cleared"
    );
    let successor = engine
        .executions
        .find_latest_successor(&run.execution_id, &workflow_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(successor.retry_count, 1);

    engine
        .sql(
            "UPDATE workflow_executions SET root_execution_id = parent_execution_id \
             WHERE execution_id = $1",
            &successor.execution_id,
        )
        .await;
    engine.cleanup(&run.execution_id).await;
}

/// A record naming no run is not moved onto a run another record already
/// names — a duplicate written before starts were idempotent.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_record_with_no_run_does_not_take_another_records_run() {
    let engine = Engine::new().await;
    let (parent_id, parent, child) = engine
        .parent_with_child("parent-duplicate-record", ORPHAN_ABANDON, None)
        .await;
    let duplicate = ChildWorkflowRepository::new(engine.pool.clone())
        .create(&ChildWorkflowExecution::new(
            parent_id.to_string(),
            parent.clone(),
            99,
            "child_0".to_string(),
            format!("legacy-{}", Uuid::new_v4()),
            "child-type".to_string(),
            TASK_QUEUE.to_string(),
            serde_json::json!({}),
            OrphanPolicy::Terminate,
        ))
        .await
        .unwrap();

    engine.reconciler.sweep().await.unwrap();

    let named: String = sqlx::query_scalar(
        "SELECT child_execution_id FROM child_workflow_executions WHERE id = $1",
    )
    .bind(duplicate.id)
    .fetch_one(&engine.pool)
    .await
    .unwrap();
    assert_eq!(
        named, duplicate.child_execution_id,
        "the duplicate took the child's run"
    );
    assert_eq!(engine.run(&child.execution_id).await.status, "running");

    engine.cleanup(&parent).await;
}
