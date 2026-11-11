//! A workflow failure raised as non-retryable is not retried.
//!
//! - A failure the workflow code raised as non-retryable ends the workflow,
//!   whatever its retry policy allows, reported either way a worker fails a
//!   workflow: a FailWorkflow command or a FailWorkflowExecution call. The
//!   retry scheduler, reading the run again, decides the same.
//! - Any other failure is retried as the policy says.
//! - The mark is honoured only from a worker reporting protocol version 2 or
//!   later. Older workers mark every workflow failure non-retryable whatever
//!   the code raised, so their failures keep being retried by the policy.
//!
//! Requires PostgreSQL. Run with:
//!
//! ```bash
//! export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/orcher_test"
//! cargo test -p orcher-core --test workflow_non_retryable_failure -- --ignored --test-threads=1
//! ```

use std::sync::Arc;
use std::time::Duration;

use orcher_core::engine::dispatch::{PollingManager, TaskPollingManager};
use orcher_core::engine::queues::outbox::OutboxQueue;
use orcher_core::engine::WorkflowCoordinator;
use orcher_core::events::EventBus;
use orcher_core::grpc::services::{ExecutionService, WorkflowService};
use orcher_core::workflows::execution::run_is_final;
use orcher_core::workflows::{
    AbandonReason, ChildWorkflowManager, CommandProcessor, JournalService, RetryOutcome,
    WorkflowExecutionService, WorkflowRetryScheduler, WorkflowRetrySchedulerConfig,
};
use orcher_events::bus::EventBusConfig;
use orcher_proto::orcher::v1::execution_service_server::ExecutionService as ExecutionServiceTrait;
use orcher_proto::orcher::v1::workflow_service_server::WorkflowService as WorkflowServiceTrait;
use orcher_proto::orcher::v1::{
    command, Command, CommandType, CompleteWorkflowExecutionRequest, FailWorkflowCommandAttributes,
    FailWorkflowExecutionRequest, Failure, PollWorkflowExecutionRequest,
    PollWorkflowExecutionResponse, RetryPolicy, StartWorkflowRequest,
};
use orcher_proto::prost_types;
use orcher_storage::repositories::domains::{
    child_workflow::ChildWorkflowRepository, coordination::OutboxRepository,
    journal::JournalRepository, task_execution::TaskExecutionRepository,
    workflow_execution::WorkflowExecutionRepository,
};
use orcher_storage::repositories::{ExecutionLogRepository, PostgresExecutionLogRepository};
use orcher_storage::schema::types::execution::WorkflowExecution;
use sqlx::PgPool;
use tonic::Request;
use uuid::Uuid;

const NAMESPACE: &str = "default";
const WORKER: &str = "non-retryable-worker";

async fn pool() -> PgPool {
    let url = std::env::var("TEST_DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:postgres@localhost:5432/orcher_test".to_string()
    });
    let pool = PgPool::connect(&url)
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

/// A policy retrying after 200ms, for `maximum_attempts` attempts.
fn policy(maximum_attempts: i32) -> RetryPolicy {
    RetryPolicy {
        initial_interval: Some(prost_types::Duration {
            seconds: 0,
            nanos: 200_000_000,
        }),
        backoff_coefficient: 1.0,
        maximum_interval: Some(prost_types::Duration {
            seconds: 0,
            nanos: 200_000_000,
        }),
        maximum_attempts,
        non_retryable_error_types: vec![],
    }
}

struct Engine {
    pool: PgPool,
    task_queue: String,
    runs: Arc<WorkflowExecutionRepository>,
    workflows: Arc<WorkflowService>,
    execution: ExecutionService,
}

impl Engine {
    async fn new() -> Self {
        let pool = pool().await;
        let task_queue = format!("wf-non-retryable-{}", Uuid::new_v4().simple());
        let journal_repo = Arc::new(JournalRepository::new(pool.clone()));
        let journal_service = Arc::new(JournalService::new(journal_repo.clone()));
        let runs = Arc::new(WorkflowExecutionRepository::new(pool.clone()));
        let outbox_queue = Arc::new(OutboxQueue::new(Arc::new(OutboxRepository::new(Arc::new(
            pool.clone(),
        )))));
        let event_bus = EventBus::new(EventBusConfig::default());
        let execution_log_repo = Arc::new(PostgresExecutionLogRepository::new(pool.clone()));
        let task_execution_repo = Arc::new(TaskExecutionRepository::new(pool.clone()));
        let polling = Arc::new(PollingManager::new(runs.clone(), journal_repo.clone(), 300));
        let starts = Arc::new(
            WorkflowExecutionService::new(
                runs.clone(),
                journal_service.clone(),
                outbox_queue.clone(),
                event_bus.clone(),
                pool.clone(),
            )
            .with_polling_manager(polling.clone()),
        );
        let command_processor = CommandProcessor::new(
            journal_service.clone(),
            Arc::new(ChildWorkflowManager::new(
                Arc::new(ChildWorkflowRepository::new(pool.clone())),
                journal_service,
                starts.clone(),
            )),
            event_bus,
            execution_log_repo.clone(),
            task_execution_repo.clone(),
            runs.clone(),
            Arc::new(pool.clone()),
        )
        .with_coordinator(Arc::new(
            WorkflowCoordinator::new(
                Arc::new(pool.clone()),
                orcher_core::engine::CoordinatorConfig::default(),
            )
            .await
            .expect("create coordinator"),
        ))
        .with_polling_manager(polling.clone());
        let workflows = Arc::new(WorkflowService::new(starts, runs.clone()));
        let execution = ExecutionService::new(
            Arc::new(command_processor),
            polling,
            execution_log_repo as Arc<dyn ExecutionLogRepository>,
            journal_repo,
            runs.clone(),
            task_execution_repo,
            pool.clone(),
        )
        .with_task_polling_manager(Arc::new(TaskPollingManager::new(outbox_queue)))
        .with_workflow_completion_notifier(workflows.completion_notifier());
        Self {
            pool,
            task_queue,
            runs,
            workflows,
            execution,
        }
    }

    /// Start a workflow through the gRPC start path, so its policy is
    /// converted as a client's is.
    async fn start(&self, maximum_attempts: i32) -> WorkflowExecution {
        let response = self
            .workflows
            .start_workflow(Request::new(self.request(Some(policy(maximum_attempts)))))
            .await
            .expect("start the workflow")
            .into_inner();
        self.run(&response.execution_id).await
    }

    fn request(&self, retry_policy: Option<RetryPolicy>) -> StartWorkflowRequest {
        StartWorkflowRequest {
            workflow_id: format!("wf-{}", Uuid::new_v4().simple()),
            workflow_type: "orders".to_string(),
            task_queue: self.task_queue.clone(),
            namespace: NAMESPACE.to_string(),
            retry_policy,
            ..Default::default()
        }
    }

    async fn run(&self, execution_id: &str) -> WorkflowExecution {
        self.runs
            .find_by_execution_id(execution_id)
            .await
            .expect("read the run")
            .expect("the run exists")
    }

    /// The next activation of this engine's queue, or `None` if none is
    /// handed out within `wait`.
    async fn poll(&self, wait: Duration) -> Option<PollWorkflowExecutionResponse> {
        let activation = self
            .execution
            .poll_workflow_execution(Request::new(PollWorkflowExecutionRequest {
                task_queue: self.task_queue.clone(),
                namespace: NAMESPACE.to_string(),
                identity: WORKER.to_string(),
                poll_timeout: Some(prost_types::Duration {
                    seconds: wait.as_secs() as i64,
                    nanos: wait.subsec_nanos() as i32,
                }),
                ..Default::default()
            }))
            .await
            .expect("poll for the workflow")
            .into_inner();
        (!activation.execution_id.is_empty()).then_some(activation)
    }

    async fn activation(&self) -> PollWorkflowExecutionResponse {
        self.poll(Duration::from_secs(10))
            .await
            .expect("the workflow was not handed out")
    }

    /// Fail the activation's run, `non_retryable` as marked, from a worker
    /// reporting `protocol_version`, the way `by` says.
    async fn fail(
        &self,
        activation: &PollWorkflowExecutionResponse,
        by: Report,
        non_retryable: bool,
        protocol_version: u32,
    ) {
        let failure = Failure {
            message: "out of stock".to_string(),
            failure_type: "OutOfStock".to_string(),
            non_retryable,
            ..Default::default()
        };
        match by {
            Report::Command => {
                self.execution
                    .complete_workflow_execution(Request::new(CompleteWorkflowExecutionRequest {
                        workflow_id: activation.workflow_id.clone(),
                        execution_id: activation.execution_id.clone(),
                        namespace: NAMESPACE.to_string(),
                        identity: WORKER.to_string(),
                        stream_entry_id: activation.stream_entry_id.clone(),
                        commands: vec![Command {
                            command_type: CommandType::FailWorkflow as i32,
                            attributes: Some(command::Attributes::FailWorkflow(
                                FailWorkflowCommandAttributes {
                                    failure: Some(failure),
                                },
                            )),
                        }],
                        protocol_version,
                        ..Default::default()
                    }))
                    .await
                    .expect("fail the workflow by command");
            }
            Report::Call => {
                self.execution
                    .fail_workflow_execution(Request::new(FailWorkflowExecutionRequest {
                        workflow_id: activation.workflow_id.clone(),
                        execution_id: activation.execution_id.clone(),
                        namespace: NAMESPACE.to_string(),
                        identity: WORKER.to_string(),
                        failure: Some(failure),
                        protocol_version,
                        ..Default::default()
                    }))
                    .await
                    .expect("fail the workflow by call");
            }
        }
    }

    /// Take the retry of a failed run now, as the retry scheduler does once
    /// its backoff has passed.
    async fn retry(&self, execution_id: &str) -> RetryOutcome {
        let failed = self.run(execution_id).await;
        WorkflowRetryScheduler::new(self.runs.clone(), WorkflowRetrySchedulerConfig::default())
            .handle_workflow_failure(&failed)
            .await
            .expect("decide the retry")
    }

    async fn run_count(&self) -> i64 {
        sqlx::query_scalar("SELECT COUNT(*) FROM workflow_executions WHERE task_queue = $1")
            .bind(&self.task_queue)
            .fetch_one(&self.pool)
            .await
            .expect("count the runs")
    }

    async fn cleanup(&self) {
        for q in [
            "DELETE FROM workflow_execution_journal WHERE execution_id IN \
             (SELECT execution_id FROM workflow_executions WHERE task_queue = $1)",
            "DELETE FROM workflow_execution_logs WHERE execution_id IN \
             (SELECT execution_id FROM workflow_executions WHERE task_queue = $1)",
            "DELETE FROM execution_log WHERE workflow_id IN \
             (SELECT id FROM workflow_executions WHERE task_queue = $1)",
            "DELETE FROM outbox_queue WHERE workflow_id IN \
             (SELECT id FROM workflow_executions WHERE task_queue = $1)",
            "DELETE FROM workflow_executions WHERE task_queue = $1",
        ] {
            let _ = sqlx::query(q)
                .bind(&self.task_queue)
                .execute(&self.pool)
                .await;
        }
    }
}

/// The two ways a worker reports a workflow's failure.
#[derive(Clone, Copy, Debug)]
enum Report {
    /// A FailWorkflow command, completing the activation.
    Command,
    /// A FailWorkflowExecution call.
    Call,
}

/// The worker protocol version from which the mark is honoured.
const MEANT: u32 = 2;

/// A failure raised as non-retryable is final at once, with retries left
/// under the policy and a type it does not list, reported either way. The
/// retry scheduler, reading the run again, does not retry it either.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_failure_raised_as_non_retryable_is_not_retried() {
    let engine = Engine::new().await;
    for by in [Report::Command, Report::Call] {
        let run = engine.start(3).await;
        let activation = engine.activation().await;
        assert_eq!(activation.execution_id, run.execution_id);
        engine.fail(&activation, by, true, MEANT).await;

        let failed = engine.run(&run.execution_id).await;
        assert_eq!(failed.status, "failed", "{by:?}");
        assert!(run_is_final(&failed), "{by:?}: the failure is final");
        assert!(
            failed.next_retry_at.is_none(),
            "{by:?}: no retry is scheduled"
        );
        assert_eq!(
            failed
                .error_details
                .as_ref()
                .and_then(|d| d.get("non_retryable")),
            Some(&serde_json::Value::Bool(true)),
            "{by:?}: the mark is recorded with the failure"
        );
        let decided = engine.retry(&run.execution_id).await;
        assert!(
            matches!(
                &decided,
                RetryOutcome::Abandoned {
                    reason: AbandonReason::NonRetryableFailure,
                    ..
                }
            ),
            "{by:?}: {decided:?}"
        );
        assert!(
            engine.poll(Duration::from_millis(500)).await.is_none(),
            "{by:?}: no retry was started"
        );
    }
    assert_eq!(engine.run_count().await, 2, "one run each, never retried");

    engine.cleanup().await;
}

/// A failure not raised as non-retryable is retried as the policy says:
/// `maximum_attempts = 2` is two runs.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_retryable_failure_is_retried_by_the_policy() {
    let engine = Engine::new().await;
    for by in [Report::Command, Report::Call] {
        let run = engine.start(2).await;
        let first = engine.activation().await;
        assert_eq!(first.execution_id, run.execution_id);
        engine.fail(&first, by, false, MEANT).await;
        let failed = engine.run(&run.execution_id).await;
        assert!(
            !run_is_final(&failed),
            "{by:?}: the first failure is retried"
        );
        assert!(failed.next_retry_at.is_some(), "{by:?}");
        assert!(
            matches!(
                engine.retry(&run.execution_id).await,
                RetryOutcome::Rescheduled { .. }
            ),
            "{by:?}"
        );

        let second = engine.activation().await;
        assert_ne!(
            second.execution_id, run.execution_id,
            "{by:?}: the retry is its own run"
        );
        engine.fail(&second, by, false, MEANT).await;
        let failed = engine.run(&second.execution_id).await;
        assert!(
            run_is_final(&failed),
            "{by:?}: the second run was the last allowed"
        );
    }
    assert_eq!(engine.run_count().await, 4);

    engine.cleanup().await;
}

/// A worker below protocol version 2 marks every workflow failure
/// non-retryable whatever its code raised, so its mark is not honoured: the
/// failure is retried by the policy, as it was before the mark was.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn an_older_workers_mark_is_not_honoured() {
    let engine = Engine::new().await;
    for older in [0, 1] {
        for by in [Report::Command, Report::Call] {
            let run = engine.start(3).await;
            let activation = engine.activation().await;
            assert_eq!(activation.execution_id, run.execution_id);
            engine.fail(&activation, by, true, older).await;

            let failed = engine.run(&run.execution_id).await;
            assert!(
                !run_is_final(&failed),
                "version {older}, {by:?}: the failure is retried"
            );
            assert!(failed.next_retry_at.is_some(), "version {older}, {by:?}");
            assert_eq!(
                failed
                    .error_details
                    .as_ref()
                    .and_then(|d| d.get("non_retryable")),
                Some(&serde_json::Value::Bool(false)),
                "version {older}, {by:?}: the mark is not recorded"
            );
            assert!(
                matches!(
                    engine.retry(&run.execution_id).await,
                    RetryOutcome::Rescheduled { .. }
                ),
                "version {older}, {by:?}"
            );
            // The retry, left for the next case.
            let retry = engine.activation().await;
            assert_ne!(retry.execution_id, run.execution_id);
            engine.fail(&retry, by, true, MEANT).await;
        }
    }

    engine.cleanup().await;
}
