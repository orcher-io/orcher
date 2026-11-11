//! What a workflow-level retry policy means, and what a client waiting on a
//! retried workflow is told.
//!
//! - A client waiting on a workflow's result is answered with the workflow's
//!   final outcome only. A run that fails with a retry still to come is not
//!   that outcome: the wait goes on, follows the workflow to the retry, and
//!   is answered with the retry's outcome. The completion path
//!   used to answer it with every failure.
//! - `RetryPolicy.maximum_attempts` counts runs, the first one included, as a
//!   task's does: `N` allows `N` runs, `1` no retry, and `0` is unlimited.
//!   It used to be stored as the retry count, one run more than
//!   it said, and `0` meant no retry.
//! - A retry with a short backoff starts when its backoff ends, not at the
//!   retry scheduler's next poll.
//!
//! Requires PostgreSQL. Run with:
//!
//! ```bash
//! export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/orcher_test"
//! cargo test -p orcher-core --test workflow_retry_semantics -- --ignored --test-threads=1
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};

use orcher_core::engine::dispatch::{PollingManager, TaskPollingManager};
use orcher_core::engine::queues::outbox::OutboxQueue;
use orcher_core::engine::WorkflowCoordinator;
use orcher_core::events::EventBus;
use orcher_core::grpc::services::{ExecutionService, WorkflowService};
use orcher_core::workflows::execution::run_is_final;
use orcher_core::workflows::{
    ChildWorkflowManager, CommandProcessor, JournalService, RetryOutcome, WorkflowExecutionService,
    WorkflowRetryScheduler, WorkflowRetrySchedulerConfig,
};
use orcher_events::bus::EventBusConfig;
use orcher_proto::orcher::v1::execution_service_server::ExecutionService as ExecutionServiceTrait;
use orcher_proto::orcher::v1::get_workflow_result_response::Outcome;
use orcher_proto::orcher::v1::workflow_service_server::WorkflowService as WorkflowServiceTrait;
use orcher_proto::orcher::v1::{
    command, BatchStartWorkflowRequest, Command, CommandType, CompleteWorkflowCommandAttributes,
    CompleteWorkflowExecutionRequest, FailWorkflowCommandAttributes, Failure,
    GetWorkflowResultRequest, GetWorkflowResultResponse, PollWorkflowExecutionRequest,
    PollWorkflowExecutionResponse, RestartFreshCommandAttributes, RetryPolicy,
    StartWorkflowRequest, WorkflowStatus,
};
use orcher_proto::prost_types;
use orcher_storage::repositories::domains::{
    child_workflow::ChildWorkflowRepository, coordination::OutboxRepository,
    journal::JournalRepository, task_execution::TaskExecutionRepository,
    workflow_execution::WorkflowExecutionRepository,
};
use orcher_storage::repositories::{ExecutionLogRepository, PostgresExecutionLogRepository};
use orcher_storage::schema::types::execution::{WorkflowExecution, UNLIMITED_RETRIES};
use sqlx::PgPool;
use tonic::Request;
use uuid::Uuid;

const NAMESPACE: &str = "default";
const WORKER: &str = "retry-semantics-worker";

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
        let task_queue = format!("wf-retry-{}", Uuid::new_v4().simple());
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
        // The waiter and the completion path share one notifier, as the
        // engine wires them.
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
        self.start_with(Some(policy(maximum_attempts))).await
    }

    async fn start_with(&self, retry_policy: Option<RetryPolicy>) -> WorkflowExecution {
        let response = self
            .workflows
            .start_workflow(Request::new(self.request(retry_policy)))
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

    async fn answer(&self, activation: &PollWorkflowExecutionResponse, command: Command) {
        self.execution
            .complete_workflow_execution(Request::new(CompleteWorkflowExecutionRequest {
                workflow_id: activation.workflow_id.clone(),
                execution_id: activation.execution_id.clone(),
                namespace: NAMESPACE.to_string(),
                identity: WORKER.to_string(),
                stream_entry_id: activation.stream_entry_id.clone(),
                commands: vec![command],
                ..Default::default()
            }))
            .await
            .expect("complete the activation");
    }

    async fn fail(&self, activation: &PollWorkflowExecutionResponse, message: &str) {
        self.answer(
            activation,
            Command {
                command_type: CommandType::FailWorkflow as i32,
                attributes: Some(command::Attributes::FailWorkflow(
                    FailWorkflowCommandAttributes {
                        failure: Some(Failure {
                            message: message.to_string(),
                            failure_type: "Boom".to_string(),
                            ..Default::default()
                        }),
                    },
                )),
            },
        )
        .await;
    }

    async fn succeed(&self, activation: &PollWorkflowExecutionResponse, result: &[u8]) {
        self.answer(
            activation,
            Command {
                command_type: CommandType::CompleteWorkflow as i32,
                attributes: Some(command::Attributes::CompleteWorkflow(
                    CompleteWorkflowCommandAttributes {
                        result: result.to_vec(),
                    },
                )),
            },
        )
        .await;
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

    /// Fail every run of a workflow, retrying each as the scheduler would,
    /// until one is not retried or `bound` runs have failed. Returns how many
    /// runs there were.
    async fn fail_every_run(&self, bound: usize) -> usize {
        let mut runs = 0;
        loop {
            let activation = self.activation().await;
            runs += 1;
            self.fail(&activation, &format!("run {runs} fails")).await;
            if runs == bound {
                return runs;
            }
            let failed = self.run(&activation.execution_id).await;
            if run_is_final(&failed) {
                return runs;
            }
            match self.retry(&activation.execution_id).await {
                RetryOutcome::Rescheduled { .. } => {}
                other => {
                    // The failure path decided it was final as it recorded
                    // it, so the scheduler has nothing to retry.
                    assert!(
                        !matches!(other, RetryOutcome::AlreadyHandled { .. }),
                        "{other:?}"
                    );
                    return runs;
                }
            }
        }
    }

    async fn run_count(&self) -> i64 {
        sqlx::query_scalar("SELECT COUNT(*) FROM workflow_executions WHERE task_queue = $1")
            .bind(&self.task_queue)
            .fetch_one(&self.pool)
            .await
            .expect("count the runs")
    }

    /// Wait on the workflow's result as a client does, in the background.
    fn wait_for_result(
        &self,
        run: &WorkflowExecution,
    ) -> tokio::task::JoinHandle<Result<GetWorkflowResultResponse, tonic::Status>> {
        let workflows = self.workflows.clone();
        let request = GetWorkflowResultRequest {
            workflow_id: run.workflow_id.clone().unwrap_or_default(),
            namespace: NAMESPACE.to_string(),
            timeout: Some(prost_types::Duration {
                seconds: 30,
                nanos: 0,
            }),
            ..Default::default()
        };
        tokio::spawn(async move {
            workflows
                .get_workflow_result(Request::new(request))
                .await
                .map(|r| r.into_inner())
        })
    }

    /// Until a client is waiting on `execution_id`'s result.
    async fn until_waited_on(&self, execution_id: &str) {
        let notifier = self.workflows.completion_notifier();
        let deadline = Instant::now() + Duration::from_secs(10);
        while !notifier.has_waiters(execution_id) {
            assert!(Instant::now() < deadline, "no client came to wait");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
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

/// A client waiting on a workflow whose run fails with a retry to come is
/// not handed that failure: it waits on, and is answered with the outcome of
/// the run that retries it.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_result_waiter_is_answered_by_the_retry_not_the_failure_before_it() {
    let engine = Engine::new().await;
    let run = engine.start(2).await;

    let waiter = engine.wait_for_result(&run);
    engine.until_waited_on(&run.execution_id).await;

    let first = engine.activation().await;
    assert_eq!(first.execution_id, run.execution_id);
    engine.fail(&first, "first attempt fails").await;
    let failed = engine.run(&run.execution_id).await;
    assert_eq!(failed.status, "failed");
    assert!(
        failed.next_retry_at.is_some(),
        "the failure is to be retried"
    );

    // Longer than the completion path takes to hand an outcome over.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !waiter.is_finished(),
        "the client was answered with a failure the workflow is about to retry: {:?}",
        waiter.await
    );

    assert!(matches!(
        engine.retry(&run.execution_id).await,
        RetryOutcome::Rescheduled { .. }
    ));
    let second = engine.activation().await;
    assert_ne!(
        second.execution_id, run.execution_id,
        "the retry is its own run"
    );
    engine.succeed(&second, br#"{"attempt":2}"#).await;

    let answer = tokio::time::timeout(Duration::from_secs(15), waiter)
        .await
        .expect("the client is answered once the retry completes")
        .expect("the waiter ran")
        .expect("the result");
    assert_eq!(answer.status, WorkflowStatus::Completed as i32);
    assert_eq!(
        answer.execution.as_ref().map(|e| e.execution_id.as_str()),
        Some(second.execution_id.as_str()),
        "the answer is the retry's"
    );
    assert_eq!(
        answer.outcome,
        Some(Outcome::Result(br#"{"attempt":2}"#.to_vec()))
    );

    engine.cleanup().await;
}

/// The failure of the last run allowed is the workflow's outcome, and a
/// client waiting on it is told at once.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_final_failure_answers_the_waiter_at_once() {
    let engine = Engine::new().await;
    let run = engine.start(1).await;

    let waiter = engine.wait_for_result(&run);
    engine.until_waited_on(&run.execution_id).await;
    let first = engine.activation().await;
    engine.fail(&first, "the only attempt fails").await;

    // Well inside the waiter's own re-read of the chain, so only the
    // completion path's hand-over answers it this soon.
    let answer = tokio::time::timeout(Duration::from_millis(1500), waiter)
        .await
        .expect("a final failure is handed over as it is recorded")
        .expect("the waiter ran")
        .expect("the result");
    assert_eq!(answer.status, WorkflowStatus::Failed as i32);
    assert_eq!(
        answer.outcome,
        Some(Outcome::Error("the only attempt fails".to_string()))
    );

    engine.cleanup().await;
}

/// `maximum_attempts = 2` is two runs: the first and one retry.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn maximum_attempts_2_runs_the_workflow_twice() {
    let engine = Engine::new().await;
    let run = engine.start(2).await;
    assert_eq!(run.max_retries, 1, "two attempts are one retry");

    assert_eq!(engine.fail_every_run(10).await, 2);
    assert_eq!(engine.run_count().await, 2);

    engine.cleanup().await;
}

/// `maximum_attempts = 1` is the first run alone: its failure is final.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn maximum_attempts_1_does_not_retry() {
    let engine = Engine::new().await;
    let run = engine.start(1).await;
    assert_eq!(run.max_retries, 0);

    assert_eq!(engine.fail_every_run(10).await, 1);
    let failed = engine.run(&run.execution_id).await;
    assert!(run_is_final(&failed), "the only run's failure is final");
    assert!(failed.next_retry_at.is_none(), "no retry is scheduled");
    assert_eq!(engine.run_count().await, 1);

    engine.cleanup().await;
}

/// `maximum_attempts = 0` is unlimited: every failure is retried.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn maximum_attempts_0_retries_without_limit() {
    let engine = Engine::new().await;
    let run = engine.start(0).await;
    assert_eq!(run.max_retries, UNLIMITED_RETRIES);

    // Bounded here; nothing in the policy ends it.
    assert_eq!(engine.fail_every_run(6).await, 6);
    assert_eq!(engine.run_count().await, 6);
    let latest: String = sqlx::query_scalar(
        "SELECT execution_id FROM workflow_executions WHERE task_queue = $1 \
         ORDER BY retry_count DESC LIMIT 1",
    )
    .bind(&engine.task_queue)
    .fetch_one(&engine.pool)
    .await
    .expect("the latest run");
    let latest = engine.run(&latest).await;
    assert_eq!(latest.retry_count, 5);
    assert!(!run_is_final(&latest), "the sixth failure is retried too");
    assert!(latest.next_retry_at.is_some());

    engine.cleanup().await;
}

/// A retry with a short backoff starts when its backoff ends — woken by its
/// failure — not at the retry scheduler's next poll, here an hour away.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_short_backoff_retry_starts_promptly() {
    let engine = Engine::new().await;
    let scheduler = Arc::new(WorkflowRetryScheduler::new_with_pool(
        engine.runs.clone(),
        WorkflowRetrySchedulerConfig {
            poll_interval_secs: 3600,
            enable_realtime_notifications: false,
            ..Default::default()
        },
        Arc::new(engine.pool.clone()),
    ));
    let handle = scheduler.clone().start();
    // The scheduler's first poll, at start, is behind it before the failure.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let run = engine.start(2).await;
    let first = engine.activation().await;
    assert_eq!(first.execution_id, run.execution_id);
    let failed_at = Instant::now();
    engine.fail(&first, "first attempt fails").await;

    let retry = engine.poll(Duration::from_secs(10)).await;
    let waited = failed_at.elapsed();
    scheduler.stop().await;
    let _ = handle.await;
    let retry = retry.expect("the retry was not started within 10s");
    assert_ne!(retry.execution_id, run.execution_id);
    assert!(
        waited < Duration::from_secs(5),
        "a 200ms backoff took {waited:?} to start its retry"
    );

    engine.cleanup().await;
}

/// A batch start converts each workflow's policy as a single start does, and
/// a workflow started without one is not retried. The batch used to drop the
/// policy and give every workflow three retries.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_batch_start_keeps_each_workflows_policy() {
    let engine = Engine::new().await;
    let started = engine
        .workflows
        .batch_start_workflow(Request::new(BatchStartWorkflowRequest {
            workflows: vec![engine.request(Some(policy(3))), engine.request(None)],
        }))
        .await
        .expect("batch start")
        .into_inner();
    assert_eq!(started.executions.len(), 2);
    let with_policy = engine.run(&started.executions[0].execution_id).await;
    assert_eq!(with_policy.max_retries, 2, "three attempts are two retries");
    assert!(with_policy.retry_policy.is_some());
    let without = engine.run(&started.executions[1].execution_id).await;
    assert_eq!(without.max_retries, 0, "no policy, no retry");

    engine.cleanup().await;
}

/// A negative `maximum_attempts` means nothing, and is refused.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_negative_maximum_attempts_is_refused() {
    let engine = Engine::new().await;
    let refused = engine
        .workflows
        .start_workflow(Request::new(engine.request(Some(policy(-1)))))
        .await
        .expect_err("a negative attempt count is refused");
    assert_eq!(refused.code(), tonic::Code::InvalidArgument);
    assert_eq!(engine.run_count().await, 0);
}

/// A workflow that continues as new goes on under its own retry policy, its
/// retries counted afresh; one started without a policy is still not retried.
/// The new run used to get three retries and no policy whatever the workflow
/// was started with.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn continuing_as_new_keeps_the_retry_policy() {
    let engine = Engine::new().await;
    for (start_policy, expected) in [(Some(policy(2)), 1), (None, 0)] {
        let run = engine.start_with(start_policy).await;
        let activation = engine.activation().await;
        assert_eq!(activation.execution_id, run.execution_id);
        engine
            .answer(
                &activation,
                Command {
                    command_type: CommandType::RestartFresh as i32,
                    attributes: Some(command::Attributes::RestartFresh(
                        RestartFreshCommandAttributes {
                            workflow_type: "orders".to_string(),
                            task_queue: engine.task_queue.clone(),
                            ..Default::default()
                        },
                    )),
                },
            )
            .await;
        let next_activation = engine.activation().await;
        assert_ne!(
            next_activation.execution_id, run.execution_id,
            "the new run is its own"
        );
        let next = engine.run(&next_activation.execution_id).await;
        assert_eq!(next.max_retries, expected);
        assert_eq!(next.retry_policy, run.retry_policy);
        assert_eq!(next.retry_count, 0);
        // Leave the queue empty for the next case.
        engine.succeed(&next_activation, b"null").await;
    }

    engine.cleanup().await;
}
