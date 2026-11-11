//! A start's run timeout and task timeout are kept and enforced.
//!
//! StartWorkflow accepted `run_timeout` and `task_timeout` and dropped both:
//! a run went on past its run timeout, and an activation a worker never
//! answered stayed with that worker for the engine's whole claim timeout. Both
//! are now stored on the run. A run past its run timeout ends timed out, as a
//! run past its execution timeout does; an activation claimed for longer than
//! the run's task timeout lapses and is handed to another worker.
//!
//! A run timeout is retried under the workflow's retry policy, as a failure
//! is, by a fresh run with its own run clock; until then the timed-out run is
//! not the workflow's outcome. The execution timeout bounds the workflow
//! across all its runs: it is one deadline, fixed by the start and carried by
//! every successor run, a run past it is never retried, and no run of the
//! workflow starts after it.
//!
//! Requires PostgreSQL. The schema is created by the engine's own migration
//! runner. Run with:
//!
//! ```bash
//! export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/orcher_test"
//! cargo test -p orcher-core --test start_timeouts -- --ignored --test-threads=1
//! ```

use std::sync::Arc;
use std::time::Duration;

use orcher_common::types::dispatch::polling::PollExecutionRequest;
use orcher_core::engine::dispatch::PollingManager;
use orcher_core::engine::queues::outbox::OutboxQueue;
use orcher_core::engine::WorkflowCoordinator;
use orcher_core::events::EventBus;
use orcher_core::grpc::services::{ExecutionService, WorkflowService};
use orcher_core::retries::timeout_watcher::{TimeoutWatcher, TimeoutWatcherConfig};
use orcher_core::workflows::execution::run_is_final;
use orcher_core::workflows::{
    ChildWorkflowManager, CommandProcessor, JournalService, WorkflowExecutionService,
    WorkflowRetryScheduler, WorkflowRetrySchedulerConfig,
};
use orcher_events::bus::EventBusConfig;
use orcher_proto::orcher::v1::execution_service_server::ExecutionService as ExecutionServiceTrait;
use orcher_proto::orcher::v1::workflow_service_server::WorkflowService as WorkflowServiceTrait;
use orcher_proto::orcher::v1::{
    command, Command, CommandType, CompleteWorkflowExecutionRequest, GetWorkflowResultRequest,
    ResetWorkflowRequest, RestartFreshCommandAttributes, RetryPolicy,
    SendEventWithStartWorkflowRequest, StartWorkflowRequest, WorkflowStatus,
};
use orcher_proto::prost_types;
use orcher_storage::repositories::domains::workflow_execution::HandOut;
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
const CLAIM_TIMEOUT_SECS: i64 = 300;

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

struct Engine {
    pool: PgPool,
    task_queue: String,
    executions: Arc<WorkflowExecutionRepository>,
    polling: Arc<PollingManager>,
    workflows: WorkflowService,
    service: ExecutionService,
}

impl Engine {
    async fn new() -> Self {
        let pool = pool().await;
        let task_queue = format!("start-timeouts-{}", Uuid::new_v4().simple());
        let journal_repo = Arc::new(JournalRepository::new(pool.clone()));
        let executions = Arc::new(WorkflowExecutionRepository::new(pool.clone()));
        let outbox_queue = Arc::new(OutboxQueue::new(Arc::new(OutboxRepository::new(Arc::new(
            pool.clone(),
        )))));
        let polling = Arc::new(PollingManager::new(
            executions.clone(),
            journal_repo.clone(),
            CLAIM_TIMEOUT_SECS,
        ));
        let journal_service = Arc::new(JournalService::new(journal_repo.clone()));
        let event_bus = EventBus::new(EventBusConfig::default());
        let execution_service = Arc::new(WorkflowExecutionService::new(
            executions.clone(),
            journal_service.clone(),
            outbox_queue,
            event_bus.clone(),
            pool.clone(),
        ));
        // The completion path, for a run that continues as new.
        let execution_log_repo = Arc::new(PostgresExecutionLogRepository::new(pool.clone()));
        let task_execution_repo = Arc::new(TaskExecutionRepository::new(pool.clone()));
        let command_processor = CommandProcessor::new(
            journal_service.clone(),
            Arc::new(ChildWorkflowManager::new(
                Arc::new(ChildWorkflowRepository::new(pool.clone())),
                journal_service,
                execution_service.clone(),
            )),
            event_bus,
            execution_log_repo.clone(),
            task_execution_repo.clone(),
            executions.clone(),
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
        let service = ExecutionService::new(
            Arc::new(command_processor),
            polling.clone(),
            execution_log_repo as Arc<dyn ExecutionLogRepository>,
            journal_repo.clone(),
            executions.clone(),
            task_execution_repo,
            pool.clone(),
        );
        let workflows = WorkflowService::new(execution_service, executions.clone())
            .with_event_support(journal_repo, polling.clone());
        Self {
            pool,
            task_queue,
            executions,
            polling,
            workflows,
            service,
        }
    }

    /// Start a run of this engine's queue under `workflow_id`.
    async fn start_as(
        &self,
        workflow_id: &str,
        request: StartWorkflowRequest,
    ) -> WorkflowExecution {
        let response = self
            .workflows
            .start_workflow(Request::new(StartWorkflowRequest {
                workflow_id: workflow_id.to_string(),
                workflow_type: "orders".to_string(),
                task_queue: self.task_queue.clone(),
                namespace: NAMESPACE.to_string(),
                ..request
            }))
            .await
            .expect("start the workflow")
            .into_inner();
        self.run(&response.execution_id).await
    }

    /// Move a run `ago` into the past, as if that long had gone by since it
    /// started: its start, and the execution deadline it carries with it.
    async fn aged(&self, execution_id: &str, ago: Duration) {
        sqlx::query(
            "UPDATE workflow_executions \
             SET submitted_at = submitted_at - make_interval(secs => $2), \
                 execution_deadline = execution_deadline - make_interval(secs => $2) \
             WHERE execution_id = $1",
        )
        .bind(execution_id)
        .bind(ago.as_secs_f64())
        .execute(&self.pool)
        .await
        .expect("age the run");
    }

    /// The run that replaced `execution_id`, if any.
    async fn successor_of(&self, execution_id: &str) -> Option<WorkflowExecution> {
        let id: Option<String> = sqlx::query_scalar(
            "SELECT execution_id FROM workflow_executions WHERE parent_execution_id = $1",
        )
        .bind(execution_id)
        .fetch_optional(&self.pool)
        .await
        .expect("read the successor");
        match id {
            Some(id) => Some(self.run(&id).await),
            None => None,
        }
    }

    /// Make the retry scheduled for `execution_id` due now.
    async fn retry_due(&self, execution_id: &str) {
        sqlx::query(
            "UPDATE workflow_executions SET next_retry_at = NOW() - INTERVAL '1 second' \
             WHERE execution_id = $1",
        )
        .bind(execution_id)
        .execute(&self.pool)
        .await
        .expect("make the retry due");
    }

    /// Fail a run with its retry due now, as the failure path leaves one.
    async fn failed_with_retry_due(&self, execution_id: &str) {
        sqlx::query(
            "UPDATE workflow_executions SET status = 'failed', error = 'boom', \
             completed_at = NOW(), claimed_by = NULL, \
             next_retry_at = NOW() - INTERVAL '1 second' WHERE execution_id = $1",
        )
        .bind(execution_id)
        .execute(&self.pool)
        .await
        .expect("fail the run");
    }

    fn retries(&self) -> WorkflowRetryScheduler {
        WorkflowRetryScheduler::new(
            self.executions.clone(),
            WorkflowRetrySchedulerConfig {
                stranded_retry_grace_secs: 0,
                ..Default::default()
            },
        )
    }

    /// The final status a client waiting on the workflow is given, and the
    /// run it comes from — `None` if it has none within a second.
    async fn result(&self, workflow_id: &str, execution_id: &str) -> Option<(i32, String)> {
        match self
            .workflows
            .get_workflow_result(Request::new(GetWorkflowResultRequest {
                workflow_id: workflow_id.to_string(),
                execution_id: execution_id.to_string(),
                namespace: NAMESPACE.to_string(),
                timeout: secs(1, 0),
            }))
            .await
        {
            Ok(response) => {
                let response = response.into_inner();
                Some((
                    response.status,
                    response
                        .execution
                        .map(|e| e.execution_id)
                        .unwrap_or_default(),
                ))
            }
            Err(status) if status.code() == tonic::Code::DeadlineExceeded => None,
            Err(status) => panic!("result: {status}"),
        }
    }

    /// Answer the activation `identity` was handed with `commands`.
    async fn complete(&self, run: &WorkflowExecution, identity: &str, commands: Vec<Command>) {
        self.service
            .complete_workflow_execution(Request::new(CompleteWorkflowExecutionRequest {
                workflow_id: run.id.as_uuid().to_string(),
                execution_id: run.execution_id.clone(),
                namespace: NAMESPACE.to_string(),
                identity: identity.to_string(),
                commands,
                ..Default::default()
            }))
            .await
            .expect("complete the workflow task");
    }

    /// Reset `run` to its first event.
    async fn reset(&self, run: &WorkflowExecution) -> Result<String, tonic::Status> {
        self.workflows
            .reset_workflow(Request::new(ResetWorkflowRequest {
                execution_id: run.execution_id.clone(),
                namespace: NAMESPACE.to_string(),
                target_event_id: 1,
                ..Default::default()
            }))
            .await
            .map(|response| response.into_inner().new_execution_id)
    }

    /// Start a run of this engine's queue through StartWorkflow.
    async fn start(&self, request: StartWorkflowRequest) -> WorkflowExecution {
        let response = self
            .workflows
            .start_workflow(Request::new(StartWorkflowRequest {
                workflow_id: format!("wf-{}", Uuid::new_v4()),
                workflow_type: "orders".to_string(),
                task_queue: self.task_queue.clone(),
                namespace: NAMESPACE.to_string(),
                ..request
            }))
            .await
            .expect("start the workflow")
            .into_inner();
        self.run(&response.execution_id).await
    }

    async fn run(&self, execution_id: &str) -> WorkflowExecution {
        self.executions
            .find_by_execution_id(execution_id)
            .await
            .expect("read the run")
            .expect("the run exists")
    }

    /// Move a run's start `ago` into the past, as if it had been running
    /// that long.
    async fn started_ago(&self, execution_id: &str, ago: Duration) {
        sqlx::query(
            "UPDATE workflow_executions \
             SET submitted_at = NOW() - make_interval(secs => $2) WHERE execution_id = $1",
        )
        .bind(execution_id)
        .bind(ago.as_secs_f64())
        .execute(&self.pool)
        .await
        .expect("backdate the run");
    }

    /// One pass of the workflow timeout check, with no grace period.
    async fn check_workflow_timeouts(&self) {
        TimeoutWatcher::new(
            self.pool.clone(),
            TimeoutWatcherConfig {
                enable_schedule_to_start: false,
                enable_start_to_close: false,
                enable_heartbeat: false,
                enable_workflow_timeout: true,
                grace_period_secs: 0,
                ..Default::default()
            },
        )
        .check_timeouts()
        .await
        .expect("check timeouts");
    }

    async fn poll(&self, identity: &str) -> Option<String> {
        self.polling
            .poll_execution(PollExecutionRequest::new(
                self.task_queue.clone(),
                NAMESPACE,
                identity,
                Duration::from_millis(500),
            ))
            .await
            .expect("poll")
            .map(|response| response.execution_id)
    }

    async fn cleanup(&self) {
        for q in [
            "DELETE FROM workflow_execution_journal WHERE execution_id IN \
             (SELECT execution_id FROM workflow_executions WHERE task_queue = $1)",
            "DELETE FROM workflow_execution_logs WHERE execution_id IN \
             (SELECT execution_id FROM workflow_executions WHERE task_queue = $1)",
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

fn secs(seconds: i64, nanos: i32) -> Option<prost_types::Duration> {
    Some(prost_types::Duration { seconds, nanos })
}

/// The seconds between a claim and the time it lapses.
fn claim_secs(run: &WorkflowExecution) -> f64 {
    let claimed_at = run.claimed_at.expect("claimed");
    let lapses_at = run.claim_timeout_at.expect("claim lapses");
    (lapses_at.timestamp_millis() - claimed_at.timestamp_millis()) as f64 / 1000.0
}

#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_start_keeps_its_run_and_task_timeouts() {
    let engine = Engine::new().await;

    let run = engine
        .start(StartWorkflowRequest {
            execution_timeout: secs(600, 0),
            run_timeout: secs(30, 0),
            // Sub-second remainders round up, as for the execution timeout.
            task_timeout: secs(1, 500_000_000),
            ..Default::default()
        })
        .await;
    assert_eq!(run.timeout_secs, Some(600));
    assert_eq!(run.run_timeout_secs, Some(30), "run_timeout was dropped");
    assert_eq!(run.task_timeout_secs, Some(2), "task_timeout was dropped");

    let unlimited = engine.start(StartWorkflowRequest::default()).await;
    assert_eq!(unlimited.run_timeout_secs, None);
    assert_eq!(unlimited.task_timeout_secs, None);

    // An event-with-start that starts the workflow keeps them as well.
    let started = engine
        .workflows
        .send_event_with_start_workflow(Request::new(SendEventWithStartWorkflowRequest {
            workflow_id: format!("wf-{}", Uuid::new_v4()),
            namespace: NAMESPACE.to_string(),
            event_name: "go".to_string(),
            event_payload: b"{}".to_vec(),
            workflow_type: "orders".to_string(),
            task_queue: engine.task_queue.clone(),
            run_timeout: secs(45, 0),
            task_timeout: secs(5, 0),
            request_id: Uuid::new_v4().to_string(),
            ..Default::default()
        }))
        .await
        .expect("event-with-start")
        .into_inner();
    assert!(started.workflow_started);
    let run = engine.run(&started.execution_id).await;
    assert_eq!(run.run_timeout_secs, Some(45));
    assert_eq!(run.task_timeout_secs, Some(5));

    engine.cleanup().await;
}

#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_run_past_its_run_timeout_ends_timed_out() {
    let engine = Engine::new().await;
    let run = engine
        .start(StartWorkflowRequest {
            run_timeout: secs(60, 0),
            retry_policy: Some(RetryPolicy {
                maximum_attempts: 3,
                ..Default::default()
            }),
            ..Default::default()
        })
        .await;
    engine
        .started_ago(&run.execution_id, Duration::from_secs(120))
        .await;

    engine.check_workflow_timeouts().await;

    let ended = engine.run(&run.execution_id).await;
    assert_eq!(
        ended.status, "timed_out",
        "the run outlived its run timeout"
    );
    assert_eq!(ended.error.as_deref(), Some("Workflow run timed out"));
    assert!(ended.completed_at.is_some(), "an ended run carries its end");
    assert!(ended.claimed_by.is_none());
    // Its retry policy is applied as for an execution timeout.
    assert!(
        ended.next_retry_at.is_some(),
        "a run timeout is retried as an execution timeout is"
    );

    engine.cleanup().await;
}

#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_run_inside_its_run_timeout_goes_on() {
    let engine = Engine::new().await;
    let inside = engine
        .start(StartWorkflowRequest {
            run_timeout: secs(3600, 0),
            ..Default::default()
        })
        .await;
    engine
        .started_ago(&inside.execution_id, Duration::from_secs(120))
        .await;
    // Long-running with no limits at all.
    let unlimited = engine.start(StartWorkflowRequest::default()).await;
    engine
        .started_ago(&unlimited.execution_id, Duration::from_secs(86_400))
        .await;
    // Inside its run timeout but past its execution timeout: the execution
    // timeout still ends it, and says so.
    let past_execution = engine
        .start(StartWorkflowRequest {
            execution_timeout: secs(60, 0),
            run_timeout: secs(3600, 0),
            ..Default::default()
        })
        .await;
    engine
        .aged(&past_execution.execution_id, Duration::from_secs(120))
        .await;

    engine.check_workflow_timeouts().await;

    assert_eq!(engine.run(&inside.execution_id).await.status, "running");
    assert_eq!(engine.run(&unlimited.execution_id).await.status, "running");
    let ended = engine.run(&past_execution.execution_id).await;
    assert_eq!(ended.status, "timed_out");
    assert_eq!(ended.error.as_deref(), Some("Workflow execution timed out"));

    engine.cleanup().await;
}

/// A retry is a new run: it carries both limits, and its run clock starts
/// when it does.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_retry_carries_the_limits_and_starts_its_own_run_clock() {
    let engine = Engine::new().await;
    let run = engine
        .start(StartWorkflowRequest {
            run_timeout: secs(60, 0),
            task_timeout: secs(7, 0),
            retry_policy: Some(RetryPolicy {
                maximum_attempts: 3,
                initial_interval: secs(1, 0),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await;
    engine
        .started_ago(&run.execution_id, Duration::from_secs(50))
        .await;
    sqlx::query(
        "UPDATE workflow_executions SET status = 'failed', error = 'boom', \
         completed_at = NOW(), next_retry_at = NOW() - INTERVAL '1 second' \
         WHERE execution_id = $1",
    )
    .bind(&run.execution_id)
    .execute(&engine.pool)
    .await
    .expect("fail the run");

    let scheduler = WorkflowRetryScheduler::new(
        engine.executions.clone(),
        WorkflowRetrySchedulerConfig::default(),
    );
    scheduler
        .process_single_workflow(&run.execution_id)
        .await
        .expect("retry the run");

    let successor_id: String = sqlx::query_scalar(
        "SELECT execution_id FROM workflow_executions WHERE parent_execution_id = $1",
    )
    .bind(&run.execution_id)
    .fetch_one(&engine.pool)
    .await
    .expect("the retry was created");
    let successor = engine.run(&successor_id).await;
    assert_eq!(successor.run_timeout_secs, Some(60));
    assert_eq!(successor.task_timeout_secs, Some(7));

    // Twenty seconds on, the failed run would be past its run timeout; the
    // retry, which started its own clock, is not.
    engine
        .started_ago(&successor_id, Duration::from_secs(20))
        .await;
    engine.check_workflow_timeouts().await;
    assert_eq!(engine.run(&successor_id).await.status, "running");

    engine.cleanup().await;
}

#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn an_unanswered_activation_is_handed_on_after_the_task_timeout() {
    let engine = Engine::new().await;
    let run = engine
        .start(StartWorkflowRequest {
            task_timeout: secs(1, 0),
            ..Default::default()
        })
        .await;

    assert_eq!(
        engine.poll("worker-a").await.as_deref(),
        Some(run.execution_id.as_str())
    );
    let claimed = engine.run(&run.execution_id).await;
    assert_eq!(claimed.claimed_by.as_deref(), Some("worker-a"));
    assert!(
        claim_secs(&claimed) <= 2.0,
        "the claim runs for the engine's {CLAIM_TIMEOUT_SECS}s, not the run's 1s: {}s",
        claim_secs(&claimed)
    );

    // worker-a never answers.
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    let reclaimed = engine.executions.reclaim_expired().await.expect("reclaim");
    assert!(
        reclaimed.contains(&run.execution_id),
        "the unanswered activation was not taken back"
    );

    assert_eq!(
        engine.poll("worker-b").await.as_deref(),
        Some(run.execution_id.as_str()),
        "the activation was not handed to another worker"
    );
    let handed_on = engine.run(&run.execution_id).await;
    assert_eq!(handed_on.claimed_by.as_deref(), Some("worker-b"));
    assert_eq!(handed_on.activation_seq, claimed.activation_seq + 1);

    engine.cleanup().await;
}

/// Every claim takes the run's task timeout: the batch claim, the hand-off
/// out of the prefetch buffer, and the claim of a named run. A run without
/// one keeps the engine's.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn every_claim_takes_the_runs_task_timeout() {
    let engine = Engine::new().await;
    let limited = engine
        .start(StartWorkflowRequest {
            task_timeout: secs(4, 0),
            ..Default::default()
        })
        .await;
    let unlimited = engine.start(StartWorkflowRequest::default()).await;

    let batch = engine
        .executions
        .claim_batch_with_offset(
            &engine.task_queue,
            NAMESPACE,
            "engine",
            CLAIM_TIMEOUT_SECS,
            10,
            0,
            None,
            None,
            &[],
        )
        .await
        .expect("claim a batch");
    assert_eq!(batch.len(), 2);
    for run in &batch {
        let expected = if run.execution_id == limited.execution_id {
            4.0
        } else {
            CLAIM_TIMEOUT_SECS as f64
        };
        assert!(
            (claim_secs(run) - expected).abs() < 1.0,
            "batch claim of {} runs {}s, not {expected}s",
            run.execution_id,
            claim_secs(run)
        );
    }

    let limited_claim = batch
        .iter()
        .find(|run| run.execution_id == limited.execution_id)
        .expect("claimed");
    assert_eq!(
        engine
            .executions
            .hand_to(
                &limited.execution_id,
                limited_claim.activation_seq,
                "worker-a",
                CLAIM_TIMEOUT_SECS,
                None,
                None,
            )
            .await
            .expect("hand out"),
        HandOut::Handed
    );
    let handed = engine.run(&limited.execution_id).await;
    assert!(
        (claim_secs(&handed) - 4.0).abs() < 1.0,
        "the hand-off restarted the claim for {}s, not 4s",
        claim_secs(&handed)
    );

    engine
        .executions
        .release_claim(&limited.execution_id)
        .await
        .expect("release");
    let by_id = engine
        .executions
        .claim_execution_by_id(
            &limited.execution_id,
            "worker-b",
            CLAIM_TIMEOUT_SECS,
            None,
            None,
        )
        .await
        .expect("claim by id")
        .expect("claimable");
    assert!(
        (claim_secs(&by_id) - 4.0).abs() < 1.0,
        "the claim by id runs {}s, not 4s",
        claim_secs(&by_id)
    );
    let _ = unlimited;

    engine.cleanup().await;
}

/// A run past its run timeout, with retries left under its policy, is retried
/// as a failure is: by a fresh run with its own run clock. Until the retry is
/// taken the workflow is open — its id is held, and a client waiting on it is
/// not told it timed out — and afterwards its result is the retry's.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_run_timeout_is_retried_by_a_fresh_run() {
    let engine = Engine::new().await;
    let workflow_id = format!("wf-{}", Uuid::new_v4());
    let run = engine
        .start_as(
            &workflow_id,
            StartWorkflowRequest {
                run_timeout: secs(60, 0),
                retry_policy: Some(RetryPolicy {
                    maximum_attempts: 3,
                    initial_interval: secs(1, 0),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .await;
    engine
        .started_ago(&run.execution_id, Duration::from_secs(120))
        .await;

    engine.check_workflow_timeouts().await;

    let timed_out = engine.run(&run.execution_id).await;
    assert_eq!(timed_out.status, "timed_out");
    assert_eq!(timed_out.error.as_deref(), Some("Workflow run timed out"));
    assert!(timed_out.next_retry_at.is_some(), "no retry was scheduled");
    assert!(timed_out.retries_settled_at.is_none());
    assert!(
        !run_is_final(&timed_out),
        "a run timeout with its retry to come was taken as the outcome"
    );
    let refused = engine
        .workflows
        .start_workflow(Request::new(StartWorkflowRequest {
            workflow_id: workflow_id.clone(),
            workflow_type: "orders".to_string(),
            task_queue: engine.task_queue.clone(),
            namespace: NAMESPACE.to_string(),
            ..Default::default()
        }))
        .await
        .expect_err("a second run of a workflow waiting for its retry");
    assert_eq!(refused.code(), tonic::Code::AlreadyExists);
    assert_eq!(
        engine.result(&workflow_id, "").await,
        None,
        "a client was told the workflow ended while its retry was to come"
    );

    engine.retry_due(&run.execution_id).await;
    let due = engine
        .executions
        .find_due_for_retry(1_000_000)
        .await
        .expect("find due retries");
    assert!(
        due.iter().any(|r| r.execution_id == run.execution_id),
        "the retry scheduler does not see the timed-out run"
    );
    engine
        .retries()
        .process_single_workflow(&run.execution_id)
        .await
        .expect("retry the run");

    assert_eq!(
        engine.run(&run.execution_id).await.status,
        "restarted_fresh"
    );
    let retry = engine
        .successor_of(&run.execution_id)
        .await
        .expect("the run timeout was not retried");
    assert_eq!(retry.status, "running");
    assert_eq!(retry.retry_count, 1);
    assert_eq!(retry.run_timeout_secs, Some(60));
    assert_eq!(retry.workflow_id.as_deref(), Some(workflow_id.as_str()));

    // Twenty seconds in, the retry is inside its run timeout: its clock
    // started with it.
    engine
        .started_ago(&retry.execution_id, Duration::from_secs(20))
        .await;
    engine.check_workflow_timeouts().await;
    assert_eq!(engine.run(&retry.execution_id).await.status, "running");

    sqlx::query(
        "UPDATE workflow_executions SET status = 'completed', result = '\"done\"'::jsonb, \
         completed_at = NOW() WHERE execution_id = $1",
    )
    .bind(&retry.execution_id)
    .execute(&engine.pool)
    .await
    .expect("complete the retry");
    let completed = Some((WorkflowStatus::Completed as i32, retry.execution_id.clone()));
    assert_eq!(engine.result(&workflow_id, "").await, completed);
    assert_eq!(
        engine.result("", &run.execution_id).await,
        completed,
        "the timed-out run's result does not follow it to its retry"
    );

    engine.cleanup().await;
}

/// A run timeout the policy will not retry — its type listed as not
/// retryable — is the workflow's outcome at once.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_run_timeout_the_policy_does_not_retry_is_the_outcome() {
    let engine = Engine::new().await;
    let workflow_id = format!("wf-{}", Uuid::new_v4());
    let run = engine
        .start_as(
            &workflow_id,
            StartWorkflowRequest {
                run_timeout: secs(60, 0),
                retry_policy: Some(RetryPolicy {
                    maximum_attempts: 3,
                    non_retryable_error_types: vec!["WorkflowRunTimedOut".to_string()],
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .await;
    engine
        .started_ago(&run.execution_id, Duration::from_secs(120))
        .await;

    engine.check_workflow_timeouts().await;

    let timed_out = engine.run(&run.execution_id).await;
    assert_eq!(timed_out.status, "timed_out");
    assert!(timed_out.next_retry_at.is_none());
    assert!(timed_out.retries_settled_at.is_some());
    assert!(run_is_final(&timed_out));
    assert_eq!(
        engine.result(&workflow_id, "").await,
        Some((WorkflowStatus::TimedOut as i32, run.execution_id.clone()))
    );

    engine.cleanup().await;
}

/// A run past the execution timeout is never retried, whatever its policy:
/// the execution timeout covers every run of the workflow.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn an_execution_timeout_is_never_retried() {
    let engine = Engine::new().await;
    let workflow_id = format!("wf-{}", Uuid::new_v4());
    let run = engine
        .start_as(
            &workflow_id,
            StartWorkflowRequest {
                execution_timeout: secs(60, 0),
                retry_policy: Some(RetryPolicy {
                    maximum_attempts: 3,
                    initial_interval: secs(1, 0),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .await;
    engine
        .aged(&run.execution_id, Duration::from_secs(120))
        .await;

    engine.check_workflow_timeouts().await;

    let timed_out = engine.run(&run.execution_id).await;
    assert_eq!(timed_out.status, "timed_out");
    assert_eq!(
        timed_out.error.as_deref(),
        Some("Workflow execution timed out")
    );
    assert!(
        timed_out.next_retry_at.is_none(),
        "a retry was scheduled past the execution timeout"
    );
    assert!(timed_out.retries_settled_at.is_some());
    assert!(run_is_final(&timed_out));
    assert!(!engine
        .executions
        .find_due_for_retry(1_000_000)
        .await
        .expect("find due retries")
        .iter()
        .any(|r| r.execution_id == run.execution_id));
    assert_eq!(
        engine.result(&workflow_id, "").await,
        Some((WorkflowStatus::TimedOut as i32, run.execution_id.clone()))
    );

    // Even a retry left scheduled on it — as the watcher used to leave one
    // on every timeout — is not started.
    sqlx::query(
        "UPDATE workflow_executions SET retries_settled_at = NULL, \
         next_retry_at = NOW() - INTERVAL '1 second' WHERE execution_id = $1",
    )
    .bind(&run.execution_id)
    .execute(&engine.pool)
    .await
    .expect("schedule a retry");
    engine
        .retries()
        .process_single_workflow(&run.execution_id)
        .await
        .expect("consider the retry");
    assert!(
        engine.successor_of(&run.execution_id).await.is_none(),
        "a run past the execution timeout was retried"
    );
    let settled = engine.run(&run.execution_id).await;
    assert_eq!(settled.status, "timed_out");
    assert!(settled.retries_settled_at.is_some());

    engine.cleanup().await;
}

/// The execution timeout is one deadline, fixed by the start: a retry and a
/// restart-fresh carry it rather than starting their own. A reset, an
/// operator restarting the workflow, starts a new one from its own start.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn every_successor_carries_the_execution_deadline() {
    let engine = Engine::new().await;
    let first = engine
        .start(StartWorkflowRequest {
            execution_timeout: secs(600, 0),
            retry_policy: Some(RetryPolicy {
                maximum_attempts: 3,
                ..Default::default()
            }),
            ..Default::default()
        })
        .await;
    let deadline = first.execution_deadline.expect("the start set a deadline");
    assert_eq!(
        deadline.timestamp_millis() - first.submitted_at.timestamp_millis(),
        600_000
    );

    // A retry.
    engine.failed_with_retry_due(&first.execution_id).await;
    engine
        .retries()
        .process_single_workflow(&first.execution_id)
        .await
        .expect("retry the run");
    let retry = engine
        .successor_of(&first.execution_id)
        .await
        .expect("retried");
    assert_eq!(retry.execution_deadline, Some(deadline), "a retry");
    assert_eq!(retry.timeout_secs, Some(600));

    // A restart-fresh.
    assert_eq!(
        engine.poll("worker-a").await.as_deref(),
        Some(retry.execution_id.as_str())
    );
    engine
        .complete(&retry, "worker-a", vec![restart_fresh()])
        .await;
    let continued = engine
        .successor_of(&retry.execution_id)
        .await
        .expect("restarted fresh");
    assert_eq!(
        continued.execution_deadline,
        Some(deadline),
        "a restart-fresh"
    );

    // A reset starts a new deadline: the same timeout, from its own start.
    let reset = engine.reset(&continued).await.expect("reset");
    let reset = engine.run(&reset).await;
    assert_eq!(
        reset
            .execution_deadline
            .expect("the reset run has a deadline")
            .timestamp_millis()
            - reset.submitted_at.timestamp_millis(),
        600_000,
        "a reset"
    );

    engine.cleanup().await;
}

/// A later run is held to the deadline the workflow started with, not to one
/// of its own: past it, it ends timed out, never to be retried.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_later_run_ends_at_the_execution_deadline() {
    let engine = Engine::new().await;
    let first = engine
        .start(StartWorkflowRequest {
            execution_timeout: secs(60, 0),
            retry_policy: Some(RetryPolicy {
                maximum_attempts: 3,
                initial_interval: secs(1, 0),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await;
    // Fifty seconds in, it fails, and is retried at once.
    engine
        .aged(&first.execution_id, Duration::from_secs(50))
        .await;
    engine.failed_with_retry_due(&first.execution_id).await;
    engine
        .retries()
        .process_single_workflow(&first.execution_id)
        .await
        .expect("retry the run");
    let retry = engine
        .successor_of(&first.execution_id)
        .await
        .expect("retried");

    // Twenty seconds on, the retry has run for twenty seconds — well inside
    // sixty — but the workflow for seventy.
    engine
        .aged(&retry.execution_id, Duration::from_secs(20))
        .await;
    engine.check_workflow_timeouts().await;

    let ended = engine.run(&retry.execution_id).await;
    assert_eq!(
        ended.status, "timed_out",
        "the retry outlived the workflow's execution timeout"
    );
    assert_eq!(ended.error.as_deref(), Some("Workflow execution timed out"));
    assert!(ended.next_retry_at.is_none());
    assert!(ended.retries_settled_at.is_some());

    engine.cleanup().await;
}

/// Past the execution deadline no run of the workflow starts: not a retry
/// due after it, not one whose scheduler stopped before creating it, not a
/// restart-fresh. A reset does, with a new deadline of its own: it is an
/// operator restarting the workflow.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn no_run_starts_past_the_execution_deadline() {
    let engine = Engine::new().await;
    let limited = || StartWorkflowRequest {
        execution_timeout: secs(60, 0),
        run_timeout: secs(30, 0),
        retry_policy: Some(RetryPolicy {
            maximum_attempts: 3,
            initial_interval: secs(1, 0),
            ..Default::default()
        }),
        ..Default::default()
    };

    // A failure whose retry falls due after the deadline is final.
    let failed = engine.start(limited()).await;
    engine
        .aged(&failed.execution_id, Duration::from_secs(70))
        .await;
    engine.failed_with_retry_due(&failed.execution_id).await;
    assert!(
        !engine
            .executions
            .claim_for_retry(failed.id.as_uuid())
            .await
            .expect("take the retry"),
        "a retry was taken past the execution deadline"
    );
    engine
        .retries()
        .process_single_workflow(&failed.execution_id)
        .await
        .expect("consider the retry");
    assert!(engine.successor_of(&failed.execution_id).await.is_none());
    let settled = engine.run(&failed.execution_id).await;
    assert_eq!(settled.status, "failed");
    assert!(
        settled.retries_settled_at.is_some(),
        "the failure was left waiting for a retry that cannot start"
    );
    assert!(run_is_final(&settled));

    // A run timeout taken for its retry in time, whose successor was not yet
    // created when the deadline passed, is closed timed out.
    let stranded = engine.start(limited()).await;
    engine
        .started_ago(&stranded.execution_id, Duration::from_secs(40))
        .await;
    engine.check_workflow_timeouts().await;
    assert!(engine
        .executions
        .claim_for_retry(stranded.id.as_uuid())
        .await
        .expect("take the retry"));
    engine
        .aged(&stranded.execution_id, Duration::from_secs(60))
        .await;
    sqlx::query(
        "UPDATE workflow_executions SET updated_at = NOW() - INTERVAL '1 second' \
         WHERE execution_id = $1",
    )
    .bind(&stranded.execution_id)
    .execute(&engine.pool)
    .await
    .expect("strand the retry");
    engine
        .retries()
        .recover_stranded_retries()
        .await
        .expect("recover stranded retries");
    assert!(
        engine.successor_of(&stranded.execution_id).await.is_none(),
        "a stranded retry was started past the execution deadline"
    );
    let closed = engine.run(&stranded.execution_id).await;
    assert_eq!(closed.status, "timed_out");
    assert!(closed.retries_settled_at.is_some());
    assert!(closed.next_retry_at.is_none());

    // A restart-fresh: the run is left for the timeout watcher to end.
    let continuing = engine.start(limited()).await;
    assert_eq!(
        engine.poll("worker-a").await.as_deref(),
        Some(continuing.execution_id.as_str())
    );
    engine
        .aged(&continuing.execution_id, Duration::from_secs(10))
        .await;
    sqlx::query(
        "UPDATE workflow_executions SET execution_deadline = NOW() - INTERVAL '1 second' \
         WHERE execution_id = $1",
    )
    .bind(&continuing.execution_id)
    .execute(&engine.pool)
    .await
    .expect("run out the execution timeout");
    engine
        .complete(&continuing, "worker-a", vec![restart_fresh()])
        .await;
    assert!(
        engine
            .successor_of(&continuing.execution_id)
            .await
            .is_none(),
        "a restart-fresh started a run past the execution deadline"
    );
    engine.check_workflow_timeouts().await;
    let ended = engine.run(&continuing.execution_id).await;
    assert_eq!(ended.status, "timed_out");
    assert_eq!(ended.error.as_deref(), Some("Workflow execution timed out"));

    // A reset recovers the timed-out workflow, under a new deadline.
    let reset = engine
        .reset(&ended)
        .await
        .expect("a reset past the execution deadline");
    let reset = engine.run(&reset).await;
    assert_eq!(reset.status, "running");
    let deadline = reset
        .execution_deadline
        .expect("the reset run has a deadline");
    assert_eq!(
        deadline.timestamp_millis() - reset.submitted_at.timestamp_millis(),
        60_000,
        "the reset run's deadline is not measured from its own start"
    );
    engine.check_workflow_timeouts().await;
    assert_eq!(
        engine.run(&reset.execution_id).await.status,
        "running",
        "the reset run was held to the deadline it was reset past"
    );

    engine.cleanup().await;
}

/// A retry given back — its successor could not be created — leaves a
/// timed-out run timed out, its retry still to come.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_run_timeout_given_back_is_still_timed_out() {
    let engine = Engine::new().await;
    let run = engine
        .start(StartWorkflowRequest {
            run_timeout: secs(60, 0),
            retry_policy: Some(RetryPolicy {
                maximum_attempts: 3,
                ..Default::default()
            }),
            ..Default::default()
        })
        .await;
    engine
        .started_ago(&run.execution_id, Duration::from_secs(120))
        .await;
    engine.check_workflow_timeouts().await;

    assert!(engine
        .executions
        .claim_for_retry(run.id.as_uuid())
        .await
        .expect("take the retry"));
    engine
        .executions
        .release_retry_claim(run.id.as_uuid())
        .await
        .expect("give it back");

    let given_back = engine.run(&run.execution_id).await;
    assert_eq!(given_back.status, "timed_out");
    assert!(given_back.can_retry());
    assert!(!run_is_final(&given_back));

    engine.cleanup().await;
}

fn restart_fresh() -> Command {
    Command {
        command_type: CommandType::RestartFresh as i32,
        attributes: Some(command::Attributes::RestartFresh(
            RestartFreshCommandAttributes::default(),
        )),
    }
}
