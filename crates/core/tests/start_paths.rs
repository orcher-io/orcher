//! Every way a workflow starts stores the run its start asked for.
//!
//! StartWorkflow, BatchStartWorkflow and SendEventWithStartWorkflow each read
//! the request and built the run's row themselves, and what one kept another
//! dropped: a batch-started run had none of its time limits, labels or id
//! reuse policy, ran on the first workflow's queue, and a child had none of
//! the time limits its start command gave it. The three now share one
//! conversion and one row builder; these checks compare the rows the paths
//! write field by field, so a field one path stores and another drops fails
//! here whether or not anyone thought to test it.
//!
//! DescribeWorkflowExecution reports a run's attempt, counted from 1, as its
//! activations carry it, and the annotations its start gave.
//!
//! A start's delay, annotations and request id were dropped by every path. A
//! delayed run is written at once but not handed out, by any claim, until it
//! is due, and its clocks start then; a start repeating a request id is
//! answered with the run the first made, by every path.
//!
//! Requires PostgreSQL. Run with:
//!
//! ```bash
//! export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/orcher_test"
//! cargo test -p orcher-core --test start_paths -- --ignored --test-threads=1
//! ```

// Helpers return `tonic::Status`, the error the engine's gRPC services answer with.
#![allow(clippy::result_large_err)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use orcher_common::types::dispatch::polling::PollExecutionRequest;
use orcher_core::engine::dispatch::{DelayedStarts, PollingManager};
use orcher_core::engine::queues::outbox::OutboxQueue;
use orcher_core::engine::WorkflowCoordinator;
use orcher_core::events::EventBus;
use orcher_core::grpc::services::{ExecutionService, QueryService, WorkflowService};
use orcher_core::retries::timeout_watcher::{TimeoutWatcher, TimeoutWatcherConfig};
use orcher_core::workflows::{
    ChildWorkflowManager, CommandProcessor, JournalService, WorkflowExecutionService,
    WorkflowRetryScheduler, WorkflowRetrySchedulerConfig,
};
use orcher_events::bus::EventBusConfig;
use orcher_proto::orcher::v1::execution_service_server::ExecutionService as ExecutionServiceTrait;
use orcher_proto::orcher::v1::query_service_server::QueryService as QueryServiceTrait;
use orcher_proto::orcher::v1::workflow_service_server::WorkflowService as WorkflowServiceTrait;
use orcher_proto::orcher::v1::{
    command, BatchStartWorkflowRequest, Command, CommandType, CompleteWorkflowExecutionRequest,
    DescribeWorkflowExecutionRequest, RetryPolicy, ScheduleConfig,
    SendEventWithStartWorkflowRequest, StartChildWorkflowCommandAttributes, StartWorkflowRequest,
    StartWorkflowResponse, WorkflowExecutionInfo, WorkflowIdReusePolicy,
};
use orcher_proto::prost_types;
use orcher_storage::repositories::domains::{
    child_workflow::ChildWorkflowRepository,
    coordination::{CoordinationRepository, OutboxRepository},
    journal::JournalRepository,
    task_execution::TaskExecutionRepository,
    workflow_execution::{logs::WorkflowExecutionLogsRepository, WorkflowExecutionRepository},
};
use orcher_storage::repositories::{ExecutionLogRepository, PostgresExecutionLogRepository};
use orcher_storage::schema::types::execution::WorkflowExecution;
use sqlx::PgPool;
use tonic::{Code, Request};
use uuid::Uuid;

const NAMESPACE: &str = "default";
const WORKER: &str = "start-paths-worker";

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
    /// Every run this engine starts is on a queue named from this, so
    /// cleanup finds them all.
    task_queue: String,
    executions: Arc<WorkflowExecutionRepository>,
    polling: Arc<PollingManager>,
    workflows: WorkflowService,
    service: ExecutionService,
    queries: QueryService,
    /// What hands a delayed run out once it is due, run by hand here.
    delayed: DelayedStarts,
}

impl Engine {
    async fn new() -> Self {
        let pool = pool().await;
        let task_queue = format!("start-paths-{}", Uuid::new_v4().simple());
        let journal_repo = Arc::new(JournalRepository::new(pool.clone()));
        let executions = Arc::new(WorkflowExecutionRepository::new(pool.clone()));
        let outbox_queue = Arc::new(OutboxQueue::new(Arc::new(OutboxRepository::new(Arc::new(
            pool.clone(),
        )))));
        let polling = Arc::new(PollingManager::new(
            executions.clone(),
            journal_repo.clone(),
            300,
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
            task_execution_repo.clone(),
            pool.clone(),
        );
        let delayed = DelayedStarts::new(
            executions.clone(),
            polling.clone(),
            execution_service.delayed_start_wake(),
        );
        let workflows = WorkflowService::new(execution_service, executions.clone())
            .with_event_support(journal_repo.clone(), polling.clone());
        let queries = QueryService::new(
            executions.clone(),
            journal_repo,
            Arc::new(WorkflowExecutionLogsRepository::new(pool.clone())),
            task_execution_repo,
            Arc::new(CoordinationRepository::new(Arc::new(pool.clone()))),
        );
        Self {
            pool,
            task_queue,
            executions,
            polling,
            workflows,
            service,
            queries,
            delayed,
        }
    }

    /// A start of `workflow_id` on this engine's queue, everything else as
    /// `request` asks.
    fn request(&self, workflow_id: &str, request: StartWorkflowRequest) -> StartWorkflowRequest {
        StartWorkflowRequest {
            workflow_id: workflow_id.to_string(),
            workflow_type: "orders".to_string(),
            task_queue: self.task_queue.clone(),
            namespace: NAMESPACE.to_string(),
            ..request
        }
    }

    async fn start(&self, request: StartWorkflowRequest) -> WorkflowExecution {
        let response = self
            .workflows
            .start_workflow(Request::new(request))
            .await
            .expect("start the workflow")
            .into_inner();
        self.run(&response.execution_id).await
    }

    async fn batch(
        &self,
        workflows: Vec<StartWorkflowRequest>,
    ) -> Result<Vec<WorkflowExecution>, tonic::Status> {
        let response = self
            .workflows
            .batch_start_workflow(Request::new(BatchStartWorkflowRequest { workflows }))
            .await?
            .into_inner();
        let mut runs = Vec::new();
        for started in response.executions {
            runs.push(self.run(&started.execution_id).await);
        }
        Ok(runs)
    }

    async fn run(&self, execution_id: &str) -> WorkflowExecution {
        self.executions
            .find_by_execution_id(execution_id)
            .await
            .expect("read the run")
            .expect("the run exists")
    }

    /// Every run of `workflow_id` this engine started, oldest first.
    async fn runs_of(&self, workflow_id: &str) -> Vec<WorkflowExecution> {
        let ids: Vec<String> = sqlx::query_scalar(
            "SELECT execution_id FROM workflow_executions \
             WHERE workflow_id = $1 AND task_queue LIKE $2 || '%' ORDER BY submitted_at",
        )
        .bind(workflow_id)
        .bind(&self.task_queue)
        .fetch_all(&self.pool)
        .await
        .expect("read the runs");
        let mut runs = Vec::new();
        for id in ids {
            runs.push(self.run(&id).await);
        }
        runs
    }

    async fn set_status(&self, execution_id: &str, status: &str) {
        sqlx::query(
            "UPDATE workflow_executions SET status = $2, completed_at = NOW(), \
             retries_settled_at = NOW() WHERE execution_id = $1",
        )
        .bind(execution_id)
        .bind(status)
        .execute(&self.pool)
        .await
        .expect("set the run's status");
    }

    async fn poll(&self) -> Option<String> {
        self.polling
            .poll_execution(PollExecutionRequest::new(
                self.task_queue.clone(),
                NAMESPACE,
                WORKER,
                Duration::from_millis(500),
            ))
            .await
            .expect("poll")
            .map(|response| response.execution_id)
    }

    async fn complete(&self, run: &WorkflowExecution, commands: Vec<Command>) {
        self.service
            .complete_workflow_execution(Request::new(CompleteWorkflowExecutionRequest {
                workflow_id: run.id.as_uuid().to_string(),
                execution_id: run.execution_id.clone(),
                namespace: NAMESPACE.to_string(),
                identity: WORKER.to_string(),
                commands,
                ..Default::default()
            }))
            .await
            .expect("complete the workflow task");
    }

    async fn described(&self, workflow_id: &str) -> WorkflowExecutionInfo {
        self.queries
            .describe_workflow_execution(Request::new(DescribeWorkflowExecutionRequest {
                workflow_id: workflow_id.to_string(),
                namespace: NAMESPACE.to_string(),
                ..Default::default()
            }))
            .await
            .expect("describe the workflow")
            .into_inner()
            .execution_info
            .expect("execution info")
    }

    async fn attempt_described(&self, workflow_id: &str) -> i32 {
        self.described(workflow_id).await.attempt
    }

    /// The response to a start of `request`, or its refusal.
    async fn try_start(
        &self,
        request: StartWorkflowRequest,
    ) -> Result<StartWorkflowResponse, tonic::Status> {
        self.workflows
            .start_workflow(Request::new(request))
            .await
            .map(|response| response.into_inner())
    }

    /// Hand out `run` by each of the ways a run is claimed — the single claim
    /// a poll makes, the batch claim, the claim of a run by its id — and
    /// return the ways that handed it out.
    async fn claim_every_way(&self, run: &WorkflowExecution) -> Vec<&'static str> {
        let mut handed_out = Vec::new();
        if self
            .executions
            .claim_batch(&self.task_queue, NAMESPACE, WORKER, 30, 10, None, None)
            .await
            .expect("batch claim")
            .iter()
            .any(|claimed| claimed.execution_id == run.execution_id)
        {
            handed_out.push("batch claim");
        }
        if self
            .executions
            .claim_execution_by_id(&run.execution_id, WORKER, 30, None, None)
            .await
            .expect("claim by id")
            .is_some()
        {
            handed_out.push("claim by id");
        }
        if self.poll().await.as_deref() == Some(run.execution_id.as_str()) {
            handed_out.push("poll");
        }
        handed_out
    }

    /// The EventReceived entries in `run`'s journal.
    async fn events_received(&self, run: &WorkflowExecution) -> i64 {
        sqlx::query_scalar(
            "SELECT COUNT(*) FROM workflow_execution_journal \
             WHERE execution_id = $1 AND event_type = 'EventReceived'",
        )
        .bind(&run.execution_id)
        .fetch_one(&self.pool)
        .await
        .expect("count the events")
    }

    async fn cleanup(&self) {
        for q in [
            "DELETE FROM child_workflow_executions WHERE parent_execution_id IN \
             (SELECT execution_id FROM workflow_executions WHERE task_queue LIKE $1 || '%')",
            "DELETE FROM workflow_execution_journal WHERE execution_id IN \
             (SELECT execution_id FROM workflow_executions WHERE task_queue LIKE $1 || '%')",
            "DELETE FROM workflow_execution_logs WHERE execution_id IN \
             (SELECT execution_id FROM workflow_executions WHERE task_queue LIKE $1 || '%')",
            "DELETE FROM outbox_queue WHERE workflow_id IN \
             (SELECT id FROM workflow_executions WHERE task_queue LIKE $1 || '%')",
            "DELETE FROM workflow_executions WHERE task_queue LIKE $1 || '%'",
        ] {
            let _ = sqlx::query(q)
                .bind(&self.task_queue)
                .execute(&self.pool)
                .await;
        }
    }
}

fn secs(seconds: i64) -> Option<prost_types::Duration> {
    Some(prost_types::Duration { seconds, nanos: 0 })
}

fn labels() -> HashMap<String, String> {
    HashMap::from([
        ("team".to_string(), "billing".to_string()),
        ("tier".to_string(), "gold".to_string()),
    ])
}

fn annotations() -> HashMap<String, String> {
    HashMap::from([("owner".to_string(), "payments@example.com".to_string())])
}

fn retried(maximum_attempts: i32) -> Option<RetryPolicy> {
    Some(RetryPolicy {
        maximum_attempts,
        initial_interval: secs(2),
        backoff_coefficient: 3.0,
        maximum_interval: secs(30),
        non_retryable_error_types: vec!["CardDeclined".to_string()],
    })
}

/// A start asking for everything a run can be started with.
fn everything() -> StartWorkflowRequest {
    StartWorkflowRequest {
        input: br#"{"order": 7}"#.to_vec(),
        execution_timeout: secs(3600),
        run_timeout: secs(600),
        task_timeout: secs(20),
        retry_policy: retried(4),
        labels: labels(),
        annotations: annotations(),
        workflow_id_reuse_policy: WorkflowIdReusePolicy::RejectDuplicate as i32,
        ..Default::default()
    }
}

/// [`everything`], given `request_id`, which no two starts share.
fn everything_as(request_id: &str) -> StartWorkflowRequest {
    StartWorkflowRequest {
        request_id: request_id.to_string(),
        ..everything()
    }
}

/// The columns two runs of the same start may differ in: what names the run
/// itself and when it was written. Everything else — and any column added
/// later — must be the same whichever way the run was started.
const PER_RUN: &[&str] = &[
    "id",
    "execution_id",
    "workflow_id",
    "root_execution_id",
    "submitted_at",
    "started_at",
    "created_at",
    "updated_at",
    // Fixed from the run's own start; compared as an offset from it below.
    "execution_deadline",
    // The start's own request id, compared with the one each start gave.
    "start_request_id",
    // When a delayed run is due; compared as an offset from its submission.
    "start_after",
];

/// `run` as its stored columns, less those that name the run.
fn what_was_asked(run: &WorkflowExecution) -> serde_json::Map<String, serde_json::Value> {
    let serde_json::Value::Object(mut row) = serde_json::to_value(run).expect("serialize the run")
    else {
        panic!("a run serializes to an object");
    };
    for column in PER_RUN {
        assert!(row.remove(*column).is_some(), "no column {column}");
    }
    row
}

/// The execution deadline, in seconds from when the run's clocks start: its
/// submission, or for a delayed start when it is due.
fn deadline_offset_secs(run: &WorkflowExecution) -> Option<i64> {
    run.execution_deadline.map(|deadline| {
        (deadline.timestamp_millis() - run.clocks_start_at().timestamp_millis()) / 1000
    })
}

/// How long after its submission a delayed run is due, in milliseconds.
fn delay_ms(run: &WorkflowExecution) -> Option<i64> {
    run.start_after
        .map(|due| due.timestamp_millis() - run.submitted_at.timestamp_millis())
}

/// A run started by StartWorkflow, by BatchStartWorkflow and by
/// SendEventWithStartWorkflow from the same settings is the same run, column
/// for column. A batch used to drop the time limits, labels, and a run
/// without a workflow id got none.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn every_start_path_stores_what_a_single_start_stores() {
    let engine = Engine::new().await;

    let request = |name: &str| format!("{name}-{}", Uuid::new_v4());
    let single_request = request("single-request");
    let single = engine
        .start(engine.request(
            &format!("single-{}", Uuid::new_v4()),
            everything_as(&single_request),
        ))
        .await;
    // What is compared is what was asked for, not nothing.
    assert_eq!(single.timeout_secs, Some(3600));
    assert_eq!(single.run_timeout_secs, Some(600));
    assert_eq!(single.task_timeout_secs, Some(20));
    assert_eq!(single.max_retries, 3, "four attempts, the first included");
    assert_eq!(
        single.labels,
        serde_json::json!({"team": "billing", "tier": "gold"})
    );
    assert_eq!(deadline_offset_secs(&single), Some(3600));
    assert_eq!(
        single.annotations,
        serde_json::json!({"owner": "payments@example.com"})
    );
    assert_eq!(
        single.start_request_id.as_deref(),
        Some(single_request.as_str())
    );

    let batch_requests = [request("batched-request"), request("batched-request")];
    let batched = engine
        .batch(vec![
            engine.request(
                &format!("batched-{}", Uuid::new_v4()),
                everything_as(&batch_requests[0]),
            ),
            // A second one, so a batch of several is what is compared.
            engine.request(
                &format!("batched-{}", Uuid::new_v4()),
                everything_as(&batch_requests[1]),
            ),
        ])
        .await
        .expect("batch start");

    let e = everything();
    let event_request = request("event-request");
    let with_event = engine
        .workflows
        .send_event_with_start_workflow(Request::new(SendEventWithStartWorkflowRequest {
            workflow_id: format!("event-{}", Uuid::new_v4()),
            namespace: NAMESPACE.to_string(),
            event_name: "paid".to_string(),
            workflow_type: "orders".to_string(),
            task_queue: engine.task_queue.clone(),
            workflow_input: e.input,
            execution_timeout: e.execution_timeout,
            run_timeout: e.run_timeout,
            task_timeout: e.task_timeout,
            retry_policy: e.retry_policy,
            labels: e.labels,
            annotations: e.annotations,
            request_id: event_request.clone(),
            ..Default::default()
        }))
        .await
        .expect("event with start")
        .into_inner();
    assert!(with_event.workflow_started);
    let with_event = engine.run(&with_event.execution_id).await;

    for (path, run, request_id) in [
        ("batch", &batched[0], &batch_requests[0]),
        ("batch", &batched[1], &batch_requests[1]),
        ("event-with-start", &with_event, &event_request),
    ] {
        assert_eq!(
            run.start_request_id.as_deref(),
            Some(request_id.as_str()),
            "{path} keeps its start's request id"
        );
        assert_eq!(
            what_was_asked(run),
            what_was_asked(&single),
            "a run started by {path} differs from a single start's"
        );
        assert_eq!(
            deadline_offset_secs(run),
            deadline_offset_secs(&single),
            "{path}"
        );
        assert!(run.holds_workflow_id, "{path}");
    }

    // A batch without workflow ids: each run is given one, as a single start
    // is, and holds it.
    let unnamed = engine
        .batch(vec![engine.request("", StartWorkflowRequest::default())])
        .await
        .expect("batch start without ids");
    let id = unnamed[0].workflow_id.clone().expect("a workflow id");
    assert!(!id.is_empty() && unnamed[0].holds_workflow_id);

    engine.cleanup().await;
}

/// Each workflow of a batch runs on its own queue. Every one used to run on
/// the first one's.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_batch_runs_each_workflow_on_its_own_queue() {
    let engine = Engine::new().await;
    let other_queue = format!("{}-other", engine.task_queue);
    let mut second = engine.request(&format!("q-{}", Uuid::new_v4()), Default::default());
    second.task_queue = other_queue.clone();

    let runs = engine
        .batch(vec![
            engine.request(&format!("q-{}", Uuid::new_v4()), Default::default()),
            second,
        ])
        .await
        .expect("batch start");
    assert_eq!(runs[0].task_queue, engine.task_queue);
    assert_eq!(runs[1].task_queue, other_queue);

    engine.cleanup().await;
}

/// A batch-started run past its run timeout is timed out, as a single
/// start's is: the limit is not only stored but enforced.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_batch_started_run_is_held_to_its_run_timeout() {
    let engine = Engine::new().await;
    let runs = engine
        .batch(vec![engine.request(
            &format!("late-{}", Uuid::new_v4()),
            StartWorkflowRequest {
                run_timeout: secs(30),
                ..Default::default()
            },
        )])
        .await
        .expect("batch start");
    sqlx::query(
        "UPDATE workflow_executions SET submitted_at = NOW() - INTERVAL '60 seconds' \
         WHERE execution_id = $1",
    )
    .bind(&runs[0].execution_id)
    .execute(&engine.pool)
    .await
    .expect("backdate the run");

    TimeoutWatcher::new(
        engine.pool.clone(),
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
    assert_eq!(engine.run(&runs[0].execution_id).await.status, "timed_out");

    engine.cleanup().await;
}

/// Each start of a batch takes its workflow id as its own reuse policy
/// allows, as a single start would; a batch any of whose starts is refused
/// starts nothing, and terminates nothing either.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_batch_takes_each_id_as_its_reuse_policy_allows() {
    let engine = Engine::new().await;
    let under = |id: &str, policy: WorkflowIdReusePolicy| {
        let mut request = engine.request(id, Default::default());
        request.workflow_id_reuse_policy = policy as i32;
        request
    };

    let completed = format!("done-{}", Uuid::new_v4());
    let done = engine
        .start(engine.request(&completed, Default::default()))
        .await;
    engine.set_status(&done.execution_id, "completed").await;
    let running = format!("running-{}", Uuid::new_v4());
    let open = engine
        .start(engine.request(&running, Default::default()))
        .await;

    // REJECT_DUPLICATE refuses an id a run has carried, closed or not; and
    // the TERMINATE_IF_RUNNING start beside it terminates nothing.
    let fresh = format!("fresh-{}", Uuid::new_v4());
    let refused = engine
        .batch(vec![
            under(&fresh, WorkflowIdReusePolicy::AllowDuplicate),
            under(&running, WorkflowIdReusePolicy::TerminateIfRunning),
            under(&completed, WorkflowIdReusePolicy::RejectDuplicate),
        ])
        .await
        .expect_err("a batch reusing a closed id under REJECT_DUPLICATE");
    assert_eq!(refused.code(), Code::AlreadyExists, "{refused:?}");
    assert!(refused.message().starts_with("workflow[2]"), "{refused:?}");
    assert!(engine.runs_of(&fresh).await.is_empty());
    assert_eq!(engine.run(&open.execution_id).await.status, "running");

    // ALLOW_DUPLICATE_FAILED_ONLY refuses an id whose run completed.
    let refused = engine
        .batch(vec![under(
            &completed,
            WorkflowIdReusePolicy::AllowDuplicateFailedOnly,
        )])
        .await
        .expect_err("a batch reusing a completed id under ALLOW_DUPLICATE_FAILED_ONLY");
    assert_eq!(refused.code(), Code::AlreadyExists, "{refused:?}");

    // TERMINATE_IF_RUNNING ends the open run and starts the id again.
    let runs = engine
        .batch(vec![
            under(&fresh, WorkflowIdReusePolicy::AllowDuplicate),
            under(&running, WorkflowIdReusePolicy::TerminateIfRunning),
        ])
        .await
        .expect("a batch terminating the run in its way");
    assert_eq!(engine.run(&open.execution_id).await.status, "terminated");
    assert_eq!(runs[1].workflow_id.as_deref(), Some(running.as_str()));
    assert_eq!(runs[1].status, "running");
    assert_eq!(engine.runs_of(&fresh).await.len(), 1);

    // An id named twice in one batch is refused whole.
    let twice = format!("twice-{}", Uuid::new_v4());
    let refused = engine
        .batch(vec![
            under(&twice, WorkflowIdReusePolicy::AllowDuplicate),
            under(&twice, WorkflowIdReusePolicy::AllowDuplicate),
        ])
        .await
        .expect_err("a batch naming one id twice");
    assert_eq!(refused.code(), Code::AlreadyExists, "{refused:?}");
    assert!(engine.runs_of(&twice).await.is_empty());

    engine.cleanup().await;
}

/// A child is started with the time limits its start command gives it, and
/// its execution deadline runs from its own start, not its parent's. A child
/// used to be started with none.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_child_carries_the_time_limits_its_command_gives_it() {
    let engine = Engine::new().await;
    let parent = engine
        .start(engine.request(
            &format!("parent-{}", Uuid::new_v4()),
            StartWorkflowRequest {
                execution_timeout: secs(86_400),
                run_timeout: secs(7_200),
                task_timeout: secs(30),
                ..Default::default()
            },
        ))
        .await;
    assert_eq!(
        engine.poll().await.as_deref(),
        Some(parent.execution_id.as_str())
    );
    engine
        .complete(
            &parent,
            vec![Command {
                command_type: CommandType::StartChildWorkflow as i32,
                attributes: Some(command::Attributes::StartChildWorkflow(
                    StartChildWorkflowCommandAttributes {
                        workflow_id: "child_0".to_string(),
                        workflow_type: "shipping".to_string(),
                        task_queue: engine.task_queue.clone(),
                        input: b"{}".to_vec(),
                        execution_timeout: secs(900),
                        run_timeout: secs(300),
                        task_timeout: secs(5),
                        labels: labels(),
                        annotations: annotations(),
                        ..Default::default()
                    },
                )),
            }],
        )
        .await;

    let child = engine
        .executions
        .find_child(&parent.execution_id, "child_0")
        .await
        .expect("find the child")
        .expect("the child was started");
    assert_eq!(child.timeout_secs, Some(900));
    assert_eq!(child.run_timeout_secs, Some(300));
    assert_eq!(child.task_timeout_secs, Some(5));
    assert_eq!(
        deadline_offset_secs(&child),
        Some(900),
        "the child's deadline runs from its own start"
    );
    assert_eq!(
        child.labels,
        serde_json::json!({"team": "billing", "tier": "gold"})
    );
    assert_eq!(
        child.annotations,
        serde_json::json!({"owner": "payments@example.com"})
    );

    engine.cleanup().await;
}

/// DescribeWorkflowExecution reports a run's attempt counted from 1: 1 for
/// the first run, 2 for the run of its first workflow-level retry. It read 0
/// for every run.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn describe_reports_the_runs_attempt() {
    let engine = Engine::new().await;
    let workflow_id = format!("described-{}", Uuid::new_v4());
    let first = engine
        .start(engine.request(
            &workflow_id,
            StartWorkflowRequest {
                retry_policy: retried(3),
                annotations: annotations(),
                ..Default::default()
            },
        ))
        .await;
    assert_eq!(engine.attempt_described(&workflow_id).await, 1);
    assert_eq!(
        engine.described(&workflow_id).await.annotations,
        annotations()
    );

    sqlx::query(
        "UPDATE workflow_executions SET status = 'failed', error = 'boom', \
         completed_at = NOW(), next_retry_at = NOW() - INTERVAL '1 second' \
         WHERE execution_id = $1",
    )
    .bind(&first.execution_id)
    .execute(&engine.pool)
    .await
    .expect("fail the run");
    WorkflowRetryScheduler::new(
        engine.executions.clone(),
        WorkflowRetrySchedulerConfig::default(),
    )
    .process_single_workflow(&first.execution_id)
    .await
    .expect("retry the run");
    assert_eq!(engine.runs_of(&workflow_id).await.len(), 2, "retried");

    assert_eq!(engine.attempt_described(&workflow_id).await, 2);
    assert_eq!(
        engine.described(&workflow_id).await.annotations,
        annotations(),
        "a retry carries the workflow's annotations"
    );

    engine.cleanup().await;
}

/// A delayed start is stored alike by a single start and a batch, column for
/// column: due its delay after its submission, waiting for it, and with its
/// execution deadline counted from when it is due.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_delayed_start_is_stored_alike_by_every_start_path() {
    let engine = Engine::new().await;
    let delayed = || StartWorkflowRequest {
        start_delay: secs(3600),
        ..everything()
    };

    let single = engine
        .start(engine.request(&format!("delayed-{}", Uuid::new_v4()), delayed()))
        .await;
    assert!(single.awaiting_start);
    assert_eq!(delay_ms(&single), Some(3_600_000));
    assert_eq!(
        deadline_offset_secs(&single),
        Some(3600),
        "the execution timeout is counted from when the run is due"
    );

    let batched = engine
        .batch(vec![engine.request(
            &format!("delayed-batched-{}", Uuid::new_v4()),
            delayed(),
        )])
        .await
        .expect("batch start");
    assert_eq!(what_was_asked(&batched[0]), what_was_asked(&single));
    assert_eq!(delay_ms(&batched[0]), delay_ms(&single));
    assert_eq!(deadline_offset_secs(&batched[0]), Some(3600));

    // A start at once is not delayed; a zero delay is none.
    let now = engine
        .start(engine.request(
            &format!("zero-{}", Uuid::new_v4()),
            StartWorkflowRequest {
                start_delay: secs(0),
                ..Default::default()
            },
        ))
        .await;
    assert!(!now.awaiting_start);
    assert_eq!(now.start_after, None);

    engine.cleanup().await;
}

/// No claim hands out a delayed run before it is due — not a poll, not a
/// batch claim, not a claim by id — and once it is due the release hands it
/// out. A run waiting out a longer delay on the same queue stays.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_delayed_run_is_not_handed_out_before_it_is_due() {
    let engine = Engine::new().await;
    let started = engine
        .try_start(engine.request(
            &format!("soon-{}", Uuid::new_v4()),
            StartWorkflowRequest {
                start_delay: secs(600),
                ..Default::default()
            },
        ))
        .await
        .expect("a delayed start");
    let soon = engine.run(&started.execution_id).await;
    // The answer says when the run starts.
    let due = soon.start_after.expect("a delayed run is due later");
    assert_eq!(
        started.started_at.map(|t| t.seconds),
        Some(due.timestamp()),
        "the start is answered with when the run starts"
    );
    let later = engine
        .start(engine.request(
            &format!("later-{}", Uuid::new_v4()),
            StartWorkflowRequest {
                start_delay: secs(3600),
                ..Default::default()
            },
        ))
        .await;

    assert_eq!(engine.claim_every_way(&soon).await, Vec::<&str>::new());
    assert!(engine
        .delayed
        .release_due()
        .await
        .expect("release")
        .is_empty());

    // Due, but not yet released: still not claimable.
    sqlx::query(
        "UPDATE workflow_executions SET start_after = NOW() - INTERVAL '1 second' \
         WHERE execution_id = $1",
    )
    .bind(&soon.execution_id)
    .execute(&engine.pool)
    .await
    .expect("bring the run's start forward");
    assert_eq!(engine.claim_every_way(&soon).await, Vec::<&str>::new());

    let released = engine.delayed.release_due().await.expect("release");
    assert!(
        released.iter().any(|r| r.execution_id == soon.execution_id),
        "{released:?}"
    );
    assert!(
        !released
            .iter()
            .any(|r| r.execution_id == later.execution_id),
        "a run not yet due stays: {released:?}"
    );
    assert_eq!(
        engine.poll().await.as_deref(),
        Some(soon.execution_id.as_str()),
        "the released run is handed out"
    );
    assert!(engine.run(&later.execution_id).await.awaiting_start);

    engine.cleanup().await;
}

/// A delayed run's clocks start when it is due: its run timeout is measured
/// from then, so the delay is not spent from it.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_delayed_runs_clocks_start_when_it_is_due() {
    let engine = Engine::new().await;
    let run = engine
        .start(engine.request(
            &format!("clocked-{}", Uuid::new_v4()),
            StartWorkflowRequest {
                start_delay: secs(120),
                run_timeout: secs(30),
                ..Default::default()
            },
        ))
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

    // Submitted a minute ago, due in a minute: past its run timeout from its
    // submission, not from when it is due.
    sqlx::query(
        "UPDATE workflow_executions SET submitted_at = NOW() - INTERVAL '60 seconds', \
         start_after = NOW() + INTERVAL '60 seconds' WHERE execution_id = $1",
    )
    .bind(&run.execution_id)
    .execute(&engine.pool)
    .await
    .expect("backdate the run");
    watcher.check_timeouts().await.expect("check timeouts");
    assert_eq!(engine.run(&run.execution_id).await.status, "running");

    // Due a minute ago: past it.
    sqlx::query(
        "UPDATE workflow_executions SET start_after = NOW() - INTERVAL '60 seconds' \
         WHERE execution_id = $1",
    )
    .bind(&run.execution_id)
    .execute(&engine.pool)
    .await
    .expect("bring the run's start forward");
    watcher.check_timeouts().await.expect("check timeouts");
    assert_eq!(engine.run(&run.execution_id).await.status, "timed_out");

    engine.cleanup().await;
}

/// A delay that means nothing is refused, not ignored: a negative one, and
/// one asked of a start on a cron schedule, whose runs start when it fires.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_delay_that_cannot_be_kept_is_refused() {
    let engine = Engine::new().await;
    let negative = engine
        .try_start(engine.request(
            &format!("negative-{}", Uuid::new_v4()),
            StartWorkflowRequest {
                start_delay: secs(-5),
                ..Default::default()
            },
        ))
        .await
        .expect_err("a negative delay");
    assert_eq!(negative.code(), Code::InvalidArgument, "{negative:?}");

    let scheduled = engine
        .try_start(engine.request(
            &format!("scheduled-{}", Uuid::new_v4()),
            StartWorkflowRequest {
                start_delay: secs(60),
                schedule_config: Some(ScheduleConfig {
                    cron_expression: "0 0 3 * * *".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            },
        ))
        .await
        .expect_err("a delayed start on a schedule");
    assert_eq!(scheduled.code(), Code::InvalidArgument, "{scheduled:?}");

    engine.cleanup().await;
}

/// A start repeating a request id is answered with the run the first made,
/// under every reuse policy and whatever has become of that run, and never
/// starts or ends one; a request id given to a start of another workflow is
/// refused. Repeats racing each other make one run between them.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_repeated_start_is_answered_with_the_run_it_made() {
    let engine = Engine::new().await;
    let workflow_id = format!("once-{}", Uuid::new_v4());
    let request_id = Uuid::new_v4().to_string();
    let start = |policy: WorkflowIdReusePolicy| {
        let mut request = engine.request(
            &workflow_id,
            StartWorkflowRequest {
                request_id: request_id.clone(),
                ..Default::default()
            },
        );
        request.workflow_id_reuse_policy = policy as i32;
        request
    };

    let first = engine
        .try_start(start(WorkflowIdReusePolicy::AllowDuplicate))
        .await
        .expect("the first start");
    for policy in [
        WorkflowIdReusePolicy::AllowDuplicate,
        WorkflowIdReusePolicy::RejectDuplicate,
        WorkflowIdReusePolicy::TerminateIfRunning,
    ] {
        let again = engine
            .try_start(start(policy))
            .await
            .unwrap_or_else(|e| panic!("a repeat under {policy:?}: {e:?}"));
        assert_eq!(again, first, "a repeat under {policy:?}");
    }
    assert_eq!(engine.runs_of(&workflow_id).await.len(), 1);
    assert_eq!(
        engine.run(&first.execution_id).await.status,
        "running",
        "a TERMINATE_IF_RUNNING repeat does not end the run it started"
    );

    // Closed, the run is still the answer: the repeat is not a new start.
    engine.set_status(&first.execution_id, "completed").await;
    let again = engine
        .try_start(start(WorkflowIdReusePolicy::AllowDuplicate))
        .await
        .expect("a repeat after the run completed");
    assert_eq!(again.execution_id, first.execution_id);
    assert_eq!(engine.runs_of(&workflow_id).await.len(), 1);

    // The request id belongs to that start: another workflow giving it is
    // refused.
    let reused = engine
        .try_start(engine.request(
            &format!("other-{}", Uuid::new_v4()),
            StartWorkflowRequest {
                request_id: request_id.clone(),
                ..Default::default()
            },
        ))
        .await
        .expect_err("a request id given to another workflow");
    assert_eq!(reused.code(), Code::InvalidArgument, "{reused:?}");

    // A start without a workflow id is given one from its request id, so a
    // repeat of it names the same workflow.
    let unnamed_request = Uuid::new_v4().to_string();
    let unnamed = || {
        engine.request(
            "",
            StartWorkflowRequest {
                request_id: unnamed_request.clone(),
                ..Default::default()
            },
        )
    };
    let first_unnamed = engine.try_start(unnamed()).await.expect("unnamed start");
    let again = engine.try_start(unnamed()).await.expect("unnamed repeat");
    assert_eq!(again, first_unnamed);

    // Repeats racing each other: one run, and every one answered with it,
    // under every policy. A repeat can find nothing by its request id and
    // then find the run its start made holding the workflow id, once that
    // start commits; it is answered with that run, not refused, and under
    // TerminateIfRunning it does not end it. The window is narrow, so each
    // policy races several rounds.
    for policy in [
        WorkflowIdReusePolicy::AllowDuplicate,
        WorkflowIdReusePolicy::RejectDuplicate,
        WorkflowIdReusePolicy::TerminateIfRunning,
    ] {
        for round in 0..10 {
            let raced_request = Uuid::new_v4().to_string();
            let raced_id = format!("raced-{}", Uuid::new_v4());
            let race = || {
                let mut request = engine.request(
                    &raced_id,
                    StartWorkflowRequest {
                        request_id: raced_request.clone(),
                        ..Default::default()
                    },
                );
                request.workflow_id_reuse_policy = policy as i32;
                engine.try_start(request)
            };
            let answers = futures::future::join_all((0..8).map(|_| race())).await;
            let executions: std::collections::HashSet<String> = answers
                .into_iter()
                .map(|answer| {
                    answer
                        .unwrap_or_else(|e| {
                            panic!("a racing repeat under {policy:?}, round {round}: {e:?}")
                        })
                        .execution_id
                })
                .collect();
            assert_eq!(
                executions.len(),
                1,
                "{policy:?}, round {round}: {executions:?}"
            );
            let runs = engine.runs_of(&raced_id).await;
            assert_eq!(runs.len(), 1, "{policy:?}, round {round}");
            let only = executions.into_iter().next().expect("one run");
            assert_eq!(
                engine.run(&only).await.status,
                "running",
                "{policy:?}, round {round}: a racing repeat does not end the run it started"
            );
        }
    }

    // The request id is the organization's: another's finds nothing.
    assert!(engine
        .executions
        .find_by_start_request(Some(Uuid::new_v4()), NAMESPACE, &request_id)
        .await
        .expect("look the request up")
        .is_none());

    engine.cleanup().await;
}

/// A batch repeated is answered with the runs it started, each in its place,
/// and starts the rest; a batch giving one request id twice is refused whole.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_repeated_batch_is_answered_with_the_runs_it_made() {
    let engine = Engine::new().await;
    let start = |request_id: &str| {
        engine.request(
            &format!("batched-once-{}", Uuid::new_v4()),
            StartWorkflowRequest {
                request_id: request_id.to_string(),
                ..Default::default()
            },
        )
    };
    let requests: Vec<String> = (0..3).map(|_| Uuid::new_v4().to_string()).collect();
    let first = start(&requests[0]);
    let single = engine.start(first.clone()).await;

    let batch = vec![first, start(&requests[1]), start(&requests[2])];
    let runs = engine.batch(batch.clone()).await.expect("a batch");
    assert_eq!(
        runs[0].execution_id, single.execution_id,
        "a start the batch repeats is answered with its run"
    );
    let again = engine.batch(batch).await.expect("the batch repeated");
    let ids = |runs: &[WorkflowExecution]| {
        runs.iter()
            .map(|r| r.execution_id.clone())
            .collect::<Vec<_>>()
    };
    assert_eq!(ids(&again), ids(&runs));

    let twice = Uuid::new_v4().to_string();
    let refused = engine
        .batch(vec![start(&twice), start(&twice)])
        .await
        .expect_err("a batch giving one request id twice");
    assert_eq!(refused.code(), Code::InvalidArgument, "{refused:?}");
    assert!(engine
        .executions
        .find_by_start_request(None, NAMESPACE, &twice)
        .await
        .expect("look the request up")
        .is_none());

    engine.cleanup().await;
}

/// An event-with-start repeated with its request id is answered with the run
/// it started, and the event is seeded into it once.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_repeated_event_with_start_seeds_its_event_once() {
    let engine = Engine::new().await;
    let request = SendEventWithStartWorkflowRequest {
        workflow_id: format!("signalled-{}", Uuid::new_v4()),
        namespace: NAMESPACE.to_string(),
        event_name: "paid".to_string(),
        workflow_type: "orders".to_string(),
        task_queue: engine.task_queue.clone(),
        request_id: Uuid::new_v4().to_string(),
        ..Default::default()
    };
    let mut answers = Vec::new();
    for _ in 0..2 {
        answers.push(
            engine
                .workflows
                .send_event_with_start_workflow(Request::new(request.clone()))
                .await
                .expect("event with start")
                .into_inner(),
        );
    }
    assert_eq!(answers[0].execution_id, answers[1].execution_id);
    assert!(answers[1].workflow_started);
    let run = engine.run(&answers[0].execution_id).await;
    assert_eq!(engine.events_received(&run).await, 1);
    assert_eq!(engine.runs_of(&request.workflow_id).await.len(), 1);

    engine.cleanup().await;
}
