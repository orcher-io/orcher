//! Every RPC that addresses one run names it the same way, and never names a
//! run the caller may not see.
//!
//! A request names a run by workflow id (its latest run in the request's
//! namespace) or by run id (that exact run). Describe and the journal used to
//! look a workflow id up as a workflow *type*, so describing a workflow by its
//! id always failed; the other RPCs looked it up across every namespace and
//! organization and only then checked ownership, so another tenant's newer run
//! of the same id hid the caller's own, and a namespace never narrowed the
//! lookup. All of them now resolve through one scoped rule.
//!
//! Requires PostgreSQL. Run with:
//!
//! ```bash
//! export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/orcher_test"
//! cargo test -p orcher-core --test run_resolution -- --ignored --test-threads=1
//! ```

use orcher_core::engine::dispatch::PollingManager;
use orcher_core::engine::queues::outbox::OutboxQueue;
use orcher_core::events::EventBus;
use orcher_core::grpc::services::{QueryService, WorkflowService};
use orcher_core::grpc::tenant::ORGANIZATION_METADATA_KEY;
use orcher_core::workflows::{JournalService, WorkflowExecutionService};
use orcher_events::bus::EventBusConfig;
use orcher_proto::orcher::v1::get_workflow_result_response::Outcome;
use orcher_proto::orcher::v1::query_service_server::QueryService as QueryServiceTrait;
use orcher_proto::orcher::v1::workflow_service_server::WorkflowService as WorkflowServiceTrait;
use orcher_proto::orcher::v1::{
    CancelWorkflowRequest, DescribeWorkflowExecutionRequest, GetExecutionJournalRequest,
    GetExecutionLogsRequest, GetTaskExecutionsRequest, GetWorkflowResultRequest,
    GetWorkflowStatusRequest, QueryWorkflowRequest, ResetWorkflowRequest, SendEventRequest,
    TerminateWorkflowRequest, UpdateWorkflowRequest,
};
use orcher_storage::repositories::domains::{
    coordination::{CoordinationRepository, OutboxRepository},
    journal::JournalRepository,
    task_execution::TaskExecutionRepository,
    workflow_execution::{WorkflowExecutionLogsRepository, WorkflowExecutionRepository},
};
use sqlx::PgPool;
use std::sync::Arc;
use tonic::{Code, Request, Status};
use uuid::Uuid;

async fn pool() -> PgPool {
    let url = std::env::var("TEST_DATABASE_URL").unwrap_or_else(|_| {
        "postgresql://postgres:postgres@localhost:5432/orcher_test".to_string()
    });
    PgPool::connect(&url)
        .await
        .expect("connect to the test database")
}

fn workflow_service(pool: &PgPool) -> WorkflowService {
    let journal_repo = Arc::new(JournalRepository::new(pool.clone()));
    let exec_repo = Arc::new(WorkflowExecutionRepository::new(pool.clone()));
    let outbox_queue = Arc::new(OutboxQueue::new(Arc::new(OutboxRepository::new(Arc::new(
        pool.clone(),
    )))));
    let execution_service = Arc::new(WorkflowExecutionService::new(
        exec_repo.clone(),
        Arc::new(JournalService::new(journal_repo.clone())),
        outbox_queue,
        EventBus::new(EventBusConfig::default()),
        pool.clone(),
    ));
    let polling_manager = Arc::new(PollingManager::new(
        exec_repo.clone(),
        journal_repo.clone(),
        300,
    ));
    WorkflowService::new(execution_service, exec_repo)
        .with_event_support(journal_repo, polling_manager)
}

fn query_service(pool: &PgPool) -> QueryService {
    QueryService::new(
        Arc::new(WorkflowExecutionRepository::new(pool.clone())),
        Arc::new(JournalRepository::new(pool.clone())),
        Arc::new(WorkflowExecutionLogsRepository::new(pool.clone())),
        Arc::new(TaskExecutionRepository::new(pool.clone())),
        Arc::new(CoordinationRepository::new(Arc::new(pool.clone()))),
    )
}

async fn organization(pool: &PgPool) -> Uuid {
    let owner = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO users (id, email, password_hash, hash_algorithm, password_changed_at) \
         VALUES ($1, $2, 'x', 'argon2', NOW())",
    )
    .bind(owner)
    .bind(format!("owner-{owner}@run-resolution.test"))
    .execute(pool)
    .await
    .expect("seed user");
    let organization = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO organizations (id, name, display_name, slug, owner_id) \
         VALUES ($1, $2, $2, $2, $3)",
    )
    .bind(organization)
    .bind(format!("org-{organization}"))
    .bind(owner)
    .execute(pool)
    .await
    .expect("seed organization");
    organization
}

/// A run seeded directly: what the resolution reads, nothing more.
struct Seed<'a> {
    organization: Uuid,
    namespace: &'a str,
    workflow_id: &'a str,
    status: &'a str,
    /// How long ago it was submitted; the latest run is the newest.
    age_secs: i64,
    parent_execution_id: Option<&'a str>,
    result: Option<serde_json::Value>,
}

impl<'a> Seed<'a> {
    fn running(organization: Uuid, namespace: &'a str, workflow_id: &'a str, age: i64) -> Self {
        Self {
            organization,
            namespace,
            workflow_id,
            status: "running",
            age_secs: age,
            parent_execution_id: None,
            result: None,
        }
    }
}

/// Seed a run; returns its run id.
async fn seed(pool: &PgPool, run: Seed<'_>) -> String {
    let id = Uuid::new_v4();
    let execution_id = id.to_string();
    sqlx::query(
        r#"INSERT INTO workflow_executions
             (id, execution_id, workflow_id, workflow_name, workflow_type, workflow_version,
              namespace, task_queue, status, trigger_type, organization_id,
              parent_execution_id, result, completed_at,
              submitted_at, created_at, updated_at)
           VALUES ($1, $2, $3, 'orders', 'orders', '1.0.0', $4, 'q', $5, 'api', $6,
                   $7, $8, CASE WHEN $5 = 'completed' THEN NOW() END,
                   NOW() - make_interval(secs => $9),
                   NOW() - make_interval(secs => $9),
                   NOW())"#,
    )
    .bind(id)
    .bind(&execution_id)
    .bind(run.workflow_id)
    .bind(run.namespace)
    .bind(run.status)
    .bind(run.organization)
    .bind(run.parent_execution_id)
    .bind(run.result)
    .bind(run.age_secs as f64)
    .execute(pool)
    .await
    .expect("seed run");
    execution_id
}

async fn status_of(pool: &PgPool, execution_id: &str) -> String {
    sqlx::query_scalar("SELECT status FROM workflow_executions WHERE execution_id = $1")
        .bind(execution_id)
        .fetch_one(pool)
        .await
        .expect("read status")
}

async fn forget(pool: &PgPool, workflow_id: &str) {
    let _ = sqlx::query(
        "DELETE FROM workflow_execution_journal WHERE workflow_id IN \
         (SELECT id::text FROM workflow_executions WHERE workflow_id = $1)",
    )
    .bind(workflow_id)
    .execute(pool)
    .await;
    let _ = sqlx::query("DELETE FROM workflow_executions WHERE workflow_id = $1")
        .bind(workflow_id)
        .execute(pool)
        .await;
}

/// `message` sent as `organization`.
fn as_org<T>(organization: Uuid, message: T) -> Request<T> {
    let mut request = Request::new(message);
    request.metadata_mut().insert(
        ORGANIZATION_METADATA_KEY,
        organization.to_string().parse().unwrap(),
    );
    request
}

fn assert_not_found<T: std::fmt::Debug>(rpc: &str, outcome: Result<T, Status>) {
    match outcome {
        Err(status) => assert_eq!(
            status.code(),
            Code::NotFound,
            "{rpc}: expected not-found, got {status:?}"
        ),
        Ok(reply) => panic!("{rpc}: another organisation's run was answered: {reply:?}"),
    }
}

/// The reported failure: describing a workflow by its id named it as a
/// workflow type, so it was never found.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_workflow_is_described_by_its_id() {
    let pool = pool().await;
    let query = query_service(&pool);
    let workflow_id = format!("order-{}", Uuid::new_v4());
    let ours = organization(&pool).await;
    let run = seed(&pool, Seed::running(ours, "default", &workflow_id, 0)).await;

    let described = query
        .describe_workflow_execution(as_org(
            ours,
            DescribeWorkflowExecutionRequest {
                namespace: "default".into(),
                workflow_id: workflow_id.clone(),
                execution_id: String::new(),
            },
        ))
        .await
        .expect("describe by workflow id")
        .into_inner();
    let info = described.execution_info.expect("execution info");
    assert_eq!(info.execution_id, run);
    assert_eq!(info.workflow_id, workflow_id);

    // The journal is read by workflow id the same way.
    query
        .get_execution_journal(as_org(
            ours,
            GetExecutionJournalRequest {
                namespace: "default".into(),
                workflow_id: workflow_id.clone(),
                ..Default::default()
            },
        ))
        .await
        .expect("journal by workflow id");

    // And a workflow *type* is no longer accepted as if it were an id.
    assert_not_found(
        "describe by type",
        query
            .describe_workflow_execution(as_org(
                ours,
                DescribeWorkflowExecutionRequest {
                    namespace: "default".into(),
                    workflow_id: "orders".into(),
                    execution_id: String::new(),
                },
            ))
            .await,
    );

    forget(&pool, &workflow_id).await;
}

/// Another organization's newer run of the same workflow id used to hide the
/// caller's own: the lookup found theirs first, then refused it. And a third
/// organization reaches neither run through any RPC.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn every_rpc_names_only_the_callers_own_run() {
    let pool = pool().await;
    let workflows = workflow_service(&pool);
    let query = query_service(&pool);
    let workflow_id = format!("order-{}", Uuid::new_v4());
    let wf = workflow_id.as_str();

    let ours = organization(&pool).await;
    let theirs = organization(&pool).await;
    let stranger = organization(&pool).await;
    let our_run = seed(&pool, Seed::running(ours, "default", wf, 60)).await;
    let their_run = seed(&pool, Seed::running(theirs, "default", wf, 0)).await;

    // The owner finds its own run by workflow id, though theirs is newer.
    let status = workflows
        .get_workflow_status(as_org(
            ours,
            GetWorkflowStatusRequest {
                workflow_id: wf.into(),
                namespace: "default".into(),
                ..Default::default()
            },
        ))
        .await
        .expect("the owner's own status")
        .into_inner();
    assert_eq!(status.execution.expect("execution").execution_id, our_run);

    // A stranger reaches neither run — by workflow id, or by workflow id and
    // run id (events, queries and updates require the workflow id) — through
    // any RPC that addresses a run.
    for (workflow_id, execution_id) in [
        (wf.to_string(), String::new()),
        (wf.to_string(), our_run.clone()),
    ] {
        let ids = || (workflow_id.clone(), execution_id.clone());
        let r = || {
            let (workflow_id, execution_id) = ids();
            (workflow_id, execution_id, "default".to_string())
        };

        let (w, e, n) = r();
        assert_not_found(
            "status",
            workflows
                .get_workflow_status(as_org(
                    stranger,
                    GetWorkflowStatusRequest {
                        workflow_id: w,
                        execution_id: e,
                        namespace: n,
                    },
                ))
                .await,
        );
        let (w, e, n) = r();
        assert_not_found(
            "result",
            workflows
                .get_workflow_result(as_org(
                    stranger,
                    GetWorkflowResultRequest {
                        workflow_id: w,
                        execution_id: e,
                        namespace: n,
                        timeout: Some(prost_types::Duration {
                            seconds: 1,
                            nanos: 0,
                        }),
                    },
                ))
                .await,
        );
        let (w, e, n) = r();
        assert_not_found(
            "describe",
            query
                .describe_workflow_execution(as_org(
                    stranger,
                    DescribeWorkflowExecutionRequest {
                        namespace: n,
                        workflow_id: w,
                        execution_id: e,
                    },
                ))
                .await,
        );
        let (w, e, n) = r();
        assert_not_found(
            "journal",
            query
                .get_execution_journal(as_org(
                    stranger,
                    GetExecutionJournalRequest {
                        namespace: n,
                        workflow_id: w,
                        execution_id: e,
                        ..Default::default()
                    },
                ))
                .await,
        );
        let (w, e, n) = r();
        assert_not_found(
            "logs",
            query
                .get_execution_logs(as_org(
                    stranger,
                    GetExecutionLogsRequest {
                        namespace: n,
                        workflow_id: w,
                        execution_id: e,
                        page_size: 10,
                        ..Default::default()
                    },
                ))
                .await,
        );
        let (w, e, n) = r();
        assert_not_found(
            "task executions",
            query
                .get_task_executions(as_org(
                    stranger,
                    GetTaskExecutionsRequest {
                        namespace: n,
                        workflow_id: w,
                        execution_id: e,
                        ..Default::default()
                    },
                ))
                .await,
        );
        let (w, e, n) = r();
        assert_not_found(
            "send event",
            workflows
                .send_event(as_org(
                    stranger,
                    SendEventRequest {
                        workflow_id: w,
                        execution_id: e,
                        namespace: n,
                        event_name: "approve".into(),
                        payload: b"{}".to_vec(),
                        ..Default::default()
                    },
                ))
                .await,
        );
        let (w, e, n) = r();
        assert_not_found(
            "query",
            workflows
                .query_workflow(as_org(
                    stranger,
                    QueryWorkflowRequest {
                        workflow_id: w,
                        execution_id: e,
                        namespace: n,
                        query_type: "state".into(),
                        ..Default::default()
                    },
                ))
                .await,
        );
        let (w, e, n) = r();
        assert_not_found(
            "update",
            workflows
                .update_workflow(as_org(
                    stranger,
                    UpdateWorkflowRequest {
                        workflow_id: w,
                        execution_id: e,
                        namespace: n,
                        update_type: "rename".into(),
                        args: b"{}".to_vec(),
                        ..Default::default()
                    },
                ))
                .await,
        );
        let (w, e, n) = r();
        assert_not_found(
            "cancel",
            workflows
                .cancel_workflow(as_org(
                    stranger,
                    CancelWorkflowRequest {
                        workflow_id: w,
                        execution_id: e,
                        namespace: n,
                        reason: "x".into(),
                        ..Default::default()
                    },
                ))
                .await,
        );
        let (w, e, n) = r();
        assert_not_found(
            "reset",
            workflows
                .reset_workflow(as_org(
                    stranger,
                    ResetWorkflowRequest {
                        workflow_id: w,
                        execution_id: e,
                        namespace: n,
                        target_event_id: 1,
                        reason: "x".into(),
                    },
                ))
                .await,
        );
        let (w, e, n) = r();
        assert_not_found(
            "terminate",
            workflows
                .terminate_workflow(as_org(
                    stranger,
                    TerminateWorkflowRequest {
                        workflow_id: w,
                        execution_id: e,
                        namespace: n,
                        reason: "x".into(),
                        ..Default::default()
                    },
                ))
                .await,
        );
    }
    assert_eq!(status_of(&pool, &our_run).await, "running");
    assert_eq!(status_of(&pool, &their_run).await, "running");

    // Terminating by workflow id ends the caller's own run, not theirs.
    workflows
        .terminate_workflow(as_org(
            ours,
            TerminateWorkflowRequest {
                workflow_id: wf.into(),
                namespace: "default".into(),
                reason: "done".into(),
                ..Default::default()
            },
        ))
        .await
        .expect("the owner terminates its own run");
    assert_eq!(status_of(&pool, &our_run).await, "terminated");
    assert_eq!(status_of(&pool, &their_run).await, "running");

    forget(&pool, wf).await;
}

/// A workflow id is unique only within a namespace: the request's namespace
/// decides which run it names, where the newest run of the id anywhere used to.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_workflow_id_names_a_run_in_the_requests_namespace() {
    let pool = pool().await;
    let workflows = workflow_service(&pool);
    let workflow_id = format!("order-{}", Uuid::new_v4());
    let ours = organization(&pool).await;
    let default_run = seed(&pool, Seed::running(ours, "default", &workflow_id, 60)).await;
    let billing_run = seed(&pool, Seed::running(ours, "billing", &workflow_id, 0)).await;

    workflows
        .terminate_workflow(as_org(
            ours,
            TerminateWorkflowRequest {
                workflow_id: workflow_id.clone(),
                namespace: "default".into(),
                reason: "done".into(),
                ..Default::default()
            },
        ))
        .await
        .expect("terminate in the default namespace");
    assert_eq!(status_of(&pool, &default_run).await, "terminated");
    assert_eq!(
        status_of(&pool, &billing_run).await,
        "running",
        "a newer run of the same id in another namespace was terminated instead"
    );

    // An empty namespace is the default one.
    let status = workflows
        .get_workflow_status(as_org(
            ours,
            GetWorkflowStatusRequest {
                workflow_id: workflow_id.clone(),
                ..Default::default()
            },
        ))
        .await
        .expect("status with no namespace")
        .into_inner();
    assert_eq!(
        status.execution.expect("execution").execution_id,
        default_run
    );

    forget(&pool, &workflow_id).await;
}

/// A result is the named run's: followed through the runs that replaced it
/// (here a reset), and never a later workflow that reused the id.
#[tokio::test]
#[ignore] // Requires PostgreSQL
async fn a_result_follows_the_named_runs_lineage() {
    let pool = pool().await;
    let workflows = workflow_service(&pool);
    let workflow_id = format!("order-{}", Uuid::new_v4());
    let wf = workflow_id.as_str();
    let ours = organization(&pool).await;

    let reset = seed(
        &pool,
        Seed {
            status: "reset",
            ..Seed::running(ours, "default", wf, 120)
        },
    )
    .await;
    seed(
        &pool,
        Seed {
            status: "completed",
            parent_execution_id: Some(&reset),
            result: Some(serde_json::json!({ "run": "successor" })),
            ..Seed::running(ours, "default", wf, 60)
        },
    )
    .await;
    // The id reused afterwards by an unrelated workflow.
    seed(
        &pool,
        Seed {
            status: "completed",
            result: Some(serde_json::json!({ "run": "reuse" })),
            ..Seed::running(ours, "default", wf, 0)
        },
    )
    .await;

    let result_of = |execution_id: String| {
        let workflows = &workflows;
        async move {
            let reply = workflows
                .get_workflow_result(as_org(
                    ours,
                    GetWorkflowResultRequest {
                        workflow_id: wf.into(),
                        execution_id,
                        namespace: "default".into(),
                        timeout: Some(prost_types::Duration {
                            seconds: 5,
                            nanos: 0,
                        }),
                    },
                ))
                .await
                .expect("a result")
                .into_inner();
            match reply.outcome {
                Some(Outcome::Result(bytes)) => {
                    serde_json::from_slice::<serde_json::Value>(&bytes).unwrap()["run"].clone()
                }
                other => panic!("expected a result, got {other:?}"),
            }
        }
    };

    assert_eq!(result_of(reset.clone()).await, "successor");
    // By workflow id alone: the workflow's latest run.
    assert_eq!(result_of(String::new()).await, "reuse");

    forget(&pool, wf).await;
}
