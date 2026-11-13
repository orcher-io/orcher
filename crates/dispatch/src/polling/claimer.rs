//! Atomically claims and releases workflow executions via `SELECT FOR UPDATE SKIP LOCKED`.

use orcher_common::error::OrcherError;
use orcher_common::OrcherResult;
use orcher_storage::{
    repositories::domains::workflow_execution::WorkflowExecutionRepository,
    schema::types::execution::WorkflowExecution,
};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

const DEFAULT_CLAIM_QUERY_TIMEOUT_MS: u64 = 2000;
const DEFAULT_BATCH_SIZE: i32 = 10;

pub struct Claimer {
    execution_repo: Arc<WorkflowExecutionRepository>,
    claim_timeout_secs: i64,
    /// How long to wait for the DB query itself, not the claim duration.
    claim_query_timeout_ms: u64,
}

/// What a claim took: each execution and the activation claimed of it.
trait Claimed {
    fn claims(&self) -> Vec<(String, i64)>;
}

impl Claimed for Option<WorkflowExecution> {
    fn claims(&self) -> Vec<(String, i64)> {
        self.iter()
            .map(|e| (e.execution_id.clone(), e.activation_seq))
            .collect()
    }
}

impl Claimed for Vec<WorkflowExecution> {
    fn claims(&self) -> Vec<(String, i64)> {
        self.iter()
            .map(|e| (e.execution_id.clone(), e.activation_seq))
            .collect()
    }
}

/// A claim running on its own task. Dropped before its result is taken —
/// its caller stopped waiting for it, or was itself dropped — it is left to
/// finish, and whatever it claimed is released: the claim may already have
/// committed, and nothing would hand out what it took.
struct ClaimInFlight<T: Claimed + Send + 'static> {
    handle: Option<tokio::task::JoinHandle<OrcherResult<T>>>,
    repo: Arc<WorkflowExecutionRepository>,
    identity: String,
}

impl<T: Claimed + Send + 'static> Drop for ClaimInFlight<T> {
    fn drop(&mut self) {
        let Some(handle) = self.handle.take() else {
            return;
        };
        let repo = self.repo.clone();
        let identity = std::mem::take(&mut self.identity);
        tokio::spawn(async move {
            let Ok(Ok(claimed)) = handle.await else {
                return;
            };
            for (execution_id, activation) in claimed.claims() {
                match repo
                    .release_undelivered(&execution_id, activation, &identity)
                    .await
                {
                    Ok(_) => info!(
                        execution_id = %execution_id,
                        "Released a claim made after its poll stopped waiting for it"
                    ),
                    Err(e) => warn!(
                        execution_id = %execution_id,
                        error = %e,
                        "Could not release a claim made after its poll stopped waiting; \
                         it is released when it times out"
                    ),
                }
            }
        });
    }
}

impl Claimer {
    pub fn new(execution_repo: Arc<WorkflowExecutionRepository>, claim_timeout_secs: i64) -> Self {
        Self::with_query_timeout(
            execution_repo,
            claim_timeout_secs,
            DEFAULT_CLAIM_QUERY_TIMEOUT_MS,
        )
    }

    pub fn with_query_timeout(
        execution_repo: Arc<WorkflowExecutionRepository>,
        claim_timeout_secs: i64,
        claim_query_timeout_ms: u64,
    ) -> Self {
        debug!(
            claim_timeout_secs = claim_timeout_secs,
            claim_query_timeout_ms = claim_query_timeout_ms,
            "Creating claimer"
        );

        Self {
            execution_repo,
            claim_timeout_secs,
            claim_query_timeout_ms,
        }
    }

    /// Run `claim`, a claim for `identity`, waiting for it at most `wait`.
    ///
    /// A claim still running then is not cancelled, nor is one whose caller
    /// is dropped meanwhile: either may already have committed, and dropped,
    /// its executions would stay claimed by nobody until the claim timed out
    /// — minutes in which their workflows could not run. It is left to finish
    /// and what it claimed is released.
    async fn bounded<T, F>(
        &self,
        identity: &str,
        wait: Duration,
        claim: F,
    ) -> Result<OrcherResult<T>, tokio::time::error::Elapsed>
    where
        T: Claimed + Send + 'static,
        F: std::future::Future<Output = OrcherResult<T>> + Send + 'static,
    {
        let mut in_flight = ClaimInFlight {
            handle: Some(tokio::spawn(claim)),
            repo: self.execution_repo.clone(),
            identity: identity.to_string(),
        };
        let handle = in_flight.handle.as_mut().expect("just spawned");
        let joined = tokio::time::timeout(wait, handle).await?;
        in_flight.handle = None;
        Ok(
            joined
                .unwrap_or_else(|e| Err(OrcherError::internal(format!("Claim task failed: {e}")))),
        )
    }

    /// Returns `None` when no work is available.
    /// `version_id` is the claiming worker's declared code release, or `None`.
    /// It binds the execution on first claim only.
    /// `organization_id` is the organization the poll was made for; with one,
    /// only its executions are claimed. `worker_instance_id` is the worker
    /// process the poll came from, recorded on the claim; `None` if it named
    /// none.
    #[allow(clippy::too_many_arguments)]
    pub async fn claim_execution(
        &self,
        task_queue: &str,
        namespace: &str,
        identity: &str,
        version_id: Option<&str>,
        organization_id: Option<uuid::Uuid>,
        worker_instance_id: Option<&str>,
        step_ids: &[i16],
    ) -> OrcherResult<Option<WorkflowExecution>> {
        debug!(
            task_queue = %task_queue,
            namespace = %namespace,
            identity = %identity,
            claim_timeout_secs = self.claim_timeout_secs,
            "Claiming an execution"
        );

        // Add explicit timeout wrapper to diagnose hanging queries
        // Uses configurable timeout (default 2000ms) to handle PostgreSQL latency under load
        let claim_timeout = Duration::from_millis(self.claim_query_timeout_ms);

        debug!(
            task_queue = %task_queue,
            timeout_ms = self.claim_query_timeout_ms,
            "Starting claim with explicit timeout wrapper"
        );

        let claim_result = self
            .bounded(identity, claim_timeout, {
                let repo = self.execution_repo.clone();
                let task_queue = task_queue.to_string();
                let namespace = namespace.to_string();
                let identity = identity.to_string();
                let claim_timeout_secs = self.claim_timeout_secs;
                let version_id = version_id.map(str::to_string);
                let worker_instance_id = worker_instance_id.map(str::to_string);
                let step_ids = step_ids.to_vec();
                async move {
                    repo.claim_execution_for_poll(
                        &task_queue,
                        &namespace,
                        &identity,
                        claim_timeout_secs,
                        version_id.as_deref(),
                        organization_id,
                        worker_instance_id.as_deref(),
                        &step_ids,
                    )
                    .await
                }
            })
            .await;

        // Handle timeout case
        let result = match claim_result {
            Ok(Ok(result)) => {
                debug!(
                    task_queue = %task_queue,
                    namespace = %namespace,
                    "Claim query completed successfully"
                );
                result
            }
            Ok(Err(e)) => {
                error!(
                    task_queue = %task_queue,
                    namespace = %namespace,
                    error = %e,
                    "Claim query failed with error"
                );
                return Err(e);
            }
            Err(_) => {
                warn!(
                    task_queue = %task_queue,
                    namespace = %namespace,
                    timeout_ms = self.claim_query_timeout_ms,
                    "Claim query timed out (likely query hanging or DB under heavy load)"
                );
                return Err(OrcherError::timeout(
                    format!(
                        "Claim query timed out after {}ms",
                        self.claim_query_timeout_ms
                    ),
                    0,
                ));
            }
        };

        match &result {
            Some(execution) => {
                debug!(
                    execution_id = %execution.execution_id,
                    workflow_id = %execution.workflow_name,
                    claimed_by = ?execution.claimed_by,
                    "Claimed an execution"
                );

                // Record schedule-to-start latency metric
                // This is the time from when the workflow was submitted to when a service claimed it
                let now = orcher_common::types::time::Timestamptz::now();
                let duration = now.signed_duration_since(execution.submitted_at);
                let latency = duration.unsigned_abs();
                {
                    orcher_monitoring::metrics::polling::record_schedule_to_start_latency(
                        task_queue, namespace, latency,
                    );
                }
            }
            None => {
                debug!(
                    task_queue = %task_queue,
                    namespace = %namespace,
                    "No execution to claim"
                );
            }
        }

        Ok(result)
    }

    /// Fast path for when the execution_id is already known (e.g. from Redis BRPOP) --
    /// just an UPDATE by primary key, typically < 10ms.
    pub async fn claim_by_execution_id(
        &self,
        execution_id: &str,
        identity: &str,
        version_id: Option<&str>,
        organization_id: Option<uuid::Uuid>,
        worker_instance_id: Option<&str>,
        step_ids: &[i16],
    ) -> OrcherResult<Option<WorkflowExecution>> {
        debug!(
            execution_id = %execution_id,
            identity = %identity,
            claim_timeout_secs = self.claim_timeout_secs,
            "Attempting fast claim by execution_id"
        );

        // Fast claim with timeout for indexed lookup (increased to handle DB latency)
        let claim_timeout = Duration::from_millis(2000);

        let claim_result = self
            .bounded(identity, claim_timeout, {
                let repo = self.execution_repo.clone();
                let execution_id = execution_id.to_string();
                let identity = identity.to_string();
                let claim_timeout_secs = self.claim_timeout_secs;
                let version_id = version_id.map(str::to_string);
                let worker_instance_id = worker_instance_id.map(str::to_string);
                let step_ids = step_ids.to_vec();
                async move {
                    repo.claim_execution_by_id_from(
                        &execution_id,
                        &identity,
                        claim_timeout_secs,
                        version_id.as_deref(),
                        organization_id,
                        worker_instance_id.as_deref(),
                        &step_ids,
                    )
                    .await
                }
            })
            .await;

        let result = match claim_result {
            Ok(Ok(result)) => result,
            Ok(Err(e)) => {
                error!(
                    execution_id = %execution_id,
                    error = %e,
                    "Fast claim by execution_id failed"
                );
                return Err(e);
            }
            Err(_) => {
                warn!(
                    execution_id = %execution_id,
                    timeout_ms = 2000,
                    "Fast claim by execution_id timed out"
                );
                return Err(OrcherError::timeout(
                    "Fast claim by execution_id timed out",
                    0,
                ));
            }
        };

        match &result {
            Some(execution) => {
                info!(
                    execution_id = %execution.execution_id,
                    workflow_name = %execution.workflow_name,
                    claimed_by = ?execution.claimed_by,
                    "Successfully claimed execution by ID (fast path)"
                );
            }
            None => {
                debug!(
                    execution_id = %execution_id,
                    "Execution not claimable (already claimed or not found)"
                );
            }
        }

        Ok(result)
    }

    /// Move a claimed activation to the poller it is handed to; see
    /// `WorkflowExecutionRepository::hand_to`.
    pub async fn hand_to(
        &self,
        execution_id: &str,
        activation: i64,
        identity: &str,
        organization_id: Option<uuid::Uuid>,
        worker_instance_id: Option<&str>,
    ) -> OrcherResult<orcher_storage::repositories::domains::workflow_execution::HandOut> {
        self.execution_repo
            .hand_to(
                execution_id,
                activation,
                identity,
                self.claim_timeout_secs,
                organization_id,
                worker_instance_id,
            )
            .await
    }

    /// The organization `execution_id` belongs to, if it is waiting to be
    /// claimed; see `WorkflowExecutionRepository::unclaimed_owner`.
    pub async fn unclaimed_owner(
        &self,
        execution_id: &str,
    ) -> OrcherResult<Option<Option<uuid::Uuid>>> {
        self.execution_repo.unclaimed_owner(execution_id).await
    }

    /// Wake a parked execution; see
    /// `WorkflowExecutionRepository::release_claim_to_resume`.
    pub async fn release_claim_to_resume(
        &self,
        execution_id: &str,
    ) -> OrcherResult<orcher_storage::repositories::domains::workflow_execution::Resume> {
        self.execution_repo
            .release_claim_to_resume(execution_id)
            .await
    }

    pub async fn release_claim(&self, execution_id: &str) -> OrcherResult<()> {
        debug!(
            execution_id = %execution_id,
            "Releasing claim"
        );

        self.execution_repo.release_claim(execution_id).await?;

        debug!(
            execution_id = %execution_id,
            "Successfully released claim"
        );

        Ok(())
    }

    pub fn claim_timeout_secs(&self) -> i64 {
        self.claim_timeout_secs
    }

    pub fn claim_query_timeout_ms(&self) -> u64 {
        self.claim_query_timeout_ms
    }

    /// Claims up to `limit` executions in a single DB round-trip.
    #[allow(clippy::too_many_arguments)]
    pub async fn claim_batch(
        &self,
        task_queue: &str,
        namespace: &str,
        identity: &str,
        limit: Option<i32>,
        version_id: Option<&str>,
        organization_id: Option<uuid::Uuid>,
        step_ids: &[i16],
    ) -> OrcherResult<Vec<WorkflowExecution>> {
        let batch_size = limit.unwrap_or(DEFAULT_BATCH_SIZE);

        debug!(
            task_queue = %task_queue,
            namespace = %namespace,
            identity = %identity,
            batch_size = batch_size,
            claim_timeout_secs = self.claim_timeout_secs,
            "Claiming a batch of executions"
        );

        // Add explicit timeout wrapper to handle slow queries under load
        let claim_timeout = Duration::from_millis(self.claim_query_timeout_ms);

        debug!(
            task_queue = %task_queue,
            batch_size = batch_size,
            timeout_ms = self.claim_query_timeout_ms,
            "Starting batch claim with explicit timeout wrapper"
        );

        let claim_result = self
            .bounded(identity, claim_timeout, {
                let repo = self.execution_repo.clone();
                let task_queue = task_queue.to_string();
                let namespace = namespace.to_string();
                let identity = identity.to_string();
                let claim_timeout_secs = self.claim_timeout_secs;
                let version_id = version_id.map(str::to_string);
                let step_ids = step_ids.to_vec();
                async move {
                    repo.claim_batch_with_offset(
                        &task_queue,
                        &namespace,
                        &identity,
                        claim_timeout_secs,
                        batch_size,
                        0,
                        version_id.as_deref(),
                        organization_id,
                        &step_ids,
                    )
                    .await
                }
            })
            .await;

        // Handle timeout case
        let result = match claim_result {
            Ok(Ok(result)) => {
                debug!(
                    task_queue = %task_queue,
                    namespace = %namespace,
                    claimed_count = result.len(),
                    "Batch claim query completed successfully"
                );
                result
            }
            Ok(Err(e)) => {
                error!(
                    task_queue = %task_queue,
                    namespace = %namespace,
                    error = %e,
                    "Batch claim query failed with error"
                );
                return Err(e);
            }
            Err(_) => {
                warn!(
                    task_queue = %task_queue,
                    namespace = %namespace,
                    timeout_ms = self.claim_query_timeout_ms,
                    "Batch claim query timed out (likely query hanging or DB under heavy load)"
                );
                return Err(OrcherError::timeout(
                    format!(
                        "Batch claim query timed out after {}ms",
                        self.claim_query_timeout_ms
                    ),
                    0,
                ));
            }
        };

        if !result.is_empty() {
            debug!(
                task_queue = %task_queue,
                namespace = %namespace,
                claimed_count = result.len(),
                requested = batch_size,
                "Claimed a batch of {} executions",
                result.len()
            );
        } else {
            debug!(
                task_queue = %task_queue,
                namespace = %namespace,
                "No executions to claim for a batch"
            );
        }

        Ok(result)
    }

    /// Like `claim_batch` but skips `offset` rows first -- stagger offsets across
    /// workers to reduce index-page lock contention.
    #[allow(clippy::too_many_arguments)]
    pub async fn claim_batch_with_offset(
        &self,
        task_queue: &str,
        namespace: &str,
        identity: &str,
        limit: Option<i32>,
        offset: i32,
        version_id: Option<&str>,
        organization_id: Option<uuid::Uuid>,
        step_ids: &[i16],
    ) -> OrcherResult<Vec<WorkflowExecution>> {
        let batch_size = limit.unwrap_or(DEFAULT_BATCH_SIZE);

        debug!(
            task_queue = %task_queue,
            namespace = %namespace,
            identity = %identity,
            batch_size = batch_size,
            offset = offset,
            claim_timeout_secs = self.claim_timeout_secs,
            "Claiming a batch of executions from an offset"
        );

        // Add explicit timeout wrapper to handle slow queries under load
        let claim_timeout = Duration::from_millis(self.claim_query_timeout_ms);

        debug!(
            task_queue = %task_queue,
            batch_size = batch_size,
            offset = offset,
            timeout_ms = self.claim_query_timeout_ms,
            "Starting batch claim with offset and explicit timeout wrapper"
        );

        let claim_result = self
            .bounded(identity, claim_timeout, {
                let repo = self.execution_repo.clone();
                let task_queue = task_queue.to_string();
                let namespace = namespace.to_string();
                let identity = identity.to_string();
                let claim_timeout_secs = self.claim_timeout_secs;
                let version_id = version_id.map(str::to_string);
                let step_ids = step_ids.to_vec();
                async move {
                    repo.claim_batch_with_offset(
                        &task_queue,
                        &namespace,
                        &identity,
                        claim_timeout_secs,
                        batch_size,
                        offset,
                        version_id.as_deref(),
                        organization_id,
                        &step_ids,
                    )
                    .await
                }
            })
            .await;

        // Handle timeout case
        let result = match claim_result {
            Ok(Ok(result)) => {
                debug!(
                    task_queue = %task_queue,
                    namespace = %namespace,
                    claimed_count = result.len(),
                    offset = offset,
                    "Batch claim with offset query completed successfully"
                );
                result
            }
            Ok(Err(e)) => {
                error!(
                    task_queue = %task_queue,
                    namespace = %namespace,
                    offset = offset,
                    error = %e,
                    "Batch claim with offset query failed with error"
                );
                return Err(e);
            }
            Err(_) => {
                warn!(
                    task_queue = %task_queue,
                    namespace = %namespace,
                    offset = offset,
                    timeout_ms = self.claim_query_timeout_ms,
                    "Batch claim with offset query timed out"
                );
                return Err(OrcherError::timeout(
                    format!(
                        "Batch claim with offset query timed out after {}ms",
                        self.claim_query_timeout_ms
                    ),
                    0,
                ));
            }
        };

        if !result.is_empty() {
            debug!(
                task_queue = %task_queue,
                namespace = %namespace,
                claimed_count = result.len(),
                requested = batch_size,
                offset = offset,
                "Claimed a batch of {} executions from an offset",
                result.len()
            );
        } else {
            debug!(
                task_queue = %task_queue,
                namespace = %namespace,
                offset = offset,
                "No executions to claim for a batch from an offset"
            );
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use orcher_storage::schema::types::execution::WorkflowExecution;
    use sqlx::PgPool;
    use uuid::Uuid;

    async fn create_test_pool() -> PgPool {
        let database_url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://orcher:orcher@localhost/orcher_test".to_string());

        PgPool::connect(&database_url)
            .await
            .expect("Failed to connect to test database")
    }

    #[tokio::test]
    #[ignore] // Requires database
    async fn test_claim_execution_success() {
        let pool = create_test_pool().await;
        let repo = Arc::new(WorkflowExecutionRepository::new(pool.clone()));
        let claimer = Claimer::new(repo.clone(), 300);

        // Create an execution
        let mut execution = WorkflowExecution::builder()
            .execution_id(format!("test_exec_{}", Uuid::new_v4()))
            .workflow_name("test_workflow".to_string())
            .workflow_version("v1.0".to_string())
            .namespace("test".to_string())
            .parameters(serde_json::json!({}))
            .build();

        execution.task_queue = "test-queue".to_string();
        execution.status = "pending".to_string();

        repo.create(&execution).await.unwrap();

        // Claim it
        let claimed = claimer
            .claim_execution("test-queue", "test", "service-1", None, None, None, &[])
            .await
            .unwrap();

        assert!(claimed.is_some());
        let claimed = claimed.unwrap();
        assert_eq!(claimed.claimed_by, Some("service-1".to_string()));
        assert!(claimed.claimed_at.is_some());
        assert!(claimed.claim_timeout_at.is_some());
    }

    #[tokio::test]
    #[ignore] // Requires database
    async fn test_claim_execution_none_available() {
        let pool = create_test_pool().await;
        let repo = Arc::new(WorkflowExecutionRepository::new(pool));
        let claimer = Claimer::new(repo, 300);

        // Try to claim from empty queue
        let claimed = claimer
            .claim_execution("empty-queue", "test", "service-1", None, None, None, &[])
            .await
            .unwrap();

        assert!(claimed.is_none());
    }

    #[tokio::test]
    #[ignore] // Requires database
    async fn test_claim_execution_atomic() {
        // Test that two services can't claim the same execution
        let pool = create_test_pool().await;
        let repo = Arc::new(WorkflowExecutionRepository::new(pool.clone()));
        let claimer1 = Claimer::new(repo.clone(), 300);
        let claimer2 = Claimer::new(repo.clone(), 300);

        // Create one execution
        let mut execution = WorkflowExecution::builder()
            .execution_id(format!("test_exec_{}", Uuid::new_v4()))
            .workflow_name("test_workflow".to_string())
            .workflow_version("v1.0".to_string())
            .namespace("test".to_string())
            .parameters(serde_json::json!({}))
            .build();

        execution.task_queue = "test-queue".to_string();
        execution.status = "pending".to_string();

        repo.create(&execution).await.unwrap();

        // Both services try to claim simultaneously
        let (result1, result2) = tokio::join!(
            claimer1.claim_execution("test-queue", "test", "service-1", None, None, None, &[]),
            claimer2.claim_execution("test-queue", "test", "service-2", None, None, None, &[])
        );

        let claimed1 = result1.unwrap();
        let claimed2 = result2.unwrap();

        // One should succeed, one should get None
        let success_count = claimed1.is_some() as i32 + claimed2.is_some() as i32;
        assert_eq!(
            success_count, 1,
            "Exactly one service should claim the execution"
        );

        // Verify the one that succeeded has a claim
        if let Some(exec) = claimed1 {
            assert_eq!(exec.claimed_by, Some("service-1".to_string()));
        }
        if let Some(exec) = claimed2 {
            assert_eq!(exec.claimed_by, Some("service-2".to_string()));
        }
    }

    #[tokio::test]
    #[ignore] // Requires database
    async fn test_release_claim() {
        let pool = create_test_pool().await;
        let repo = Arc::new(WorkflowExecutionRepository::new(pool.clone()));
        let claimer = Claimer::new(repo.clone(), 300);

        // Create and claim an execution
        let mut execution = WorkflowExecution::builder()
            .execution_id(format!("test_exec_{}", Uuid::new_v4()))
            .workflow_name("test_workflow".to_string())
            .workflow_version("v1.0".to_string())
            .namespace("test".to_string())
            .parameters(serde_json::json!({}))
            .build();

        execution.task_queue = "test-queue".to_string();
        execution.status = "pending".to_string();

        let _created = repo.create(&execution).await.unwrap();

        let claimed = claimer
            .claim_execution("test-queue", "test", "service-1", None, None, None, &[])
            .await
            .unwrap()
            .unwrap();

        // Release the claim
        claimer.release_claim(&claimed.execution_id).await.unwrap();

        // Verify claim was released
        let execution = repo
            .find_by_execution_id(&claimed.execution_id)
            .await
            .unwrap()
            .unwrap();

        assert!(execution.claimed_by.is_none());
        assert!(execution.claimed_at.is_none());
        assert!(execution.claim_timeout_at.is_none());
    }

    #[tokio::test]
    #[ignore] // Requires database
    async fn test_release_claim_allows_reclaim() {
        let pool = create_test_pool().await;
        let repo = Arc::new(WorkflowExecutionRepository::new(pool.clone()));
        let claimer = Claimer::new(repo.clone(), 300);

        // Create and claim an execution
        let mut execution = WorkflowExecution::builder()
            .execution_id(format!("test_exec_{}", Uuid::new_v4()))
            .workflow_name("test_workflow".to_string())
            .workflow_version("v1.0".to_string())
            .namespace("test".to_string())
            .parameters(serde_json::json!({}))
            .build();

        execution.task_queue = "test-queue".to_string();
        execution.status = "pending".to_string();

        repo.create(&execution).await.unwrap();

        // Service 1 claims
        let claimed1 = claimer
            .claim_execution("test-queue", "test", "service-1", None, None, None, &[])
            .await
            .unwrap()
            .unwrap();

        // Service 1 releases
        claimer.release_claim(&claimed1.execution_id).await.unwrap();

        // Service 2 should be able to claim
        let claimed2 = claimer
            .claim_execution("test-queue", "test", "service-2", None, None, None, &[])
            .await
            .unwrap();

        assert!(claimed2.is_some());
        assert_eq!(claimed2.unwrap().claimed_by, Some("service-2".to_string()));
    }

    #[tokio::test]
    async fn test_claimer_configuration() {
        let pool = PgPool::connect_lazy("postgres://test").expect("Failed to create lazy pool");
        let repo = Arc::new(WorkflowExecutionRepository::new(pool));
        let claimer = Claimer::new(repo, 600);

        assert_eq!(claimer.claim_timeout_secs(), 600);
    }
}
