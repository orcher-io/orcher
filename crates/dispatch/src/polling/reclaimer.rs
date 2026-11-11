//! Periodically recovers executions left behind by crashed services.

use orcher_common::types::time::Timestamptz;
use orcher_common::OrcherResult;
use orcher_storage::repositories::domains::matching::MatchingStore;
use orcher_storage::repositories::domains::task_execution::TaskExecutionRepository;
use orcher_storage::repositories::domains::workflow_execution::WorkflowExecutionRepository;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tracing::{debug, error, info};

use super::notifier::Notifier;
use crate::routing::WorkerAffinityManager;

pub struct Reclaimer {
    execution_repo: Arc<WorkflowExecutionRepository>,
    notifier: Notifier,
    shutdown: Arc<Notify>,
    /// Optional task execution repository for eager-claim TTL reclamation.
    task_execution_repo: Option<Arc<TaskExecutionRepository>>,
    /// Optional affinity manager for periodic expired-lease cleanup.
    affinity_manager: Option<Arc<WorkerAffinityManager>>,
    /// The wake-up sweep, when enabled; see [`Reclaimer::with_wake_sweep`].
    wake_sweep: Option<WakeSweep>,
    /// The matching partitions, once the engine has them. Set after the
    /// reclaimer starts, so read on every tick.
    matching: Option<Arc<std::sync::OnceLock<Arc<dyn MatchingStore>>>>,
}

/// Queues a continuation for an execution the wake-up sweep released:
/// `(task_queue, namespace, execution_id)`.
pub type Continuation = Arc<dyn Fn(&str, &str, &str) + Send + Sync>;

/// How long the ordinary wake-up is given to follow its outcome before the
/// sweep takes it as lost. It follows by milliseconds; the margin is for a
/// wake-up that is retried on a transient error.
pub const WAKE_GRACE_SECS: i64 = 10;

/// How far back past the previous sweep each sweep reads again, for an outcome
/// whose journal entry was stamped before it committed.
const WAKE_OVERLAP_SECS: i64 = 30;

/// Parked executions one wake-up sweep considers at most; the next tick goes on.
const WAKE_BATCH: i64 = 200;

/// Matching rows returned to their partition in one tick at most.
const MATCHING_RECLAIM_BATCH: i64 = 500;

struct WakeSweep {
    continuation: Continuation,
    /// How far back the first sweep reads: an outcome older than a claim
    /// timeout has had its workflow's claim time out and be reclaimed anyway.
    lookback_secs: i64,
    /// Up to where the previous sweep read.
    read_up_to: std::sync::Mutex<Option<Timestamptz>>,
}

impl Reclaimer {
    pub fn new(execution_repo: Arc<WorkflowExecutionRepository>, notifier: Notifier) -> Self {
        debug!("Creating reclaimer");

        Self {
            execution_repo,
            notifier,
            shutdown: Arc::new(Notify::new()),
            task_execution_repo: None,
            affinity_manager: None,
            wake_sweep: None,
            matching: None,
        }
    }

    /// Also wake the parked workflows whose wake-up was lost, queuing a
    /// continuation for each through `continuation`; see
    /// `WorkflowExecutionRepository::wake_parked_with_unconsumed_outcomes`.
    /// `claim_timeout_secs` bounds how far back the first sweep looks.
    pub fn with_wake_sweep(mut self, continuation: Continuation, claim_timeout_secs: i64) -> Self {
        self.wake_sweep = Some(WakeSweep {
            continuation,
            lookback_secs: claim_timeout_secs,
            read_up_to: std::sync::Mutex::new(None),
        });
        self
    }

    /// Also return to their partition the matching rows whose lease expired
    /// before the task was taken, once `matching` is set.
    ///
    /// A poll claims a row and then marks its task started; the row is
    /// deleted once the task is taken. A row still claimed when its lease
    /// runs out is one whose poll never got that far — an engine that stopped
    /// in between, or a start that failed on a transient error — and its task
    /// is still waiting, with nothing else to offer it again. Returned to
    /// `ready`, it is claimed by the next poll, which either takes the task or,
    /// finding it already taken, drops the row.
    pub fn with_matching(
        mut self,
        matching: Arc<std::sync::OnceLock<Arc<dyn MatchingStore>>>,
    ) -> Self {
        self.matching = Some(matching);
        self
    }

    /// Attach a task execution repository so the reclaimer also resets expired eager claims.
    pub fn with_task_repo(mut self, repo: Arc<TaskExecutionRepository>) -> Self {
        self.task_execution_repo = Some(repo);
        self
    }

    /// Attach a worker affinity manager so the reclaimer periodically cleans up expired leases.
    pub fn with_affinity(mut self, manager: Arc<WorkerAffinityManager>) -> Self {
        self.affinity_manager = Some(manager);
        self
    }

    /// Call `notify_one()` on the returned handle to stop the reclaimer.
    pub fn shutdown_handle(&self) -> Arc<Notify> {
        Arc::clone(&self.shutdown)
    }

    /// Runs until shutdown is signaled, checking for expired claims every 5 seconds.
    pub async fn start(self) -> OrcherResult<()> {
        info!("Starting reclaimer background task");

        // 5 second interval for faster crash recovery
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(e) = self.reclaim_expired_once().await {
                        error!(error = %e, "Failed to reclaim expired claims");
                        // Continue running even if one reclaim fails
                    }
                }
                _ = self.shutdown.notified() => {
                    info!("Reclaimer shutdown signal received");
                    break;
                }
            }
        }

        info!("Reclaimer background task stopped");
        Ok(())
    }

    pub async fn start_with_interval(self, interval_secs: u64) -> OrcherResult<()> {
        info!(
            interval_secs = interval_secs,
            "Starting reclaimer background task with custom interval"
        );

        let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(e) = self.reclaim_expired_once().await {
                        error!(error = %e, "Failed to reclaim expired claims");
                    }
                }
                _ = self.shutdown.notified() => {
                    info!("Reclaimer shutdown signal received");
                    break;
                }
            }
        }

        info!("Reclaimer background task stopped");
        Ok(())
    }

    async fn reclaim_expired_once(&self) -> OrcherResult<()> {
        debug!("Checking for expired claims");

        // Reclaim expired workflow executions
        let reclaimed_ids = self.execution_repo.reclaim_expired().await?;

        if !reclaimed_ids.is_empty() {
            info!(
                count = reclaimed_ids.len(),
                executions = ?reclaimed_ids,
                "Reclaimed executions with expired claims"
            );

            // Notify all queues that work may be available
            self.notifier.notify_all_queues().await;

            debug!("Notified all queues after reclaiming expired claims");
        } else {
            debug!("No expired workflow claims found");
        }

        // Reclaim expired eager task claims (short-TTL claims from eager dispatch)
        if let Some(ref task_repo) = self.task_execution_repo {
            match task_repo.reclaim_expired_eager_tasks().await {
                Ok(reclaimed) if !reclaimed.is_empty() => {
                    info!(
                        count = reclaimed.len(),
                        "Reclaimed expired eager task claims — notifying queues"
                    );
                    // Notify all affected queues so pollers wake up promptly
                    self.notifier.notify_all_queues().await;
                }
                Ok(_) => {}
                Err(e) => {
                    error!(error = %e, "Failed to reclaim expired eager task claims (non-fatal)");
                }
            }
        }

        if let Some(ref sweep) = self.wake_sweep {
            if let Err(e) = self.wake_parked(sweep).await {
                error!(error = %e, "Failed to wake parked workflows (non-fatal)");
            }
        }

        if let Some(store) = self.matching.as_ref().and_then(|m| m.get()) {
            match store.reclaim_expired(MATCHING_RECLAIM_BATCH).await {
                Ok(0) => {}
                Ok(count) => info!(
                    count,
                    "Returned expired matching leases to their partitions"
                ),
                Err(e) => {
                    error!(error = %e, "Failed to reclaim expired matching leases (non-fatal)")
                }
            }
        }

        // Cleanup expired worker affinity leases
        if let Some(ref affinity) = self.affinity_manager {
            let expired = affinity.cleanup_expired();
            if expired > 0 {
                debug!(
                    expired_leases = expired,
                    "Cleaned up expired affinity leases"
                );
            }
        }

        Ok(())
    }
}

impl Reclaimer {
    async fn wake_parked(&self, sweep: &WakeSweep) -> OrcherResult<()> {
        let now = Timestamptz::now();
        let since = {
            let read_up_to = sweep
                .read_up_to
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            match *read_up_to {
                Some(t) => t - std::time::Duration::from_secs(WAKE_OVERLAP_SECS as u64),
                None => now - std::time::Duration::from_secs(sweep.lookback_secs.max(0) as u64),
            }
        };
        let (woken, more) = self
            .execution_repo
            .wake_parked_with_unconsumed_outcomes(since, WAKE_GRACE_SECS, WAKE_BATCH)
            .await?;
        // Only once the whole window was read: a failed sweep, or one cut off
        // at the batch, is read again from where it started.
        if !more {
            *sweep
                .read_up_to
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) =
                Some(now - std::time::Duration::from_secs(WAKE_GRACE_SECS as u64));
        }
        for claim in &woken {
            (sweep.continuation)(&claim.task_queue, &claim.namespace, &claim.execution_id);
            self.notifier
                .notify_work_available(&claim.task_queue, &claim.namespace)
                .await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orcher_storage::schema::types::execution::WorkflowExecution;
    use sqlx::PgPool;
    use std::time::Duration as StdDuration;
    use tokio::time::timeout;
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
    async fn test_reclaimer_reclaims_expired() {
        let pool = create_test_pool().await;
        let repo = Arc::new(WorkflowExecutionRepository::new(pool.clone()));
        let notifier = Notifier::new();
        let reclaimer = Reclaimer::new(repo.clone(), notifier.clone());

        // Create an execution with an expired claim
        let mut execution = WorkflowExecution::builder()
            .execution_id(format!("test_exec_{}", Uuid::new_v4()))
            .workflow_name("test_workflow".to_string())
            .workflow_version("v1.0".to_string())
            .namespace("test".to_string())
            .parameters(serde_json::json!({}))
            .build();

        execution.task_queue = "test-queue".to_string();
        execution.status = "running".to_string();

        let created = repo.create(&execution).await.unwrap();

        // Manually set an expired claim
        sqlx::query(
            r#"
            UPDATE workflow_executions
            SET claimed_by = $1,
                claimed_at = NOW() - INTERVAL '2 hours',
                claim_timeout_at = NOW() - INTERVAL '1 hour'
            WHERE execution_id = $2
            "#,
        )
        .bind("crashed-service")
        .bind(&created.execution_id)
        .execute(&pool)
        .await
        .unwrap();

        // Run one reclaim cycle
        reclaimer.reclaim_expired_once().await.unwrap();

        // Verify claim was released
        let execution = repo
            .find_by_execution_id(&created.execution_id)
            .await
            .unwrap()
            .unwrap();

        assert!(execution.claimed_by.is_none());
        assert!(execution.claimed_at.is_none());
        assert!(execution.claim_timeout_at.is_none());
    }

    #[tokio::test]
    #[ignore] // Requires database
    async fn test_reclaimer_preserves_active_claims() {
        let pool = create_test_pool().await;
        let repo = Arc::new(WorkflowExecutionRepository::new(pool.clone()));
        let notifier = Notifier::new();
        let reclaimer = Reclaimer::new(repo.clone(), notifier.clone());

        // Create an execution with an active claim
        let mut execution = WorkflowExecution::builder()
            .execution_id(format!("test_exec_{}", Uuid::new_v4()))
            .workflow_name("test_workflow".to_string())
            .workflow_version("v1.0".to_string())
            .namespace("test".to_string())
            .parameters(serde_json::json!({}))
            .build();

        execution.task_queue = "test-queue".to_string();
        execution.status = "running".to_string();

        let created = repo.create(&execution).await.unwrap();

        // Set an active claim (timeout in the future)
        sqlx::query(
            r#"
            UPDATE workflow_executions
            SET claimed_by = $1,
                claimed_at = NOW(),
                claim_timeout_at = NOW() + INTERVAL '1 hour'
            WHERE execution_id = $2
            "#,
        )
        .bind("active-service")
        .bind(&created.execution_id)
        .execute(&pool)
        .await
        .unwrap();

        // Run one reclaim cycle
        reclaimer.reclaim_expired_once().await.unwrap();

        // Verify claim was NOT released
        let execution = repo
            .find_by_execution_id(&created.execution_id)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(execution.claimed_by, Some("active-service".to_string()));
        assert!(execution.claimed_at.is_some());
        assert!(execution.claim_timeout_at.is_some());
    }

    #[tokio::test]
    #[ignore] // Requires database
    async fn test_reclaimer_notifies_on_reclaim() {
        let pool = create_test_pool().await;
        let repo = Arc::new(WorkflowExecutionRepository::new(pool.clone()));
        let notifier = Notifier::new();
        let reclaimer = Reclaimer::new(repo.clone(), notifier.clone());

        // Set up a waiter
        let notify = notifier.get_or_create("test-queue", "test").await;
        let waiter = tokio::spawn({
            let notify = Arc::clone(&notify);
            async move {
                notify.notified().await;
                "notified"
            }
        });

        // Give waiter time to start waiting
        tokio::time::sleep(StdDuration::from_millis(10)).await;

        // Create an execution with an expired claim
        let mut execution = WorkflowExecution::builder()
            .execution_id(format!("test_exec_{}", Uuid::new_v4()))
            .workflow_name("test_workflow".to_string())
            .workflow_version("v1.0".to_string())
            .namespace("test".to_string())
            .parameters(serde_json::json!({}))
            .build();

        execution.task_queue = "test-queue".to_string();
        execution.status = "running".to_string();

        let created = repo.create(&execution).await.unwrap();

        sqlx::query(
            r#"
            UPDATE workflow_executions
            SET claimed_by = $1,
                claimed_at = NOW() - INTERVAL '2 hours',
                claim_timeout_at = NOW() - INTERVAL '1 hour'
            WHERE execution_id = $2
            "#,
        )
        .bind("crashed-service")
        .bind(&created.execution_id)
        .execute(&pool)
        .await
        .unwrap();

        // Run reclaim - should notify waiting services
        reclaimer.reclaim_expired_once().await.unwrap();

        // Waiter should be notified
        let result = timeout(StdDuration::from_secs(1), waiter)
            .await
            .expect("Timeout waiting for notification")
            .expect("Task failed");

        assert_eq!(result, "notified");
    }

    #[tokio::test]
    async fn test_reclaimer_shutdown() {
        let pool = PgPool::connect_lazy("postgres://test").expect("Failed to create lazy pool");
        let repo = Arc::new(WorkflowExecutionRepository::new(pool));
        let notifier = Notifier::new();
        let reclaimer = Reclaimer::new(repo, notifier);

        let shutdown = reclaimer.shutdown_handle();

        // Start reclaimer with a long interval
        let reclaimer_task = tokio::spawn(async move { reclaimer.start_with_interval(3600).await });

        // Give it time to start
        tokio::time::sleep(StdDuration::from_millis(10)).await;

        // Signal shutdown
        shutdown.notify_one();

        // Should stop quickly
        let result = timeout(StdDuration::from_secs(1), reclaimer_task)
            .await
            .expect("Reclaimer did not stop in time");

        assert!(result.is_ok());
    }

    #[tokio::test]
    #[ignore] // Requires database
    async fn test_reclaimer_multiple_expired() {
        let pool = create_test_pool().await;
        let repo = Arc::new(WorkflowExecutionRepository::new(pool.clone()));
        let notifier = Notifier::new();
        let reclaimer = Reclaimer::new(repo.clone(), notifier.clone());

        // Create 3 executions with expired claims
        let mut exec_ids = Vec::new();
        for _ in 0..3 {
            let mut execution = WorkflowExecution::builder()
                .execution_id(format!("test_exec_{}", Uuid::new_v4()))
                .workflow_name("test_workflow".to_string())
                .workflow_version("v1.0".to_string())
                .namespace("test".to_string())
                .parameters(serde_json::json!({}))
                .build();

            execution.task_queue = "test-queue".to_string();
            execution.status = "running".to_string();

            let created = repo.create(&execution).await.unwrap();
            exec_ids.push(created.execution_id.clone());

            sqlx::query(
                r#"
                UPDATE workflow_executions
                SET claimed_by = $1,
                    claimed_at = NOW() - INTERVAL '2 hours',
                    claim_timeout_at = NOW() - INTERVAL '1 hour'
                WHERE execution_id = $2
                "#,
            )
            .bind("crashed-service")
            .bind(&created.execution_id)
            .execute(&pool)
            .await
            .unwrap();
        }

        // Run reclaim
        reclaimer.reclaim_expired_once().await.unwrap();

        // Verify all were reclaimed
        for exec_id in exec_ids {
            let execution = repo.find_by_execution_id(&exec_id).await.unwrap().unwrap();
            assert!(execution.claimed_by.is_none());
        }
    }
}
