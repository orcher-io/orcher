//! # Outbox Queue Repository Implementation
//!
//! Repository implementation for outbox queue operations following the outbox pattern
//! for atomic task dispatch with guaranteed delivery.
//!
//! This implementation provides persistence operations for outbox tasks that are
//! created atomically with workflow state changes to ensure reliable task dispatch.

use jiff::SignedDuration;
use orcher_common::types::time::Timestamptz;
use orcher_common::OrcherResult;
use sqlx::{PgPool, Postgres, Transaction};
use std::sync::Arc;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::schema::types::coordination::{OutboxTask, OutboxTaskType, QueueMetrics};
use crate::sharding::{shard_predicate, ClaimBounds, ClaimParams, ClaimScope};

/// Outbox queue repository for task dispatch operations
#[derive(Clone)]
pub struct OutboxRepository {
    pool: Arc<PgPool>,
    /// Restricts claims to this node's shards. Absent ⇒ claim everything, which
    /// is every single-node deployment and the behaviour that predates this.
    claim_scope: Arc<std::sync::OnceLock<Arc<dyn ClaimScope>>>,
}

impl OutboxRepository {
    /// Create a new outbox repository
    pub fn new(pool: Arc<PgPool>) -> Self {
        Self {
            pool,
            claim_scope: Arc::new(std::sync::OnceLock::new()),
        }
    }

    /// Restrict this repository's claims to the shards `scope` reports.
    ///
    /// Held behind a shared `OnceLock` so setting it reaches clones already
    /// handed to running pollers, the same way the shard resolver is injected.
    pub fn set_claim_scope(&self, scope: Arc<dyn ClaimScope>) {
        let _ = self.claim_scope.set(scope);
    }

    /// Bind values for this poll's shard filter, or all-`None` to run unfiltered.
    fn claim_bounds(&self) -> ClaimParams {
        ClaimBounds::params(ClaimBounds::for_poll(self.claim_scope.get()).as_ref())
    }

    /// Create a new outbox task
    pub async fn create(&self, task: OutboxTask) -> OrcherResult<OutboxTask> {
        let result = sqlx::query_as::<_, OutboxTask>(concat!(
            r#"
            INSERT INTO outbox_queue (
                task_id, organization_id, workflow_id, task_type, payload, idempotency_key,
                status, created_at, attempt_count, max_retries, backoff_multiplier,
                shard_id
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (idempotency_key) DO UPDATE
                SET task_id = outbox_queue.task_id
            RETURNING "#,
            crate::repositories::columns::outbox_queue!(),
            r#"
            "#
        ))
        .bind(task.task_id)
        .bind(task.organization_id)
        .bind(task.workflow_id)
        .bind(task.task_type.to_string())
        .bind(&task.payload)
        .bind(&task.idempotency_key)
        .bind(task.status.to_string())
        .bind(task.created_at)
        .bind(task.attempt_count)
        .bind(task.max_retries)
        .bind(task.backoff_multiplier)
        .bind(task.shard_id)
        .fetch_one(self.pool.as_ref())
        .await
        .map_err(|e| {
            error!("Failed to create outbox task: {}", e);
            orcher_common::error::OrcherError::Internal {
                message: format!("Failed to create outbox task: {}", e),
                source: None,
            }
        })?;

        debug!(
            task_id = %result.task_id,
            workflow_id = %result.workflow_id,
            task_type = %result.task_type,
            "Created outbox task"
        );

        Ok(result)
    }

    /// Create a new outbox task within an existing transaction
    pub async fn create_in_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        task: OutboxTask,
    ) -> OrcherResult<Uuid> {
        let requested = task.task_id;
        // Return original ID if duplicate
        Ok(self
            .create_in_tx_if_new(tx, task)
            .await?
            .unwrap_or(requested))
    }

    /// Create an outbox task within an existing transaction, reporting whether
    /// it went in: `None` when a task with the same idempotency key is already
    /// there, so the caller can tell a new dispatch from one that was made
    /// before.
    pub async fn create_in_tx_if_new(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        task: OutboxTask,
    ) -> OrcherResult<Option<Uuid>> {
        let task_id = sqlx::query_scalar::<_, Uuid>(
            r#"
            INSERT INTO outbox_queue (
                task_id, organization_id, workflow_id, task_type, payload, idempotency_key,
                status, created_at, attempt_count, max_retries, backoff_multiplier,
                shard_id
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (idempotency_key) DO NOTHING
            RETURNING task_id
            "#,
        )
        .bind(task.task_id)
        .bind(task.organization_id)
        .bind(task.workflow_id)
        .bind(task.task_type.to_string())
        .bind(&task.payload)
        .bind(&task.idempotency_key)
        .bind(task.status.to_string())
        .bind(task.created_at)
        .bind(task.attempt_count)
        .bind(task.max_retries)
        .bind(task.backoff_multiplier)
        .bind(task.shard_id)
        .fetch_optional(&mut **tx)
        .await
        .map_err(|e| {
            error!("Failed to create outbox task in transaction: {}", e);
            orcher_common::error::OrcherError::Internal {
                message: format!("Failed to create outbox task: {}", e),
                source: None,
            }
        })?;

        debug!(
            task_id = %task.task_id,
            workflow_id = %task.workflow_id,
            task_type = %task.task_type,
            created = task_id.is_some(),
            "Created outbox task in transaction"
        );

        Ok(task_id)
    }

    /// Find an outbox task by ID
    pub async fn find_by_id(&self, task_id: Uuid) -> OrcherResult<Option<OutboxTask>> {
        let result = sqlx::query_as::<_, OutboxTask>(concat!(
            r#"
            SELECT "#,
            crate::repositories::columns::outbox_queue!(),
            r#" FROM outbox_queue
            WHERE task_id = $1
            "#
        ))
        .bind(task_id)
        .fetch_optional(self.pool.as_ref())
        .await
        .map_err(|e| {
            error!("Failed to find outbox task: {}", e);
            orcher_common::error::OrcherError::Internal {
                message: format!("Failed to find outbox task: {}", e),
                source: None,
            }
        })?;

        Ok(result)
    }

    /// Claim a batch of tasks for a specific workflow (FIFO ordering)
    ///
    /// # Arguments
    /// * `organization_id` - Optional organization ID for multi-tenant filtering
    /// * `workflow_id` - The workflow ID to claim tasks for
    /// * `batch_size` - Maximum number of tasks to claim
    pub async fn claim_tasks_for_workflow(
        &self,
        organization_id: Option<Uuid>,
        workflow_id: Uuid,
        batch_size: usize,
    ) -> OrcherResult<Vec<OutboxTask>> {
        let tasks = sqlx::query_as::<_, OutboxTask>(concat!(
            r#"
            UPDATE outbox_queue
            SET status = 'processing',
                processing_started_at = NOW(),
                attempt_count = attempt_count + 1
            WHERE task_id IN (
                SELECT task_id
                FROM outbox_queue
                WHERE ($1::uuid IS NULL OR organization_id = $1)
                  AND workflow_id = $2
                  AND status = 'pending'
                  AND (next_retry_at IS NULL OR next_retry_at <= NOW())
                ORDER BY created_at ASC
                LIMIT $3
                FOR UPDATE SKIP LOCKED
            )
            RETURNING "#,
            crate::repositories::columns::outbox_queue!(),
            r#"
            "#
        ))
        .bind(organization_id)
        .bind(workflow_id)
        .bind(batch_size as i64)
        .fetch_all(self.pool.as_ref())
        .await
        .map_err(|e| {
            error!("Failed to claim outbox tasks for workflow: {}", e);
            orcher_common::error::OrcherError::Internal {
                message: format!("Failed to claim outbox tasks: {}", e),
                source: None,
            }
        })?;

        if !tasks.is_empty() {
            debug!(
                organization_id = ?organization_id,
                workflow_id = %workflow_id,
                count = tasks.len(),
                "Claimed outbox tasks for workflow"
            );
        }

        Ok(tasks)
    }

    /// Claim the oldest ready outbox rows, leaving out the types in
    /// `exclude_types`.
    ///
    /// The outbox processor claims this way: by default every type but
    /// `dispatch_task`, which the task pollers claim per queue, and with the
    /// matching layer on every type, so that it moves each task dispatch into
    /// its matching partition.
    ///
    /// Rows are claimed oldest first, each handed to one claimer at a time: a
    /// row is claimed by moving it from `pending` to `processing` under its
    /// row lock, and a row another claimer holds is skipped, never waited on,
    /// so it stands in front of nothing. A row waiting out a backoff, or with
    /// no attempt left, is not ready and is passed over the same way.
    ///
    /// There is no ordering between one workflow's rows beyond their age, and
    /// none is needed. What reaches this claim is a `dispatch_step`, written
    /// with a new run to wake its first activation, and with the matching
    /// layer on a `dispatch_task`; the other types are never written. Handling a
    /// `dispatch_step` only tells the workflow pollers of its queue that there
    /// is work: the activation itself is claimed from the run's row, one claim
    /// at a time, and reads the whole journal, so two wakes of one workflow
    /// handled together, or in either order, wake the same activation. A
    /// `dispatch_task` is moved into its queue's partition, idempotently on
    /// its task id, and its workflow's tasks were scheduled without waiting on
    /// each other (see [`Self::claim_dispatch_tasks`]). The claim used to keep
    /// only the oldest ready row of each workflow, which ordered nothing
    /// either: rows already `processing` were not counted, so the next claim
    /// took the workflow's next row while the first was still being handled.
    /// What it did do was starve: it chose its workflows by id, not by age,
    /// so with more workflows waiting than it took, those whose ids sort last
    /// waited behind every other; a workflow with several ready rows had one
    /// of them taken per claim; and it chose before locking, so a row another
    /// claimer held still used up the claim.
    ///
    /// Each type is read apart, in created_at order, from the pending rows by
    /// type (idx_outbox_queue_task_polling), and the oldest of all of them
    /// taken: read as one set of types, the rows could only be read whole and
    /// sorted. Each part locks what it reads and skips what another claimer
    /// holds; a row one part locks and the merge leaves is free again when
    /// the statement ends. The task type column is constrained to the
    /// [`OutboxTaskType`] values, so the types read are all of them but the
    /// excluded ones.
    ///
    /// # Arguments
    /// * `organization_id` - Optional organization ID for multi-tenant filtering
    /// * `batch_size` - Maximum number of tasks to claim
    /// * `exclude_types` - Task types to EXCLUDE from claiming (e.g., ["dispatch_task"])
    pub async fn claim_tasks_excluding_types(
        &self,
        organization_id: Option<Uuid>,
        batch_size: usize,
        exclude_types: &[&str],
    ) -> OrcherResult<Vec<OutboxTask>> {
        tracing::debug!(
            organization_id = ?organization_id,
            batch_size = batch_size,
            exclude_types = ?exclude_types,
            "Claiming outbox tasks (excluding types)"
        );

        let include_types: Vec<String> = OutboxTaskType::ALL
            .iter()
            .map(|t| t.to_string())
            .filter(|t| !exclude_types.contains(&t.as_str()))
            .collect();
        if include_types.is_empty() || batch_size == 0 {
            return Ok(Vec::new());
        }

        let (los, his, min, max) = self.claim_bounds();
        // The shard filter is applied where rows are read, so a claim does not
        // lock another node's rows only to leave them; $3 is the types read,
        // so it starts at $4.
        let sql = format!(
            concat!(
                r#"
            WITH candidates AS (
                SELECT c.task_id, c.created_at
                FROM unnest($3::text[]) AS t(task_type)
                CROSS JOIN LATERAL (
                    SELECT oq.task_id, oq.created_at
                    FROM outbox_queue oq
                    WHERE oq.status = 'pending'
                      AND oq.task_type = t.task_type
                      AND ($1::uuid IS NULL OR oq.organization_id = $1)
                      AND (oq.next_retry_at IS NULL OR oq.next_retry_at <= NOW())
                      AND oq.attempt_count < oq.max_retries + 1
                      AND {shard}
                    ORDER BY oq.created_at ASC
                    LIMIT $2
                    FOR UPDATE OF oq SKIP LOCKED
                ) c
            ),
            selected_tasks AS (
                SELECT task_id
                FROM candidates
                ORDER BY created_at ASC
                LIMIT $2
            )
            UPDATE outbox_queue
            SET status = 'processing',
                processing_started_at = NOW(),
                attempt_count = attempt_count + 1
            WHERE task_id IN (SELECT task_id FROM selected_tasks)
            RETURNING "#,
                crate::repositories::columns::outbox_queue!(),
                r#"
            "#
            ),
            shard = shard_predicate("oq", 4),
        );
        let tasks = sqlx::query_as::<_, OutboxTask>(&sql)
            .bind(organization_id)
            .bind(batch_size as i64)
            .bind(&include_types)
            .bind(los)
            .bind(his)
            .bind(min)
            .bind(max)
            .fetch_all(self.pool.as_ref())
            .await
            .map_err(|e| {
                error!("Failed to claim outbox tasks (excluding types): {}", e);
                orcher_common::error::OrcherError::Internal {
                    message: format!("Failed to claim outbox tasks: {}", e),
                    source: None,
                }
            })?;

        if !tasks.is_empty() {
            debug!(
                organization_id = ?organization_id,
                count = tasks.len(),
                excluded = ?exclude_types,
                "Claimed outbox tasks (excluding types)"
            );
        }

        Ok(tasks)
    }

    /// Claim the oldest pending task dispatches of one `(namespace, task_queue)`.
    ///
    /// This is how a poll is handed its task when there is no matching layer,
    /// so the rows are chosen by the poll's own queue in the statement: every
    /// row claimed is one the poll can deliver.
    ///
    /// Each dispatch is handed out once at a time. A row is claimed by moving
    /// it from `pending` to `processing` under its row lock, and a row another
    /// poller holds is skipped, never waited on or taken a second time.
    /// Dispatches leave in the order they were made, oldest first within the
    /// queue, as the matching layer hands them out. There is no ordering
    /// between one workflow's dispatches beyond that: its tasks pending at once
    /// were scheduled without waiting on each other, two of them already run
    /// at the same time once the first is claimed, and the eager hand-out takes
    /// them in any order. The claim used to keep only the oldest pending
    /// dispatch of each workflow across all queues, which was that ordering
    /// and no more, and it let a dispatch on a queue nobody serves hold back
    /// its workflow's dispatches on every other queue.
    ///
    /// # Arguments
    /// * `organization_id` - Optional organization ID for multi-tenant filtering
    /// * `batch_size` - Maximum number of tasks to claim
    /// * `task_queue` - The task queue the poll serves
    /// * `namespace` - The namespace the poll serves
    pub async fn claim_dispatch_tasks(
        &self,
        organization_id: Option<Uuid>,
        batch_size: usize,
        task_queue: &str,
        namespace: &str,
    ) -> OrcherResult<Vec<OutboxTask>> {
        tracing::debug!(
            organization_id = ?organization_id,
            batch_size = batch_size,
            task_queue = %task_queue,
            namespace = %namespace,
            "Claiming task dispatches"
        );

        // A row's organization is its own stamp. A row written before rows
        // were stamped carries none, and is taken to be its workflow's, where
        // the workflow is on this database, so it still reaches its own
        // organization's poll rather than only a poll acting for none. The
        // stamped rows and the unstamped ones are read apart, each in
        // created_at order, and the oldest of both taken:
        // tested as one condition with OR, the two could only be read whole
        // and sorted, or the claim fell back on the queue's index and read
        // every organization's pending rows on the queue, which is what an
        // idle organization sharing a queue name with a busy one paid on each
        // of its polls. Each part locks what it reads and skips what another
        // poll holds, so a dispatch locked elsewhere never stands in front of
        // the next one; a row one part locks and the merge leaves is free
        // again when the statement ends.
        //
        // The organization is written into the statement rather than bound,
        // so each organization's claim is planned for that organization and
        // both its parts are answered in order from the pending dispatches by
        // organization and queue (V068). Planned once for every organization,
        // it could only walk every pending row in the outbox, which is what an
        // idle poll of an organization with no work did, five times a second:
        // 30 ms each on a million-row outbox against 0.1 ms planned for it. A
        // uuid's text is hex digits and hyphens, so nothing can be injected
        // through it. A poll acting for no organization has its own statement,
        // with no organization test at all, read from the queue's index
        // (idx_outbox_queue_task_queue_match).
        //
        // The task type is written in too, not bound, so that those indexes,
        // partial on it, can serve the claim.
        let claimable = "oq.status = 'pending' \
              AND oq.task_type = 'dispatch_task' \
              AND (oq.payload->>'task_queue') = $3 \
              AND (oq.payload->>'namespace') = $4 \
              AND (oq.next_retry_at IS NULL OR oq.next_retry_at <= NOW()) \
              AND oq.attempt_count < oq.max_retries + 1";
        let selected = match organization_id {
            Some(organization_id) => format!(
                r#"
            stamped AS (
                SELECT oq.task_id, oq.created_at
                FROM outbox_queue oq
                WHERE $1::uuid IS NOT NULL
                  AND oq.organization_id = '{organization_id}'::uuid
                  AND {claimable}
                ORDER BY oq.created_at ASC
                LIMIT $2
                FOR UPDATE OF oq SKIP LOCKED
            ),
            unstamped AS (
                SELECT oq.task_id, oq.created_at
                FROM outbox_queue oq
                WHERE oq.organization_id IS NULL
                  AND EXISTS (SELECT 1 FROM workflow_executions w
                              WHERE w.id = oq.workflow_id
                                AND w.organization_id = '{organization_id}'::uuid)
                  AND {claimable}
                ORDER BY oq.created_at ASC
                LIMIT $2
                FOR UPDATE OF oq SKIP LOCKED
            ),
            selected_tasks AS (
                SELECT task_id
                FROM (SELECT task_id, created_at FROM stamped
                      UNION ALL
                      SELECT task_id, created_at FROM unstamped) candidates
                ORDER BY created_at ASC
                LIMIT $2
            )"#
            ),
            None => format!(
                r#"
            selected_tasks AS (
                SELECT oq.task_id
                FROM outbox_queue oq
                WHERE $1::uuid IS NULL
                  AND {claimable}
                ORDER BY oq.created_at ASC
                LIMIT $2
                FOR UPDATE OF oq SKIP LOCKED
            )"#
            ),
        };
        let sql = format!(
            concat!(
                r#"
            WITH {selected}
            UPDATE outbox_queue
            SET status = 'processing',
                processing_started_at = NOW(),
                attempt_count = attempt_count + 1
            WHERE task_id IN (SELECT task_id FROM selected_tasks)
            RETURNING "#,
                crate::repositories::columns::outbox_queue!(),
                r#"
            "#
            ),
            selected = selected,
        );
        let tasks = sqlx::query_as::<_, OutboxTask>(&sql)
            .bind(organization_id)
            .bind(batch_size as i64)
            .bind(task_queue)
            .bind(namespace)
            .fetch_all(self.pool.as_ref())
            .await
            .map_err(|e| {
                error!("Failed to claim task dispatches: {}", e);
                orcher_common::error::OrcherError::Internal {
                    message: format!("Failed to claim outbox tasks: {}", e),
                    source: None,
                }
            })?;

        if !tasks.is_empty() {
            debug!(
                organization_id = ?organization_id,
                count = tasks.len(),
                task_queue = %task_queue,
                namespace = %namespace,
                "Claimed task dispatches"
            );
        }

        Ok(tasks)
    }

    /// Claim a single task for eager return during task completion.
    ///
    /// This is used to implement eager task return: when a worker completes a task,
    /// we can immediately return another pending task from the same task queue,
    /// eliminating a poll round-trip.
    ///
    /// # Arguments
    /// * `organization_id` - Optional organization ID for multi-tenant filtering
    /// * `task_queue` - The task queue to claim from
    /// * `namespace` - The namespace to claim from
    ///
    /// # Returns
    /// * `Ok(Some(task))` - A claimed task ready for execution
    /// * `Ok(None)` - No pending tasks available
    pub async fn claim_task_for_eager_return(
        &self,
        organization_id: Option<Uuid>,
        task_queue: &str,
        namespace: &str,
    ) -> OrcherResult<Option<OutboxTask>> {
        tracing::debug!(
            organization_id = ?organization_id,
            task_queue = %task_queue,
            namespace = %namespace,
            "Attempting to claim task for eager return"
        );

        // Query for a single pending dispatch_task that matches the task_queue and namespace
        // Uses SKIP LOCKED to avoid contention with other pollers
        let task = sqlx::query_as::<_, OutboxTask>(concat!(
            r#"
            WITH selected_task AS (
                SELECT task_id
                FROM outbox_queue
                WHERE ($1::uuid IS NULL OR organization_id = $1)
                  AND status = 'pending'
                  AND task_type = 'dispatch_task'
                  AND (next_retry_at IS NULL OR next_retry_at <= NOW())
                  AND attempt_count < max_retries + 1
                  AND payload->>'task_queue' = $2
                  AND payload->>'namespace' = $3
                ORDER BY created_at ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
            UPDATE outbox_queue
            SET status = 'processing',
                processing_started_at = NOW(),
                attempt_count = attempt_count + 1
            WHERE task_id IN (SELECT task_id FROM selected_task)
            RETURNING "#,
            crate::repositories::columns::outbox_queue!(),
            r#"
            "#
        ))
        .bind(organization_id)
        .bind(task_queue)
        .bind(namespace)
        .fetch_optional(self.pool.as_ref())
        .await
        .map_err(|e| {
            error!("Failed to claim task for eager return: {}", e);
            orcher_common::error::OrcherError::Internal {
                message: format!("Failed to claim task for eager return: {}", e),
                source: None,
            }
        })?;

        if let Some(ref t) = task {
            info!(
                task_id = %t.task_id,
                organization_id = ?organization_id,
                task_queue = %task_queue,
                namespace = %namespace,
                "Claimed task for eager return"
            );
        }

        Ok(task)
    }

    /// Mark a task as completed
    pub async fn complete(&self, task_id: Uuid) -> OrcherResult<()> {
        let rows_affected = sqlx::query(
            r#"
            UPDATE outbox_queue
            SET status = 'completed',
                completed_at = NOW()
            WHERE task_id = $1
            "#,
        )
        .bind(task_id)
        .execute(self.pool.as_ref())
        .await
        .map_err(|e| {
            error!("Failed to complete outbox task: {}", e);
            orcher_common::error::OrcherError::Internal {
                message: format!("Failed to complete outbox task: {}", e),
                source: None,
            }
        })?
        .rows_affected();

        if rows_affected > 0 {
            debug!(task_id = %task_id, "Completed outbox task");
        } else {
            warn!(task_id = %task_id, "Task not found for completion");
        }

        Ok(())
    }

    /// Complete a task dispatch this engine claimed and found it could not
    /// deliver, its task no longer waiting.
    ///
    /// Only a row still `processing`, as the claim left it: a row the eager
    /// reclaimer has opened again since, for a task it put back to waiting,
    /// is `pending` and is left to deliver it.
    pub async fn settle_claimed(&self, task_id: Uuid) -> OrcherResult<()> {
        sqlx::query(
            r#"
            UPDATE outbox_queue
            SET status = 'completed',
                completed_at = NOW(),
                lease_expires_at = NULL,
                lease_owner = NULL
            WHERE task_id = $1
              AND status = 'processing'
            "#,
        )
        .bind(task_id)
        .execute(self.pool.as_ref())
        .await
        .map_err(|e| {
            error!("Failed to settle claimed outbox task: {}", e);
            orcher_common::error::OrcherError::Internal {
                message: format!("Failed to settle claimed outbox task: {}", e),
                source: None,
            }
        })?;
        Ok(())
    }

    /// Mark a task as failed and schedule retry or move to failed status
    pub async fn fail(
        &self,
        task_id: Uuid,
        error_message: String,
        error_details: Option<serde_json::Value>,
    ) -> OrcherResult<()> {
        // First, get the task to check retry logic
        let task = self.find_by_id(task_id).await?;

        let task = match task {
            Some(t) => t,
            None => {
                warn!(task_id = %task_id, "Task not found for failure");
                return Ok(());
            }
        };

        // Check if we can retry
        if task.can_retry() {
            // Calculate next retry time with exponential backoff
            let backoff_seconds =
                (task.backoff_multiplier.powi(task.attempt_count)).min(3600.0) as i64; // Cap at 1 hour
            let next_retry_at = Timestamptz::now() + SignedDuration::from_secs(backoff_seconds);

            sqlx::query(
                r#"
                UPDATE outbox_queue
                SET status = 'pending',
                    processing_started_at = NULL,
                    next_retry_at = $2,
                    last_error = $3,
                    error_details = $4,
                    lease_expires_at = NULL,
                    lease_owner = NULL
                WHERE task_id = $1
                "#,
            )
            .bind(task_id)
            .bind(next_retry_at)
            .bind(&error_message)
            .bind(&error_details)
            .execute(self.pool.as_ref())
            .await
            .map_err(|e| {
                error!("Failed to schedule retry for outbox task: {}", e);
                orcher_common::error::OrcherError::Internal {
                    message: format!("Failed to schedule retry: {}", e),
                    source: None,
                }
            })?;

            debug!(
                task_id = %task_id,
                next_retry_at = %next_retry_at,
                attempt = task.attempt_count,
                "Scheduled outbox task for retry"
            );
        } else {
            // No more retries, mark as failed
            sqlx::query(
                r#"
                UPDATE outbox_queue
                SET status = 'failed',
                    completed_at = NOW(),
                    last_error = $2,
                    error_details = $3
                WHERE task_id = $1
                "#,
            )
            .bind(task_id)
            .bind(&error_message)
            .bind(&error_details)
            .execute(self.pool.as_ref())
            .await
            .map_err(|e| {
                error!("Failed to mark outbox task as failed: {}", e);
                orcher_common::error::OrcherError::Internal {
                    message: format!("Failed to mark task as failed: {}", e),
                    source: None,
                }
            })?;

            warn!(
                task_id = %task_id,
                error = %error_message,
                "Outbox task failed after all retries"
            );
        }

        Ok(())
    }

    /// Reset a specific task back to pending status
    ///
    /// Used when a worker claims a task but determines it's not the right match.
    ///
    /// # Arguments
    ///
    /// * `task_id` - The task ID to reset
    pub async fn reset_task_to_pending(&self, task_id: Uuid) -> OrcherResult<()> {
        sqlx::query(
            r#"
            UPDATE outbox_queue
            SET status = 'pending',
                processing_started_at = NULL,
                attempt_count = GREATEST(0, attempt_count - 1),
                lease_expires_at = NULL,
                lease_owner = NULL
            WHERE task_id = $1
              AND status = 'processing'
            "#,
        )
        .bind(task_id)
        .execute(self.pool.as_ref())
        .await
        .map_err(|e| {
            error!("Failed to reset task to pending: {}", e);
            orcher_common::error::OrcherError::Internal {
                message: format!("Failed to reset task to pending: {}", e),
                source: None,
            }
        })?;

        debug!(task_id = %task_id, "Reset task to pending status");

        Ok(())
    }

    /// Reset stuck processing tasks back to pending
    pub async fn reset_stuck_tasks(&self, stuck_threshold: SignedDuration) -> OrcherResult<i64> {
        let stuck_time = Timestamptz::now() - stuck_threshold;

        let rows_affected = sqlx::query(
            r#"
            UPDATE outbox_queue
            SET status = 'pending',
                processing_started_at = NULL,
                next_retry_at = NOW(),
                lease_expires_at = NULL,
                lease_owner = NULL
            WHERE status = 'processing'
              AND processing_started_at < $1
            "#,
        )
        .bind(stuck_time)
        .execute(self.pool.as_ref())
        .await
        .map_err(|e| {
            error!("Failed to reset stuck outbox tasks: {}", e);
            orcher_common::error::OrcherError::Internal {
                message: format!("Failed to reset stuck tasks: {}", e),
                source: None,
            }
        })?
        .rows_affected();

        if rows_affected > 0 {
            info!(count = rows_affected, "Reset stuck outbox tasks");
        }

        Ok(rows_affected as i64)
    }

    /// List pending tasks for a workflow
    pub async fn list_pending_for_workflow(
        &self,
        workflow_id: Uuid,
    ) -> OrcherResult<Vec<OutboxTask>> {
        let tasks = sqlx::query_as::<_, OutboxTask>(concat!(
            r#"
            SELECT "#,
            crate::repositories::columns::outbox_queue!(),
            r#" FROM outbox_queue
            WHERE workflow_id = $1
              AND status = 'pending'
            ORDER BY created_at ASC
            "#
        ))
        .bind(workflow_id)
        .fetch_all(self.pool.as_ref())
        .await
        .map_err(|e| {
            error!("Failed to list pending outbox tasks: {}", e);
            orcher_common::error::OrcherError::Internal {
                message: format!("Failed to list pending tasks: {}", e),
                source: None,
            }
        })?;

        Ok(tasks)
    }

    /// Get queue metrics for monitoring
    pub async fn get_metrics(&self) -> OrcherResult<QueueMetrics> {
        #[derive(sqlx::FromRow)]
        struct MetricsRow {
            pending_count: Option<i64>,
            processing_count: Option<i64>,
            completed_count: Option<i64>,
            failed_count: Option<i64>,
            avg_processing_time_ms: Option<i64>,
            oldest_pending_age_seconds: Option<i64>,
        }

        let row = sqlx::query_as::<_, MetricsRow>(
            r#"
            SELECT
                COUNT(CASE WHEN status = 'pending' THEN 1 END) as pending_count,
                COUNT(CASE WHEN status = 'processing' THEN 1 END) as processing_count,
                COUNT(CASE WHEN status = 'completed' AND completed_at > NOW() - INTERVAL '1 hour' THEN 1 END) as completed_count,
                COUNT(CASE WHEN status = 'failed' AND completed_at > NOW() - INTERVAL '1 hour' THEN 1 END) as failed_count,
                AVG(CASE WHEN status = 'completed' AND completed_at > NOW() - INTERVAL '1 hour'
                    THEN EXTRACT(EPOCH FROM (completed_at - processing_started_at)) * 1000 END)::BIGINT as avg_processing_time_ms,
                MAX(CASE WHEN status = 'pending' THEN EXTRACT(EPOCH FROM (NOW() - created_at)) END)::BIGINT as oldest_pending_age_seconds
            FROM outbox_queue
            "#
        )
        .fetch_one(self.pool.as_ref())
        .await
        .map_err(|e| {
            error!("Failed to get outbox queue metrics: {}", e);
            orcher_common::error::OrcherError::Internal {
                message: format!("Failed to get queue metrics: {}", e),
                source: None,
            }
        })?;

        Ok(QueueMetrics {
            pending_count: row.pending_count.unwrap_or(0),
            processing_count: row.processing_count.unwrap_or(0),
            completed_count: row.completed_count.unwrap_or(0),
            failed_count: row.failed_count.unwrap_or(0),
            avg_processing_time_ms: row.avg_processing_time_ms,
            oldest_pending_age_seconds: row.oldest_pending_age_seconds,
        })
    }

    /// Cleanup old completed tasks
    pub async fn cleanup_completed(&self, retention_days: i32) -> OrcherResult<i64> {
        let rows_affected = sqlx::query(
            r#"
            DELETE FROM outbox_queue
            WHERE status = 'completed'
              AND completed_at < NOW() - ($1 || ' days')::INTERVAL
            "#,
        )
        .bind(retention_days)
        .execute(self.pool.as_ref())
        .await
        .map_err(|e| {
            error!("Failed to cleanup completed outbox tasks: {}", e);
            orcher_common::error::OrcherError::Internal {
                message: format!("Failed to cleanup completed tasks: {}", e),
                source: None,
            }
        })?
        .rows_affected();

        if rows_affected > 0 {
            info!(
                count = rows_affected,
                retention_days = retention_days,
                "Cleaned up completed outbox tasks"
            );
        }

        Ok(rows_affected as i64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::types::coordination::OutboxTaskType;

    // Helper function to create test pool (would be implemented in test setup)
    // For now, these are just structure tests

    #[test]
    fn test_outbox_repository_creation() {
        // This is a compile-time test to ensure the repository can be created
        fn _assert_can_create(pool: Arc<PgPool>) {
            let _repo = OutboxRepository::new(pool);
        }
    }

    #[test]
    fn test_outbox_task_types() {
        // Ensure all task types are valid
        let task = OutboxTask::new(
            Uuid::new_v4(),
            OutboxTaskType::DispatchStep,
            serde_json::json!({"test": "data"}),
            Some("test-key".to_string()),
        );

        assert!(task.is_pending());
        assert!(!task.is_processing());
        assert!(task.can_retry());
    }
}
