//! Postgres repository backing the matching partitions (the `matching_partitions`
//! table on the coordination database).

use std::time::Duration;

use async_trait::async_trait;
use orcher_common::{error::OrcherError, OrcherResult};
use sqlx::PgPool;
use uuid::Uuid;

use super::{MatchTask, MatchingStore};

/// Postgres-backed matching store over the `matching_partitions` table on the
/// coordination database.
#[derive(Debug, Clone)]
pub struct PgMatchingStore {
    pool: PgPool,
    /// The cluster node this engine runs as, recorded on every claim it makes
    /// so that if it dies its survivors can find the ones it left undelivered.
    /// `None` without clustering.
    node: Option<String>,
}

impl PgMatchingStore {
    /// A store over `pool` (the coordination database pool).
    pub fn new(pool: PgPool) -> Self {
        Self { pool, node: None }
    }

    /// Record `node` as the engine making every claim through this store.
    pub fn with_node(mut self, node: impl Into<String>) -> Self {
        self.node = Some(node.into());
        self
    }

    /// The tasks engines that have died claimed in their last moments, as
    /// `(matching task id, task execution id)` — the candidates for
    /// [`release_to_ready`](Self::release_to_ready).
    ///
    /// A poll claims a task's matching row and then marks the task started.
    /// An engine that died between the two left the row claimed with nothing
    /// to return it, and the task scheduled for good. Only rows it claimed
    /// within `margin_secs` of its last heartbeat are read: the row of a task
    /// that was delivered stays claimed too, so over an engine's life the
    /// rows naming it are many, and the ones that can still be caught in that
    /// gap are the last few. Whether a candidate's task is still scheduled is
    /// for the caller to find out, on the database that holds it.
    pub async fn claims_of_dead_nodes(
        &self,
        dead: &[crate::repositories::domains::workflow_execution::DeadNode],
        margin_secs: i64,
    ) -> OrcherResult<Vec<(Uuid, Uuid)>> {
        if dead.is_empty() {
            return Ok(Vec::new());
        }
        let nodes: Vec<&str> = dead.iter().map(|d| d.node_id.as_str()).collect();
        let last_seen: Vec<orcher_common::types::time::Timestamptz> =
            dead.iter().map(|d| d.last_seen).collect();
        let rows = sqlx::query_as::<_, (Uuid, Option<String>)>(
            r#"
            SELECT m.task_id, m.payload->>'task_execution_id'
            FROM matching_partitions m
            JOIN unnest($1::text[], $2::timestamptz[]) AS dead(node_id, last_seen)
              ON m.claimed_on_node = dead.node_id
            WHERE m.status = 'claimed'
              AND m.claimed_at BETWEEN dead.last_seen - make_interval(secs => $3)
                                   AND dead.last_seen + make_interval(secs => $3)
            "#,
        )
        .bind(&nodes)
        .bind(&last_seen)
        .bind(margin_secs as f64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| OrcherError::internal(format!("matching dead-node read failed: {e}")))?;
        Ok(rows
            .into_iter()
            .filter_map(|(task_id, execution)| {
                Some((task_id, Uuid::parse_str(execution.as_deref()?).ok()?))
            })
            .collect())
    }

    /// Return to `ready` the rows among `task_ids` that are still claimed by
    /// one of `dead`, and say which were returned, with their partition.
    ///
    /// Each row is returned by one caller however many engines try at once:
    /// the others find it no longer claimed by a dead engine.
    pub async fn release_to_ready(
        &self,
        task_ids: &[Uuid],
        dead: &[crate::repositories::domains::workflow_execution::DeadNode],
    ) -> OrcherResult<Vec<MatchTask>> {
        if task_ids.is_empty() {
            return Ok(Vec::new());
        }
        let nodes: Vec<&str> = dead.iter().map(|d| d.node_id.as_str()).collect();
        #[allow(clippy::type_complexity)]
        let rows =
            sqlx::query_as::<_, (Uuid, String, String, Uuid, serde_json::Value, Option<Uuid>)>(
                r#"
            UPDATE matching_partitions
            SET status = 'ready',
                lease_owner = NULL,
                lease_expires_at = NULL,
                claimed_on_node = NULL,
                claimed_at = NULL
            WHERE task_id = ANY($1)
              AND status = 'claimed'
              AND claimed_on_node = ANY($2)
            RETURNING task_id, namespace, task_queue, workflow_id, payload, organization_id
            "#,
            )
            .bind(task_ids)
            .bind(&nodes)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| {
                OrcherError::internal(format!("matching dead-node release failed: {e}"))
            })?;
        Ok(rows
            .into_iter()
            .map(
                |(task_id, namespace, task_queue, workflow_id, payload, organization_id)| {
                    MatchTask {
                        task_id,
                        namespace,
                        task_queue,
                        workflow_id,
                        payload,
                        organization_id,
                    }
                },
            )
            .collect())
    }
}

#[async_trait]
impl MatchingStore for PgMatchingStore {
    async fn enqueue(&self, task: &MatchTask) -> OrcherResult<()> {
        sqlx::query(
            r#"
            INSERT INTO matching_partitions
                (task_id, namespace, task_queue, workflow_id, payload, status, organization_id)
            VALUES ($1, $2, $3, $4, $5, 'ready', $6)
            ON CONFLICT (task_id) DO NOTHING
            "#,
        )
        .bind(task.task_id)
        .bind(&task.namespace)
        .bind(&task.task_queue)
        .bind(task.workflow_id)
        .bind(&task.payload)
        .bind(task.organization_id)
        .execute(&self.pool)
        .await
        .map_err(|e| OrcherError::internal(format!("matching enqueue failed: {e}")))?;
        Ok(())
    }

    async fn claim(
        &self,
        namespace: &str,
        task_queue: &str,
        owner: &str,
        lease: Duration,
        organization_id: Option<Uuid>,
    ) -> OrcherResult<Option<MatchTask>> {
        // Claim the oldest ready task in the partition, atomically leasing it. The
        // subquery + FOR UPDATE SKIP LOCKED lets many pollers race without blocking.
        let lease_secs = lease.as_secs_f64();
        // The row's own stamp first, and its workflow's organization only for
        // a row transferred before rows were stamped, where the workflow is on
        // this database; as the outbox claim does, and for the same reason: a
        // join to every row's workflow slowed the claim several times over. A
        // poll acting for no organization has its own statement.
        let organization = match organization_id {
            Some(_) => {
                "(mp.organization_id = $6 \
                  OR (mp.organization_id IS NULL \
                      AND EXISTS (SELECT 1 FROM workflow_executions w \
                                  WHERE w.id = mp.workflow_id AND w.organization_id = $6)))"
            }
            None => "$6::uuid IS NULL",
        };
        let sql = format!(
            r#"
            UPDATE matching_partitions m
            SET status = 'claimed',
                lease_owner = $3,
                lease_expires_at = NOW() + make_interval(secs => $4),
                claimed_on_node = $5,
                claimed_at = NOW()
            WHERE m.task_id = (
                SELECT mp.task_id FROM matching_partitions mp
                WHERE mp.namespace = $1 AND mp.task_queue = $2 AND mp.status = 'ready'
                  AND {organization}
                ORDER BY mp.created_at
                FOR UPDATE OF mp SKIP LOCKED
                LIMIT 1
            )
            RETURNING m.task_id, m.namespace, m.task_queue, m.workflow_id, m.payload,
                      m.organization_id
            "#
        );
        #[allow(clippy::type_complexity)]
        let row =
            sqlx::query_as::<_, (Uuid, String, String, Uuid, serde_json::Value, Option<Uuid>)>(
                &sql,
            )
            // Planned on every call, never from a cached plan. This table is a
            // queue: a few live rows among the dead ones every claim and ack
            // leave until vacuum. A plan cached while it looked empty (just
            // after a vacuum) scans every page, dead ones included, on every
            // later claim; on a stock Postgres two of three claims ran that way,
            // reading up to 141 pages for one row. A fresh plan sees the
            // table's size and takes the index.
            .persistent(false)
            .bind(namespace)
            .bind(task_queue)
            .bind(owner)
            .bind(lease_secs)
            .bind(self.node.as_deref())
            .bind(organization_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| OrcherError::internal(format!("matching claim failed: {e}")))?;

        Ok(row.map(
            |(task_id, namespace, task_queue, workflow_id, payload, organization_id)| MatchTask {
                task_id,
                namespace,
                task_queue,
                workflow_id,
                payload,
                organization_id,
            },
        ))
    }

    async fn ack(&self, task_id: Uuid) -> OrcherResult<()> {
        sqlx::query("DELETE FROM matching_partitions WHERE task_id = $1")
            // Planned on every call, as the claim is, for the same reason.
            .persistent(false)
            .bind(task_id)
            .execute(&self.pool)
            .await
            .map_err(|e| OrcherError::internal(format!("matching ack failed: {e}")))?;
        Ok(())
    }

    async fn reclaim_expired(&self, limit: i64) -> OrcherResult<u64> {
        let res = sqlx::query(
            r#"
            UPDATE matching_partitions
            SET status = 'ready', lease_owner = NULL, lease_expires_at = NULL
            WHERE task_id IN (
                SELECT task_id FROM matching_partitions
                WHERE status = 'claimed' AND lease_expires_at < NOW()
                ORDER BY lease_expires_at
                LIMIT $1
            )
            "#,
        )
        .bind(limit)
        .execute(&self.pool)
        .await
        .map_err(|e| OrcherError::internal(format!("matching reclaim failed: {e}")))?;
        Ok(res.rows_affected())
    }
}
