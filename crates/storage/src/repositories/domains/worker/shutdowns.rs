//! The workers that have said they are shutting down.
//!
//! One row per worker process, or per identity for a worker that names no
//! process, kept until it expires (V053). Recording one also announces it on
//! [`WORKER_SHUTDOWN_CHANNEL`], so every replica learns of it as soon as it
//! commits rather than when it next reads the table.

use orcher_common::{error::OrcherError, OrcherResult};
use sqlx::PgPool;
use std::time::Duration;
use uuid::Uuid;

/// The channel a recorded shutdown is announced on. The payload is a
/// [`WorkerShutdown`] as JSON.
pub const WORKER_SHUTDOWN_CHANNEL: &str = "orcher_worker_shutdown";

/// The largest payload `pg_notify` accepts is 8000 bytes, and a larger one
/// fails the statement, which rolls back the row with it. A shutdown whose
/// announcement would not fit is refused before anything is written.
pub const MAX_NOTICE_BYTES: usize = 7900;

/// A worker that has said it is shutting down.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WorkerShutdown {
    /// The nil UUID when the worker named no organization.
    pub organization_id: Uuid,
    /// The namespace it polls.
    pub namespace: String,
    /// The identity it completes under.
    pub identity: String,
    /// The process shutting down, or empty when the worker named none.
    #[serde(default)]
    pub worker_instance_id: String,
    /// The task queues whose polls it covers; empty means all of them.
    #[serde(default)]
    pub task_queues: Vec<String>,
    /// How much longer the shutdown holds, measured by the database's clock
    /// when it was read or announced. Carried as a remainder rather than an
    /// instant so that replicas whose clocks disagree still agree on it.
    pub remaining_ms: i64,
    /// How much longer it covers polls that name no process. The same as
    /// `remaining_ms` for a shutdown that names none; shorter for one that
    /// names one, since such a poll may come from a later process.
    #[serde(default)]
    pub unnamed_remaining_ms: i64,
}

impl WorkerShutdown {
    /// The announcement of this shutdown, as sent on the channel.
    pub fn notice(&self) -> OrcherResult<String> {
        serde_json::to_string(self)
            .map_err(|e| OrcherError::internal(format!("Failed to encode the shutdown: {e}")))
    }
}

/// How long shutdowns hold, and how many there may be.
#[derive(Debug, Clone, Copy)]
pub struct ShutdownLimits {
    /// How long a shutdown that names its process holds.
    pub ttl: Duration,
    /// How long a shutdown covers polls that name no process: all of its
    /// period for one that names none, the start of it for one that does.
    pub anonymous_ttl: Duration,
    /// The most live shutdowns one organization may hold.
    pub max_per_organization: usize,
    /// The most processes of one identity that may be shut down at once.
    pub max_per_identity: usize,
    /// The most task queues one shutdown may cover, merged across repeats.
    pub max_task_queues: usize,
}

/// Stores and reads worker shutdowns.
#[derive(Debug, Clone)]
pub struct WorkerShutdownRepository {
    pool: PgPool,
}

impl WorkerShutdownRepository {
    /// A repository on `pool`.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Record that the worker is shutting down, and announce it. The remainders
    /// in `shutdown` are ignored; the returned shutdown carries the database's
    /// own, and the task queues it covers now.
    ///
    /// Recording it again restarts its period, and widens it to the queues
    /// named this time as well: a shutdown only ever grows, and one covering
    /// every queue stays so. So a worker may send it as often as it likes, and
    /// sending it with different queues does not add rows. Rows already expired
    /// are deleted in the same transaction, which is what keeps the table small
    /// without a sweep of its own.
    ///
    /// Refused with `ResourceExhausted` when the organization already holds
    /// `max_per_organization` other live shutdowns or the identity
    /// `max_per_identity`, so no tenant can grow the table, and every replica's
    /// copy of it, without bound; and with `InvalidInput` when the merged
    /// queues would exceed `max_task_queues` or the announcement would not
    /// fit. Nothing is written then. The announcement is sent on commit, so no
    /// replica hears of a shutdown the table does not hold.
    pub async fn record(
        &self,
        shutdown: &WorkerShutdown,
        limits: ShutdownLimits,
    ) -> OrcherResult<WorkerShutdown> {
        let failed = |e: sqlx::Error| {
            OrcherError::internal(format!("Failed to record the worker's shutdown: {e}"))
        };
        let exhausted = |message: String| OrcherError::ResourceExhausted {
            resource: "worker_shutdowns".to_string(),
            message,
        };
        let mut tx = self.pool.begin().await.map_err(failed)?;

        sqlx::query("DELETE FROM worker_shutdowns WHERE expires_at <= NOW()")
            .execute(&mut *tx)
            .await
            .map_err(failed)?;

        // Counted in the transaction that inserts, so the limits are enforced
        // where the rows are, whichever replica took the call. Two calls racing
        // may each see room for one more, which overshoots by a row or two and
        // is harmless; the limits are there to stop unbounded growth.
        let (in_organization, of_identity): (i64, i64) = sqlx::query_as(
            r#"
            SELECT COUNT(*),
                   COUNT(*) FILTER (WHERE namespace = $2 AND identity = $3)
            FROM worker_shutdowns
            WHERE organization_id = $1
              AND NOT (namespace = $2 AND identity = $3 AND worker_instance_id = $4)
            "#,
        )
        .bind(shutdown.organization_id)
        .bind(&shutdown.namespace)
        .bind(&shutdown.identity)
        .bind(&shutdown.worker_instance_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(failed)?;
        if in_organization >= limits.max_per_organization as i64 {
            return Err(exhausted(format!(
                "the organization already has {in_organization} workers shutting down; \
                 try again once some have expired"
            )));
        }
        if of_identity >= limits.max_per_identity as i64 {
            return Err(exhausted(format!(
                "{of_identity} processes of this identity are already shutting down; \
                 try again once some have expired"
            )));
        }

        #[allow(clippy::type_complexity)]
        let (task_queues, remaining_ms, unnamed_remaining_ms): (Vec<String>, i64, i64) =
            sqlx::query_as(
                r#"
            INSERT INTO worker_shutdowns
                (organization_id, namespace, identity, worker_instance_id, task_queues, expires_at)
            VALUES ($1, $2, $3, $4, $5, NOW() + make_interval(secs => $6))
            ON CONFLICT (organization_id, namespace, identity, worker_instance_id)
            DO UPDATE SET
                shutdown_at = NOW(),
                expires_at = EXCLUDED.expires_at,
                -- The union of what it covered and what it covers now; every
                -- queue (the empty set) absorbs any other.
                task_queues = CASE
                    WHEN cardinality(worker_shutdowns.task_queues) = 0
                      OR cardinality(EXCLUDED.task_queues) = 0 THEN '{}'::text[]
                    ELSE ARRAY(
                        SELECT DISTINCT q
                        FROM unnest(worker_shutdowns.task_queues || EXCLUDED.task_queues) AS q
                        ORDER BY q
                    )
                END
            RETURNING task_queues,
                      (EXTRACT(EPOCH FROM (expires_at - NOW())) * 1000)::BIGINT,
                      (EXTRACT(EPOCH FROM (
                          LEAST(expires_at, shutdown_at + make_interval(secs => $7)) - NOW()
                      )) * 1000)::BIGINT
            "#,
            )
            .bind(shutdown.organization_id)
            .bind(&shutdown.namespace)
            .bind(&shutdown.identity)
            .bind(&shutdown.worker_instance_id)
            .bind(&shutdown.task_queues)
            .bind(ttl_for(shutdown, limits).as_secs_f64())
            .bind(limits.anonymous_ttl.as_secs_f64())
            .fetch_one(&mut *tx)
            .await
            .map_err(failed)?;

        // Checked on what was merged, before anything commits: the queues of
        // one request are limited where it arrives, but repeats could grow the
        // set past what an announcement can carry.
        if task_queues.len() > limits.max_task_queues {
            return Err(OrcherError::invalid_input_field(
                "task_queues",
                format!(
                    "a worker's shutdown may cover at most {} task queues, across every \
                     time it is sent",
                    limits.max_task_queues
                ),
            ));
        }
        let shutdown = WorkerShutdown {
            task_queues,
            remaining_ms,
            unnamed_remaining_ms,
            ..shutdown.clone()
        };
        let payload = shutdown.notice()?;
        if payload.len() > MAX_NOTICE_BYTES {
            return Err(OrcherError::invalid_input_field(
                "task_queues",
                "the shutdown is too large to announce",
            ));
        }
        sqlx::query("SELECT pg_notify($1, $2)")
            .bind(WORKER_SHUTDOWN_CHANNEL)
            .bind(&payload)
            .execute(&mut *tx)
            .await
            .map_err(failed)?;

        tx.commit().await.map_err(failed)?;
        Ok(shutdown)
    }

    /// Every shutdown that has not expired. `anonymous_ttl` is the
    /// [`ShutdownLimits::anonymous_ttl`] they were recorded with.
    pub async fn active(&self, anonymous_ttl: Duration) -> OrcherResult<Vec<WorkerShutdown>> {
        #[allow(clippy::type_complexity)]
        let rows: Vec<(Uuid, String, String, String, Vec<String>, i64, i64)> = sqlx::query_as(
            r#"
            SELECT organization_id, namespace, identity, worker_instance_id, task_queues,
                   (EXTRACT(EPOCH FROM (expires_at - NOW())) * 1000)::BIGINT,
                   (EXTRACT(EPOCH FROM (
                       LEAST(expires_at, shutdown_at + make_interval(secs => $1)) - NOW()
                   )) * 1000)::BIGINT
            FROM worker_shutdowns
            WHERE expires_at > NOW()
            "#,
        )
        .bind(anonymous_ttl.as_secs_f64())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| OrcherError::internal(format!("Failed to read worker shutdowns: {e}")))?;
        Ok(rows
            .into_iter()
            .map(
                |(
                    organization_id,
                    namespace,
                    identity,
                    worker_instance_id,
                    task_queues,
                    remaining_ms,
                    unnamed_remaining_ms,
                )| WorkerShutdown {
                    organization_id,
                    namespace,
                    identity,
                    worker_instance_id,
                    task_queues,
                    remaining_ms,
                    unnamed_remaining_ms,
                },
            )
            .collect())
    }
}

/// How long `shutdown` holds: the full period when it names its process, and
/// only the anonymous one when it does not, since then it also refuses any
/// later process reusing the identity.
pub fn ttl_for(shutdown: &WorkerShutdown, limits: ShutdownLimits) -> Duration {
    if shutdown.worker_instance_id.is_empty() {
        limits.anonymous_ttl
    } else {
        limits.ttl
    }
}
