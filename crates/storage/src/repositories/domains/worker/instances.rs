//! When each worker process was last seen polling (V060).
//!
//! Every engine writes, every few seconds, the worker processes it has seen
//! polling since it last wrote — those with a poll open on it, and those whose
//! poll ended since. A process is named by the `worker_instance_id` its polls
//! carry, within the organization and namespace the polls were made for. Any
//! engine may then read the processes nobody has seen for a while and release
//! what they held. The release only takes claims still held, so reading the
//! same process again on the next sweep finds nothing more to do.

use orcher_common::types::time::Timestamptz;
use orcher_common::{error::OrcherError, OrcherResult};
use sqlx::PgPool;
use uuid::Uuid;

/// The most rows one write or one read takes.
const BATCH: usize = 1000;

/// A worker process an engine has seen polling.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SeenInstance {
    /// The organization its polls were made for; the nil UUID for none.
    pub organization_id: Uuid,
    /// The namespace they polled.
    pub namespace: String,
    /// The process, as its polls name it.
    pub worker_instance_id: String,
    /// The identity it last polled under, for the record only.
    pub identity: String,
}

/// A worker process nobody has seen polling for longer than the lapse.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LapsedInstance {
    /// The organization its polls were made for; the nil UUID for none.
    pub organization_id: Uuid,
    /// The namespace they polled.
    pub namespace: String,
    /// The process.
    pub worker_instance_id: String,
    /// When it was last seen, by the database's clock.
    pub last_seen: Timestamptz,
}

/// Records and reads when worker processes were last seen.
#[derive(Debug, Clone)]
pub struct WorkerInstanceRepository {
    pool: PgPool,
}

impl WorkerInstanceRepository {
    /// A repository on `pool`, which must be the primary database: every
    /// engine reads and writes the same rows, whichever shards it owns.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Record that `seen` were seen polling now.
    ///
    /// Written in key order, so two engines writing overlapping sets at once
    /// take their row locks in the same order and cannot deadlock.
    pub async fn record_seen(&self, seen: &[SeenInstance]) -> OrcherResult<()> {
        let mut seen = seen.to_vec();
        seen.sort();
        seen.dedup_by(|a, b| {
            a.organization_id == b.organization_id
                && a.namespace == b.namespace
                && a.worker_instance_id == b.worker_instance_id
        });
        for chunk in seen.chunks(BATCH) {
            let organizations: Vec<Uuid> = chunk.iter().map(|s| s.organization_id).collect();
            let namespaces: Vec<&str> = chunk.iter().map(|s| s.namespace.as_str()).collect();
            let instances: Vec<&str> = chunk
                .iter()
                .map(|s| s.worker_instance_id.as_str())
                .collect();
            let identities: Vec<&str> = chunk.iter().map(|s| s.identity.as_str()).collect();
            sqlx::query(
                r#"
                INSERT INTO worker_instances
                    (organization_id, namespace, worker_instance_id, identity, last_seen)
                SELECT o, n, i, id, NOW()
                FROM unnest($1::uuid[], $2::text[], $3::text[], $4::text[]) AS s(o, n, i, id)
                ON CONFLICT (organization_id, namespace, worker_instance_id) DO UPDATE
                SET last_seen = NOW(),
                    identity = EXCLUDED.identity
                "#,
            )
            .bind(&organizations)
            .bind(&namespaces)
            .bind(&instances)
            .bind(&identities)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                OrcherError::internal(format!("Failed to record the worker processes seen: {e}"))
            })?;
        }
        Ok(())
    }

    /// The processes last seen more than `lapse_secs` and at most
    /// `lookback_secs` ago, most recently seen first. Anything a process last
    /// seen longer ago than that held has timed out already.
    pub async fn lapsed(
        &self,
        lapse_secs: i64,
        lookback_secs: i64,
    ) -> OrcherResult<Vec<LapsedInstance>> {
        let rows: Vec<(Uuid, String, String, Timestamptz)> = sqlx::query_as(
            r#"
            SELECT organization_id, namespace, worker_instance_id, last_seen
            FROM worker_instances
            WHERE last_seen < NOW() - make_interval(secs => $1)
              AND last_seen >= NOW() - make_interval(secs => $2)
            ORDER BY last_seen DESC
            LIMIT $3
            "#,
        )
        .bind(lapse_secs as f64)
        .bind(lookback_secs as f64)
        .bind(BATCH as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            OrcherError::internal(format!("Failed to read the lapsed worker processes: {e}"))
        })?;
        Ok(rows
            .into_iter()
            .map(
                |(organization_id, namespace, worker_instance_id, last_seen)| LapsedInstance {
                    organization_id,
                    namespace,
                    worker_instance_id,
                    last_seen,
                },
            )
            .collect())
    }

    /// Forget the processes not seen for more than `secs`. By then every
    /// claim they made has timed out and been reclaimed anyway.
    pub async fn forget_older_than(&self, secs: i64) -> OrcherResult<u64> {
        let forgotten = sqlx::query(
            "DELETE FROM worker_instances WHERE last_seen < NOW() - make_interval(secs => $1)",
        )
        .bind(secs as f64)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            OrcherError::internal(format!("Failed to forget old worker processes: {e}"))
        })?;
        Ok(forgotten.rows_affected())
    }
}
