//! # Cluster Membership & Shard Fencing Repository
//!
//! Persistence for the flat ownership model (tables `cluster_members` and
//! `cluster_shards`, migration V028). This is the storage substrate the
//! `orcher-cluster` crate builds its live `Membership`/`ShardMap` on:
//!
//! - **Membership** — nodes register and heartbeat; the *live* set (recent
//!   heartbeat) is the input every node partitions into contiguous shard ranges
//!   to derive the same ownership map. No leader, no parent
//!   cluster (contrast the legacy `ClusterRepository` in `repository.rs`).
//! - **Shard fencing** — each shard carries a monotonic `generation`. Acquiring
//!   ownership is a CAS that bumps the generation and is rejected if a *live*
//!   other node still owns the shard. The generation — not the `owner_node`
//!   field — is the authoritative write arbiter (a monotonic fencing token).
//!
//! **Nothing calls this yet.** It is inert substrate: the generation guard on the
//! engine write path, and the live membership loop, land in later increments and
//! only merge behind the multi-node E2E harness (Gate B). Adding these methods
//! changes zero runtime behavior.

use orcher_common::{error::OrcherError, OrcherResult};
use sqlx::PgPool;
use tracing::debug;

use crate::schema::types::{ClusterMember, ClusterShard};

/// PostgreSQL persistence for cluster membership + shard fencing.
#[derive(Debug, Clone)]
pub struct ClusterMembershipRepository {
    pool: PgPool,
    /// Where each shard's rows are written, when that is not only `pool`.
    data_pools: std::sync::Arc<
        std::sync::OnceLock<std::sync::Arc<dyn crate::sharding::pool_routing::ShardPoolResolver>>,
    >,
}

impl ClusterMembershipRepository {
    /// Create a repository over the given connection pool.
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            data_pools: std::sync::Arc::new(std::sync::OnceLock::new()),
        }
    }

    /// Tell the repository where shards' rows live, so a newly acquired
    /// generation is published there. Set once, before anything acquires a
    /// shard; a second call is ignored.
    pub fn set_data_pools(
        &self,
        resolver: std::sync::Arc<dyn crate::sharding::pool_routing::ShardPoolResolver>,
    ) {
        let _ = self.data_pools.set(resolver);
    }

    /// Access the underlying pool (used by the cluster crate to compose ops).
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    // ── Membership ──────────────────────────────────────────────────────────

    /// Register this node (or refresh it if already present): sets `address`,
    /// marks it `active`, and stamps a fresh heartbeat. Idempotent — a node calls
    /// this on join and again on every heartbeat tick.
    pub async fn upsert_member(&self, node_id: &str, address: &str) -> OrcherResult<ClusterMember> {
        debug!("Upserting cluster member: node_id={node_id}, address={address}");
        sqlx::query_as::<_, ClusterMember>(
            r#"
            INSERT INTO cluster_members (node_id, address, status, heartbeat_at, created_at)
            VALUES ($1, $2, 'active', now(), now())
            ON CONFLICT (node_id) DO UPDATE
                SET address = EXCLUDED.address,
                    status = 'active',
                    heartbeat_at = now()
            RETURNING node_id, address, status, heartbeat_at, created_at
            "#,
        )
        .bind(node_id)
        .bind(address)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| OrcherError::internal(format!("Failed to upsert cluster member: {e}")))
    }

    /// Refresh only the heartbeat (the steady-state tick). Returns `false` if the
    /// node has no membership row (it must `upsert_member` first).
    pub async fn heartbeat(&self, node_id: &str) -> OrcherResult<bool> {
        let result =
            sqlx::query("UPDATE cluster_members SET heartbeat_at = now() WHERE node_id = $1")
                .bind(node_id)
                .execute(&self.pool)
                .await
                .map_err(|e| OrcherError::internal(format!("Failed to heartbeat member: {e}")))?;
        Ok(result.rows_affected() > 0)
    }

    /// The live member set: `active` nodes whose heartbeat is within
    /// `stale_after_secs`. This — not the raw table — is what feeds ownership.
    pub async fn live_members(&self, stale_after_secs: i64) -> OrcherResult<Vec<ClusterMember>> {
        sqlx::query_as::<_, ClusterMember>(
            r#"
            SELECT node_id, address, status, heartbeat_at, created_at
            FROM cluster_members
            WHERE status = 'active'
              AND heartbeat_at >= now() - make_interval(secs => $1::double precision)
            ORDER BY node_id
            "#,
        )
        .bind(stale_after_secs)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| OrcherError::internal(format!("Failed to list live members: {e}")))
    }

    /// Members that have stopped heartbeating: last seen more than
    /// `stale_after_secs` ago, but within `lookback_secs`.
    ///
    /// The complement of [`live_members`](Self::live_members) over the recent
    /// past, whatever the status: a node that stopped heartbeating while
    /// draining is as gone as an active one. Rows are never deleted, so a
    /// node that died long ago would otherwise be listed for good; past the
    /// lookback anything it held has timed out on its own.
    pub async fn stale_members(
        &self,
        stale_after_secs: i64,
        lookback_secs: i64,
    ) -> OrcherResult<Vec<ClusterMember>> {
        sqlx::query_as::<_, ClusterMember>(
            r#"
            SELECT node_id, address, status, heartbeat_at, created_at
            FROM cluster_members
            WHERE heartbeat_at < now() - make_interval(secs => $1::double precision)
              AND heartbeat_at >= now() - make_interval(secs => $2::double precision)
            ORDER BY node_id
            "#,
        )
        .bind(stale_after_secs as f64)
        .bind(lookback_secs as f64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| OrcherError::internal(format!("Failed to list stale members: {e}")))
    }

    /// Set a member's status (e.g. `draining` on graceful shutdown). Returns
    /// `false` if the node is unknown.
    pub async fn set_member_status(&self, node_id: &str, status: &str) -> OrcherResult<bool> {
        let result = sqlx::query("UPDATE cluster_members SET status = $2 WHERE node_id = $1")
            .bind(node_id)
            .bind(status)
            .execute(&self.pool)
            .await
            .map_err(|e| OrcherError::internal(format!("Failed to set member status: {e}")))?;
        Ok(result.rows_affected() > 0)
    }

    /// Remove a member outright (deregister). Its shards are reclaimed by the next
    /// acquirer via the liveness check, not by this call.
    pub async fn remove_member(&self, node_id: &str) -> OrcherResult<bool> {
        let result = sqlx::query("DELETE FROM cluster_members WHERE node_id = $1")
            .bind(node_id)
            .execute(&self.pool)
            .await
            .map_err(|e| OrcherError::internal(format!("Failed to remove member: {e}")))?;
        Ok(result.rows_affected() > 0)
    }

    // ── Shard fencing ────────────────────────────────────────────────────────

    /// Acquire (or reclaim) ownership of `shard_id` for `node_id`, bumping the
    /// fencing generation. The CAS succeeds — and returns the **new** generation —
    /// iff the shard is unowned, already this node's, or owned by a node that is
    /// no longer live (heartbeat older than `stale_after_secs`). It returns `None`
    /// if a *live* other node still owns the shard (no takeover; no bump).
    ///
    /// The returned generation is the token the caller must present on every
    /// subsequent per-shard write; a superseded owner's writes fail the guard.
    pub async fn acquire_shard(
        &self,
        shard_id: i32,
        node_id: &str,
        stale_after_secs: i64,
    ) -> OrcherResult<Option<i64>> {
        debug!("Acquiring shard {shard_id} for node {node_id}");
        let generation = sqlx::query_scalar::<_, i64>(
            r#"
            INSERT INTO cluster_shards AS s (shard_id, owner_node, generation, updated_at)
            VALUES ($1, $2, 1, now())
            ON CONFLICT (shard_id) DO UPDATE
                SET owner_node = $2,
                    generation = s.generation + 1,
                    updated_at = now()
                WHERE s.owner_node IS NULL
                   OR s.owner_node = $2
                   OR NOT EXISTS (
                        SELECT 1 FROM cluster_members m
                        WHERE m.node_id = s.owner_node
                          AND m.status = 'active'
                          AND m.heartbeat_at >= now() - make_interval(secs => $3::double precision)
                      )
            RETURNING generation
            "#,
        )
        .bind(shard_id)
        .bind(node_id)
        .bind(stale_after_secs)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| OrcherError::internal(format!("Failed to acquire shard: {e}")))?;

        if let Some(generation) = generation {
            self.publish_generation(shard_id, node_id, generation)
                .await?;
        }
        Ok(generation)
    }

    /// Copy a newly acquired generation to every pool that holds the shard's
    /// rows, before the caller writes with it.
    ///
    /// The write guard reads the generation inside the writing transaction, on
    /// the pool it writes to. With shard databases that is not this pool, and
    /// the guard found no row there at all: every write to a workflow off the
    /// primary was refused as "ownership lost", so none of them could finish.
    /// Publishing here keeps the check local and atomic with the write. The
    /// update takes the row's write lock, so it waits for the previous owner's
    /// in-flight guarded writes (which hold it FOR SHARE) and fences out any
    /// that follow; the generation only ever moves forward.
    async fn publish_generation(
        &self,
        shard_id: i32,
        node_id: &str,
        generation: i64,
    ) -> OrcherResult<()> {
        let Some(resolver) = self.data_pools.get() else {
            return Ok(());
        };
        // Where a data pool is the coordination database itself, this finds
        // the row the acquire just wrote and leaves it as it is.
        for pool in resolver.pools_for_shard(shard_id) {
            sqlx::query(
                r#"
                INSERT INTO cluster_shards AS s (shard_id, owner_node, generation, updated_at)
                VALUES ($1, $2, $3, now())
                ON CONFLICT (shard_id) DO UPDATE
                    SET owner_node = $2,
                        generation = GREATEST(s.generation, $3),
                        updated_at = now()
                "#,
            )
            .bind(shard_id)
            .bind(node_id)
            .bind(generation)
            .execute(pool)
            .await
            .map_err(|e| {
                OrcherError::internal(format!(
                    "Failed to publish shard {shard_id} generation to its data pool: {e}"
                ))
            })?;
        }
        Ok(())
    }

    /// Renew ownership without bumping the generation (the steady-state keep-alive:
    /// the owner keeps the same token). Returns `false` if this node no longer
    /// holds `shard_id` at `held_generation` — i.e. it has been fenced out.
    pub async fn renew_shard(
        &self,
        shard_id: i32,
        node_id: &str,
        held_generation: i64,
    ) -> OrcherResult<bool> {
        let result = sqlx::query(
            r#"
            UPDATE cluster_shards SET updated_at = now()
            WHERE shard_id = $1 AND owner_node = $2 AND generation = $3
            "#,
        )
        .bind(shard_id)
        .bind(node_id)
        .bind(held_generation)
        .execute(&self.pool)
        .await
        .map_err(|e| OrcherError::internal(format!("Failed to renew shard: {e}")))?;
        Ok(result.rows_affected() > 0)
    }

    /// Relinquish a shard on graceful drain — but only if this node still holds it
    /// at `held_generation` (never yank it from whoever fenced us out). Leaves the
    /// generation intact so the next acquirer's bump stays monotonic.
    pub async fn release_shard(
        &self,
        shard_id: i32,
        node_id: &str,
        held_generation: i64,
    ) -> OrcherResult<bool> {
        let result = sqlx::query(
            r#"
            UPDATE cluster_shards SET owner_node = NULL, updated_at = now()
            WHERE shard_id = $1 AND owner_node = $2 AND generation = $3
            "#,
        )
        .bind(shard_id)
        .bind(node_id)
        .bind(held_generation)
        .execute(&self.pool)
        .await
        .map_err(|e| OrcherError::internal(format!("Failed to release shard: {e}")))?;
        Ok(result.rows_affected() > 0)
    }

    /// Read a shard's current fencing row (owner + generation), if it exists.
    pub async fn get_shard(&self, shard_id: i32) -> OrcherResult<Option<ClusterShard>> {
        sqlx::query_as::<_, ClusterShard>(
            r#"
            SELECT shard_id, owner_node, generation, updated_at
            FROM cluster_shards
            WHERE shard_id = $1
            "#,
        )
        .bind(shard_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| OrcherError::internal(format!("Failed to get shard: {e}")))
    }

    /// The shards currently attributed to `node_id` (advisory — ownership is only
    /// authoritative under the generation guard, but useful to know what to process).
    pub async fn shards_owned_by(&self, node_id: &str) -> OrcherResult<Vec<i32>> {
        sqlx::query_scalar::<_, i32>(
            "SELECT shard_id FROM cluster_shards WHERE owner_node = $1 ORDER BY shard_id",
        )
        .bind(node_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| OrcherError::internal(format!("Failed to list owned shards: {e}")))
    }
}

/// Open a transaction fenced by a shard's ownership generation — the write arbiter.
///
/// As its first statement it locks the shard row (`FOR SHARE`) and checks that the
/// live generation equals `held_gen` (the generation the caller acquired). On a
/// match it returns the open transaction for the caller to write in and commit; a
/// per-shard write therefore commits **iff** this node still holds the shard. On a
/// mismatch (a new owner bumped the generation) it rolls back and returns an error,
/// so a node that lost ownership cannot corrupt state even if it still believes it
/// owns the shard.
///
/// The `FOR SHARE` lock is the composition point: a concurrent takeover
/// (`acquire_shard` does `generation + 1`, which needs the row's write lock) blocks
/// until this transaction commits, so a guarded write and an ownership handover can
/// never interleave.
pub async fn begin_shard_guarded_tx(
    pool: &PgPool,
    shard_id: i32,
    held_gen: i64,
) -> OrcherResult<sqlx::Transaction<'_, sqlx::Postgres>> {
    let mut tx = pool.begin().await.map_err(|e| {
        OrcherError::internal(format!("Failed to begin shard-guarded transaction: {e}"))
    })?;

    let current: Option<i64> =
        sqlx::query_scalar("SELECT generation FROM cluster_shards WHERE shard_id = $1 FOR SHARE")
            .bind(shard_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| OrcherError::internal(format!("Failed to read shard generation: {e}")))?;

    match current {
        Some(gen) if gen == held_gen => Ok(tx), // still ours — proceed
        other => {
            // Superseded (or the row vanished): roll back and reject.
            let _ = tx.rollback().await;
            Err(OrcherError::internal(format!(
                "{}: shard {shard_id} held generation {held_gen}, current {other:?}",
                crate::sharding::OWNERSHIP_LOST
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Shard data held in a second database, as with ORCHER_SHARD_DATABASE_URLS.
    #[derive(Debug)]
    struct OneDataPool(PgPool);

    impl crate::sharding::pool_routing::ShardPoolResolver for OneDataPool {
        fn pool_for_workflow(&self, _: Option<uuid::Uuid>, _: uuid::Uuid) -> &PgPool {
            &self.0
        }
        fn all_pools(&self) -> Vec<&PgPool> {
            vec![&self.0]
        }
        fn shard_of(&self, _: uuid::Uuid) -> i32 {
            0
        }
        fn pools_for_shard(&self, _: i32) -> Vec<&PgPool> {
            vec![&self.0]
        }
    }

    /// The coordination database from TEST_DATABASE_URL, and a second
    /// database beside it, created and migrated on first use.
    async fn coordination_and_data_pools() -> (PgPool, PgPool) {
        use sqlx::postgres::PgConnectOptions;
        use std::str::FromStr;

        let url = std::env::var("TEST_DATABASE_URL").unwrap_or_else(|_| {
            "postgresql://postgres:postgres@localhost:5432/orcher_test".to_string()
        });
        let coordination = PgPool::connect(&url).await.expect("coordination database");
        let options = PgConnectOptions::from_str(&url).expect("TEST_DATABASE_URL");
        let data_name = format!(
            "{}_fence_data",
            options.get_database().unwrap_or("orcher_test")
        );
        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)")
                .bind(&data_name)
                .fetch_one(&coordination)
                .await
                .unwrap();
        if !exists {
            sqlx::query(&format!("CREATE DATABASE \"{data_name}\""))
                .execute(&coordination)
                .await
                .expect("create the data database");
        }
        let data = PgPool::connect_with(options.database(&data_name))
            .await
            .expect("data database");
        crate::setup::run_migrations(&data)
            .await
            .expect("migrate the data database");
        (coordination, data)
    }

    /// With shard databases the write guard reads the generation on the
    /// shard's own database, where nothing had ever written one: every write
    /// off the primary was refused as "ownership lost". An acquired generation
    /// must be published there, and a takeover must still fence the old owner
    /// out on that database.
    #[tokio::test]
    #[ignore] // Requires PostgreSQL
    async fn an_acquired_generation_fences_writes_on_the_shards_own_database() {
        let (coordination, data) = coordination_and_data_pools().await;
        let repo = ClusterMembershipRepository::new(coordination.clone());
        repo.set_data_pools(std::sync::Arc::new(OneDataPool(data.clone())));

        let shard = 9_000 + (uuid::Uuid::new_v4().as_u128() % 900) as i32;
        let first = format!("node-a-{shard}");
        let second = format!("node-b-{shard}");
        repo.upsert_member(&first, "a:1").await.unwrap();

        let held = repo
            .acquire_shard(shard, &first, 30)
            .await
            .unwrap()
            .expect("an unowned shard is acquired");
        let published: i64 =
            sqlx::query_scalar("SELECT generation FROM cluster_shards WHERE shard_id = $1")
                .bind(shard)
                .fetch_one(&data)
                .await
                .expect("the generation is on the shard's database");
        assert_eq!(published, held);
        begin_shard_guarded_tx(&data, shard, held)
            .await
            .expect("the owner writes to its shard")
            .rollback()
            .await
            .unwrap();

        // The first node stops heartbeating and the second takes the shard.
        sqlx::query("UPDATE cluster_members SET heartbeat_at = now() - interval '1 hour' WHERE node_id = $1")
            .bind(&first)
            .execute(&coordination)
            .await
            .unwrap();
        repo.upsert_member(&second, "b:1").await.unwrap();
        let taken = repo
            .acquire_shard(shard, &second, 30)
            .await
            .unwrap()
            .expect("a dead owner's shard is taken over");
        assert!(taken > held);

        assert!(
            begin_shard_guarded_tx(&data, shard, held).await.is_err(),
            "the superseded owner must be refused on the shard's database"
        );
        begin_shard_guarded_tx(&data, shard, taken)
            .await
            .expect("the new owner writes")
            .rollback()
            .await
            .unwrap();

        for pool in [&coordination, &data] {
            let _ = sqlx::query("DELETE FROM cluster_shards WHERE shard_id = $1")
                .bind(shard)
                .execute(pool)
                .await;
        }
        let _ = sqlx::query("DELETE FROM cluster_members WHERE node_id = ANY($1)")
            .bind(vec![first, second])
            .execute(&coordination)
            .await;
    }

    #[test]
    fn test_membership_repository_is_send_sync_clone() {
        fn _assert<T: Clone + Send + Sync + std::fmt::Debug>() {}
        _assert::<ClusterMembershipRepository>();
    }
}
