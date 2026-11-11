//! Postgres-backed [`ClusterStore`] — the real deployment binding. Wraps
//! `orcher-storage`'s `ClusterMembershipRepository`, mapping its rows/errors onto
//! the crate-local seam. Compiled only under the `storage` feature.

use orcher_storage::repositories::domains::cluster::ClusterMembershipRepository;

use super::{ClusterStore, StoreResult};
use crate::types::{Generation, NodeId, ShardId};

#[async_trait::async_trait]
impl ClusterStore for ClusterMembershipRepository {
    async fn register(&self, node: &NodeId, address: &str) -> StoreResult<()> {
        self.upsert_member(node.as_str(), address).await?;
        Ok(())
    }

    async fn heartbeat(&self, node: &NodeId) -> StoreResult<()> {
        self.heartbeat(node.as_str()).await?;
        Ok(())
    }

    async fn live_members(&self, stale_after_secs: i64) -> StoreResult<Vec<(NodeId, String)>> {
        let members = ClusterMembershipRepository::live_members(self, stale_after_secs).await?;
        Ok(members
            .into_iter()
            .map(|m| (NodeId::new(m.node_id), m.address))
            .collect())
    }

    async fn acquire_shard(
        &self,
        shard: ShardId,
        node: &NodeId,
        stale_after_secs: i64,
    ) -> StoreResult<Option<Generation>> {
        let gen = ClusterMembershipRepository::acquire_shard(
            self,
            shard.0 as i32,
            node.as_str(),
            stale_after_secs,
        )
        .await?;
        Ok(gen.map(|g| g as Generation))
    }

    async fn renew_shard(
        &self,
        shard: ShardId,
        node: &NodeId,
        held: Generation,
    ) -> StoreResult<bool> {
        Ok(ClusterMembershipRepository::renew_shard(
            self,
            shard.0 as i32,
            node.as_str(),
            held as i64,
        )
        .await?)
    }

    async fn release_shard(
        &self,
        shard: ShardId,
        node: &NodeId,
        held: Generation,
    ) -> StoreResult<bool> {
        Ok(ClusterMembershipRepository::release_shard(
            self,
            shard.0 as i32,
            node.as_str(),
            held as i64,
        )
        .await?)
    }
}
