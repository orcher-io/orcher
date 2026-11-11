//! Matching domain — task delivery partitioned by `(namespace, task_queue)`.
//!
//! Persistence is sharded by `(namespace, workflow_id)`; delivery is a separate,
//! cluster-local concern. A worker polls by `(namespace, task_queue)`, so it must not
//! scan every persistence shard. Each shard's transfer emitter hands a ready task into
//! the matching partition for its `(namespace, task_queue)`; a worker poll consults
//! exactly one partition — O(1). The in-process sync-match fast path (wired in the
//! dispatch layer) serves the common worker-waiting case with zero reads here; this
//! store is the durable backstop (no waiter parked, crash recovery, fairness).
//!
//! [`MatchingStore`] is the seam: today a table on the coordination database
//! ([`PgMatchingStore`]); sharding the matching partitions to remove that funnel at
//! very high scale is additive behind this trait. The trait + Postgres repository live
//! here (persistence's domain, with sqlx) and the dispatch layer consumes the trait,
//! so the dispatch crate stays storage-agnostic. Design:
//! `orcher.io/matching-layer-design.md`.

mod repository;

use std::time::Duration;

use async_trait::async_trait;
use orcher_common::OrcherResult;
use uuid::Uuid;

pub use repository::PgMatchingStore;

/// A task ready for delivery, held in (or claimed from) a matching partition.
#[derive(Debug, Clone)]
pub struct MatchTask {
    /// The task's own id — the idempotency key for transfer and the ack handle.
    pub task_id: Uuid,
    /// Partition key part 1 — the namespace a worker polls.
    pub namespace: String,
    /// Partition key part 2 — the task queue a worker polls.
    pub task_queue: String,
    /// Correlation back to the durable record (the workflow on its shard).
    pub workflow_id: Uuid,
    /// Everything the worker needs to run the task.
    pub payload: serde_json::Value,
    /// The organization the task's workflow belongs to, if any. A partition is
    /// keyed by names, and a namespace name is unique only within an
    /// organization, so a claim is scoped by this as well.
    pub organization_id: Option<Uuid>,
}

/// The delivery-side store: tasks partitioned by `(namespace, task_queue)`. The
/// transfer emitter [`enqueue`](MatchingStore::enqueue)s; a worker poll
/// [`claim`](MatchingStore::claim)s. Behind a trait so the durable backing can move
/// from the coordination database to sharded partitions later without touching callers.
#[async_trait]
pub trait MatchingStore: Send + Sync + std::fmt::Debug {
    /// Enqueue a ready task into its `(namespace, task_queue)` partition. Idempotent on
    /// `task_id` — a re-delivered transfer is a no-op.
    async fn enqueue(&self, task: &MatchTask) -> OrcherResult<()>;

    /// Claim the oldest ready task in `(namespace, task_queue)`, leasing it to `owner`
    /// for `lease`. `None` if the partition holds no ready task. With
    /// `organization_id`, only that organization's tasks are claimed; without
    /// one, any are, as for every other claim (`tenant::may_act_on`).
    async fn claim(
        &self,
        namespace: &str,
        task_queue: &str,
        owner: &str,
        lease: Duration,
        organization_id: Option<Uuid>,
    ) -> OrcherResult<Option<MatchTask>>;

    /// Acknowledge a delivered task — remove it from the partition.
    async fn ack(&self, task_id: Uuid) -> OrcherResult<()>;

    /// Return claimed tasks whose lease has expired to `ready` (the reclaimer). Returns
    /// how many rows were reclaimed.
    async fn reclaim_expired(&self, limit: i64) -> OrcherResult<u64>;
}
