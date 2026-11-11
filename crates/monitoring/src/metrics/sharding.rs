//! Horizontal sharding metrics (ownership, routing, heartbeats).

use metrics::{counter, describe_counter, describe_gauge, gauge};

pub mod names {
    pub const OWNED_REQUESTS_TOTAL: &str = "orcher_sharding_owned_requests_total";
    pub const NON_OWNED_REQUESTS_TOTAL: &str = "orcher_sharding_non_owned_requests_total";
    pub const REDIRECTS_TOTAL: &str = "orcher_sharding_redirects_total";
    pub const OWNED_SHARDS: &str = "orcher_sharding_owned_shards";
    pub const TOTAL_SHARDS: &str = "orcher_sharding_total_shards";
    pub const HEARTBEAT_SUCCESS_TOTAL: &str = "orcher_sharding_heartbeat_success_total";
    pub const HEARTBEAT_FAILED_TOTAL: &str = "orcher_sharding_heartbeat_failed_total";
    pub const CLAIMS_SUCCESS_TOTAL: &str = "orcher_sharding_claims_success_total";
    pub const CLAIMS_FAILED_TOTAL: &str = "orcher_sharding_claims_failed_total";
    pub const SHARDS_LOST_TOTAL: &str = "orcher_sharding_shards_lost_total";
    pub const SHARDS_GAINED_TOTAL: &str = "orcher_sharding_shards_gained_total";
    pub const REGISTRATIONS_TOTAL: &str = "orcher_sharding_registrations_total";
    pub const SHARD_LOOKUPS_TOTAL: &str = "orcher_sharding_shard_lookups_total";
    pub const OWNERSHIP_RATE: &str = "orcher_sharding_ownership_rate";
    pub const FORWARDED_REQUESTS_TOTAL: &str = "orcher_forwarded_requests_total";
}

/// Register metric descriptions. Call once at startup.
pub fn describe_sharding_metrics() {
    // Counters
    describe_counter!(
        names::OWNED_REQUESTS_TOTAL,
        "Total requests handled for shards owned by this instance"
    );
    describe_counter!(
        names::NON_OWNED_REQUESTS_TOTAL,
        "Total requests received for shards not owned by this instance"
    );
    describe_counter!(
        names::REDIRECTS_TOTAL,
        "Total requests redirected to other orchestrator instances"
    );
    describe_counter!(
        names::HEARTBEAT_SUCCESS_TOTAL,
        "Total successful shard ownership heartbeats"
    );
    describe_counter!(
        names::HEARTBEAT_FAILED_TOTAL,
        "Total failed shard ownership heartbeats"
    );
    describe_counter!(
        names::CLAIMS_SUCCESS_TOTAL,
        "Total successful shard claims during registration"
    );
    describe_counter!(
        names::CLAIMS_FAILED_TOTAL,
        "Total failed shard claims (shard already owned by another instance)"
    );
    describe_counter!(
        names::SHARDS_LOST_TOTAL,
        "Total shards lost due to missed heartbeats or ownership transfer"
    );
    describe_counter!(
        names::SHARDS_GAINED_TOTAL,
        "Total shards gained through claims or rebalancing"
    );
    describe_counter!(
        names::REGISTRATIONS_TOTAL,
        "Total orchestrator registration attempts"
    );
    describe_counter!(
        names::SHARD_LOOKUPS_TOTAL,
        "Total shard lookup operations performed"
    );
    describe_counter!(
        names::FORWARDED_REQUESTS_TOTAL,
        "Requests relayed to the node owning the workflow's shard, by RPC and outcome"
    );

    // Gauges
    describe_gauge!(
        names::OWNED_SHARDS,
        "Number of shards currently owned by this instance"
    );
    describe_gauge!(names::TOTAL_SHARDS, "Total number of shards in the cluster");
    describe_gauge!(
        names::OWNERSHIP_RATE,
        "Ratio of requests for owned shards (0.0 to 1.0)"
    );
}

// ============================================================================
// Request Routing Metrics
// ============================================================================

pub fn record_owned_request(namespace: &str, task_queue: &str, shard: u32) {
    counter!(
        names::OWNED_REQUESTS_TOTAL,
        "namespace" => namespace.to_string(),
        "task_queue" => task_queue.to_string(),
        "shard" => shard.to_string()
    )
    .increment(1);
}

pub fn record_non_owned_request(namespace: &str, task_queue: &str, shard: u32) {
    counter!(
        names::NON_OWNED_REQUESTS_TOTAL,
        "namespace" => namespace.to_string(),
        "task_queue" => task_queue.to_string(),
        "shard" => shard.to_string()
    )
    .increment(1);
}

pub fn record_redirect(namespace: &str, task_queue: &str, shard: u32, target_instance: &str) {
    counter!(
        names::REDIRECTS_TOTAL,
        "namespace" => namespace.to_string(),
        "task_queue" => task_queue.to_string(),
        "shard" => shard.to_string(),
        "target_instance" => target_instance.to_string()
    )
    .increment(1);
}

/// A request relayed to the node owning its workflow's shard.
///
/// `outcome` is one of: `forwarded` (the owner answered successfully),
/// `owner_error` (the owner answered with an error status, relayed as is),
/// `unreachable` (the owner could not be reached, so the request was handled
/// here or answered as retryable), `failed` (the relay broke after sending).
pub fn record_forwarded_request(rpc: &'static str, outcome: &'static str) {
    counter!(
        names::FORWARDED_REQUESTS_TOTAL,
        "rpc" => rpc,
        "outcome" => outcome
    )
    .increment(1);
}

pub fn record_shard_lookup() {
    counter!(names::SHARD_LOOKUPS_TOTAL).increment(1);
}

// ============================================================================
// Coordinator Metrics
// ============================================================================

pub fn record_heartbeat_success(instance_id: &str) {
    counter!(
        names::HEARTBEAT_SUCCESS_TOTAL,
        "instance_id" => instance_id.to_string()
    )
    .increment(1);
}

pub fn record_heartbeat_failed(instance_id: &str) {
    counter!(
        names::HEARTBEAT_FAILED_TOTAL,
        "instance_id" => instance_id.to_string()
    )
    .increment(1);
}

pub fn record_claim_success(instance_id: &str, shard: u32) {
    counter!(
        names::CLAIMS_SUCCESS_TOTAL,
        "instance_id" => instance_id.to_string(),
        "shard" => shard.to_string()
    )
    .increment(1);
}

pub fn record_claim_failed(instance_id: &str, shard: u32) {
    counter!(
        names::CLAIMS_FAILED_TOTAL,
        "instance_id" => instance_id.to_string(),
        "shard" => shard.to_string()
    )
    .increment(1);
}

pub fn record_shard_lost(instance_id: &str, shard: u32) {
    counter!(
        names::SHARDS_LOST_TOTAL,
        "instance_id" => instance_id.to_string(),
        "shard" => shard.to_string()
    )
    .increment(1);
}

pub fn record_shard_gained(instance_id: &str, shard: u32) {
    counter!(
        names::SHARDS_GAINED_TOTAL,
        "instance_id" => instance_id.to_string(),
        "shard" => shard.to_string()
    )
    .increment(1);
}

pub fn record_registration(instance_id: &str) {
    counter!(
        names::REGISTRATIONS_TOTAL,
        "instance_id" => instance_id.to_string()
    )
    .increment(1);
}

// ============================================================================
// Gauge Updates
// ============================================================================

pub fn update_owned_shards(instance_id: &str, count: usize) {
    gauge!(
        names::OWNED_SHARDS,
        "instance_id" => instance_id.to_string()
    )
    .set(count as f64);
}

pub fn update_total_shards(count: u32) {
    gauge!(names::TOTAL_SHARDS).set(count as f64);
}

pub fn update_ownership_rate(instance_id: &str, rate: f64) {
    gauge!(
        names::OWNERSHIP_RATE,
        "instance_id" => instance_id.to_string()
    )
    .set(rate);
}

// ============================================================================
// Aggregate Stats
// ============================================================================

#[derive(Debug, Clone, Default)]
pub struct ShardingStats {
    pub owned_requests: u64,
    pub non_owned_requests: u64,
    pub redirects: u64,
    pub heartbeat_success: u64,
    pub heartbeat_failed: u64,
    pub claims_success: u64,
    pub claims_failed: u64,
    pub shards_lost: u64,
    pub shards_gained: u64,
    pub owned_shards: usize,
    pub total_shards: u32,
}

impl ShardingStats {
    /// 0.0 to 1.0
    pub fn ownership_rate(&self) -> f64 {
        let total = self.owned_requests + self.non_owned_requests;
        if total == 0 {
            1.0
        } else {
            self.owned_requests as f64 / total as f64
        }
    }

    pub fn shard_coverage(&self) -> f64 {
        if self.total_shards == 0 {
            0.0
        } else {
            self.owned_shards as f64 / self.total_shards as f64
        }
    }

    /// Healthy = has shards and zero failed heartbeats.
    pub fn is_healthy(&self) -> bool {
        self.owned_shards > 0 && self.heartbeat_failed == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sharding_stats_ownership_rate() {
        let mut stats = ShardingStats::default();

        // No requests = 100% ownership rate
        assert!((stats.ownership_rate() - 1.0).abs() < f64::EPSILON);

        // All owned
        stats.owned_requests = 100;
        stats.non_owned_requests = 0;
        assert!((stats.ownership_rate() - 1.0).abs() < f64::EPSILON);

        // Half and half
        stats.owned_requests = 50;
        stats.non_owned_requests = 50;
        assert!((stats.ownership_rate() - 0.5).abs() < f64::EPSILON);

        // Mostly non-owned
        stats.owned_requests = 25;
        stats.non_owned_requests = 75;
        assert!((stats.ownership_rate() - 0.25).abs() < f64::EPSILON);
    }

    #[test]
    fn test_sharding_stats_coverage() {
        let mut stats = ShardingStats::default();

        // No shards
        assert!((stats.shard_coverage() - 0.0).abs() < f64::EPSILON);

        // Full coverage
        stats.total_shards = 16;
        stats.owned_shards = 16;
        assert!((stats.shard_coverage() - 1.0).abs() < f64::EPSILON);

        // Partial coverage
        stats.owned_shards = 4;
        assert!((stats.shard_coverage() - 0.25).abs() < f64::EPSILON);
    }

    #[test]
    fn test_sharding_stats_health() {
        let mut stats = ShardingStats::default();

        // No shards = unhealthy
        assert!(!stats.is_healthy());

        // Has shards, no failures = healthy
        stats.owned_shards = 4;
        assert!(stats.is_healthy());

        // Has failures = unhealthy
        stats.heartbeat_failed = 1;
        assert!(!stats.is_healthy());
    }
}
