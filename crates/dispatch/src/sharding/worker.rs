//! # Workflow Sharding
//!
//! Consistent hashing for distributing workflows across shards to enable
//! horizontal worker scaling without contention.
//!
//! ## Overview
//!
//! When multiple workers poll the same Redis queue (BRPOP), they compete for work,
//! creating contention that limits horizontal scaling. Sharding distributes workflows
//! across multiple queues, allowing workers to poll dedicated shards without competition.
//!
//! ## Architecture
//!
//! ```text
//! Without Sharding (Contention):
//! ┌──────────────────────────────────────────────────────────────────┐
//! │                                                                  │
//! │   Worker 1 ─┐                                                    │
//! │   Worker 2 ─┼──► BRPOP "task-queue:namespace" ──► Only ONE wins  │
//! │   Worker 3 ─┤                                                    │
//! │   Worker 4 ─┤     (Workers fight for same queue)                 │
//! │   Worker 5 ─┘                                                    │
//! │                                                                  │
//! └──────────────────────────────────────────────────────────────────┘
//!
//! With Sharding (Parallel):
//! ┌──────────────────────────────────────────────────────────────────┐
//! │                                                                  │
//! │   Worker 1 ──► BRPOP "task-queue:namespace:shard:0"              │
//! │   Worker 2 ──► BRPOP "task-queue:namespace:shard:1"              │
//! │   Worker 3 ──► BRPOP "task-queue:namespace:shard:2"              │
//! │   Worker 4 ──► BRPOP "task-queue:namespace:shard:3"              │
//! │   Worker 5 ──► BRPOP "task-queue:namespace:shard:4"              │
//! │                                                                  │
//! │   (Each worker has dedicated queue - no contention!)             │
//! │                                                                  │
//! └──────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Consistent Hashing
//!
//! Workflows are assigned to shards using consistent hashing on the workflow_id:
//! - Same workflow_id always maps to same shard (deterministic)
//! - Even distribution across shards
//! - Shard count can be changed (with redistribution)
//!
//! ## Usage
//!
//! ```rust,ignore
//! use orcher_core::execution::sharding::{ShardConfig, ShardRouter};
//!
//! // Create a router with 8 shards
//! let router = ShardRouter::new(ShardConfig::new(8));
//!
//! // Get shard for a workflow
//! let shard = router.get_shard("my-workflow-id");
//! println!("Workflow routes to shard {}", shard); // e.g., "shard 3"
//!
//! // Get sharded queue name
//! let queue = router.get_sharded_queue_name("orders", "production", "workflow-123");
//! println!("Queue: {}", queue); // e.g., "orders:production:shard:5"
//! ```

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Configuration for workflow sharding
#[derive(Debug, Clone)]
pub struct ShardConfig {
    /// Total number of shards (should be power of 2 for even distribution)
    pub shard_count: u32,
    /// Whether sharding is enabled
    pub enabled: bool,
}

impl ShardConfig {
    /// Create a new shard configuration
    ///
    /// # Arguments
    /// * `shard_count` - Number of shards to distribute workflows across
    ///
    /// # Panics
    /// Panics if shard_count is 0
    pub fn new(shard_count: u32) -> Self {
        assert!(shard_count > 0, "Shard count must be greater than 0");
        Self {
            shard_count,
            enabled: true,
        }
    }

    /// Create a disabled shard configuration (all workflows go to single queue)
    pub fn disabled() -> Self {
        Self {
            shard_count: 1,
            enabled: false,
        }
    }

    /// Create a shard configuration with explicit enabled flag
    ///
    /// # Arguments
    /// * `shard_count` - Number of shards to distribute workflows across
    /// * `enabled` - Whether sharding is enabled
    pub fn new_with_enabled(shard_count: u32, enabled: bool) -> Self {
        Self {
            shard_count: shard_count.max(1),
            enabled: enabled && shard_count > 1,
        }
    }

    /// Create configuration from environment or defaults
    ///
    /// Reads from:
    /// - `ORCHER_SHARD_COUNT` - Number of shards (default: 1, disabled)
    /// - `ORCHER_SHARDING_ENABLED` - Whether to enable sharding (default: false)
    pub fn from_env() -> Self {
        let shard_count = std::env::var("ORCHER_SHARD_COUNT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1);

        let enabled = std::env::var("ORCHER_SHARDING_ENABLED")
            .ok()
            .map(|s| s.to_lowercase() == "true" || s == "1")
            .unwrap_or(false);

        Self {
            shard_count: shard_count.max(1),
            enabled: enabled && shard_count > 1,
        }
    }

    /// Check if sharding is effectively enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled && self.shard_count > 1
    }
}

impl Default for ShardConfig {
    fn default() -> Self {
        Self::disabled()
    }
}

/// Router for determining workflow shard assignments
#[derive(Debug, Clone)]
pub struct ShardRouter {
    config: ShardConfig,
}

impl ShardRouter {
    /// Create a new shard router
    pub fn new(config: ShardConfig) -> Self {
        Self { config }
    }

    /// Create a router from environment configuration
    pub fn from_env() -> Self {
        Self::new(ShardConfig::from_env())
    }

    /// Get the shard ID for a workflow
    ///
    /// Uses consistent hashing to deterministically map workflow_id to a shard.
    /// The same workflow_id will always map to the same shard.
    ///
    /// # Arguments
    /// * `workflow_id` - The workflow identifier to hash
    ///
    /// # Returns
    /// Shard ID (0 to shard_count - 1)
    pub fn get_shard(&self, workflow_id: &str) -> u32 {
        if !self.config.is_enabled() {
            return 0;
        }

        let hash = self.hash_workflow_id(workflow_id);
        (hash % self.config.shard_count as u64) as u32
    }

    /// Get the sharded queue name for a workflow
    ///
    /// # Arguments
    /// * `task_queue` - Base task queue name
    /// * `namespace` - Namespace for isolation
    /// * `workflow_id` - Workflow ID to determine shard
    ///
    /// # Returns
    /// Queue name with shard suffix if sharding enabled, otherwise base queue name
    pub fn get_sharded_queue_name(
        &self,
        task_queue: &str,
        namespace: &str,
        workflow_id: &str,
    ) -> String {
        if !self.config.is_enabled() {
            return format!("{}:{}", task_queue, namespace);
        }

        let shard = self.get_shard(workflow_id);
        format!("{}:{}:shard:{}", task_queue, namespace, shard)
    }

    /// Get queue name for a specific shard
    ///
    /// # Arguments
    /// * `task_queue` - Base task queue name
    /// * `namespace` - Namespace for isolation
    /// * `shard_id` - Specific shard ID
    ///
    /// # Returns
    /// Queue name for the specified shard
    pub fn get_queue_for_shard(&self, task_queue: &str, namespace: &str, shard_id: u32) -> String {
        if !self.config.is_enabled() {
            return format!("{}:{}", task_queue, namespace);
        }

        format!("{}:{}:shard:{}", task_queue, namespace, shard_id)
    }

    /// Get all queue names for assigned shards
    ///
    /// # Arguments
    /// * `task_queue` - Base task queue name
    /// * `namespace` - Namespace for isolation
    /// * `assigned_shards` - List of shard IDs this worker handles
    ///
    /// # Returns
    /// List of queue names to poll
    pub fn get_queues_for_shards(
        &self,
        task_queue: &str,
        namespace: &str,
        assigned_shards: &[u32],
    ) -> Vec<String> {
        if !self.config.is_enabled() {
            return vec![format!("{}:{}", task_queue, namespace)];
        }

        assigned_shards
            .iter()
            .map(|&shard| self.get_queue_for_shard(task_queue, namespace, shard))
            .collect()
    }

    /// Get the shard configuration
    pub fn config(&self) -> &ShardConfig {
        &self.config
    }

    /// Get total shard count
    pub fn shard_count(&self) -> u32 {
        self.config.shard_count
    }

    /// Check if sharding is enabled
    pub fn is_enabled(&self) -> bool {
        self.config.is_enabled()
    }

    /// Hash a workflow ID using consistent hashing
    fn hash_workflow_id(&self, workflow_id: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        workflow_id.hash(&mut hasher);
        hasher.finish()
    }
}

impl Default for ShardRouter {
    fn default() -> Self {
        Self::new(ShardConfig::default())
    }
}

/// Worker shard assignment
///
/// Represents which shards a worker is responsible for polling.
#[derive(Debug, Clone)]
pub struct WorkerShardAssignment {
    /// Worker identity
    pub worker_id: String,
    /// Assigned shard IDs
    pub shard_ids: Vec<u32>,
    /// Total number of shards in the system
    pub total_shards: u32,
}

impl WorkerShardAssignment {
    /// Create a new worker shard assignment
    pub fn new(worker_id: String, shard_ids: Vec<u32>, total_shards: u32) -> Self {
        Self {
            worker_id,
            shard_ids,
            total_shards,
        }
    }

    /// Create assignment from environment variables
    ///
    /// Reads from:
    /// - `ORCHER_WORKER_ID` - Worker identity (default: "worker-1")
    /// - `ORCHER_ASSIGNED_SHARDS` - Comma-separated shard IDs (default: all shards)
    /// - `ORCHER_SHARD_COUNT` - Total shard count
    pub fn from_env() -> Self {
        let worker_id =
            std::env::var("ORCHER_WORKER_ID").unwrap_or_else(|_| "worker-1".to_string());

        let total_shards = std::env::var("ORCHER_SHARD_COUNT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1);

        let shard_ids = std::env::var("ORCHER_ASSIGNED_SHARDS")
            .ok()
            .map(|s| {
                s.split(',')
                    .filter_map(|id| id.trim().parse().ok())
                    .collect()
            })
            .unwrap_or_else(|| {
                // Default: assign all shards to this worker
                (0..total_shards).collect()
            });

        Self {
            worker_id,
            shard_ids,
            total_shards,
        }
    }

    /// Create assignment where this worker handles all shards
    pub fn all_shards(worker_id: String, total_shards: u32) -> Self {
        let shard_ids = (0..total_shards).collect();
        Self::new(worker_id, shard_ids, total_shards)
    }

    /// Create assignment for a specific worker in a worker pool
    ///
    /// Automatically distributes shards across workers:
    /// - Worker 0 gets shards [0, 3, 6, ...]
    /// - Worker 1 gets shards [1, 4, 7, ...]
    /// - Worker 2 gets shards [2, 5, 8, ...]
    ///
    /// # Arguments
    /// * `worker_index` - Zero-based worker index
    /// * `worker_count` - Total number of workers
    /// * `total_shards` - Total number of shards
    pub fn for_worker_pool(worker_index: u32, worker_count: u32, total_shards: u32) -> Self {
        let shard_ids: Vec<u32> = (0..total_shards)
            .filter(|shard| shard % worker_count == worker_index)
            .collect();

        Self::new(format!("worker-{}", worker_index), shard_ids, total_shards)
    }

    /// Check if this worker handles a specific shard
    pub fn handles_shard(&self, shard_id: u32) -> bool {
        self.shard_ids.contains(&shard_id)
    }

    /// Get the number of assigned shards
    pub fn assigned_count(&self) -> usize {
        self.shard_ids.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shard_config_new() {
        let config = ShardConfig::new(8);
        assert_eq!(config.shard_count, 8);
        assert!(config.enabled);
        assert!(config.is_enabled());
    }

    #[test]
    fn test_shard_config_disabled() {
        let config = ShardConfig::disabled();
        assert_eq!(config.shard_count, 1);
        assert!(!config.enabled);
        assert!(!config.is_enabled());
    }

    #[test]
    fn test_shard_router_consistent_hashing() {
        let router = ShardRouter::new(ShardConfig::new(8));

        // Same workflow_id should always map to same shard
        let shard1 = router.get_shard("workflow-123");
        let shard2 = router.get_shard("workflow-123");
        assert_eq!(shard1, shard2);

        // Different workflow_ids may map to different shards
        let shard_a = router.get_shard("workflow-a");
        let shard_b = router.get_shard("workflow-b");
        // (They might be the same by chance, so we just verify they're valid)
        assert!(shard_a < 8);
        assert!(shard_b < 8);
    }

    #[test]
    fn test_shard_router_distribution() {
        let router = ShardRouter::new(ShardConfig::new(8));

        // Generate many workflow IDs and check distribution
        let mut shard_counts = vec![0u32; 8];
        for i in 0..1000 {
            let workflow_id = format!("workflow-{}", i);
            let shard = router.get_shard(&workflow_id);
            shard_counts[shard as usize] += 1;
        }

        // Each shard should have roughly 125 workflows (1000/8)
        // Allow 50% variance for statistical randomness
        for count in shard_counts {
            assert!(count > 50, "Shard has too few workflows: {}", count);
            assert!(count < 200, "Shard has too many workflows: {}", count);
        }
    }

    #[test]
    fn test_shard_router_disabled() {
        let router = ShardRouter::new(ShardConfig::disabled());

        // All workflows should map to shard 0 when disabled
        assert_eq!(router.get_shard("workflow-1"), 0);
        assert_eq!(router.get_shard("workflow-2"), 0);
        assert_eq!(router.get_shard("workflow-3"), 0);
    }

    #[test]
    fn test_sharded_queue_name() {
        let router = ShardRouter::new(ShardConfig::new(8));

        let queue = router.get_sharded_queue_name("orders", "production", "workflow-123");
        let shard = router.get_shard("workflow-123");

        assert_eq!(queue, format!("orders:production:shard:{}", shard));
    }

    #[test]
    fn test_sharded_queue_name_disabled() {
        let router = ShardRouter::new(ShardConfig::disabled());

        let queue = router.get_sharded_queue_name("orders", "production", "workflow-123");

        // When disabled, no shard suffix
        assert_eq!(queue, "orders:production");
    }

    #[test]
    fn test_queue_for_shard() {
        let router = ShardRouter::new(ShardConfig::new(8));

        let queue = router.get_queue_for_shard("orders", "production", 3);
        assert_eq!(queue, "orders:production:shard:3");
    }

    #[test]
    fn test_queues_for_shards() {
        let router = ShardRouter::new(ShardConfig::new(8));

        let queues = router.get_queues_for_shards("orders", "production", &[0, 2, 4]);
        assert_eq!(queues.len(), 3);
        assert_eq!(queues[0], "orders:production:shard:0");
        assert_eq!(queues[1], "orders:production:shard:2");
        assert_eq!(queues[2], "orders:production:shard:4");
    }

    #[test]
    fn test_worker_shard_assignment() {
        let assignment = WorkerShardAssignment::new("worker-1".to_string(), vec![0, 1, 2], 8);

        assert!(assignment.handles_shard(0));
        assert!(assignment.handles_shard(1));
        assert!(assignment.handles_shard(2));
        assert!(!assignment.handles_shard(3));
        assert_eq!(assignment.assigned_count(), 3);
    }

    #[test]
    fn test_worker_shard_assignment_all() {
        let assignment = WorkerShardAssignment::all_shards("worker-1".to_string(), 8);

        assert_eq!(assignment.assigned_count(), 8);
        for i in 0..8 {
            assert!(assignment.handles_shard(i));
        }
    }

    #[test]
    fn test_worker_pool_assignment() {
        // 3 workers, 8 shards
        let worker0 = WorkerShardAssignment::for_worker_pool(0, 3, 8);
        let worker1 = WorkerShardAssignment::for_worker_pool(1, 3, 8);
        let worker2 = WorkerShardAssignment::for_worker_pool(2, 3, 8);

        // Worker 0: [0, 3, 6]
        assert_eq!(worker0.shard_ids, vec![0, 3, 6]);

        // Worker 1: [1, 4, 7]
        assert_eq!(worker1.shard_ids, vec![1, 4, 7]);

        // Worker 2: [2, 5]
        assert_eq!(worker2.shard_ids, vec![2, 5]);

        // All shards are covered
        let all_shards: std::collections::HashSet<u32> = worker0
            .shard_ids
            .iter()
            .chain(worker1.shard_ids.iter())
            .chain(worker2.shard_ids.iter())
            .copied()
            .collect();

        assert_eq!(all_shards.len(), 8);
    }

    #[test]
    fn test_shard_boundary() {
        // Test with single shard
        let router = ShardRouter::new(ShardConfig::new(1));
        assert_eq!(router.get_shard("any-workflow"), 0);

        // Test with many shards
        let router = ShardRouter::new(ShardConfig::new(256));
        for i in 0..100 {
            let shard = router.get_shard(&format!("workflow-{}", i));
            assert!(shard < 256);
        }
    }
}
