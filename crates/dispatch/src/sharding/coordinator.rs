//! # Shard Coordinator for Redis-Based Ownership
//!
//! This module provides distributed coordination for shard ownership across
//! multiple orchestrator instances using Redis.
//!
//! ## Overview
//!
//! When running multiple orchestrator instances, each needs to claim ownership
//! of specific shards to avoid duplicate processing. The ShardCoordinator uses
//! Redis to:
//!
//! - Register instances and claim available shards
//! - Maintain ownership with periodic heartbeats
//! - Detect failed instances and reassign their shards
//! - Provide graceful handoff during scaling events
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────┐
//! │                           Redis                                         │
//! │                                                                         │
//! │  ┌─────────────────────────────────────────────────────────────────┐   │
//! │  │  Shard Ownership Keys (with TTL)                                │   │
//! │  │                                                                 │   │
//! │  │  orcher:shards:0  → "orchestrator-1" (TTL: 30s)                │   │
//! │  │  orcher:shards:1  → "orchestrator-1" (TTL: 30s)                │   │
//! │  │  orcher:shards:2  → "orchestrator-2" (TTL: 30s)                │   │
//! │  │  orcher:shards:3  → "orchestrator-2" (TTL: 30s)                │   │
//! │  │  ...                                                           │   │
//! │  └─────────────────────────────────────────────────────────────────┘   │
//! │                                                                         │
//! │  ┌─────────────────────────────────────────────────────────────────┐   │
//! │  │  Instance Registry                                              │   │
//! │  │                                                                 │   │
//! │  │  orcher:instances → {"orchestrator-1": {...}, ...}             │   │
//! │  └─────────────────────────────────────────────────────────────────┘   │
//! └─────────────────────────────────────────────────────────────────────────┘
//!
//!        │                        │                        │
//!        ▼                        ▼                        ▼
//! ┌──────────────┐        ┌──────────────┐        ┌──────────────┐
//! │ Orchestrator │        │ Orchestrator │        │ Orchestrator │
//! │     #1       │        │     #2       │        │     #3       │
//! │              │        │              │        │              │
//! │ Shards: 0,1  │        │ Shards: 2,3  │        │ Shards: 4,5  │
//! └──────────────┘        └──────────────┘        └──────────────┘
//! ```
//!
//! ## Usage
//!
//! ```rust,ignore
//! use orcher_core::execution::shard_coordinator::ShardCoordinator;
//!
//! // Create coordinator
//! let coordinator = ShardCoordinator::new(redis_client, config).await?;
//!
//! // Register and claim shards
//! let owned_shards = coordinator.register().await?;
//! println!("Claimed shards: {:?}", owned_shards);
//!
//! // Start background heartbeat
//! let handle = coordinator.start_heartbeat();
//!
//! // On shutdown
//! coordinator.release_shards().await?;
//! ```
//!
//! ## Failure Handling
//!
//! - **Instance crash**: TTL expires, shards become available for claim
//! - **Network partition**: Instance loses shards, re-claims on reconnect
//! - **Redis unavailable**: Coordinator operates in degraded mode (no new claims)

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use tokio::sync::Notify;
use tracing::{debug, error, info, warn};

use super::horizontal::HorizontalShardingConfig;

// ============================================================================
// ShardCoordinator
// ============================================================================

/// Coordinates shard ownership across orchestrator instances using Redis
///
/// Provides distributed locking and coordination for horizontal sharding,
/// ensuring each shard is owned by exactly one orchestrator at a time.
pub struct ShardCoordinator {
    /// Redis connection manager
    redis_client: Arc<dyn ShardCoordinatorRedis>,

    /// Configuration
    config: HorizontalShardingConfig,

    /// Currently owned shards (updated by heartbeat)
    owned_shards: Arc<RwLock<HashSet<u32>>>,

    /// Whether the coordinator is running
    running: Arc<AtomicBool>,

    /// Shutdown signal
    shutdown: Arc<Notify>,

    /// Metrics
    metrics: ShardCoordinatorMetrics,

    /// Last successful heartbeat time
    last_heartbeat: Arc<RwLock<Option<Instant>>>,
}

/// Metrics for shard coordination
#[derive(Debug, Default)]
pub struct ShardCoordinatorMetrics {
    /// Successful shard claims
    pub claims_success: AtomicU64,
    /// Failed shard claims (already owned by another)
    pub claims_failed: AtomicU64,
    /// Successful heartbeats
    pub heartbeats_success: AtomicU64,
    /// Failed heartbeats
    pub heartbeats_failed: AtomicU64,
    /// Shards lost due to missed heartbeat
    pub shards_lost: AtomicU64,
    /// Shards gained
    pub shards_gained: AtomicU64,
    /// Registration attempts
    pub registrations: AtomicU64,
}

impl ShardCoordinatorMetrics {
    /// Create a new metrics instance
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a successful shard claim
    pub fn record_claim_success(&self) {
        self.claims_success.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a failed shard claim (already owned)
    pub fn record_claim_failed(&self) {
        self.claims_failed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a successful heartbeat
    pub fn record_heartbeat_success(&self) {
        self.heartbeats_success.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a failed heartbeat
    pub fn record_heartbeat_failed(&self) {
        self.heartbeats_failed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a shard lost due to missed heartbeat
    pub fn record_shard_lost(&self) {
        self.shards_lost.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a shard gained
    pub fn record_shard_gained(&self) {
        self.shards_gained.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a registration attempt
    pub fn record_registration(&self) {
        self.registrations.fetch_add(1, Ordering::Relaxed);
    }
}

/// Trait for Redis operations needed by ShardCoordinator
///
/// This trait allows for mocking in tests and different Redis implementations.
#[async_trait::async_trait]
pub trait ShardCoordinatorRedis: Send + Sync {
    /// Try to claim a shard using SETNX with TTL
    ///
    /// Returns true if the claim was successful (key didn't exist).
    async fn try_claim_shard(
        &self,
        key: &str,
        instance_id: &str,
        ttl_secs: u64,
    ) -> Result<bool, ShardCoordinatorError>;

    /// Refresh a shard's TTL if we still own it
    ///
    /// Uses SET with XX (only set if exists) and compares the value.
    /// Returns true if the refresh was successful.
    async fn refresh_shard(
        &self,
        key: &str,
        instance_id: &str,
        ttl_secs: u64,
    ) -> Result<bool, ShardCoordinatorError>;

    /// Release a shard (delete key if we own it)
    async fn release_shard(
        &self,
        key: &str,
        instance_id: &str,
    ) -> Result<bool, ShardCoordinatorError>;

    /// Get current owner of a shard
    async fn get_shard_owner(&self, key: &str) -> Result<Option<String>, ShardCoordinatorError>;

    /// Check connection health
    async fn ping(&self) -> Result<(), ShardCoordinatorError>;
}

/// Errors from shard coordination
#[derive(Debug, thiserror::Error)]
pub enum ShardCoordinatorError {
    /// Redis operation failed
    #[error("Redis error: {0}")]
    Redis(String),

    /// Connection to Redis failed
    #[error("Connection error: {0}")]
    Connection(String),

    /// Coordinator is not running
    #[error("Coordinator not running")]
    NotRunning,

    /// Configuration is invalid
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    /// Timeout waiting for shard operations
    #[error("Timeout waiting for shards")]
    Timeout,
}

impl ShardCoordinator {
    /// Create a new shard coordinator
    pub fn new(
        redis_client: Arc<dyn ShardCoordinatorRedis>,
        config: HorizontalShardingConfig,
    ) -> Result<Self, ShardCoordinatorError> {
        // Validate configuration
        config
            .validate()
            .map_err(ShardCoordinatorError::InvalidConfig)?;

        info!(
            instance_id = %config.instance_id,
            total_shards = config.total_shards,
            "Creating ShardCoordinator"
        );

        Ok(Self {
            redis_client,
            config,
            owned_shards: Arc::new(RwLock::new(HashSet::new())),
            running: Arc::new(AtomicBool::new(false)),
            shutdown: Arc::new(Notify::new()),
            metrics: ShardCoordinatorMetrics::new(),
            last_heartbeat: Arc::new(RwLock::new(None)),
        })
    }

    /// Register this instance and claim available shards
    ///
    /// If `assigned_shards` is configured, attempts to claim those specific shards.
    /// If empty (auto mode), claims any available shards up to a fair share.
    ///
    /// # Returns
    /// Vector of successfully claimed shard IDs
    pub async fn register(&self) -> Result<Vec<u32>, ShardCoordinatorError> {
        self.metrics.record_registration();

        info!(
            instance_id = %self.config.instance_id,
            "Registering orchestrator instance"
        );

        let shards_to_claim = if self.config.assigned_shards.is_empty() {
            // Auto mode: try to claim all shards, keep what we get
            (0..self.config.total_shards).collect::<Vec<_>>()
        } else {
            // Explicit assignment: claim specified shards
            self.config.assigned_shards.clone()
        };

        let mut claimed = Vec::new();

        for shard in shards_to_claim {
            let key = self.shard_key(shard);

            match self
                .redis_client
                .try_claim_shard(
                    &key,
                    &self.config.instance_id,
                    self.config.ownership_ttl_secs,
                )
                .await
            {
                Ok(true) => {
                    self.metrics.record_claim_success();
                    self.metrics.record_shard_gained();
                    claimed.push(shard);
                    debug!(shard, "Successfully claimed shard");
                }
                Ok(false) => {
                    self.metrics.record_claim_failed();
                    debug!(shard, "Shard already owned by another instance");
                }
                Err(e) => {
                    warn!(shard, error = %e, "Failed to claim shard");
                }
            }
        }

        // Update owned shards
        {
            let mut owned = self.owned_shards.write();
            *owned = claimed.iter().copied().collect();
        }

        info!(
            instance_id = %self.config.instance_id,
            claimed_shards = ?claimed,
            count = claimed.len(),
            "Registration complete"
        );

        Ok(claimed)
    }

    /// Send heartbeat to maintain shard ownership
    ///
    /// Refreshes the TTL on all owned shards. If a shard refresh fails
    /// (e.g., another instance claimed it), that shard is removed from
    /// the owned set.
    pub async fn heartbeat(&self) -> Result<HeartbeatResult, ShardCoordinatorError> {
        let owned: Vec<u32> = self.owned_shards.read().iter().copied().collect();

        if owned.is_empty() {
            debug!("No shards to heartbeat");
            return Ok(HeartbeatResult {
                refreshed: 0,
                lost: Vec::new(),
            });
        }

        debug!(shards = ?owned, "Sending heartbeat for {} shards", owned.len());

        let mut refreshed = 0;
        let mut lost = Vec::new();

        for shard in owned {
            let key = self.shard_key(shard);

            match self
                .redis_client
                .refresh_shard(
                    &key,
                    &self.config.instance_id,
                    self.config.ownership_ttl_secs,
                )
                .await
            {
                Ok(true) => {
                    refreshed += 1;
                }
                Ok(false) => {
                    // Lost ownership
                    self.metrics.record_shard_lost();
                    lost.push(shard);
                    warn!(shard, "Lost ownership of shard");
                }
                Err(e) => {
                    self.metrics.record_heartbeat_failed();
                    warn!(shard, error = %e, "Failed to refresh shard");
                    // Don't remove on transient errors
                }
            }
        }

        // Remove lost shards from owned set
        if !lost.is_empty() {
            let mut owned = self.owned_shards.write();
            for shard in &lost {
                owned.remove(shard);
            }
        }

        if refreshed > 0 {
            self.metrics.record_heartbeat_success();
            *self.last_heartbeat.write() = Some(Instant::now());
        }

        Ok(HeartbeatResult { refreshed, lost })
    }

    /// Start the background heartbeat task
    ///
    /// This task runs until shutdown is signaled, periodically refreshing
    /// shard ownership.
    pub fn start_heartbeat(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        self.running.store(true, Ordering::SeqCst);

        let coordinator = self.clone();
        let interval = self.config.heartbeat_interval();

        info!(
            instance_id = %self.config.instance_id,
            interval_secs = interval.as_secs(),
            "Starting shard heartbeat task"
        );

        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            interval_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = interval_timer.tick() => {
                        if !coordinator.running.load(Ordering::SeqCst) {
                            break;
                        }

                        match coordinator.heartbeat().await {
                            Ok(result) => {
                                if !result.lost.is_empty() {
                                    warn!(
                                        lost = ?result.lost,
                                        "Lost shards during heartbeat"
                                    );
                                }
                            }
                            Err(e) => {
                                error!(error = %e, "Heartbeat failed");
                            }
                        }
                    }
                    _ = coordinator.shutdown.notified() => {
                        info!("Heartbeat task received shutdown signal");
                        break;
                    }
                }
            }

            coordinator.running.store(false, Ordering::SeqCst);
            info!("Heartbeat task stopped");
        })
    }

    /// Release all owned shards
    ///
    /// Should be called during graceful shutdown to allow other instances
    /// to immediately claim the shards.
    pub async fn release_shards(&self) -> Result<usize, ShardCoordinatorError> {
        let owned: Vec<u32> = self.owned_shards.read().iter().copied().collect();

        if owned.is_empty() {
            return Ok(0);
        }

        info!(
            instance_id = %self.config.instance_id,
            shards = ?owned,
            "Releasing {} shards",
            owned.len()
        );

        let mut released = 0;

        for shard in owned {
            let key = self.shard_key(shard);

            match self
                .redis_client
                .release_shard(&key, &self.config.instance_id)
                .await
            {
                Ok(true) => {
                    released += 1;
                    debug!(shard, "Released shard");
                }
                Ok(false) => {
                    debug!(shard, "Shard was not owned (already released or claimed)");
                }
                Err(e) => {
                    warn!(shard, error = %e, "Failed to release shard");
                }
            }
        }

        // Clear owned shards
        self.owned_shards.write().clear();

        info!(released, "Shards released");
        Ok(released)
    }

    /// Shutdown the coordinator
    ///
    /// Signals the heartbeat task to stop and releases all shards.
    pub async fn shutdown(&self) -> Result<(), ShardCoordinatorError> {
        info!(
            instance_id = %self.config.instance_id,
            "Shutting down ShardCoordinator"
        );

        self.running.store(false, Ordering::SeqCst);
        self.shutdown.notify_waiters();

        // Release shards
        self.release_shards().await?;

        Ok(())
    }

    /// Get currently owned shards
    pub fn owned_shards(&self) -> Vec<u32> {
        self.owned_shards.read().iter().copied().collect()
    }

    /// Get the number of owned shards
    pub fn owned_shard_count(&self) -> usize {
        self.owned_shards.read().len()
    }

    /// Check if a specific shard is owned
    pub fn owns_shard(&self, shard: u32) -> bool {
        self.owned_shards.read().contains(&shard)
    }

    /// Get the instance ID
    pub fn instance_id(&self) -> &str {
        &self.config.instance_id
    }

    /// Get the configuration
    pub fn config(&self) -> &HorizontalShardingConfig {
        &self.config
    }

    /// Get metrics
    pub fn metrics(&self) -> &ShardCoordinatorMetrics {
        &self.metrics
    }

    /// Check if the coordinator is running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Get time since last successful heartbeat
    pub fn time_since_heartbeat(&self) -> Option<Duration> {
        self.last_heartbeat.read().map(|t| t.elapsed())
    }

    /// Check health status
    pub async fn health_check(&self) -> HealthStatus {
        // Check Redis connection
        let redis_healthy = self.redis_client.ping().await.is_ok();

        // Check heartbeat recency
        let heartbeat_ok = self
            .last_heartbeat
            .read()
            .map(|t| t.elapsed() < self.config.ownership_ttl())
            .unwrap_or(false);

        let owned = self.owned_shard_count();

        HealthStatus {
            redis_healthy,
            heartbeat_ok,
            owned_shards: owned,
            is_running: self.is_running(),
        }
    }

    /// Try to claim additional shards
    ///
    /// Useful when scaling up or rebalancing.
    pub async fn try_claim_additional(
        &self,
        target_shards: &[u32],
    ) -> Result<Vec<u32>, ShardCoordinatorError> {
        let currently_owned: HashSet<u32> = self.owned_shards.read().iter().copied().collect();

        let mut newly_claimed = Vec::new();

        for &shard in target_shards {
            if currently_owned.contains(&shard) {
                continue; // Already owned
            }

            let key = self.shard_key(shard);

            match self
                .redis_client
                .try_claim_shard(
                    &key,
                    &self.config.instance_id,
                    self.config.ownership_ttl_secs,
                )
                .await
            {
                Ok(true) => {
                    self.metrics.record_claim_success();
                    self.metrics.record_shard_gained();
                    newly_claimed.push(shard);

                    // Add to owned set
                    self.owned_shards.write().insert(shard);

                    info!(shard, "Claimed additional shard");
                }
                Ok(false) => {
                    self.metrics.record_claim_failed();
                }
                Err(e) => {
                    warn!(shard, error = %e, "Failed to claim additional shard");
                }
            }
        }

        Ok(newly_claimed)
    }

    /// Get the Redis key for a shard
    fn shard_key(&self, shard: u32) -> String {
        format!("{}:{}", self.config.redis_prefix, shard)
    }
}

/// Result of a heartbeat operation
#[derive(Debug)]
pub struct HeartbeatResult {
    /// Number of shards successfully refreshed
    pub refreshed: usize,
    /// Shards that were lost (no longer owned)
    pub lost: Vec<u32>,
}

impl HeartbeatResult {
    /// Check if all shards were refreshed successfully
    pub fn is_healthy(&self) -> bool {
        self.lost.is_empty()
    }
}

/// Health status of the coordinator
#[derive(Debug, Clone)]
pub struct HealthStatus {
    /// Redis connection is healthy
    pub redis_healthy: bool,
    /// Recent heartbeat was successful
    pub heartbeat_ok: bool,
    /// Number of owned shards
    pub owned_shards: usize,
    /// Coordinator is running
    pub is_running: bool,
}

impl HealthStatus {
    /// Check if the coordinator is fully healthy
    pub fn is_healthy(&self) -> bool {
        self.redis_healthy && self.heartbeat_ok && self.is_running
    }
}

// ============================================================================
// Mock Implementation for Tests
// ============================================================================

/// Mock implementations for testing
#[cfg(test)]
pub mod mock {
    use super::*;
    use std::collections::HashMap;

    /// Mock Redis implementation for testing
    pub struct MockRedisCoordinator {
        shards: Arc<RwLock<HashMap<String, (String, Instant)>>>,
        ttl: Duration,
    }

    impl MockRedisCoordinator {
        /// Create a new mock coordinator with the specified TTL
        pub fn new(ttl: Duration) -> Self {
            Self {
                shards: Arc::new(RwLock::new(HashMap::new())),
                ttl,
            }
        }
    }

    #[async_trait::async_trait]
    impl ShardCoordinatorRedis for MockRedisCoordinator {
        async fn try_claim_shard(
            &self,
            key: &str,
            instance_id: &str,
            _ttl_secs: u64,
        ) -> Result<bool, ShardCoordinatorError> {
            let mut shards = self.shards.write();

            // Check if key exists and not expired
            if let Some((_, expires_at)) = shards.get(key) {
                if *expires_at > Instant::now() {
                    return Ok(false); // Already claimed
                }
            }

            // Claim it
            shards.insert(
                key.to_string(),
                (instance_id.to_string(), Instant::now() + self.ttl),
            );
            Ok(true)
        }

        async fn refresh_shard(
            &self,
            key: &str,
            instance_id: &str,
            _ttl_secs: u64,
        ) -> Result<bool, ShardCoordinatorError> {
            let mut shards = self.shards.write();

            if let Some((owner, expires_at)) = shards.get_mut(key) {
                if owner == instance_id && *expires_at > Instant::now() {
                    *expires_at = Instant::now() + self.ttl;
                    return Ok(true);
                }
            }

            Ok(false)
        }

        async fn release_shard(
            &self,
            key: &str,
            instance_id: &str,
        ) -> Result<bool, ShardCoordinatorError> {
            let mut shards = self.shards.write();

            if let Some((owner, _)) = shards.get(key) {
                if owner == instance_id {
                    shards.remove(key);
                    return Ok(true);
                }
            }

            Ok(false)
        }

        async fn get_shard_owner(
            &self,
            key: &str,
        ) -> Result<Option<String>, ShardCoordinatorError> {
            let shards = self.shards.read();

            if let Some((owner, expires_at)) = shards.get(key) {
                if *expires_at > Instant::now() {
                    return Ok(Some(owner.clone()));
                }
            }

            Ok(None)
        }

        async fn ping(&self) -> Result<(), ShardCoordinatorError> {
            Ok(())
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::mock::MockRedisCoordinator;
    use super::*;

    fn test_config(instance_id: &str, shards: Vec<u32>) -> HorizontalShardingConfig {
        HorizontalShardingConfig {
            enabled: true,
            total_shards: 8,
            assigned_shards: shards,
            instance_id: instance_id.to_string(),
            heartbeat_interval_secs: 1,
            ownership_ttl_secs: 5,
            redis_prefix: "test:shards".to_string(),
            allow_non_owned: true,
        }
    }

    #[tokio::test]
    async fn test_register_and_claim() {
        let redis = Arc::new(MockRedisCoordinator::new(Duration::from_secs(5)));
        let config = test_config("instance-1", vec![0, 1, 2, 3]);

        let coordinator = ShardCoordinator::new(redis, config).unwrap();
        let claimed = coordinator.register().await.unwrap();

        assert_eq!(claimed, vec![0, 1, 2, 3]);
        assert_eq!(coordinator.owned_shard_count(), 4);
    }

    #[tokio::test]
    async fn test_claim_conflict() {
        let redis = Arc::new(MockRedisCoordinator::new(Duration::from_secs(5)));

        // Instance 1 claims shards 0-3
        let config1 = test_config("instance-1", vec![0, 1, 2, 3]);
        let coord1 = ShardCoordinator::new(redis.clone(), config1).unwrap();
        coord1.register().await.unwrap();

        // Instance 2 tries to claim same shards
        let config2 = test_config("instance-2", vec![0, 1, 2, 3]);
        let coord2 = ShardCoordinator::new(redis.clone(), config2).unwrap();
        let claimed2 = coord2.register().await.unwrap();

        // Instance 2 should get nothing (all claimed)
        assert!(claimed2.is_empty());
    }

    #[tokio::test]
    async fn test_heartbeat() {
        let redis = Arc::new(MockRedisCoordinator::new(Duration::from_secs(5)));
        let config = test_config("instance-1", vec![0, 1]);

        let coordinator = ShardCoordinator::new(redis, config).unwrap();
        coordinator.register().await.unwrap();

        // Heartbeat should succeed
        let result = coordinator.heartbeat().await.unwrap();
        assert_eq!(result.refreshed, 2);
        assert!(result.lost.is_empty());
    }

    #[tokio::test]
    async fn test_release_shards() {
        let redis = Arc::new(MockRedisCoordinator::new(Duration::from_secs(5)));
        let config = test_config("instance-1", vec![0, 1, 2]);

        let coordinator = ShardCoordinator::new(redis.clone(), config).unwrap();
        coordinator.register().await.unwrap();
        assert_eq!(coordinator.owned_shard_count(), 3);

        // Release shards
        let released = coordinator.release_shards().await.unwrap();
        assert_eq!(released, 3);
        assert_eq!(coordinator.owned_shard_count(), 0);

        // Now another instance can claim them
        let config2 = test_config("instance-2", vec![0, 1, 2]);
        let coord2 = ShardCoordinator::new(redis.clone(), config2).unwrap();
        let claimed = coord2.register().await.unwrap();
        assert_eq!(claimed, vec![0, 1, 2]);
    }

    #[tokio::test]
    async fn test_auto_assignment() {
        let redis = Arc::new(MockRedisCoordinator::new(Duration::from_secs(5)));

        // Empty assigned_shards = auto mode (tries all shards)
        let config = HorizontalShardingConfig {
            enabled: true,
            total_shards: 4,
            assigned_shards: vec![], // Auto
            instance_id: "auto-instance".to_string(),
            heartbeat_interval_secs: 1,
            ownership_ttl_secs: 5,
            redis_prefix: "test:shards".to_string(),
            allow_non_owned: true,
        };

        let coordinator = ShardCoordinator::new(redis, config).unwrap();
        let claimed = coordinator.register().await.unwrap();

        // Should claim all available shards
        assert_eq!(claimed.len(), 4);
    }

    #[tokio::test]
    async fn test_claim_additional() {
        let redis = Arc::new(MockRedisCoordinator::new(Duration::from_secs(5)));
        let config = test_config("instance-1", vec![0, 1]);

        let coordinator = ShardCoordinator::new(redis, config).unwrap();
        coordinator.register().await.unwrap();
        assert_eq!(coordinator.owned_shard_count(), 2);

        // Try to claim additional shards
        let additional = coordinator.try_claim_additional(&[2, 3, 4]).await.unwrap();
        assert_eq!(additional, vec![2, 3, 4]);
        assert_eq!(coordinator.owned_shard_count(), 5);
    }

    #[tokio::test]
    async fn test_health_check() {
        let redis = Arc::new(MockRedisCoordinator::new(Duration::from_secs(5)));
        let config = test_config("instance-1", vec![0]);

        let coordinator = ShardCoordinator::new(redis, config).unwrap();
        coordinator.register().await.unwrap();
        coordinator.heartbeat().await.unwrap();

        let health = coordinator.health_check().await;
        assert!(health.redis_healthy);
        assert!(health.heartbeat_ok);
        assert_eq!(health.owned_shards, 1);
    }
}
