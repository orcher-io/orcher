//! # Horizontal Sharding for Orchestrator Instances
//!
//! This module provides horizontal sharding infrastructure for distributing
//! task queues across multiple orchestrator instances.

#![allow(missing_docs)]

use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tracing::{debug, info, warn};

/// Configuration for horizontal orchestrator sharding
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HorizontalShardingConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_total_shards")]
    pub total_shards: u32,
    #[serde(default)]
    pub assigned_shards: Vec<u32>,
    #[serde(default = "default_instance_id")]
    pub instance_id: String,
    #[serde(default = "default_heartbeat_interval_secs")]
    pub heartbeat_interval_secs: u64,
    #[serde(default = "default_ownership_ttl_secs")]
    pub ownership_ttl_secs: u64,
    #[serde(default = "default_redis_prefix")]
    pub redis_prefix: String,
    #[serde(default = "default_allow_non_owned")]
    pub allow_non_owned: bool,
}

impl Default for HorizontalShardingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            total_shards: default_total_shards(),
            assigned_shards: Vec::new(),
            instance_id: default_instance_id(),
            heartbeat_interval_secs: default_heartbeat_interval_secs(),
            ownership_ttl_secs: default_ownership_ttl_secs(),
            redis_prefix: default_redis_prefix(),
            allow_non_owned: default_allow_non_owned(),
        }
    }
}

impl HorizontalShardingConfig {
    pub fn new(total_shards: u32, assigned_shards: Vec<u32>, instance_id: String) -> Self {
        Self {
            enabled: true,
            total_shards,
            assigned_shards,
            instance_id,
            ..Default::default()
        }
    }
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            ..Default::default()
        }
    }
    pub fn auto(total_shards: u32, instance_id: String) -> Self {
        Self {
            enabled: true,
            total_shards,
            assigned_shards: Vec::new(),
            instance_id,
            ..Default::default()
        }
    }
    pub fn from_env() -> Self {
        let enabled = std::env::var("ORCHER_SHARDING_ENABLED")
            .ok()
            .map(|s| s.to_lowercase() == "true" || s == "1")
            .unwrap_or(false);
        let total_shards = std::env::var("ORCHER_SHARDING_TOTAL_SHARDS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(default_total_shards());
        let assigned_shards = std::env::var("ORCHER_SHARDING_ASSIGNED_SHARDS")
            .ok()
            .map(|s| {
                s.split(',')
                    .filter_map(|p| p.trim().parse::<u32>().ok())
                    .collect()
            })
            .unwrap_or_default();
        let instance_id = std::env::var("ORCHER_SHARDING_INSTANCE_ID")
            .ok()
            .unwrap_or_else(default_instance_id);
        let heartbeat_interval_secs = std::env::var("ORCHER_SHARDING_HEARTBEAT_INTERVAL")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(default_heartbeat_interval_secs());
        let ownership_ttl_secs = std::env::var("ORCHER_SHARDING_OWNERSHIP_TTL")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(default_ownership_ttl_secs());
        Self {
            enabled,
            total_shards,
            assigned_shards,
            instance_id,
            heartbeat_interval_secs,
            ownership_ttl_secs,
            ..Default::default()
        }
    }
    pub fn heartbeat_interval(&self) -> Duration {
        Duration::from_secs(self.heartbeat_interval_secs)
    }
    pub fn ownership_ttl(&self) -> Duration {
        Duration::from_secs(self.ownership_ttl_secs)
    }
    pub fn needs_auto_assignment(&self) -> bool {
        self.enabled && self.assigned_shards.is_empty()
    }
    pub fn validate(&self) -> Result<(), String> {
        if self.total_shards == 0 {
            return Err("total_shards must be greater than 0".to_string());
        }
        for shard in &self.assigned_shards {
            if *shard >= self.total_shards {
                return Err(format!(
                    "assigned shard {} is >= total_shards {}",
                    shard, self.total_shards
                ));
            }
        }
        if self.heartbeat_interval_secs >= self.ownership_ttl_secs {
            return Err(format!(
                "heartbeat_interval_secs ({}) should be less than ownership_ttl_secs ({})",
                self.heartbeat_interval_secs, self.ownership_ttl_secs
            ));
        }
        if self.instance_id.is_empty() {
            return Err("instance_id cannot be empty".to_string());
        }
        Ok(())
    }
    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }
    pub fn with_total_shards(mut self, total: u32) -> Self {
        self.total_shards = total;
        self
    }
    pub fn with_assigned_shards(mut self, shards: Vec<u32>) -> Self {
        self.assigned_shards = shards;
        self
    }
    pub fn with_instance_id(mut self, id: String) -> Self {
        self.instance_id = id;
        self
    }
}

fn default_total_shards() -> u32 {
    16
}
fn default_instance_id() -> String {
    hostname::get()
        .ok()
        .and_then(|h| h.into_string().ok())
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string())
}
fn default_heartbeat_interval_secs() -> u64 {
    10
}
fn default_ownership_ttl_secs() -> u64 {
    30
}
fn default_redis_prefix() -> String {
    "orcher:shards".to_string()
}
fn default_allow_non_owned() -> bool {
    true
}

#[derive(Debug, Default)]
pub struct HorizontalShardMetrics {
    pub owned_requests: AtomicU64,
    pub non_owned_requests: AtomicU64,
    pub redirect_candidates: AtomicU64,
    pub shard_lookups: AtomicU64,
}

impl HorizontalShardMetrics {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn record_owned_request(&self) {
        self.owned_requests.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_non_owned_request(&self) {
        self.non_owned_requests.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_redirect_candidate(&self) {
        self.redirect_candidates.fetch_add(1, Ordering::Relaxed);
    }
    pub fn record_shard_lookup(&self) {
        self.shard_lookups.fetch_add(1, Ordering::Relaxed);
    }
    pub fn ownership_rate(&self) -> f64 {
        let owned = self.owned_requests.load(Ordering::Relaxed);
        let non_owned = self.non_owned_requests.load(Ordering::Relaxed);
        let total = owned + non_owned;
        if total == 0 {
            1.0
        } else {
            owned as f64 / total as f64
        }
    }
}

#[derive(Debug)]
pub struct HorizontalShardRouter {
    config: HorizontalShardingConfig,
    owned_shards: HashSet<u32>,
    metrics: HorizontalShardMetrics,
}

impl HorizontalShardRouter {
    pub fn new(config: HorizontalShardingConfig) -> Self {
        let owned_shards: HashSet<u32> = config.assigned_shards.iter().copied().collect();
        info!(enabled = config.enabled, total_shards = config.total_shards, owned_shards = ?config.assigned_shards, instance_id = %config.instance_id, "Created HorizontalShardRouter");
        Self {
            config,
            owned_shards,
            metrics: HorizontalShardMetrics::new(),
        }
    }
    pub fn from_env() -> Self {
        Self::new(HorizontalShardingConfig::from_env())
    }
    pub fn disabled() -> Self {
        Self::new(HorizontalShardingConfig::disabled())
    }
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }
    pub fn get_shard(&self, namespace: &str, task_queue: &str) -> u32 {
        self.metrics.record_shard_lookup();
        if !self.config.enabled || self.config.total_shards <= 1 {
            return 0;
        }
        let key = format!("{}:{}", namespace, task_queue);
        let hash = self.hash_key(&key);
        (hash % self.config.total_shards as u64) as u32
    }
    pub fn owns_task_queue(&self, namespace: &str, task_queue: &str) -> bool {
        if !self.config.enabled {
            return true;
        }
        let shard = self.get_shard(namespace, task_queue);
        let owns = self.owned_shards.contains(&shard);
        if owns {
            self.metrics.record_owned_request();
            debug!(namespace, task_queue, shard, instance_id = %self.config.instance_id, "Task queue owned by this instance");
        } else {
            self.metrics.record_non_owned_request();
            debug!(namespace, task_queue, shard, instance_id = %self.config.instance_id, "Task queue NOT owned by this instance");
        }
        owns
    }
    pub fn owns_shard(&self, shard: u32) -> bool {
        !self.config.enabled || self.owned_shards.contains(&shard)
    }
    pub fn owned_shards(&self) -> &HashSet<u32> {
        &self.owned_shards
    }
    pub fn owned_shard_count(&self) -> usize {
        if self.config.enabled {
            self.owned_shards.len()
        } else {
            self.config.total_shards as usize
        }
    }
    pub fn total_shards(&self) -> u32 {
        self.config.total_shards
    }
    pub fn instance_id(&self) -> &str {
        &self.config.instance_id
    }
    pub fn config(&self) -> &HorizontalShardingConfig {
        &self.config
    }
    pub fn metrics(&self) -> &HorizontalShardMetrics {
        &self.metrics
    }
    pub fn allows_non_owned(&self) -> bool {
        self.config.allow_non_owned
    }
    pub fn with_updated_shards(&self, shards: Vec<u32>) -> Self {
        let mut new_config = self.config.clone();
        new_config.assigned_shards = shards;
        Self::new(new_config)
    }
    pub fn iter_owned_shards(&self) -> impl Iterator<Item = u32> + '_ {
        if self.config.enabled {
            itertools::Either::Left(self.owned_shards.iter().copied())
        } else {
            itertools::Either::Right(0..self.config.total_shards)
        }
    }
    pub fn get_redirect_info(
        &self,
        namespace: &str,
        task_queue: &str,
    ) -> Option<ShardRedirectInfo> {
        if self.owns_task_queue(namespace, task_queue) {
            return None;
        }
        self.metrics.record_redirect_candidate();
        let shard = self.get_shard(namespace, task_queue);
        Some(ShardRedirectInfo {
            shard,
            namespace: namespace.to_string(),
            task_queue: task_queue.to_string(),
            target_instance: None,
        })
    }
    fn hash_key(&self, key: &str) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }
}

#[derive(Debug, Clone)]
pub struct ShardRedirectInfo {
    pub shard: u32,
    pub namespace: String,
    pub task_queue: String,
    pub target_instance: Option<String>,
}

impl ShardRedirectInfo {
    pub fn has_target(&self) -> bool {
        self.target_instance.is_some()
    }
}

#[derive(Debug, Clone)]
pub enum ShardOwnershipChange {
    Gained(Vec<u32>),
    Lost(Vec<u32>),
    NoChange,
}

impl ShardOwnershipChange {
    pub fn has_change(&self) -> bool {
        !matches!(self, ShardOwnershipChange::NoChange)
    }
    pub fn gained(&self) -> Option<&[u32]> {
        match self {
            ShardOwnershipChange::Gained(shards) => Some(shards),
            _ => None,
        }
    }
    pub fn lost(&self) -> Option<&[u32]> {
        match self {
            ShardOwnershipChange::Lost(shards) => Some(shards),
            _ => None,
        }
    }
}

pub fn compute_ownership_change(
    old_shards: &HashSet<u32>,
    new_shards: &HashSet<u32>,
) -> ShardOwnershipChange {
    let gained: Vec<u32> = new_shards.difference(old_shards).copied().collect();
    let lost: Vec<u32> = old_shards.difference(new_shards).copied().collect();
    match (gained.is_empty(), lost.is_empty()) {
        (true, true) => ShardOwnershipChange::NoChange,
        (false, true) => ShardOwnershipChange::Gained(gained),
        (true, false) => ShardOwnershipChange::Lost(lost),
        (false, false) => {
            warn!(gained = ?gained, lost = ?lost, "Shard ownership changed with both gains and losses");
            ShardOwnershipChange::Gained(gained)
        }
    }
}
