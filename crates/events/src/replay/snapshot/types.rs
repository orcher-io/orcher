//! Snapshot types, strategies, and the `SnapshotStore` trait.

use crate::error::Result;
use crate::metadata::StreamId;

use async_trait::async_trait;
use jiff::SignedDuration;
use orcher_common::types::time::Timestamptz;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// Captured state at a specific stream position, allowing replay to skip earlier events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub snapshot_id: String,
    pub stream_id: StreamId,
    pub stream_position: u64,
    pub global_position: u64,
    pub created_at: Timestamptz,
    pub data: serde_json::Value,
    pub metadata: HashMap<String, String>,
    pub version: u32,
    pub size_bytes: Option<usize>,
}

impl Snapshot {
    pub fn new(
        snapshot_id: String,
        stream_id: StreamId,
        stream_position: u64,
        data: serde_json::Value,
    ) -> Self {
        Self {
            snapshot_id,
            stream_id,
            stream_position,
            global_position: 0, // Will be set by snapshot manager
            created_at: Timestamptz::now(),
            data,
            metadata: HashMap::new(),
            version: 1,
            size_bytes: None,
        }
    }

    pub fn with_global_position(mut self, position: u64) -> Self {
        self.global_position = position;
        self
    }

    pub fn with_metadata(mut self, key: String, value: String) -> Self {
        self.metadata.insert(key, value);
        self
    }

    pub fn with_version(mut self, version: u32) -> Self {
        self.version = version;
        self
    }

    pub fn with_size(mut self, size_bytes: usize) -> Self {
        self.size_bytes = Some(size_bytes);
        self
    }

    pub fn age(&self) -> SignedDuration {
        Timestamptz::now() - self.created_at
    }

    pub fn is_older_than(&self, duration: SignedDuration) -> bool {
        self.age() > duration
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotStrategy {
    Never,
    EveryNEvents(usize),
    EveryDuration(SignedDuration),
    /// Snapshot when serialized state exceeds this byte threshold.
    SizeThreshold(usize),
    /// Any trigger fires (event count or duration).
    Combined,
}

impl SnapshotStrategy {
    pub fn should_snapshot_at_event_count(&self, event_count: usize) -> bool {
        match self {
            SnapshotStrategy::EveryNEvents(n) => event_count.is_multiple_of(*n),
            _ => false,
        }
    }

    pub fn should_snapshot_at_duration(&self, last_snapshot: Timestamptz) -> bool {
        match self {
            SnapshotStrategy::EveryDuration(duration) => {
                Timestamptz::now() - last_snapshot > *duration
            }
            _ => false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SnapshotConfig {
    pub strategy: SnapshotStrategy,
    pub max_snapshots_per_stream: usize,
    pub max_snapshot_age: Option<SignedDuration>,
    /// Won't create a new snapshot until at least this many events have passed.
    pub min_events_between_snapshots: usize,
    pub compress: bool,
    pub encryption_key: Option<Vec<u8>>,
}

impl Default for SnapshotConfig {
    fn default() -> Self {
        Self {
            strategy: SnapshotStrategy::EveryNEvents(1000),
            max_snapshots_per_stream: 10,
            max_snapshot_age: Some(SignedDuration::from_hours(30 * 24)),
            min_events_between_snapshots: 100,
            compress: true,
            encryption_key: None,
        }
    }
}

impl SnapshotConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_strategy(mut self, strategy: SnapshotStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    pub fn with_max_snapshots(mut self, max: usize) -> Self {
        self.max_snapshots_per_stream = max;
        self
    }

    pub fn with_max_age(mut self, duration: SignedDuration) -> Self {
        self.max_snapshot_age = Some(duration);
        self
    }

    pub fn with_compression(mut self, compress: bool) -> Self {
        self.compress = compress;
        self
    }
}

#[async_trait]
pub trait SnapshotStore: Send + Sync {
    async fn save_snapshot(&self, snapshot: Snapshot) -> Result<()>;
    async fn load_latest_snapshot(&self, stream_id: &StreamId) -> Result<Option<Snapshot>>;
    async fn load_snapshot(&self, snapshot_id: &str) -> Result<Option<Snapshot>>;
    async fn delete_snapshot(&self, snapshot_id: &str) -> Result<()>;
    async fn list_snapshots(&self, stream_id: &StreamId) -> Result<Vec<Snapshot>>;
    async fn cleanup_old_snapshots(
        &self,
        stream_id: &StreamId,
        config: &SnapshotConfig,
    ) -> Result<usize>;
    async fn snapshot_count(&self, stream_id: &StreamId) -> Result<usize>;
}

/// Manages snapshot lifecycle: creation, retrieval, and cleanup.
pub struct SnapshotManager<S: SnapshotStore> {
    store: Arc<S>,
    config: SnapshotConfig,
    last_snapshot_times: Arc<tokio::sync::RwLock<HashMap<StreamId, Timestamptz>>>,
}

impl<S: SnapshotStore> SnapshotManager<S> {
    pub fn new(store: Arc<S>, config: SnapshotConfig) -> Self {
        Self {
            store,
            config,
            last_snapshot_times: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
        }
    }

    pub async fn save_snapshot(
        &self,
        stream_id: StreamId,
        stream_position: u64,
        data: serde_json::Value,
    ) -> Result<Snapshot> {
        let snapshot_id = format!("{}-{}", stream_id, stream_position);
        let snapshot = Snapshot::new(snapshot_id, stream_id.clone(), stream_position, data);

        self.store.save_snapshot(snapshot.clone()).await?;

        // Update last snapshot time
        {
            let mut times = self.last_snapshot_times.write().await;
            times.insert(stream_id, Timestamptz::now());
        }

        Ok(snapshot)
    }

    pub async fn load_latest(&self, stream_id: &StreamId) -> Result<Option<Snapshot>> {
        self.store.load_latest_snapshot(stream_id).await
    }

    pub async fn load(&self, snapshot_id: &str) -> Result<Option<Snapshot>> {
        self.store.load_snapshot(snapshot_id).await
    }

    pub async fn delete(&self, snapshot_id: &str) -> Result<()> {
        self.store.delete_snapshot(snapshot_id).await
    }

    pub async fn should_create_snapshot(
        &self,
        stream_id: &StreamId,
        events_since_last: usize,
    ) -> bool {
        // Check minimum events threshold
        if events_since_last < self.config.min_events_between_snapshots {
            return false;
        }

        match self.config.strategy {
            SnapshotStrategy::Never => false,
            SnapshotStrategy::EveryNEvents(n) => events_since_last >= n,
            SnapshotStrategy::EveryDuration(duration) => {
                let times = self.last_snapshot_times.read().await;
                if let Some(last_time) = times.get(stream_id) {
                    Timestamptz::now() - *last_time > duration
                } else {
                    true // No snapshot yet
                }
            }
            SnapshotStrategy::SizeThreshold(_) => {
                // Would need to check actual state size
                false
            }
            SnapshotStrategy::Combined => {
                // Check all strategies
                self.config
                    .strategy
                    .should_snapshot_at_event_count(events_since_last)
                    || {
                        let times = self.last_snapshot_times.read().await;
                        if let Some(last_time) = times.get(stream_id) {
                            Timestamptz::now() - *last_time > SignedDuration::from_hours(1)
                        } else {
                            true
                        }
                    }
            }
        }
    }

    pub async fn cleanup(&self, stream_id: &StreamId) -> Result<usize> {
        self.store
            .cleanup_old_snapshots(stream_id, &self.config)
            .await
    }

    pub async fn list_snapshots(&self, stream_id: &StreamId) -> Result<Vec<Snapshot>> {
        self.store.list_snapshots(stream_id).await
    }

    pub async fn snapshot_count(&self, stream_id: &StreamId) -> Result<usize> {
        self.store.snapshot_count(stream_id).await
    }

    pub fn config(&self) -> &SnapshotConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot_creation() {
        let stream_id = StreamId::task("test-123");
        let data = serde_json::json!({"state": "active"});

        let snapshot = Snapshot::new("snap-1".to_string(), stream_id.clone(), 100, data.clone());

        assert_eq!(snapshot.snapshot_id, "snap-1");
        assert_eq!(snapshot.stream_id, stream_id);
        assert_eq!(snapshot.stream_position, 100);
        assert_eq!(snapshot.data, data);
    }

    #[test]
    fn test_snapshot_builder() {
        let snapshot = Snapshot::new(
            "snap-1".to_string(),
            StreamId::task("test-123"),
            100,
            serde_json::json!({}),
        )
        .with_global_position(500)
        .with_version(2)
        .with_metadata("key".to_string(), "value".to_string())
        .with_size(1024);

        assert_eq!(snapshot.global_position, 500);
        assert_eq!(snapshot.version, 2);
        assert_eq!(snapshot.metadata.get("key"), Some(&"value".to_string()));
        assert_eq!(snapshot.size_bytes, Some(1024));
    }

    #[test]
    fn test_snapshot_age() {
        let snapshot = Snapshot::new(
            "snap-1".to_string(),
            StreamId::task("test-123"),
            100,
            serde_json::json!({}),
        );

        let age = snapshot.age();
        assert!(age.as_millis() as i64 >= 0);
        assert!(age.as_secs() < 1);
    }

    #[test]
    fn test_snapshot_strategy_event_count() {
        let strategy = SnapshotStrategy::EveryNEvents(100);

        assert!(strategy.should_snapshot_at_event_count(100));
        assert!(strategy.should_snapshot_at_event_count(200));
        assert!(!strategy.should_snapshot_at_event_count(99));
        assert!(!strategy.should_snapshot_at_event_count(101));
    }

    #[test]
    fn test_snapshot_strategy_never() {
        let strategy = SnapshotStrategy::Never;

        assert!(!strategy.should_snapshot_at_event_count(100));
        assert!(!strategy.should_snapshot_at_event_count(1000));
    }

    #[test]
    fn test_snapshot_config_defaults() {
        let config = SnapshotConfig::default();

        assert_eq!(config.max_snapshots_per_stream, 10);
        assert_eq!(config.min_events_between_snapshots, 100);
        assert!(config.compress);
    }

    #[test]
    fn test_snapshot_config_builder() {
        let config = SnapshotConfig::new()
            .with_strategy(SnapshotStrategy::EveryNEvents(500))
            .with_max_snapshots(5)
            .with_compression(false);

        assert_eq!(config.strategy, SnapshotStrategy::EveryNEvents(500));
        assert_eq!(config.max_snapshots_per_stream, 5);
        assert!(!config.compress);
    }

    #[test]
    fn test_snapshot_strategy_duration() {
        let strategy = SnapshotStrategy::EveryDuration(SignedDuration::from_hours(1));
        let old_time = Timestamptz::now() - SignedDuration::from_hours(2);

        assert!(strategy.should_snapshot_at_duration(old_time));

        let recent_time = Timestamptz::now() - SignedDuration::from_mins(30);
        assert!(!strategy.should_snapshot_at_duration(recent_time));
    }

    #[test]
    fn test_snapshot_is_older_than() {
        let mut snapshot = Snapshot::new(
            "snap-1".to_string(),
            StreamId::task("test-123"),
            100,
            serde_json::json!({}),
        );

        // Set created time to 2 hours ago
        snapshot.created_at = Timestamptz::now() - SignedDuration::from_hours(2);

        assert!(snapshot.is_older_than(SignedDuration::from_hours(1)));
        assert!(!snapshot.is_older_than(SignedDuration::from_hours(3)));
    }
}
