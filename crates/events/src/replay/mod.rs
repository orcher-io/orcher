//! Event replay from stored event streams.

pub mod config;
pub mod metrics;
pub mod replayer;
pub mod snapshot;
pub mod store;
pub mod stream;

pub use config::{EventStoreConfig, RetryConfig, RetryableError};
pub use metrics::{EventStoreMetrics, MetricsSnapshot};
pub use replayer::{EventReplayer, ReplayHandler, ReplayOptions, ReplayProgress, ReplayResult};
pub use snapshot::{Snapshot, SnapshotConfig, SnapshotManager, SnapshotStore, SnapshotStrategy};
pub use store::{
    AppendResult, EventQuery, EventStore, QueryResult, ReadDirection, StoredEvent, StreamPosition,
};
pub use stream::{EventStream, StreamConfig, StreamFilter};

pub use crate::metadata::StreamId;

use jiff::SignedDuration;
use orcher_common::types::time::Timestamptz;

#[derive(Debug, thiserror::Error)]
pub enum ReplayError {
    #[error("Event store error: {0}")]
    StoreError(String),

    #[error("Snapshot error: {0}")]
    SnapshotError(String),

    #[error("Stream error: {0}")]
    StreamError(String),

    #[error("Replay failed: {0}")]
    ReplayFailed(String),

    #[error("Invalid replay options: {0}")]
    InvalidOptions(String),

    #[error("Event not found: {0}")]
    EventNotFound(String),

    #[error("Stream not found: {stream_id}")]
    StreamNotFound { stream_id: String },

    #[error("Serialization error: {0}")]
    SerializationError(String),

    #[error("Deserialization error: {0}")]
    DeserializationError(String),

    #[error("Version mismatch: expected {expected}, got {actual}")]
    VersionMismatch { expected: u64, actual: u64 },

    #[error("Concurrency conflict on stream {stream_id}")]
    ConcurrencyConflict { stream_id: String },
}

impl From<ReplayError> for crate::error::EventError {
    fn from(err: ReplayError) -> Self {
        crate::error::EventError::internal(err.to_string())
    }
}

#[derive(Debug, Clone, Default)]
pub struct ReplayStats {
    pub events_replayed: u64,
    pub events_skipped: u64,
    pub events_failed: u64,
    pub started_at: Option<Timestamptz>,
    pub completed_at: Option<Timestamptz>,
    pub snapshots_used: u64,
}

impl ReplayStats {
    pub fn new() -> Self {
        Self {
            events_replayed: 0,
            events_skipped: 0,
            events_failed: 0,
            started_at: Some(Timestamptz::now()),
            completed_at: None,
            snapshots_used: 0,
        }
    }

    pub fn complete(&mut self) {
        self.completed_at = Some(Timestamptz::now());
    }

    pub fn duration(&self) -> Option<SignedDuration> {
        if let (Some(start), Some(end)) = (self.started_at, self.completed_at) {
            Some(end - start)
        } else {
            None
        }
    }

    /// Ratio of replayed to total (replayed + failed). Returns 1.0 if no events processed.
    pub fn success_rate(&self) -> f64 {
        let total = self.events_replayed + self.events_failed;
        if total == 0 {
            return 1.0;
        }
        self.events_replayed as f64 / total as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replay_stats_creation() {
        let stats = ReplayStats::new();
        assert_eq!(stats.events_replayed, 0);
        assert_eq!(stats.events_failed, 0);
        assert!(stats.started_at.is_some());
        assert!(stats.completed_at.is_none());
    }

    #[test]
    fn test_replay_stats_complete() {
        let mut stats = ReplayStats::new();
        stats.events_replayed = 100;
        stats.complete();

        assert!(stats.completed_at.is_some());
        assert!(stats.duration().is_some());
    }

    #[test]
    fn test_replay_stats_success_rate() {
        let mut stats = ReplayStats::new();
        stats.events_replayed = 80;
        stats.events_failed = 20;

        assert_eq!(stats.success_rate(), 0.8);
    }

    #[test]
    fn test_replay_error_display() {
        let err = ReplayError::StreamNotFound {
            stream_id: "test-stream".to_string(),
        };
        assert!(err.to_string().contains("test-stream"));
    }

    #[test]
    fn test_version_mismatch_error() {
        let err = ReplayError::VersionMismatch {
            expected: 5,
            actual: 3,
        };
        assert!(err.to_string().contains("expected 5"));
        assert!(err.to_string().contains("got 3"));
    }
}
