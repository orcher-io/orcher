//! Core event replay logic with filtering and progress tracking.

use crate::error::Result;
use crate::metadata::StreamId;
use crate::replay::{EventStore, ReplayStats, StoredEvent, StreamPosition};
use async_trait::async_trait;
use orcher_common::types::time::Timestamptz;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub struct ReplayOptions {
    pub stream_id: Option<StreamId>,
    pub event_types: Option<Vec<String>>,
    pub from_timestamp: Option<Timestamptz>,
    pub to_timestamp: Option<Timestamptz>,
    pub from_position: Option<u64>,
    pub to_position: Option<u64>,
    pub batch_size: usize,
    pub continue_on_error: bool,
    pub use_snapshots: bool,
    pub max_events: Option<usize>,
    /// Report progress every N events.
    pub progress_interval: Option<usize>,
}

impl ReplayOptions {
    pub fn new() -> Self {
        Self {
            stream_id: None,
            event_types: None,
            from_timestamp: None,
            to_timestamp: None,
            from_position: None,
            to_position: None,
            batch_size: 1000,
            continue_on_error: false,
            use_snapshots: true,
            max_events: None,
            progress_interval: Some(1000),
        }
    }

    pub fn for_stream(mut self, stream_id: StreamId) -> Self {
        self.stream_id = Some(stream_id);
        self
    }

    pub fn with_event_types(mut self, event_types: Vec<String>) -> Self {
        self.event_types = Some(event_types);
        self
    }

    pub fn from_timestamp(mut self, timestamp: Timestamptz) -> Self {
        self.from_timestamp = Some(timestamp);
        self
    }

    pub fn to_timestamp(mut self, timestamp: Timestamptz) -> Self {
        self.to_timestamp = Some(timestamp);
        self
    }

    pub fn from_position(mut self, position: u64) -> Self {
        self.from_position = Some(position);
        self
    }

    pub fn to_position(mut self, position: u64) -> Self {
        self.to_position = Some(position);
        self
    }

    pub fn batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    pub fn continue_on_error(mut self, continue_on_error: bool) -> Self {
        self.continue_on_error = continue_on_error;
        self
    }

    pub fn use_snapshots(mut self, use_snapshots: bool) -> Self {
        self.use_snapshots = use_snapshots;
        self
    }

    pub fn max_events(mut self, max: usize) -> Self {
        self.max_events = Some(max);
        self
    }

    pub fn progress_interval(mut self, interval: usize) -> Self {
        self.progress_interval = Some(interval);
        self
    }
}

impl Default for ReplayOptions {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct ReplayProgress {
    pub current_position: u64,
    pub total_events: Option<u64>,
    pub events_replayed: u64,
    pub events_failed: u64,
    pub percentage: Option<f64>,
    pub estimated_remaining: Option<std::time::Duration>,
}

impl ReplayProgress {
    pub fn new(current_position: u64, total_events: Option<u64>) -> Self {
        let percentage = total_events.map(|total| {
            if total == 0 {
                100.0
            } else {
                (current_position as f64 / total as f64) * 100.0
            }
        });

        Self {
            current_position,
            total_events,
            events_replayed: 0,
            events_failed: 0,
            percentage,
            estimated_remaining: None,
        }
    }

    pub fn update(&mut self, position: u64, replayed: u64, failed: u64) {
        self.current_position = position;
        self.events_replayed = replayed;
        self.events_failed = failed;

        if let Some(total) = self.total_events {
            self.percentage = Some((position as f64 / total as f64) * 100.0);
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReplayResult {
    pub stats: ReplayStats,
    pub final_position: u64,
    pub success: bool,
    pub error: Option<String>,
}

impl ReplayResult {
    pub fn success(stats: ReplayStats, final_position: u64) -> Self {
        Self {
            stats,
            final_position,
            success: true,
            error: None,
        }
    }

    pub fn failure(stats: ReplayStats, final_position: u64, error: String) -> Self {
        Self {
            stats,
            final_position,
            success: false,
            error: Some(error),
        }
    }
}

#[async_trait]
pub trait ReplayHandler: Send + Sync {
    async fn handle_event(&self, event: &StoredEvent) -> Result<()>;

    async fn on_replay_start(&self) -> Result<()> {
        Ok(())
    }

    async fn on_replay_complete(&self, stats: &ReplayStats) -> Result<()> {
        let _ = stats;
        Ok(())
    }

    async fn on_progress(&self, progress: &ReplayProgress) -> Result<()> {
        let _ = progress;
        Ok(())
    }
}

/// Replays events from a store through a handler, with batching and progress tracking.
pub struct EventReplayer<S: EventStore> {
    store: Arc<S>,
    stats: Arc<RwLock<ReplayStats>>,
}

impl<S: EventStore> EventReplayer<S> {
    pub fn new(store: Arc<S>) -> Self {
        Self {
            store,
            stats: Arc::new(RwLock::new(ReplayStats::new())),
        }
    }

    pub async fn replay_all<H: ReplayHandler>(&self, handler: Arc<H>) -> Result<ReplayResult> {
        self.replay_with_options(ReplayOptions::default(), handler)
            .await
    }

    pub async fn replay_stream<H: ReplayHandler>(
        &self,
        stream_id: StreamId,
        handler: Arc<H>,
    ) -> Result<ReplayResult> {
        let options = ReplayOptions::new().for_stream(stream_id);
        self.replay_with_options(options, handler).await
    }

    pub async fn replay_with_options<H: ReplayHandler>(
        &self,
        options: ReplayOptions,
        handler: Arc<H>,
    ) -> Result<ReplayResult> {
        // Reset statistics
        {
            let mut stats = self.stats.write().await;
            *stats = ReplayStats::new();
        }

        // Notify handler of replay start
        handler.on_replay_start().await?;

        // Determine starting position
        let start_position = options.from_position.unwrap_or(0);
        let mut current_position = start_position;
        let mut total_replayed = 0u64;
        let mut total_failed = 0u64;

        loop {
            // Check if we've reached max events
            if let Some(max) = options.max_events {
                if total_replayed >= max as u64 {
                    break;
                }
            }

            // Read batch of events
            let batch = if let Some(stream_id) = &options.stream_id {
                self.store
                    .read_stream(
                        stream_id,
                        StreamPosition::Position(current_position),
                        options.batch_size,
                    )
                    .await?
            } else {
                self.store
                    .read_all(current_position, options.batch_size)
                    .await?
            };

            // No more events
            if batch.is_empty() {
                break;
            }

            // Process batch
            for event in &batch {
                // Filter by event types
                if let Some(types) = &options.event_types {
                    if !types.contains(&event.event_type) {
                        continue;
                    }
                }

                // Filter by timestamp
                if let Some(from_ts) = options.from_timestamp {
                    if event.timestamp < from_ts {
                        continue;
                    }
                }

                if let Some(to_ts) = options.to_timestamp {
                    if event.timestamp >= to_ts {
                        continue;
                    }
                }

                // Handle event
                match handler.handle_event(event).await {
                    Ok(_) => {
                        total_replayed += 1;

                        let mut stats = self.stats.write().await;
                        stats.events_replayed += 1;
                    }
                    Err(e) => {
                        total_failed += 1;

                        let mut stats = self.stats.write().await;
                        stats.events_failed += 1;

                        if !options.continue_on_error {
                            stats.complete();
                            return Ok(ReplayResult::failure(
                                stats.clone(),
                                current_position,
                                e.to_string(),
                            ));
                        }
                    }
                }

                current_position = event.global_position + 1;

                // Report progress
                if let Some(interval) = options.progress_interval {
                    if total_replayed.is_multiple_of(interval as u64) {
                        let mut progress = ReplayProgress::new(current_position, None);
                        progress.update(current_position, total_replayed, total_failed);
                        handler.on_progress(&progress).await?;
                    }
                }

                // Check max events again
                if let Some(max) = options.max_events {
                    if total_replayed >= max as u64 {
                        break;
                    }
                }
            }

            // Check if we've reached the end position
            if let Some(to_pos) = options.to_position {
                if current_position >= to_pos {
                    break;
                }
            }

            // If batch was smaller than requested, we've reached the end
            if batch.len() < options.batch_size {
                break;
            }
        }

        // Finalize statistics
        let mut stats = self.stats.write().await;
        stats.complete();

        // Notify handler of completion
        handler.on_replay_complete(&stats).await?;

        Ok(ReplayResult::success(stats.clone(), current_position))
    }

    pub async fn get_stats(&self) -> ReplayStats {
        self.stats.read().await.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replay_options_builder() {
        let stream_id = StreamId::task("test-123");
        let options = ReplayOptions::new()
            .for_stream(stream_id.clone())
            .batch_size(500)
            .continue_on_error(true)
            .max_events(1000);

        assert_eq!(options.stream_id, Some(stream_id));
        assert_eq!(options.batch_size, 500);
        assert!(options.continue_on_error);
        assert_eq!(options.max_events, Some(1000));
    }

    #[test]
    fn test_replay_options_defaults() {
        let options = ReplayOptions::default();
        assert_eq!(options.batch_size, 1000);
        assert!(!options.continue_on_error);
        assert!(options.use_snapshots);
    }

    #[test]
    fn test_replay_progress() {
        let progress = ReplayProgress::new(50, Some(100));
        assert_eq!(progress.current_position, 50);
        assert_eq!(progress.total_events, Some(100));
        assert_eq!(progress.percentage, Some(50.0));
    }

    #[test]
    fn test_replay_progress_update() {
        let mut progress = ReplayProgress::new(0, Some(100));
        progress.update(75, 70, 5);

        assert_eq!(progress.current_position, 75);
        assert_eq!(progress.events_replayed, 70);
        assert_eq!(progress.events_failed, 5);
        assert_eq!(progress.percentage, Some(75.0));
    }

    #[test]
    fn test_replay_result_success() {
        let stats = ReplayStats::new();
        let result = ReplayResult::success(stats, 100);

        assert!(result.success);
        assert_eq!(result.final_position, 100);
        assert!(result.error.is_none());
    }

    #[test]
    fn test_replay_result_failure() {
        let stats = ReplayStats::new();
        let result = ReplayResult::failure(stats, 50, "Test error".to_string());

        assert!(!result.success);
        assert_eq!(result.final_position, 50);
        assert_eq!(result.error, Some("Test error".to_string()));
    }
}
