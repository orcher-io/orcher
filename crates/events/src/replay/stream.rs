//! Continuous event streaming with filtering and backpressure.

use crate::error::Result;
use crate::metadata::StreamId;
use crate::replay::{EventStore, StoredEvent, StreamPosition};

use std::sync::Arc;
use tokio::sync::mpsc;

#[derive(Clone)]
pub struct StreamFilter {
    pub event_types: Option<Vec<String>>,
    pub stream_ids: Option<Vec<StreamId>>,
    #[allow(clippy::type_complexity)]
    pub predicate: Option<Arc<dyn Fn(&StoredEvent) -> bool + Send + Sync>>,
}

impl std::fmt::Debug for StreamFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamFilter")
            .field("event_types", &self.event_types)
            .field("stream_ids", &self.stream_ids)
            .field("predicate", &self.predicate.as_ref().map(|_| "<function>"))
            .finish()
    }
}

impl StreamFilter {
    pub fn new() -> Self {
        Self {
            event_types: None,
            stream_ids: None,
            predicate: None,
        }
    }

    pub fn with_event_types(mut self, event_types: Vec<String>) -> Self {
        self.event_types = Some(event_types);
        self
    }

    pub fn with_stream_ids(mut self, stream_ids: Vec<StreamId>) -> Self {
        self.stream_ids = Some(stream_ids);
        self
    }

    pub fn with_predicate<F>(mut self, predicate: F) -> Self
    where
        F: Fn(&StoredEvent) -> bool + Send + Sync + 'static,
    {
        self.predicate = Some(Arc::new(predicate));
        self
    }

    pub fn matches(&self, event: &StoredEvent) -> bool {
        // Check event types
        if let Some(types) = &self.event_types {
            if !types.contains(&event.event_type) {
                return false;
            }
        }

        // Check stream IDs
        if let Some(stream_ids) = &self.stream_ids {
            if !stream_ids.contains(&event.stream_id) {
                return false;
            }
        }

        // Check custom predicate
        if let Some(predicate) = &self.predicate {
            if !predicate(event) {
                return false;
            }
        }

        true
    }
}

impl Default for StreamFilter {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct StreamConfig {
    pub batch_size: usize,
    pub buffer_size: usize,
    pub start_position: u64,
    /// Keep polling for new events instead of ending at the tail.
    pub follow: bool,
    pub poll_interval_ms: u64,
    pub max_events: Option<usize>,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            batch_size: 100,
            buffer_size: 1000,
            start_position: 0,
            follow: false,
            poll_interval_ms: 1000,
            max_events: None,
        }
    }
}

impl StreamConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    pub fn with_buffer_size(mut self, size: usize) -> Self {
        self.buffer_size = size;
        self
    }

    pub fn from_position(mut self, position: u64) -> Self {
        self.start_position = position;
        self
    }

    pub fn follow(mut self, follow: bool) -> Self {
        self.follow = follow;
        self
    }

    pub fn poll_interval_ms(mut self, interval: u64) -> Self {
        self.poll_interval_ms = interval;
        self
    }

    pub fn max_events(mut self, max: usize) -> Self {
        self.max_events = Some(max);
        self
    }
}

/// Async event reader that spawns a background task to batch-read from the store.
pub struct EventStream<S: EventStore> {
    store: Arc<S>,
    config: StreamConfig,
    filter: StreamFilter,
    current_position: u64,
    events_streamed: usize,
    receiver: Option<mpsc::Receiver<StoredEvent>>,
    ended: bool,
}

impl<S: EventStore + 'static> EventStream<S> {
    pub fn new(store: Arc<S>, config: StreamConfig, filter: StreamFilter) -> Self {
        Self {
            store,
            config,
            filter,
            current_position: 0,
            events_streamed: 0,
            receiver: None,
            ended: false,
        }
    }

    pub async fn start(&mut self) -> Result<()> {
        let (tx, rx) = mpsc::channel(self.config.buffer_size);
        self.receiver = Some(rx);
        self.current_position = self.config.start_position;

        let store = self.store.clone();
        let config = self.config.clone();
        let filter = self.filter.clone();
        let mut position = self.config.start_position;
        let mut total_sent = 0;

        // Spawn background task to read and send events
        tokio::spawn(async move {
            loop {
                // Check max events limit
                if let Some(max) = config.max_events {
                    if total_sent >= max {
                        break;
                    }
                }

                // Read batch of events
                let batch = match store.read_all(position, config.batch_size).await {
                    Ok(events) => events,
                    Err(_) => break,
                };

                if batch.is_empty() {
                    // No more events
                    if config.follow {
                        // Wait and retry
                        tokio::time::sleep(tokio::time::Duration::from_millis(
                            config.poll_interval_ms,
                        ))
                        .await;
                        continue;
                    } else {
                        // End of stream
                        break;
                    }
                }

                // Filter and send events
                for event in batch {
                    if filter.matches(&event) {
                        if tx.send(event.clone()).await.is_err() {
                            // Receiver dropped
                            return;
                        }
                        total_sent += 1;

                        // Check max events again
                        if let Some(max) = config.max_events {
                            if total_sent >= max {
                                return;
                            }
                        }
                    }
                    position = event.global_position + 1;
                }
            }
        });

        Ok(())
    }

    /// Returns the next event, auto-starting the background reader if needed.
    pub async fn next(&mut self) -> Result<Option<StoredEvent>> {
        // Start stream if not started
        if self.receiver.is_none() && !self.ended {
            self.start().await?;
        }

        if self.ended {
            return Ok(None);
        }

        // Receive from channel
        if let Some(receiver) = &mut self.receiver {
            match receiver.recv().await {
                Some(event) => {
                    self.current_position = event.global_position + 1;
                    self.events_streamed += 1;
                    Ok(Some(event))
                }
                None => {
                    // Channel closed
                    self.ended = true;
                    Ok(None)
                }
            }
        } else {
            Ok(None)
        }
    }

    pub fn is_ended(&self) -> bool {
        self.ended
    }

    pub fn position(&self) -> u64 {
        self.current_position
    }

    pub fn events_streamed(&self) -> usize {
        self.events_streamed
    }

    pub fn close(&mut self) {
        self.receiver.take();
        self.ended = true;
    }
}

/// Reads events from a single stream, tracking position between batches.
pub struct SingleStreamReader<S: EventStore> {
    store: Arc<S>,
    stream_id: StreamId,
    position: u64,
}

impl<S: EventStore> SingleStreamReader<S> {
    pub fn new(store: Arc<S>, stream_id: StreamId) -> Self {
        Self {
            store,
            stream_id,
            position: 0,
        }
    }

    pub fn from_position(store: Arc<S>, stream_id: StreamId, position: u64) -> Self {
        Self {
            store,
            stream_id,
            position,
        }
    }

    pub async fn read_next(&mut self, batch_size: usize) -> Result<Vec<StoredEvent>> {
        let events = self
            .store
            .read_stream(
                &self.stream_id,
                StreamPosition::Position(self.position),
                batch_size,
            )
            .await?;

        if let Some(last) = events.last() {
            self.position = last.stream_position + 1;
        }

        Ok(events)
    }

    pub fn position(&self) -> u64 {
        self.position
    }

    pub fn stream_id(&self) -> &StreamId {
        &self.stream_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stream_filter_creation() {
        let filter = StreamFilter::new();
        assert!(filter.event_types.is_none());
        assert!(filter.stream_ids.is_none());
    }

    #[test]
    fn test_stream_filter_with_event_types() {
        let filter = StreamFilter::new()
            .with_event_types(vec!["TaskEvent".to_string(), "WorkflowEvent".to_string()]);

        assert_eq!(filter.event_types.as_ref().unwrap().len(), 2);
    }

    #[test]
    fn test_stream_filter_with_stream_ids() {
        let stream_id = StreamId::task("test-123");
        let filter = StreamFilter::new().with_stream_ids(vec![stream_id.clone()]);

        assert_eq!(filter.stream_ids.as_ref().unwrap().len(), 1);
    }

    #[test]
    fn test_stream_config_defaults() {
        let config = StreamConfig::default();

        assert_eq!(config.batch_size, 100);
        assert_eq!(config.buffer_size, 1000);
        assert_eq!(config.start_position, 0);
        assert_eq!(config.follow, false);
    }

    #[test]
    fn test_stream_config_builder() {
        let config = StreamConfig::new()
            .with_batch_size(500)
            .with_buffer_size(5000)
            .from_position(100)
            .follow(true)
            .poll_interval_ms(2000)
            .max_events(10000);

        assert_eq!(config.batch_size, 500);
        assert_eq!(config.buffer_size, 5000);
        assert_eq!(config.start_position, 100);
        assert_eq!(config.follow, true);
        assert_eq!(config.poll_interval_ms, 2000);
        assert_eq!(config.max_events, Some(10000));
    }

    #[test]
    fn test_single_stream_reader_creation() {
        // We can't test the actual reading without a real store,
        // but we can test creation
        let _stream_id = StreamId::task("test-123");
        // Note: This won't compile without a real EventStore implementation
        // This is just to show the API
        // let reader = SingleStreamReader::new(store, stream_id);
        // assert_eq!(reader.position(), 0);
    }

    #[test]
    fn test_stream_filter_matches() {
        let filter = StreamFilter::new().with_event_types(vec!["TaskEvent".to_string()]);

        let event = StoredEvent::new(
            1u64,
            StreamId::task("test-123"),
            "TaskEvent".to_string(),
            serde_json::json!({}),
        );

        assert!(filter.matches(&event));

        let other_event = StoredEvent::new(
            2u64,
            StreamId::task("test-123"),
            "WorkflowEvent".to_string(),
            serde_json::json!({}),
        );

        assert!(!filter.matches(&other_event));
    }
}
