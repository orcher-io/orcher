//! Buffered stream subscription with automatic checkpointing.

use async_trait::async_trait;

use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::Mutex;

use super::checkpoint::CheckpointStore;
use super::types::{
    ErrorHandlingStrategy, Subscription, SubscriptionConfig, SubscriptionId, SubscriptionStatus,
};
use crate::metadata::StreamId;
use crate::replay::{EventStore, StoredEvent, StreamPosition};

/// Reads events from a single stream with buffering, checkpointing, and error handling.
pub struct StreamSubscription<S> {
    id: SubscriptionId,
    store: Arc<S>,
    stream_id: StreamId,
    position: StreamPosition,
    status: SubscriptionStatus,
    config: SubscriptionConfig,
    buffer: Arc<Mutex<VecDeque<StoredEvent>>>,
    checkpoint_store: Option<Arc<dyn CheckpointStore>>,
    last_checkpoint: std::time::Instant,
    events_since_checkpoint: usize,
    error_count: u32,
}

impl<S> StreamSubscription<S>
where
    S: EventStore + Send + Sync + 'static,
{
    pub fn new(
        id: SubscriptionId,
        store: Arc<S>,
        stream_id: StreamId,
        config: SubscriptionConfig,
    ) -> Self {
        Self {
            id,
            store,
            stream_id,
            position: config.start_position,
            status: SubscriptionStatus::Starting,
            config,
            buffer: Arc::new(Mutex::new(VecDeque::new())),
            checkpoint_store: None,
            last_checkpoint: std::time::Instant::now(),
            events_since_checkpoint: 0,
            error_count: 0,
        }
    }

    pub fn with_checkpoint_store(mut self, checkpoint_store: Arc<dyn CheckpointStore>) -> Self {
        self.checkpoint_store = Some(checkpoint_store);
        self
    }

    async fn fill_buffer(&mut self) -> super::Result<bool> {
        let mut buffer = self.buffer.lock().await;

        // Don't fetch if buffer is full
        if buffer.len() >= self.config.buffer_size {
            return Ok(false);
        }

        // Calculate how many events to fetch
        let fetch_count = self
            .config
            .batch_size
            .min(self.config.buffer_size - buffer.len());

        // Read events from the store
        let events = self
            .store
            .read_stream(&self.stream_id, self.position, fetch_count)
            .await
            .map_err(|e| super::SubscriptionError::StreamError(e.to_string()))?;

        if events.is_empty() {
            return Ok(false);
        }

        // Update position to the last event
        if let Some(last_event) = events.last() {
            self.position = StreamPosition::Position(last_event.stream_position + 1);
        }

        // Add events to buffer
        for event in events {
            buffer.push_back(event);
        }

        Ok(true)
    }

    async fn maybe_checkpoint(&mut self) -> super::Result<()> {
        if !self.config.auto_checkpoint {
            return Ok(());
        }

        let should_checkpoint = self.last_checkpoint.elapsed() >= self.config.checkpoint_interval
            || self.events_since_checkpoint >= self.config.batch_size;

        if should_checkpoint {
            self.save_checkpoint().await?;
        }

        Ok(())
    }

    async fn save_checkpoint(&mut self) -> super::Result<()> {
        if let Some(checkpoint_store) = &self.checkpoint_store {
            let subscription_id = if let Some(name) = &self.config.name {
                name.as_str()
            } else {
                self.id.as_str()
            };

            checkpoint_store
                .save_checkpoint(subscription_id, self.position)
                .await?;

            self.last_checkpoint = std::time::Instant::now();
            self.events_since_checkpoint = 0;
        }

        Ok(())
    }

    pub async fn load_checkpoint(&mut self) -> super::Result<()> {
        if let Some(checkpoint_store) = &self.checkpoint_store {
            let subscription_id = if let Some(name) = &self.config.name {
                name.as_str()
            } else {
                self.id.as_str()
            };

            if let Some(position) = checkpoint_store.load_checkpoint(subscription_id).await? {
                self.position = position;
            }
        }

        Ok(())
    }

    async fn handle_error(&mut self, error: super::SubscriptionError) -> super::Result<bool> {
        match &self.config.error_strategy {
            ErrorHandlingStrategy::Stop => {
                self.status = SubscriptionStatus::Failed;
                Err(error)
            }
            ErrorHandlingStrategy::Skip => {
                self.error_count += 1;
                Ok(true) // Continue processing
            }
            ErrorHandlingStrategy::RetryWithBackoff { max_attempts } => {
                if self.error_count >= *max_attempts {
                    self.status = SubscriptionStatus::Failed;
                    return Err(super::SubscriptionError::ProcessingError(format!(
                        "Max retry attempts ({}) exceeded: {}",
                        max_attempts, error
                    )));
                }

                // Calculate backoff delay
                let delay_ms = 100 * 2u64.pow(self.error_count);
                let delay = std::time::Duration::from_millis(delay_ms.min(30_000));

                tokio::time::sleep(delay).await;
                self.error_count += 1;
                Ok(true) // Retry
            }
            ErrorHandlingStrategy::Park => {
                self.status = SubscriptionStatus::Paused;
                Ok(false) // Stop processing but don't fail
            }
        }
    }
}

#[async_trait]
impl<S> Subscription for StreamSubscription<S>
where
    S: EventStore + Send + Sync + 'static,
{
    async fn next(&mut self) -> super::Result<Option<StoredEvent>> {
        if self.status == SubscriptionStatus::Stopped || self.status == SubscriptionStatus::Failed {
            return Ok(None);
        }

        if self.status == SubscriptionStatus::Starting {
            self.status = SubscriptionStatus::Running;
        }

        if self.status == SubscriptionStatus::Paused {
            return Ok(None);
        }

        loop {
            // Try to get event from buffer
            {
                let mut buffer = self.buffer.lock().await;
                if let Some(event) = buffer.pop_front() {
                    self.events_since_checkpoint += 1;
                    self.error_count = 0; // Reset error count on success

                    // Maybe save checkpoint
                    drop(buffer); // Release lock before checkpoint
                    self.maybe_checkpoint().await?;

                    return Ok(Some(event));
                }
            }

            // Buffer is empty, try to fill it
            match self.fill_buffer().await {
                Ok(true) => {
                    // Successfully fetched events, continue loop to return one
                    continue;
                }
                Ok(false) => {
                    // No more events available
                    if let Some(timeout) = self.config.read_timeout {
                        // Wait for timeout before trying again
                        tokio::time::sleep(timeout).await;
                        continue;
                    } else {
                        // No timeout, end of stream
                        return Ok(None);
                    }
                }
                Err(e) => {
                    // Error fetching events, handle according to strategy
                    match self.handle_error(e).await {
                        Ok(true) => continue,         // Retry or skip
                        Ok(false) => return Ok(None), // Parked
                        Err(e) => return Err(e),      // Fatal error
                    }
                }
            }
        }
    }

    fn id(&self) -> &SubscriptionId {
        &self.id
    }

    fn current_position(&self) -> StreamPosition {
        self.position
    }

    fn status(&self) -> SubscriptionStatus {
        self.status
    }

    async fn pause(&mut self) -> super::Result<()> {
        if self.status != SubscriptionStatus::Running {
            return Err(super::SubscriptionError::InvalidState {
                expected: "Running".to_string(),
                actual: format!("{:?}", self.status),
            });
        }

        self.status = SubscriptionStatus::Paused;

        // Save checkpoint when pausing
        self.save_checkpoint().await?;

        Ok(())
    }

    async fn resume(&mut self) -> super::Result<()> {
        if self.status != SubscriptionStatus::Paused {
            return Err(super::SubscriptionError::InvalidState {
                expected: "Paused".to_string(),
                actual: format!("{:?}", self.status),
            });
        }

        self.status = SubscriptionStatus::Running;
        self.error_count = 0; // Reset error count on resume

        Ok(())
    }

    async fn stop(&mut self) -> super::Result<()> {
        if self.status == SubscriptionStatus::Stopped {
            return Ok(());
        }

        self.status = SubscriptionStatus::Stopping;

        // Save final checkpoint
        self.save_checkpoint().await?;

        // Clear buffer
        {
            let mut buffer = self.buffer.lock().await;
            buffer.clear();
        }

        self.status = SubscriptionStatus::Stopped;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replay::AppendResult;
    use orcher_common::types::time::Timestamptz;
    use serde_json::json;

    // Mock EventStore for testing
    struct MockEventStore {
        events: Vec<StoredEvent>,
    }

    #[async_trait]
    impl EventStore for MockEventStore {
        async fn append_to_stream(
            &self,
            _stream_id: &StreamId,
            _expected_position: StreamPosition,
            _events: Vec<crate::Event>,
        ) -> Result<AppendResult, crate::error::EventError> {
            unimplemented!()
        }

        async fn read_stream(
            &self,
            _stream_id: &StreamId,
            from_position: StreamPosition,
            max_count: usize,
        ) -> Result<Vec<StoredEvent>, crate::error::EventError> {
            let start = match from_position {
                StreamPosition::Start => 0,
                StreamPosition::Position(pos) => pos as usize,
                StreamPosition::End => self.events.len(),
            };

            Ok(self
                .events
                .iter()
                .skip(start)
                .take(max_count)
                .cloned()
                .collect())
        }

        async fn read_stream_backwards(
            &self,
            _stream_id: &StreamId,
            _from_position: Option<StreamPosition>,
            _max_count: usize,
        ) -> Result<Vec<StoredEvent>, crate::error::EventError> {
            unimplemented!()
        }

        async fn get_stream_position(
            &self,
            _stream_id: &StreamId,
        ) -> Result<u64, crate::error::EventError> {
            Ok(self.events.len() as u64)
        }

        async fn stream_exists(
            &self,
            _stream_id: &StreamId,
        ) -> Result<bool, crate::error::EventError> {
            Ok(!self.events.is_empty())
        }

        async fn query_events(
            &self,
            _query: &crate::replay::store::EventQuery,
        ) -> Result<crate::replay::store::QueryResult, crate::error::EventError> {
            unimplemented!()
        }

        async fn read_all(
            &self,
            _from_position: u64,
            _max_count: usize,
        ) -> Result<Vec<StoredEvent>, crate::error::EventError> {
            Ok(self.events.clone())
        }

        async fn get_global_position(&self) -> Result<u64, crate::error::EventError> {
            Ok(self.events.len() as u64)
        }

        async fn delete_stream(
            &self,
            _stream_id: &StreamId,
        ) -> Result<(), crate::error::EventError> {
            unimplemented!()
        }

        async fn get_event_by_id(
            &self,
            _event_id: u64,
        ) -> Result<Option<StoredEvent>, crate::error::EventError> {
            unimplemented!()
        }

        async fn query_category(
            &self,
            _category: &str,
            _from_position: u64,
            _max_count: usize,
        ) -> Result<Vec<StoredEvent>, crate::error::EventError> {
            unimplemented!()
        }
    }

    fn create_test_event(stream_position: u64) -> StoredEvent {
        StoredEvent {
            event_id: stream_position,
            stream_id: StreamId::new("test", "123"),
            event_type: "TestEvent".to_string(),
            version: 1,
            stream_position,
            global_position: stream_position,
            data: json!({"test": "data"}),
            metadata: json!({}),
            timestamp: Timestamptz::now(),
            stored_at: Timestamptz::now(),
            correlation_id: None,
            causation_id: None,
        }
    }

    #[tokio::test]
    async fn test_subscription_creation() {
        let store = Arc::new(MockEventStore { events: vec![] });
        let stream_id = StreamId::new("test", "123");
        let config = SubscriptionConfig::default();

        let subscription =
            StreamSubscription::new(SubscriptionId::generate(), store, stream_id, config);

        assert_eq!(subscription.status(), SubscriptionStatus::Starting);
    }

    #[tokio::test]
    async fn test_subscription_reads_events() {
        let events = vec![
            create_test_event(0),
            create_test_event(1),
            create_test_event(2),
        ];

        let store = Arc::new(MockEventStore { events });
        let stream_id = StreamId::new("test", "123");
        let config = SubscriptionConfig::default().with_batch_size(2);

        let mut subscription =
            StreamSubscription::new(SubscriptionId::generate(), store, stream_id, config);

        // Should read first event
        let event = subscription.next().await.unwrap();
        assert!(event.is_some());
        assert_eq!(event.unwrap().stream_position, 0);

        // Should read second event
        let event = subscription.next().await.unwrap();
        assert!(event.is_some());
        assert_eq!(event.unwrap().stream_position, 1);
    }

    #[tokio::test]
    async fn test_subscription_pause_resume() {
        let store = Arc::new(MockEventStore {
            events: vec![create_test_event(0)],
        });
        let stream_id = StreamId::new("test", "123");
        let config = SubscriptionConfig::default();

        let mut subscription =
            StreamSubscription::new(SubscriptionId::generate(), store, stream_id, config);

        // Read first event to transition to Running
        let _ = subscription.next().await.unwrap();
        assert_eq!(subscription.status(), SubscriptionStatus::Running);

        // Pause
        subscription.pause().await.unwrap();
        assert_eq!(subscription.status(), SubscriptionStatus::Paused);

        // Next should return None when paused
        let event = subscription.next().await.unwrap();
        assert!(event.is_none());

        // Resume
        subscription.resume().await.unwrap();
        assert_eq!(subscription.status(), SubscriptionStatus::Running);
    }

    #[tokio::test]
    async fn test_subscription_stop() {
        let store = Arc::new(MockEventStore {
            events: vec![create_test_event(0)],
        });
        let stream_id = StreamId::new("test", "123");
        let config = SubscriptionConfig::default();

        let mut subscription =
            StreamSubscription::new(SubscriptionId::generate(), store, stream_id, config);

        // Stop subscription
        subscription.stop().await.unwrap();
        assert_eq!(subscription.status(), SubscriptionStatus::Stopped);

        // Next should return None when stopped
        let event = subscription.next().await.unwrap();
        assert!(event.is_none());
    }

    #[tokio::test]
    async fn test_subscription_position_tracking() {
        let events = vec![
            create_test_event(0),
            create_test_event(1),
            create_test_event(2),
        ];

        let store = Arc::new(MockEventStore { events });
        let stream_id = StreamId::new("test", "123");
        let config = SubscriptionConfig::default();

        let mut subscription =
            StreamSubscription::new(SubscriptionId::generate(), store, stream_id, config);

        // Initial position
        assert_eq!(subscription.current_position(), StreamPosition::Start);

        // After reading events, position should advance
        subscription.next().await.unwrap();
        subscription.next().await.unwrap();

        // Position should be after the last read event
        match subscription.current_position() {
            StreamPosition::Position(pos) => assert!(pos > 0),
            _ => panic!("Expected Position variant"),
        }
    }
}
