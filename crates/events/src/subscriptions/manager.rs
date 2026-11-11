//! Manages active subscriptions and their lifecycles.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

use super::checkpoint::CheckpointStore;
use super::stream::StreamSubscription;
use super::types::{Subscription, SubscriptionConfig, SubscriptionId, SubscriptionStatus};
use crate::metadata::StreamId;
use crate::replay::{EventStore, StreamPosition};

/// Tracks active subscriptions, handles creation/teardown, and coordinates checkpointing.
pub struct SubscriptionManager<S> {
    store: Arc<S>,
    #[allow(clippy::type_complexity)]
    subscriptions: Arc<RwLock<HashMap<SubscriptionId, Arc<Mutex<Box<dyn Subscription>>>>>>,
    checkpoint_store: Arc<dyn CheckpointStore>,
    shutting_down: Arc<Mutex<bool>>,
}

impl<S> SubscriptionManager<S>
where
    S: EventStore + Send + Sync + 'static,
{
    pub fn new(store: S, checkpoint_store: impl CheckpointStore + 'static) -> Self {
        Self {
            store: Arc::new(store),
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            checkpoint_store: Arc::new(checkpoint_store),
            shutting_down: Arc::new(Mutex::new(false)),
        }
    }

    pub fn with_arc_store(store: Arc<S>, checkpoint_store: impl CheckpointStore + 'static) -> Self {
        Self {
            store,
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            checkpoint_store: Arc::new(checkpoint_store),
            shutting_down: Arc::new(Mutex::new(false)),
        }
    }

    /// Creates a subscription that reads from the given stream, resuming from
    /// checkpoint if auto-checkpoint is enabled.
    pub async fn subscribe_to_stream(
        &self,
        stream_id: StreamId,
        start_position: StreamPosition,
        config: SubscriptionConfig,
    ) -> super::Result<Arc<Mutex<Box<dyn Subscription>>>> {
        // Check if shutting down
        if *self.shutting_down.lock().await {
            return Err(super::SubscriptionError::Internal(
                "Manager is shutting down".to_string(),
            ));
        }

        // Validate configuration
        config
            .validate()
            .map_err(super::SubscriptionError::Internal)?;

        // Generate subscription ID
        let subscription_id = if let Some(name) = &config.name {
            SubscriptionId::new(name.clone())
        } else {
            SubscriptionId::generate()
        };

        // Check if subscription already exists
        {
            let subscriptions = self.subscriptions.read().await;
            if subscriptions.contains_key(&subscription_id) {
                return Err(super::SubscriptionError::AlreadyExists(
                    subscription_id.to_string(),
                ));
            }
        }

        // Create configuration with the determined start position
        let mut effective_config = config.clone();
        effective_config.start_position = start_position;

        // Create subscription
        let mut subscription = StreamSubscription::new(
            subscription_id.clone(),
            self.store.clone(),
            stream_id,
            effective_config,
        )
        .with_checkpoint_store(self.checkpoint_store.clone());

        // Try to load checkpoint if auto-checkpoint is enabled
        if config.auto_checkpoint {
            if let Err(_e) = subscription.load_checkpoint().await {
                // Failed to load checkpoint, starting from configured position
            }
        }

        // Wrap in Arc<Mutex<Box<dyn Subscription>>>
        let subscription: Arc<Mutex<Box<dyn Subscription>>> =
            Arc::new(Mutex::new(Box::new(subscription)));

        // Register subscription
        {
            let mut subscriptions = self.subscriptions.write().await;
            subscriptions.insert(subscription_id.clone(), subscription.clone());
        }

        Ok(subscription)
    }

    /// Stops and removes a subscription, saving its checkpoint.
    pub async fn unsubscribe(&self, subscription_id: &SubscriptionId) -> super::Result<()> {
        // Get and remove subscription
        let subscription = {
            let mut subscriptions = self.subscriptions.write().await;
            subscriptions.remove(subscription_id)
        };

        if let Some(subscription) = subscription {
            // Stop the subscription
            let mut sub = subscription.lock().await;
            sub.stop().await?;

            Ok(())
        } else {
            Err(super::SubscriptionError::NotFound(
                subscription_id.to_string(),
            ))
        }
    }

    pub async fn get_subscription(
        &self,
        subscription_id: &SubscriptionId,
    ) -> Option<Arc<Mutex<Box<dyn Subscription>>>> {
        let subscriptions = self.subscriptions.read().await;
        subscriptions.get(subscription_id).cloned()
    }

    pub async fn list_subscriptions(&self) -> Vec<SubscriptionId> {
        let subscriptions = self.subscriptions.read().await;
        subscriptions.keys().cloned().collect()
    }

    pub async fn subscription_count(&self) -> usize {
        let subscriptions = self.subscriptions.read().await;
        subscriptions.len()
    }

    pub async fn get_all_statuses(&self) -> HashMap<SubscriptionId, SubscriptionStatus> {
        let subscriptions = self.subscriptions.read().await;
        let mut statuses = HashMap::new();

        for (id, subscription) in subscriptions.iter() {
            let sub = subscription.lock().await;
            statuses.insert(id.clone(), sub.status());
        }

        statuses
    }

    pub async fn pause_subscription(&self, subscription_id: &SubscriptionId) -> super::Result<()> {
        let subscription = self
            .get_subscription(subscription_id)
            .await
            .ok_or_else(|| super::SubscriptionError::NotFound(subscription_id.to_string()))?;

        let mut sub = subscription.lock().await;
        sub.pause().await?;

        Ok(())
    }

    pub async fn resume_subscription(&self, subscription_id: &SubscriptionId) -> super::Result<()> {
        let subscription = self
            .get_subscription(subscription_id)
            .await
            .ok_or_else(|| super::SubscriptionError::NotFound(subscription_id.to_string()))?;

        let mut sub = subscription.lock().await;
        sub.resume().await?;

        Ok(())
    }

    pub async fn stop_subscription(&self, subscription_id: &SubscriptionId) -> super::Result<()> {
        let subscription = self
            .get_subscription(subscription_id)
            .await
            .ok_or_else(|| super::SubscriptionError::NotFound(subscription_id.to_string()))?;

        let mut sub = subscription.lock().await;
        sub.stop().await?;

        Ok(())
    }

    /// Stops all subscriptions. Call before application exit.
    pub async fn shutdown(&self) -> super::Result<()> {
        // Mark as shutting down
        {
            let mut shutting_down = self.shutting_down.lock().await;
            *shutting_down = true;
        }

        // Stop all subscriptions
        let subscription_ids: Vec<SubscriptionId> = {
            let subscriptions = self.subscriptions.read().await;
            subscriptions.keys().cloned().collect()
        };

        for subscription_id in subscription_ids {
            let _ = self.unsubscribe(&subscription_id).await;
        }

        Ok(())
    }

    pub async fn is_shutting_down(&self) -> bool {
        *self.shutting_down.lock().await
    }

    /// Removes subscriptions in terminal states (Stopped or Failed).
    pub async fn cleanup_stopped_subscriptions(&self) -> usize {
        let mut to_remove = Vec::new();

        // Identify stopped subscriptions
        {
            let subscriptions = self.subscriptions.read().await;
            for (id, subscription) in subscriptions.iter() {
                let sub = subscription.lock().await;
                let status = sub.status();
                if status == SubscriptionStatus::Stopped || status == SubscriptionStatus::Failed {
                    to_remove.push(id.clone());
                }
            }
        }

        // Remove them
        let count = to_remove.len();
        {
            let mut subscriptions = self.subscriptions.write().await;
            for id in to_remove {
                subscriptions.remove(&id);
            }
        }

        count
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replay::{AppendResult, StoredEvent};
    use crate::subscriptions::checkpoint::CheckpointStore;
    use async_trait::async_trait;
    use orcher_common::types::time::Timestamptz;
    use serde_json::json;
    use std::sync::Arc;

    // Mock EventStore
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

    // Mock CheckpointStore
    struct MockCheckpointStore {
        checkpoints: Arc<Mutex<HashMap<String, StreamPosition>>>,
    }

    impl MockCheckpointStore {
        fn new() -> Self {
            Self {
                checkpoints: Arc::new(Mutex::new(HashMap::new())),
            }
        }
    }

    #[async_trait]
    impl CheckpointStore for MockCheckpointStore {
        async fn save_checkpoint(
            &self,
            subscription_id: &str,
            position: StreamPosition,
        ) -> super::super::Result<()> {
            let mut checkpoints = self.checkpoints.lock().await;
            checkpoints.insert(subscription_id.to_string(), position);
            Ok(())
        }

        async fn load_checkpoint(
            &self,
            subscription_id: &str,
        ) -> super::super::Result<Option<StreamPosition>> {
            let checkpoints = self.checkpoints.lock().await;
            Ok(checkpoints.get(subscription_id).cloned())
        }

        async fn delete_checkpoint(&self, subscription_id: &str) -> super::super::Result<()> {
            let mut checkpoints = self.checkpoints.lock().await;
            checkpoints.remove(subscription_id);
            Ok(())
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
    async fn test_manager_creation() {
        let store = MockEventStore { events: vec![] };
        let checkpoint_store = MockCheckpointStore::new();
        let manager = SubscriptionManager::new(store, checkpoint_store);

        assert_eq!(manager.subscription_count().await, 0);
    }

    #[tokio::test]
    async fn test_subscribe_to_stream() {
        let store = MockEventStore {
            events: vec![create_test_event(0)],
        };
        let checkpoint_store = MockCheckpointStore::new();
        let manager = SubscriptionManager::new(store, checkpoint_store);

        let stream_id = StreamId::new("test", "123");
        let config = SubscriptionConfig::default().with_name("test-sub");

        let subscription = manager
            .subscribe_to_stream(stream_id, StreamPosition::Start, config)
            .await
            .unwrap();

        assert_eq!(manager.subscription_count().await, 1);
        assert!(subscription.lock().await.id().as_str().contains("test-sub"));
    }

    #[tokio::test]
    async fn test_duplicate_subscription_fails() {
        let store = MockEventStore { events: vec![] };
        let checkpoint_store = MockCheckpointStore::new();
        let manager = SubscriptionManager::new(store, checkpoint_store);

        let stream_id = StreamId::new("test", "123");
        let config = SubscriptionConfig::default().with_name("test-sub");

        // First subscription should succeed
        manager
            .subscribe_to_stream(stream_id.clone(), StreamPosition::Start, config.clone())
            .await
            .unwrap();

        // Second subscription with same name should fail
        let result = manager
            .subscribe_to_stream(stream_id, StreamPosition::Start, config)
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_unsubscribe() {
        let store = MockEventStore {
            events: vec![create_test_event(0)],
        };
        let checkpoint_store = MockCheckpointStore::new();
        let manager = SubscriptionManager::new(store, checkpoint_store);

        let stream_id = StreamId::new("test", "123");
        let config = SubscriptionConfig::default().with_name("test-sub");

        let subscription = manager
            .subscribe_to_stream(stream_id, StreamPosition::Start, config)
            .await
            .unwrap();

        let subscription_id = subscription.lock().await.id().clone();

        assert_eq!(manager.subscription_count().await, 1);

        // Unsubscribe
        manager.unsubscribe(&subscription_id).await.unwrap();

        assert_eq!(manager.subscription_count().await, 0);
    }

    #[tokio::test]
    async fn test_list_subscriptions() {
        let store = MockEventStore {
            events: vec![create_test_event(0)],
        };
        let checkpoint_store = MockCheckpointStore::new();
        let manager = SubscriptionManager::new(store, checkpoint_store);

        let stream_id = StreamId::new("test", "123");

        // Create multiple subscriptions
        manager
            .subscribe_to_stream(
                stream_id.clone(),
                StreamPosition::Start,
                SubscriptionConfig::default().with_name("sub1"),
            )
            .await
            .unwrap();

        manager
            .subscribe_to_stream(
                stream_id,
                StreamPosition::Start,
                SubscriptionConfig::default().with_name("sub2"),
            )
            .await
            .unwrap();

        let subscriptions = manager.list_subscriptions().await;
        assert_eq!(subscriptions.len(), 2);
    }

    #[tokio::test]
    async fn test_shutdown() {
        let store = MockEventStore {
            events: vec![create_test_event(0)],
        };
        let checkpoint_store = MockCheckpointStore::new();
        let manager = SubscriptionManager::new(store, checkpoint_store);

        let stream_id = StreamId::new("test", "123");
        let config = SubscriptionConfig::default().with_name("test-sub");

        manager
            .subscribe_to_stream(stream_id, StreamPosition::Start, config)
            .await
            .unwrap();

        assert_eq!(manager.subscription_count().await, 1);

        // Shutdown
        manager.shutdown().await.unwrap();

        assert_eq!(manager.subscription_count().await, 0);
        assert!(manager.is_shutting_down().await);
    }
}
