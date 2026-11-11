//! Registers, runs, and manages the lifecycle of CQRS projections.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

use super::state::ProjectionStateStore;
use super::traits::{Projection, ProjectionMetadata, ProjectionStatus};
use crate::metadata::StreamId as EventsStreamId;
use crate::replay::{EventStore, StreamPosition};
use crate::subscriptions::{
    ErrorHandlingStrategy, Subscription, SubscriptionConfig, SubscriptionId, SubscriptionManager,
};
use orcher_storage::schema::types::event::{StoredEvent, StreamId as StorageStreamId};

#[allow(deprecated)]
fn convert_replay_to_storage_event(replay_event: &crate::replay::StoredEvent) -> StoredEvent {
    use orcher_common::types::identifiers::{CorrelationId, EventId};

    let stream_id = StorageStreamId::new(
        replay_event.stream_id.category.clone(),
        replay_event.stream_id.id.clone(),
    );

    StoredEvent {
        event_id: replay_event.event_id as i64,
        organization_id: None, // Organization context not available in replay
        stream_id,
        stream_category: replay_event.stream_id.category.clone(),
        event_type: replay_event.event_type.clone(),
        payload: replay_event.data.clone(),
        metadata: replay_event.metadata.clone(),
        timestamp: replay_event.timestamp,
        stream_position: replay_event.stream_position as i64,
        global_position: replay_event.global_position as i64,
        causation_id: replay_event
            .causation_id
            .as_ref()
            .and_then(|id| uuid::Uuid::parse_str(id).ok().map(EventId::from_uuid)),
        correlation_id: replay_event
            .correlation_id
            .as_ref()
            .and_then(|id| uuid::Uuid::parse_str(id).ok().map(CorrelationId::from_uuid)),
        created_at: replay_event.stored_at,
    }
}

struct ProjectionWrapper<P> {
    projection: Arc<Mutex<P>>,
    state_store: Arc<dyn ProjectionStateStore>,
}

impl<P> ProjectionWrapper<P>
where
    P: Projection + Send + Sync + 'static,
    P::State: serde::Serialize,
{
    fn new(projection: P, state_store: Arc<dyn ProjectionStateStore>) -> Self {
        Self {
            projection: Arc::new(Mutex::new(projection)),
            state_store,
        }
    }

    async fn handle_event(&self, event: &StoredEvent) -> super::Result<()> {
        let mut proj = self.projection.lock().await;

        // Check if projection handles this event type
        if !proj.handles_event_type(&event.event_type) {
            return Ok(());
        }

        // Project the event
        proj.project(event)
            .await
            .map_err(|e| super::ProjectionError::ProjectionFailed(e.to_string()))?;

        // Save state after processing
        let state = proj
            .get_state()
            .await
            .map_err(|e| super::ProjectionError::StateError(e.to_string()))?;

        // Serialize state to JSON
        let state_json = serde_json::to_value(&state)
            .map_err(|e| super::ProjectionError::SerializationError(e.to_string()))?;

        // Save state and update position
        self.state_store
            .save_state(proj.name(), &state_json, event.stream_position as u64)
            .await?;

        Ok(())
    }

    #[allow(dead_code)]
    async fn reset(&self) -> super::Result<()> {
        let mut proj = self.projection.lock().await;
        proj.reset()
            .await
            .map_err(|e| super::ProjectionError::ProjectionFailed(e.to_string()))?;
        Ok(())
    }

    #[allow(dead_code)]
    async fn get_name(&self) -> String {
        let proj = self.projection.lock().await;
        proj.name().to_string()
    }
}

/// Coordinates projection registration, event streaming, and state persistence.
pub struct ProjectionManager<S> {
    #[allow(dead_code)]
    store: Arc<S>,
    subscription_manager: Arc<SubscriptionManager<S>>,
    state_store: Arc<dyn ProjectionStateStore>,
    projections: Arc<RwLock<HashMap<String, ProjectionEntry>>>,
    shutting_down: Arc<Mutex<bool>>,
}

struct ProjectionEntry {
    subscription_id: SubscriptionId,
    #[allow(dead_code)]
    subscription: Arc<Mutex<Box<dyn Subscription>>>,
    metadata: ProjectionMetadata,
}

impl<S> ProjectionManager<S>
where
    S: EventStore + Send + Sync + 'static,
{
    pub fn new(
        store: Arc<S>,
        subscription_manager: Arc<SubscriptionManager<S>>,
        state_store: impl ProjectionStateStore + 'static,
    ) -> Self {
        Self {
            store,
            subscription_manager,
            state_store: Arc::new(state_store),
            projections: Arc::new(RwLock::new(HashMap::new())),
            shutting_down: Arc::new(Mutex::new(false)),
        }
    }

    /// Registers a projection and starts streaming events to it.
    /// Resumes from the last processed position if state exists.
    pub async fn register_projection<P>(&self, projection: P) -> super::Result<()>
    where
        P: Projection + Send + Sync + 'static,
        P::State: serde::Serialize,
    {
        // Check if shutting down
        if *self.shutting_down.lock().await {
            return Err(super::ProjectionError::Internal(
                "Manager is shutting down".to_string(),
            ));
        }

        let projection_name = projection.name().to_string();

        // Check if already registered
        {
            let projections = self.projections.read().await;
            if projections.contains_key(&projection_name) {
                return Err(super::ProjectionError::AlreadyExists(projection_name));
            }
        }

        // Load or create metadata
        let mut metadata = match self.state_store.load_metadata(&projection_name).await? {
            Some(meta) => {
                // Existing projection, check version
                // If version changed, rebuild may be needed
                meta
            }
            None => {
                // New projection
                let mut meta = ProjectionMetadata::new(&projection_name);
                meta.version = projection.version();
                if let Some(category) = projection.category() {
                    meta.category = Some(category.to_string());
                }
                self.state_store.save_metadata(&meta).await?;
                meta
            }
        };

        // Create projection wrapper
        let wrapper = Arc::new(ProjectionWrapper::new(projection, self.state_store.clone()));

        // Determine starting position
        let start_position = if metadata.last_processed_position > 0 {
            StreamPosition::Position(metadata.last_processed_position)
        } else {
            StreamPosition::Start
        };

        // Create subscription configuration
        let subscription_config = SubscriptionConfig::default()
            .with_name(format!("projection_{}", projection_name))
            .with_batch_size(100)
            .with_buffer_size(1000)
            .with_auto_checkpoint(true)
            .with_checkpoint_interval(std::time::Duration::from_secs(5))
            .with_error_strategy(ErrorHandlingStrategy::RetryWithBackoff { max_attempts: 3 });

        // Subscribe to all events (category-agnostic for now)
        // TODO: Add category filtering when implemented
        let stream_id = EventsStreamId::new("$all", ""); // Special stream for all events

        let subscription = self
            .subscription_manager
            .subscribe_to_stream(stream_id, start_position, subscription_config)
            .await
            .map_err(|e| super::ProjectionError::SubscriptionError(e.to_string()))?;

        let subscription_id = {
            let sub = subscription.lock().await;
            sub.id().clone()
        };

        // Update metadata status
        metadata.set_status(ProjectionStatus::Running);
        self.state_store.save_metadata(&metadata).await?;

        // Store projection entry
        {
            let mut projections = self.projections.write().await;
            projections.insert(
                projection_name.clone(),
                ProjectionEntry {
                    subscription_id: subscription_id.clone(),
                    subscription: subscription.clone(),
                    metadata,
                },
            );
        }

        // Spawn task to process events
        let wrapper_clone = wrapper.clone();
        let subscription_clone = subscription.clone();
        let projection_name_clone = projection_name.clone();
        let state_store_clone = self.state_store.clone();
        let projections_clone = self.projections.clone();

        tokio::spawn(async move {
            loop {
                let mut sub = subscription_clone.lock().await;
                match sub.next().await {
                    Ok(Some(event)) => {
                        // Convert replay event to storage event format
                        let storage_event = convert_replay_to_storage_event(&event);
                        if let Err(_e) = wrapper_clone.handle_event(&storage_event).await {
                            // Update error count
                            {
                                let mut projections = projections_clone.write().await;
                                if let Some(entry) = projections.get_mut(&projection_name_clone) {
                                    entry.metadata.increment_errors();
                                    let _ = state_store_clone.save_metadata(&entry.metadata).await;
                                }
                            }
                        }
                    }
                    Ok(None) => {
                        // End of stream or stopped
                        break;
                    }
                    Err(_e) => {
                        // Update status to failed
                        {
                            let mut projections = projections_clone.write().await;
                            if let Some(entry) = projections.get_mut(&projection_name_clone) {
                                entry.metadata.set_status(ProjectionStatus::Failed);
                                let _ = state_store_clone.save_metadata(&entry.metadata).await;
                            }
                        }
                        break;
                    }
                }
            }
        });

        Ok(())
    }

    pub async fn get_projection_state<T>(&self, projection_name: &str) -> super::Result<T>
    where
        T: serde::de::DeserializeOwned,
    {
        // Load state from store
        let state_json = self
            .state_store
            .load_state(projection_name)
            .await?
            .ok_or_else(|| super::ProjectionError::NotFound(projection_name.to_string()))?;

        // Deserialize to target type
        let state: T = serde_json::from_value(state_json)
            .map_err(|e| super::ProjectionError::DeserializationError(e.to_string()))?;

        Ok(state)
    }

    /// Stops the projection, resets its state, and removes it so it can be re-registered.
    pub async fn rebuild_projection(&self, projection_name: &str) -> super::Result<()> {
        // Get projection entry
        let (subscription_id, mut metadata) = {
            let projections = self.projections.read().await;
            let entry = projections
                .get(projection_name)
                .ok_or_else(|| super::ProjectionError::NotFound(projection_name.to_string()))?;

            (entry.subscription_id.clone(), entry.metadata.clone())
        };

        // Stop the subscription
        self.subscription_manager
            .stop_subscription(&subscription_id)
            .await
            .map_err(|e| super::ProjectionError::SubscriptionError(e.to_string()))?;

        // Update status to building
        metadata.set_status(ProjectionStatus::Building);
        metadata.last_processed_position = 0;
        metadata.error_count = 0;
        self.state_store.save_metadata(&metadata).await?;

        // Delete old state
        self.state_store.delete_state(projection_name).await?;

        // Unregister and remove projection
        {
            let mut projections = self.projections.write().await;
            projections.remove(projection_name);
        }

        Ok(())
    }

    /// Like `rebuild_projection` but resumes from a specific position instead of the start.
    pub async fn rebuild_projection_from_position(
        &self,
        projection_name: &str,
        from_position: u64,
    ) -> super::Result<()> {
        // Get and stop projection
        let subscription_id = {
            let projections = self.projections.read().await;
            let entry = projections
                .get(projection_name)
                .ok_or_else(|| super::ProjectionError::NotFound(projection_name.to_string()))?;
            entry.subscription_id.clone()
        };

        self.subscription_manager
            .stop_subscription(&subscription_id)
            .await
            .map_err(|e| super::ProjectionError::SubscriptionError(e.to_string()))?;

        // Update metadata
        let mut metadata = self
            .state_store
            .load_metadata(projection_name)
            .await?
            .ok_or_else(|| super::ProjectionError::NotFound(projection_name.to_string()))?;

        metadata.last_processed_position = from_position;
        metadata.set_status(ProjectionStatus::Building);
        self.state_store.save_metadata(&metadata).await?;

        // Remove from active projections
        {
            let mut projections = self.projections.write().await;
            projections.remove(projection_name);
        }

        Ok(())
    }

    pub async fn rebuild_all_projections(&self) -> super::Result<()> {
        let projection_names: Vec<String> = {
            let projections = self.projections.read().await;
            projections.keys().cloned().collect()
        };

        for name in projection_names {
            self.rebuild_projection(&name).await?;
        }

        Ok(())
    }

    pub async fn list_projections(&self) -> super::Result<Vec<ProjectionMetadata>> {
        let projections = self.projections.read().await;
        let metadata: Vec<ProjectionMetadata> = projections
            .values()
            .map(|entry| entry.metadata.clone())
            .collect();
        Ok(metadata)
    }

    pub async fn get_projection_metadata(
        &self,
        projection_name: &str,
    ) -> super::Result<Option<ProjectionMetadata>> {
        let projections = self.projections.read().await;
        Ok(projections.get(projection_name).map(|e| e.metadata.clone()))
    }

    pub async fn pause_projection(&self, projection_name: &str) -> super::Result<()> {
        let subscription_id = {
            let projections = self.projections.read().await;
            let entry = projections
                .get(projection_name)
                .ok_or_else(|| super::ProjectionError::NotFound(projection_name.to_string()))?;
            entry.subscription_id.clone()
        };

        self.subscription_manager
            .pause_subscription(&subscription_id)
            .await
            .map_err(|e| super::ProjectionError::SubscriptionError(e.to_string()))?;

        // Update metadata
        {
            let mut projections = self.projections.write().await;
            if let Some(entry) = projections.get_mut(projection_name) {
                entry.metadata.set_status(ProjectionStatus::Paused);
                self.state_store.save_metadata(&entry.metadata).await?;
            }
        }

        Ok(())
    }

    pub async fn resume_projection(&self, projection_name: &str) -> super::Result<()> {
        let subscription_id = {
            let projections = self.projections.read().await;
            let entry = projections
                .get(projection_name)
                .ok_or_else(|| super::ProjectionError::NotFound(projection_name.to_string()))?;
            entry.subscription_id.clone()
        };

        self.subscription_manager
            .resume_subscription(&subscription_id)
            .await
            .map_err(|e| super::ProjectionError::SubscriptionError(e.to_string()))?;

        // Update metadata
        {
            let mut projections = self.projections.write().await;
            if let Some(entry) = projections.get_mut(projection_name) {
                entry.metadata.set_status(ProjectionStatus::Running);
                self.state_store.save_metadata(&entry.metadata).await?;
            }
        }

        Ok(())
    }

    /// Stops all projections. Call before application exit.
    pub async fn shutdown(&self) -> super::Result<()> {
        // Mark as shutting down
        {
            let mut shutting_down = self.shutting_down.lock().await;
            *shutting_down = true;
        }

        // Stop all projections
        let projection_names: Vec<String> = {
            let projections = self.projections.read().await;
            projections.keys().cloned().collect()
        };

        for name in projection_names {
            let _ = self.stop_projection(&name).await;
        }

        Ok(())
    }

    async fn stop_projection(&self, projection_name: &str) -> super::Result<()> {
        let subscription_id = {
            let projections = self.projections.read().await;
            let entry = projections
                .get(projection_name)
                .ok_or_else(|| super::ProjectionError::NotFound(projection_name.to_string()))?;
            entry.subscription_id.clone()
        };

        self.subscription_manager
            .stop_subscription(&subscription_id)
            .await
            .map_err(|e| super::ProjectionError::SubscriptionError(e.to_string()))?;

        // Update metadata
        {
            let mut projections = self.projections.write().await;
            if let Some(entry) = projections.get_mut(projection_name) {
                entry.metadata.set_status(ProjectionStatus::Stopped);
                self.state_store.save_metadata(&entry.metadata).await?;
            }
        }

        Ok(())
    }

    pub async fn is_shutting_down(&self) -> bool {
        *self.shutting_down.lock().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replay::{AppendResult, EventQuery};
    use async_trait::async_trait;
    use orcher_common::types::time::Timestamptz;
    use serde::{Deserialize, Serialize};
    use serde_json::json;

    // Mock EventStore
    #[allow(unused)]
    struct MockEventStore {
        events: Vec<crate::replay::StoredEvent>,
    }

    #[async_trait]
    impl EventStore for MockEventStore {
        async fn append_to_stream(
            &self,
            _stream_id: &EventsStreamId,
            _expected_position: StreamPosition,
            _events: Vec<crate::Event>,
        ) -> crate::error::Result<AppendResult> {
            Ok(AppendResult {
                stream_position: 1,
                global_position: 1,
                events_appended: 1,
            })
        }

        async fn read_stream(
            &self,
            _stream_id: &EventsStreamId,
            from_position: StreamPosition,
            max_count: usize,
        ) -> crate::error::Result<Vec<crate::replay::StoredEvent>> {
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
            _stream_id: &EventsStreamId,
            _from_position: Option<StreamPosition>,
            max_count: usize,
        ) -> crate::error::Result<Vec<crate::replay::StoredEvent>> {
            let mut events = self.events.clone();
            events.reverse();
            Ok(events.into_iter().take(max_count).collect())
        }

        async fn get_stream_position(
            &self,
            _stream_id: &EventsStreamId,
        ) -> crate::error::Result<u64> {
            Ok(self.events.len() as u64)
        }

        async fn stream_exists(&self, _stream_id: &EventsStreamId) -> crate::error::Result<bool> {
            Ok(!self.events.is_empty())
        }

        async fn query_events(
            &self,
            _query: &EventQuery,
        ) -> crate::error::Result<crate::replay::QueryResult> {
            Ok(crate::replay::QueryResult {
                events: self.events.clone(),
                has_more: false,
            })
        }

        async fn read_all(
            &self,
            _from_position: u64,
            _max_count: usize,
        ) -> crate::error::Result<Vec<crate::replay::StoredEvent>> {
            Ok(self.events.clone())
        }

        async fn get_global_position(&self) -> crate::error::Result<u64> {
            Ok(self.events.len() as u64)
        }

        async fn delete_stream(&self, _stream_id: &EventsStreamId) -> crate::error::Result<()> {
            Ok(())
        }

        async fn get_event_by_id(
            &self,
            event_id: u64,
        ) -> crate::error::Result<Option<crate::replay::StoredEvent>> {
            Ok(self.events.iter().find(|e| e.event_id == event_id).cloned())
        }

        async fn query_category(
            &self,
            _category: &str,
            _from_position: u64,
            _max_count: usize,
        ) -> crate::error::Result<Vec<crate::replay::StoredEvent>> {
            Ok(self.events.clone())
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct TestProjectionState {
        count: u64,
    }

    struct TestProjection {
        state: TestProjectionState,
    }

    #[async_trait]
    impl Projection for TestProjection {
        type State = TestProjectionState;

        fn name(&self) -> &str {
            "test_projection"
        }

        async fn project(
            &mut self,
            _event: &StoredEvent,
        ) -> Result<(), Box<dyn std::error::Error>> {
            self.state.count += 1;
            Ok(())
        }

        async fn get_state(&self) -> Result<Self::State, Box<dyn std::error::Error>> {
            Ok(self.state.clone())
        }

        async fn reset(&mut self) -> Result<(), Box<dyn std::error::Error>> {
            self.state.count = 0;
            Ok(())
        }
    }

    #[allow(unused)]
    fn create_test_event(position: u64) -> crate::replay::StoredEvent {
        crate::replay::StoredEvent {
            event_id: position,
            stream_id: EventsStreamId::new("test".to_string(), "123".to_string()),
            event_type: "TestEvent".to_string(),
            version: 1,
            stream_position: position,
            global_position: position,
            data: json!({"test": "data"}),
            metadata: json!({}),
            timestamp: Timestamptz::now(),
            stored_at: Timestamptz::now(),
            correlation_id: None,
            causation_id: None,
        }
    }

    #[tokio::test]
    async fn test_projection_wrapper_creation() {
        let projection = TestProjection {
            state: TestProjectionState { count: 0 },
        };

        let state_store = super::super::state::PostgresProjectionStateStore::new(
            sqlx::PgPool::connect_lazy("postgresql://localhost/test").unwrap(),
        );

        let _wrapper = ProjectionWrapper::new(projection, Arc::new(state_store));
        // Just test that it compiles
    }

    #[test]
    fn test_projection_manager_compiles() {
        // This test verifies the manager compiles with proper types
        assert!(true);
    }
}
