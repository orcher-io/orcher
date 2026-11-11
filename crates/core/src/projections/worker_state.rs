//! Worker state projection.

use async_trait::async_trait;
use dashmap::DashMap;
use orcher_common::types::identifiers::{AggregateId, WorkerId};
use orcher_common::types::time::Timestamptz;
use orcher_events::projections::Projection;
use orcher_storage::schema::types::event::StoredEvent;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Worker state information stored in the projection
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WorkerState {
    /// Worker identifier
    pub worker_id: WorkerId,
    /// Current status
    pub status: WorkerStatus,
    /// Worker capabilities
    pub capabilities: Vec<String>,
    /// Registration timestamp
    pub registered_at: Timestamptz,
    /// Last heartbeat timestamp
    pub last_heartbeat: Option<Timestamptz>,
    /// Current load (0.0 to 1.0)
    pub load: f64,
    /// Available capacity
    pub available_capacity: f64,
    /// Whether worker is healthy
    pub healthy: bool,
    /// Reason for current state (if unhealthy or offline)
    pub state_reason: Option<String>,
}

/// Worker status in the projection
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum WorkerStatus {
    /// Worker is registered and ready
    Registered,
    /// Worker is online and accepting tasks
    Online,
    /// Worker is offline
    Offline,
    /// Worker is overloaded
    Overloaded,
    /// Worker has been deregistered
    Deregistered,
}

/// In-memory projection of worker states
#[derive(Clone)]
pub struct WorkerStateProjection {
    /// Map of worker ID to worker state
    states: Arc<DashMap<WorkerId, WorkerState>>,
    /// Projection name
    name: String,
    /// Last processed sequence number
    last_sequence: Arc<RwLock<i64>>,
}

impl WorkerStateProjection {
    /// Create a new worker state projection
    pub fn new() -> Self {
        Self {
            states: Arc::new(DashMap::new()),
            name: "worker_state".to_string(),
            last_sequence: Arc::new(RwLock::new(0)),
        }
    }

    /// Get the current state of a worker
    pub fn get_worker_state(&self, worker_id: &WorkerId) -> Option<WorkerState> {
        self.states
            .get(worker_id)
            .map(|entry| entry.value().clone())
    }

    /// Get all workers with a specific status
    pub fn get_workers_by_status(&self, status: WorkerStatus) -> Vec<WorkerState> {
        self.states
            .iter()
            .filter(|entry| entry.value().status == status)
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Get all healthy workers
    pub fn get_healthy_workers(&self) -> Vec<WorkerState> {
        self.states
            .iter()
            .filter(|entry| entry.value().healthy && entry.value().status != WorkerStatus::Offline)
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Get workers with available capacity
    pub fn get_available_workers(&self, min_capacity: f64) -> Vec<WorkerState> {
        self.states
            .iter()
            .filter(|entry| {
                let state = entry.value();
                state.healthy
                    && state.status == WorkerStatus::Online
                    && state.available_capacity >= min_capacity
            })
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Get the count of workers by status
    pub fn count_by_status(&self, status: WorkerStatus) -> usize {
        self.states
            .iter()
            .filter(|entry| entry.value().status == status)
            .count()
    }

    /// Get total number of workers in the projection
    pub fn total_workers(&self) -> usize {
        self.states.len()
    }

    /// Get all workers as a vector of (WorkerId, WorkerState) tuples
    pub fn get_all_workers(&self) -> Vec<(WorkerId, WorkerState)> {
        self.states
            .iter()
            .map(|entry| (*entry.key(), entry.value().clone()))
            .collect()
    }

    /// Get average load across all online workers
    pub fn average_load(&self) -> f64 {
        let online_workers: Vec<_> = self
            .states
            .iter()
            .filter(|entry| entry.value().status == WorkerStatus::Online)
            .collect();

        if online_workers.is_empty() {
            return 0.0;
        }

        let total_load: f64 = online_workers.iter().map(|w| w.value().load).sum();
        total_load / online_workers.len() as f64
    }

    /// Get all worker states as a HashMap
    fn get_all_states(&self) -> HashMap<WorkerId, WorkerState> {
        self.states
            .iter()
            .map(|entry| (*entry.key(), entry.value().clone()))
            .collect()
    }

    /// Handle WorkerRegistered event
    fn handle_worker_registered(
        &mut self,
        event: &StoredEvent,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(AggregateId::Worker(worker_id)) = event.stream_id.to_aggregate_id() {
            let capabilities = event
                .payload
                .get("capabilities")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();

            let state = WorkerState {
                worker_id,
                status: WorkerStatus::Registered,
                capabilities,
                registered_at: event.timestamp,
                last_heartbeat: None,
                load: 0.0,
                available_capacity: 1.0,
                healthy: true,
                state_reason: None,
            };

            self.states.insert(worker_id, state);
        }
        Ok(())
    }

    /// Handle WorkerHeartbeat event
    fn handle_worker_heartbeat(
        &mut self,
        event: &StoredEvent,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(AggregateId::Worker(worker_id)) = event.stream_id.to_aggregate_id() {
            if let Some(mut entry) = self.states.get_mut(&worker_id) {
                entry.status = WorkerStatus::Online;
                entry.last_heartbeat = Some(event.timestamp);

                if let Some(load) = event.payload.get("load").and_then(|v| v.as_f64()) {
                    entry.load = load;
                }
            }
        }
        Ok(())
    }

    /// Handle WorkerHealthChanged event
    fn handle_worker_health_changed(
        &mut self,
        event: &StoredEvent,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(AggregateId::Worker(worker_id)) = event.stream_id.to_aggregate_id() {
            if let Some(mut entry) = self.states.get_mut(&worker_id) {
                let healthy = event
                    .payload
                    .get("healthy")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(true);

                entry.healthy = healthy;
                entry.state_reason = event
                    .payload
                    .get("reason")
                    .and_then(|v| v.as_str())
                    .map(String::from);
            }
        }
        Ok(())
    }

    /// Handle WorkerOffline event
    fn handle_worker_offline(
        &mut self,
        event: &StoredEvent,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(AggregateId::Worker(worker_id)) = event.stream_id.to_aggregate_id() {
            if let Some(mut entry) = self.states.get_mut(&worker_id) {
                entry.status = WorkerStatus::Offline;
                entry.healthy = false;
                entry.state_reason = event
                    .payload
                    .get("reason")
                    .and_then(|v| v.as_str())
                    .map(String::from);
            }
        }
        Ok(())
    }

    /// Handle WorkerLoadUpdated event
    fn handle_worker_load_updated(
        &mut self,
        event: &StoredEvent,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(AggregateId::Worker(worker_id)) = event.stream_id.to_aggregate_id() {
            if let Some(mut entry) = self.states.get_mut(&worker_id) {
                if let Some(load) = event.payload.get("load").and_then(|v| v.as_f64()) {
                    entry.load = load;
                }
                if let Some(capacity) = event
                    .payload
                    .get("available_capacity")
                    .and_then(|v| v.as_f64())
                {
                    entry.available_capacity = capacity;
                }
            }
        }
        Ok(())
    }

    /// Handle WorkerDeregistered event
    fn handle_worker_deregistered(
        &mut self,
        event: &StoredEvent,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(AggregateId::Worker(worker_id)) = event.stream_id.to_aggregate_id() {
            if let Some(mut entry) = self.states.get_mut(&worker_id) {
                entry.status = WorkerStatus::Deregistered;
                entry.healthy = false;
                entry.state_reason = event
                    .payload
                    .get("reason")
                    .and_then(|v| v.as_str())
                    .map(String::from);
            }
        }
        Ok(())
    }

    /// Handle WorkerOverloaded event
    fn handle_worker_overloaded(
        &mut self,
        event: &StoredEvent,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(AggregateId::Worker(worker_id)) = event.stream_id.to_aggregate_id() {
            if let Some(mut entry) = self.states.get_mut(&worker_id) {
                entry.status = WorkerStatus::Overloaded;
                if let Some(load) = event.payload.get("current_load").and_then(|v| v.as_f64()) {
                    entry.load = load;
                }
            }
        }
        Ok(())
    }
}

impl Default for WorkerStateProjection {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Projection for WorkerStateProjection {
    type State = HashMap<WorkerId, WorkerState>;

    fn name(&self) -> &str {
        &self.name
    }

    async fn project(&mut self, event: &StoredEvent) -> Result<(), Box<dyn std::error::Error>> {
        // Route event to appropriate handler based on event type
        match event.event_type.as_str() {
            "WorkerRegistered" => self.handle_worker_registered(event)?,
            "WorkerHeartbeat" => self.handle_worker_heartbeat(event)?,
            "WorkerHealthChanged" => self.handle_worker_health_changed(event)?,
            "WorkerOffline" => self.handle_worker_offline(event)?,
            "WorkerLoadUpdated" => self.handle_worker_load_updated(event)?,
            "WorkerDeregistered" => self.handle_worker_deregistered(event)?,
            "WorkerOverloaded" => self.handle_worker_overloaded(event)?,
            _ => {} // Ignore unknown event types
        }

        // Update last processed sequence
        let mut last_seq = self.last_sequence.write().await;
        *last_seq = event.stream_position;

        Ok(())
    }

    async fn get_state(&self) -> Result<Self::State, Box<dyn std::error::Error>> {
        Ok(self.get_all_states())
    }

    async fn reset(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.states.clear();
        Ok(())
    }

    fn handles_event_type(&self, event_type: &str) -> bool {
        matches!(
            event_type,
            "WorkerRegistered"
                | "WorkerHeartbeat"
                | "WorkerHealthChanged"
                | "WorkerOffline"
                | "WorkerLoadUpdated"
                | "WorkerDeregistered"
                | "WorkerOverloaded"
                | "WorkerCapacityRestored"
                | "WorkerCapabilityAdded"
                | "WorkerCapabilityRemoved"
        )
    }

    fn version(&self) -> u32 {
        1
    }

    fn category(&self) -> Option<&str> {
        Some("worker")
    }
}

impl WorkerStateProjection {
    /// Get the last processed sequence number
    pub fn last_processed_sequence(&self) -> i64 {
        self.last_sequence.try_read().map(|v| *v).unwrap_or(0)
    }

    /// Set the last processed sequence number
    pub fn set_last_processed_sequence(&mut self, sequence: i64) {
        if let Ok(mut last_seq) = self.last_sequence.try_write() {
            *last_seq = sequence;
        }
    }

    /// Get the projection name
    pub fn projection_name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orcher_events::projections::Projection;

    fn create_test_event(
        worker_id: WorkerId,
        event_type: &str,
        sequence: i64,
        payload: serde_json::Value,
    ) -> StoredEvent {
        StoredEvent::from_aggregate(
            AggregateId::Worker(worker_id),
            event_type.to_string(),
            payload,
            sequence,
        )
    }

    #[test]
    fn test_projection_creation() {
        let projection = WorkerStateProjection::new();
        assert_eq!(projection.projection_name(), "worker_state");
        assert_eq!(projection.last_processed_sequence(), 0);
        assert_eq!(projection.total_workers(), 0);
    }

    #[tokio::test]
    async fn test_worker_registered_event() {
        let mut projection = WorkerStateProjection::new();
        let worker_id = WorkerId::new_v4();

        let event = create_test_event(
            worker_id,
            "WorkerRegistered",
            1,
            serde_json::json!({
                "worker_id": worker_id.as_uuid(),
                "capabilities": ["compute", "gpu"]
            }),
        );

        projection.project(&event).await.unwrap();

        let state = projection.get_worker_state(&worker_id).unwrap();
        assert_eq!(state.worker_id, worker_id);
        assert_eq!(state.status, WorkerStatus::Registered);
        assert_eq!(state.capabilities, vec!["compute", "gpu"]);
        assert!(state.healthy);
        assert_eq!(projection.last_processed_sequence(), 1);
    }

    #[tokio::test]
    async fn test_worker_lifecycle() {
        let mut projection = WorkerStateProjection::new();
        let worker_id = WorkerId::new_v4();

        // Register
        let registered_event = create_test_event(
            worker_id,
            "WorkerRegistered",
            1,
            serde_json::json!({
                "worker_id": worker_id.as_uuid(),
                "capabilities": ["compute"]
            }),
        );
        projection.project(&registered_event).await.unwrap();

        // Heartbeat (goes online)
        let heartbeat_event = create_test_event(
            worker_id,
            "WorkerHeartbeat",
            2,
            serde_json::json!({
                "worker_id": worker_id.as_uuid(),
                "load": 0.3
            }),
        );
        projection.project(&heartbeat_event).await.unwrap();

        let state = projection.get_worker_state(&worker_id).unwrap();
        assert_eq!(state.status, WorkerStatus::Online);
        assert_eq!(state.load, 0.3);

        // Load updated
        let load_event = create_test_event(
            worker_id,
            "WorkerLoadUpdated",
            3,
            serde_json::json!({
                "worker_id": worker_id.as_uuid(),
                "load": 0.8,
                "available_capacity": 0.2
            }),
        );
        projection.project(&load_event).await.unwrap();

        let state = projection.get_worker_state(&worker_id).unwrap();
        assert_eq!(state.load, 0.8);
        assert_eq!(state.available_capacity, 0.2);

        // Goes offline
        let offline_event = create_test_event(
            worker_id,
            "WorkerOffline",
            4,
            serde_json::json!({
                "worker_id": worker_id.as_uuid(),
                "reason": "Connection lost"
            }),
        );
        projection.project(&offline_event).await.unwrap();

        let state = projection.get_worker_state(&worker_id).unwrap();
        assert_eq!(state.status, WorkerStatus::Offline);
        assert!(!state.healthy);
        assert_eq!(state.state_reason, Some("Connection lost".to_string()));
        assert_eq!(projection.last_processed_sequence(), 4);
    }

    #[tokio::test]
    async fn test_worker_health_changed() {
        let mut projection = WorkerStateProjection::new();
        let worker_id = WorkerId::new_v4();

        let registered_event = create_test_event(
            worker_id,
            "WorkerRegistered",
            1,
            serde_json::json!({
                "worker_id": worker_id.as_uuid(),
                "capabilities": []
            }),
        );
        projection.project(&registered_event).await.unwrap();

        let health_event = create_test_event(
            worker_id,
            "WorkerHealthChanged",
            2,
            serde_json::json!({
                "worker_id": worker_id.as_uuid(),
                "healthy": false,
                "reason": "High CPU usage"
            }),
        );
        projection.project(&health_event).await.unwrap();

        let state = projection.get_worker_state(&worker_id).unwrap();
        assert!(!state.healthy);
        assert_eq!(state.state_reason, Some("High CPU usage".to_string()));
    }

    #[tokio::test]
    async fn test_get_workers_by_status() {
        let mut projection = WorkerStateProjection::new();

        // Create multiple workers
        for i in 0..5 {
            let worker_id = WorkerId::new_v4();
            let event = create_test_event(
                worker_id,
                "WorkerRegistered",
                i + 1,
                serde_json::json!({
                    "worker_id": worker_id.as_uuid(),
                    "capabilities": []
                }),
            );
            projection.project(&event).await.unwrap();
        }

        let registered_workers = projection.get_workers_by_status(WorkerStatus::Registered);
        assert_eq!(registered_workers.len(), 5);
        assert_eq!(projection.count_by_status(WorkerStatus::Registered), 5);
    }

    #[tokio::test]
    async fn test_get_healthy_workers() {
        let mut projection = WorkerStateProjection::new();

        // Create healthy worker
        let worker1 = WorkerId::new_v4();
        let event1 = create_test_event(
            worker1,
            "WorkerRegistered",
            1,
            serde_json::json!({
                "worker_id": worker1.as_uuid(),
                "capabilities": []
            }),
        );
        projection.project(&event1).await.unwrap();

        // Create unhealthy worker
        let worker2 = WorkerId::new_v4();
        let event2 = create_test_event(
            worker2,
            "WorkerRegistered",
            2,
            serde_json::json!({
                "worker_id": worker2.as_uuid(),
                "capabilities": []
            }),
        );
        projection.project(&event2).await.unwrap();

        let health_event = create_test_event(
            worker2,
            "WorkerHealthChanged",
            3,
            serde_json::json!({
                "worker_id": worker2.as_uuid(),
                "healthy": false
            }),
        );
        projection.project(&health_event).await.unwrap();

        let healthy_workers = projection.get_healthy_workers();
        assert_eq!(healthy_workers.len(), 1);
        assert_eq!(healthy_workers[0].worker_id, worker1);
    }

    #[tokio::test]
    async fn test_get_available_workers() {
        let mut projection = WorkerStateProjection::new();

        let worker1 = WorkerId::new_v4();
        projection
            .project(&create_test_event(
                worker1,
                "WorkerRegistered",
                1,
                serde_json::json!({
                    "worker_id": worker1.as_uuid(),
                    "capabilities": []
                }),
            ))
            .await
            .unwrap();

        projection
            .project(&create_test_event(
                worker1,
                "WorkerHeartbeat",
                2,
                serde_json::json!({
                    "worker_id": worker1.as_uuid(),
                    "load": 0.3
                }),
            ))
            .await
            .unwrap();

        projection
            .project(&create_test_event(
                worker1,
                "WorkerLoadUpdated",
                3,
                serde_json::json!({
                    "worker_id": worker1.as_uuid(),
                    "load": 0.3,
                    "available_capacity": 0.7
                }),
            ))
            .await
            .unwrap();

        let available = projection.get_available_workers(0.5);
        assert_eq!(available.len(), 1);
        assert_eq!(available[0].worker_id, worker1);

        let available_high = projection.get_available_workers(0.9);
        assert_eq!(available_high.len(), 0);
    }

    #[tokio::test]
    async fn test_average_load() {
        let mut projection = WorkerStateProjection::new();

        // Create two online workers with different loads
        for (i, load) in [(1, 0.4), (2, 0.6)].iter() {
            let worker_id = WorkerId::new_v4();
            projection
                .project(&create_test_event(
                    worker_id,
                    "WorkerRegistered",
                    *i,
                    serde_json::json!({
                        "worker_id": worker_id.as_uuid(),
                        "capabilities": []
                    }),
                ))
                .await
                .unwrap();

            projection
                .project(&create_test_event(
                    worker_id,
                    "WorkerHeartbeat",
                    i + 10,
                    serde_json::json!({
                        "worker_id": worker_id.as_uuid(),
                        "load": load
                    }),
                ))
                .await
                .unwrap();
        }

        let avg = projection.average_load();
        assert!((avg - 0.5).abs() < 0.01); // Average of 0.4 and 0.6 is 0.5
    }

    #[tokio::test]
    async fn test_worker_overloaded() {
        let mut projection = WorkerStateProjection::new();
        let worker_id = WorkerId::new_v4();

        projection
            .project(&create_test_event(
                worker_id,
                "WorkerRegistered",
                1,
                serde_json::json!({
                    "worker_id": worker_id.as_uuid(),
                    "capabilities": []
                }),
            ))
            .await
            .unwrap();

        projection
            .project(&create_test_event(
                worker_id,
                "WorkerOverloaded",
                2,
                serde_json::json!({
                    "worker_id": worker_id.as_uuid(),
                    "current_load": 0.95
                }),
            ))
            .await
            .unwrap();

        let state = projection.get_worker_state(&worker_id).unwrap();
        assert_eq!(state.status, WorkerStatus::Overloaded);
        assert_eq!(state.load, 0.95);
    }

    #[test]
    fn test_set_last_processed_sequence() {
        let mut projection = WorkerStateProjection::new();
        projection.set_last_processed_sequence(42);
        assert_eq!(projection.last_processed_sequence(), 42);
    }
}
