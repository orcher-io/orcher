//! Dual-write publisher: events go to the bus *and* optionally to persistent storage.

use crate::bus::EventBus;
use crate::domain::{
    DlqEvent, ExecutionEvent, SchedulerEvent, TaskEvent, WorkerEvent, WorkerTaskEvent,
    WorkflowEvent,
};
use crate::error::Result;
use crate::publisher::traits::EventPublisher;
use async_trait::async_trait;
use orcher_common::OrcherResult;
use std::sync::Arc;
use tokio::sync::RwLock;

#[cfg(feature = "storage")]
use crate::replay::{EventStore, StreamId, StreamPosition};
#[cfg(feature = "storage")]
use crate::Event;
#[cfg(feature = "storage")]
use orcher_common::prelude::Timestamptz;
#[cfg(feature = "storage")]
use std::collections::HashMap;
#[cfg(feature = "storage")]
use tokio::sync::mpsc;
#[cfg(feature = "storage")]
use tokio::task::JoinHandle;

/// Controls how the bridge handles persistence.
#[derive(Debug, Clone)]
pub struct BridgeConfig {
    /// Whether to write events to the store at all.
    pub enable_persistence: bool,

    /// Depth of the background persistence channel.
    pub persistence_queue_size: usize,

    /// Whether to feed events into projections.
    pub enable_projections: bool,

    /// Retry persistence on transient failures.
    pub retry_on_failure: bool,

    /// Cap on retry attempts per event.
    pub max_retries: usize,

    /// If true, persistence happens in a background task (non-blocking).
    /// If false, persistence is inline (blocks the publish call).
    pub enable_background_persistence: bool,
}

impl Default for BridgeConfig {
    fn default() -> Self {
        Self {
            enable_persistence: true,
            persistence_queue_size: 10_000,
            enable_projections: false,
            retry_on_failure: true,
            max_retries: 3,
            enable_background_persistence: true,
        }
    }
}

impl BridgeConfig {
    /// Defaults: persistence on, background on, 3 retries, 10k queue depth.
    pub fn new() -> Self {
        Self::default()
    }

    /// Toggle persistence.
    pub fn with_persistence(mut self, enabled: bool) -> Self {
        self.enable_persistence = enabled;
        self
    }

    /// Set the background queue depth.
    pub fn with_queue_size(mut self, size: usize) -> Self {
        self.persistence_queue_size = size;
        self
    }

    /// Toggle projection feeding.
    pub fn with_projections(mut self, enabled: bool) -> Self {
        self.enable_projections = enabled;
        self
    }

    /// Toggle retry on persistence failures.
    pub fn with_retry(mut self, enabled: bool) -> Self {
        self.retry_on_failure = enabled;
        self
    }

    /// Cap retry attempts.
    pub fn with_max_retries(mut self, retries: usize) -> Self {
        self.max_retries = retries;
        self
    }

    /// Choose between background (async) and inline (blocking) persistence.
    pub fn with_background_persistence(mut self, enabled: bool) -> Self {
        self.enable_background_persistence = enabled;
        self
    }
}

/// Item sent over the background persistence channel.
#[cfg(feature = "storage")]
struct PersistenceTask {
    stream_id: StreamId,
    event: Event,
}

/// Publishes events to the bus and optionally writes them to persistent storage
/// (either inline or via a background worker).
pub struct BridgePublisher {
    bus: Arc<EventBus>,
    config: BridgeConfig,
    #[cfg(feature = "storage")]
    event_store: Option<Arc<dyn EventStore>>,

    #[cfg(feature = "storage")]
    persistence_tx: Option<mpsc::Sender<PersistenceTask>>,

    projections: Arc<RwLock<Vec<String>>>,
}

impl BridgePublisher {
    /// Create a bridge publisher (no event store wired up yet).
    pub fn new(bus: Arc<EventBus>, config: BridgeConfig) -> Self {
        Self {
            bus,
            config,
            #[cfg(feature = "storage")]
            event_store: None,
            #[cfg(feature = "storage")]
            persistence_tx: None,
            projections: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Create a bridge publisher with an event store for inline persistence.
    #[cfg(feature = "storage")]
    pub fn with_event_store(
        bus: Arc<EventBus>,
        config: BridgeConfig,
        event_store: Arc<dyn EventStore>,
    ) -> Self {
        Self {
            bus,
            config,
            event_store: Some(event_store),
            persistence_tx: None,
            projections: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Create a bridge with a background persistence worker.
    /// Returns `(publisher, worker_handle)` — abort the handle on shutdown.
    #[cfg(feature = "storage")]
    pub fn new_with_background(
        bus: Arc<EventBus>,
        config: BridgeConfig,
        event_store: Arc<dyn EventStore>,
    ) -> (Self, Option<JoinHandle<()>>) {
        let (persistence_tx, handle) = if config.enable_background_persistence {
            let (tx, rx) = mpsc::channel(config.persistence_queue_size);
            let worker_handle = tokio::spawn(Self::persistence_worker(
                event_store.clone(),
                rx,
                config.clone(),
            ));
            (Some(tx), Some(worker_handle))
        } else {
            (None, None)
        };

        let publisher = Self {
            bus,
            config,
            event_store: Some(event_store),
            persistence_tx,
            projections: Arc::new(RwLock::new(Vec::new())),
        };

        (publisher, handle)
    }

    /// Shorthand: default config, no store.
    pub fn with_defaults(bus: Arc<EventBus>) -> Self {
        Self::new(bus, BridgeConfig::default())
    }

    /// Default config + background persistence.
    #[cfg(feature = "storage")]
    pub fn with_defaults_and_background(
        bus: Arc<EventBus>,
        event_store: Arc<dyn EventStore>,
    ) -> (Self, Option<JoinHandle<()>>) {
        Self::new_with_background(bus, BridgeConfig::default(), event_store)
    }

    /// Access the underlying bus.
    pub fn bus(&self) -> &Arc<EventBus> {
        &self.bus
    }

    /// Access the current config.
    pub fn config(&self) -> &BridgeConfig {
        &self.config
    }

    /// Track a projection by name (actual projection logic lives elsewhere).
    pub async fn register_projection(&self, name: String) {
        let mut projections = self.projections.write().await;
        projections.push(name);
    }

    /// Unregister all projections.
    pub async fn clear_projections(&self) {
        let mut projections = self.projections.write().await;
        projections.clear();
    }

    /// How many projections are registered.
    pub async fn projection_count(&self) -> usize {
        let projections = self.projections.read().await;
        projections.len()
    }

    /// List registered projection names.
    pub async fn projections(&self) -> Vec<String> {
        let projections = self.projections.read().await;
        projections.clone()
    }

    /// Background loop: drains the persistence channel and writes to the store with retries.
    #[cfg(feature = "storage")]
    async fn persistence_worker(
        event_store: Arc<dyn EventStore>,
        mut rx: mpsc::Receiver<PersistenceTask>,
        config: BridgeConfig,
    ) {
        while let Some(task) = rx.recv().await {
            let mut retries = 0;

            loop {
                match event_store
                    .append_to_stream(
                        &task.stream_id,
                        StreamPosition::End,
                        vec![task.event.clone()],
                    )
                    .await
                {
                    Ok(_) => {
                        tracing::debug!("Event persisted to stream: {}", task.stream_id);
                        break;
                    }
                    Err(e) => {
                        tracing::error!(
                            "Failed to persist event to stream {}: {}",
                            task.stream_id,
                            e
                        );

                        if !config.retry_on_failure || retries >= config.max_retries {
                            tracing::error!(
                                "Dropping event after {} retries for stream: {}",
                                retries,
                                task.stream_id
                            );
                            break;
                        }

                        retries += 1;
                        // Exponential backoff: 100ms, 200ms, 400ms, 800ms...
                        let delay =
                            std::time::Duration::from_millis(100 * 2_u64.pow(retries as u32));
                        tokio::time::sleep(delay).await;
                    }
                }
            }
        }

        tracing::info!("Persistence worker shutting down");
    }

    #[allow(dead_code)]
    async fn publish_and_persist<E>(&self, event: E, persist_fn: impl Fn(&E)) -> Result<()>
    where
        E: Clone + Send + Sync + 'static,
    {
        // If persistence is enabled, persist the event
        if self.config.enable_persistence {
            persist_fn(&event);
        }

        Ok(())
    }

    /// Write a task event to the store synchronously (used when background mode is off).
    #[cfg(feature = "storage")]
    async fn persist_task_event_inline(&self, event: &TaskEvent) -> Result<()> {
        if !self.config.enable_persistence {
            return Ok(());
        }

        let Some(event_store) = &self.event_store else {
            return Ok(());
        };

        let stream_id = StreamId::new("task", event.task_id().to_string());
        let mut metadata = HashMap::new();
        metadata.insert("task_id".to_string(), event.task_id().to_string());

        let event_obj = Event {
            event_type: event.event_type().to_string(),
            data: serde_json::to_value(event).unwrap_or_default(),
            metadata,
            timestamp: Timestamptz::now(),
        };

        event_store
            .append_to_stream(&stream_id, StreamPosition::End, vec![event_obj])
            .await?;

        Ok(())
    }

    /// Send a task event to the background persistence worker.
    #[cfg(feature = "storage")]
    async fn queue_task_event(&self, event: &TaskEvent) -> Result<()> {
        let Some(tx) = &self.persistence_tx else {
            return Ok(());
        };

        let stream_id = StreamId::new("task", event.task_id().to_string());
        let mut metadata = HashMap::new();
        metadata.insert("task_id".to_string(), event.task_id().to_string());

        let event_obj = Event {
            event_type: event.event_type().to_string(),
            data: serde_json::to_value(event).unwrap_or_default(),
            metadata,
            timestamp: Timestamptz::now(),
        };

        let task = PersistenceTask {
            stream_id,
            event: event_obj,
        };

        tx.send(task)
            .await
            .map_err(|e| crate::error::EventError::Internal {
                message: format!("Persistence queue full: {}", e),
            })?;

        Ok(())
    }

    #[cfg(not(feature = "storage"))]
    #[allow(dead_code)]
    async fn persist_task_event_inline(&self, _event: &TaskEvent) -> Result<()> {
        Ok(())
    }

    #[cfg(not(feature = "storage"))]
    #[allow(dead_code)]
    async fn queue_task_event(&self, _event: &TaskEvent) -> Result<()> {
        Ok(())
    }

    /// Write a worker event to the store synchronously.
    #[cfg(feature = "storage")]
    async fn persist_worker_event_inline(&self, event: &WorkerEvent) -> Result<()> {
        if !self.config.enable_persistence {
            return Ok(());
        }

        let Some(event_store) = &self.event_store else {
            return Ok(());
        };

        let stream_id = StreamId::new("worker", event.worker_id().to_string());
        let mut metadata = HashMap::new();
        metadata.insert("worker_id".to_string(), event.worker_id().to_string());

        let event_obj = Event {
            event_type: event.event_type().to_string(),
            data: serde_json::to_value(event).unwrap_or_default(),
            metadata,
            timestamp: Timestamptz::now(),
        };

        event_store
            .append_to_stream(&stream_id, StreamPosition::End, vec![event_obj])
            .await?;

        Ok(())
    }

    /// Send a worker event to the background persistence worker.
    #[cfg(feature = "storage")]
    async fn queue_worker_event(&self, event: &WorkerEvent) -> Result<()> {
        let Some(tx) = &self.persistence_tx else {
            return Ok(());
        };

        let stream_id = StreamId::new("worker", event.worker_id().to_string());
        let mut metadata = HashMap::new();
        metadata.insert("worker_id".to_string(), event.worker_id().to_string());

        let event_obj = Event {
            event_type: event.event_type().to_string(),
            data: serde_json::to_value(event).unwrap_or_default(),
            metadata,
            timestamp: Timestamptz::now(),
        };

        let task = PersistenceTask {
            stream_id,
            event: event_obj,
        };

        tx.send(task)
            .await
            .map_err(|e| crate::error::EventError::Internal {
                message: format!("Persistence queue full: {}", e),
            })?;

        Ok(())
    }

    #[cfg(not(feature = "storage"))]
    #[allow(dead_code)]
    async fn persist_worker_event_inline(&self, _event: &WorkerEvent) -> Result<()> {
        Ok(())
    }

    #[cfg(not(feature = "storage"))]
    #[allow(dead_code)]
    async fn queue_worker_event(&self, _event: &WorkerEvent) -> Result<()> {
        Ok(())
    }

    /// Write a workflow event to the store synchronously.
    #[cfg(feature = "storage")]
    async fn persist_workflow_event_inline(&self, event: &WorkflowEvent) -> Result<()> {
        if !self.config.enable_persistence {
            return Ok(());
        }

        let Some(event_store) = &self.event_store else {
            return Ok(());
        };

        let stream_id = StreamId::new("workflow", event.workflow_id().to_string());
        let mut metadata = HashMap::new();
        metadata.insert("workflow_id".to_string(), event.workflow_id().to_string());

        let event_obj = Event {
            event_type: event.event_type().to_string(),
            data: serde_json::to_value(event).unwrap_or_default(),
            metadata,
            timestamp: Timestamptz::now(),
        };

        event_store
            .append_to_stream(&stream_id, StreamPosition::End, vec![event_obj])
            .await?;

        Ok(())
    }

    /// Send a workflow event to the background persistence worker.
    #[cfg(feature = "storage")]
    async fn queue_workflow_event(&self, event: &WorkflowEvent) -> Result<()> {
        let Some(tx) = &self.persistence_tx else {
            return Ok(());
        };

        let stream_id = StreamId::new("workflow", event.workflow_id().to_string());
        let mut metadata = HashMap::new();
        metadata.insert("workflow_id".to_string(), event.workflow_id().to_string());

        let event_obj = Event {
            event_type: event.event_type().to_string(),
            data: serde_json::to_value(event).unwrap_or_default(),
            metadata,
            timestamp: Timestamptz::now(),
        };

        let task = PersistenceTask {
            stream_id,
            event: event_obj,
        };

        tx.send(task)
            .await
            .map_err(|e| crate::error::EventError::Internal {
                message: format!("Persistence queue full: {}", e),
            })?;

        Ok(())
    }

    #[cfg(not(feature = "storage"))]
    #[allow(dead_code)]
    async fn persist_workflow_event_inline(&self, _event: &WorkflowEvent) -> Result<()> {
        Ok(())
    }

    #[cfg(not(feature = "storage"))]
    #[allow(dead_code)]
    async fn queue_workflow_event(&self, _event: &WorkflowEvent) -> Result<()> {
        Ok(())
    }

    /// Write a worker-task event to the store.
    #[cfg(feature = "storage")]
    #[allow(dead_code)]
    async fn persist_worker_task_event(&self, event: &WorkerTaskEvent) -> Result<()> {
        if !self.config.enable_persistence {
            return Ok(());
        }

        let Some(event_store) = &self.event_store else {
            return Ok(());
        };

        let stream_id = StreamId::new("worker-task", event.worker_id.to_string());
        let mut metadata = HashMap::new();
        metadata.insert("worker_id".to_string(), event.worker_id.to_string());
        metadata.insert("task_id".to_string(), event.task_id.to_string());

        let event_obj = Event {
            event_type: format!("{:?}", event.event_type),
            data: serde_json::to_value(event).unwrap_or_default(),
            metadata,
            timestamp: Timestamptz::now(),
        };

        event_store
            .append_to_stream(&stream_id, StreamPosition::End, vec![event_obj])
            .await?;

        Ok(())
    }

    #[cfg(not(feature = "storage"))]
    #[allow(dead_code)]
    async fn persist_worker_task_event(&self, _event: &WorkerTaskEvent) -> Result<()> {
        Ok(())
    }

    /// Write an execution event to the store.
    #[cfg(feature = "storage")]
    #[allow(dead_code)]
    async fn persist_execution_event(&self, event: &ExecutionEvent) -> Result<()> {
        if !self.config.enable_persistence {
            return Ok(());
        }

        let Some(event_store) = &self.event_store else {
            return Ok(());
        };

        let stream_id = StreamId::new("execution", event.execution_id().to_string());
        let mut metadata = HashMap::new();
        metadata.insert("execution_id".to_string(), event.execution_id().to_string());

        let event_obj = Event {
            event_type: event.event_type().to_string(),
            data: serde_json::to_value(event).unwrap_or_default(),
            metadata,
            timestamp: Timestamptz::now(),
        };

        event_store
            .append_to_stream(&stream_id, StreamPosition::End, vec![event_obj])
            .await?;

        Ok(())
    }

    #[cfg(not(feature = "storage"))]
    #[allow(dead_code)]
    async fn persist_execution_event(&self, _event: &ExecutionEvent) -> Result<()> {
        Ok(())
    }

    /// Write a scheduler event to the store.
    #[cfg(feature = "storage")]
    #[allow(dead_code)]
    async fn persist_scheduler_event(&self, event: &SchedulerEvent) -> Result<()> {
        if !self.config.enable_persistence {
            return Ok(());
        }

        let Some(event_store) = &self.event_store else {
            return Ok(());
        };

        let stream_id = StreamId::new("scheduler", "global");
        let metadata = HashMap::new();

        let event_obj = Event {
            event_type: event.event_type().to_string(),
            data: serde_json::to_value(event).unwrap_or_default(),
            metadata,
            timestamp: Timestamptz::now(),
        };

        event_store
            .append_to_stream(&stream_id, StreamPosition::End, vec![event_obj])
            .await?;

        Ok(())
    }

    #[cfg(not(feature = "storage"))]
    #[allow(dead_code)]
    async fn persist_scheduler_event(&self, _event: &SchedulerEvent) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl EventPublisher for BridgePublisher {
    async fn publish_task_event(&self, event: TaskEvent) -> Result<()> {
        // Publish to bus first (fast)
        self.bus.publish_task_event(event.clone()).await?;

        // Then persist (background queue or inline)
        #[cfg(feature = "storage")]
        if self.config.enable_background_persistence {
            self.queue_task_event(&event).await?;
        } else {
            self.persist_task_event_inline(&event).await?;
        }

        Ok(())
    }

    async fn publish_worker_event(&self, event: WorkerEvent) -> Result<()> {
        // Publish to bus first (fast)
        self.bus.publish_worker_event(event.clone()).await?;

        // Then persist (background queue or inline)
        #[cfg(feature = "storage")]
        if self.config.enable_background_persistence {
            self.queue_worker_event(&event).await?;
        } else {
            self.persist_worker_event_inline(&event).await?;
        }

        Ok(())
    }

    async fn publish_workflow_event(&self, event: WorkflowEvent) -> Result<()> {
        // Publish to bus first (fast)
        self.bus.publish_workflow_event(event.clone()).await?;

        // Then persist (background queue or inline)
        #[cfg(feature = "storage")]
        if self.config.enable_background_persistence {
            self.queue_workflow_event(&event).await?;
        } else {
            self.persist_workflow_event_inline(&event).await?;
        }

        Ok(())
    }

    async fn publish_worker_task_event(&self, event: WorkerTaskEvent) -> Result<()> {
        self.bus.publish_worker_task_event(event.clone()).await?;
        // Worker task events don't have background persistence yet
        Ok(())
    }

    async fn publish_execution_event(&self, event: ExecutionEvent) -> Result<()> {
        self.bus.publish_execution_event(event.clone()).await?;
        // Execution events don't have background persistence yet
        Ok(())
    }

    async fn publish_scheduler_event(&self, event: SchedulerEvent) -> Result<()> {
        self.bus.publish_scheduler_event(event.clone()).await?;
        // Scheduler events don't have background persistence yet
        Ok(())
    }

    async fn publish_dlq_event(&self, event: DlqEvent) -> Result<()> {
        self.bus.publish_dlq_event(event.clone()).await?;
        // DLQ events don't have background persistence yet
        Ok(())
    }
}

// Implement orcher_common::events::EventPublisher for worker integration
#[async_trait]
impl orcher_common::events::EventPublisher for BridgePublisher {
    async fn publish_task_event(
        &self,
        event: orcher_common::events::WorkerTaskEvent,
    ) -> OrcherResult<()> {
        // Convert worker task event to domain task event
        use orcher_common::events::WorkerTaskEventType;

        let task_event = match event.event_type {
            WorkerTaskEventType::ExecutionStarted => TaskEvent::Started {
                task_id: event.task_id,
                worker_id: event.worker_id,
                started_at: event.timestamp,
            },
            WorkerTaskEventType::ExecutionCompleted {
                output,
                duration_ms,
            } => TaskEvent::Completed {
                task_id: event.task_id,
                completed_at: event.timestamp,
                result: crate::domain::TaskResult::success(Some(output), Some(duration_ms)),
            },
            WorkerTaskEventType::ExecutionFailed {
                error,
                duration_ms: _,
            } => TaskEvent::Failed {
                task_id: event.task_id,
                failed_at: event.timestamp,
                error,
                retry_count: 0,
            },
            WorkerTaskEventType::ExecutionTimedOut { timeout_seconds } => TaskEvent::Failed {
                task_id: event.task_id,
                failed_at: event.timestamp,
                error: format!("Task timed out after {} seconds", timeout_seconds),
                retry_count: 0,
            },
        };

        // Use the EventPublisher trait implementation
        EventPublisher::publish_task_event(self, task_event)
            .await
            .map_err(|e| orcher_common::error::OrcherError::internal(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::publisher::EventPublisher;
    use orcher_common::types::identifiers::{Identifier, TaskId, WorkerId, WorkflowId};
    use orcher_common::types::time::Timestamptz;

    #[test]
    fn test_bridge_config_default() {
        let config = BridgeConfig::default();
        assert!(config.enable_persistence);
        assert_eq!(config.persistence_queue_size, 10_000);
        assert!(!config.enable_projections);
        assert!(config.retry_on_failure);
        assert_eq!(config.max_retries, 3);
    }

    #[test]
    fn test_bridge_config_builder() {
        let config = BridgeConfig::new()
            .with_persistence(false)
            .with_queue_size(5_000)
            .with_projections(true)
            .with_retry(false)
            .with_max_retries(5);

        assert!(!config.enable_persistence);
        assert_eq!(config.persistence_queue_size, 5_000);
        assert!(config.enable_projections);
        assert!(!config.retry_on_failure);
        assert_eq!(config.max_retries, 5);
    }

    #[tokio::test]
    async fn test_bridge_publisher_creation() {
        let bus = EventBus::with_defaults();
        let config = BridgeConfig::default();
        let publisher = BridgePublisher::new(bus, config);
        assert!(publisher.config().enable_persistence);
    }

    #[tokio::test]
    async fn test_bridge_publisher_with_defaults() {
        let bus = EventBus::with_defaults();
        let publisher = BridgePublisher::with_defaults(bus);
        assert!(publisher.config().enable_persistence);
    }

    #[tokio::test]
    async fn test_publish_task_event() {
        let bus = EventBus::with_defaults();
        let publisher = BridgePublisher::with_defaults(Arc::clone(&bus));
        let mut receiver = bus.subscribe_task_events();

        let task_id = TaskId::generate();
        let event = TaskEvent::Submitted {
            task_id,
            submitted_at: Timestamptz::now(),
            priority: 5,
        };

        publisher.publish_task_event(event.clone()).await.unwrap();

        let received = receiver.recv().await.unwrap();
        match received {
            TaskEvent::Submitted {
                task_id: id,
                priority,
                ..
            } => {
                assert_eq!(id, task_id);
                assert_eq!(priority, 5);
            }
            _ => panic!("Expected TaskEvent::Submitted"),
        }
    }

    #[tokio::test]
    async fn test_publish_without_persistence() {
        let bus = EventBus::with_defaults();
        let config = BridgeConfig::new().with_persistence(false);
        let publisher = BridgePublisher::new(Arc::clone(&bus), config);
        let mut receiver = bus.subscribe_task_events();

        let task_id = TaskId::generate();
        let event = TaskEvent::Submitted {
            task_id,
            submitted_at: Timestamptz::now(),
            priority: 5,
        };

        publisher.publish_task_event(event.clone()).await.unwrap();

        let received = receiver.recv().await.unwrap();
        assert!(matches!(received, TaskEvent::Submitted { .. }));
    }

    #[tokio::test]
    async fn test_publish_worker_event() {
        let bus = EventBus::with_defaults();
        let publisher = BridgePublisher::with_defaults(Arc::clone(&bus));
        let mut receiver = bus.subscribe_worker_events();

        let worker_id = WorkerId::generate();
        let event = WorkerEvent::Registered {
            worker_id,
            capabilities: vec!["compute".to_string()],
            registered_at: Timestamptz::now(),
        };

        publisher.publish_worker_event(event.clone()).await.unwrap();

        let received = receiver.recv().await.unwrap();
        assert!(matches!(received, WorkerEvent::Registered { .. }));
    }

    #[tokio::test]
    async fn test_publish_workflow_event() {
        let bus = EventBus::with_defaults();
        let publisher = BridgePublisher::with_defaults(Arc::clone(&bus));
        let mut receiver = bus.subscribe_workflow_events();

        let workflow_id = WorkflowId::generate();
        let event = WorkflowEvent::Started {
            workflow_id,
            started_at: Timestamptz::now(),
        };

        publisher
            .publish_workflow_event(event.clone())
            .await
            .unwrap();

        let received = receiver.recv().await.unwrap();
        assert!(matches!(received, WorkflowEvent::Started { .. }));
    }

    #[tokio::test]
    async fn test_publish_worker_task_event() {
        let bus = EventBus::with_defaults();
        let publisher = BridgePublisher::with_defaults(Arc::clone(&bus));
        let mut receiver = bus.subscribe_worker_task_events();

        let task_id = TaskId::generate();
        let worker_id = WorkerId::generate();
        let event = WorkerTaskEvent::execution_started(task_id, worker_id);

        publisher
            .publish_worker_task_event(event.clone())
            .await
            .unwrap();

        let received = receiver.recv().await.unwrap();
        assert_eq!(received.task_id, task_id);
        assert_eq!(received.worker_id, worker_id);
    }

    #[tokio::test]
    async fn test_publish_execution_event() {
        let bus = EventBus::with_defaults();
        let publisher = BridgePublisher::with_defaults(Arc::clone(&bus));
        let mut receiver = bus.subscribe_execution_events();

        let event = ExecutionEvent::Started {
            execution_id: "exec_123".to_string(),
            started_at: Timestamptz::now(),
        };

        publisher
            .publish_execution_event(event.clone())
            .await
            .unwrap();

        let received = receiver.recv().await.unwrap();
        assert_eq!(received.execution_id(), "exec_123");
    }

    #[tokio::test]
    async fn test_publish_scheduler_event() {
        let bus = EventBus::with_defaults();
        let publisher = BridgePublisher::with_defaults(Arc::clone(&bus));
        let mut receiver = bus.subscribe_scheduler_events();

        let event = SchedulerEvent::ScheduleEnabled {
            schedule_id: "sched_123".to_string(),
            enabled_at: Timestamptz::now(),
        };

        publisher
            .publish_scheduler_event(event.clone())
            .await
            .unwrap();

        let received = receiver.recv().await.unwrap();
        assert_eq!(received.schedule_id(), "sched_123");
    }

    #[tokio::test]
    async fn test_bridge_publisher_with_no_subscribers() {
        let bus = EventBus::with_defaults();
        let publisher = BridgePublisher::with_defaults(Arc::clone(&bus));

        let task_id = TaskId::generate();
        let event = TaskEvent::Submitted {
            task_id,
            submitted_at: Timestamptz::now(),
            priority: 5,
        };

        // Should not panic even without subscribers
        publisher.publish_task_event(event).await.unwrap();
    }

    #[tokio::test]
    async fn test_projection_management() {
        let bus = EventBus::with_defaults();
        let publisher = BridgePublisher::with_defaults(bus);

        // Initially no projections
        assert_eq!(publisher.projection_count().await, 0);

        // Register projections
        publisher
            .register_projection("task_projection".to_string())
            .await;
        publisher
            .register_projection("worker_projection".to_string())
            .await;

        assert_eq!(publisher.projection_count().await, 2);

        let projections = publisher.projections().await;
        assert!(projections.contains(&"task_projection".to_string()));
        assert!(projections.contains(&"worker_projection".to_string()));

        // Clear projections
        publisher.clear_projections().await;
        assert_eq!(publisher.projection_count().await, 0);
    }

    #[tokio::test]
    async fn test_background_persistence_config() {
        let config = BridgeConfig::default().with_background_persistence(true);

        assert!(config.enable_background_persistence);

        let config2 = BridgeConfig::default().with_background_persistence(false);

        assert!(!config2.enable_background_persistence);
    }

    #[tokio::test]
    async fn test_config_defaults_include_background_persistence() {
        let config = BridgeConfig::default();
        assert!(config.enable_background_persistence);
    }
}
