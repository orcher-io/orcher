//! Configuration for the event bus, publishers, and related infra.

use serde::{Deserialize, Serialize};

/// Top-level config — groups bus + publisher settings.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct EventSystemConfig {
    /// Bus settings.
    #[serde(default)]
    pub bus: EventBusConfig,
    /// Publisher settings.
    #[serde(default)]
    pub publisher: EventPublisherConfig,
}

/// Tokio broadcast channel capacities, per event type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventBusConfig {
    /// Task events tend to be the highest volume.
    pub task_channel_capacity: usize,
    /// Worker events (heartbeats, health, etc).
    pub worker_channel_capacity: usize,
    /// Workflow lifecycle events.
    pub workflow_channel_capacity: usize,
    /// Fallback for any other event type.
    pub default_channel_capacity: usize,
}

impl Default for EventBusConfig {
    fn default() -> Self {
        Self {
            task_channel_capacity: 10_000,
            worker_channel_capacity: 5_000,
            workflow_channel_capacity: 5_000,
            default_channel_capacity: 1_000,
        }
    }
}

impl EventBusConfig {
    /// Start with defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Override the task channel capacity.
    pub fn with_task_capacity(mut self, capacity: usize) -> Self {
        self.task_channel_capacity = capacity;
        self
    }

    /// Override the worker channel capacity.
    pub fn with_worker_capacity(mut self, capacity: usize) -> Self {
        self.worker_channel_capacity = capacity;
        self
    }

    /// Override the workflow channel capacity.
    pub fn with_workflow_capacity(mut self, capacity: usize) -> Self {
        self.workflow_channel_capacity = capacity;
        self
    }

    /// Override the default channel capacity.
    pub fn with_default_capacity(mut self, capacity: usize) -> Self {
        self.default_channel_capacity = capacity;
        self
    }
}

/// Publisher-side settings: persistence, projections, queue depth.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventPublisherConfig {
    /// Whether to persist events to the store.
    pub enable_persistence: bool,
    /// How many events to buffer before back-pressure kicks in.
    pub persistence_queue_size: usize,
    /// Whether to feed events into CQRS projections.
    pub enable_projections: bool,
}

impl Default for EventPublisherConfig {
    fn default() -> Self {
        Self {
            enable_persistence: true,
            persistence_queue_size: 10_000,
            enable_projections: true,
        }
    }
}

impl EventPublisherConfig {
    /// Start with defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Toggle persistence on or off.
    pub fn with_persistence(mut self, enabled: bool) -> Self {
        self.enable_persistence = enabled;
        self
    }

    /// Set the persistence buffer depth.
    pub fn with_queue_size(mut self, size: usize) -> Self {
        self.persistence_queue_size = size;
        self
    }

    /// Toggle projection feeding on or off.
    pub fn with_projections(mut self, enabled: bool) -> Self {
        self.enable_projections = enabled;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = EventSystemConfig::default();
        assert_eq!(config.bus.task_channel_capacity, 10_000);
        assert_eq!(config.publisher.enable_persistence, true);
    }

    #[test]
    fn test_bus_config_builder() {
        let config = EventBusConfig::new()
            .with_task_capacity(5_000)
            .with_worker_capacity(2_500);

        assert_eq!(config.task_channel_capacity, 5_000);
        assert_eq!(config.worker_channel_capacity, 2_500);
    }

    #[test]
    fn test_publisher_config_builder() {
        let config = EventPublisherConfig::new()
            .with_persistence(false)
            .with_queue_size(5_000);

        assert_eq!(config.enable_persistence, false);
        assert_eq!(config.persistence_queue_size, 5_000);
    }
}
