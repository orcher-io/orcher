//! Broadcast channel capacities for the event bus.

/// Per-channel buffer sizes. Larger = more memory, but less chance of lagging subscribers.
#[derive(Debug, Clone)]
pub struct EventBusConfig {
    /// Tasks are the highest-volume event type.
    pub task_channel_capacity: usize,
    pub worker_channel_capacity: usize,
    pub workflow_channel_capacity: usize,
    /// Worker-task events track per-task execution on a specific worker.
    pub worker_task_channel_capacity: usize,
    pub execution_channel_capacity: usize,
    /// Scheduler events are relatively rare.
    pub scheduler_channel_capacity: usize,
    /// Fallback for any future event types.
    pub default_channel_capacity: usize,
}

impl Default for EventBusConfig {
    fn default() -> Self {
        Self {
            task_channel_capacity: 10_000,
            worker_channel_capacity: 5_000,
            workflow_channel_capacity: 5_000,
            worker_task_channel_capacity: 10_000,
            execution_channel_capacity: 5_000,
            scheduler_channel_capacity: 1_000,
            default_channel_capacity: 1_000,
        }
    }
}

impl EventBusConfig {
    /// Same as `Default`.
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_task_capacity(mut self, capacity: usize) -> Self {
        self.task_channel_capacity = capacity;
        self
    }

    pub fn with_worker_capacity(mut self, capacity: usize) -> Self {
        self.worker_channel_capacity = capacity;
        self
    }

    pub fn with_workflow_capacity(mut self, capacity: usize) -> Self {
        self.workflow_channel_capacity = capacity;
        self
    }

    pub fn with_worker_task_capacity(mut self, capacity: usize) -> Self {
        self.worker_task_channel_capacity = capacity;
        self
    }

    pub fn with_execution_capacity(mut self, capacity: usize) -> Self {
        self.execution_channel_capacity = capacity;
        self
    }

    pub fn with_scheduler_capacity(mut self, capacity: usize) -> Self {
        self.scheduler_channel_capacity = capacity;
        self
    }

    pub fn with_default_capacity(mut self, capacity: usize) -> Self {
        self.default_channel_capacity = capacity;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = EventBusConfig::default();
        assert_eq!(config.task_channel_capacity, 10_000);
        assert_eq!(config.worker_channel_capacity, 5_000);
        assert_eq!(config.workflow_channel_capacity, 5_000);
        assert_eq!(config.worker_task_channel_capacity, 10_000);
        assert_eq!(config.execution_channel_capacity, 5_000);
        assert_eq!(config.scheduler_channel_capacity, 1_000);
        assert_eq!(config.default_channel_capacity, 1_000);
    }

    #[test]
    fn test_config_builder() {
        let config = EventBusConfig::new()
            .with_task_capacity(20_000)
            .with_worker_capacity(10_000)
            .with_workflow_capacity(8_000)
            .with_worker_task_capacity(15_000)
            .with_execution_capacity(7_000)
            .with_scheduler_capacity(2_000)
            .with_default_capacity(5_000);

        assert_eq!(config.task_channel_capacity, 20_000);
        assert_eq!(config.worker_channel_capacity, 10_000);
        assert_eq!(config.workflow_channel_capacity, 8_000);
        assert_eq!(config.worker_task_channel_capacity, 15_000);
        assert_eq!(config.execution_channel_capacity, 7_000);
        assert_eq!(config.scheduler_channel_capacity, 2_000);
        assert_eq!(config.default_channel_capacity, 5_000);
    }

    #[test]
    fn test_config_clone() {
        let config1 = EventBusConfig::new().with_task_capacity(50_000);
        let config2 = config1.clone();
        assert_eq!(config1.task_channel_capacity, config2.task_channel_capacity);
    }
}
