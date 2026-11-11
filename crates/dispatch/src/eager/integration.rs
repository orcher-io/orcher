//! Bridges the eager dispatch system with the execution service.

use std::sync::Arc;

use super::dispatcher::{
    EagerDispatchConfig, EagerDispatchMetrics, EagerDispatchStats, EagerDispatchable, EagerTask,
    EagerTaskDispatcher,
};
use super::predictor::WorkflowPredictor;
use crate::capacity::CapacityRegistry;

/// Simplified facade over `EagerTaskDispatcher` for use by the execution service.
pub struct EagerDispatchIntegration {
    dispatcher: EagerTaskDispatcher,
    enabled: bool,
}

impl EagerDispatchIntegration {
    pub fn new(capacity_registry: Arc<CapacityRegistry>) -> Self {
        let predictor = Arc::new(WorkflowPredictor::new());
        let dispatcher = EagerTaskDispatcher::new(predictor, capacity_registry);

        Self {
            dispatcher,
            enabled: true,
        }
    }

    pub fn with_config(
        capacity_registry: Arc<CapacityRegistry>,
        config: EagerDispatchConfig,
    ) -> Self {
        let predictor = Arc::new(WorkflowPredictor::new());
        let dispatcher = EagerTaskDispatcher::with_config(predictor, capacity_registry, config);

        Self {
            dispatcher,
            enabled: true,
        }
    }

    /// Uses a shared predictor so multiple services can contribute to the same model.
    pub fn with_predictor(
        predictor: Arc<WorkflowPredictor>,
        capacity_registry: Arc<CapacityRegistry>,
        config: EagerDispatchConfig,
    ) -> Self {
        let dispatcher = EagerTaskDispatcher::with_config(predictor, capacity_registry, config);

        Self {
            dispatcher,
            enabled: true,
        }
    }

    pub fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub fn record_task_execution(&self, workflow_type: &str, task_type: &str) {
        if self.enabled {
            self.dispatcher
                .record_task_execution(workflow_type, task_type);
        }
    }

    pub fn record_workflow_completion(&self, workflow_type: &str) {
        if self.enabled {
            self.dispatcher.record_workflow_completion(workflow_type);
        }
    }

    pub fn select_tasks<T: EagerDispatchable + Clone>(
        &self,
        scheduled_tasks: &[T],
        workflow_type: &str,
        worker_identity: &str,
        worker_task_queue: &str,
        worker_namespace: &str,
    ) -> Vec<EagerTask<T>> {
        if !self.enabled {
            return Vec::new();
        }

        self.dispatcher.select_tasks(
            scheduled_tasks,
            workflow_type,
            worker_identity,
            worker_task_queue,
            worker_namespace,
        )
    }

    pub fn select_matching_tasks<T: EagerDispatchable + Clone>(
        &self,
        scheduled_tasks: &[T],
        worker_task_queue: &str,
        worker_namespace: &str,
        max_tasks: usize,
    ) -> Vec<T> {
        if !self.enabled {
            return Vec::new();
        }

        self.dispatcher.select_matching_tasks(
            scheduled_tasks,
            worker_task_queue,
            worker_namespace,
            max_tasks,
        )
    }

    pub fn predictor(&self) -> &Arc<WorkflowPredictor> {
        self.dispatcher.predictor()
    }

    pub fn stats(&self) -> EagerDispatchStats {
        self.dispatcher.stats()
    }

    pub fn metrics(&self) -> &EagerDispatchMetrics {
        self.dispatcher.metrics()
    }

    pub fn config(&self) -> &EagerDispatchConfig {
        self.dispatcher.config()
    }
}

impl Default for EagerDispatchIntegration {
    fn default() -> Self {
        Self::new(Arc::new(CapacityRegistry::new()))
    }
}

pub struct EagerDispatchIntegrationBuilder {
    capacity_registry: Option<Arc<CapacityRegistry>>,
    predictor: Option<Arc<WorkflowPredictor>>,
    config: EagerDispatchConfig,
    enabled: bool,
}

impl EagerDispatchIntegrationBuilder {
    pub fn new() -> Self {
        Self {
            capacity_registry: None,
            predictor: None,
            config: EagerDispatchConfig::default(),
            enabled: true,
        }
    }

    pub fn capacity_registry(mut self, registry: Arc<CapacityRegistry>) -> Self {
        self.capacity_registry = Some(registry);
        self
    }

    pub fn predictor(mut self, predictor: Arc<WorkflowPredictor>) -> Self {
        self.predictor = Some(predictor);
        self
    }

    pub fn config(mut self, config: EagerDispatchConfig) -> Self {
        self.config = config;
        self
    }

    pub fn high_throughput(mut self) -> Self {
        self.config = EagerDispatchConfig::high_throughput();
        self
    }

    pub fn low_latency(mut self) -> Self {
        self.config = EagerDispatchConfig::low_latency();
        self
    }

    pub fn conservative(mut self) -> Self {
        self.config = EagerDispatchConfig::conservative();
        self
    }

    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    pub fn build(self) -> EagerDispatchIntegration {
        let capacity_registry = self
            .capacity_registry
            .unwrap_or_else(|| Arc::new(CapacityRegistry::new()));

        let mut integration = if let Some(predictor) = self.predictor {
            EagerDispatchIntegration::with_predictor(predictor, capacity_registry, self.config)
        } else {
            EagerDispatchIntegration::with_config(capacity_registry, self.config)
        };

        integration.set_enabled(self.enabled);
        integration
    }
}

impl Default for EagerDispatchIntegrationBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test task implementation
    #[derive(Debug, Clone)]
    struct TestTask {
        task_type: String,
        task_queue: String,
        namespace: String,
    }

    impl TestTask {
        fn new(task_type: &str, task_queue: &str, namespace: &str) -> Self {
            Self {
                task_type: task_type.to_string(),
                task_queue: task_queue.to_string(),
                namespace: namespace.to_string(),
            }
        }
    }

    impl EagerDispatchable for TestTask {
        fn task_type(&self) -> &str {
            &self.task_type
        }

        fn task_queue(&self) -> &str {
            &self.task_queue
        }

        fn namespace(&self) -> &str {
            &self.namespace
        }

        fn workflow_type(&self) -> Option<&str> {
            None
        }
    }

    fn create_integration_with_worker() -> EagerDispatchIntegration {
        let registry = Arc::new(CapacityRegistry::new());
        registry.get_or_register("test-queue", "default", "worker-1", 100);
        EagerDispatchIntegration::new(registry)
    }

    #[test]
    fn test_integration_creation() {
        let integration = EagerDispatchIntegration::default();
        assert!(integration.is_enabled());
    }

    #[test]
    fn test_integration_with_registry() {
        let registry = Arc::new(CapacityRegistry::new());
        let integration = EagerDispatchIntegration::new(registry);
        assert!(integration.is_enabled());
    }

    #[test]
    fn test_integration_disable() {
        let mut integration = EagerDispatchIntegration::default();
        integration.set_enabled(false);
        assert!(!integration.is_enabled());
    }

    #[test]
    fn test_record_task_execution() {
        let integration = EagerDispatchIntegration::default();

        // Should not panic
        integration.record_task_execution("OrderWorkflow", "validate");
        integration.record_task_execution("OrderWorkflow", "process");
        integration.record_task_execution("OrderWorkflow", "complete");
    }

    #[test]
    fn test_record_workflow_completion() {
        let integration = EagerDispatchIntegration::default();

        integration.record_task_execution("OrderWorkflow", "validate");
        integration.record_workflow_completion("OrderWorkflow");

        // Should track workflow type
        assert!(integration.predictor().workflow_type_count() >= 1);
    }

    #[test]
    fn test_record_disabled_is_noop() {
        let mut integration = EagerDispatchIntegration::default();
        integration.set_enabled(false);

        // Should not record when disabled
        integration.record_task_execution("OrderWorkflow", "validate");

        // Predictor should have no patterns
        assert_eq!(integration.predictor().workflow_type_count(), 0);
    }

    #[test]
    fn test_builder_default() {
        let integration = EagerDispatchIntegrationBuilder::new().build();
        assert!(integration.is_enabled());
    }

    #[test]
    fn test_builder_high_throughput() {
        let integration = EagerDispatchIntegrationBuilder::new()
            .high_throughput()
            .build();

        assert_eq!(integration.config().max_eager_tasks, 20);
    }

    #[test]
    fn test_builder_low_latency() {
        let integration = EagerDispatchIntegrationBuilder::new().low_latency().build();

        assert_eq!(integration.config().max_eager_tasks, 5);
    }

    #[test]
    fn test_builder_conservative() {
        let integration = EagerDispatchIntegrationBuilder::new()
            .conservative()
            .build();

        assert_eq!(integration.config().max_eager_tasks, 3);
    }

    #[test]
    fn test_builder_disabled() {
        let integration = EagerDispatchIntegrationBuilder::new()
            .enabled(false)
            .build();

        assert!(!integration.is_enabled());
    }

    #[test]
    fn test_builder_with_registry() {
        let registry = Arc::new(CapacityRegistry::new());
        let integration = EagerDispatchIntegrationBuilder::new()
            .capacity_registry(registry)
            .build();

        assert!(integration.is_enabled());
    }

    #[test]
    fn test_builder_with_predictor() {
        let predictor = Arc::new(WorkflowPredictor::new());
        let integration = EagerDispatchIntegrationBuilder::new()
            .predictor(predictor)
            .build();

        assert!(integration.is_enabled());
    }

    #[test]
    fn test_select_tasks_when_disabled() {
        let mut integration = create_integration_with_worker();
        integration.set_enabled(false);

        let tasks = vec![TestTask::new("task1", "test-queue", "default")];

        let selected =
            integration.select_tasks(&tasks, "OrderWorkflow", "worker-1", "test-queue", "default");
        assert!(selected.is_empty());
    }

    #[test]
    fn test_select_tasks_when_enabled() {
        let integration = create_integration_with_worker();

        let tasks = vec![
            TestTask::new("validate", "test-queue", "default"),
            TestTask::new("process", "test-queue", "default"),
        ];

        let selected =
            integration.select_tasks(&tasks, "OrderWorkflow", "worker-1", "test-queue", "default");

        assert_eq!(selected.len(), 2);
    }

    #[test]
    fn test_select_matching_tasks_when_disabled() {
        let mut integration = create_integration_with_worker();
        integration.set_enabled(false);

        let tasks = vec![TestTask::new("task1", "test-queue", "default")];

        let selected = integration.select_matching_tasks(&tasks, "test-queue", "default", 10);
        assert!(selected.is_empty());
    }

    #[test]
    fn test_select_matching_tasks_when_enabled() {
        let integration = create_integration_with_worker();

        let tasks = vec![
            TestTask::new("task1", "test-queue", "default"),
            TestTask::new("task2", "other-queue", "default"),
            TestTask::new("task3", "test-queue", "default"),
        ];

        let selected = integration.select_matching_tasks(&tasks, "test-queue", "default", 10);

        assert_eq!(selected.len(), 2);
    }

    #[test]
    fn test_stats() {
        let integration = create_integration_with_worker();

        let tasks = vec![TestTask::new("task1", "test-queue", "default")];

        integration.select_tasks(&tasks, "Workflow", "worker-1", "test-queue", "default");

        let stats = integration.stats();
        assert!(stats.total_dispatched >= 1);
    }

    #[test]
    fn test_metrics() {
        let integration = create_integration_with_worker();

        let tasks = vec![TestTask::new("task1", "test-queue", "default")];

        integration.select_tasks(&tasks, "Workflow", "worker-1", "test-queue", "default");

        let metrics = integration.metrics();
        assert!(
            metrics
                .dispatch_attempts
                .load(std::sync::atomic::Ordering::Relaxed)
                >= 1
        );
    }
}
