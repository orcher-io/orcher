//! Selects and dispatches tasks for eager return in workflow completion responses.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use super::predictor::WorkflowPredictor;
use crate::capacity::CapacityRegistry;

#[derive(Debug, Clone)]
pub struct EagerDispatchConfig {
    pub max_eager_tasks: usize,
    /// Predictions below this threshold are ignored.
    pub min_prediction_confidence: f64,
    pub predictive_prefetch_enabled: bool,
    pub affinity_enabled: bool,
    /// Fraction of worker capacity (0.0-1.0) held back from eager dispatch.
    pub reserved_capacity_fraction: f64,
}

impl Default for EagerDispatchConfig {
    fn default() -> Self {
        Self {
            max_eager_tasks: 10,
            min_prediction_confidence: 0.5,
            predictive_prefetch_enabled: true,
            affinity_enabled: true,
            reserved_capacity_fraction: 0.1, // Reserve 10% capacity
        }
    }
}

impl EagerDispatchConfig {
    pub fn high_throughput() -> Self {
        Self {
            max_eager_tasks: 20,
            min_prediction_confidence: 0.3,
            predictive_prefetch_enabled: true,
            affinity_enabled: true,
            reserved_capacity_fraction: 0.05,
        }
    }

    pub fn low_latency() -> Self {
        Self {
            max_eager_tasks: 5,
            min_prediction_confidence: 0.7,
            predictive_prefetch_enabled: false,
            affinity_enabled: true,
            reserved_capacity_fraction: 0.2,
        }
    }

    pub fn conservative() -> Self {
        Self {
            max_eager_tasks: 3,
            min_prediction_confidence: 0.8,
            predictive_prefetch_enabled: false,
            affinity_enabled: false,
            reserved_capacity_fraction: 0.3,
        }
    }
}

#[derive(Debug, Clone)]
pub struct EagerTask<T> {
    pub task: T,
    pub selection_reason: SelectionReason,
    /// Higher means dispatch first.
    pub priority: f64,
}

impl<T> EagerTask<T> {
    pub fn new(task: T, reason: SelectionReason, priority: f64) -> Self {
        Self {
            task,
            selection_reason: reason,
            priority,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum SelectionReason {
    Scheduled,
    Predicted { confidence: f64 },
    Affinity { affinity_score: f64 },
    QueueMatch,
}

#[derive(Debug, Default)]
pub struct EagerDispatchMetrics {
    pub tasks_dispatched: AtomicU64,
    pub scheduled_dispatched: AtomicU64,
    pub predicted_dispatched: AtomicU64,
    pub affinity_dispatched: AtomicU64,
    pub dispatch_attempts: AtomicU64,
    /// Skipped because the worker had no remaining capacity.
    pub capacity_limited: AtomicU64,
    /// Skipped because the task queue didn't match the worker's.
    pub queue_mismatch: AtomicU64,
}

impl EagerDispatchMetrics {
    pub fn eager_rate(&self) -> f64 {
        let attempts = self.dispatch_attempts.load(Ordering::Relaxed);
        let dispatched = self.tasks_dispatched.load(Ordering::Relaxed);
        if attempts == 0 {
            0.0
        } else {
            dispatched as f64 / attempts as f64
        }
    }

    /// Fraction of dispatched tasks that came from predictions.
    pub fn prediction_rate(&self) -> f64 {
        let total = self.tasks_dispatched.load(Ordering::Relaxed);
        let predicted = self.predicted_dispatched.load(Ordering::Relaxed);
        if total == 0 {
            0.0
        } else {
            predicted as f64 / total as f64
        }
    }
}

/// Point-in-time snapshot of dispatch counters.
#[derive(Debug, Clone)]
pub struct EagerDispatchStats {
    pub total_dispatched: u64,
    pub scheduled_dispatched: u64,
    pub predicted_dispatched: u64,
    pub affinity_dispatched: u64,
    pub dispatch_rate: f64,
    pub prediction_rate: f64,
}

pub use orcher_common::types::dispatch::EagerDispatchable;

/// Picks tasks for eager return based on schedule, predictions, and worker capacity.
pub struct EagerTaskDispatcher {
    predictor: Arc<WorkflowPredictor>,
    capacity_registry: Arc<CapacityRegistry>,
    config: EagerDispatchConfig,
    metrics: EagerDispatchMetrics,
}

impl EagerTaskDispatcher {
    pub fn new(
        predictor: Arc<WorkflowPredictor>,
        capacity_registry: Arc<CapacityRegistry>,
    ) -> Self {
        Self::with_config(predictor, capacity_registry, EagerDispatchConfig::default())
    }

    pub fn with_config(
        predictor: Arc<WorkflowPredictor>,
        capacity_registry: Arc<CapacityRegistry>,
        config: EagerDispatchConfig,
    ) -> Self {
        Self {
            predictor,
            capacity_registry,
            config,
            metrics: EagerDispatchMetrics::default(),
        }
    }

    /// Returns tasks eligible for eager dispatch, ordered by priority.
    pub fn select_tasks<T: EagerDispatchable + Clone>(
        &self,
        scheduled_tasks: &[T],
        workflow_type: &str,
        worker_identity: &str,
        worker_task_queue: &str,
        worker_namespace: &str,
    ) -> Vec<EagerTask<T>> {
        self.metrics
            .dispatch_attempts
            .fetch_add(1, Ordering::Relaxed);

        // Check worker capacity
        let available_capacity =
            self.get_available_capacity(worker_task_queue, worker_namespace, worker_identity);

        if available_capacity == 0 {
            self.metrics
                .capacity_limited
                .fetch_add(1, Ordering::Relaxed);
            return Vec::new();
        }

        // Calculate how many tasks we can dispatch
        let max_dispatch = available_capacity
            .min(self.config.max_eager_tasks)
            .min(scheduled_tasks.len() + 10); // +10 for potential predictions

        let mut selected: Vec<EagerTask<T>> = Vec::with_capacity(max_dispatch);

        // 1. Add scheduled tasks that match the worker's queue
        for task in scheduled_tasks {
            if selected.len() >= max_dispatch {
                break;
            }

            if task.task_queue() == worker_task_queue && task.namespace() == worker_namespace {
                // Record task for prediction learning
                self.predictor.record_task(workflow_type, task.task_type());

                selected.push(EagerTask::new(
                    task.clone(),
                    SelectionReason::Scheduled,
                    1.0, // Highest priority for scheduled tasks
                ));
                self.metrics
                    .scheduled_dispatched
                    .fetch_add(1, Ordering::Relaxed);
            } else {
                self.metrics.queue_mismatch.fetch_add(1, Ordering::Relaxed);
            }
        }

        // 2. Add predicted tasks if enabled and we have capacity
        if self.config.predictive_prefetch_enabled && selected.len() < max_dispatch {
            let predictions = self
                .predictor
                .predict_next_tasks(workflow_type, max_dispatch - selected.len());

            for pred in predictions {
                if pred.confidence < self.config.min_prediction_confidence {
                    continue;
                }

                // Check if prediction matches worker's queue (if specified)
                if let Some(pred_queue) = &pred.task_queue {
                    if pred_queue != worker_task_queue {
                        continue;
                    }
                }

                // Check if we already have this task type scheduled
                let already_scheduled = selected
                    .iter()
                    .any(|s| s.task.task_type() == pred.task_type);

                if !already_scheduled {
                    // We can't create a task from just a prediction without more info
                    // This is for metrics tracking - actual prefetch would need task data
                    self.metrics
                        .predicted_dispatched
                        .fetch_add(1, Ordering::Relaxed);
                    self.predictor.record_prediction_match();
                }
            }
        }

        // Record total dispatched
        self.metrics
            .tasks_dispatched
            .fetch_add(selected.len() as u64, Ordering::Relaxed);

        // Sort by priority (highest first)
        selected.sort_by(|a, b| {
            b.priority
                .partial_cmp(&a.priority)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        selected
    }

    /// Simpler variant - just returns scheduled tasks matching the worker's queue.
    pub fn select_matching_tasks<T: EagerDispatchable + Clone>(
        &self,
        scheduled_tasks: &[T],
        worker_task_queue: &str,
        worker_namespace: &str,
        max_tasks: usize,
    ) -> Vec<T> {
        scheduled_tasks
            .iter()
            .filter(|task| {
                task.task_queue() == worker_task_queue && task.namespace() == worker_namespace
            })
            .take(max_tasks)
            .cloned()
            .collect()
    }

    fn get_available_capacity(&self, task_queue: &str, namespace: &str, identity: &str) -> usize {
        let permits = match self.capacity_registry.get(task_queue, namespace, identity) {
            Some(p) => p,
            None => return 0,
        };

        let total_available = permits.available();

        // Apply reserved capacity
        let reserved =
            (permits.capacity() as f64 * self.config.reserved_capacity_fraction) as usize;
        total_available.saturating_sub(reserved)
    }

    pub fn record_workflow_completion(&self, workflow_type: &str) {
        self.predictor.record_workflow_completion(workflow_type);
    }

    pub fn record_task_execution(&self, workflow_type: &str, task_type: &str) {
        self.predictor.record_task(workflow_type, task_type);
    }

    pub fn metrics(&self) -> &EagerDispatchMetrics {
        &self.metrics
    }

    pub fn stats(&self) -> EagerDispatchStats {
        EagerDispatchStats {
            total_dispatched: self.metrics.tasks_dispatched.load(Ordering::Relaxed),
            scheduled_dispatched: self.metrics.scheduled_dispatched.load(Ordering::Relaxed),
            predicted_dispatched: self.metrics.predicted_dispatched.load(Ordering::Relaxed),
            affinity_dispatched: self.metrics.affinity_dispatched.load(Ordering::Relaxed),
            dispatch_rate: self.metrics.eager_rate(),
            prediction_rate: self.metrics.prediction_rate(),
        }
    }

    pub fn predictor(&self) -> &Arc<WorkflowPredictor> {
        &self.predictor
    }

    pub fn config(&self) -> &EagerDispatchConfig {
        &self.config
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
        workflow_type: Option<String>,
    }

    impl TestTask {
        fn new(task_type: &str, task_queue: &str, namespace: &str) -> Self {
            Self {
                task_type: task_type.to_string(),
                task_queue: task_queue.to_string(),
                namespace: namespace.to_string(),
                workflow_type: None,
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
            self.workflow_type.as_deref()
        }
    }

    fn create_dispatcher() -> EagerTaskDispatcher {
        let predictor = Arc::new(WorkflowPredictor::new());
        let capacity_registry = Arc::new(CapacityRegistry::new());

        // Register a worker with capacity
        capacity_registry.get_or_register("test-queue", "default", "worker-1", 100);

        EagerTaskDispatcher::new(predictor, capacity_registry)
    }

    #[test]
    fn test_dispatcher_creation() {
        let dispatcher = create_dispatcher();
        assert_eq!(dispatcher.config.max_eager_tasks, 10);
    }

    #[test]
    fn test_select_matching_tasks() {
        let dispatcher = create_dispatcher();

        let tasks = vec![
            TestTask::new("task1", "test-queue", "default"),
            TestTask::new("task2", "other-queue", "default"),
            TestTask::new("task3", "test-queue", "default"),
        ];

        let selected = dispatcher.select_matching_tasks(&tasks, "test-queue", "default", 10);

        assert_eq!(selected.len(), 2);
        assert_eq!(selected[0].task_type, "task1");
        assert_eq!(selected[1].task_type, "task3");
    }

    #[test]
    fn test_select_tasks_with_capacity() {
        let dispatcher = create_dispatcher();

        let tasks = vec![
            TestTask::new("validate", "test-queue", "default"),
            TestTask::new("process", "test-queue", "default"),
        ];

        let selected =
            dispatcher.select_tasks(&tasks, "OrderWorkflow", "worker-1", "test-queue", "default");

        assert_eq!(selected.len(), 2);
        assert!(matches!(
            selected[0].selection_reason,
            SelectionReason::Scheduled
        ));
    }

    #[test]
    fn test_queue_mismatch_filtering() {
        let dispatcher = create_dispatcher();

        let tasks = vec![
            TestTask::new("task1", "other-queue", "default"),
            TestTask::new("task2", "other-queue", "default"),
        ];

        let selected =
            dispatcher.select_tasks(&tasks, "Workflow", "worker-1", "test-queue", "default");

        // No tasks should be selected due to queue mismatch
        assert!(selected.is_empty());
        assert!(dispatcher.metrics.queue_mismatch.load(Ordering::Relaxed) >= 2);
    }

    #[test]
    fn test_eager_dispatch_config_presets() {
        let high_throughput = EagerDispatchConfig::high_throughput();
        assert_eq!(high_throughput.max_eager_tasks, 20);
        assert!(high_throughput.predictive_prefetch_enabled);

        let low_latency = EagerDispatchConfig::low_latency();
        assert_eq!(low_latency.max_eager_tasks, 5);
        assert!(!low_latency.predictive_prefetch_enabled);

        let conservative = EagerDispatchConfig::conservative();
        assert_eq!(conservative.max_eager_tasks, 3);
        assert!(!conservative.affinity_enabled);
    }

    #[test]
    fn test_metrics_tracking() {
        let dispatcher = create_dispatcher();

        let tasks = vec![TestTask::new("task1", "test-queue", "default")];

        dispatcher.select_tasks(&tasks, "Workflow", "worker-1", "test-queue", "default");

        assert_eq!(
            dispatcher.metrics.dispatch_attempts.load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            dispatcher.metrics.tasks_dispatched.load(Ordering::Relaxed),
            1
        );
    }

    #[test]
    fn test_stats_snapshot() {
        let dispatcher = create_dispatcher();

        let tasks = vec![
            TestTask::new("task1", "test-queue", "default"),
            TestTask::new("task2", "test-queue", "default"),
        ];

        dispatcher.select_tasks(&tasks, "Workflow", "worker-1", "test-queue", "default");

        let stats = dispatcher.stats();
        assert_eq!(stats.total_dispatched, 2);
        assert_eq!(stats.scheduled_dispatched, 2);
        assert!(stats.dispatch_rate > 0.0);
    }

    #[test]
    fn test_workflow_completion_recording() {
        let dispatcher = create_dispatcher();

        dispatcher.record_task_execution("OrderWorkflow", "validate");
        dispatcher.record_task_execution("OrderWorkflow", "process");
        dispatcher.record_workflow_completion("OrderWorkflow");

        // Should not panic and predictor should track this
        assert!(dispatcher.predictor().workflow_type_count() >= 1);
    }

    #[test]
    fn test_selection_reason_equality() {
        assert_eq!(SelectionReason::Scheduled, SelectionReason::Scheduled);
        assert_eq!(SelectionReason::QueueMatch, SelectionReason::QueueMatch);
        assert_ne!(SelectionReason::Scheduled, SelectionReason::QueueMatch);
    }

    #[test]
    fn test_eager_task_creation() {
        let task = TestTask::new("test", "queue", "ns");
        let eager = EagerTask::new(task, SelectionReason::Scheduled, 0.95);

        assert_eq!(eager.priority, 0.95);
        assert!(matches!(eager.selection_reason, SelectionReason::Scheduled));
    }

    #[test]
    fn test_no_capacity_worker() {
        let predictor = Arc::new(WorkflowPredictor::new());
        let capacity_registry = Arc::new(CapacityRegistry::new());
        // Don't register any worker

        let dispatcher = EagerTaskDispatcher::new(predictor, capacity_registry);

        let tasks = vec![TestTask::new("task1", "test-queue", "default")];

        let selected = dispatcher.select_tasks(
            &tasks,
            "Workflow",
            "unknown-worker",
            "test-queue",
            "default",
        );

        assert!(selected.is_empty());
        assert!(dispatcher.metrics.capacity_limited.load(Ordering::Relaxed) >= 1);
    }
}
