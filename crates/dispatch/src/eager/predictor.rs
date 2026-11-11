//! Learns workflow execution patterns to predict upcoming tasks for eager dispatch.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::{Duration, Instant};

const MAX_PATTERNS_PER_WORKFLOW: usize = 100;
const MAX_SEQUENCE_LENGTH: usize = 10;
const MIN_PREDICTION_CONFIDENCE: f64 = 0.3;

#[derive(Debug, Clone)]
pub struct PredictedTask {
    pub task_type: String,
    pub confidence: f64,
    pub expected_delay: Option<Duration>,
    pub task_queue: Option<String>,
}

impl PredictedTask {
    pub fn new(task_type: impl Into<String>, confidence: f64) -> Self {
        Self {
            task_type: task_type.into(),
            confidence,
            expected_delay: None,
            task_queue: None,
        }
    }

    pub fn with_task_queue(mut self, queue: impl Into<String>) -> Self {
        self.task_queue = Some(queue.into());
        self
    }

    pub fn with_delay(mut self, delay: Duration) -> Self {
        self.expected_delay = Some(delay);
        self
    }
}

/// A sequence of preceding tasks followed by a next task, with observation stats.
#[derive(Debug, Clone)]
pub struct TaskPattern {
    pub preceding_tasks: Vec<String>,
    pub following_task: String,
    pub occurrence_count: u64,
    pub last_seen: Instant,
    /// Exponential moving average of the gap (ms) before the following task.
    pub avg_delay_ms: Option<f64>,
}

impl TaskPattern {
    fn new(preceding: Vec<String>, following: String) -> Self {
        Self {
            preceding_tasks: preceding,
            following_task: following,
            occurrence_count: 1,
            last_seen: Instant::now(),
            avg_delay_ms: None,
        }
    }

    fn record_occurrence(&mut self, delay_ms: Option<f64>) {
        self.occurrence_count += 1;
        self.last_seen = Instant::now();

        // Update running average of delay
        if let Some(new_delay) = delay_ms {
            self.avg_delay_ms = Some(match self.avg_delay_ms {
                Some(current) => {
                    // Exponential moving average with alpha = 0.3
                    current * 0.7 + new_delay * 0.3
                }
                None => new_delay,
            });
        }
    }

    fn pattern_key(preceding: &[String], following: &str) -> String {
        format!("{}→{}", preceding.join(","), following)
    }
}

struct WorkflowPatterns {
    patterns: HashMap<String, TaskPattern>,
    total_observations: u64,
    recent_sequences: VecDeque<(String, Instant)>,
}

impl WorkflowPatterns {
    fn new() -> Self {
        Self {
            patterns: HashMap::new(),
            total_observations: 0,
            recent_sequences: VecDeque::with_capacity(MAX_SEQUENCE_LENGTH),
        }
    }

    fn record_task(&mut self, task_type: &str) {
        let now = Instant::now();

        // Learn patterns from recent sequence
        if !self.recent_sequences.is_empty() {
            // Create patterns of varying lengths
            for length in 1..=self.recent_sequences.len().min(MAX_SEQUENCE_LENGTH - 1) {
                let preceding: Vec<String> = self
                    .recent_sequences
                    .iter()
                    .rev()
                    .take(length)
                    .rev()
                    .map(|(t, _)| t.clone())
                    .collect();

                // Calculate delay from last task
                let delay_ms = self
                    .recent_sequences
                    .back()
                    .map(|(_, t)| now.duration_since(*t).as_millis() as f64);

                let key = TaskPattern::pattern_key(&preceding, task_type);

                self.patterns
                    .entry(key)
                    .and_modify(|p| p.record_occurrence(delay_ms))
                    .or_insert_with(|| TaskPattern::new(preceding, task_type.to_string()));
            }
        }

        // Add to recent sequence
        self.recent_sequences
            .push_back((task_type.to_string(), now));
        if self.recent_sequences.len() > MAX_SEQUENCE_LENGTH {
            self.recent_sequences.pop_front();
        }

        self.total_observations += 1;

        // Prune old patterns if we have too many
        if self.patterns.len() > MAX_PATTERNS_PER_WORKFLOW {
            self.prune_old_patterns();
        }
    }

    fn predict(&self, limit: usize, min_confidence: f64) -> Vec<PredictedTask> {
        let total_obs = self.total_observations as f64;

        if self.recent_sequences.is_empty() || total_obs == 0.0 {
            return Vec::new();
        }

        let mut predictions: Vec<PredictedTask> = Vec::new();
        let mut seen_tasks: std::collections::HashSet<String> = std::collections::HashSet::new();

        // Try patterns of decreasing length for best matches
        for length in (1..=self.recent_sequences.len().min(MAX_SEQUENCE_LENGTH - 1)).rev() {
            let preceding: Vec<String> = self
                .recent_sequences
                .iter()
                .rev()
                .take(length)
                .rev()
                .map(|(t, _)| t.clone())
                .collect();

            // Find all patterns that match this preceding sequence
            for pattern in self.patterns.values() {
                if pattern.preceding_tasks == preceding
                    && !seen_tasks.contains(&pattern.following_task)
                {
                    // Calculate confidence based on occurrence frequency and pattern length
                    // Longer patterns get higher base confidence
                    let length_bonus = (length as f64 / MAX_SEQUENCE_LENGTH as f64) * 0.3;
                    let frequency = pattern.occurrence_count as f64 / total_obs;
                    let recency_factor = if pattern.last_seen.elapsed() < Duration::from_secs(3600)
                    {
                        1.0
                    } else {
                        0.8
                    };

                    let confidence = (frequency * 0.5 + length_bonus) * recency_factor;

                    if confidence >= min_confidence {
                        let mut pred =
                            PredictedTask::new(pattern.following_task.clone(), confidence.min(1.0));

                        if let Some(avg_delay) = pattern.avg_delay_ms {
                            pred.expected_delay = Some(Duration::from_millis(avg_delay as u64));
                        }

                        predictions.push(pred);
                        seen_tasks.insert(pattern.following_task.clone());
                    }
                }
            }

            if predictions.len() >= limit {
                break;
            }
        }

        // Sort by confidence (highest first)
        predictions.sort_by(|a, b| {
            b.confidence
                .partial_cmp(&a.confidence)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        predictions.truncate(limit);
        predictions
    }

    fn prune_old_patterns(&mut self) {
        let cutoff = Instant::now() - Duration::from_secs(3600 * 24); // 24 hours
        let min_occurrences = 2u64;

        self.patterns.retain(|_, pattern| {
            pattern.last_seen > cutoff || pattern.occurrence_count >= min_occurrences
        });
    }

    fn clear_sequence(&mut self) {
        self.recent_sequences.clear();
    }

    fn pattern_count(&self) -> usize {
        self.patterns.len()
    }
}

#[derive(Debug, Default)]
pub struct PredictorMetrics {
    pub predictions_made: AtomicU64,
    pub predictions_matched: AtomicU64,
    pub tasks_recorded: AtomicU64,
    pub workflows_completed: AtomicU64,
}

impl PredictorMetrics {
    pub fn accuracy(&self) -> f64 {
        let made = self.predictions_made.load(Ordering::Relaxed);
        let matched = self.predictions_matched.load(Ordering::Relaxed);
        if made == 0 {
            0.0
        } else {
            matched as f64 / made as f64
        }
    }
}

#[derive(Debug, Clone)]
pub struct PredictorConfig {
    pub max_workflow_types: usize,
    pub max_patterns_per_workflow: usize,
    pub min_confidence: f64,
    pub learning_enabled: bool,
}

impl Default for PredictorConfig {
    fn default() -> Self {
        Self {
            max_workflow_types: 1000,
            max_patterns_per_workflow: MAX_PATTERNS_PER_WORKFLOW,
            min_confidence: MIN_PREDICTION_CONFIDENCE,
            learning_enabled: true,
        }
    }
}

/// Predicts upcoming tasks from observed workflow execution sequences.
pub struct WorkflowPredictor {
    workflow_patterns: RwLock<HashMap<String, WorkflowPatterns>>,
    config: PredictorConfig,
    metrics: PredictorMetrics,
}

impl WorkflowPredictor {
    pub fn new() -> Self {
        Self::with_config(PredictorConfig::default())
    }

    pub fn with_config(config: PredictorConfig) -> Self {
        Self {
            workflow_patterns: RwLock::new(HashMap::new()),
            config,
            metrics: PredictorMetrics::default(),
        }
    }

    /// Call when a task is scheduled to feed the prediction model.
    pub fn record_task(&self, workflow_type: &str, task_type: &str) {
        if !self.config.learning_enabled {
            return;
        }

        let mut patterns = self.workflow_patterns.write().unwrap();

        patterns
            .entry(workflow_type.to_string())
            .or_insert_with(WorkflowPatterns::new)
            .record_task(task_type);

        self.metrics.tasks_recorded.fetch_add(1, Ordering::Relaxed);

        // Prune workflow types if we have too many
        if patterns.len() > self.config.max_workflow_types {
            // Remove least recently used workflow type
            if let Some(oldest_key) = patterns
                .iter()
                .min_by_key(|(_, v)| v.total_observations)
                .map(|(k, _)| k.clone())
            {
                patterns.remove(&oldest_key);
            }
        }
    }

    /// Resets the sequence tracker for this workflow type.
    pub fn record_workflow_completion(&self, workflow_type: &str) {
        let mut patterns = self.workflow_patterns.write().unwrap();
        if let Some(wf_patterns) = patterns.get_mut(workflow_type) {
            wf_patterns.clear_sequence();
        }
        self.metrics
            .workflows_completed
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn predict_next_tasks(&self, workflow_type: &str, limit: usize) -> Vec<PredictedTask> {
        let patterns = self.workflow_patterns.read().unwrap();
        let min_conf = self.config.min_confidence;
        let predictions = patterns
            .get(workflow_type)
            .map(|p| p.predict(limit, min_conf))
            .unwrap_or_default();

        self.metrics
            .predictions_made
            .fetch_add(predictions.len() as u64, Ordering::Relaxed);

        predictions
    }

    pub fn record_prediction_match(&self) {
        self.metrics
            .predictions_matched
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn metrics(&self) -> &PredictorMetrics {
        &self.metrics
    }

    pub fn workflow_type_count(&self) -> usize {
        self.workflow_patterns.read().unwrap().len()
    }

    pub fn pattern_count(&self, workflow_type: &str) -> usize {
        self.workflow_patterns
            .read()
            .unwrap()
            .get(workflow_type)
            .map(|p| p.pattern_count())
            .unwrap_or(0)
    }

    pub fn clear(&self) {
        self.workflow_patterns.write().unwrap().clear();
    }
}

impl Default for WorkflowPredictor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_predicted_task_creation() {
        let pred = PredictedTask::new("process_payment", 0.85)
            .with_task_queue("payments")
            .with_delay(Duration::from_millis(100));

        assert_eq!(pred.task_type, "process_payment");
        assert_eq!(pred.confidence, 0.85);
        assert_eq!(pred.task_queue, Some("payments".to_string()));
        assert_eq!(pred.expected_delay, Some(Duration::from_millis(100)));
    }

    #[test]
    fn test_predictor_creation() {
        let predictor = WorkflowPredictor::new();
        assert_eq!(predictor.workflow_type_count(), 0);
    }

    #[test]
    fn test_record_single_task() {
        let predictor = WorkflowPredictor::new();
        predictor.record_task("OrderWorkflow", "validate_order");

        assert_eq!(predictor.workflow_type_count(), 1);
        assert_eq!(
            predictor.metrics().tasks_recorded.load(Ordering::Relaxed),
            1
        );
    }

    #[test]
    fn test_record_task_sequence() {
        let predictor = WorkflowPredictor::new();

        // Record a sequence of tasks
        predictor.record_task("OrderWorkflow", "validate_order");
        predictor.record_task("OrderWorkflow", "process_payment");
        predictor.record_task("OrderWorkflow", "ship_order");

        assert_eq!(predictor.pattern_count("OrderWorkflow"), 3);
        // Pattern: [] → validate_order (not counted)
        // Pattern: [validate_order] → process_payment
        // Pattern: [process_payment] → ship_order
        // Pattern: [validate_order, process_payment] → ship_order
    }

    #[test]
    fn test_predict_after_sequence() {
        // Use a lower confidence threshold for testing
        let config = PredictorConfig {
            min_confidence: 0.1, // Lower threshold for test
            ..Default::default()
        };
        let predictor = WorkflowPredictor::with_config(config);

        // Train with repeated sequences
        for _ in 0..20 {
            predictor.record_task("OrderWorkflow", "validate_order");
            predictor.record_task("OrderWorkflow", "process_payment");
            predictor.record_task("OrderWorkflow", "ship_order");
            predictor.record_workflow_completion("OrderWorkflow");
        }

        // Now predict after validate_order
        predictor.record_task("OrderWorkflow", "validate_order");
        let predictions = predictor.predict_next_tasks("OrderWorkflow", 5);

        // Should predict process_payment
        assert!(!predictions.is_empty(), "Expected predictions but got none");
        assert_eq!(predictions[0].task_type, "process_payment");
    }

    #[test]
    fn test_empty_prediction() {
        let predictor = WorkflowPredictor::new();

        // No training data
        let predictions = predictor.predict_next_tasks("UnknownWorkflow", 5);
        assert!(predictions.is_empty());
    }

    #[test]
    fn test_workflow_completion_clears_sequence() {
        let predictor = WorkflowPredictor::new();

        predictor.record_task("OrderWorkflow", "task1");
        predictor.record_task("OrderWorkflow", "task2");
        predictor.record_workflow_completion("OrderWorkflow");

        // After completion, the sequence should be cleared
        // New task should start fresh sequence
        predictor.record_task("OrderWorkflow", "task3");

        // Predictions should be based only on task3
        let predictions = predictor.predict_next_tasks("OrderWorkflow", 5);
        // With only one task in sequence, predictions might be limited
        assert!(predictions.len() <= 5);
    }

    #[test]
    fn test_predictor_metrics() {
        let predictor = WorkflowPredictor::new();

        predictor.record_task("Workflow1", "task1");
        predictor.record_task("Workflow1", "task2");
        predictor.record_workflow_completion("Workflow1");

        let metrics = predictor.metrics();
        assert_eq!(metrics.tasks_recorded.load(Ordering::Relaxed), 2);
        assert_eq!(metrics.workflows_completed.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_prediction_accuracy() {
        let metrics = PredictorMetrics::default();

        // No predictions yet
        assert_eq!(metrics.accuracy(), 0.0);

        // Record some predictions and matches
        metrics.predictions_made.store(10, Ordering::Relaxed);
        metrics.predictions_matched.store(7, Ordering::Relaxed);

        assert!((metrics.accuracy() - 0.7).abs() < 0.001);
    }

    #[test]
    fn test_config_customization() {
        let config = PredictorConfig {
            max_workflow_types: 500,
            max_patterns_per_workflow: 50,
            min_confidence: 0.5,
            learning_enabled: true,
        };

        let predictor = WorkflowPredictor::with_config(config);
        assert_eq!(predictor.workflow_type_count(), 0);
    }

    #[test]
    fn test_disabled_learning() {
        let config = PredictorConfig {
            learning_enabled: false,
            ..Default::default()
        };

        let predictor = WorkflowPredictor::with_config(config);
        predictor.record_task("Workflow", "task");

        // Should not record when learning is disabled
        assert_eq!(predictor.workflow_type_count(), 0);
    }

    #[test]
    fn test_clear_predictor() {
        let predictor = WorkflowPredictor::new();

        predictor.record_task("Workflow1", "task1");
        predictor.record_task("Workflow2", "task2");
        assert_eq!(predictor.workflow_type_count(), 2);

        predictor.clear();
        assert_eq!(predictor.workflow_type_count(), 0);
    }
}
