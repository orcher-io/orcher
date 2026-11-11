//! Type definitions for worker capacity management.

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Strategy for determining worker concurrency capacity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CapacityProvider {
    /// Fixed number of permits (static capacity).
    Fixed { max_permits: usize },

    /// Auto-tuning capacity based on system resource utilization.
    ResourceBased {
        target_cpu_percent: f64,
        target_memory_percent: f64,
        min_permits: usize,
        max_permits: usize,
    },
}

impl Default for CapacityProvider {
    fn default() -> Self {
        // Default to 100 concurrent tasks (matches SDK default)
        Self::Fixed { max_permits: 100 }
    }
}

impl CapacityProvider {
    pub fn fixed(max_permits: usize) -> Self {
        Self::Fixed { max_permits }
    }

    pub fn resource_based(
        target_cpu_percent: f64,
        target_memory_percent: f64,
        min_permits: usize,
        max_permits: usize,
    ) -> Self {
        Self::ResourceBased {
            target_cpu_percent,
            target_memory_percent,
            min_permits,
            max_permits,
        }
    }

    pub fn initial_permits(&self) -> usize {
        match self {
            Self::Fixed { max_permits } => *max_permits,
            Self::ResourceBased { max_permits, .. } => *max_permits,
        }
    }
}

/// Statistics for a worker's task permits.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskPermitStats {
    pub identity: String,
    pub task_queue: String,
    pub namespace: String,
    pub max_permits: usize,
    pub used_permits: usize,
    pub available_permits: usize,
    pub utilization: f64,
    pub total_acquired: usize,
    pub total_released: usize,
    pub uptime: Duration,
}

/// Capacity statistics for a task queue.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueCapacityStats {
    pub task_queue: String,
    pub namespace: String,
    pub worker_count: usize,
    pub total_capacity: usize,
    pub total_used: usize,
    pub total_available: usize,
    pub utilization: f64,
    pub workers: Vec<TaskPermitStats>,
}

/// Global capacity statistics across all queues.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalCapacityStats {
    pub queue_count: usize,
    pub worker_count: usize,
    pub total_capacity: usize,
    pub total_used: usize,
    pub total_available: usize,
    pub utilization: f64,
}

/// Configuration for eager task dispatch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EagerDispatchConfig {
    pub enabled: bool,
    pub max_eager_tasks: usize,
    pub enable_prediction: bool,
    pub prediction_confidence_threshold: f64,
    pub pattern_max_age_secs: u64,
}

impl Default for EagerDispatchConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_eager_tasks: 3,
            enable_prediction: true,
            prediction_confidence_threshold: 0.8,
            pattern_max_age_secs: 3600,
        }
    }
}

/// Reason a task was selected for eager dispatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SelectionReason {
    /// Task was explicitly scheduled.
    Scheduled,
    /// Task was predicted from workflow patterns.
    Predicted,
    /// Task matched worker affinity.
    Affinity,
    /// Task matched the polled queue.
    QueueMatch,
}

/// Statistics for eager task dispatch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EagerDispatchStats {
    pub total_dispatched: u64,
    pub scheduled_count: u64,
    pub predicted_count: u64,
    pub affinity_count: u64,
    pub queue_match_count: u64,
    pub prediction_accuracy: f64,
}

impl Default for EagerDispatchStats {
    fn default() -> Self {
        Self {
            total_dispatched: 0,
            scheduled_count: 0,
            predicted_count: 0,
            affinity_count: 0,
            queue_match_count: 0,
            prediction_accuracy: 0.0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_capacity_provider_default() {
        let provider = CapacityProvider::default();
        assert_eq!(provider.initial_permits(), 100);
    }

    #[test]
    fn test_capacity_provider_fixed() {
        let provider = CapacityProvider::fixed(50);
        assert_eq!(provider.initial_permits(), 50);
    }

    #[test]
    fn test_capacity_provider_resource_based() {
        let provider = CapacityProvider::resource_based(80.0, 70.0, 10, 200);
        assert_eq!(provider.initial_permits(), 200);
    }

    #[test]
    fn test_eager_dispatch_config_defaults() {
        let config = EagerDispatchConfig::default();
        assert!(config.enabled);
        assert_eq!(config.max_eager_tasks, 3);
        assert!(config.enable_prediction);
        assert!((config.prediction_confidence_threshold - 0.8).abs() < 0.001);
    }

    #[test]
    fn test_selection_reason() {
        assert_eq!(SelectionReason::Scheduled, SelectionReason::Scheduled);
        assert_ne!(SelectionReason::Scheduled, SelectionReason::Predicted);
    }
}
