//! Core configuration.

use serde::{Deserialize, Serialize};

/// Core orchestration configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CoreConfig {
    /// Task coordination settings
    pub task_coordination: TaskCoordinationConfig,
    /// Worker management settings
    pub worker_management: WorkerManagementConfig,
    /// Queue management settings
    pub queue_management: QueueManagementConfig,
    /// Retry and recovery settings
    pub retry_recovery: RetryRecoveryConfig,
}

/// Task coordination configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskCoordinationConfig {
    /// Maximum concurrent task submissions
    pub max_concurrent_submissions: usize,
    /// Task submission timeout in seconds
    pub submission_timeout_seconds: u64,
    /// Maximum task execution time in seconds
    pub max_task_execution_seconds: u64,
    /// Enable task deduplication
    pub enable_task_deduplication: bool,
}

/// Worker management configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerManagementConfig {
    /// Maximum number of workers
    pub max_workers: usize,
    /// Worker heartbeat timeout in seconds
    pub heartbeat_timeout_seconds: u64,
    /// Load balancing strategy
    pub load_balancing_strategy: LoadBalancingStrategy,
    /// Enable auto-scaling
    pub enable_auto_scaling: bool,
}

/// Load balancing strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LoadBalancingStrategy {
    /// Round-robin assignment
    RoundRobin,
    /// Least loaded worker first
    LeastLoaded,
    /// Random assignment
    Random,
    /// Capability-based assignment
    CapabilityBased,
}

/// Queue management configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueManagementConfig {
    /// Maximum total queue size
    pub max_total_queue_size: usize,
    /// Processing batch size
    pub processing_batch_size: usize,
    /// Queue overflow strategy
    pub overflow_strategy: QueueOverflowStrategy,
}

/// Queue overflow strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueueOverflowStrategy {
    /// Block until space available
    Block,
    /// Reject new submissions
    Reject,
    /// Drop oldest tasks
    DropOldest,
}

/// Retry and recovery configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryRecoveryConfig {
    /// Default maximum retry attempts
    pub default_max_retry_attempts: u32,
    /// Default backoff strategy
    pub default_backoff_strategy: BackoffStrategy,
    /// Enable jitter in backoff
    pub enable_jitter: bool,
    /// Circuit breaker settings
    pub circuit_breaker: CircuitBreakerConfig,
}

/// Backoff strategy for retries
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackoffStrategy {
    /// Constant backoff
    Constant,
    /// Linear backoff
    Linear,
    /// Exponential backoff
    Exponential,
}

/// Circuit breaker configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircuitBreakerConfig {
    /// Enable circuit breaker
    pub enabled: bool,
    /// Failure threshold before opening circuit
    pub failure_threshold: u32,
    /// Recovery timeout in seconds
    pub recovery_timeout_seconds: u64,
}

impl Default for TaskCoordinationConfig {
    fn default() -> Self {
        Self {
            max_concurrent_submissions: 1000,
            submission_timeout_seconds: 30,
            max_task_execution_seconds: 3600,
            enable_task_deduplication: true,
        }
    }
}

impl Default for WorkerManagementConfig {
    fn default() -> Self {
        Self {
            max_workers: 100,
            heartbeat_timeout_seconds: 30,
            load_balancing_strategy: LoadBalancingStrategy::LeastLoaded,
            enable_auto_scaling: false,
        }
    }
}

impl Default for QueueManagementConfig {
    fn default() -> Self {
        Self {
            max_total_queue_size: 10000,
            processing_batch_size: 100,
            overflow_strategy: QueueOverflowStrategy::Reject,
        }
    }
}

impl Default for RetryRecoveryConfig {
    fn default() -> Self {
        Self {
            default_max_retry_attempts: 3,
            default_backoff_strategy: BackoffStrategy::Exponential,
            enable_jitter: true,
            circuit_breaker: CircuitBreakerConfig::default(),
        }
    }
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            failure_threshold: 5,
            recovery_timeout_seconds: 60,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_core_config_default() {
        let config = CoreConfig::default();
        assert_eq!(config.task_coordination.max_concurrent_submissions, 1000);
        assert_eq!(config.worker_management.max_workers, 100);
    }

    #[test]
    fn test_load_balancing_strategy() {
        let strategy = LoadBalancingStrategy::LeastLoaded;
        assert_eq!(strategy, LoadBalancingStrategy::LeastLoaded);
    }

    #[test]
    fn test_queue_overflow_strategy() {
        let strategy = QueueOverflowStrategy::Reject;
        assert_eq!(strategy, QueueOverflowStrategy::Reject);
    }

    #[test]
    fn test_backoff_strategy() {
        let strategy = BackoffStrategy::Exponential;
        assert_eq!(strategy, BackoffStrategy::Exponential);
    }

    #[test]
    fn test_circuit_breaker_config() {
        let config = CircuitBreakerConfig::default();
        assert!(config.enabled);
        assert_eq!(config.failure_threshold, 5);
    }

    #[test]
    fn test_serialization() {
        let config = CoreConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let deserialized: CoreConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(
            deserialized.task_coordination.max_concurrent_submissions,
            config.task_coordination.max_concurrent_submissions
        );
    }
}
